package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erwannd/dfs/messages"
	"github.com/erwannd/dfs/utils"
)

// StorageNode struct, register, heartbeat loop
type StorageNode struct {
	nodeId         uint32
	controllerHost string
	controllerPort int32
	storageDir     string
	port           int32         // this node's listening port
	numRequests    atomic.Uint64 // use s.numRequests.Add(1)
	mu             sync.Mutex
	newChunks      []*messages.ChunkInfo
}

// Storage Node                    Controller
//
//	|                               |
//	|---[ TCP connect ]------------>|
//	|---[ RegisterRequest ]-------->|
//	|<--[ RegisterResponse ]--------|
//	|                               |
//	| store nodeId                  |
//	| start heartbeat loop          |

// | Approach 					| Controller restart behavior |
// |----------------------------|-----------------------------|
// | Store handler in struct 	| Node is permanently broken until manually restarted |
// | Pass handler as parameter 	| Node automatically reconnects and re-registers |
func (node *StorageNode) register() (*messages.MessageHandler, error) {

	// Connect to Controller + create handler
	controllerAddr := net.JoinHostPort(node.controllerHost, fmt.Sprintf("%d", node.controllerPort))
	conn, err := net.Dial("tcp", controllerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to controller: %w", err)
	}
	handler := messages.NewMessageHandler(conn)

	// Send registerRequest
	hostname, _ := os.Hostname()
	freeSpace := utils.GetAvailableDiskSpace(node.storageDir)
	req := &messages.Wrapper{
		Msg: &messages.Wrapper_RegisterRequest{
			RegisterRequest: &messages.RegisterRequest{
				Hostname:  hostname,
				Port:      int32(node.port),
				FreeSpace: freeSpace,
			},
		},
	}
	if err := handler.Send(req); err != nil {
		return nil, fmt.Errorf("failed to send RegisterRequest: %w", err)
	}

	// Wait for RegisterResponse
	wrapper, err := handler.Receive()
	if err != nil {
		return nil, fmt.Errorf("failed to receive RegisterResponse: %w", err)
	}
	resp := wrapper.Msg.(*messages.Wrapper_RegisterResponse).RegisterResponse
	if !resp.Ok {
		return nil, fmt.Errorf("registration rejected by controller: %s", resp.Error)
	}

	// Save NodeId
	node.nodeId = resp.NodeId
	log.Printf("[StorageNode] Registered with Controller, assigned ID: %d", node.nodeId)
	return handler, nil
}

func (s *StorageNode) heartbeatLoop(handler *messages.MessageHandler) {
	for {
		time.Sleep(5 * time.Second)

		// Pop pending new chunks
		s.mu.Lock()
		newChunks := s.newChunks
		s.newChunks = nil // reset
		s.mu.Unlock()

		heartbeat := &messages.Wrapper{
			Msg: &messages.Wrapper_Heartbeat{
				Heartbeat: &messages.Heartbeat{
					NodeId:     s.nodeId,
					FreeSpace:  utils.GetAvailableDiskSpace(s.storageDir),
					NumRequest: s.numRequests.Load(),
					NewChunks:  newChunks, // ← piggybacked
				},
			},
		}

		err := handler.Send(heartbeat)
		if err != nil {
			log.Printf("[StorageNode] Heartbeat failed: %v", err)
			return // ← signal that connection is dead
		}

		wrapper, err := handler.Receive()
		if err != nil {
			log.Printf("[StorageNode] Lost Controller response: %v", err)
			return // ← signal that connection is dead
		}
		resp := wrapper.Msg.(*messages.Wrapper_HeartbeatResponse).HeartbeatResponse

		// Handle replication requests
		for _, req := range resp.ReplicateRequest {
			go s.handleReplicateRequest(req) // handle in background
		}

		log.Printf("[StorageNode] Heartbeat acknowledged by Controller (free space: %d MB)", utils.GetAvailableDiskSpace(s.storageDir)>>20)
	}
}

func (s *StorageNode) handleReplicateRequest(req *messages.ReplicateRequest) {
	// 1. Fetch chunk from source node
	addr := fmt.Sprintf("%s:%d", req.SrcHost, req.SrcPort)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("[StorageNode] Failed to connect to source %s: %v", addr, err)
		return
	}
	srcHandler := messages.NewMessageHandler(conn)
	defer srcHandler.Close()

	fetchReq := &messages.Wrapper{
		Msg: &messages.Wrapper_RetrieveChunkRequest{
			RetrieveChunkRequest: &messages.RetrieveChunkRequest{
				ChunkInfo: req.ChunkInfo,
			},
		},
	}
	if err := srcHandler.Send(fetchReq); err != nil {
		log.Printf("[StorageNode] Failed to fetch chunk for replication: %v", err)
		return
	}

	fetchWrapper, err := srcHandler.Receive()
	if err != nil {
		log.Printf("[StorageNode] Failed to receive chunk for replication: %v", err)
		return
	}
	fetchResp := fetchWrapper.Msg.(*messages.Wrapper_RetrieveChunkResponse).RetrieveChunkResponse
	if !fetchResp.Ok {
		log.Printf("[StorageNode] Source rejected replication fetch: %s", fetchResp.Error)
		return
	}

	// 2. Verify checksum
	if !utils.VerifyChecksum(fetchResp.ChunkData, fetchResp.Checksum) {
		log.Printf("[StorageNode] Checksum mismatch during replication of %s[%d]",
			req.ChunkInfo.Filename, req.ChunkInfo.ChunkIndex)
		return
	}

	// 3. Store chunk locally
	chunkPath := utils.ChunkPath(s.storageDir, req.ChunkInfo.Filename, req.ChunkInfo.ChunkIndex)
	if err := os.WriteFile(chunkPath, fetchResp.ChunkData, 0644); err != nil {
		log.Printf("[StorageNode] Failed to write replicated chunk: %v", err)
		return
	}
	checksumPath := utils.ChecksumPath(s.storageDir, req.ChunkInfo.Filename, req.ChunkInfo.ChunkIndex)
	if err := os.WriteFile(checksumPath, fetchResp.Checksum, 0644); err != nil {
		log.Printf("[StorageNode] Failed to write checksum for replicated chunk: %v", err)
	}

	// 4. Report back to Controller via next heartbeat
	s.mu.Lock()
	s.newChunks = append(s.newChunks, req.ChunkInfo)
	s.mu.Unlock()

	s.numRequests.Add(1)
	log.Printf("[StorageNode] Replicated chunk %s[%d] from %s",
		req.ChunkInfo.Filename, req.ChunkInfo.ChunkIndex, addr)
}

/**
 * Listen for store chunk request from Client.
 */
func (s *StorageNode) listenForChunks(listener net.Listener) {
	log.Printf("[StorageNode] Listening for chunks on :%d", s.port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[StorageNode] Accept error: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

/**
 * Only handles one message then closes.
 * This is fine for now since each client connection sends one request and closes.
 * But for future-proofing (e.g. a client that streams multiple chunks), a loop would be more robust.
 */
func (s *StorageNode) handleConnection(conn net.Conn) {
	handler := messages.NewMessageHandler(conn)
	defer handler.Close()

	wrapper, err := handler.Receive()
	if err != nil {
		log.Printf("[StorageNode] Failed to receive message: %v", err)
		return
	}

	switch msg := wrapper.Msg.(type) {
	case *messages.Wrapper_StoreChunkRequest:
		s.handleStoreChunk(msg.StoreChunkRequest, handler)
	case *messages.Wrapper_RetrieveChunkRequest:
		s.handleRetrieveChunk(msg.RetrieveChunkRequest, handler)
	case *messages.Wrapper_DeleteChunkRequest:
		s.handleDeleteChunk(msg.DeleteChunkRequest, handler)
	default:
		log.Printf("[StorageNode] Unknown message type")
	}
}

func (s *StorageNode) handleStoreChunk(msg *messages.StoreChunkRequest, handler *messages.MessageHandler) {
	// 1. Verify checksum
	if !utils.VerifyChecksum(msg.ChunkData, msg.Checksum) {
		handler.Send(errorResponse(msg.ChunkInfo, "checksum mismatch"))
		return
	}

	// 2. Write chunk to disk
	path := utils.ChunkPath(s.storageDir, msg.ChunkInfo.Filename, msg.ChunkInfo.ChunkIndex)
	if err := os.WriteFile(path, msg.ChunkData, 0644); err != nil {
		if err := handler.Send(errorResponse(msg.ChunkInfo, err.Error())); err != nil {
			log.Printf("[StorageNode] Failed to send error response: %v", err)
		}
		return
	}

	// 3. Store checksum sidecar file
	checksumPath := utils.ChecksumPath(s.storageDir, msg.ChunkInfo.Filename, msg.ChunkInfo.ChunkIndex)
	if err := os.WriteFile(checksumPath, msg.Checksum, 0644); err != nil {
		log.Printf("[StorageNode] Failed to write checksum: %v", err)
	}

	s.numRequests.Add(1)
	log.Printf("[StorageNode] Stored chunk %s[%d]", msg.ChunkInfo.Filename, msg.ChunkInfo.ChunkIndex)

	// 4. Forward to next node in pipeline if any
	if len(msg.Pipeline) > 0 {
		if err := s.forwardChunk(msg); err != nil {
			log.Printf("[StorageNode] Pipeline forward failed: %v", err)
			if err := handler.Send(errorResponse(msg.ChunkInfo, "pipeline failed to forward to next replica")); err != nil {
				log.Printf("[StorageNode] Failed to send error response: %v", err)
			}
			return
		}
	}

	// 5. Send ack back
	handler.Send(&messages.Wrapper{
		Msg: &messages.Wrapper_StoreChunkResponse{
			StoreChunkResponse: &messages.StoreChunkResponse{
				Ok:        true,
				ChunkInfo: msg.ChunkInfo,
			},
		},
	})
}

/**
 * Handle chunk retrieval.
 * TODO: Add corruption detection later.
 * Read the chunk, recompute the checksum, compare against the sidecar file.
 */
func (s *StorageNode) handleRetrieveChunk(msg *messages.RetrieveChunkRequest, handler *messages.MessageHandler) {
	filename := msg.ChunkInfo.Filename
	chunkIndex := msg.ChunkInfo.ChunkIndex

	// Get the file path
	chunkPath := utils.ChunkPath(s.storageDir, filename, chunkIndex)
	checksumPath := utils.ChecksumPath(s.storageDir, filename, chunkIndex)

	// Read chunk data from disk
	data, err := os.ReadFile(chunkPath)
	if err != nil {
		log.Printf("[StorageNode] Failed to read chunk %s[%d]: %v", filename, chunkIndex, err)
		resp := &messages.Wrapper{
			Msg: &messages.Wrapper_RetrieveChunkResponse{
				RetrieveChunkResponse: &messages.RetrieveChunkResponse{
					Ok:    false,
					Error: fmt.Sprintf("chunk not found: %v", err),
				},
			},
		}
		if err := handler.Send(resp); err != nil {
			log.Printf("[StorageNode] Failed to send error RetrieveChunkResponse: %v", err)
		}
		return
	}

	// Read checksum
	checksum, err := os.ReadFile(checksumPath)
	if err != nil {
		// Non-fatal — send data without checksum, client can still use it
		log.Printf("[StorageNode] Failed to read checksum for %s[%d]: %v", filename, chunkIndex, err)
		checksum = nil
	}

	s.numRequests.Add(1)

	// Send response
	resp := &messages.Wrapper{
		Msg: &messages.Wrapper_RetrieveChunkResponse{
			RetrieveChunkResponse: &messages.RetrieveChunkResponse{
				Ok:        true,
				ChunkData: data,
				Checksum:  checksum,
			},
		},
	}
	if err := handler.Send(resp); err != nil {
		log.Printf("[StorageNode] Failed to send RetrieveChunkResponse: %v", err)
		return
	}
	log.Printf("[StorageNode] Sent chunk %s[%d] (%d bytes)", filename, chunkIndex, len(data))
}

/**
 * Delete a chunk and its checksum.
 */
func (s *StorageNode) handleDeleteChunk(msg *messages.DeleteChunkRequest, handler *messages.MessageHandler) {
	chunkPath := utils.ChunkPath(s.storageDir, msg.ChunkInfo.Filename, msg.ChunkInfo.ChunkIndex)
	checksumPath := utils.ChecksumPath(s.storageDir, msg.ChunkInfo.Filename, msg.ChunkInfo.ChunkIndex)

	// Delete chunk file
	if err := os.Remove(chunkPath); err != nil && !os.IsNotExist(err) {
		log.Printf("[StorageNode] Failed to delete chunk %s[%d]: %v",
			msg.ChunkInfo.Filename, msg.ChunkInfo.ChunkIndex, err)
		handler.Send(errorResponse(msg.ChunkInfo, err.Error()))
		return
	}

	// Delete checksum sidecar
	if err := os.Remove(checksumPath); err != nil && !os.IsNotExist(err) {
		log.Printf("[StorageNode] Failed to delete checksum %s[%d]: %v",
			msg.ChunkInfo.Filename, msg.ChunkInfo.ChunkIndex, err)
	}

	s.numRequests.Add(1)
	log.Printf("[StorageNode] Deleted chunk %s[%d]", msg.ChunkInfo.Filename, msg.ChunkInfo.ChunkIndex)

	handler.Send(&messages.Wrapper{
		Msg: &messages.Wrapper_DeleteChunkResponse{
			DeleteChunkResponse: &messages.DeleteChunkResponse{
				Ok:        true,
				ChunkInfo: msg.ChunkInfo,
			},
		},
	})
}

func (s *StorageNode) forwardChunk(msg *messages.StoreChunkRequest) error {
	nextNode := msg.Pipeline[0]
	addr := fmt.Sprintf("%s:%d", nextNode.Hostname, nextNode.Port)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	handler := messages.NewMessageHandler(conn)
	defer handler.Close()

	// Forward same request but with pipeline shrunk by one
	forward := &messages.Wrapper{
		Msg: &messages.Wrapper_StoreChunkRequest{
			StoreChunkRequest: &messages.StoreChunkRequest{
				ChunkInfo: msg.ChunkInfo,
				ChunkData: msg.ChunkData,
				Checksum:  msg.Checksum,
				Pipeline:  msg.Pipeline[1:], // pop current node
			},
		},
	}
	if err := handler.Send(forward); err != nil {
		return fmt.Errorf("failed to forward chunk: %w", err)
	}

	// Wait for ack from downstream
	wrapper, err := handler.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive pipeline ack: %w", err)
	}
	resp := wrapper.Msg.(*messages.Wrapper_StoreChunkResponse).StoreChunkResponse
	if !resp.Ok {
		return fmt.Errorf("downstream node rejected chunk: %s", resp.Error)
	}
	return nil
}

/**
 * Creates an error message to a store chunk request.
 * Indicates that the Storage Node could not complete the request.
 */
func errorResponse(chunkInfo *messages.ChunkInfo, errMsg string) *messages.Wrapper {
	return &messages.Wrapper{
		Msg: &messages.Wrapper_StoreChunkResponse{
			StoreChunkResponse: &messages.StoreChunkResponse{
				Ok:        false,
				Error:     errMsg,
				ChunkInfo: chunkInfo,
			},
		},
	}
}
