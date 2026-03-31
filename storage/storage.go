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

/**
 * Handle a re-replication request from the Controller.
 * The destination node fetches a verified local copy from another storage node.
 * If the suggested source cannot serve a valid copy, fall back to the Controller
 * for a refreshed holder list and try the remaining replicas.
 */
func (s *StorageNode) handleReplicateRequest(req *messages.ReplicateRequest) {
	source := &messages.NodeInfo{
		Hostname: req.SrcHost,
		Port:     req.SrcPort,
	}

	data, err := s.fetchRepairChunk(source, req.ChunkInfo)
	if err != nil {
		log.Printf("[StorageNode] Suggested replication source %s:%d failed for %s[%d]: %v",
			req.SrcHost, req.SrcPort, req.ChunkInfo.Filename, req.ChunkInfo.ChunkIndex, err)

		nodes, locErr := s.fetchChunkLocations(req.ChunkInfo)
		if locErr != nil {
			log.Printf("[StorageNode] Failed to refresh replication sources for %s[%d]: %v",
				req.ChunkInfo.Filename, req.ChunkInfo.ChunkIndex, locErr)
			return
		}

		tried := map[uint32]struct{}{s.nodeId: {}}
		data, err = s.tryRepairCandidates(req.ChunkInfo, nodes, tried)
		if err != nil {
			log.Printf("[StorageNode] Failed to fetch replicated chunk %s[%d] from refreshed sources: %v",
				req.ChunkInfo.Filename, req.ChunkInfo.ChunkIndex, err)
			return
		}
	}

	// Store the replicated chunk and recomputed checksum locally.
	if _, _, err := s.persistRepairedChunk(req.ChunkInfo, data); err != nil {
		log.Printf("[StorageNode] Failed to persist replicated chunk %s[%d]: %v",
			req.ChunkInfo.Filename, req.ChunkInfo.ChunkIndex, err)
		return
	}

	// Report back to Controller via next heartbeat
	s.mu.Lock()
	s.newChunks = append(s.newChunks, req.ChunkInfo)
	s.mu.Unlock()

	s.numRequests.Add(1)
	log.Printf("[StorageNode] Replicated chunk %s[%d] from another replica",
		req.ChunkInfo.Filename, req.ChunkInfo.ChunkIndex)
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
	case *messages.Wrapper_RepairChunkRequest:
		s.handleRepairChunk(msg.RepairChunkRequest, handler)
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

func (s *StorageNode) handleRetrieveChunk(msg *messages.RetrieveChunkRequest, handler *messages.MessageHandler) {
	// Fast path: serve the local chunk if its on-disk checksum still matches.
	data, checksum, err := s.readVerifiedLocalChunk(msg.ChunkInfo)
	if err != nil {
		log.Printf("[StorageNode] Local chunk verification failed for %s[%d]: %v",
			msg.ChunkInfo.Filename, msg.ChunkInfo.ChunkIndex, err)

		// Slow path: repair from a replica, then return the repaired bytes.
		data, checksum, err = s.repairChunk(msg.ChunkInfo, msg.ReplicaHints)
		if err != nil {
			resp := &messages.Wrapper{
				Msg: &messages.Wrapper_RetrieveChunkResponse{
					RetrieveChunkResponse: &messages.RetrieveChunkResponse{
						Ok:    false,
						Error: err.Error(),
					},
				},
			}
			if sendErr := handler.Send(resp); sendErr != nil {
				log.Printf("[StorageNode] Failed to send error RetrieveChunkResponse: %v", sendErr)
			}
			return
		}
	}

	s.numRequests.Add(1)

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
	log.Printf("[StorageNode] Sent chunk %s[%d] (%d bytes)", msg.ChunkInfo.Filename, msg.ChunkInfo.ChunkIndex, len(data))
}

/**
 * Handle an internal storage-to-storage repair fetch.
 * This path never triggers another repair attempt: it only returns a verified
 * local copy, or reports failure immediately.
 */
func (s *StorageNode) handleRepairChunk(msg *messages.RepairChunkRequest, handler *messages.MessageHandler) {
	data, _, err := s.readVerifiedLocalChunk(msg.ChunkInfo)
	if err != nil {
		resp := &messages.Wrapper{
			Msg: &messages.Wrapper_RepairChunkResponse{
				RepairChunkResponse: &messages.RepairChunkResponse{
					Ok:        false,
					Error:     err.Error(),
					ChunkInfo: msg.ChunkInfo,
				},
			},
		}
		if sendErr := handler.Send(resp); sendErr != nil {
			log.Printf("[StorageNode] Failed to send RepairChunkResponse: %v", sendErr)
		}
		return
	}

	s.numRequests.Add(1)

	resp := &messages.Wrapper{
		Msg: &messages.Wrapper_RepairChunkResponse{
			RepairChunkResponse: &messages.RepairChunkResponse{
				Ok:        true,
				ChunkInfo: msg.ChunkInfo,
				ChunkData: data,
			},
		},
	}
	if err := handler.Send(resp); err != nil {
		log.Printf("[StorageNode] Failed to send RepairChunkResponse: %v", err)
		return
	}

	log.Printf("[StorageNode] Served repair chunk %s[%d]", msg.ChunkInfo.Filename, msg.ChunkInfo.ChunkIndex)
}

/**
 * Delete a chunk and its checksum sidecar.
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

/**
 * Read a chunk and verify it against the checksum sidecar stored on disk.
 * Missing checksum files are treated as corruption so the caller can repair
 * from another replica instead of serving unverified data.
 */
func (s *StorageNode) readVerifiedLocalChunk(chunkInfo *messages.ChunkInfo) ([]byte, []byte, error) {
	chunkPath := utils.ChunkPath(s.storageDir, chunkInfo.Filename, chunkInfo.ChunkIndex)
	checksumPath := utils.ChecksumPath(s.storageDir, chunkInfo.Filename, chunkInfo.ChunkIndex)

	data, err := os.ReadFile(chunkPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read chunk %s[%d]: %w", chunkInfo.Filename, chunkInfo.ChunkIndex, err)
	}

	checksum, err := os.ReadFile(checksumPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read checksum for %s[%d]: %w", chunkInfo.Filename, chunkInfo.ChunkIndex, err)
	}

	if !utils.VerifyChecksum(data, checksum) {
		return nil, nil, fmt.Errorf("checksum mismatch for %s[%d]", chunkInfo.Filename, chunkInfo.ChunkIndex)
	}

	return data, checksum, nil
}

/**
 * Repair a corrupted local chunk.
 * First try the replica hints from the client request, then fall back to the
 * controller for a refreshed holder list if those hints are stale.
 */
func (s *StorageNode) repairChunk(chunkInfo *messages.ChunkInfo, replicaHints []*messages.NodeInfo) ([]byte, []byte, error) {
	tried := make(map[uint32]struct{})
	tried[s.nodeId] = struct{}{}

	data, err := s.tryRepairCandidates(chunkInfo, replicaHints, tried)
	if err == nil {
		return s.persistRepairedChunk(chunkInfo, data)
	}

	nodes, locErr := s.fetchChunkLocations(chunkInfo)
	if locErr != nil {
		return nil, nil, fmt.Errorf("failed to repair chunk %s[%d]: %w", chunkInfo.Filename, chunkInfo.ChunkIndex, locErr)
	}

	data, err = s.tryRepairCandidates(chunkInfo, nodes, tried)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to repair chunk %s[%d]: %w", chunkInfo.Filename, chunkInfo.ChunkIndex, err)
	}

	return s.persistRepairedChunk(chunkInfo, data)
}

/**
 * Try a list of replica candidates until one returns a verified local copy.
 * The `tried` map prevents retry loops across the primary path and fallback
 * controller lookup.
 */
func (s *StorageNode) tryRepairCandidates(chunkInfo *messages.ChunkInfo, nodes []*messages.NodeInfo, tried map[uint32]struct{}) ([]byte, error) {
	var lastErr error

	for _, node := range nodes {
		if node == nil {
			continue
		}
		if _, seen := tried[node.NodeId]; seen {
			continue
		}
		tried[node.NodeId] = struct{}{}

		data, err := s.fetchRepairChunk(node, chunkInfo)
		if err == nil {
			return data, nil
		}

		lastErr = err
		log.Printf("[StorageNode] Repair attempt from %s:%d failed for %s[%d]: %v",
			node.Hostname, node.Port, chunkInfo.Filename, chunkInfo.ChunkIndex, err)
	}

	if lastErr == nil {
		return nil, fmt.Errorf("no replica candidates available")
	}

	return nil, lastErr
}

/**
 * Ask one peer Storage Node for a verified copy of the requested chunk.
 * The peer only serves its local copy; it does not recursively repair.
 */
func (s *StorageNode) fetchRepairChunk(node *messages.NodeInfo, chunkInfo *messages.ChunkInfo) ([]byte, error) {
	addr := fmt.Sprintf("%s:%d", node.Hostname, node.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	handler := messages.NewMessageHandler(conn)
	defer handler.Close()

	req := &messages.Wrapper{
		Msg: &messages.Wrapper_RepairChunkRequest{
			RepairChunkRequest: &messages.RepairChunkRequest{
				ChunkInfo: chunkInfo,
			},
		},
	}
	if err := handler.Send(req); err != nil {
		return nil, fmt.Errorf("failed to send RepairChunkRequest: %w", err)
	}

	wrapper, err := handler.Receive()
	if err != nil {
		return nil, fmt.Errorf("failed to receive RepairChunkResponse: %w", err)
	}
	resp := wrapper.Msg.(*messages.Wrapper_RepairChunkResponse).RepairChunkResponse
	if !resp.Ok {
		return nil, fmt.Errorf("replica rejected repair request: %s", resp.Error)
	}

	return resp.ChunkData, nil
}

/**
 * Overwrite the corrupted local copy with data fetched from a healthy replica,
 * then persist the recomputed checksum sidecar.
 */
func (s *StorageNode) persistRepairedChunk(chunkInfo *messages.ChunkInfo, data []byte) ([]byte, []byte, error) {
	checksum := utils.ComputeChecksum(data)
	chunkPath := utils.ChunkPath(s.storageDir, chunkInfo.Filename, chunkInfo.ChunkIndex)
	checksumPath := utils.ChecksumPath(s.storageDir, chunkInfo.Filename, chunkInfo.ChunkIndex)

	if err := os.WriteFile(chunkPath, data, 0644); err != nil {
		return nil, nil, fmt.Errorf("failed to write repaired chunk %s[%d]: %w", chunkInfo.Filename, chunkInfo.ChunkIndex, err)
	}
	if err := os.WriteFile(checksumPath, checksum, 0644); err != nil {
		return nil, nil, fmt.Errorf("failed to write repaired checksum %s[%d]: %w", chunkInfo.Filename, chunkInfo.ChunkIndex, err)
	}

	log.Printf("[StorageNode] Repaired chunk %s[%d] from replica", chunkInfo.Filename, chunkInfo.ChunkIndex)
	return data, checksum, nil
}

/**
 * Ask the Controller for the current holders of a single chunk.
 * This is only used after the original replica hints have been exhausted.
 */
func (s *StorageNode) fetchChunkLocations(chunkInfo *messages.ChunkInfo) ([]*messages.NodeInfo, error) {
	controllerAddr := net.JoinHostPort(s.controllerHost, fmt.Sprintf("%d", s.controllerPort))
	conn, err := net.Dial("tcp", controllerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to controller: %w", err)
	}
	handler := messages.NewMessageHandler(conn)
	defer handler.Close()

	req := &messages.Wrapper{
		Msg: &messages.Wrapper_ChunkLocationsRequest{
			ChunkLocationsRequest: &messages.ChunkLocationsRequest{
				ChunkInfo: chunkInfo,
			},
		},
	}
	if err := handler.Send(req); err != nil {
		return nil, fmt.Errorf("failed to send ChunkLocationsRequest: %w", err)
	}

	wrapper, err := handler.Receive()
	if err != nil {
		return nil, fmt.Errorf("failed to receive ChunkLocationsResponse: %w", err)
	}
	resp := wrapper.Msg.(*messages.Wrapper_ChunkLocationsResponse).ChunkLocationsResponse
	if !resp.Ok {
		return nil, fmt.Errorf("controller rejected chunk locations request: %s", resp.Error)
	}

	return resp.Nodes, nil
}
