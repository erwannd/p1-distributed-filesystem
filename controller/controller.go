package main

import (
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/erwannd/dfs/messages"
	"github.com/erwannd/dfs/utils"
)

type NodeInfo struct {
	NodeId      uint32
	Hostname    string
	Port        int32
	FreeSpace   uint64
	NumRequests uint64
	LastSeen    time.Time // for heartbeat timeout detection
}

type Controller struct {
	nodes               map[uint32]*NodeInfo                    // maps NodeID -> NodeInfo
	files               map[string]*FileMetadata                // maps filename -> metadata
	mu                  sync.RWMutex                            // for concurrent access
	nextId              uint32                                  // incremental ID
	pendingReplications map[uint32][]*messages.ReplicateRequest // maps dest NodeID -> replication request
}

type FileMetadata struct {
	Filename   string
	FileSize   uint64
	ChunkSize  uint64
	ChunkCount uint32
	Chunks     map[uint32]*ChunkMetadata // maps chunk idx -> metadata
}

type ChunkMetadata struct {
	ChunkIndex uint32
	Nodes      []*NodeInfo // which nodes has this chunk?
}

/**
 * Parse message type & dispatch to appropriate handler.
 */
func handleConnection(controller *Controller, conn net.Conn) {
	handler := messages.NewMessageHandler(conn)
	defer handler.Close()

	for {
		wrapper, err := handler.Receive()
		if err != nil {
			log.Printf("[Controller] Connection lost from %s: %v", handler.RemoteAddr(), err)
			return
		}

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_RegisterRequest:
			handleRegister(controller, msg.RegisterRequest, handler)
		case *messages.Wrapper_Heartbeat:
			handleHeartbeat(controller, msg.Heartbeat, handler)
		case *messages.Wrapper_StoreRequest:
			handleStoreRequest(controller, msg.StoreRequest, handler)
		case *messages.Wrapper_RetrieveRequest:
			handleRetrieveRequest(controller, msg.RetrieveRequest, handler)
		case *messages.Wrapper_DeleteRequest:
			handleDeleteRequest(controller, msg.DeleteRequest, handler)
		case *messages.Wrapper_ListRequest:
			handleListRequest(controller, handler)
		case *messages.Wrapper_ClusterInfoRequest:
			handleClusterInfoRequest(controller, handler)
		default:
			log.Printf("[Controller] Unknown message type from %s", handler.RemoteAddr())
		}
	}
}

/**
 * Handle storage registration request.
 */
func handleRegister(controller *Controller, msg *messages.RegisterRequest, handler *messages.MessageHandler) {

	// Create a new NodeInfo and add it to the Controller
	// Need to acquire lock before adding node
	controller.mu.Lock()

	node := &NodeInfo{
		NodeId:      controller.nextId,
		Hostname:    msg.Hostname,
		Port:        msg.Port,
		FreeSpace:   msg.FreeSpace,
		NumRequests: 0,
		LastSeen:    time.Now(),
	}

	controller.nodes[controller.nextId] = node
	controller.nextId++
	controller.mu.Unlock()

	// Send RegisterResponse
	resp := &messages.Wrapper{
		Msg: &messages.Wrapper_RegisterResponse{
			RegisterResponse: &messages.RegisterResponse{
				Ok:     true,
				NodeId: node.NodeId,
			},
		},
	}
	if err := handler.Send(resp); err != nil {
		log.Printf("[Controller] Failed to send RegisterResponse to %s: %v", handler.RemoteAddr(), err)
		return
	}
	log.Printf("[Controller] Node %d registered from %s:%d", node.NodeId, node.Hostname, node.Port)
}

/**
 * Handle heartbeat from storage node + send response back.
 * Update the Controller's in-memory view of the cluster (number of nodes & number of requests).
 * Piggyback re-replication request in the heartbeat.
 */
func handleHeartbeat(controller *Controller, msg *messages.Heartbeat, handler *messages.MessageHandler) {
	// Update storage node freeSpace + lastSeen
	controller.mu.Lock()
	node, exists := controller.nodes[msg.NodeId]
	if !exists {
		controller.mu.Unlock()
		log.Printf("[Controller] Heartbeat from unknown node %d, ignoring", msg.NodeId)
		return
	}
	node.FreeSpace = msg.FreeSpace
	node.NumRequests = msg.NumRequest
	node.LastSeen = time.Now()

	// Handle new chunks reported via piggyback
	for _, chunkInfo := range msg.NewChunks {
		fileMeta, exists := controller.files[chunkInfo.Filename]
		if !exists {
			continue
		}
		chunkMeta, exists := fileMeta.Chunks[chunkInfo.ChunkIndex]
		if !exists {
			continue
		}
		// Add this node to replica list if not already there
		alreadyTracked := false
		for _, n := range chunkMeta.Nodes {
			if n.Hostname == node.Hostname && n.Port == node.Port {
				alreadyTracked = true
				break
			}
		}
		if !alreadyTracked {
			chunkMeta.Nodes = append(chunkMeta.Nodes, node)
			log.Printf("[Controller] Node %d now holds chunk %s[%d]", node.NodeId, chunkInfo.Filename, chunkInfo.ChunkIndex)
		}
	}

	// Pop pending replication requests for this node
	pending := controller.pendingReplications[msg.NodeId]
	delete(controller.pendingReplications, msg.NodeId)
	nodeId, hostname, port, freeSpace := node.NodeId, node.Hostname, node.Port, node.FreeSpace
	controller.mu.Unlock()

	// Send HeartbeatResponse with pending re-replication request
	resp := &messages.Wrapper{
		Msg: &messages.Wrapper_HeartbeatResponse{
			HeartbeatResponse: &messages.HeartbeatResponse{
				Ack:              true,
				ReplicateRequest: pending,
			},
		},
	}
	if err := handler.Send(resp); err != nil {
		log.Printf("[Controller] Failed to send HeartbeatResponse to %s: %v", handler.RemoteAddr(), err)
		return
	}
	log.Printf("[Controller] Heartbeat from Node %d (%s:%d) - free space: %d MB", nodeId, hostname, port, freeSpace>>20)
}

/**
 * Handle Client's store request.
 */
func handleStoreRequest(controller *Controller, msg *messages.StoreRequest, handler *messages.MessageHandler) {
	// Build destinations first (outside lock — selectNodes has its own RLock)
	destinations := make([]*messages.ChunkMapping, msg.ChunkCount)
	chunkMetas := make(map[uint32]*ChunkMetadata)

	for i := uint32(0); i < msg.ChunkCount; i++ {
		nodes, err := controller.selectNodes(3)
		if err != nil {
			resp := &messages.Wrapper{
				Msg: &messages.Wrapper_StoreResponse{
					StoreResponse: &messages.StoreResponse{
						Ok:    false,
						Error: "not enough storage nodes for replication",
					},
				},
			}
			if err := handler.Send(resp); err != nil {
				log.Printf("[Controller] Failed to send StoreResponse: %v", err)
			}
			return
		}

		nodeInfos := make([]*messages.NodeInfo, len(nodes))
		for j, n := range nodes {
			nodeInfos[j] = &messages.NodeInfo{
				NodeId:   n.NodeId,
				Hostname: n.Hostname,
				Port:     n.Port,
			}
		}
		destinations[i] = &messages.ChunkMapping{
			ChunkInfo: &messages.ChunkInfo{
				Filename:   msg.Filename,
				ChunkIndex: i,
			},
			Nodes: nodeInfos,
		}
		chunkMetas[i] = &ChunkMetadata{
			ChunkIndex: i,
			Nodes:      nodes,
		}
	}

	// Atomic check + store under write lock
	controller.mu.Lock()
	_, exists := controller.files[msg.Filename]
	if !exists {
		controller.files[msg.Filename] = &FileMetadata{
			Filename:   msg.Filename,
			FileSize:   msg.FileSize,
			ChunkSize:  msg.ChunkSize,
			ChunkCount: msg.ChunkCount,
			Chunks:     chunkMetas,
		}
	}
	controller.mu.Unlock()

	// Save snapshot after store
	if err := controller.saveSnapshot(); err != nil {
		log.Printf("[Controller] Failed to save snapshot after store: %v", err)
	}

	if exists {
		resp := &messages.Wrapper{
			Msg: &messages.Wrapper_StoreResponse{
				StoreResponse: &messages.StoreResponse{
					Ok:    false,
					Error: fmt.Sprintf("file %s already exists", msg.Filename),
				},
			},
		}
		if err := handler.Send(resp); err != nil {
			log.Printf("[Controller] Failed to send error StoreResponse: %v", err)
		}
		return
	}

	// Send success response
	resp := &messages.Wrapper{
		Msg: &messages.Wrapper_StoreResponse{
			StoreResponse: &messages.StoreResponse{
				Ok:           true,
				Destinations: destinations,
			},
		},
	}
	if err := handler.Send(resp); err != nil {
		log.Printf("[Controller] Failed to send StoreResponse: %v", err)
	}
	log.Printf("[Controller] Store request for %s: %d chunks assigned", msg.Filename, msg.ChunkCount)
}

/**
 * Handle retrieval request from Client.
 * TODO: Maybe can move the RUnlock to before Send().
 */
func handleRetrieveRequest(controller *Controller, msg *messages.RetrieveRequest, handler *messages.MessageHandler) {
	controller.mu.RLock()
	defer controller.mu.RUnlock()

	// Check if file exists
	fileMeta, exists := controller.files[msg.Filename]
	if !exists {
		resp := &messages.Wrapper{
			Msg: &messages.Wrapper_RetrieveResponse{
				RetrieveResponse: &messages.RetrieveResponse{
					Ok:    false,
					Error: fmt.Sprintf("file %s does not exist", msg.Filename),
				},
			},
		}
		if err := handler.Send(resp); err != nil {
			log.Printf("[Controller] Failed to send error RetrieveResponse: %v", err)
		}
		return
	}

	// Build chunk mapping from in-memory metadata
	locations := make([]*messages.ChunkMapping, fileMeta.ChunkCount)
	for i := uint32(0); i < fileMeta.ChunkCount; i++ {
		chunkMeta := fileMeta.Chunks[i]

		nodeInfos := make([]*messages.NodeInfo, len(chunkMeta.Nodes))
		for j, node := range chunkMeta.Nodes {
			nodeInfos[j] = &messages.NodeInfo{
				NodeId:   node.NodeId,
				Hostname: node.Hostname,
				Port:     node.Port,
			}
		}

		locations[i] = &messages.ChunkMapping{
			ChunkInfo: &messages.ChunkInfo{
				Filename:   msg.Filename,
				ChunkIndex: i,
			},
			Nodes: nodeInfos,
		}
	}

	// Send response
	resp := &messages.Wrapper{
		Msg: &messages.Wrapper_RetrieveResponse{
			RetrieveResponse: &messages.RetrieveResponse{
				Ok:        true,
				Locations: locations,
			},
		},
	}
	if err := handler.Send(resp); err != nil {
		log.Printf("[Controller] Failed to send RetrieveResponse: %v", err)
		return
	}
	log.Printf("[Controller] Retrieve request for %s: %d chunks located", msg.Filename, fileMeta.ChunkCount)
}

/**
 * Handle delete request from Client.
 * Steps:
 * 	1. Verify that the file exists from in-memory data
 *	2. How many chunks are there and which nodes are responsible for each chunk
 *	3. Send a delete chunk request to all Storage nodes (including replicas) for each chunk
 *	4. Send response to client (failure or success)
 */
func handleDeleteRequest(controller *Controller, msg *messages.DeleteRequest, handler *messages.MessageHandler) {
	// 1. Check if file exists
	controller.mu.Lock()
	fileMeta, exists := controller.files[msg.Filename]
	controller.mu.Unlock()

	if !exists {
		resp := &messages.Wrapper{
			Msg: &messages.Wrapper_RetrieveResponse{
				RetrieveResponse: &messages.RetrieveResponse{
					Ok:    false,
					Error: fmt.Sprintf("file %s does not exist", msg.Filename),
				},
			},
		}
		if err := handler.Send(resp); err != nil {
			log.Printf("[Controller] Failed to send error DeleteResponse: %v", err)
		}
		return
	}

	// 2. Delete chunks from all storage nodes
	controller.deleteFile(msg.Filename, fileMeta)

	// 3. Remove metadata only after chunks are deleted
	controller.mu.Lock()
	delete(controller.files, msg.Filename)
	controller.mu.Unlock()

	// 4. Save snapshot after deletion
	if err := controller.saveSnapshot(); err != nil {
		log.Printf("[Controller] Failed to save snapshot after delete: %v", err)
	}

	// 5. Send success response
	resp := &messages.Wrapper{
		Msg: &messages.Wrapper_DeleteResponse{
			DeleteResponse: &messages.DeleteResponse{
				Ok: true,
			},
		},
	}
	if err := handler.Send(resp); err != nil {
		log.Printf("[Controller] Failed to send DeleteResponse: %v", err)
	}
	log.Printf("[Controller] Deleted file %s", msg.Filename)
}

/**
 * Handle list request from Client.
 * Build FileInfo list from in-memory data.
 */
func handleListRequest(controller *Controller, handler *messages.MessageHandler) {
	controller.mu.RLock()
	files := make([]*messages.FileInfo, 0, len(controller.files))
	for _, meta := range controller.files {
		fileInfo := &messages.FileInfo{
			Filename:   meta.Filename,
			FileSize:   meta.FileSize,
			ChunkCount: meta.ChunkCount,
		}
		files = append(files, fileInfo)
	}
	controller.mu.RUnlock()

	resp := &messages.Wrapper{
		Msg: &messages.Wrapper_ListResponse{
			ListResponse: &messages.ListResponse{
				Ok:    true,
				Files: files,
			},
		},
	}
	if err := handler.Send(resp); err != nil {
		log.Printf("[Controller] Failed to send ListResponse: %v", err)
	}
}

/**
 * Handle cluster info request from Client.
 * Build ClusterInfo from in-memory data.
 * Sends cluster info response.
 */
func handleClusterInfoRequest(controller *Controller, handler *messages.MessageHandler) {
	controller.mu.RLock()

	totalFreeSpace := uint64(0)
	totalRequests := uint64(0)

	nodeStats := make([]*messages.NodeStats, 0, len(controller.nodes))
	for _, meta := range controller.nodes {
		nodeInfo := &messages.NodeStats{
			NodeInfo: &messages.NodeInfo{
				NodeId:   meta.NodeId,
				Hostname: meta.Hostname,
				Port:     meta.Port,
			},
			FreeSpace:   meta.FreeSpace,
			NumRequests: meta.NumRequests,
		}
		nodeStats = append(nodeStats, nodeInfo)
		totalFreeSpace += meta.FreeSpace
		totalRequests += meta.NumRequests

	}
	controller.mu.RUnlock()

	resp := &messages.Wrapper{
		Msg: &messages.Wrapper_ClusterInfoResponse{
			ClusterInfoResponse: &messages.ClusterInfoResponse{
				NodeStats:      nodeStats,
				TotalFreeSpace: totalFreeSpace,
				TotalRequests:  totalRequests,
			},
		},
	}
	if err := handler.Send(resp); err != nil {
		log.Printf("[Controller] Failed to send CluserInfoResponse: %v", err)
	}
}

/**
 * Check that Controller has received heartbeat from Storage Node within the time limit of 15 secs.
 */
func (c *Controller) startFailureDetector() {
	for {
		time.Sleep(utils.HeartbeatInterval)

		c.mu.Lock()
		for id, node := range c.nodes {
			if time.Since(node.LastSeen) > utils.HeartbeatTimeout {
				log.Printf("[Controller] Node %d (%s:%d) timed out, marking dead", id, node.Hostname, node.Port)
				delete(c.nodes, id)
				// Need to find all chunks on this failed node -> trigger re-replication
				// Use hostname:port to find affected chunks
				c.handleNodeFailure(node.Hostname, node.Port)
			}
		}
		c.mu.Unlock()
	}
}

/**
 * Find all chunks stored on failed node using hostname and port.
 */
func (c *Controller) handleNodeFailure(hostname string, port int32) {
	type affectedChunk struct {
		filename   string
		chunkIndex uint32
	}

	var affected []affectedChunk

	for filename, fileMeta := range c.files {
		for _, chunkMeta := range fileMeta.Chunks {
			for _, n := range chunkMeta.Nodes {
				if n.Hostname == hostname && n.Port == port {
					affected = append(affected, affectedChunk{
						filename:   filename,
						chunkIndex: chunkMeta.ChunkIndex,
					})
					break
				}
			}
		}
	}

	log.Printf("[Controller] Node %s:%d failed, %d chunks affected",
		hostname, port, len(affected))

	// Queue re-replication for each affected chunk
	for _, chunk := range affected {
		c.queueReplication(chunk.filename, chunk.chunkIndex, hostname, port)
	}
}

/**
 * When a node fails, remove it from the chunk's replica list.
 */
func (c *Controller) queueReplication(filename string, chunkIndex uint32, failedHost string, failedPort int32) {
	fileMeta, exists := c.files[filename]
	if !exists {
		return
	}
	chunkMeta, exists := fileMeta.Chunks[chunkIndex]
	if !exists {
		return
	}

	// Remove failed node from replica list
	remaining := make([]*NodeInfo, 0)
	for _, n := range chunkMeta.Nodes {
		if n.Hostname != failedHost || n.Port != failedPort {
			remaining = append(remaining, n)
		}
	}
	chunkMeta.Nodes = remaining

	log.Printf("[Controller] Chunk %s[%d] now has %d replicas, needs re-replication",
		filename, chunkIndex, len(remaining))

	// Need at least one surviving replica to replicate FROM
	if len(remaining) == 0 {
		log.Printf("[Controller] WARNING: chunk %s[%d] has NO replicas — data lost!", filename, chunkIndex)
		return
	}

	// Pick a source node (first surviving replica)
	srcNode := remaining[0]

	// Pick a destination node (not already holding this chunk)
	destNode := c.selectReplicationTarget(chunkMeta)
	if destNode == nil {
		log.Printf("[Controller] No available node to replicate chunk %s[%d] to", filename, chunkIndex)
		return
	}

	// Queue ReplicateRequest to be sent via next heartbeat to destNode
	c.pendingReplications[destNode.NodeId] = append(
		c.pendingReplications[destNode.NodeId],
		&messages.ReplicateRequest{
			SrcHost: srcNode.Hostname,
			SrcPort: srcNode.Port,
			ChunkInfo: &messages.ChunkInfo{
				Filename:   filename,
				ChunkIndex: chunkIndex,
			},
		},
	)

	// Update metadata — add destNode as new replica
	chunkMeta.Nodes = append(chunkMeta.Nodes, destNode)
}

/**
 * Pick a storage node to re-replicate chunk onto.
 */
func (c *Controller) selectReplicationTarget(chunkMeta *ChunkMetadata) *NodeInfo {
	// Build set of nodes already holding this chunk
	alreadyHas := make(map[string]bool)
	for _, n := range chunkMeta.Nodes {
		key := fmt.Sprintf("%s:%d", n.Hostname, n.Port)
		alreadyHas[key] = true
	}

	// Pick a node not already holding this chunk
	for _, node := range c.nodes {
		key := fmt.Sprintf("%s:%d", node.Hostname, node.Port)
		if !alreadyHas[key] {
			return node
		}
	}
	return nil // no available node
}

/**
 * Delete chunks from all nodes, including replicas.
 * Two-level parallelism
 */
func (c *Controller) deleteFile(filename string, fileMeta *FileMetadata) {
	var wg sync.WaitGroup

	// Lv 1: Iterate chunks
	for i := uint32(0); i < fileMeta.ChunkCount; i++ {
		chunkMeta := fileMeta.Chunks[i]

		// Lv 2: Delete from all replicas in parallel
		for _, node := range chunkMeta.Nodes {
			wg.Add(1)
			go func(n *NodeInfo, chunkIndex uint32) {
				defer wg.Done()
				if err := sendDeleteChunk(n, filename, chunkIndex); err != nil {
					log.Printf("[Controller] Failed to delete chunk %d from Node %d: %v",
						chunkIndex, n.NodeId, err)
				}
			}(node, i)
		}
	}
	wg.Wait()
}

/**
 * Send delete chunk request to storage node.
 */
func sendDeleteChunk(node *NodeInfo, filename string, chunkIndex uint32) error {
	addr := fmt.Sprintf("%s:%d", node.Hostname, node.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	handler := messages.NewMessageHandler(conn)
	defer handler.Close()

	req := &messages.Wrapper{
		Msg: &messages.Wrapper_DeleteChunkRequest{
			DeleteChunkRequest: &messages.DeleteChunkRequest{
				ChunkInfo: &messages.ChunkInfo{
					Filename:   filename,
					ChunkIndex: chunkIndex,
				},
			},
		},
	}
	if err := handler.Send(req); err != nil {
		return fmt.Errorf("failed to send DeleteChunkRequest: %w", err)
	}

	wrapper, err := handler.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive DeleteChunkResponse: %w", err)
	}
	resp := wrapper.Msg.(*messages.Wrapper_DeleteChunkResponse).DeleteChunkResponse
	if !resp.Ok {
		return fmt.Errorf("node rejected delete: %s", resp.Error)
	}
	return nil
}

/**
 * Replica placement algorithm.
 * Simple approach: pick nodes by free space (descending).
 */
func (c *Controller) selectNodes(count int) ([]*NodeInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.nodes) < count {
		return nil, fmt.Errorf("not enough nodes: need %d, have %d", count, len(c.nodes))
	}

	// Sort nodes by free space descending, pick top N
	nodes := make([]*NodeInfo, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].FreeSpace > nodes[j].FreeSpace
	})

	selected := nodes[:count]
	return selected, nil
}
