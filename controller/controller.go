package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/erwannd/p1-distributed-filesystem/messages"
	"github.com/erwannd/p1-distributed-filesystem/utils"
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
	nodes                map[uint32]*NodeInfo                       // maps NodeID -> NodeInfo
	files                map[string]*FileMetadata                   // completed files only
	pendingFiles         map[string]*FileMetadata                   // files still being confirmed
	mu                   sync.RWMutex                               // for concurrent access
	nextId               uint32                                     // incremental Node ID
	pendingReplications  map[uint32][]*messages.ReplicateRequest    // maps dest NodeID -> replication request
	inFlightReplications map[string]map[uint32]uint32               // filename -> chunk index -> dest NodeID for unconfirmed replication
	pendingStores        map[string]map[uint32]map[string]*NodeInfo // filename -> chunk index -> host:port -> intended holder not yet confirmed
	nextPlacementIndex   int                                        // round-robin cursor for initial chunk placement
	snapshotPath         string                                     // where to save .snapshot file
}

type FileMetadata struct {
	Filename   string
	FileSize   uint64
	ChunkSize  uint64 // chunk size in bytes
	ChunkCount uint32
	Chunks     map[uint32]*ChunkMetadata // maps chunk idx -> metadata
}

type ChunkMetadata struct {
	ChunkIndex uint32
	Nodes      []*NodeInfo // which nodes has this chunk?
}

// filePlanningSnapshot is a controller-local immutable view of a completed file
// that the split planner can use without holding controller.mu during network
// reads to storage nodes.
type filePlanningSnapshot struct {
	Filename   string
	FileSize   uint64
	ChunkSize  uint64
	ChunkCount uint32
	ChunkNodes map[uint32][]*messages.NodeInfo
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
		case *messages.Wrapper_ChunkLocationsRequest:
			handleChunkLocationsRequest(controller, msg.ChunkLocationsRequest, handler)
		case *messages.Wrapper_StoreRequest:
			handleStoreRequest(controller, msg.StoreRequest, handler)
		case *messages.Wrapper_RetrieveRequest:
			handleRetrieveRequest(controller, msg.RetrieveRequest, handler)
		case *messages.Wrapper_StatFileRequest:
			handleStatFileRequest(controller, msg.StatFileRequest, handler)
		case *messages.Wrapper_GetInputSplitsRequest:
			handleGetInputSplitsRequest(controller, msg.GetInputSplitsRequest, handler)
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
 * A chunk only becomes a confirmed replica when the destination node later
 * reports it in Heartbeat.new_chunks.
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
		fileMeta, pending, exists := controller.lookupFileMetadata(chunkInfo.Filename)
		if !exists {
			continue
		}
		chunkMeta, exists := fileMeta.Chunks[chunkInfo.ChunkIndex]
		if !exists {
			continue
		}

		confirmedPendingStore := pending && controller.isPendingStoreDestination(chunkInfo.Filename, chunkInfo.ChunkIndex, node.Hostname, node.Port)
		confirmedReplication := controller.isInFlightReplicationDestination(chunkInfo.Filename, chunkInfo.ChunkIndex, node.NodeId)
		if !confirmedPendingStore && !confirmedReplication {
			// Ignore unexpected chunk reports so pending files do not accidentally
			// become visible without an original assignment or re-replication.
			continue
		}

		if !controller.chunkHasNode(chunkMeta, node) {
			chunkMeta.Nodes = append(chunkMeta.Nodes, node)
			log.Printf("[Controller] Node %d now holds chunk %s[%d]", node.NodeId, chunkInfo.Filename, chunkInfo.ChunkIndex)
		}

		if confirmedPendingStore {
			controller.clearPendingStoreDestination(chunkInfo.Filename, chunkInfo.ChunkIndex, node.Hostname, node.Port)
			log.Printf("[Controller] Initial store for %s[%d] confirmed by %s:%d",
				chunkInfo.Filename, chunkInfo.ChunkIndex, node.Hostname, node.Port)
		}

		if confirmedReplication {
			controller.clearInFlightReplication(chunkInfo.Filename, chunkInfo.ChunkIndex)
			log.Printf("[Controller] Replication for %s[%d] confirmed by Node %d",
				chunkInfo.Filename, chunkInfo.ChunkIndex, node.NodeId)
		}

		if pending {
			if controller.isFileComplete(fileMeta) {
				controller.promotePendingFile(chunkInfo.Filename)
				log.Printf("[Controller] File %s fully confirmed and now available", chunkInfo.Filename)
			} else {
				controller.maybeQueueReplication(chunkInfo.Filename, chunkInfo.ChunkIndex)
			}
		} else {
			controller.maybeQueueReplication(chunkInfo.Filename, chunkInfo.ChunkIndex)
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
	// Build destinations first; selectNodes manages its own locking because it
	// advances the round-robin cursor as each chunk is assigned.
	destinations := make([]*messages.ChunkMapping, msg.ChunkCount)
	chunkMetas := make(map[uint32]*ChunkMetadata)
	pendingStores := make(map[uint32]map[string]*NodeInfo)

	for i := uint32(0); i < msg.ChunkCount; i++ {
		chunkSize := chunkSizeForIndex(msg.FileSize, msg.ChunkSize, msg.ChunkCount, i)
		nodes, err := controller.selectNodes(int(utils.ReplicationFactor), chunkSize)
		if err != nil {
			resp := &messages.Wrapper{
				Msg: &messages.Wrapper_StoreResponse{
					StoreResponse: &messages.StoreResponse{
						Ok:    false,
						Error: fmt.Sprintf("not enough storage nodes with %d bytes free for replication", chunkSize),
					},
				},
			}
			if err := handler.Send(resp); err != nil {
				log.Printf("[Controller] Failed to send StoreResponse: %v", err)
			}
			return
		}

		nodeInfos := make([]*messages.NodeInfo, len(nodes))
		pendingNodes := make(map[string]*NodeInfo)
		for j, n := range nodes {
			nodeInfos[j] = &messages.NodeInfo{
				NodeId:   n.NodeId,
				Hostname: n.Hostname,
				Port:     n.Port,
			}
			pendingNodes[nodeAddressKey(n.Hostname, n.Port)] = n
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
			Nodes:      []*NodeInfo{},
		}
		pendingStores[i] = pendingNodes
	}

	// Atomic check + store under write lock.
	// The file remains pending until the assigned nodes report successful local
	// persistence via Heartbeat.new_chunks.
	controller.mu.Lock()
	_, exists := controller.files[msg.Filename]
	_, pendingExists := controller.pendingFiles[msg.Filename]
	if !exists && !pendingExists {
		controller.pendingFiles[msg.Filename] = &FileMetadata{
			Filename:   msg.Filename,
			FileSize:   msg.FileSize,
			ChunkSize:  msg.ChunkSize,
			ChunkCount: msg.ChunkCount,
			Chunks:     chunkMetas,
		}
		controller.pendingStores[msg.Filename] = pendingStores
	}
	controller.mu.Unlock()

	if exists || pendingExists {
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
		if _, pending := controller.pendingFiles[msg.Filename]; pending {
			resp := &messages.Wrapper{
				Msg: &messages.Wrapper_RetrieveResponse{
					RetrieveResponse: &messages.RetrieveResponse{
						Ok:    false,
						Error: fmt.Sprintf("file %s is still being stored", msg.Filename),
					},
				},
			}
			if err := handler.Send(resp); err != nil {
				log.Printf("[Controller] Failed to send pending RetrieveResponse: %v", err)
			}
			return
		}
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
 * Return metadata for one completed file without exposing the full retrieve
 * path. Project 2 uses this to reason about chunk sizing and split planning.
 */
func handleStatFileRequest(controller *Controller, msg *messages.StatFileRequest, handler *messages.MessageHandler) {
	controller.mu.RLock()
	defer controller.mu.RUnlock()

	fileMeta, exists := controller.files[msg.Filename]
	if !exists {
		if _, pending := controller.pendingFiles[msg.Filename]; pending {
			sendStatFileResponse(handler, &messages.StatFileResponse{
				Ok:    false,
				Error: fmt.Sprintf("file %s is still being stored", msg.Filename),
			})
			return
		}

		sendStatFileResponse(handler, &messages.StatFileResponse{
			Ok:    false,
			Error: fmt.Sprintf("file %s does not exist", msg.Filename),
		})
		return
	}

	sendStatFileResponse(handler, &messages.StatFileResponse{
		Ok:         true,
		Filename:   fileMeta.Filename,
		FileSize:   fileMeta.FileSize,
		ChunkSize:  fileMeta.ChunkSize,
		ChunkCount: fileMeta.ChunkCount,
	})
}

/**
 * Build logical read splits for MapReduce without changing the DFS' physical
 * chunk layout. Binary mode mirrors Project 1's chunk boundaries. Text mode
 * expands split ends to the next newline so each mapper receives whole lines.
 */
func handleGetInputSplitsRequest(controller *Controller, msg *messages.GetInputSplitsRequest, handler *messages.MessageHandler) {
	snapshot, pending, exists := controller.snapshotFileForPlanning(msg.Filename)
	if !exists {
		errMsg := fmt.Sprintf("file %s does not exist", msg.Filename)
		if pending {
			errMsg = fmt.Sprintf("file %s is still being stored", msg.Filename)
		}
		sendInputSplitsResponse(handler, &messages.GetInputSplitsResponse{
			Ok:    false,
			Error: errMsg,
		})
		return
	}

	desiredSplitSize := msg.DesiredSplitSizeBytes
	if desiredSplitSize == 0 {
		desiredSplitSize = snapshot.ChunkSize
	}

	var (
		splits []*messages.InputSplit
		err    error
	)

	switch msg.SplitMode {
	case messages.SplitMode_BINARY_CHUNKS:
		splits = buildBinaryChunkSplits(snapshot)
	case messages.SplitMode_TEXT_LINES:
		splits, err = controller.buildTextLineSplits(snapshot, desiredSplitSize)
	case messages.SplitMode_SPLIT_MODE_UNSPECIFIED:
		err = fmt.Errorf("split mode must be specified")
	default:
		err = fmt.Errorf("unsupported split mode %v", msg.SplitMode)
	}

	if err != nil {
		sendInputSplitsResponse(handler, &messages.GetInputSplitsResponse{
			Ok:    false,
			Error: err.Error(),
		})
		return
	}

	sendInputSplitsResponse(handler, &messages.GetInputSplitsResponse{
		Ok:     true,
		Splits: splits,
	})
}

/**
 * Handle chunk-location lookup from a Storage Node.
 * This is the fallback path when a node cannot repair a corrupted chunk
 * from the replica hints that originally came from the client.
 */
func handleChunkLocationsRequest(controller *Controller, msg *messages.ChunkLocationsRequest, handler *messages.MessageHandler) {
	controller.mu.RLock()
	defer controller.mu.RUnlock()

	fileMeta, _, exists := controller.lookupFileMetadata(msg.ChunkInfo.Filename)
	if !exists {
		handler.Send(&messages.Wrapper{
			Msg: &messages.Wrapper_ChunkLocationsResponse{
				ChunkLocationsResponse: &messages.ChunkLocationsResponse{
					Ok:        false,
					Error:     fmt.Sprintf("file %s does not exist", msg.ChunkInfo.Filename),
					ChunkInfo: msg.ChunkInfo,
				},
			},
		})
		return
	}

	chunkMeta, exists := fileMeta.Chunks[msg.ChunkInfo.ChunkIndex]
	if !exists {
		handler.Send(&messages.Wrapper{
			Msg: &messages.Wrapper_ChunkLocationsResponse{
				ChunkLocationsResponse: &messages.ChunkLocationsResponse{
					Ok:        false,
					Error:     fmt.Sprintf("chunk %s[%d] does not exist", msg.ChunkInfo.Filename, msg.ChunkInfo.ChunkIndex),
					ChunkInfo: msg.ChunkInfo,
				},
			},
		})
		return
	}

	nodes := make([]*messages.NodeInfo, 0, len(chunkMeta.Nodes))
	for _, node := range chunkMeta.Nodes {
		nodes = append(nodes, &messages.NodeInfo{
			NodeId:   node.NodeId,
			Hostname: node.Hostname,
			Port:     node.Port,
		})
	}

	if err := handler.Send(&messages.Wrapper{
		Msg: &messages.Wrapper_ChunkLocationsResponse{
			ChunkLocationsResponse: &messages.ChunkLocationsResponse{
				Ok:        true,
				ChunkInfo: msg.ChunkInfo,
				Nodes:     nodes,
			},
		},
	}); err != nil {
		log.Printf("[Controller] Failed to send ChunkLocationsResponse: %v", err)
		return
	}

	log.Printf("[Controller] Chunk location request for %s[%d]: %d node(s)",
		msg.ChunkInfo.Filename, msg.ChunkInfo.ChunkIndex, len(nodes))
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
	// 1. Check if file exists and gather every node that might hold a local copy.
	controller.mu.Lock()
	fileMeta, pending, exists := controller.lookupFileMetadata(msg.Filename)
	if !exists {
		controller.mu.Unlock()
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

	deleteTasks := controller.buildDeleteTasksLocked(msg.Filename, fileMeta, pending)
	delete(controller.files, msg.Filename)
	delete(controller.pendingFiles, msg.Filename)
	delete(controller.pendingStores, msg.Filename)
	delete(controller.inFlightReplications, msg.Filename)
	controller.mu.Unlock()

	// 2. Delete chunks from all known storage nodes.
	controller.deleteTasks(msg.Filename, deleteTasks)

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
				c.requeueInflightReplicationsForFailedNode(id)
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
		isPending  bool
	}

	var affected []affectedChunk

	for filename, fileMeta := range c.files {
		for _, chunkMeta := range fileMeta.Chunks {
			for _, n := range chunkMeta.Nodes {
				if n.Hostname == hostname && n.Port == port {
					affected = append(affected, affectedChunk{
						filename:   filename,
						chunkIndex: chunkMeta.ChunkIndex,
						isPending:  false,
					})
					break
				}
			}
		}
	}

	for filename, fileMeta := range c.pendingFiles {
		for _, chunkMeta := range fileMeta.Chunks {
			if c.chunkHasHostPort(chunkMeta, hostname, port) || c.pendingStoreContainsHostPort(filename, chunkMeta.ChunkIndex, hostname, port) {
				affected = append(affected, affectedChunk{
					filename:   filename,
					chunkIndex: chunkMeta.ChunkIndex,
					isPending:  true,
				})
			}
		}
	}

	log.Printf("[Controller] Node %s:%d failed, %d chunks affected",
		hostname, port, len(affected))

	// Queue re-replication for each affected chunk. Pending files use the same
	// healing path, but only if they still have at least one confirmed replica.
	for _, chunk := range affected {
		c.removeFailedNodeFromChunkState(chunk.filename, chunk.chunkIndex, hostname, port)
		c.maybeQueueReplication(chunk.filename, chunk.chunkIndex)
	}
}

/**
 * Queue a re-replication request if a chunk has at least one confirmed copy
 * but not enough total confirmed/planned replicas to reach the target.
 */
func (c *Controller) maybeQueueReplication(filename string, chunkIndex uint32) {
	fileMeta, pending, exists := c.lookupFileMetadata(filename)
	if !exists {
		return
	}

	chunkMeta, exists := fileMeta.Chunks[chunkIndex]
	if !exists {
		return
	}

	confirmedCount := len(chunkMeta.Nodes)
	if confirmedCount == 0 {
		log.Printf("[Controller] Chunk %s[%d] has no confirmed replicas; cannot heal automatically",
			filename, chunkIndex)
		return
	}

	// Only one re-replication attempt should be in flight for a chunk at a time.
	if c.hasInFlightReplication(filename, chunkIndex) {
		return
	}

	plannedCount := confirmedCount + c.pendingStoreCount(filename, chunkIndex)
	if plannedCount >= int(utils.ReplicationFactor) {
		return
	}

	// Pick a source node (first surviving confirmed replica)
	srcNode := chunkMeta.Nodes[0]

	// Pick a destination node that is not already confirmed or pending.
	destNode := c.selectReplicationTarget(filename, chunkMeta, pending)
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

	c.markInFlightReplication(filename, chunkIndex, destNode.NodeId)
	log.Printf("[Controller] Queued replication for %s[%d]: src=%s:%d dest=Node %d (%s:%d)",
		filename, chunkIndex, srcNode.Hostname, srcNode.Port, destNode.NodeId, destNode.Hostname, destNode.Port)
}

/**
 * Returns true when a chunk already has an assigned destination node waiting
 * to confirm re-replication.
 */
func (c *Controller) hasInFlightReplication(filename string, chunkIndex uint32) bool {
	chunks, exists := c.inFlightReplications[filename]
	if !exists {
		return false
	}
	_, exists = chunks[chunkIndex]
	return exists
}

// markInFlightReplication records that a destination node has been assigned but
// has not yet acknowledged it via Heartbeat.new_chunks.
func (c *Controller) markInFlightReplication(filename string, chunkIndex uint32, destNodeID uint32) {
	if _, exists := c.inFlightReplications[filename]; !exists {
		c.inFlightReplications[filename] = make(map[uint32]uint32)
	}
	c.inFlightReplications[filename][chunkIndex] = destNodeID
}

// isInFlightReplicationDestination checks whether the reporting node is the
// expected destination for the chunk's unconfirmed replication.
func (c *Controller) isInFlightReplicationDestination(filename string, chunkIndex uint32, nodeID uint32) bool {
	chunks, exists := c.inFlightReplications[filename]
	if !exists {
		return false
	}
	destNodeID, exists := chunks[chunkIndex]
	return exists && destNodeID == nodeID
}

// clearInFlightReplication removes a chunk from the set of unconfirmed
// replications after the destination node reports success.
func (c *Controller) clearInFlightReplication(filename string, chunkIndex uint32) {
	chunks, exists := c.inFlightReplications[filename]
	if !exists {
		return
	}
	delete(chunks, chunkIndex)
	if len(chunks) == 0 {
		delete(c.inFlightReplications, filename)
	}
}

/**
 * If a destination node dies before confirming re-replication, clear those
 * in-flight assignments so they can be queued again.
 */
func (c *Controller) requeueInflightReplicationsForFailedNode(nodeID uint32) {
	type affectedChunk struct {
		filename   string
		chunkIndex uint32
	}

	var affected []affectedChunk

	for filename, chunks := range c.inFlightReplications {
		for chunkIndex, destNodeID := range chunks {
			if destNodeID != nodeID {
				continue
			}
			affected = append(affected, affectedChunk{
				filename:   filename,
				chunkIndex: chunkIndex,
			})
			delete(chunks, chunkIndex)
		}
		if len(chunks) == 0 {
			delete(c.inFlightReplications, filename)
		}
	}

	for _, chunk := range affected {
		log.Printf("[Controller] Unconfirmed replication for %s[%d] lost because destination Node %d failed; re-queueing",
			chunk.filename, chunk.chunkIndex, nodeID)
		c.maybeQueueReplication(chunk.filename, chunk.chunkIndex)
	}
}

/**
 * Pick a storage node to re-replicate chunk onto.
 */
func (c *Controller) selectReplicationTarget(filename string, chunkMeta *ChunkMetadata, pending bool) *NodeInfo {
	// Build set of nodes already holding this chunk
	alreadyHas := make(map[string]bool)
	for _, n := range chunkMeta.Nodes {
		key := fmt.Sprintf("%s:%d", n.Hostname, n.Port)
		alreadyHas[key] = true
	}
	if pending {
		pendingNodes := c.pendingStores[filename][chunkMeta.ChunkIndex]
		for _, n := range pendingNodes {
			key := fmt.Sprintf("%s:%d", n.Hostname, n.Port)
			alreadyHas[key] = true
		}
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
 * Delete chunks from all known nodes that may hold them.
 */
func (c *Controller) deleteTasks(filename string, tasks []deleteTask) {
	var wg sync.WaitGroup

	for _, task := range tasks {
		wg.Add(1)
		go func(t deleteTask) {
			defer wg.Done()
			if err := sendDeleteChunk(t.node, filename, t.chunkIndex); err != nil {
				log.Printf("[Controller] Failed to delete chunk %d from Node %d: %v",
					t.chunkIndex, t.node.NodeId, err)
			}
		}(task)
	}
	wg.Wait()
}

type deleteTask struct {
	node       *NodeInfo
	chunkIndex uint32
}

func (c *Controller) lookupFileMetadata(filename string) (*FileMetadata, bool, bool) {
	if meta, exists := c.files[filename]; exists {
		return meta, false, true
	}
	if meta, exists := c.pendingFiles[filename]; exists {
		return meta, true, true
	}
	return nil, false, false
}

func (c *Controller) chunkHasNode(chunkMeta *ChunkMetadata, node *NodeInfo) bool {
	return c.chunkHasHostPort(chunkMeta, node.Hostname, node.Port)
}

func (c *Controller) chunkHasHostPort(chunkMeta *ChunkMetadata, hostname string, port int32) bool {
	for _, n := range chunkMeta.Nodes {
		if n.Hostname == hostname && n.Port == port {
			return true
		}
	}
	return false
}

/**
 * Checks if a heartbeat from Storage is confirming an initial store assignment (that Controller is waiting on)
 * For the given file chunk, is this node the intended initial storage destination.
 */
func (c *Controller) isPendingStoreDestination(filename string, chunkIndex uint32, hostname string, port int32) bool {
	chunks, exists := c.pendingStores[filename]
	if !exists {
		return false
	}
	nodes, exists := chunks[chunkIndex]
	if !exists {
		return false
	}
	_, exists = nodes[nodeAddressKey(hostname, port)]
	return exists
}

func (c *Controller) clearPendingStoreDestination(filename string, chunkIndex uint32, hostname string, port int32) {
	chunks, exists := c.pendingStores[filename]
	if !exists {
		return
	}
	nodes, exists := chunks[chunkIndex]
	if !exists {
		return
	}
	delete(nodes, nodeAddressKey(hostname, port))
	if len(nodes) == 0 {
		delete(chunks, chunkIndex)
	}
	if len(chunks) == 0 {
		delete(c.pendingStores, filename)
	}
}

func (c *Controller) pendingStoreCount(filename string, chunkIndex uint32) int {
	chunks, exists := c.pendingStores[filename]
	if !exists {
		return 0
	}
	return len(chunks[chunkIndex])
}

func (c *Controller) pendingStoreContainsHostPort(filename string, chunkIndex uint32, hostname string, port int32) bool {
	chunks, exists := c.pendingStores[filename]
	if !exists {
		return false
	}
	nodes, exists := chunks[chunkIndex]
	if !exists {
		return false
	}
	for _, node := range nodes {
		if node.Hostname == hostname && node.Port == port {
			return true
		}
	}
	return false
}

func (c *Controller) removeFailedNodeFromChunkState(filename string, chunkIndex uint32, hostname string, port int32) {
	fileMeta, pending, exists := c.lookupFileMetadata(filename)
	if !exists {
		return
	}
	chunkMeta, exists := fileMeta.Chunks[chunkIndex]
	if !exists {
		return
	}

	remaining := make([]*NodeInfo, 0, len(chunkMeta.Nodes))
	for _, node := range chunkMeta.Nodes {
		if node.Hostname == hostname && node.Port == port {
			continue
		}
		remaining = append(remaining, node)
	}
	chunkMeta.Nodes = remaining

	if !pending {
		return
	}

	chunks, exists := c.pendingStores[filename]
	if !exists {
		return
	}
	nodes, exists := chunks[chunkIndex]
	if !exists {
		return
	}
	for key, node := range nodes {
		if node.Hostname == hostname && node.Port == port {
			delete(nodes, key)
		}
	}
	if len(nodes) == 0 {
		delete(chunks, chunkIndex)
	}
	if len(chunks) == 0 {
		delete(c.pendingStores, filename)
	}
}

func (c *Controller) isFileComplete(meta *FileMetadata) bool {
	for chunkIndex := uint32(0); chunkIndex < meta.ChunkCount; chunkIndex++ {
		chunkMeta, exists := meta.Chunks[chunkIndex]
		if !exists || len(chunkMeta.Nodes) < int(utils.ReplicationFactor) {
			return false
		}
	}
	return true
}

// promotePendingFile makes an in-progress file visible for normal retrieve/list
// operations once every chunk has enough confirmed replicas.
func (c *Controller) promotePendingFile(filename string) {
	fileMeta, exists := c.pendingFiles[filename]
	if !exists {
		return
	}
	c.files[filename] = fileMeta
	delete(c.pendingFiles, filename)
	delete(c.pendingStores, filename)
}

func (c *Controller) buildDeleteTasksLocked(filename string, fileMeta *FileMetadata, pending bool) []deleteTask {
	tasks := make([]deleteTask, 0)

	for chunkIndex := uint32(0); chunkIndex < fileMeta.ChunkCount; chunkIndex++ {
		chunkMeta := fileMeta.Chunks[chunkIndex]
		targets := make(map[string]*NodeInfo)

		for _, node := range chunkMeta.Nodes {
			key := fmt.Sprintf("%s:%d", node.Hostname, node.Port)
			targets[key] = node
		}

		if pending {
			if chunks, exists := c.pendingStores[filename]; exists {
				if nodes, exists := chunks[chunkIndex]; exists {
					for _, node := range nodes {
						key := fmt.Sprintf("%s:%d", node.Hostname, node.Port)
						targets[key] = node
					}
				}
			}
		}

		if inFlight, exists := c.inFlightReplications[filename]; exists {
			if destNodeID, exists := inFlight[chunkIndex]; exists {
				if node, exists := c.nodes[destNodeID]; exists {
					key := fmt.Sprintf("%s:%d", node.Hostname, node.Port)
					targets[key] = node
				}
			}
		}

		for _, node := range targets {
			tasks = append(tasks, deleteTask{
				node:       node,
				chunkIndex: chunkIndex,
			})
		}
	}

	return tasks
}

func nodeAddressKey(hostname string, port int32) string {
	return fmt.Sprintf("%s:%d", hostname, port)
}

func sendStatFileResponse(handler *messages.MessageHandler, resp *messages.StatFileResponse) {
	if err := handler.Send(&messages.Wrapper{
		Msg: &messages.Wrapper_StatFileResponse{
			StatFileResponse: resp,
		},
	}); err != nil {
		log.Printf("[Controller] Failed to send StatFileResponse: %v", err)
	}
}

func sendInputSplitsResponse(handler *messages.MessageHandler, resp *messages.GetInputSplitsResponse) {
	if err := handler.Send(&messages.Wrapper{
		Msg: &messages.Wrapper_GetInputSplitsResponse{
			GetInputSplitsResponse: resp,
		},
	}); err != nil {
		log.Printf("[Controller] Failed to send GetInputSplitsResponse: %v", err)
	}
}

func (c *Controller) snapshotFileForPlanning(filename string) (*filePlanningSnapshot, bool, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if _, pending := c.pendingFiles[filename]; pending {
		return nil, true, false
	}

	fileMeta, exists := c.files[filename]
	if !exists {
		return nil, false, false
	}

	snapshot := &filePlanningSnapshot{
		Filename:   fileMeta.Filename,
		FileSize:   fileMeta.FileSize,
		ChunkSize:  fileMeta.ChunkSize,
		ChunkCount: fileMeta.ChunkCount,
		ChunkNodes: make(map[uint32][]*messages.NodeInfo, len(fileMeta.Chunks)),
	}

	for chunkIndex, chunkMeta := range fileMeta.Chunks {
		nodes := make([]*messages.NodeInfo, 0, len(chunkMeta.Nodes))
		for _, node := range chunkMeta.Nodes {
			nodes = append(nodes, &messages.NodeInfo{
				NodeId:   node.NodeId,
				Hostname: node.Hostname,
				Port:     node.Port,
			})
		}
		snapshot.ChunkNodes[chunkIndex] = nodes
	}

	return snapshot, false, true
}

func buildBinaryChunkSplits(snapshot *filePlanningSnapshot) []*messages.InputSplit {
	splits := make([]*messages.InputSplit, 0, snapshot.ChunkCount)

	for chunkIndex := uint32(0); chunkIndex < snapshot.ChunkCount; chunkIndex++ {
		start := uint64(chunkIndex) * snapshot.ChunkSize
		end := start + chunkSizeForIndex(snapshot.FileSize, snapshot.ChunkSize, snapshot.ChunkCount, chunkIndex)

		splits = append(splits, &messages.InputSplit{
			SplitIndex:         chunkIndex,
			Filename:           snapshot.Filename,
			StartOffset:        start,
			EndOffsetExclusive: end,
			StartLineNumber:    0,
			PreferredNodes:     cloneMessageNodes(snapshot.ChunkNodes[chunkIndex]),
		})
	}

	return splits
}

// buildTextLineSplits computes exact line-aware logical splits on demand. The
// controller keeps storage chunking unchanged and uses verified range reads to
// scan the file sequentially, so start_line_number values stay exact without a
// persistent secondary index.
func (c *Controller) buildTextLineSplits(snapshot *filePlanningSnapshot, desiredSplitSize uint64) ([]*messages.InputSplit, error) {
	if desiredSplitSize == 0 {
		return nil, fmt.Errorf("desired split size must be greater than zero")
	}
	if snapshot.FileSize == 0 {
		return []*messages.InputSplit{}, nil
	}

	splits := make([]*messages.InputSplit, 0)
	startOffset := uint64(0)
	startLineNumber := uint64(0)
	splitIndex := uint32(0)

	for startOffset < snapshot.FileSize {
		nominalEnd := minUint64(startOffset+desiredSplitSize, snapshot.FileSize)
		endOffset := nominalEnd
		if nominalEnd < snapshot.FileSize {
			var err error
			endOffset, err = c.advanceToNextLineBoundary(snapshot, nominalEnd)
			if err != nil {
				return nil, err
			}
		}

		splitBytes, err := c.readFileRange(snapshot, startOffset, endOffset-startOffset)
		if err != nil {
			return nil, err
		}

		preferredNodes := cloneMessageNodes(snapshot.chunkNodesForOffset(startOffset))
		splits = append(splits, &messages.InputSplit{
			SplitIndex:         splitIndex,
			Filename:           snapshot.Filename,
			StartOffset:        startOffset,
			EndOffsetExclusive: endOffset,
			StartLineNumber:    startLineNumber,
			PreferredNodes:     preferredNodes,
		})

		startLineNumber += uint64(bytes.Count(splitBytes, []byte{'\n'}))
		startOffset = endOffset
		splitIndex++
	}

	return splits, nil
}

// advanceToNextLineBoundary scans forward from an approximate split boundary
// until it finds '\n' or EOF. This lets text-mode splits honor full records
// while still using the DFS' existing fixed-size chunk storage underneath.
func (c *Controller) advanceToNextLineBoundary(snapshot *filePlanningSnapshot, offset uint64) (uint64, error) {
	if offset >= snapshot.FileSize {
		return snapshot.FileSize, nil
	}

	const scanWindow uint64 = 64 * 1024
	current := offset
	for current < snapshot.FileSize {
		windowSize := minUint64(scanWindow, snapshot.FileSize-current)
		data, err := c.readFileRange(snapshot, current, windowSize)
		if err != nil {
			return 0, err
		}
		if idx := bytes.IndexByte(data, '\n'); idx >= 0 {
			return current + uint64(idx) + 1, nil
		}
		if len(data) == 0 {
			break
		}
		current += uint64(len(data))
	}

	return snapshot.FileSize, nil
}

// readFileRange composes verified per-chunk range reads into a logical file
// range. The controller uses this internally for split planning; Project 2 can
// later call the public ReadChunkRange API directly from workers.
func (c *Controller) readFileRange(snapshot *filePlanningSnapshot, startOffset uint64, length uint64) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}
	if startOffset >= snapshot.FileSize {
		return nil, fmt.Errorf("read range starts beyond EOF")
	}

	endOffset := minUint64(startOffset+length, snapshot.FileSize)
	data := make([]byte, 0, endOffset-startOffset)

	for current := startOffset; current < endOffset; {
		chunkIndex := uint32(current / snapshot.ChunkSize)
		chunkOffset := current % snapshot.ChunkSize
		chunkEnd := minUint64(uint64(chunkIndex+1)*snapshot.ChunkSize, snapshot.FileSize)
		readLength := minUint64(endOffset-current, chunkEnd-current)

		chunkBytes, err := c.readChunkRange(snapshot, chunkIndex, chunkOffset, readLength)
		if err != nil {
			return nil, err
		}
		if len(chunkBytes) == 0 {
			return nil, fmt.Errorf("received empty range for %s[%d] at offset %d", snapshot.Filename, chunkIndex, chunkOffset)
		}

		data = append(data, chunkBytes...)
		current += uint64(len(chunkBytes))
	}

	return data, nil
}

func (c *Controller) readChunkRange(snapshot *filePlanningSnapshot, chunkIndex uint32, chunkOffset uint64, length uint64) ([]byte, error) {
	nodes := snapshot.ChunkNodes[chunkIndex]
	if len(nodes) == 0 {
		return nil, fmt.Errorf("chunk %s[%d] has no readable replicas", snapshot.Filename, chunkIndex)
	}

	chunkInfo := &messages.ChunkInfo{
		Filename:   snapshot.Filename,
		ChunkIndex: chunkIndex,
	}

	var lastErr error
	for _, node := range nodes {
		data, err := readChunkRangeFromNode(node, chunkInfo, chunkOffset, length)
		if err == nil {
			return data, nil
		}
		lastErr = err
	}

	return nil, fmt.Errorf("failed to read %s[%d] range from all replicas: %w", snapshot.Filename, chunkIndex, lastErr)
}

func readChunkRangeFromNode(node *messages.NodeInfo, chunkInfo *messages.ChunkInfo, chunkOffset uint64, length uint64) ([]byte, error) {
	addr := fmt.Sprintf("%s:%d", node.Hostname, node.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	handler := messages.NewMessageHandler(conn)
	defer handler.Close()

	req := &messages.Wrapper{
		Msg: &messages.Wrapper_ReadChunkRangeRequest{
			ReadChunkRangeRequest: &messages.ReadChunkRangeRequest{
				ChunkInfo:   chunkInfo,
				ChunkOffset: chunkOffset,
				Length:      length,
			},
		},
	}
	if err := handler.Send(req); err != nil {
		return nil, fmt.Errorf("failed to send ReadChunkRangeRequest: %w", err)
	}

	wrapper, err := handler.Receive()
	if err != nil {
		return nil, fmt.Errorf("failed to receive ReadChunkRangeResponse: %w", err)
	}

	resp := wrapper.Msg.(*messages.Wrapper_ReadChunkRangeResponse).ReadChunkRangeResponse
	if !resp.Ok {
		return nil, fmt.Errorf("storage rejected range read: %s", resp.Error)
	}

	return resp.ChunkData, nil
}

func (s *filePlanningSnapshot) chunkNodesForOffset(offset uint64) []*messages.NodeInfo {
	if s.ChunkCount == 0 {
		return nil
	}
	chunkIndex := uint32(offset / s.ChunkSize)
	if chunkIndex >= s.ChunkCount {
		chunkIndex = s.ChunkCount - 1
	}
	return s.ChunkNodes[chunkIndex]
}

func cloneMessageNodes(nodes []*messages.NodeInfo) []*messages.NodeInfo {
	cloned := make([]*messages.NodeInfo, 0, len(nodes))
	for _, node := range nodes {
		cloned = append(cloned, &messages.NodeInfo{
			NodeId:   node.NodeId,
			Hostname: node.Hostname,
			Port:     node.Port,
		})
	}
	return cloned
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
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
 * Simple approach: pick nodes in round-robin order while excluding nodes that
 * cannot fit the chunk being assigned.
 */
func (c *Controller) selectNodes(count int, requiredBytes uint64) ([]*NodeInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	eligible := make([]*NodeInfo, 0, len(c.nodes))
	for _, n := range c.nodes {
		if n.FreeSpace >= requiredBytes {
			eligible = append(eligible, n)
		}
	}
	if len(eligible) < count {
		return nil, fmt.Errorf("not enough eligible nodes: need %d, have %d", count, len(eligible))
	}

	// Build a stable order so round-robin behavior is deterministic across runs.
	// We rotate through this ordered slice using nextPlacementIndex.
	for i := 0; i < len(eligible)-1; i++ {
		for j := i + 1; j < len(eligible); j++ {
			if eligible[j].NodeId < eligible[i].NodeId {
				eligible[i], eligible[j] = eligible[j], eligible[i]
			}
		}
	}

	selected := make([]*NodeInfo, 0, count)
	start := 0
	if len(eligible) > 0 {
		start = c.nextPlacementIndex % len(eligible)
	}
	for i := 0; i < count; i++ {
		idx := (start + i) % len(eligible)
		selected = append(selected, eligible[idx])
	}

	// Advance the cursor once per chunk assignment so the next chunk starts at
	// the next eligible node in the stable ordering.
	c.nextPlacementIndex = (start + 1) % len(eligible)
	return selected, nil
}

// chunkSizeForIndex returns the exact number of bytes that chunk i will carry.
// All chunks except the last use the requested chunk size; the last chunk may be
// smaller when the file size is not an exact multiple.
func chunkSizeForIndex(fileSize uint64, chunkSize uint64, chunkCount uint32, index uint32) uint64 {
	if chunkCount == 0 {
		return 0
	}
	if index < chunkCount-1 {
		return chunkSize
	}

	offset := uint64(index) * chunkSize
	if offset >= fileSize {
		return 0
	}
	return fileSize - offset
}
