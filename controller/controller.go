package main

import (
	"log"
	"net"
	"sync"
	"time"

	"github.com/erwannd/dfs/messages"
)

// Controller struct, node table, handlers

type NodeInfo struct {
	NodeId    uint32
	Hostname  string
	Port      int32
	FreeSpace uint64
	LastSeen  time.Time // for heartbeat timeout detection
}

type Controller struct {
	nodes  map[uint32]*NodeInfo // maps NodeID -> NodeInfo
	mu     sync.RWMutex         // for concurrent access
	nextId uint32               // incremental ID
}

const (
	HeartbeatInterval = 5 * time.Second
	HeartbeatTimeout  = 15 * time.Second
)

/**
 * Parse message type & dispatch work
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
		default:
			log.Printf("[Controller] Unknown message type from %s", handler.RemoteAddr())
		}
	}
}

/**
 * Handle registration request message
 */
func handleRegister(controller *Controller, msg *messages.RegisterRequest, handler *messages.MessageHandler) {
	// Create a new NodeInfo and add it to the Controller
	node := &NodeInfo{
		NodeId:    controller.nextId,
		Hostname:  msg.Hostname,
		Port:      msg.Port,
		FreeSpace: msg.FreeSpace,
		LastSeen:  time.Now(),
	}

	// Need to lock the mutex before adding node
	controller.mu.Lock()
	controller.nodes[controller.nextId] = node
	controller.nextId += 1
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
 * Handle heartbeat from storage node + send response back
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
	node.LastSeen = time.Now()
	nodeId, hostname, port, freeSpace := node.NodeId, node.Hostname, node.Port, node.FreeSpace
	controller.mu.Unlock()

	// Send HeartbeatResponse
	// TODO: Add replicate request once failure detection + replication are implemented
	resp := &messages.Wrapper{
		Msg: &messages.Wrapper_HeartbeatResponse{
			HeartbeatResponse: &messages.HeartbeatResponse{
				Ack:              true,
				ReplicateRequest: nil,
			},
		},
	}
	if err := handler.Send(resp); err != nil {
		log.Printf("[Controller] Failed to send HeartbeatResponse to %s: %v", handler.RemoteAddr(), err)
		return
	}
	log.Printf("[Controller] Heartbeat from Node %d (%s:%d) - free space: %d MB", nodeId, hostname, port, freeSpace>>20)
}

func (c *Controller) startFailureDetector() {
	for {
		time.Sleep(HeartbeatInterval)

		c.mu.Lock()
		for id, node := range c.nodes {
			if time.Since(node.LastSeen) > HeartbeatTimeout {
				log.Printf("[Controller] Node %d (%s:%d) timed out, marking dead", id, node.Hostname, node.Port)
				delete(c.nodes, id)
				// TODO: need to find all chunks on this failed node -> trigger re-replication
			}
		}
		c.mu.Unlock()
	}
}
