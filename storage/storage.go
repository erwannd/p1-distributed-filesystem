package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/erwannd/dfs/messages"
)

// StorageNode struct, register, heartbeat loop
type StorageNode struct {
	nodeId         uint32
	controllerHost string
	controllerPort int32
	storageDir     string
	port           int32         // this node's listening port
	numRequests    atomic.Uint64 // use s.numRequests.Add(1)
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
	freeSpace := getAvailableDiskSpace(node.storageDir)
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

	node.nodeId = resp.NodeId
	log.Printf("[StorageNode] Registered with Controller, assigned ID: %d", node.nodeId)
	return handler, nil
}

func (s *StorageNode) heartbeatLoop(handler *messages.MessageHandler) {
	for {
		time.Sleep(5 * time.Second)

		// TODO: handle NewChunks once replication logic is in-place
		heartbeat := &messages.Wrapper{
			Msg: &messages.Wrapper_Heartbeat{
				Heartbeat: &messages.Heartbeat{
					NodeId:     s.nodeId,
					FreeSpace:  getAvailableDiskSpace(s.storageDir),
					NumRequest: s.numRequests.Load(),
					NewChunks:  nil,
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
		// TODO: handle replicate requests once failure detection is implemented
		if len(resp.ReplicateRequest) > 0 {
			log.Printf("[StorageNode] Received %d replication requests (not yet implemented)", len(resp.ReplicateRequest))
		}

		log.Printf("[StorageNode] Heartbeat acknowledged by Controller (free space: %d MB)", getAvailableDiskSpace(s.storageDir)>>20)
	}
}

func getAvailableDiskSpace(path string) uint64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0
	}
	return stat.Bavail * uint64(stat.Bsize)
}
