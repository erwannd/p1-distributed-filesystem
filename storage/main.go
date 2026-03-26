package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/erwannd/dfs/utils"
)

// Storage Node main():
//     connect to Controller
//     send RegisterRequest
//     receive RegisterResponse → store nodeId
//     go heartbeatLoop()       ← separate goroutine
//     ... (later: listen for chunk requests)

// heartbeatLoop():
//     for:
//         time.Sleep(5 * time.Second)
//         send Heartbeat
//         receive HeartbeatResponse

// entry point, reads args (controller host, storage dir)
// controllerAddr from command line e.g. "orion01:8000"

func main() {
	// Controller addr must be provided in config file
	// base storage dir must be provided in config file
	configPath := flag.String("config", "config.json", "Path to config file")
	port := flag.Int("port", 0, "Override port for this storage node to listen on")
	flag.Parse()

	config, err := utils.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Determine this node's port:
	// 1. CLI flag takes priority
	// 2. Fall back to finding this host in config nodes list
	nodePort := int32(*port)
	if nodePort == 0 {
		nodePort = findMyPort(config)
	}
	if nodePort == 0 {
		log.Fatalf("[StorageNode] No port specified — use --port flag")
	}

	hostname, _ := os.Hostname()
	nodeStorageDir := filepath.Join(
		config.Storage.BaseOutputDir,
		hostname,
		fmt.Sprintf("node_%d", nodePort),
	)

	node := &StorageNode{
		controllerHost: config.Controller.Host,
		controllerPort: config.Controller.Port,
		storageDir:     nodeStorageDir,
		port:           nodePort,
	}

	// Create storage directory if it doesn't exist
	if err := os.MkdirAll(nodeStorageDir, 0755); err != nil {
		log.Fatalf("[StorageNode] Failed to create storage directory %s: %v", nodeStorageDir, err)
	}

	// Start chunk listener BEFORE registering
	// so the node is ready to receive chunks by the time
	// the Controller tells clients about it
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("[StorageNode] Failed to listen on port %d: %v", *port, err)
	}
	go node.listenForChunks(listener)

	// Reconnect loop to Controller
	for {
		handler, err := node.register()
		if err != nil {
			log.Printf("[StorageNode] Failed to register, retrying in 5s: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Blocks until connection dies
		node.heartbeatLoop(handler)

		log.Printf("[StorageNode] Lost connection to Controller, reconnecting...")
		handler.Close()
		time.Sleep(5 * time.Second)
	}
}

/**
 * Looks up this machine's hostname in the config nodes list
 * Returns 0 if not found
 */
func findMyPort(config *utils.Config) int32 {
	hostname, err := os.Hostname()
	if err != nil {
		return 0
	}
	for _, node := range config.Storage.Nodes {
		if node.Host == hostname {
			return node.Port
		}
	}
	return 0
}
