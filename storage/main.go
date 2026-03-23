package main

import (
	"flag"
	"fmt"
	"log"
	"net"
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
	controllerAddr := flag.String("controller", "", "Controller address (host:port)")
	storageDir := flag.String("storage-dir", "", "Directory to store chunks")
	port := flag.Int("port", 10000, "Port for this storage node to listen on")
	flag.Parse()

	if *controllerAddr == "" {
		log.Fatalf("Usage: storage --controller <host:port> --storage-dir <path> --port <port>")
	}
	if *storageDir == "" {
		log.Fatalf("Usage: storage --controller <host:port> --storage-dir <path> --port <port>")
	}

	controllerHost, controllerPort, err := utils.ParseAddr(*controllerAddr)
	if err != nil {
		log.Fatalf("Invalid controller address %s: %v", *controllerAddr, err)
	}

	node := &StorageNode{
		controllerHost: controllerHost,
		controllerPort: controllerPort,
		storageDir:     *storageDir,
		port:           int32(*port),
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
