package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"time"
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

	controllerHost, controllerPort, err := parseAddr(*controllerAddr)
	if err != nil {
		log.Fatalf("Invalid controller address %s: %v", *controllerAddr, err)
	}

	node := &StorageNode{
		controllerHost: controllerHost,
		controllerPort: controllerPort,
		storageDir:     *storageDir,
		port:           int32(*port),
	}

	// Reconnect loop
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
 * Split "host:port" into separate host and port
 */
func parseAddr(addr string) (string, int32, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}

	var port int
	_, err = fmt.Sscanf(portStr, "%d", &port)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port: %s", portStr)
	}
	return host, int32(port), nil
}
