package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/erwannd/dfs/messages"
	"github.com/erwannd/dfs/utils"
)

func main() {
	port := flag.Int("port", 8000, "Port for controller to listen on")
	flag.Parse()

	controller := &Controller{
		nodes:               make(map[uint32]*NodeInfo),
		files:               make(map[string]*FileMetadata),
		mu:                  sync.RWMutex{},
		nextId:              1,
		pendingReplications: make(map[uint32][]*messages.ReplicateRequest),
	}

	// Load snapshot BEFORE starting listener
	// so metadata is ready before any connections arrive
	if err := controller.loadSnapshot(); err != nil {
		log.Fatalf("[Controller] Failed to load snapshot: %v", err)
	}

	go controller.startFailureDetector()
	go controller.startSnapshotLoop(time.Duration(utils.SnapshotInterval))

	// Listen for connection request
	address := fmt.Sprintf(":%d", *port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("[Controller] Failed to listen on %s: %v", address, err)
	}
	log.Println("[Controller] Listening on ", address)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[Controller] Accept error: %v", err)
			continue
		}
		go handleConnection(controller, conn)
	}
}
