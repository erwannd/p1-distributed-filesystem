package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
)

func main() {
	port := flag.String("port", "8000", "Port for controller to listen on")
	flag.Parse()

	controller := &Controller{
		nodes:  make(map[uint32]*NodeInfo),
		mu:     sync.RWMutex{},
		nextId: 1,
	}

	go controller.startFailureDetector()

	// Listen for connection request
	addr := fmt.Sprint(":%s", *port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Println("[Controller] Listening on :8000")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[Controller] Accept error: %v", err)
			continue
		}
		go handleConnection(controller, conn)
	}
}
