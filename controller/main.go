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
	address := fmt.Sprintf(":%s", *port)
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
