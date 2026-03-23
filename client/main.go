package main

import (
	"flag"
	"log"
	"os"
)

// ./bin/client --controller localhost:8000 store --file foo.txt --chunk-size 64
// ./bin/client --controller localhost:8000 retrieve --file foo.txt --output ./downloads
// ./bin/client --controller localhost:8000 delete --file foo.txt
// ./bin/client --controller localhost:8000 list

/**
 * Parse args, create Client struct, dispatch to client handler
 */
func main() {
	controllerAddr := flag.String("controller", "", "Controller address (host:port)")
	flag.Parse()

	if *controllerAddr == "" {
		log.Fatalf("Usage: client --controller <host:port> <command> [options]")
	}

	// Create Client
	client := &Client{
		controllerAddr: *controllerAddr,
	}

	// Subcommand (store, retrieve, list)
	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "store":
		client.handleStore(args[1:])
	case "retrieve":
		client.handleRetrieve(args[1:])
	case "delete":
		client.handleDelete(args[1:])
	case "list":
		client.list()
	case "nodes":
		client.nodes()
	default:
		log.Fatalf("Unknown command: %s", args[0])
	}
}

func printUsage() {
	log.Println("Usage: client --controller <host:port> <command> [options]")
	log.Println("Commands:")
	log.Println("  store    --file <path> [--chunk-size <bytes>]")
	log.Println("  retrieve --file <name> --output <path>")
	log.Println("  delete   --file <name>")
	log.Println("  list")
	log.Println("  nodes")
}
