package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/erwannd/dfs/messages"
	"github.com/erwannd/dfs/utils"
)

type Client struct {
	controllerAddr string // controller address (host:port)
	outputDir      string // where to save retrieved file
	chunkSize      uint64 // chunk size (bytes)
	maxConcurrent  int
}

/**
 * Connect to the Controller.
 * Returns a message handler.
 */
func (client *Client) connectToController() (*messages.MessageHandler, error) {
	conn, err := net.Dial("tcp", client.controllerAddr)
	if err != nil {
		return nil, err
	}
	return messages.NewMessageHandler(conn), nil
}

/**
 * Parses flags for store command.
 * Chunk size can be provided in config or via CLI, in either case must be in MB.
 * Chunk size provided via CLI flag overrides value set in config.
 */
func (client *Client) handleStore(args []string) {
	fs := flag.NewFlagSet("store", flag.ExitOnError)
	file := fs.String("file", "", "Path to the file to store")
	chunkSize := fs.Uint64("chunk-size", client.chunkSize, "Chunk size in bytes")
	fs.Parse(args)

	if *file == "" {
		log.Fatalf("store requires --file")
	}

	fmt.Printf("Store: controller=%s file=%s chunkSize=%d\n bytes", client.controllerAddr, *file, *chunkSize)

	if err := client.store(*file, *chunkSize); err != nil {
		log.Fatalf("[Client] Store failed: %v", err)
	}
}

/**
 * Parses flags for retrieve command.
 * Output dir can be provided via CLI or config file.
 * Value set via CLI takes priority over config.
 */
func (client *Client) handleRetrieve(args []string) {
	fs := flag.NewFlagSet("retrieve", flag.ExitOnError)
	filename := fs.String("file", "", "File to retrieve")
	output := fs.String("output", client.outputDir, "Output directory")
	fs.Parse(args)

	if *filename == "" {
		log.Fatalf("retrieve requires --file")
	}

	// Ensure output directory exists before retrieve
	if err := os.MkdirAll(client.outputDir, 0755); err != nil {
		log.Fatalf("[Client] Failed to create output directory: %v", err)
	}

	fmt.Printf("Retrieve: controller=%s file=%s output=%s\n", client.controllerAddr, *filename, *output)

	if err := client.retrieve(*filename, *output); err != nil {
		log.Fatalf("[Client] Retrieve failed: %v", err)
	}
}

/**
 * Parses flags for delete command.
 */
func (client *Client) handleDelete(args []string) {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	filename := fs.String("file", "", "File to delete")
	fs.Parse(args)

	if *filename == "" {
		log.Fatalf("delete requires --file")
	}

	fmt.Printf("Delete: controller=%s file=%s", client.controllerAddr, *filename)

	if err := client.delete(*filename); err != nil {
		log.Fatalf("[Client] Delete failed: %v", err)
	}
}

/**
 * Handle store request. The steps roughly are:
 * 	1. Open & stat the file -> get file size
 *	2. Calculate chunk count based on chunk size
 *	3. Send StoreRequest to Controller -> get ChunkDestinations
 *	4. Split file into chunks
 *	5. For each chunk -> send StoreChunkRequest to first pipeline node
 *	6. Wait for StoreChunkResponse ACK
 *	7. Repeat for all chunks (can be parallel with goroutines)
 */
func (client *Client) store(filename string, chunkSize uint64) error {
	// Get file size
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	// Calculate chunk count
	fileSize := uint64(fileInfo.Size())
	chunkCount := (fileSize + chunkSize - 1) / chunkSize
	log.Printf("[Client] Storing %s: %d bytes, %d chunks", filename, fileSize, chunkCount)

	// Ask Controller where to store chunks
	handler, err := client.connectToController()
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %w", err)
	}
	defer handler.Close()

	req := &messages.Wrapper{
		Msg: &messages.Wrapper_StoreRequest{
			StoreRequest: &messages.StoreRequest{
				Filename:   filepath.Base(filename),
				ChunkCount: uint32(chunkCount),
				ChunkSize:  chunkSize,
				FileSize:   fileSize,
			},
		},
	}
	if err := handler.Send(req); err != nil {
		return fmt.Errorf("failed to send StoreRequest: %w", err)
	}

	/* Receive destination storage nodes from Controller's response */
	wrapper, err := handler.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive StoreResponse: %w", err)
	}
	resp := wrapper.Msg.(*messages.Wrapper_StoreResponse).StoreResponse
	if !resp.Ok {
		return fmt.Errorf("controller rejected store: %s", resp.Error)
	}

	/* Send chunks in parallel */
	var wg sync.WaitGroup
	sem := make(chan struct{}, client.maxConcurrent) // limit how many goroutine be running at a time
	errors := make([]error, chunkCount)

	for i, dest := range resp.Destinations {
		sem <- struct{}{} // acquire slot before spawning goroutine
		wg.Add(1)
		go func(idx int, d *messages.ChunkMapping) {
			defer wg.Done()
			defer func() { <-sem }() // release slot

			offset := int64(idx) * int64(chunkSize)

			size := chunkSize
			// Last chunk might be smaller
			if idx == int(chunkCount)-1 {
				remaining := fileSize - uint64(offset)
				if remaining < chunkSize {
					size = remaining
				}
			}

			data := make([]byte, size)
			if _, err := file.ReadAt(data, offset); err != nil {
				errors[idx] = fmt.Errorf("failed to read chunk %d: %w", idx, err)
				return
			}

			if err := sendChunk(data, idx, d); err != nil {
				errors[idx] = err
			}
		}(i, dest)
	}
	wg.Wait()

	// Check errors
	for i, err := range errors {
		if err != nil {
			log.Printf("[Client] Failed to send chunk %d: %v", i, err)
		}
	}

	return nil
}

func (client *Client) retrieve(filename string, outputDir string) error {
	// 1. Connect to Controller
	handler, err := client.connectToController()
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %w", err)
	}
	defer handler.Close()

	// 2. Send retrive request
	req := &messages.Wrapper{
		Msg: &messages.Wrapper_RetrieveRequest{
			RetrieveRequest: &messages.RetrieveRequest{
				Filename: filename,
			},
		},
	}
	if err := handler.Send(req); err != nil {
		return fmt.Errorf("failed to send RetrieveRequest to Controller: %w", err)
	}

	// 3. Get chunk mapping (chunk id -> storage nodes) from Controller
	wrapper, err := handler.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive RetrieveResponse: %w", err)
	}
	resp := wrapper.Msg.(*messages.Wrapper_RetrieveResponse).RetrieveResponse
	if !resp.Ok {
		return fmt.Errorf("controller rejected retrieve: %s", resp.Error)
	}

	// 4. Fetch chunks (parallel)
	// `chunks` for a large file this holds all chunk data in memory simultaneously
	// An alternative is to write each chunk to a temp file as it arrives, then combine
	chunkCount := len(resp.Locations)
	chunks := make([][]byte, chunkCount) // store chunks in order
	errors := make([]error, chunkCount)

	var wg sync.WaitGroup
	sem := make(chan struct{}, client.maxConcurrent)

	for i, location := range resp.Locations {
		sem <- struct{}{}
		wg.Add(1)
		go func(idx int, loc *messages.ChunkMapping) {
			defer wg.Done()
			defer func() { <-sem }()

			data, err := fetchChunk(loc)
			if err != nil {
				errors[idx] = err
				return
			}
			chunks[idx] = data // store at correct index — order preserved
		}(i, location)
	}
	wg.Wait()

	// Check for errors before writing
	for i, err := range errors {
		if err != nil {
			return fmt.Errorf("failed to retrieve chunk %d: %w", i, err)
		}
	}

	// 6. Reassemble in order, then write to disk
	outputPath := filepath.Join(outputDir, filename)
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	for i, chunk := range chunks {
		if _, err := outFile.Write(chunk); err != nil {
			return fmt.Errorf("failed to write chunk %d: %w", i, err)
		}
	}

	log.Printf("[Client] Retrieved %s -> %s", filename, outputPath)
	return nil
}

/**
 * Handle delete command.
 */
func (client *Client) delete(filename string) error {
	// 1. Connect to Controller
	handler, err := client.connectToController()
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %w", err)
	}
	defer handler.Close()

	// 2. Send delete request
	req := &messages.Wrapper{
		Msg: &messages.Wrapper_DeleteRequest{
			DeleteRequest: &messages.DeleteRequest{
				Filename: filename,
			},
		},
	}

	if err := handler.Send(req); err != nil {
		return fmt.Errorf("failed to send DeleteRequest to Controller: %w", err)
	}

	// 3. Receive delete response from Controller
	wrapper, err := handler.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive DeleteResponse: %w", err)
	}
	resp := wrapper.Msg.(*messages.Wrapper_DeleteResponse).DeleteResponse
	if !resp.Ok {
		return fmt.Errorf("delete failed: %s", resp.Error)
	}
	log.Printf("[Client] Deleted %s", filename)
	return nil
}

/**
 * Handle list command.
 */
func (client *Client) list() error {
	// 1. Connect to Controller
	handler, err := client.connectToController()
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %w", err)
	}
	defer handler.Close()

	// 2. Build request message
	req := &messages.Wrapper{
		Msg: &messages.Wrapper_ListRequest{
			ListRequest: &messages.ListRequest{},
		},
	}
	if err := handler.Send(req); err != nil {
		return fmt.Errorf("failed to send ListRequest: %w", err)
	}

	// 3. Receive response from Controller
	wrapper, err := handler.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive ListResponse: %w", err)
	}
	resp := wrapper.Msg.(*messages.Wrapper_ListResponse).ListResponse
	if !resp.Ok {
		return fmt.Errorf("list failed: %s", resp.Error)
	}
	printFileList(resp.Files)
	return nil
}

/**
 * Handle nodes command (get cluster info).
 */
func (client *Client) nodes() error {
	// 1. Connect to Controller
	handler, err := client.connectToController()
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %w", err)
	}
	defer handler.Close()

	// 2. Build request message
	req := &messages.Wrapper{
		Msg: &messages.Wrapper_ClusterInfoRequest{
			ClusterInfoRequest: &messages.ClusterInfoRequest{},
		},
	}
	if err := handler.Send(req); err != nil {
		return fmt.Errorf("failed to send ClusterInfoRequest: %w", err)
	}

	// 3. Receive response from Controller
	wrapper, err := handler.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive ClusterInfoResponse: %w", err)
	}
	resp := wrapper.Msg.(*messages.Wrapper_ClusterInfoResponse).ClusterInfoResponse
	printClusterInfo(resp)
	return nil
}

/**
 * Retrieve one chunk for the client.
 * The client still tries the mapped nodes in order, but each request now carries
 * the sibling replicas so the contacted Storage Node can self-repair first.
 */
func fetchChunk(loc *messages.ChunkMapping) ([]byte, error) {
	if len(loc.Nodes) == 0 {
		return nil, fmt.Errorf("no storage nodes available for %s[%d]", loc.ChunkInfo.Filename, loc.ChunkInfo.ChunkIndex)
	}

	var lastErr error
	for idx, node := range loc.Nodes {
		data, err := fetchChunkFromNode(loc.ChunkInfo, node, replicaHints(loc.Nodes, idx))
		if err == nil {
			return data, nil
		}
		lastErr = err
	}

	return nil, fmt.Errorf("failed to retrieve %s[%d] from all replicas: %w", loc.ChunkInfo.Filename, loc.ChunkInfo.ChunkIndex, lastErr)
}

/**
 * Fetch a chunk from one specific Storage Node, passing the remaining replicas
 * as repair hints in case that node's local copy is corrupted.
 */
func fetchChunkFromNode(chunkInfo *messages.ChunkInfo, node *messages.NodeInfo, hints []*messages.NodeInfo) ([]byte, error) {
	addr := fmt.Sprintf("%s:%d", node.Hostname, node.Port)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	handler := messages.NewMessageHandler(conn)
	defer handler.Close()

	req := &messages.Wrapper{
		Msg: &messages.Wrapper_RetrieveChunkRequest{
			RetrieveChunkRequest: &messages.RetrieveChunkRequest{
				ChunkInfo:    chunkInfo,
				ReplicaHints: hints,
			},
		},
	}
	if err := handler.Send(req); err != nil {
		return nil, fmt.Errorf("failed to send RetrieveChunkRequest: %w", err)
	}

	wrapper, err := handler.Receive()
	if err != nil {
		return nil, fmt.Errorf("failed to receive chunk: %w", err)
	}
	resp := wrapper.Msg.(*messages.Wrapper_RetrieveChunkResponse).RetrieveChunkResponse
	if !resp.Ok {
		return nil, fmt.Errorf("storage node rejected retrieve: %s", resp.Error)
	}

	return resp.ChunkData, nil
}

// replicaHints returns every node except the one the client is contacting now.
func replicaHints(nodes []*messages.NodeInfo, skipIdx int) []*messages.NodeInfo {
	hints := make([]*messages.NodeInfo, 0, len(nodes)-1)
	for idx, node := range nodes {
		if idx == skipIdx {
			continue
		}
		hints = append(hints, node)
	}
	return hints
}

/**
 * Wraps data in a protobuf message and sends to primary storage node.
 */
func sendChunk(data []byte, idx int, destNodes *messages.ChunkMapping) error {
	primaryNode := destNodes.Nodes[0]
	primaryAddr := fmt.Sprintf("%s:%d", primaryNode.Hostname, primaryNode.Port)

	conn, err := net.Dial("tcp", primaryAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to storage node %s: %w", primaryAddr, err)
	}
	handler := messages.NewMessageHandler(conn)
	defer handler.Close()

	req := &messages.Wrapper{
		Msg: &messages.Wrapper_StoreChunkRequest{
			StoreChunkRequest: &messages.StoreChunkRequest{
				ChunkInfo: destNodes.ChunkInfo,
				ChunkData: data,
				Checksum:  utils.ComputeChecksum(data),
				Pipeline:  destNodes.Nodes[1:],
			},
		},
	}

	if err := handler.Send(req); err != nil {
		return fmt.Errorf("failed to send chunk %d: %w", idx, err)
	}

	wrapper, err := handler.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive ack for chunk %d: %w", idx, err)
	}

	resp := wrapper.Msg.(*messages.Wrapper_StoreChunkResponse).StoreChunkResponse
	if !resp.Ok {
		return fmt.Errorf("storage node rejected chunk %d: %s", idx, resp.Error)
	}

	log.Printf("[Client] Chunk %d stored at %s", idx, primaryAddr)
	return nil
}

/**
 * Helper to print file info list.
 */
func printFileList(files []*messages.FileInfo) {
	if len(files) == 0 {
		fmt.Println("No files stored in the system.")
		return
	}

	fmt.Printf("Files stored in system:\n")
	fmt.Printf("%-30s %12s %8s\n", "Filename", "Size", "Chunks")
	fmt.Printf("%-30s %12s %8s\n",
		strings.Repeat("-", 30),
		strings.Repeat("-", 12),
		strings.Repeat("-", 8))

	for _, f := range files {
		fmt.Printf("%-30s %12s %8d\n",
			f.Filename,
			formatSize(f.FileSize),
			f.ChunkCount)
	}
	fmt.Printf("\nTotal: %d file(s)\n", len(files))
}

/**
 * Helper to print cluster info.
 */
func printClusterInfo(resp *messages.ClusterInfoResponse) {
	fmt.Printf("Cluster Info:\n")
	fmt.Printf("%-6s %-30s %12s %12s\n", "ID", "Hostname", "Free Space", "Requests")
	fmt.Printf("%-6s %-30s %12s %12s\n",
		strings.Repeat("-", 6),
		strings.Repeat("-", 30),
		strings.Repeat("-", 12),
		strings.Repeat("-", 12))

	for _, n := range resp.NodeStats {
		fmt.Printf("%-6d %-30s %12s %12d\n",
			n.NodeInfo.NodeId,
			fmt.Sprintf("%s:%d", n.NodeInfo.Hostname, n.NodeInfo.Port),
			formatSize(n.FreeSpace),
			n.NumRequests)
	}

	fmt.Printf("\nTotal free space: %s\n", formatSize(resp.TotalFreeSpace))
	fmt.Printf("Total requests:   %d\n", resp.TotalRequests)
	fmt.Printf("Active nodes:     %d\n", len(resp.NodeStats))
}

func formatSize(bytes uint64) string {
	const (
		KB = 1 << 10
		MB = 1 << 20
		GB = 1 << 30
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
