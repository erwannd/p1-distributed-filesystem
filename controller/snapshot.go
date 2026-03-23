package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/erwannd/dfs/utils"
)

type SnapshotNodeInfo struct {
	NodeId   uint32 `json:"node_id"`
	Hostname string `json:"hostname"`
	Port     int32  `json:"port"`
}

type SnapshotChunkMetadata struct {
	ChunkIndex uint32             `json:"chunk_index"`
	Nodes      []SnapshotNodeInfo `json:"nodes"`
}

type SnapshotFileMetadata struct {
	Filename   string                           `json:"filename"`
	FileSize   uint64                           `json:"file_size"`
	ChunkSize  uint64                           `json:"chunk_size"`
	ChunkCount uint32                           `json:"chunk_count"`
	Chunks     map[uint32]SnapshotChunkMetadata `json:"chunks"`
}

type Snapshot struct {
	Files map[string]SnapshotFileMetadata `json:"files"`
}

/**
 * Serialize runtime structs to JSON and save to disk.
 */
func (c *Controller) saveSnapshot() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Convert in-memory structs to snapshot structs
	snapshot := Snapshot{
		Files: make(map[string]SnapshotFileMetadata),
	}

	for filename, fileMeta := range c.files {
		chunks := make(map[uint32]SnapshotChunkMetadata)
		for idx, chunkMeta := range fileMeta.Chunks {
			nodes := make([]SnapshotNodeInfo, len(chunkMeta.Nodes))
			for i, n := range chunkMeta.Nodes {
				nodes[i] = SnapshotNodeInfo{
					NodeId:   n.NodeId,
					Hostname: n.Hostname,
					Port:     n.Port,
				}
			}
			chunks[idx] = SnapshotChunkMetadata{
				ChunkIndex: chunkMeta.ChunkIndex,
				Nodes:      nodes,
			}
		}
		snapshot.Files[filename] = SnapshotFileMetadata{
			Filename:   fileMeta.Filename,
			FileSize:   fileMeta.FileSize,
			ChunkSize:  fileMeta.ChunkSize,
			ChunkCount: fileMeta.ChunkCount,
			Chunks:     chunks,
		}
	}

	// Write to temp file first, then rename — atomic write
	// prevents corrupted snapshot if Controller crashes mid-write
	tmpPath := utils.ControllerSnapshotPath + ".tmp"
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}
	if err := os.Rename(tmpPath, utils.ControllerSnapshotPath); err != nil {
		return fmt.Errorf("failed to finalize snapshot: %w", err)
	}

	log.Printf("[Controller] Snapshot saved (%d files)", len(snapshot.Files))
	return nil
}

/**
 * Deserialize snapshot from disk on startup.
 */
func (c *Controller) loadSnapshot() error {
	data, err := os.ReadFile(utils.ControllerSnapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[Controller] No snapshot found, starting fresh")
			return nil // not an error — first startup
		}
		return fmt.Errorf("failed to read snapshot: %w", err)
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	// Convert snapshot structs back to runtime structs
	for filename, fileMeta := range snapshot.Files {
		chunks := make(map[uint32]*ChunkMetadata)
		for idx, chunkMeta := range fileMeta.Chunks {
			nodes := make([]*NodeInfo, len(chunkMeta.Nodes))
			for i, n := range chunkMeta.Nodes {
				nodes[i] = &NodeInfo{
					NodeId:   n.NodeId,
					Hostname: n.Hostname,
					Port:     n.Port,
				}
			}
			chunks[idx] = &ChunkMetadata{
				ChunkIndex: chunkMeta.ChunkIndex,
				Nodes:      nodes,
			}
		}
		c.files[filename] = &FileMetadata{
			Filename:   fileMeta.Filename,
			FileSize:   fileMeta.FileSize,
			ChunkSize:  fileMeta.ChunkSize,
			ChunkCount: fileMeta.ChunkCount,
			Chunks:     chunks,
		}
	}

	log.Printf("[Controller] Snapshot loaded (%d files)", len(c.files))
	return nil
}

/**
 * Save Controller state periodically in background.
 */
func (c *Controller) startSnapshotLoop(interval time.Duration) {
	for {
		time.Sleep(interval)
		if err := c.saveSnapshot(); err != nil {
			log.Printf("[Controller] Failed to save snapshot: %v", err)
		}
	}
}
