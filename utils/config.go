package utils

import (
	"encoding/json"
	"fmt"
	"os"
)

type ControllerConfig struct {
	Host                 string `json:"host"`
	Port                 int32  `json:"port"`
	SnapshotPath         string `json:"snapshotPath"`
	SnapshotIntervalSecs int    `json:"snapshotIntervalSecs"`
}

type StorageConfig struct {
	BaseOutputDir string       `json:"baseOutputDir"`
	Nodes         []NodeConfig `json:"nodes"`
}

type NodeConfig struct {
	Host string `json:"host"`
	Port int32  `json:"port"`
}

type ClientConfig struct {
	OutputDir     string `json:"outputDir"`
	ChunkSizeMB   uint64 `json:"chunkSizeMB"`
	MaxConcurrent int    `json:"maxConcurrent"`
}

type Config struct {
	Controller ControllerConfig `json:"controller"`
	Storage    StorageConfig    `json:"storage"`
	Client     ClientConfig     `json:"client"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	return &config, nil
}

/**
 * Returns chunk size in bytes
 */
func (c *ClientConfig) ChunkSizeBytes() uint64 {
	return c.ChunkSizeMB << 20
}

/**
 * Returns controller address as host:port string
 */
func (c *ControllerConfig) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
