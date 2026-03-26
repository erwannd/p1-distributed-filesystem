package utils

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"net"
	"syscall"
	"time"
)

const (
	ReplicationFactor      = 3 // number of replicas per chunk
	HeartbeatInterval      = 5 * time.Second
	HeartbeatTimeout       = 15 * time.Second
	SnapshotInterval       = 30 * time.Second
	ControllerSnapshotPath = "controller_snapshot.json"
)

/**
 * Compute checksum.
 * Returns MD5 hash of data.
 */
func ComputeChecksum(data []byte) []byte {
	hash := md5.Sum(data)
	return hash[:]
}

/**
 * Checks if data matches expected checksum
 */
func VerifyChecksum(data []byte, expected []byte) bool {
	computed := ComputeChecksum(data)
	return bytes.Equal(computed, expected)
}

/**
 * Returns free bytes at the given path.
 */
func GetAvailableDiskSpace(path string) uint64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0
	}
	return stat.Bavail * uint64(stat.Bsize)
}

/**
 * Returns the standard path for a chunk file.
 */
func ChunkPath(storageDir, filename string, chunkIndex uint32) string {
	return fmt.Sprintf("%s/%s_chunk_%d", storageDir, filename, chunkIndex)
}

// ChecksumPath returns the standard path for a checksum sidecar file
func ChecksumPath(storageDir, filename string, chunkIndex uint32) string {
	return ChunkPath(storageDir, filename, chunkIndex) + ".checksum"
}

/**
 * Split "host:port" into separate host and port
 */
func ParseAddr(addr string) (string, int32, error) {
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
