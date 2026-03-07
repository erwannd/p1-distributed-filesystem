package messages

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"google.golang.org/protobuf/proto"
)

const (
	MaxFrameSize = 128 << 20 // 128MB for chunk transfer
)

type MessageHandler struct {
	conn net.Conn
}

func NewMessageHandler(conn net.Conn) *MessageHandler {
	m := &MessageHandler{
		conn: conn,
	}
	return m
}

func (m *MessageHandler) RemoteAddr() string {
	if m.conn == nil {
		return "unknown"
	}
	return m.conn.RemoteAddr().String()
}

func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

/**
 * Convert protobuf objects to bytes
 */
func (m *MessageHandler) Send(task *Wrapper) error {
	if task == nil {
		return errors.New("Send: nil Wrapper")
	}

	serialized, err := proto.Marshal(task)
	if err != nil {
		return fmt.Errorf("Send: marshal Wrapper: %w", err)
	}
	if len(serialized) > MaxFrameSize {
		return fmt.Errorf("Send: payload too large: %d bytes > max %d bytes", len(serialized), MaxFrameSize)
	}

	var prefix [4]byte
	binary.LittleEndian.PutUint32(prefix[:], uint32(len(serialized)))

	if err := writeFull(m.conn, prefix[:]); err != nil {
		return fmt.Errorf("Send: write length prefix to %s: %w", m.RemoteAddr(), err)
	}
	if err := writeFull(m.conn, serialized); err != nil {
		return fmt.Errorf("Send: write payload to %s: %w", m.RemoteAddr(), err)
	}
	return nil
}

/**
 * Convert bytes back to protobuf objects
 */
func (m *MessageHandler) Receive() (*Wrapper, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(m.conn, prefix[:]); err != nil {
		return nil, fmt.Errorf("Receive: read length prefix from %s: %w", m.RemoteAddr(), err)
	}

	payloadSize := binary.LittleEndian.Uint32(prefix[:])
	if payloadSize > MaxFrameSize {
		return nil, fmt.Errorf("Receive: payload too large: %d bytes > max %d bytes from %s", payloadSize, MaxFrameSize, m.RemoteAddr())
	}

	payload := make([]byte, payloadSize)
	if _, err := io.ReadFull(m.conn, payload); err != nil {
		return nil, fmt.Errorf("Receive: read payload from %s: %w", m.RemoteAddr(), err)
	}

	wrapper := &Wrapper{}
	if err := proto.Unmarshal(payload, wrapper); err != nil {
		return nil, fmt.Errorf("Receive: unmarshal payload from %s: %w", m.RemoteAddr(), err)
	}
	return wrapper, nil
}

func (m *MessageHandler) SetReadDeadline(d time.Duration) error {
	if d <= 0 {
		return m.conn.SetReadDeadline(time.Time{})
	}
	return m.conn.SetReadDeadline(time.Now().Add(d))
}

func (m *MessageHandler) SetWriteDeadline(d time.Duration) error {
	if d <= 0 {
		return m.conn.SetWriteDeadline(time.Time{})
	}
	return m.conn.SetWriteDeadline(time.Now().Add(d))
}

func (m *MessageHandler) Close() {
	m.conn.Close()
}
