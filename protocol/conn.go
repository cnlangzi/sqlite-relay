package protocol

import (
	"net"
	"sync"
	"time"
)

// conn is a connection of the protocol.
//
// The writes are serialized, so that the goroutines of a session may write at
// the same time. The deadlines of the connection close it when it stays
// silent for too long.
type conn struct {
	net.Conn

	wmu      sync.Mutex
	maxFrame int64
	timeout  time.Duration
}

// newConn wraps a connection of the protocol.
func newConn(c net.Conn, maxFrame int64, timeout time.Duration) *conn {
	return &conn{Conn: c, maxFrame: maxFrame, timeout: timeout}
}

// writeFrame writes a frame.
func (c *conn) writeFrame(t MessageType, payload []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return err
	}
	_, err := c.Write(EncodeFrame(t, payload))
	return err
}

// writeMessage writes the payload of a message.
func (c *conn) writeMessage(m message) error {
	return c.writeFrame(m.Type(), m.encode())
}

// readFrame reads a frame.
func (c *conn) readFrame() (MessageType, []byte, error) {
	if err := c.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, nil, err
	}
	return ReadFrame(c.Conn, c.maxFrame)
}
