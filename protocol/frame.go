package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Layout of a frame.
const (
	// FrameMagic identifies a frame of the protocol.
	FrameMagic = "SQRL"
	// FrameVersion is the version of the frame format.
	FrameVersion = 1
	// FrameHeaderSize is the size of the header of a frame.
	FrameHeaderSize = 12
	// MaxFrameBytes is the size of the payload of a frame the protocol
	// accepts.
	MaxFrameBytes = 64 << 20
)

// MessageType is the type of a message. It is the type byte of the frame
// that carries the message.
type MessageType uint8

// The messages of the protocol.
const (
	// MsgHello opens a connection. The backup sends it.
	MsgHello MessageType = 1
	// MsgHelloAck answers a hello. The source sends it.
	MsgHelloAck MessageType = 2
	// MsgBatch carries the segments the source pushes.
	MsgBatch MessageType = 3
	// MsgAck carries the sequence number the backup applied.
	MsgAck MessageType = 4
	// MsgHeartbeat keeps a connection alive.
	MsgHeartbeat MessageType = 5
	// MsgFlowControl carries the limit of the bytes in flight.
	MsgFlowControl MessageType = 6
	// MsgPause asks the source to stop pushing.
	MsgPause MessageType = 7
	// MsgPaused answers a pause.
	MsgPaused MessageType = 8
	// MsgResume tells the source to push again.
	MsgResume MessageType = 9
	// MsgResyncRequired tells the peer that the chain is not continuous.
	MsgResyncRequired MessageType = 10
	// MsgError tells the peer that the connection is ended.
	MsgError MessageType = 11
)

// String returns the name of the type.
func (t MessageType) String() string {
	switch t {
	case MsgHello:
		return "HELLO"
	case MsgHelloAck:
		return "HELLO_ACK"
	case MsgBatch:
		return "BATCH"
	case MsgAck:
		return "ACK"
	case MsgHeartbeat:
		return "HEARTBEAT"
	case MsgFlowControl:
		return "FLOW_CONTROL"
	case MsgPause:
		return "PAUSE"
	case MsgPaused:
		return "PAUSED"
	case MsgResume:
		return "RESUME"
	case MsgResyncRequired:
		return "RESYNC_REQUIRED"
	case MsgError:
		return "ERROR"
	}
	return fmt.Sprintf("MessageType(%d)", uint8(t))
}

// EncodeFrame encodes the header and the payload of a frame.
//
// The header holds the magic of the protocol, the version of the frame
// format, the type of the message, the flags and the size of the payload.
// Every field is big endian.
func EncodeFrame(t MessageType, payload []byte) []byte {
	b := make([]byte, FrameHeaderSize+len(payload))
	copy(b[0:4], FrameMagic)
	b[4] = FrameVersion
	b[5] = byte(t)
	binary.BigEndian.PutUint16(b[6:8], 0)
	binary.BigEndian.PutUint32(b[8:12], uint32(len(payload)))
	copy(b[FrameHeaderSize:], payload)
	return b
}

// ReadFrame reads a frame from r.
//
// A frame whose magic is not the magic of the protocol, whose version is not
// the version of the package or whose payload is bigger than maxBytes is
// rejected.
func ReadFrame(r io.Reader, maxBytes int64) (MessageType, []byte, error) {
	var hdr [FrameHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	if string(hdr[0:4]) != FrameMagic {
		return 0, nil, fmt.Errorf("%w: the magic is %q", ErrInvalidFrame, hdr[0:4])
	}
	if hdr[4] != FrameVersion {
		return 0, nil, fmt.Errorf("%w: the version of the frame is %d", ErrInvalidFrame, hdr[4])
	}
	n := int64(binary.BigEndian.Uint32(hdr[8:12]))
	if n > maxBytes {
		return 0, nil, fmt.Errorf("%w: the payload of the frame is %d bytes, the limit is %d bytes", ErrInvalidFrame, n, maxBytes)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return MessageType(hdr[5]), payload, nil
}
