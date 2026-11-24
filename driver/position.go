package driver

import (
	"encoding/binary"
	"fmt"

	"github.com/cnlangzi/sqlite-relay/internal/wal"
)

// Position locates a point in the write-ahead log.
//
// Salt1 and Salt2 identify the generation the position belongs to. A WAL
// reset starts a new generation; in that case the frame number and the byte
// offset restart as well. The frame number is a cursor into the WAL of one
// generation and is not a global sequence number.
type Position struct {
	// Salt1 and Salt2 identify the WAL generation.
	Salt1 uint32
	Salt2 uint32
	// Frame is the number of frames committed in this generation.
	Frame uint64
	// WALOffset is the byte offset in the WAL file just past that frame.
	WALOffset int64
}

// positionLen is the length of the binary form of a Position.
const positionLen = 4 + 4 + 8 + 8

// MarshalBinary encodes the position.
func (p Position) MarshalBinary() ([]byte, error) {
	b := make([]byte, positionLen)
	binary.BigEndian.PutUint32(b[0:], p.Salt1)
	binary.BigEndian.PutUint32(b[4:], p.Salt2)
	binary.BigEndian.PutUint64(b[8:], p.Frame)
	binary.BigEndian.PutUint64(b[16:], uint64(p.WALOffset))
	return b, nil
}

// UnmarshalBinary decodes a position.
func (p *Position) UnmarshalBinary(b []byte) error {
	if len(b) != positionLen {
		return fmt.Errorf("driver: position is %d bytes, want %d", len(b), positionLen)
	}
	p.Salt1 = binary.BigEndian.Uint32(b[0:])
	p.Salt2 = binary.BigEndian.Uint32(b[4:])
	p.Frame = binary.BigEndian.Uint64(b[8:])
	p.WALOffset = int64(binary.BigEndian.Uint64(b[16:]))
	return nil
}

func (p Position) String() string {
	return fmt.Sprintf("%08x/%08x@%d+%d", p.Salt1, p.Salt2, p.Frame, p.WALOffset)
}

// sameGeneration reports whether two positions belong to the same WAL
// generation.
func (p Position) sameGeneration(o Position) bool {
	return p.Salt1 == o.Salt1 && p.Salt2 == o.Salt2
}

// walOffset is the byte offset in a WAL of the given page size just past the
// given number of committed frames.
func walOffset(frame uint64, pageSize int) int64 {
	return int64(wal.HeaderSize) + int64(frame)*(int64(wal.FrameHeaderSize)+int64(pageSize))
}
