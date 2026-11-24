package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// WAL header layout. Every field is big endian.
const (
	HeaderSize = 32
	// FrameHeaderSize is the size of the header that precedes each page image.
	FrameHeaderSize = 24

	// MagicLittleEndian selects little endian words for the checksum.
	MagicLittleEndian = 0x377f0682
	// MagicBigEndian selects big endian words for the checksum.
	MagicBigEndian = 0x377f0683

	// FormatVersion is the only WAL format version this package understands.
	FormatVersion = 3007000

	// MinPageSize and MaxPageSize bound the page size SQLite accepts.
	MinPageSize = 512
	MaxPageSize = 65536
)

// ErrNoWAL reports that the WAL file is missing or too short to hold a header.
var ErrNoWAL = errors.New("wal: no WAL file")

// ErrInvalidHeader reports a WAL header that is not a WAL header.
var ErrInvalidHeader = errors.New("wal: invalid header")

// Header is the 32 byte header at the start of a WAL file.
type Header struct {
	Magic         uint32
	Version       uint32
	PageSize      uint32
	CheckpointSeq uint32
	Salt1         uint32
	Salt2         uint32
	Checksum1     uint32
	Checksum2     uint32
}

// ParseHeader decodes and validates a WAL header.
//
// The magic number selects the byte order used for the checksum words: the
// low bit clear means little endian, the low bit set means big endian. That
// choice is independent of the host byte order.
func ParseHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, ErrNoWAL
	}
	h := Header{
		Magic:         binary.BigEndian.Uint32(b[0:4]),
		Version:       binary.BigEndian.Uint32(b[4:8]),
		PageSize:      binary.BigEndian.Uint32(b[8:12]),
		CheckpointSeq: binary.BigEndian.Uint32(b[12:16]),
		Salt1:         binary.BigEndian.Uint32(b[16:20]),
		Salt2:         binary.BigEndian.Uint32(b[20:24]),
		Checksum1:     binary.BigEndian.Uint32(b[24:28]),
		Checksum2:     binary.BigEndian.Uint32(b[28:32]),
	}
	if err := h.Validate(); err != nil {
		return Header{}, err
	}
	// The header checksum covers the first 24 bytes with a zero seed.
	sum := Checksum{}.update(h.wordOrder(), b[:HeaderSize-2*4])
	if sum.S1 != h.Checksum1 || sum.S2 != h.Checksum2 {
		return Header{}, fmt.Errorf("%w: header checksum mismatch", ErrInvalidHeader)
	}
	return h, nil
}

// Validate reports whether the header describes a WAL this package can read.
func (h Header) Validate() error {
	if h.Magic != MagicLittleEndian && h.Magic != MagicBigEndian {
		return fmt.Errorf("%w: magic 0x%08x", ErrInvalidHeader, h.Magic)
	}
	if h.Version != FormatVersion {
		return fmt.Errorf("%w: format version %d", ErrInvalidHeader, h.Version)
	}
	if h.PageSize < MinPageSize || h.PageSize > MaxPageSize || h.PageSize&(h.PageSize-1) != 0 {
		return fmt.Errorf("%w: page size %d", ErrInvalidHeader, h.PageSize)
	}
	return nil
}

// wordOrder reports how the checksum words of this WAL are read.
func (h Header) wordOrder() wordOrder {
	if h.Magic == MagicBigEndian {
		return bigEndianWords
	}
	return littleEndianWords
}

// FrameSize is the size of a frame header plus its page image.
func (h Header) FrameSize() int64 {
	return int64(FrameHeaderSize) + int64(h.PageSize)
}
