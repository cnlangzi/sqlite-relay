package wal

import "encoding/binary"

// Frame is a single WAL frame: a 24 byte header followed by one page image.
type Frame struct {
	// Index is the 1-based position of the frame in the WAL.
	Index uint64
	// Pgno is the database page the frame holds. It is never zero.
	Pgno uint32
	// CommitPages is the size of the database in pages after the transaction
	// this frame belongs to. A non zero value marks the commit frame of that
	// transaction.
	CommitPages uint32
	// Salt1 and Salt2 identify the generation the frame belongs to.
	Salt1 uint32
	Salt2 uint32
	// Checksum1 and Checksum2 are the checksum of this frame.
	Checksum1 uint32
	Checksum2 uint32
	// Data is the page image. It is exactly one page long.
	Data []byte
}

// IsCommit reports whether the frame closes a transaction.
func (f Frame) IsCommit() bool {
	return f.CommitPages != 0
}

// parseFrameHeader decodes the 24 byte frame header.
func parseFrameHeader(b []byte) Frame {
	return Frame{
		Pgno:        binary.BigEndian.Uint32(b[0:4]),
		CommitPages: binary.BigEndian.Uint32(b[4:8]),
		Salt1:       binary.BigEndian.Uint32(b[8:12]),
		Salt2:       binary.BigEndian.Uint32(b[12:16]),
		Checksum1:   binary.BigEndian.Uint32(b[16:20]),
		Checksum2:   binary.BigEndian.Uint32(b[20:24]),
	}
}
