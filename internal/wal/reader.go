// Package wal parses SQLite write-ahead log files.
//
// The package deliberately reads the WAL as raw bytes instead of asking
// SQLite for its state: the wal-index lives in shared memory in a private
// host specific format, and relying on it would hide exactly the corruption
// this parser exists to detect.
package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// ErrInvalidFrame reports a frame that fails validation. The scan stops at
// the first such frame.
var ErrInvalidFrame = errors.New("wal: invalid frame")

// Transaction is one committed transaction of the WAL.
type Transaction struct {
	// CommitFrame is the 1-based index of the frame that closed the
	// transaction.
	CommitFrame uint64
	// CommitPages is the size of the database in pages after the transaction.
	CommitPages uint32
	// Frames holds the page images of the transaction in WAL order.
	Frames []Frame
}

// Scan is the outcome of reading a WAL file.
type Scan struct {
	// Header is the header the scan started from.
	Header Header
	// Transactions holds the committed transactions found, in WAL order.
	Transactions []Transaction
	// CommitFrame is the index of the last valid commit frame.
	CommitFrame uint64
	// CommitPages is the size of the database in pages after CommitFrame.
	CommitPages uint32
	// Offset is the byte offset just past CommitFrame.
	Offset int64
	// Truncated reports that the scan stopped on an invalid frame rather
	// than at the end of the file.
	Truncated bool
}

// Reader reads committed transactions out of a WAL file.
//
// A Reader keeps the checksum chain of the frames it has read, so it can
// resume at an arbitrary frame without re-reading the whole file. A reader
// that is positioned at a frame it did not read itself has to be rewound
// first.
type Reader struct {
	file  *os.File
	hdr   Header
	order wordOrder
	seed  Checksum
	next  uint64
	off   int64

	// commitFrame is the last commit frame the reader has seen. The reader
	// always resumes there, so that a transaction is never split across two
	// scans and a torn tail frame is read again once it has been rewritten.
	commitFrame uint64
	commitOff   int64
	commitSeed  Checksum
}

// Open opens the WAL file at path and validates its header.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNoWAL
		}
		return nil, err
	}
	var head [HeaderSize]byte
	if _, err := io.ReadFull(f, head[:]); err != nil {
		f.Close()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrNoWAL
		}
		return nil, err
	}
	hdr, err := ParseHeader(head[:])
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Reader{
		file:  f,
		hdr:   hdr,
		order: hdr.wordOrder(),
		// The checksum of the first frame is seeded with the checksum the
		// WAL header carries.
		seed:       Checksum{S1: hdr.Checksum1, S2: hdr.Checksum2},
		next:       1,
		off:        int64(HeaderSize),
		commitOff:  int64(HeaderSize),
		commitSeed: Checksum{S1: hdr.Checksum1, S2: hdr.Checksum2},
	}, nil
}

// ReadHeader reads the header of the WAL file at path.
//
// The driver uses it to find out whether the WAL was reset while it was read:
// a reset writes a new header with a new salt and a new checkpoint sequence
// number.
func ReadHeader(path string) (Header, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Header{}, ErrNoWAL
		}
		return Header{}, err
	}
	defer f.Close()
	var head [HeaderSize]byte
	if _, err := io.ReadFull(f, head[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Header{}, ErrNoWAL
		}
		return Header{}, err
	}
	return ParseHeader(head[:])
}

// Close releases the underlying file.
func (r *Reader) Close() error {
	return r.file.Close()
}

// Header returns the WAL header.
func (r *Reader) Header() Header {
	return r.hdr
}

// Frame returns the index of the next frame to be read.
func (r *Reader) Frame() uint64 {
	return r.next
}

// Offset returns the byte offset of the next frame to be read.
func (r *Reader) Offset() int64 {
	return r.off
}

// Seed returns the checksum seed for the next frame to be read.
func (r *Reader) Seed() Checksum {
	return r.seed
}

// Rewind moves the reader back to the first frame.
func (r *Reader) Rewind() error {
	r.next = 1
	r.off = int64(HeaderSize)
	r.seed = Checksum{S1: r.hdr.Checksum1, S2: r.hdr.Checksum2}
	r.commitFrame = 0
	r.commitOff = int64(HeaderSize)
	r.commitSeed = r.seed
	return nil
}

// RewindTo moves the reader to the given 1-based frame, rebuilding the
// checksum chain by re-reading the WAL from the beginning.
//
// It is the way to resume at a position that was persisted by an earlier
// process: only the position itself survives a restart, not the checksum
// chain that validates the frames behind it.
func (r *Reader) RewindTo(frame uint64) error {
	if err := r.Rewind(); err != nil {
		return err
	}
	if frame <= 1 {
		return nil
	}
	for r.next < frame {
		if _, err := r.nextFrame(); err != nil {
			return fmt.Errorf("wal: cannot rewind to frame %d: %w", frame, err)
		}
	}
	return nil
}

// nextFrame reads and validates the frame at the current position. It
// returns io.EOF at the end of the file and ErrInvalidFrame for a frame that
// fails validation.
func (r *Reader) nextFrame() (*Frame, error) {
	var head [FrameHeaderSize]byte
	if _, err := io.ReadFull(r.file, head[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, io.EOF
		}
		return nil, err
	}
	f := parseFrameHeader(head[:])
	if f.Salt1 != r.hdr.Salt1 || f.Salt2 != r.hdr.Salt2 {
		return nil, fmt.Errorf("%w: salt mismatch at frame %d", ErrInvalidFrame, r.next)
	}
	if f.Pgno == 0 {
		return nil, fmt.Errorf("%w: page number 0 at frame %d", ErrInvalidFrame, r.next)
	}
	data := make([]byte, r.hdr.PageSize)
	if _, err := io.ReadFull(r.file, data); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, io.EOF
		}
		return nil, err
	}
	sum := r.seed.update(r.order, head[:8])
	sum = sum.update(r.order, data)
	if sum.S1 != f.Checksum1 || sum.S2 != f.Checksum2 {
		return nil, fmt.Errorf("%w: checksum mismatch at frame %d", ErrInvalidFrame, r.next)
	}
	f.Index = r.next
	f.Data = data
	r.seed = sum
	r.next++
	r.off += r.hdr.FrameSize()
	return &f, nil
}

// Scan reads the WAL from the current position and returns the committed
// transactions that follow.
//
// The scan stops at the first frame that fails validation. Frames that follow
// the last commit frame belong to a transaction that never committed and are
// dropped. The reader is left positioned just past the last commit frame, so
// that a later scan re-reads the uncommitted tail.
func (r *Reader) Scan() (*Scan, error) {
	scan := &Scan{Header: r.hdr}
	var pending []Frame

	for {
		f, err := r.nextFrame()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if !errors.Is(err, ErrInvalidFrame) {
				return nil, err
			}
			scan.Truncated = true
			break
		}
		pending = append(pending, *f)
		if !f.IsCommit() {
			continue
		}
		scan.Transactions = append(scan.Transactions, Transaction{
			CommitFrame: f.Index,
			CommitPages: f.CommitPages,
			Frames:      pending,
		})
		pending = nil
		r.commitFrame = f.Index
		r.commitOff = r.off
		r.commitSeed = r.seed
	}

	scan.CommitFrame = r.commitFrame
	if len(scan.Transactions) > 0 {
		scan.CommitPages = scan.Transactions[len(scan.Transactions)-1].CommitPages
	}
	scan.Offset = r.commitOff

	// Rewind to the last commit frame so that the uncommitted tail is read
	// again on the next scan.
	r.next = r.commitFrame + 1
	r.off = r.commitOff
	r.seed = r.commitSeed
	return scan, nil
}
