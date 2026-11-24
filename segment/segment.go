// Package segment encodes and decodes the segments of a spool.
//
// A segment is an immutable file that holds a range of the transactions of
// one database. The layout is fixed:
//
//	[0, 64)               header
//	[64, 64+payload)      transaction records
//	[64+payload, +64)     trailer
//
// Every field is big endian and every checksum is a CRC32C (Castagnoli).
// The header names the database the segment belongs to, the epoch of the
// chain it is part of and the first sequence number it holds. The trailer
// closes the segment: it names the last sequence number, the number of the
// transactions, the size of the payload, the checksum of the payload and the
// position in the write-ahead log the last transaction was committed at.
//
// A segment is written once and is never changed afterwards.
package segment

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"time"
)

// Layout of a segment file.
const (
	// HeaderSize is the size of the header of a segment.
	HeaderSize = 64
	// TrailerSize is the size of the trailer of a segment.
	TrailerSize = 64
	// Magic identifies a segment file.
	Magic = "SEG1"
	// Version is the format version this package reads and writes.
	Version = 1
	// MaxBytes is the size a segment may not grow beyond. A segment holds
	// the increments of a database, not a copy of it.
	MaxBytes = 1 << 30
	// MinPageSize and MaxPageSize bound the page size a segment may name.
	MinPageSize = 512
	MaxPageSize = 65536
)

// Errors of the package.
var (
	// ErrInvalid reports a file that is not a segment or that does not
	// hold what its header and its trailer say.
	ErrInvalid = errors.New("segment: invalid segment")
	// ErrShort reports a file that is too short to hold a header or a
	// trailer.
	ErrShort = errors.New("segment: the file is too short")
)

// castagnoli is the table of the checksums of a segment.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Pos is a position in the write-ahead log of the primary database.
type Pos struct {
	// Salt1 and Salt2 identify the generation of the log.
	Salt1 uint32
	Salt2 uint32
	// Frame is the number of frames that are committed in the generation.
	Frame uint64
	// WALOffset is the byte offset in the log just past that frame.
	WALOffset int64
}

// Page is one page image of a transaction.
type Page struct {
	// Pgno is the number of the page, starting at one.
	Pgno uint32
	// Data is the page image. It is exactly one page long.
	Data []byte
}

// Tx is one committed transaction.
type Tx struct {
	// Seq is the sequence number of the transaction.
	Seq uint64
	// CommitPages is the size of the database in pages after the commit.
	CommitPages uint32
	// Pages holds the pages the transaction changed.
	Pages []Page
}

// Header is the header of a segment.
type Header struct {
	// Version is the format version of the segment.
	Version uint32
	// Flags is reserved for future use and is zero.
	Flags uint32
	// HeaderCRC is the checksum of the first twelve bytes of the header.
	HeaderCRC uint32
	// DatabaseID names the database the segment belongs to.
	DatabaseID [16]byte
	// Epoch is the generation of the chain the segment is part of.
	Epoch uint64
	// SeqStart is the sequence number of the first transaction of the
	// segment.
	SeqStart uint64
	// PageSize is the size of the pages of the database.
	PageSize uint32
	// Reserved is reserved for future use and is zero.
	Reserved uint32
	// CreatedAt is the time the segment was written, in milliseconds since
	// the epoch.
	CreatedAt int64
}

// Trailer is the trailer of a segment.
type Trailer struct {
	// SeqEnd is the sequence number of the last transaction of the
	// segment.
	SeqEnd uint64
	// TxCount is the number of transactions of the segment.
	TxCount uint64
	// PageCount is the number of pages of the segment.
	PageCount uint64
	// PayloadBytes is the size of the payload of the segment.
	PayloadBytes uint64
	// PayloadCRC is the checksum of the payload of the segment.
	PayloadCRC uint32
	// Salt1 and Salt2 identify the generation of the write-ahead log the
	// last transaction was committed in.
	Salt1 uint32
	Salt2 uint32
	// WALFrame is the number of frames that are committed in that
	// generation.
	WALFrame uint64
	// WALOffset is the byte offset in the log just past that frame.
	WALOffset int64
	// TrailerCRC is the checksum of the first sixty bytes of the trailer.
	TrailerCRC uint32
}

// Pos returns the position in the write-ahead log of the last transaction
// of the segment.
func (t Trailer) Pos() Pos {
	return Pos{
		Salt1:     t.Salt1,
		Salt2:     t.Salt2,
		Frame:     t.WALFrame,
		WALOffset: t.WALOffset,
	}
}

// CreatedAtTime returns the time the segment was written.
func (h Header) CreatedAtTime() time.Time {
	return time.UnixMilli(h.CreatedAt)
}

// DatabaseID returns the identifier of a database.
//
// The identifier is the first sixteen bytes of the SHA-256 of the name of
// the database. It names the database a segment belongs to.
func DatabaseID(name string) [16]byte {
	sum := sha256.Sum256([]byte(name))
	var id [16]byte
	copy(id[:], sum[:16])
	return id
}

// EncodeHeader encodes the header.
//
// The checksum of the header is computed, the HeaderCRC field of the value
// is ignored.
func EncodeHeader(h Header) []byte {
	b := make([]byte, HeaderSize)
	copy(b[0:4], Magic)
	binary.BigEndian.PutUint32(b[4:8], h.Version)
	binary.BigEndian.PutUint32(b[8:12], h.Flags)
	copy(b[16:32], h.DatabaseID[:])
	binary.BigEndian.PutUint64(b[32:40], h.Epoch)
	binary.BigEndian.PutUint64(b[40:48], h.SeqStart)
	binary.BigEndian.PutUint32(b[48:52], h.PageSize)
	binary.BigEndian.PutUint32(b[52:56], h.Reserved)
	binary.BigEndian.PutUint64(b[56:64], uint64(h.CreatedAt))
	binary.BigEndian.PutUint32(b[12:16], crc32.Checksum(b[0:12], castagnoli))
	return b
}

// ParseHeader decodes the header and checks the magic, the version and the
// checksum of the header.
func ParseHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, fmt.Errorf("%w: the header is %d bytes", ErrShort, len(b))
	}
	h := Header{
		Version:   binary.BigEndian.Uint32(b[4:8]),
		Flags:     binary.BigEndian.Uint32(b[8:12]),
		HeaderCRC: binary.BigEndian.Uint32(b[12:16]),
		Epoch:     binary.BigEndian.Uint64(b[32:40]),
		SeqStart:  binary.BigEndian.Uint64(b[40:48]),
		PageSize:  binary.BigEndian.Uint32(b[48:52]),
		Reserved:  binary.BigEndian.Uint32(b[52:56]),
		CreatedAt: int64(binary.BigEndian.Uint64(b[56:64])),
	}
	copy(h.DatabaseID[:], b[16:32])
	if string(b[0:4]) != Magic {
		return h, fmt.Errorf("%w: the magic is %q", ErrInvalid, b[0:4])
	}
	if h.Version != Version {
		return h, fmt.Errorf("%w: the version is %d", ErrInvalid, h.Version)
	}
	if h.PageSize < MinPageSize || h.PageSize > MaxPageSize || h.PageSize&(h.PageSize-1) != 0 {
		return h, fmt.Errorf("%w: the page size is %d", ErrInvalid, h.PageSize)
	}
	if crc32.Checksum(b[0:12], castagnoli) != h.HeaderCRC {
		return h, fmt.Errorf("%w: the checksum of the header does not match", ErrInvalid)
	}
	return h, nil
}

// EncodeTrailer encodes the trailer.
//
// The checksum of the trailer is computed, the TrailerCRC field of the value
// is ignored.
func EncodeTrailer(t Trailer) []byte {
	b := make([]byte, TrailerSize)
	binary.BigEndian.PutUint64(b[0:8], t.SeqEnd)
	binary.BigEndian.PutUint64(b[8:16], t.TxCount)
	binary.BigEndian.PutUint64(b[16:24], t.PageCount)
	binary.BigEndian.PutUint64(b[24:32], t.PayloadBytes)
	binary.BigEndian.PutUint32(b[32:36], t.PayloadCRC)
	binary.BigEndian.PutUint32(b[36:40], t.Salt1)
	binary.BigEndian.PutUint32(b[40:44], t.Salt2)
	binary.BigEndian.PutUint64(b[44:52], t.WALFrame)
	binary.BigEndian.PutUint64(b[52:60], uint64(t.WALOffset))
	binary.BigEndian.PutUint32(b[60:64], crc32.Checksum(b[0:60], castagnoli))
	return b
}

// ParseTrailer decodes the trailer and checks its checksum.
func ParseTrailer(b []byte) (Trailer, error) {
	if len(b) < TrailerSize {
		return Trailer{}, fmt.Errorf("%w: the trailer is %d bytes", ErrShort, len(b))
	}
	t := Trailer{
		SeqEnd:       binary.BigEndian.Uint64(b[0:8]),
		TxCount:      binary.BigEndian.Uint64(b[8:16]),
		PageCount:    binary.BigEndian.Uint64(b[16:24]),
		PayloadBytes: binary.BigEndian.Uint64(b[24:32]),
		PayloadCRC:   binary.BigEndian.Uint32(b[32:36]),
		Salt1:        binary.BigEndian.Uint32(b[36:40]),
		Salt2:        binary.BigEndian.Uint32(b[40:44]),
		WALFrame:     binary.BigEndian.Uint64(b[44:52]),
		WALOffset:    int64(binary.BigEndian.Uint64(b[52:60])),
		TrailerCRC:   binary.BigEndian.Uint32(b[60:64]),
	}
	if crc32.Checksum(b[0:60], castagnoli) != t.TrailerCRC {
		return t, fmt.Errorf("%w: the checksum of the trailer does not match", ErrInvalid)
	}
	return t, nil
}

// Writer writes the transactions of a segment.
//
// The writer writes the header when it is created and the trailer when it
// is closed. A segment that was not closed has no trailer and cannot be
// read.
type Writer struct {
	w   io.Writer
	crc hash.Hash32
	hdr Header

	payloadBytes uint64
	txCount      uint64
	pageCount    uint64
	lastSeq      uint64
	closed       bool
}

// NewWriter writes the header of a segment into w.
func NewWriter(w io.Writer, h Header) (*Writer, error) {
	if h.Version == 0 {
		h.Version = Version
	}
	if _, err := w.Write(EncodeHeader(h)); err != nil {
		return nil, fmt.Errorf("segment: write the header: %w", err)
	}
	return &Writer{w: w, crc: crc32.New(castagnoli), hdr: h}, nil
}

// WriteTx appends a transaction to the segment.
//
// The sequence numbers of the transactions have to increase and the first
// one has to be the sequence number the header names.
func (wr *Writer) WriteTx(tx Tx) error {
	if wr.closed {
		return errors.New("segment: the writer is closed")
	}
	if tx.Seq <= wr.lastSeq {
		return fmt.Errorf("segment: the sequence number %d does not follow %d", tx.Seq, wr.lastSeq)
	}
	if wr.txCount == 0 && tx.Seq != wr.hdr.SeqStart {
		return fmt.Errorf("segment: the segment starts at %d, the transaction is %d", wr.hdr.SeqStart, tx.Seq)
	}
	rec, err := encodeTx(tx, wr.hdr.PageSize)
	if err != nil {
		return err
	}
	if _, err := wr.w.Write(rec); err != nil {
		return fmt.Errorf("segment: write a transaction: %w", err)
	}
	wr.crc.Write(rec)
	wr.payloadBytes += uint64(len(rec))
	wr.txCount++
	wr.pageCount += uint64(len(tx.Pages))
	wr.lastSeq = tx.Seq
	return nil
}

// Close writes the trailer of the segment and returns it.
//
// The position is the position in the write-ahead log the last transaction
// of the segment was committed at.
func (wr *Writer) Close(pos Pos) (Trailer, error) {
	if wr.closed {
		return Trailer{}, errors.New("segment: the writer is closed")
	}
	if wr.txCount == 0 {
		return Trailer{}, fmt.Errorf("%w: the segment holds no transaction", ErrInvalid)
	}
	t := Trailer{
		SeqEnd:       wr.lastSeq,
		TxCount:      wr.txCount,
		PageCount:    wr.pageCount,
		PayloadBytes: wr.payloadBytes,
		PayloadCRC:   wr.crc.Sum32(),
		Salt1:        pos.Salt1,
		Salt2:        pos.Salt2,
		WALFrame:     pos.Frame,
		WALOffset:    pos.WALOffset,
	}
	if _, err := wr.w.Write(EncodeTrailer(t)); err != nil {
		return Trailer{}, fmt.Errorf("segment: write the trailer: %w", err)
	}
	wr.closed = true
	return t, nil
}

// encodeTx encodes the record of a transaction.
func encodeTx(tx Tx, pageSize uint32) ([]byte, error) {
	size := 16
	for _, p := range tx.Pages {
		if uint32(len(p.Data)) != pageSize {
			return nil, fmt.Errorf("segment: the page %d holds %d bytes, the page size is %d", p.Pgno, len(p.Data), pageSize)
		}
		size += 4 + len(p.Data)
	}
	b := make([]byte, 0, size)
	b = binary.BigEndian.AppendUint64(b, tx.Seq)
	b = binary.BigEndian.AppendUint32(b, tx.CommitPages)
	b = binary.BigEndian.AppendUint32(b, uint32(len(tx.Pages)))
	for _, p := range tx.Pages {
		b = binary.BigEndian.AppendUint32(b, p.Pgno)
		b = append(b, p.Data...)
	}
	return b, nil
}

// Segment is a segment file that was read.
type Segment struct {
	// Header is the header of the segment.
	Header Header
	// Trailer is the trailer of the segment.
	Trailer Trailer
	// Txs holds the transactions of the segment in the order they were
	// written.
	Txs []Tx
}

// Read reads a segment from r.
func Read(r io.Reader) (*Segment, error) {
	hb := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, hb); err != nil {
		return nil, fmt.Errorf("%w: read the header: %v", ErrShort, err)
	}
	h, err := ParseHeader(hb)
	if err != nil {
		return nil, err
	}
	rest, err := io.ReadAll(io.LimitReader(r, MaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("segment: read: %w", err)
	}
	if len(rest) > MaxBytes {
		return nil, fmt.Errorf("%w: the segment is bigger than %d bytes", ErrInvalid, MaxBytes)
	}
	if len(rest) < TrailerSize {
		return nil, fmt.Errorf("%w: %d bytes follow the header", ErrShort, len(rest))
	}
	t, err := ParseTrailer(rest[len(rest)-TrailerSize:])
	if err != nil {
		return nil, err
	}
	payload := rest[:len(rest)-TrailerSize]
	if t.PayloadBytes != uint64(len(payload)) {
		return nil, fmt.Errorf("%w: the payload is %d bytes, the trailer says %d", ErrInvalid, len(payload), t.PayloadBytes)
	}
	if crc32.Checksum(payload, castagnoli) != t.PayloadCRC {
		return nil, fmt.Errorf("%w: the checksum of the payload does not match", ErrInvalid)
	}
	txs, err := decodePayload(payload, h, t)
	if err != nil {
		return nil, err
	}
	return &Segment{Header: h, Trailer: t, Txs: txs}, nil
}

// ReadFile reads the segment file at path.
func ReadFile(path string) (*Segment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Read(f)
}

// ReadTrailer reads the trailer of the segment file at path.
//
// The segment itself is not read, only the trailer that closes it.
func ReadTrailer(path string) (Trailer, error) {
	f, err := os.Open(path)
	if err != nil {
		return Trailer{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return Trailer{}, err
	}
	if info.Size() < int64(HeaderSize+TrailerSize) {
		return Trailer{}, fmt.Errorf("%w: %s is %d bytes", ErrShort, path, info.Size())
	}
	b := make([]byte, TrailerSize)
	if _, err := f.ReadAt(b, info.Size()-TrailerSize); err != nil {
		return Trailer{}, err
	}
	return ParseTrailer(b)
}

// decodePayload decodes the transaction records of a payload.
func decodePayload(payload []byte, h Header, t Trailer) ([]Tx, error) {
	if t.TxCount == 0 {
		return nil, fmt.Errorf("%w: the segment holds no transaction", ErrInvalid)
	}
	if t.TxCount > uint64(len(payload)/16) {
		return nil, fmt.Errorf("%w: the segment holds %d transactions, the payload holds %d bytes", ErrInvalid, t.TxCount, len(payload))
	}
	txs := make([]Tx, 0, t.TxCount)
	var pages uint64
	off := 0
	for len(txs) < int(t.TxCount) {
		if off+16 > len(payload) {
			return nil, fmt.Errorf("%w: the payload ends in the middle of a transaction", ErrInvalid)
		}
		tx := Tx{
			Seq:         binary.BigEndian.Uint64(payload[off:]),
			CommitPages: binary.BigEndian.Uint32(payload[off+8:]),
		}
		count := binary.BigEndian.Uint32(payload[off+12:])
		off += 16
		for i := uint32(0); i < count; i++ {
			if off+4 > len(payload) {
				return nil, fmt.Errorf("%w: the payload ends in the middle of a page", ErrInvalid)
			}
			pgno := binary.BigEndian.Uint32(payload[off:])
			off += 4
			if off+int(h.PageSize) > len(payload) {
				return nil, fmt.Errorf("%w: the payload ends in the middle of a page image", ErrInvalid)
			}
			data := make([]byte, h.PageSize)
			copy(data, payload[off:off+int(h.PageSize)])
			off += int(h.PageSize)
			tx.Pages = append(tx.Pages, Page{Pgno: pgno, Data: data})
			pages++
		}
		if len(txs) > 0 && tx.Seq <= txs[len(txs)-1].Seq {
			return nil, fmt.Errorf("%w: the sequence number %d does not follow the previous one", ErrInvalid, tx.Seq)
		}
		txs = append(txs, tx)
	}
	if off != len(payload) {
		return nil, fmt.Errorf("%w: the payload holds %d bytes too many", ErrInvalid, len(payload)-off)
	}
	if pages != t.PageCount {
		return nil, fmt.Errorf("%w: the payload holds %d pages, the trailer says %d", ErrInvalid, pages, t.PageCount)
	}
	if txs[0].Seq != h.SeqStart {
		return nil, fmt.Errorf("%w: the segment starts at %d, the header says %d", ErrInvalid, txs[0].Seq, h.SeqStart)
	}
	if txs[len(txs)-1].Seq != t.SeqEnd {
		return nil, fmt.Errorf("%w: the segment ends at %d, the trailer says %d", ErrInvalid, txs[len(txs)-1].Seq, t.SeqEnd)
	}
	return txs, nil
}
