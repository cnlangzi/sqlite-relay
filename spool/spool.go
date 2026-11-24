// Package spool is a durable file queue for the increments of one database.
//
// The spool sits between the driver that captures the increments of a primary
// database and the transport that carries them to a backup. It writes the
// increments into immutable segment files, publishes the segments that
// passed the persistence barrier of the configured mode and holds the
// durable capture acknowledgement, so that the driver may write the frames
// of the write-ahead log back into the database.
//
// The spool is passive. It has no goroutine of its own, it times nothing and
// it does not talk to the network. The caller drives it with Append, Flush
// and Sync, and it decides when a segment is closed and when the barrier
// runs.
package spool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cnlangzi/sqlite-relay/segment"
)

// Mode is the persistence barrier of the spool.
//
// The names follow the ones of the synchronous pragma of SQLite, but the
// unit of the barrier is a segment or a batch of segments and not a
// transaction of the primary database.
type Mode int

const (
	// ModeOff publishes a segment as soon as it is written. The barrier is
	// the write of the operating system, a power failure may lose the
	// segments at the end of the queue.
	ModeOff Mode = iota
	// ModeNormal publishes a batch of segments at once. The barrier is a
	// sync of the files of the batch and of the directory that holds them.
	ModeNormal
	// ModeFull publishes every segment on its own. The barrier is a sync of
	// the file of the segment and of the directory that holds it.
	ModeFull
)

// String returns the name of the mode.
func (m Mode) String() string {
	switch m {
	case ModeOff:
		return "OFF"
	case ModeNormal:
		return "NORMAL"
	case ModeFull:
		return "FULL"
	}
	return fmt.Sprintf("Mode(%d)", int(m))
}

// syncs reports whether the mode syncs files and directories.
func (m Mode) syncs() bool {
	return m != ModeOff
}

// Errors of the package.
var (
	// ErrSegmentFull reports that the segment that is open is full. The
	// transaction was not written. The caller closes the segment and
	// retries the transaction.
	ErrSegmentFull = errors.New("spool: the segment is full")
	// ErrSpoolFull reports that the spool reached its size limit. The
	// transaction was not written.
	ErrSpoolFull = errors.New("spool: the spool is full")
	// ErrChainBroken reports that the chain of the segments has a hole or
	// a segment that does not hold what it says. The data of the hole is
	// gone, only a reset and a new snapshot bring the spool back.
	ErrChainBroken = errors.New("spool: the chain of the segments is broken")
	// ErrDatabaseMismatch reports that the directory belongs to another
	// database.
	ErrDatabaseMismatch = errors.New("spool: the directory belongs to another database")
	// errClosed reports that the spool was closed.
	errClosed = errors.New("spool: the spool is closed")
)

// Defaults of the configuration.
const (
	defaultSyncInterval    = time.Second
	defaultSyncBytes       = 4 << 20
	defaultSegmentMaxBytes = 8 << 20
	defaultPeerID          = "backup"
)

// Config configures a Spool.
type Config struct {
	// Root is the directory the spools of the databases live in.
	Root string
	// DatabaseID is the name of the database the spool belongs to. It is
	// the name of the directory of the spool and the identity of the
	// database the segments belong to.
	DatabaseID string
	// Mode is the persistence barrier of the spool. The default is
	// ModeOff.
	Mode Mode
	// SyncInterval is the shortest interval between two barriers.
	SyncInterval time.Duration
	// SyncBytes is the number of bytes that have to be written between two
	// barriers.
	SyncBytes int64
	// SegmentMaxBytes is the size a segment may not grow beyond.
	SegmentMaxBytes int64
	// MaxBytes is the size the spool may not grow beyond. Zero means no
	// limit.
	MaxBytes int64
	// WarnBytes is the size the spool warns about. Zero means no warning.
	WarnBytes int64
	// Retention is the time a segment is kept after the backup
	// acknowledged it.
	Retention time.Duration
	// PeerID is the name of the peer the backup acknowledgement belongs
	// to. The default is "backup".
	PeerID string
}

// withDefaults returns the configuration with the defaults filled in.
func (c Config) withDefaults() Config {
	if c.SyncInterval <= 0 {
		c.SyncInterval = defaultSyncInterval
	}
	if c.SyncBytes <= 0 {
		c.SyncBytes = defaultSyncBytes
	}
	if c.SegmentMaxBytes <= 0 {
		c.SegmentMaxBytes = defaultSegmentMaxBytes
	}
	if c.PeerID == "" {
		c.PeerID = defaultPeerID
	}
	return c
}

// Page is one page image of a transaction.
type Page struct {
	// Pgno is the number of the page, starting at one.
	Pgno uint32
	// Data is the page image. It is exactly one page long.
	Data []byte
}

// Pos is a position in the write-ahead log of the primary database.
//
// It has the same shape as the position of the driver, but the spool does
// not know the driver.
type Pos struct {
	// Salt1 and Salt2 identify the generation of the log.
	Salt1 uint32
	Salt2 uint32
	// Frame is the number of frames that are committed in the generation.
	Frame uint64
	// WALOffset is the byte offset in the log just past that frame.
	WALOffset int64
}

// Tx is one committed transaction.
type Tx struct {
	// Pos is the position in the write-ahead log the transaction was
	// committed at.
	Pos Pos
	// CommitPages is the size of the database in pages after the commit.
	CommitPages uint32
	// Pages holds the pages the transaction changed.
	Pages []Page
}

// Ack is the durable capture acknowledgement of the spool.
type Ack struct {
	// Seq is the sequence number of the last transaction that is durable.
	Seq uint64
	// Pos is the position in the write-ahead log that transaction was
	// committed at.
	Pos Pos
}

// SegmentRef describes a published segment.
type SegmentRef struct {
	// Path is the file of the segment.
	Path string
	// SeqStart is the sequence number of the first transaction of the
	// segment.
	SeqStart uint64
	// SeqEnd is the sequence number of the last transaction of the
	// segment.
	SeqEnd uint64
	// Epoch is the generation of the chain the segment is part of.
	Epoch uint64
	// Bytes is the size of the file of the segment.
	Bytes int64
	// CreatedAt is the time the segment was written.
	CreatedAt time.Time
}

// Stats is a snapshot of the state of the spool.
type Stats struct {
	// Mode is the persistence barrier of the spool.
	Mode Mode
	// Epoch is the generation of the chain of the spool.
	Epoch uint64
	// Segments is the number of the published segments.
	Segments int
	// Bytes is the number of bytes the spool holds on the disk.
	Bytes int64
	// DurableSeq is the sequence number of the durable capture
	// acknowledgement.
	DurableSeq uint64
	// BackupAckSeq is the sequence number the backup acknowledged.
	BackupAckSeq uint64
	// UnackedSegments is the number of the published segments the backup
	// did not acknowledge.
	UnackedSegments int
	// UnackedBytes is the number of bytes those segments hold.
	UnackedBytes int64
	// UnackedAge is the age of the oldest segment the backup did not
	// acknowledge.
	UnackedAge time.Duration
	// Warn reports that the spool holds more bytes than it should.
	Warn bool
	// DiskFreeBytes is the number of bytes that are left on the file
	// system of the spool.
	DiskFreeBytes int64
	// Broken reports that the chain of the segments has a hole.
	Broken bool
	// BrokenReason explains why the chain is broken.
	BrokenReason string
}

// Pending describes the work the spool holds.
type Pending struct {
	// Segments is the number of the segments that are not published yet.
	Segments int
	// Bytes is the number of bytes those segments hold.
	Bytes int64
	// OpenBytes is the number of bytes the segment that is open holds.
	OpenBytes int64
	// Age is the age of the oldest segment that is not published yet.
	Age time.Duration
	// Due reports whether the thresholds of the configuration say that the
	// peer should sync.
	Due bool
}

// GCResult describes what a garbage collection removed.
type GCResult struct {
	// Segments is the number of the segments that were removed.
	Segments int
	// Bytes is the number of bytes those segments held.
	Bytes int64
}

// Spool is a durable queue of the increments of one database.
type Spool struct {
	cfg Config

	dir      string
	segDir   string
	tmpDir   string
	stateDir string
	peerDir  string

	mu sync.Mutex

	dbID      [16]byte
	createdAt time.Time
	epoch     uint64
	nextSeq   uint64
	pageSize  uint32
	durable   Ack
	segs      []SegmentRef
	pending   []*pendingSegment
	open      *openSegment
	broken    bool
	reason    string
	closed    bool

	// lastPublished is the segment that was published last. The
	// acknowledgement moves to it when the barrier succeeded.
	lastPublished *pendingSegment
	// barrierPending reports that the barrier of the last publish did not
	// finish. The next barrier runs it again.
	barrierPending bool

	lastBarrier       time.Time
	bytesSinceBarrier int64

	// syncFile and syncDir are the seam through which the spool syncs
	// files and directories. The production implementation calls fsync, a
	// test replaces it to count the calls.
	syncFile func(path string, f *os.File) error
	syncDir  func(path string) error
}

// openSegment is the segment that is written.
type openSegment struct {
	path      string
	file      *os.File
	writer    *segment.Writer
	seqStart  uint64
	lastPos   Pos
	trailer   segment.Trailer
	bytes     int64
	createdAt time.Time
}

// pendingSegment is a segment that waits for the barrier.
type pendingSegment struct {
	path      string
	seqStart  uint64
	bytes     int64
	createdAt time.Time
	trailer   segment.Trailer
}

// Open opens the spool of a database.
//
// The directory of the spool is created when it does not exist. The segments
// that are in it are read and checked, the temporary files are removed: a
// temporary file was never published and its data is captured from the
// write-ahead log again.
//
// A chain that is broken is reported as an error. The spool that comes with
// the error may be used to look at the state and to reset the spool, nothing
// else.
func Open(ctx context.Context, cfg Config) (*Spool, error) {
	cfg = cfg.withDefaults()
	if cfg.Root == "" {
		return nil, errors.New("spool: the root directory is empty")
	}
	if cfg.DatabaseID == "" {
		return nil, errors.New("spool: the database ID is empty")
	}
	if err := validName("the database ID", cfg.DatabaseID); err != nil {
		return nil, err
	}
	if err := validName("the peer ID", cfg.PeerID); err != nil {
		return nil, err
	}
	switch cfg.Mode {
	case ModeOff, ModeNormal, ModeFull:
	default:
		return nil, fmt.Errorf("spool: unknown mode %d", cfg.Mode)
	}

	s := &Spool{
		cfg:      cfg,
		syncFile: defaultSyncFile,
		syncDir:  defaultSyncDir,
	}
	s.dir = filepath.Join(cfg.Root, cfg.DatabaseID)
	s.segDir = filepath.Join(s.dir, "segments")
	s.tmpDir = filepath.Join(s.dir, "tmp")
	s.stateDir = filepath.Join(s.dir, "state")
	s.peerDir = filepath.Join(s.dir, "peers")
	if err := s.makeDirs(); err != nil {
		return nil, err
	}
	if err := s.recover(); err != nil {
		if errors.Is(err, ErrChainBroken) {
			return s, err
		}
		return nil, err
	}
	return s, nil
}

// validName checks that a name may be used as the name of a file or of a
// directory.
func validName(kind, name string) error {
	if name == "" || name == "." || name == ".." {
		return fmt.Errorf("spool: %s %q is not a valid name", kind, name)
	}
	if strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("spool: %s %q holds a path separator", kind, name)
	}
	return nil
}

// makeDirs creates the directories of the spool.
func (s *Spool) makeDirs() error {
	for _, dir := range []string{s.dir, s.segDir, s.tmpDir, s.stateDir, s.peerDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("spool: create %s: %w", dir, err)
		}
	}
	if s.cfg.Mode.syncs() {
		if err := s.syncDir(s.dir); err != nil {
			return fmt.Errorf("spool: sync %s: %w", s.dir, err)
		}
	}
	return nil
}

// Close releases the file of the segment that is open.
//
// The segment is not published, the data of the transactions that were
// written into it is captured from the write-ahead log again. The spool
// cannot be used any more.
func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.open == nil {
		return nil
	}
	err := s.open.file.Close()
	_ = os.Remove(s.open.path)
	s.open = nil
	return err
}

// Epoch returns the generation of the chain of the spool.
func (s *Spool) Epoch() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch
}

// DatabaseID returns the name of the database the spool belongs to.
func (s *Spool) DatabaseID() string {
	return s.cfg.DatabaseID
}

// Append writes a transaction into the segment that is open.
//
// The transaction gets the next sequence number of the spool. The sequence
// numbers survive a restart and they do not belong to the driver.
//
// The transaction is not written when the segment is full (ErrSegmentFull)
// or when the spool reached its size limit (ErrSpoolFull). The caller closes
// the segment and retries the transaction in the first case.
func (s *Spool) Append(ctx context.Context, tx Tx) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken {
		return s.brokenError()
	}
	if s.closed {
		return errClosed
	}

	size := txSize(tx)
	if s.cfg.MaxBytes > 0 && s.totalBytes()+size > s.cfg.MaxBytes {
		return fmt.Errorf("%w: %d bytes are in use, the limit is %d bytes", ErrSpoolFull, s.totalBytes(), s.cfg.MaxBytes)
	}
	if s.open != nil && s.open.bytes+size > s.cfg.SegmentMaxBytes {
		return fmt.Errorf("%w: the segment holds %d bytes, the limit is %d bytes", ErrSegmentFull, s.open.bytes, s.cfg.SegmentMaxBytes)
	}
	if err := s.checkPageSize(tx); err != nil {
		return err
	}
	if s.open == nil {
		if err := s.openSegment(); err != nil {
			return err
		}
	}

	rec := segment.Tx{Seq: s.nextSeq, CommitPages: tx.CommitPages}
	for _, p := range tx.Pages {
		rec.Pages = append(rec.Pages, segment.Page{Pgno: p.Pgno, Data: p.Data})
	}
	if err := s.open.writer.WriteTx(rec); err != nil {
		// The segment holds half a transaction, it is not published.
		s.discardOpen()
		return fmt.Errorf("spool: write the transaction: %w", err)
	}
	s.open.lastPos = tx.Pos
	s.open.bytes += size
	s.nextSeq++
	s.bytesSinceBarrier += size
	return nil
}

// txSize returns the number of bytes a transaction takes in a segment.
func txSize(tx Tx) int64 {
	size := int64(16)
	for _, p := range tx.Pages {
		size += int64(4 + len(p.Data))
	}
	return size
}

// checkPageSize learns the page size of the database from the pages of a
// transaction and checks that the transaction holds pages of that size.
func (s *Spool) checkPageSize(tx Tx) error {
	if len(tx.Pages) == 0 {
		return nil
	}
	size := uint32(len(tx.Pages[0].Data))
	for _, p := range tx.Pages {
		if uint32(len(p.Data)) != size {
			return fmt.Errorf("spool: the pages of a transaction have different sizes")
		}
	}
	if s.pageSize == 0 {
		s.pageSize = size
		return nil
	}
	if s.pageSize != size {
		return fmt.Errorf("spool: the page size of the database is %d, the transaction holds pages of %d bytes", s.pageSize, size)
	}
	return nil
}

// openSegment opens the segment that the next transactions are written into.
func (s *Spool) openSegment() error {
	if s.pageSize == 0 {
		return errors.New("spool: the page size of the database is unknown, a transaction without pages does not tell it")
	}
	path := filepath.Join(s.tmpDir, tmpName(s.nextSeq))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if os.IsExist(err) {
		// A segment that was not published before holds data that is
		// captured again, it may not be mistaken for the new one.
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("spool: remove %s: %w", path, err)
		}
		file, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	}
	if err != nil {
		return fmt.Errorf("spool: create %s: %w", path, err)
	}
	h := segment.Header{
		Version:    segment.Version,
		DatabaseID: segment.DatabaseID(s.cfg.DatabaseID),
		Epoch:      s.epoch,
		SeqStart:   s.nextSeq,
		PageSize:   s.pageSize,
		CreatedAt:  time.Now().UnixMilli(),
	}
	writer, err := segment.NewWriter(file, h)
	if err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return fmt.Errorf("spool: write the header of the segment: %w", err)
	}
	s.open = &openSegment{
		path:      path,
		file:      file,
		writer:    writer,
		seqStart:  s.nextSeq,
		bytes:     segment.HeaderSize,
		createdAt: time.Now(),
	}
	return nil
}

// discardOpen throws the segment that is open away.
//
// The segment holds no complete transaction, it is not published and the
// data of it is captured again.
func (s *Spool) discardOpen() {
	if s.open == nil {
		return
	}
	_ = s.open.file.Close()
	_ = os.Remove(s.open.path)
	s.open = nil
}

// Flush closes the segment that is open.
//
// In ModeOff and ModeFull the segment is published and the durable capture
// acknowledgement advances to its last transaction. In ModeNormal the segment
// waits in the temporary directory until the barrier of Sync publishes it.
//
// A segment that could not be published stays in the queue: the sequence
// numbers of the chain stay continuous and the next barrier tries it again.
func (s *Spool) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken {
		return s.brokenError()
	}
	if s.closed {
		return errClosed
	}
	if s.cfg.Mode == ModeNormal {
		if s.open == nil {
			return nil
		}
		if err := s.closeOpen(); err != nil {
			return err
		}
		return nil
	}
	if s.open != nil {
		if err := s.closeOpen(); err != nil {
			return err
		}
	}
	return s.publish()
}

// closeOpen closes the segment that is open and puts it into the queue.
func (s *Spool) closeOpen() error {
	o := s.open
	trailer, err := o.writer.Close(segmentPos(o.lastPos))
	if err != nil {
		s.discardOpen()
		return fmt.Errorf("spool: close the segment: %w", err)
	}
	o.trailer = trailer
	o.bytes += segment.TrailerSize

	if err := o.file.Close(); err != nil {
		s.open = nil
		_ = os.Remove(o.path)
		return fmt.Errorf("spool: close the segment: %w", err)
	}
	s.open = nil
	s.pending = append(s.pending, &pendingSegment{
		path:      o.path,
		seqStart:  o.seqStart,
		bytes:     o.bytes,
		createdAt: o.createdAt,
		trailer:   trailer,
	})
	return nil
}

// publish publishes the segments that wait for it and runs the barrier.
//
// The segments are renamed into the segments directory, the barrier syncs the
// directory. The capture acknowledgement only moves when the barrier
// succeeded.
//
// A segment that could not be published stays in the queue: the sequence
// numbers of the chain stay continuous and the next barrier tries it again.
func (s *Spool) publish() error {
	if len(s.pending) == 0 && !s.barrierPending {
		return nil
	}
	for i, p := range s.pending {
		if err := s.publishSegment(p); err != nil {
			s.pending = s.pending[i:]
			return err
		}
	}
	s.pending = nil
	if s.cfg.Mode.syncs() {
		if err := s.syncDir(s.segDir); err != nil {
			s.barrierPending = true
			return fmt.Errorf("spool: sync the segments directory: %w", err)
		}
	}
	s.barrierPending = false
	if s.lastPublished != nil {
		return s.setDurable(Ack{Seq: s.lastPublished.trailer.SeqEnd, Pos: spoolPos(s.lastPublished.trailer.Pos())})
	}
	return nil
}

// publishSegment syncs a segment and renames it into the segments directory.
func (s *Spool) publishSegment(p *pendingSegment) error {
	if s.cfg.Mode.syncs() {
		file, err := os.OpenFile(p.path, os.O_RDWR, 0)
		if err != nil {
			return fmt.Errorf("spool: open %s: %w", p.path, err)
		}
		err = s.syncFile(p.path, file)
		_ = file.Close()
		if err != nil {
			return fmt.Errorf("spool: sync the segment: %w", err)
		}
	}
	final := filepath.Join(s.segDir, segmentName(p.seqStart, p.trailer.SeqEnd))
	if err := os.Rename(p.path, final); err != nil {
		return fmt.Errorf("spool: publish the segment: %w", err)
	}
	s.segs = append(s.segs, SegmentRef{
		Path:      final,
		SeqStart:  p.seqStart,
		SeqEnd:    p.trailer.SeqEnd,
		Epoch:     s.epoch,
		Bytes:     p.bytes,
		CreatedAt: p.createdAt,
	})
	s.lastPublished = p
	return nil
}

// Sync runs the persistence barrier.
//
// In ModeNormal the segments that wait in the temporary directory are synced,
// published and made durable as one batch. In the other modes every segment
// passed the barrier when it was flushed and Sync does nothing.
func (s *Spool) Sync(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken {
		return s.brokenError()
	}
	if s.closed {
		return errClosed
	}
	if s.cfg.Mode != ModeNormal {
		return nil
	}
	return s.publish()
}

// Durable returns the durable capture acknowledgement of the spool.
//
// The acknowledgement names the last transaction that is durable and the
// position in the write-ahead log it was committed at. The driver may write
// the frames of the log up to that position back into the database.
//
// The sequence number is zero when nothing is durable yet.
func (s *Spool) Durable() (Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken {
		return Ack{}, s.brokenError()
	}
	return s.durable, nil
}

// Segments returns the segments that are published and durable.
//
// A segment that waits for the barrier is not part of the list, it may not
// be sent to the backup and it may not be acknowledged.
func (s *Spool) Segments() ([]SegmentRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken {
		return nil, s.brokenError()
	}
	out := make([]SegmentRef, len(s.segs))
	copy(out, s.segs)
	return out, nil
}

// Pending returns the work the spool holds.
func (s *Spool) Pending() (Pending, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken {
		return Pending{}, s.brokenError()
	}
	p := Pending{Segments: len(s.pending), Due: s.due()}
	oldest := time.Time{}
	for _, seg := range s.pending {
		p.Bytes += seg.bytes
		if oldest.IsZero() || seg.createdAt.Before(oldest) {
			oldest = seg.createdAt
		}
	}
	if s.open != nil {
		p.Segments++
		p.OpenBytes = s.open.bytes
		if oldest.IsZero() || s.open.createdAt.Before(oldest) {
			oldest = s.open.createdAt
		}
	}
	if !oldest.IsZero() {
		p.Age = time.Since(oldest)
	}
	return p, nil
}

// due reports whether the thresholds of the configuration say that the peer
// should sync.
//
// The thresholds batch the barriers: the peer waits until the interval has
// passed or until it wrote enough bytes.
func (s *Spool) due() bool {
	if s.cfg.Mode != ModeNormal {
		return false
	}
	if len(s.pending) == 0 && s.open == nil && !s.barrierPending {
		return false
	}
	if s.bytesSinceBarrier >= s.cfg.SyncBytes {
		return true
	}
	return time.Since(s.lastBarrier) >= s.cfg.SyncInterval
}

// SetBackupAck records the sequence number the backup applied.
//
// The acknowledgement may only be written after the backup applied the
// segments up to that sequence number and persisted the sequence number. The
// segments that the acknowledgement covers may be removed by a garbage
// collection.
func (s *Spool) SetBackupAck(ctx context.Context, seq uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken {
		return s.brokenError()
	}
	if s.closed {
		return errClosed
	}
	return s.writeAckState(seq)
}

// BackupAck returns the sequence number the backup acknowledged.
//
// The acknowledgement of a chain that was reset does not count, the backup
// has to acknowledge the new chain.
func (s *Spool) BackupAck() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.backupAckLocked()
}

// GC removes the segments the backup acknowledged.
//
// A segment is removed when the backup acknowledged it and when it is older
// than the retention of the configuration. The capture acknowledgement never
// removes a segment: the data of a segment that the backup did not apply is
// still needed.
func (s *Spool) GC(ctx context.Context) (GCResult, error) {
	if err := ctx.Err(); err != nil {
		return GCResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken {
		return GCResult{}, s.brokenError()
	}
	if s.closed {
		return GCResult{}, errClosed
	}

	ack, err := s.backupAckLocked()
	if err != nil {
		return GCResult{}, err
	}
	now := time.Now()
	var res GCResult
	keep := make([]SegmentRef, 0, len(s.segs))
	for _, seg := range s.segs {
		if seg.SeqEnd > ack || now.Sub(seg.CreatedAt) < s.cfg.Retention {
			keep = append(keep, seg)
			continue
		}
		if err := os.Remove(seg.Path); err != nil {
			return res, fmt.Errorf("spool: remove %s: %w", seg.Path, err)
		}
		res.Segments++
		res.Bytes += seg.Bytes
	}
	s.segs = keep
	if res.Segments > 0 && s.cfg.Mode.syncs() {
		if err := s.syncDir(s.segDir); err != nil {
			return res, fmt.Errorf("spool: sync the segments directory: %w", err)
		}
	}
	return res, nil
}

// Reset empties the spool.
//
// The segments and the temporary files are removed, the epoch is raised, the
// sequence numbers start over and the acknowledgements are cleared. The chain
// that was reset is gone, the backup needs a new snapshot.
func (s *Spool) Reset(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.open != nil {
		s.discardOpen()
	}
	if err := s.removeFiles(s.segDir); err != nil {
		return err
	}
	if err := s.removeFiles(s.tmpDir); err != nil {
		return err
	}
	s.segs = nil
	s.pending = nil
	s.lastPublished = nil
	s.barrierPending = false
	s.broken = false
	s.reason = ""
	s.epoch++
	s.nextSeq = 1
	s.durable = Ack{}
	s.bytesSinceBarrier = 0

	if err := os.Remove(s.ackPath()); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("spool: remove the acknowledgement: %w", err)
	}
	if s.cfg.Mode.syncs() {
		if err := s.syncDir(s.segDir); err != nil {
			return fmt.Errorf("spool: sync the segments directory: %w", err)
		}
		if err := s.syncDir(s.peerDir); err != nil {
			return fmt.Errorf("spool: sync the peers directory: %w", err)
		}
	}
	if err := s.writeStreamState(); err != nil {
		return err
	}
	return s.writeCaptureState()
}

// removeFiles removes every file of a directory.
func (s *Spool) removeFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("spool: read %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			return fmt.Errorf("spool: remove %s: %w", filepath.Join(dir, e.Name()), err)
		}
	}
	return nil
}

// Stats returns a snapshot of the state of the spool.
func (s *Spool) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	st := Stats{
		Mode:          s.cfg.Mode,
		Epoch:         s.epoch,
		Segments:      len(s.segs),
		Bytes:         s.totalBytes(),
		DurableSeq:    s.durable.Seq,
		DiskFreeBytes: diskFree(s.dir),
		Broken:        s.broken,
		BrokenReason:  s.reason,
	}
	if ack, err := s.backupAckLocked(); err == nil {
		st.BackupAckSeq = ack
	}
	oldest := time.Time{}
	for _, seg := range s.segs {
		if seg.SeqEnd > st.BackupAckSeq {
			st.UnackedSegments++
			st.UnackedBytes += seg.Bytes
			if oldest.IsZero() || seg.CreatedAt.Before(oldest) {
				oldest = seg.CreatedAt
			}
		}
	}
	if !oldest.IsZero() {
		st.UnackedAge = time.Since(oldest)
	}
	if s.cfg.WarnBytes > 0 && st.Bytes > s.cfg.WarnBytes {
		st.Warn = true
	}
	return st
}

// totalBytes returns the number of bytes the spool holds on the disk.
func (s *Spool) totalBytes() int64 {
	var total int64
	for _, seg := range s.segs {
		total += seg.Bytes
	}
	for _, seg := range s.pending {
		total += seg.bytes
	}
	if s.open != nil {
		total += s.open.bytes
	}
	return total
}

// brokenError returns the error that describes the broken chain.
func (s *Spool) brokenError() error {
	return fmt.Errorf("%w: %s", ErrChainBroken, s.reason)
}

// setDurable records the durable capture acknowledgement.
//
// The acknowledgement is written to the state file. The state file is only
// an accelerator, the segments are the truth, so a failure to write it is
// reported but does not undo the acknowledgement.
func (s *Spool) setDurable(a Ack) error {
	s.durable = a
	s.lastBarrier = time.Now()
	s.bytesSinceBarrier = 0
	return s.writeCaptureState()
}

// segmentPos turns the position of the spool into the position of a segment.
func segmentPos(p Pos) segment.Pos {
	return segment.Pos{Salt1: p.Salt1, Salt2: p.Salt2, Frame: p.Frame, WALOffset: p.WALOffset}
}

// spoolPos turns the position of a segment into the position of the spool.
func spoolPos(p segment.Pos) Pos {
	return Pos{Salt1: p.Salt1, Salt2: p.Salt2, Frame: p.Frame, WALOffset: p.WALOffset}
}

// segmentName returns the name of the file of a segment.
func segmentName(seqStart, seqEnd uint64) string {
	return fmt.Sprintf("%016d-%016d.seg", seqStart, seqEnd)
}

// tmpName returns the name of the temporary file of a segment.
func tmpName(seqStart uint64) string {
	return fmt.Sprintf("%016d.tmp", seqStart)
}

// defaultSyncFile syncs a file.
func defaultSyncFile(_ string, f *os.File) error {
	return f.Sync()
}

// defaultSyncDir syncs a directory.
func defaultSyncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
