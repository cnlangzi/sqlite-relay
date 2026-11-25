// Package driver captures the committed changes of a SQLite database from its
// write-ahead log.
//
// The driver owns every connection to the primary database inside the
// process. It installs a WAL hook on each of them, reads the committed
// transactions out of the WAL and hands them to the consumer as a stream of
// page level increments.
//
// The driver never writes to the primary database outside of the business
// transactions, it never waits for the consumer on the business path and it
// never writes a frame back into the database file that the consumer has not
// acknowledged yet.
package driver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cnlangzi/sqlite-relay/internal/wal"
)

// ErrSnapshotRequired reports that the increments of the primary database
// are not continuous any more.
//
// The consumer cannot close the gap with the increments of the log, it needs
// a new snapshot of the primary database as the baseline of the chain that
// follows it.
var ErrSnapshotRequired = errors.New("driver: a new snapshot is required")

// Config configures a Driver.
type Config struct {
	// DSN is the mattn DSN of the primary database.
	DSN string
	// BusyTimeout is the busy timeout every managed connection runs with.
	BusyTimeout time.Duration
	// ScanInterval is the interval of the periodic scan that backs up the
	// commit notifications.
	ScanInterval time.Duration
	// ReclaimBytes is the size of the WAL above which the driver tries to
	// give the WAL space back to SQLite.
	ReclaimBytes int64
	// ReclaimInterval is the shortest interval between two reclaim attempts.
	ReclaimInterval time.Duration
	// ReclaimTimeout bounds how long a reclaim waits for the capture
	// position to be acknowledged.
	ReclaimTimeout time.Duration
	// SnapshotRetries is the number of times a snapshot is retried before it
	// gives up.
	SnapshotRetries int
	// TruncateOnReclaim lets the reclaim shrink the WAL file.
	TruncateOnReclaim bool
	// MaxOpenConns bounds the size of the business connection pool.
	MaxOpenConns int
	// EventBuffer is the size of the notification channel.
	EventBuffer int
	// ResumePosition is the capture position the driver continues from. It
	// is the position a snapshot returned.
	ResumePosition *Position
}

// Defaults of the configuration.
const (
	defaultBusyTimeout     = 5 * time.Second
	defaultScanInterval    = time.Second
	defaultReclaimBytes    = 64 << 20
	defaultReclaimInterval = 5 * time.Minute
	defaultReclaimTimeout  = 5 * time.Second
	defaultSnapshotRetries = 3
	defaultEventBuffer     = 64
)

func (c Config) withDefaults() Config {
	if c.BusyTimeout <= 0 {
		c.BusyTimeout = defaultBusyTimeout
	}
	if c.ScanInterval <= 0 {
		c.ScanInterval = defaultScanInterval
	}
	if c.ReclaimBytes <= 0 {
		c.ReclaimBytes = defaultReclaimBytes
	}
	if c.ReclaimInterval <= 0 {
		c.ReclaimInterval = defaultReclaimInterval
	}
	if c.ReclaimTimeout <= 0 {
		c.ReclaimTimeout = defaultReclaimTimeout
	}
	if c.SnapshotRetries <= 0 {
		c.SnapshotRetries = defaultSnapshotRetries
	}
	if c.EventBuffer <= 0 {
		c.EventBuffer = defaultEventBuffer
	}
	return c
}

// captureAttempts is the number of times a capture round is retried when the
// WAL moves while it is read.
const captureAttempts = 5

// Driver captures the committed changes of a SQLite database.
type Driver struct {
	cfg Config

	db       *sql.DB
	ctlDB    *sql.DB
	pinDB    *sql.DB
	sealDB   *sql.DB
	ctlConn  *sql.Conn
	pinConn  *sql.Conn
	sealConn *sql.Conn

	dbPath   string
	walPath  string
	pageSize int

	events chan Notification
	gate   *gate

	// mu guards the state below.
	mu            sync.Mutex
	conns         map[*managedConn]struct{}
	cursor        Position
	cursorSet     bool
	walBoundary   uint64
	startFrames   uint64
	ack           Position
	ackSet        bool
	backfillFence int
	fenceSeen     bool
	ckptStats     CheckpointStats
	reclaimStats  ReclaimStats
	fatalErr      error
	lastErr       error
	hookPanics    int
	openTx        int
	seq           uint64
	walHighWater  int64
	lastReclaim   time.Time
	commitFrames  uint64

	// fenceMu serialises the observation of the backfill count with the
	// checkpoints of the driver.
	fenceMu sync.Mutex

	// notifications counts the notifications the WAL hook delivered.
	notifications atomic.Uint64
	// droppedNotifications counts the notifications that the full channel
	// of the notifications did not take.
	droppedNotifications atomic.Uint64
	// scanCompensations counts the capture rounds that returned
	// transactions that no notification announced.
	scanCompensations atomic.Uint64
	// notified reports that a notification was delivered since the last
	// capture round.
	notified atomic.Bool

	// eventsMu guards the channel of the notifications.
	eventsMu     sync.Mutex
	eventsClosed bool

	// captureMu serializes the capture rounds.
	captureMu sync.Mutex
	reader    *wal.Reader

	// ckptMu guards the checkpoint manager.
	ckptMu     sync.Mutex
	ckptPaused bool
	pin        *readPin

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// Open opens the primary database and starts the driver.
func Open(ctx context.Context, cfg Config) (*Driver, error) {
	cfg = cfg.withDefaults()
	if cfg.DSN == "" {
		return nil, errors.New("driver: the DSN is empty")
	}
	d := &Driver{
		cfg:    cfg,
		events: make(chan Notification, cfg.EventBuffer),
		gate:   newGate(),
		conns:  make(map[*managedConn]struct{}),
	}
	d.ctx, d.cancel = context.WithCancel(ctx)
	// The first reclaim happens one interval after the driver started, not
	// right away: a reclaim stops the business writers while it waits for the
	// capture position to be acknowledged.
	d.lastReclaim = time.Now()

	cleanup := func() {
		d.cancel()
		d.closePools()
	}

	// The business pool. Every connection of it carries the WAL hook and
	// the connection invariants.
	db, err := d.openPool(true, cfg.MaxOpenConns)
	if err != nil {
		cleanup()
		return nil, err
	}
	d.db = db

	// The control connections. They are not gated: a quiesce must not stop
	// the driver from checkpointing.
	for _, spec := range []struct {
		db   **sql.DB
		conn **sql.Conn
	}{
		{&d.ctlDB, &d.ctlConn},
		{&d.pinDB, &d.pinConn},
		{&d.sealDB, &d.sealConn},
	} {
		pool, err := d.openPool(false, 1)
		if err != nil {
			cleanup()
			return nil, err
		}
		*spec.db = pool
		conn, err := pool.Conn(ctx)
		if err != nil {
			cleanup()
			return nil, err
		}
		*spec.conn = conn
	}

	if err := d.resolvePaths(); err != nil {
		cleanup()
		return nil, err
	}
	if err := d.readPageSize(); err != nil {
		cleanup()
		return nil, err
	}
	if err := d.recover(); err != nil {
		cleanup()
		return nil, err
	}

	d.wg.Add(1)
	go d.loop()
	return d, nil
}

// DB returns the handle of the business connection pool.
//
// It is the only way into the primary database inside the process.
func (d *Driver) DB() *sql.DB {
	return d.db
}

// Events returns the channel the commit notifications arrive on.
//
// The channel is allowed to be full: a notification may be dropped and
// several commits may be reported as one. The notifications are a wake-up
// hint, the increments themselves are always complete.
func (d *Driver) Events() <-chan Notification {
	return d.events
}

// Scan returns the transactions that were committed since the last call.
//
// The scan advances the capture cursor of the driver. Every change of the
// batch is a whole transaction.
func (d *Driver) Scan(ctx context.Context) (Batch, error) {
	if err := d.fatal(); err != nil {
		return Batch{}, err
	}
	return d.capture(ctx)
}

// ReadChanges returns the transactions that were committed after pos.
//
// The read does not touch the capture cursor of the driver. It is the way to
// continue after a restart, with the position a snapshot or an earlier run
// returned.
func (d *Driver) ReadChanges(ctx context.Context, pos Position) (Batch, error) {
	if err := d.fatal(); err != nil {
		return Batch{}, err
	}
	reader, err := wal.Open(d.walPath)
	if err != nil {
		if errors.Is(err, wal.ErrNoWAL) {
			return Batch{}, nil
		}
		return Batch{}, fmt.Errorf("driver: open WAL: %w", err)
	}
	defer func() { _ = reader.Close() }()

	hdr := reader.Header()
	if !pos.sameGeneration(Position{Salt1: hdr.Salt1, Salt2: hdr.Salt2}) {
		return Batch{}, fmt.Errorf("driver: the WAL was reset, the position %s does not belong to the current generation", pos)
	}
	if err := reader.RewindTo(pos.Frame + 1); err != nil {
		return Batch{}, fmt.Errorf("driver: read WAL: %w", err)
	}
	scan, err := reader.Scan()
	if err != nil {
		return Batch{}, fmt.Errorf("driver: read WAL: %w", err)
	}
	return d.batchOf(scan, pos), nil
}

// CaptureAck records that the increments up to pos have been persisted.
//
// The driver may only write the frames back into the database file that the
// acknowledgement covers. The acknowledgement does not wait for the remote
// side, it only says that the local spool holds the increments.
func (d *Driver) CaptureAck(_ context.Context, pos Position) error {
	if err := d.fatal(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ackSet && !d.cursor.sameGeneration(pos) {
		// The acknowledgement belongs to another generation and says
		// nothing about the frames of the current one.
		return nil
	}
	if d.ackSet && d.ack.sameGeneration(pos) && pos.Frame <= d.ack.Frame {
		// An acknowledgement never goes back. The frames of two
		// generations are counted apart, an acknowledgement of a new
		// generation is not a step back.
		return nil
	}
	d.ack = pos
	d.ackSet = true
	return nil
}

// Checkpoint runs a passive checkpoint of the primary database.
//
// The checkpoint only writes back the frames the consumer has acknowledged.
// It does nothing while the acknowledged position is behind the frames that
// would be written back.
func (d *Driver) Checkpoint(ctx context.Context) (CheckpointStats, error) {
	if err := d.fatal(); err != nil {
		return CheckpointStats{}, err
	}
	return d.runCheckpoint(ctx)
}

// Stats returns a snapshot of the state of the driver.
//
// The backfill count is read before the acknowledged position, so that the
// two can be compared: the acknowledged position never goes back, so the
// pair is consistent even when a checkpoint runs in between.
func (d *Driver) Stats() Stats {
	ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
	defer cancel()
	log, ckpt := 0, 0
	if l, c, err := d.walFrames(ctx); err == nil {
		log, ckpt = l, c
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	st := Stats{
		Generation:           Position{Salt1: d.cursor.Salt1, Salt2: d.cursor.Salt2},
		CaptureCursor:        d.cursor,
		AckPosition:          d.ack,
		BackfillFrames:       ckpt,
		WALFrames:            log,
		CommitFrames:         d.commitFrames,
		WALHighWater:         d.walHighWater,
		Notifications:        d.notifications.Load(),
		DroppedNotifications: d.droppedNotifications.Load(),
		ScanCompensations:    d.scanCompensations.Load(),
		Checkpoint:           d.ckptStats,
		Reclaim:              d.reclaimStats,
		HookPanics:           d.hookPanics,
	}
	if d.lastErr != nil {
		st.LastError = d.lastErr.Error()
	}
	if info, err := os.Stat(d.walPath); err == nil {
		st.WALBytes = info.Size()
	}
	return st
}

// QuiesceOptions configures a quiesce.
type QuiesceOptions struct {
	// BlockReads also blocks new read transactions.
	BlockReads bool
}

// Quiesce blocks new write transactions.
//
// A blocked writer waits for the resume instead of failing, so that a window
// in which no new write starts stays free of errors. The call returns once
// the write transactions that were open have finished.
func (d *Driver) Quiesce(ctx context.Context) error {
	return d.QuiesceWith(ctx, QuiesceOptions{})
}

// QuiesceWith blocks new write transactions and, if asked for, new read
// transactions as well.
func (d *Driver) QuiesceWith(ctx context.Context, opts QuiesceOptions) error {
	d.gate.quiesce(opts.BlockReads)
	for {
		d.mu.Lock()
		open := d.openTx
		d.mu.Unlock()
		if open == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// Resume lets the blocked writers continue.
func (d *Driver) Resume() error {
	d.gate.resumeGate()
	return nil
}

// Close stops the driver and releases its connections.
//
// The channel of Events is closed, a consumer that ranges over it ends.
func (d *Driver) Close() error {
	d.closeOnce.Do(func() {
		d.cancel()
		d.wg.Wait()
		d.gate.resumeGate()
		d.ckptMu.Lock()
		d.releasePin()
		d.ckptMu.Unlock()
		d.closePools()
		d.closeEvents()
	})
	return nil
}

// closeEvents closes the channel of the notifications.
//
// The channel may have been closed by the caller of the driver, which the
// driver cannot tell apart from its own close.
func (d *Driver) closeEvents() {
	d.eventsMu.Lock()
	defer d.eventsMu.Unlock()
	if d.eventsClosed {
		return
	}
	d.eventsClosed = true
	close(d.events)
}

// closePools releases the connection pools.
func (d *Driver) closePools() {
	for _, c := range []*sql.Conn{d.ctlConn, d.pinConn, d.sealConn} {
		if c != nil {
			_ = c.Close()
		}
	}
	for _, p := range []*sql.DB{d.db, d.ctlDB, d.pinDB, d.sealDB} {
		if p != nil {
			_ = p.Close()
		}
	}
}

// fatal returns the error that makes the driver unusable.
func (d *Driver) fatal() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.fatalErr
}

// txOpened counts an open write transaction.
func (d *Driver) txOpened() {
	d.mu.Lock()
	d.openTx++
	d.mu.Unlock()
}

// txClosed counts a finished write transaction.
func (d *Driver) txClosed() {
	d.mu.Lock()
	if d.openTx > 0 {
		d.openTx--
	}
	d.mu.Unlock()
}

// resolvePaths finds the database file and its WAL.
func (d *Driver) resolvePaths() error {
	var path string
	err := d.ctlConn.Raw(func(dc any) error {
		mc, ok := dc.(*managedConn)
		if !ok {
			return fmt.Errorf("driver: unexpected connection type %T", dc)
		}
		path = mc.conn.GetFilename("main")
		return nil
	})
	if err != nil {
		return err
	}
	if path == "" {
		return errors.New("driver: the database has no file, an in-memory database cannot be captured")
	}
	d.dbPath = path
	d.walPath = path + "-wal"
	return nil
}

// readPageSize reads the page size of the database.
func (d *Driver) readPageSize() error {
	var size int
	if err := d.ctlConn.QueryRowContext(d.ctx, "PRAGMA main.page_size").Scan(&size); err != nil {
		return fmt.Errorf("driver: read page size: %w", err)
	}
	if size <= 0 {
		return fmt.Errorf("driver: the database reports a page size of %d", size)
	}
	d.pageSize = size
	return nil
}

// recover positions the capture cursor after a restart.
//
// The startup scan reads the whole WAL, which rebuilds the checksum chain
// the reader needs to continue in the middle of the WAL.
func (d *Driver) recover() error {
	reader, err := d.walReader()
	if err != nil {
		if errors.Is(err, wal.ErrNoWAL) {
			// There is no WAL yet. The cursor takes the generation of
			// the WAL that shows up later.
			return nil
		}
		return err
	}
	hdr := reader.Header()

	if d.cfg.ResumePosition != nil {
		pos := *d.cfg.ResumePosition
		if !pos.sameGeneration(Position{Salt1: hdr.Salt1, Salt2: hdr.Salt2}) {
			return fmt.Errorf("%w: the WAL was reset, the position %s does not belong to the current generation", ErrSnapshotRequired, pos)
		}
		if err := reader.RewindTo(pos.Frame + 1); err != nil {
			return fmt.Errorf("driver: resume at %s: %w", pos, err)
		}
		d.mu.Lock()
		d.cursor = pos
		d.cursorSet = true
		// The position of the resume is the commit frame of the last
		// transaction that was captured: the log holds at least that many
		// committed frames.
		d.commitFrames = pos.Frame
		d.mu.Unlock()
		return nil
	}

	// Start at the beginning of the current generation. The transactions
	// that were committed before the driver was started are still in the
	// WAL and must not be lost. The consumer recognises the ones it has
	// seen already by their position.
	//
	// The frames that the log held already are the business of the
	// consumer: a reset of the log does not lose them, the consumer holds
	// the ones it has seen.
	frames := uint64(0)
	if log, _, err := d.walFrames(d.ctx); err == nil && log > 0 {
		frames = uint64(log)
	}
	d.mu.Lock()
	d.cursor = Position{Salt1: hdr.Salt1, Salt2: hdr.Salt2, WALOffset: walOffset(0, d.pageSize)}
	d.cursorSet = true
	d.startFrames = frames
	d.mu.Unlock()
	return nil
}

// walReader returns the reader of the WAL, opening it if needed.
func (d *Driver) walReader() (*wal.Reader, error) {
	if d.reader != nil {
		return d.reader, nil
	}
	reader, err := wal.Open(d.walPath)
	if err != nil {
		return nil, err
	}
	d.reader = reader
	return reader, nil
}

// resetReader forgets the reader so that the next capture opens the WAL
// again.
func (d *Driver) resetReader() {
	if d.reader != nil {
		_ = d.reader.Close()
		d.reader = nil
	}
}

// capture reads the WAL and returns the transactions that were committed
// since the last round.
func (d *Driver) capture(ctx context.Context) (Batch, error) {
	d.captureMu.Lock()
	defer d.captureMu.Unlock()

	var lastErr error
	for attempt := 0; attempt < captureAttempts; attempt++ {
		batch, retry, err := d.captureOnce(ctx)
		if err != nil {
			return Batch{}, err
		}
		if !retry {
			d.noteScanCompensation(batch)
			return batch, nil
		}
		lastErr = errors.New("driver: the WAL kept moving while it was read")
	}
	return Batch{}, lastErr
}

// noteScanCompensation counts a capture round that found transactions that
// no notification announced.
//
// The periodic scan of the driver backs the notifications of the WAL hook
// up. A round that found transactions without a notification found them on
// its own.
func (d *Driver) noteScanCompensation(batch Batch) {
	notified := d.notified.Swap(false)
	if len(batch.Changes) == 0 || notified {
		return
	}
	d.scanCompensations.Add(1)
}

// captureOnce runs a single capture round.
func (d *Driver) captureOnce(ctx context.Context) (Batch, bool, error) {
	reader, err := d.walReader()
	if err != nil {
		if errors.Is(err, wal.ErrNoWAL) {
			return Batch{}, false, nil
		}
		return Batch{}, false, err
	}
	hdr := reader.Header()

	// The WAL was reset since the last round. The cursor moves to the new
	// generation, but only if the old one was captured completely.
	if err := d.handleGenerationChange(hdr); err != nil {
		return Batch{}, false, err
	}

	before, _, err := d.walFrames(ctx)
	if err != nil {
		return Batch{}, false, err
	}
	scan, err := reader.Scan()
	if err != nil {
		return Batch{}, false, fmt.Errorf("driver: read WAL: %w", err)
	}
	d.noteCommitFrame(scan.CommitFrame)
	after, _, err := d.walFrames(ctx)
	if err != nil {
		return Batch{}, false, err
	}
	d.noteWALBoundary(after)
	// The pragma reports a negative count when it could not read the state
	// of the WAL. The generation check below covers that case.
	if before >= 0 && after >= 0 {
		if err := checkFrames(before, int(scan.CommitFrame), after); err != nil {
			// The WAL was reset while it was read.
			d.resetReader()
			return Batch{}, true, nil
		}
	}
	// The header on disk has to be the one the reader worked with.
	onDisk, err := wal.ReadHeader(d.walPath)
	if errors.Is(err, wal.ErrNoWAL) {
		// The WAL is gone. SQLite removes it when the last connection
		// closes the database, so there is nothing left to capture.
		d.resetReader()
		return Batch{}, false, nil
	}
	if err != nil {
		return Batch{}, false, err
	}
	if onDisk.Salt1 != hdr.Salt1 || onDisk.Salt2 != hdr.Salt2 {
		// The WAL was reset while it was read.
		d.resetReader()
		return Batch{}, true, nil
	}

	from := d.cursorPosition()
	batch := d.batchOf(scan, from)
	d.advanceCursorTo(batch.To)
	d.noteWALSize()
	return batch, false, nil
}

// batchOf turns the transactions of a scan into a batch.
//
// The transactions that were captured already are left out. A scan that
// starts at the beginning of the WAL, which happens when the reader was
// reset, would hand them out a second time.
func (d *Driver) batchOf(scan *wal.Scan, from Position) Batch {
	batch := Batch{From: from}
	for _, tx := range scan.Transactions {
		if tx.CommitFrame <= from.Frame {
			continue
		}
		change := changeFromTransaction(tx)
		change.Seq = d.nextSeq()
		change.Pos = Position{
			Salt1:     scan.Header.Salt1,
			Salt2:     scan.Header.Salt2,
			Frame:     tx.CommitFrame,
			WALOffset: walOffset(tx.CommitFrame, d.pageSize),
		}
		batch.Changes = append(batch.Changes, change)
		batch.To = change.Pos
	}
	if len(batch.Changes) == 0 {
		batch.To = from
	}
	return batch
}

// handleGenerationChange moves the cursor to a new WAL generation.
//
// The old generation has to be captured completely, otherwise the increments
// that were not captured are gone and the consumer needs a new snapshot.
func (d *Driver) handleGenerationChange(hdr wal.Header) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	next := Position{Salt1: hdr.Salt1, Salt2: hdr.Salt2}
	if d.cursor.sameGeneration(next) {
		return nil
	}
	if !d.cursorSet {
		// The driver started before the WAL existed. The first
		// generation it sees is the one it captures from.
		d.cursor = Position{Salt1: hdr.Salt1, Salt2: hdr.Salt2, WALOffset: walOffset(0, d.pageSize)}
		d.cursorSet = true
		d.walBoundary = 0
		d.commitFrames = 0
		d.clearAck()
		return nil
	}
	// The frames that the log held before the driver started are the
	// business of the consumer: it holds the ones it has seen already.
	// Only the frames that the driver had a chance to capture are lost
	// when the log is reset.
	lost := d.walBoundary - max(d.cursor.Frame, d.startFrames)
	if lost > 0 {
		return fmt.Errorf("%w: the WAL was reset with %d of %d frames uncaptured", ErrSnapshotRequired, lost, d.walBoundary)
	}
	d.cursor = Position{Salt1: hdr.Salt1, Salt2: hdr.Salt2, Frame: 0, WALOffset: walOffset(0, d.pageSize)}
	d.cursorSet = true
	d.walBoundary = 0
	d.commitFrames = 0
	d.startFrames = 0
	d.clearAck()
	return nil
}

// clearAck forgets the acknowledged position.
//
// The acknowledgement of a generation says nothing about the frames of
// another one: the frame numbers of a new generation start over. An
// acknowledgement that is left behind would keep the checkpoint manager from
// writing the frames of the new generation back into the database.
//
// The caller holds the lock of the driver.
func (d *Driver) clearAck() {
	d.ack = Position{}
	d.ackSet = false
}

// noteWALBoundary records the number of frames the WAL holds.
//
// The boundary of a generation is the last frame that was committed in it.
// The driver needs it to find out whether a reset of the WAL lost frames
// that it had not captured yet.
func (d *Driver) noteWALBoundary(frames int) {
	if frames < 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if uint64(frames) > d.walBoundary {
		d.walBoundary = uint64(frames)
	}
}

// noteCommitFrame records the boundary of the committed frames of the WAL.
//
// The boundary is the frame of the last complete transaction of the
// generation that is read now. The frames of a transaction that did not
// commit are not part of it.
func (d *Driver) noteCommitFrame(frame uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if frame > d.commitFrames {
		d.commitFrames = frame
	}
}

// setWALBoundary records the number of frames the WAL holds.
//
// The boundary belongs to the generation of the log that is read now. A
// generation that is left behind does not say anything about the frames of
// the current one.
func (d *Driver) setWALBoundary(frames uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.walBoundary = frames
}

// cursorPosition returns the position the cursor is at.
func (d *Driver) cursorPosition() Position {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cursor
}

// advanceCursorTo moves the cursor to the given position.
func (d *Driver) advanceCursorTo(pos Position) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cursor = pos
	d.cursorSet = true
}

// nextSeq returns the next number of the change counter.
func (d *Driver) nextSeq() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seq++
	return d.seq
}

// noteWALSize remembers the size of the WAL file.
func (d *Driver) noteWALSize() {
	info, err := os.Stat(d.walPath)
	if err != nil {
		return
	}
	d.mu.Lock()
	if info.Size() > d.walHighWater {
		d.walHighWater = info.Size()
	}
	d.mu.Unlock()
}

// loop runs the periodic work of the driver.
func (d *Driver) loop() {
	defer d.wg.Done()
	ticker := time.NewTicker(d.cfg.ScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			d.tick()
		}
	}
}

// tick runs one round of the periodic work.
//
// The periodic scan finds the commits that the WAL hook did not report, the
// checkpoint writes back the frames the consumer has acknowledged and the
// reclaim gives the WAL space back to SQLite.
func (d *Driver) tick() {
	ctx := d.ctx
	if err := d.fatal(); err != nil {
		return
	}
	// The invariants of the managed connections have to hold. Somebody who
	// set wal_autocheckpoint on a managed connection replaced the WAL hook
	// of that connection and the driver has to report that.
	if err := d.checkInvariants(); err != nil {
		d.recordError(err)
	}
	if log, _, err := d.walFrames(ctx); err == nil && log >= 0 {
		d.noteWALBoundary(log)
		if uint64(log) > d.cursorPosition().Frame {
			d.notify(Notification{DBName: "main", Frames: log})
		}
	}
	if _, err := d.runCheckpoint(ctx); err != nil {
		d.recordError(err)
	}
	d.maybeReclaim(ctx)
}

// maybeReclaim gives the WAL space back when it grew too much.
func (d *Driver) maybeReclaim(ctx context.Context) {
	info, err := os.Stat(d.walPath)
	if err != nil {
		return
	}
	d.mu.Lock()
	last := d.lastReclaim
	d.mu.Unlock()
	if info.Size() < d.cfg.ReclaimBytes && time.Since(last) < d.cfg.ReclaimInterval {
		return
	}
	res, err := d.reclaim(ctx)
	d.mu.Lock()
	d.lastReclaim = time.Now()
	d.reclaimStats = ReclaimStats{
		At:        d.lastReclaim,
		Done:      res.Done,
		Reason:    res.Reason,
		Stall:     res.Stall,
		Frames:    res.Frames,
		Truncated: res.Truncated,
	}
	d.mu.Unlock()
	if err != nil {
		d.recordError(err)
		return
	}
	if !res.Done {
		d.recordError(fmt.Errorf("driver: the WAL was not reclaimed: %s", res.Reason))
	}
}

// recordError remembers the last error of the driver.
func (d *Driver) recordError(err error) {
	if err == nil {
		return
	}
	d.mu.Lock()
	d.lastErr = err
	d.mu.Unlock()
}
