// Package source is the primary end of a relay.
//
// The source owns the primary database and the spool of its increments. It
// captures the committed transactions of the write-ahead log, it writes them
// into the spool and it pushes the segments of the spool to the backup.
//
// The two acknowledgements of a relay are separate. The capture
// acknowledgement says that the increments are in the local spool: it does
// not wait for the network and it lets the driver write the frames of the
// log back into the database. The backup acknowledgement says that the
// backup applied the increments: it lets the source collect the segments
// that the backup does not need any more.
//
// The source never blocks the business. A business transaction does not
// wait for the network, for the capture acknowledgement or for the backup
// acknowledgement. When the spool is full the capture stands still and the
// increments are captured again as soon as the spool has room again.
package source

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cnlangzi/sqlite-relay/driver"
	"github.com/cnlangzi/sqlite-relay/protocol"
	"github.com/cnlangzi/sqlite-relay/snapshot"
	"github.com/cnlangzi/sqlite-relay/spool"
)

// Defaults of the configuration.
const (
	defaultScanInterval = time.Second
	defaultGCInterval   = 5 * time.Second
	defaultPeerID       = "backup"
)

// closeTimeout bounds the time the runtime spends on the chain of the
// segments when it closes.
const closeTimeout = 30 * time.Second

// Config configures the primary end of a relay.
type Config struct {
	// Driver is the driver of the primary database. The application opens it
	// and hands it over, the runtime does not close it.
	Driver *driver.Driver
	// Spool holds the settings of the local spool. The runtime opens the
	// spool itself: the name of the database and the name of the peer of
	// the spool are taken from the protocol configuration and from PeerID.
	Spool spool.Config
	// Protocol holds the settings of the connection to the backup: the
	// token file, the name of the database, the number of the stream and
	// the backups that may connect.
	Protocol protocol.Config
	// PeerID is the name of the peer the acknowledgement of the backup
	// belongs to. The default is "backup".
	PeerID string
	// SnapshotDir is the directory the snapshots are written into.
	SnapshotDir string
	// ScanInterval is the interval of the periodic scan. The default is one
	// second.
	ScanInterval time.Duration
	// GCInterval is the interval of the poll of the acknowledgement of the
	// backup. The default is five seconds.
	GCInterval time.Duration
}

// withDefaults returns the configuration with the defaults filled in.
func (c Config) withDefaults() Config {
	if c.PeerID == "" {
		c.PeerID = defaultPeerID
	}
	if c.ScanInterval <= 0 {
		c.ScanInterval = defaultScanInterval
	}
	if c.GCInterval <= 0 {
		c.GCInterval = defaultGCInterval
	}
	return c
}

// Stats is a snapshot of the state of the runtime.
type Stats struct {
	// Driver is the state of the driver of the primary database.
	Driver driver.Stats
	// Spool is the state of the spool.
	Spool spool.Stats
	// CapturedSeq is the highest sequence number that is durable in the
	// spool.
	CapturedSeq uint64
	// BackupSeq is the highest sequence number the backup acknowledged.
	BackupSeq uint64
	// LagSeq is the number of the sequence numbers the backup did not
	// acknowledge yet.
	LagSeq uint64
	// Lag is the time since the last acknowledgement of the backup.
	Lag time.Duration
	// LastAck is the time of the last acknowledgement of the backup.
	LastAck time.Time
	// Connected reports that a backup is on the line.
	Connected bool
	// Sessions is the number of the sessions the source accepted since it
	// started.
	Sessions uint64
	// Resyncs is the number of the resyncs the source asked the backup
	// for since it started.
	Resyncs uint64
	// SpoolFull reports that the spool reached its limit and that the
	// capture stands still.
	SpoolFull bool
	// SnapshotRequired reports that the chain is broken and that the backup
	// needs a new baseline.
	SnapshotRequired bool
	// SnapshotReason explains why the chain is broken.
	SnapshotReason string
	// LastError is the description of the last failure.
	LastError string
}

// Peer is the primary end of a relay.
//
// The peer captures the increments of the primary database, it writes them
// into the spool, it serves the segments of the spool to the backup and it
// collects the segments the backup acknowledged.
type Peer struct {
	cfg   Config
	drv   *driver.Driver
	sp    *spool.Spool
	prod  *snapshot.Producer
	proto *protocol.Source

	// gateMu guards the state of the capture gate.
	gateMu sync.Mutex
	// suspends is the number of the callers that suspended the capture.
	suspends int
	// resumed is closed when the capture continues. It is nil while the
	// capture runs.
	resumed chan struct{}
	// roundEnd is closed when the capture round that is running ends. It is
	// nil while no round is running.
	roundEnd chan struct{}

	// mu guards the state below.
	mu sync.Mutex
	// pending holds the transactions that did not fit into the spool.
	pending []spool.Tx
	// spoolFull reports that the spool reached its limit.
	spoolFull bool
	// snapshotRequired reports that the chain is broken.
	snapshotRequired bool
	// snapshotReason explains why the chain is broken.
	snapshotReason string
	// lastErr is the error of the last failure.
	lastErr error
	// backupAckSeq is the sequence number that was written into the
	// acknowledgement of the spool.
	backupAckSeq uint64
	// lastAck is the time of the last acknowledgement of the backup.
	lastAck time.Time
	// appliedSeen is the sequence number the protocol reported the last
	// time it was read.
	appliedSeen uint64
	// resumePos is the position the runtime continues from after a restart.
	resumePos spool.Pos
	// resumeSeq is the sequence number the runtime continues from.
	resumeSeq uint64
	// resumeSet reports that resumePos belongs to a chain.
	resumeSet bool
	// continuityChecked reports that the continuity of the chain was
	// decided.
	continuityChecked bool

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once

	// runMu guards the start of the run against the close of the runtime.
	runMu sync.Mutex
	// closed reports that the runtime was closed.
	closed bool
}

// Open opens the runtime of the primary end of a relay.
//
// The spool is opened and the producer of the snapshots and the server of
// the protocol are built. The driver is not closed by the runtime: the
// application closes it.
func Open(ctx context.Context, cfg Config) (*Peer, error) {
	cfg = cfg.withDefaults()
	if cfg.Driver == nil {
		return nil, errors.New("source: the driver is empty")
	}
	if cfg.Spool.Root == "" {
		return nil, errors.New("source: the root directory of the spool is empty")
	}
	if cfg.Protocol.DatabaseID == "" {
		return nil, errors.New("source: the database ID is empty")
	}
	if cfg.SnapshotDir == "" {
		return nil, errors.New("source: the directory of the snapshots is empty")
	}

	// The segments of the spool belong to the database of the protocol.
	cfg.Spool.DatabaseID = cfg.Protocol.DatabaseID
	cfg.Spool.PeerID = cfg.PeerID

	sp, err := spool.Open(ctx, cfg.Spool)
	broken := ""
	if err != nil {
		if !errors.Is(err, spool.ErrChainBroken) || sp == nil {
			return nil, err
		}
		// The chain of the segments has a hole. It is of no use any more:
		// the spool starts a new chain and the backup needs a new
		// baseline.
		broken = err.Error()
		if err := sp.Reset(ctx); err != nil {
			_ = sp.Close()
			return nil, err
		}
		// The marker describes the chain that is gone.
		if err := removeMarker(markerPathOf(cfg)); err != nil {
			_ = sp.Close()
			return nil, err
		}
	}

	p := &Peer{cfg: cfg, drv: cfg.Driver, sp: sp}
	prod, err := snapshot.New(snapshot.Config{
		Dir:        cfg.SnapshotDir,
		DatabaseID: cfg.Protocol.DatabaseID,
		StreamID:   cfg.Protocol.StreamID,
	}, cfg.Driver, sp, p)
	if err != nil {
		_ = sp.Close()
		return nil, err
	}
	p.prod = prod

	src, err := protocol.NewSourceWithSnapshot(cfg.Protocol, protocol.NewSpoolSource(sp), prod)
	if err != nil {
		_ = sp.Close()
		return nil, err
	}
	p.proto = src

	// The runtime continues from the durable acknowledgement of the spool:
	// the increments it holds are captured again.
	ack, err := sp.Durable()
	if err != nil {
		_ = sp.Close()
		return nil, err
	}
	if ack.Seq > 0 {
		p.resumePos = ack.Pos
		p.resumeSeq = ack.Seq
		p.resumeSet = true
	}
	if broken != "" {
		p.snapshotRequired = true
		p.snapshotReason = broken
	}

	p.ctx, p.cancel = context.WithCancel(ctx)
	return p, nil
}

// Run serves the backup and captures the increments of the primary database.
//
// The run ends when the context is done or when the listener fails. The
// server closes the listener when the context is done, the caller may close
// it to stop the relay.
func (p *Peer) Run(ctx context.Context, ln net.Listener) error {
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(p.ctx, cancel)
	defer stop()

	// The loops are started under the lock of the run, so that a close
	// that waits for them does not wait for a start that did not happen
	// yet.
	p.runMu.Lock()
	if p.closed {
		p.runMu.Unlock()
		return errors.New("source: the runtime is closed")
	}
	p.wg.Add(2)
	p.runMu.Unlock()

	go func() {
		defer p.wg.Done()
		p.captureLoop(rctx)
	}()
	go func() {
		defer p.wg.Done()
		p.gcLoop(rctx)
	}()

	err := p.proto.Serve(rctx, ln)
	cancel()
	p.wg.Wait()
	return err
}

// Stats returns a snapshot of the state of the runtime.
func (p *Peer) Stats() Stats {
	p.mu.Lock()
	st := Stats{
		BackupSeq:        p.backupAckSeq,
		SpoolFull:        p.spoolFull,
		SnapshotRequired: p.snapshotRequired,
		SnapshotReason:   p.snapshotReason,
		LastAck:          p.lastAck,
	}
	if p.lastErr != nil {
		st.LastError = p.lastErr.Error()
	}
	p.mu.Unlock()

	st.Driver = p.drv.Stats()
	st.Spool = p.sp.Stats()
	st.Connected = p.proto.Connected()
	st.Sessions = p.proto.Sessions()
	st.Resyncs = p.proto.Resyncs()
	st.CapturedSeq = st.Spool.DurableSeq
	if st.CapturedSeq > st.BackupSeq {
		st.LagSeq = st.CapturedSeq - st.BackupSeq
	}
	if !st.LastAck.IsZero() {
		st.Lag = time.Since(st.LastAck)
	}
	return st
}

// Close stops the runtime and releases the spool.
//
// The runtime writes the transactions that are still pending into the spool,
// it captures the transactions that were committed after the last round, it
// publishes the segments and it marks the chain of the segments as complete.
// The marker tells the next runtime that the chain covers the primary
// database.
//
// The application has to stop writing to the primary database before it
// closes the runtime: a transaction that is committed afterwards is not in
// the chain.
//
// Close does not report a failure: the state of the chain is reported in the
// statistics of the runtime.
//
// The driver is not closed: the application owns it.
func (p *Peer) Close() error {
	p.closeOnce.Do(func() {
		p.runMu.Lock()
		p.closed = true
		p.cancel()
		p.wg.Wait()
		p.runMu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()
		if err := p.finishChain(ctx); err != nil {
			p.setError(err)
		}
		_ = p.sp.Close()
	})
	return nil
}

// finishChain writes the transactions that are still pending into the spool
// and marks the chain of the segments as complete.
//
// The marker says that the chain covers the primary database. It is only
// written when the chain ends where the capture cursor of the driver ends: a
// chain that ends somewhere else may have a hole.
func (p *Peer) finishChain(ctx context.Context) error {
	if err := p.writePending(ctx); err != nil {
		return err
	}
	// The capture stands still: the transactions that were committed after
	// the last round are not in the chain yet.
	if err := p.captureTheRest(ctx); err != nil {
		return err
	}
	if err := p.sp.Flush(ctx); err != nil {
		return err
	}
	pending, err := p.sp.Pending()
	if err != nil {
		return err
	}
	if pending.Due {
		if err := p.sp.Sync(ctx); err != nil {
			return err
		}
	}
	ack, err := p.sp.Durable()
	if err != nil {
		return err
	}
	if ack.Seq == 0 {
		// The chain is empty: there is nothing to mark and nothing that
		// could have a hole.
		return removeMarker(p.markerPath())
	}
	cursor := p.drv.Stats().CaptureCursor
	switch {
	case p.hasPending():
		return p.discardMarker("the transactions that did not fit into the spool are not in the chain")
	case samePosition(ack.Pos, cursor):
		// The chain ends where the driver captured up to: it covers the
		// primary database.
		return writeMarker(p.markerPath(), cleanMarker{Seq: ack.Seq, Pos: markerPosOf(ack.Pos)})
	case p.chainUnchanged(ack):
		// The driver captured nothing and the chain did not move since the
		// runtime started: the marker of the last clean close describes
		// the chain and it stays.
		return nil
	default:
		return p.discardMarker("the chain does not end where the driver captured up to")
	}
}

// discardMarker removes the marker of the last clean close and reports that
// the chain does not cover the primary database.
func (p *Peer) discardMarker(reason string) error {
	if err := removeMarker(p.markerPath()); err != nil {
		return err
	}
	return fmt.Errorf("%w: %s", errChainIncomplete, reason)
}

// chainUnchanged reports whether the chain of the segments is the chain the
// runtime continued from.
//
// The marker of the last clean close describes that chain: it stays accurate
// as long as the chain does not move.
func (p *Peer) chainUnchanged(ack spool.Ack) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.resumeSet {
		return false
	}
	return ack.Seq == p.resumeSeq && samePosition(ack.Pos, toDriverPos(p.resumePos))
}

// Suspend stops the capture and waits for the round that is running to end.
//
// The returned function starts the capture again. The producer of the
// snapshots suspends the capture while it takes a snapshot: the snapshot and
// the drain that follows it have to see the same transactions.
//
// The capture stands still until every caller that suspended it called the
// returned function.
func (p *Peer) Suspend(ctx context.Context) (func(), error) {
	p.gateMu.Lock()
	p.suspends++
	ch := p.resumed
	if ch == nil {
		ch = make(chan struct{})
		p.resumed = ch
	}
	end := p.roundEnd
	p.gateMu.Unlock()

	// The round that is running has to end before the capture stands still.
	if end != nil {
		select {
		case <-end:
		case <-ctx.Done():
			p.resumeCapture(ch)
			return nil, ctx.Err()
		}
	}
	return func() { p.resumeCapture(ch) }, nil
}

// resumeCapture starts the capture again.
//
// The capture continues when every caller that suspended it resumed.
func (p *Peer) resumeCapture(ch chan struct{}) {
	p.gateMu.Lock()
	defer p.gateMu.Unlock()
	if p.resumed != ch {
		return
	}
	p.suspends--
	if p.suspends > 0 {
		return
	}
	close(ch)
	p.resumed = nil
}

// setError remembers the error of the last failure.
func (p *Peer) setError(err error) {
	if err == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastErr = err
}

// toDriverPos turns the position of the spool into the position of the
// driver.
func toDriverPos(pos spool.Pos) driver.Position {
	return driver.Position{
		Salt1:     pos.Salt1,
		Salt2:     pos.Salt2,
		Frame:     pos.Frame,
		WALOffset: pos.WALOffset,
	}
}

// samePosition reports whether a position of the spool and a position of the
// driver name the same frame of the same generation.
func samePosition(pos spool.Pos, cursor driver.Position) bool {
	return pos.Salt1 == cursor.Salt1 &&
		pos.Salt2 == cursor.Salt2 &&
		pos.Frame == cursor.Frame
}
