package protocol

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/cnlangzi/sqlite-relay/segment"
)

// Backup dials the source and applies the segments it pushes.
//
// The backup starts the connection, because the durable applied sequence and
// the pause live on its side. A connection that breaks is opened again with
// a growing delay until it succeeds.
type Backup struct {
	cfg  Config
	sink SegmentSink

	// snapSink is the sink that takes a snapshot. It is nil when the sink
	// does not implement one.
	snapSink SnapshotSink

	mu       sync.Mutex
	sess     *backupSession
	paused   bool
	pauseSeq uint64
	lastErr  error

	// sourceEpoch is the generation of the chain the source is part of.
	sourceEpoch uint64

	// lastAck is the time of the last acknowledgement the backup sent.
	lastAck time.Time

	pauseMu sync.Mutex
}

// NewBackup creates a backup that applies the segments of the source.
func NewBackup(cfg Config, sink SegmentSink) (*Backup, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if sink == nil {
		return nil, errors.New("protocol: the segment sink is nil")
	}
	b := &Backup{cfg: cfg, sink: sink}
	if s, ok := sink.(SnapshotSink); ok {
		b.snapSink = s
	}
	return b, nil
}

// Run dials the source and applies the segments it pushes until the context
// is done.
//
// The backup opens the connection again when it breaks. The delay grows
// with every attempt and is capped at the maximum of the configuration. A
// connection that needs a new snapshot ends the run with ErrResyncRequired.
func (b *Backup) Run(ctx context.Context, addr string) error {
	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		started := time.Now()
		handshaked, err := b.runSession(ctx, addr)
		if errors.Is(err, ErrResyncRequired) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		b.mu.Lock()
		b.lastErr = err
		b.mu.Unlock()
		// A connection that lived longer than a heartbeat worked, the
		// delay starts over. A connection that breaks right away keeps
		// the delay growing.
		if handshaked && time.Since(started) >= b.cfg.HeartbeatInterval {
			attempt = 0
		}
		delay := backoffDelay(b.cfg.BackoffMin, b.cfg.BackoffMax, attempt)
		attempt++
		if err := sleep(ctx, delay); err != nil {
			return err
		}
	}
}

// Epoch returns the generation of the chain the source is part of.
//
// It is zero when the source did not tell the epoch yet. The backup belongs
// to the epoch of the applied sequence of its sink, the caller keeps both
// together.
func (b *Backup) Epoch() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sourceEpoch
}

// setSourceEpoch records the generation of the chain the source is part of.
func (b *Backup) setSourceEpoch(epoch uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sourceEpoch = epoch
}

// setLastAck records the time of the last acknowledgement.
func (b *Backup) setLastAck(t time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastAck = t
}

// LastError returns the error the last connection ended with.
func (b *Backup) LastError() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastErr
}

// LastAck returns the time of the last acknowledgement the backup sent.
//
// It is the time the source received the last sequence number of the backup.
func (b *Backup) LastAck() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastAck
}

// isPaused reports whether the backup is paused.
func (b *Backup) isPaused() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.paused
}

// needsSnapshot reports whether the backup asks for a snapshot.
//
// A backup without a baseline asks for one, the source sends it instead of
// the segments of the chain.
func (b *Backup) needsSnapshot() bool {
	return b.snapSink != nil && b.snapSink.NeedsSnapshot()
}

// Pause asks the source to stop pushing segments.
//
// The call waits for the answer of the source. A pause that is already
// running returns without an error. A pause that is asked for while the
// backup is not connected is remembered, the source is paused again as soon
// as the connection is open.
//
// The backup stops applying the segments it receives as soon as the pause is
// asked for: the segments that arrive while the pause runs are skipped and
// the source sends them again after the resume.
func (b *Backup) Pause(ctx context.Context, appliedSeq uint64) error {
	b.pauseMu.Lock()
	defer b.pauseMu.Unlock()
	b.mu.Lock()
	if b.paused {
		b.mu.Unlock()
		return nil
	}
	sess := b.sess
	b.paused = true
	b.pauseSeq = appliedSeq
	b.mu.Unlock()
	if sess == nil {
		return nil
	}
	return sess.pause(ctx, appliedSeq)
}

// Resume tells the source to push segments again.
//
// The source continues after the sequence number the backup names. A resume
// that is asked for while the backup is not connected clears the pause, the
// source pushes again with the connection that is opened next.
func (b *Backup) Resume(ctx context.Context, appliedSeq uint64) error {
	b.pauseMu.Lock()
	defer b.pauseMu.Unlock()
	b.mu.Lock()
	sess := b.sess
	b.paused = false
	b.pauseSeq = appliedSeq
	b.mu.Unlock()
	if sess == nil {
		return nil
	}
	return sess.conn.writeMessage(Resume{AppliedSeq: appliedSeq})
}

// runSession opens a connection and runs the session on it.
//
// It reports whether the handshake succeeded.
func (b *Backup) runSession(ctx context.Context, addr string) (bool, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return false, err
	}
	sess := &backupSession{
		b:        b,
		conn:     newConn(c, b.cfg.MaxFrameBytes, b.cfg.IdleTimeout),
		pausedCh: make(chan Paused, 1),
		done:     make(chan struct{}),
	}
	defer sess.conn.Close()
	defer b.clearSession(sess)

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sess.mu.Lock()
	sess.cancel = cancel
	sess.mu.Unlock()

	// The reader waits for the peer, the connection is closed when the
	// session ends so that the wait does not hold the session up.
	go func() {
		<-sctx.Done()
		sess.conn.Close()
	}()

	if err := sess.handshake(ctx); err != nil {
		return false, err
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		sess.finish(sess.readLoop(sctx))
	}()
	go func() {
		defer wg.Done()
		sess.finish(sess.heartbeatLoop(sctx))
	}()

	// The source is paused again when the connection was opened while the
	// backup was paused.
	sess.b.mu.Lock()
	paused := sess.b.paused
	pauseSeq := sess.b.pauseSeq
	sess.b.mu.Unlock()
	if paused {
		if err := sess.pause(sctx, pauseSeq); err != nil {
			sess.finish(err)
		}
	}

	wg.Wait()
	return true, sess.errLocked()
}

// clearSession forgets the session when it is over.
//
// The pause stays: the source is paused again as soon as the connection is
// open again.
func (b *Backup) clearSession(sess *backupSession) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sess == sess {
		b.sess = nil
	}
}

// backupSession is the session of one connection of the backup.
type backupSession struct {
	b    *Backup
	conn *conn

	mu      sync.Mutex
	applied uint64
	epoch   uint64
	err     error
	resync  error

	// snapWriter receives the bytes of the snapshot. It is set when the
	// manifest of the snapshot arrives.
	snapWriter io.Writer
	// snapMeta is the manifest of the snapshot that is received.
	snapMeta SnapshotMeta
	// snapBytes is the number of bytes of the snapshot that arrived.
	snapBytes uint64

	cancel   context.CancelFunc
	once     sync.Once
	pausedCh chan Paused
	done     chan struct{}
}

// handshake sends the hello of the backup and reads the answer of the
// source.
func (sess *backupSession) handshake(ctx context.Context) error {
	applied, err := sess.b.sink.AppliedSeq(ctx)
	if err != nil {
		return err
	}
	token, err := LoadToken(sess.b.cfg.TokenFile)
	if err != nil {
		return err
	}
	nonce, err := newNonce()
	if err != nil {
		return err
	}
	hello := Hello{
		ProtocolVersion: ProtocolVersion,
		DatabaseID:      segment.DatabaseID(sess.b.cfg.DatabaseID),
		StreamID:        sess.b.cfg.StreamID,
		Epoch:           sess.b.sink.Epoch(),
		NeedsSnapshot:   sess.b.needsSnapshot(),
		BackupID:        sess.b.cfg.BackupID,
		AppliedSeq:      applied,
		Nonce:           nonce,
	}
	hello.MAC = clientMAC(token, nonce, hello.macPrefix())
	if err := sess.conn.writeMessage(hello); err != nil {
		return err
	}
	t, payload, err := sess.conn.readFrame()
	if err != nil {
		return err
	}
	switch t {
	case MsgHelloAck:
	case MsgResyncRequired:
		m, _ := decodeResyncRequired(payload)
		sess.b.setSourceEpoch(m.Epoch)
		return fmt.Errorf("%w: the source asks for a new snapshot: %s", ErrResyncRequired, m.Reason)
	case MsgError:
		m, _ := decodeError(payload)
		return fmt.Errorf("protocol: the source sent an error: %s", m.Message)
	default:
		return fmt.Errorf("%w: the answer of the source is %s", ErrInvalidFrame, t)
	}
	ack, err := decodeHelloAck(payload)
	if err != nil {
		return err
	}
	if ack.ProtocolVersion != ProtocolVersion {
		return fmt.Errorf("protocol: the source speaks the protocol version %d", ack.ProtocolVersion)
	}
	if !macEqual(ack.MAC, serverMAC(token, nonce, ack.Nonce, ack.macPrefix())) {
		return errors.New("protocol: the token does not match")
	}
	if ack.ResumeSeq != hello.AppliedSeq {
		return fmt.Errorf("protocol: the source continues after %d, the backup applied %d", ack.ResumeSeq, hello.AppliedSeq)
	}
	sess.mu.Lock()
	sess.applied = applied
	sess.epoch = ack.Epoch
	sess.mu.Unlock()
	sess.b.mu.Lock()
	sess.b.sourceEpoch = ack.Epoch
	sess.b.sess = sess
	sess.b.mu.Unlock()
	return sess.conn.writeMessage(FlowControl{MaxUnackedBytes: uint64(sess.b.cfg.MaxUnackedBytes)})
}

// finish records the error the session ended with and closes the connection.
func (sess *backupSession) finish(err error) {
	if err == nil {
		err = errSessionEnded
	}
	sess.once.Do(func() {
		sess.mu.Lock()
		sess.err = err
		cancel := sess.cancel
		sess.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		sess.conn.Close()
		close(sess.done)
	})
	// A resync matters more than an error of the transport: the peer may
	// close the connection while the message that asks for the snapshot is
	// read, and the write of the answer may fail first.
	if errors.Is(err, ErrResyncRequired) {
		sess.mu.Lock()
		sess.resync = err
		sess.mu.Unlock()
	}
}

// errLocked returns the error the session ended with.
func (sess *backupSession) errLocked() error {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.resync != nil {
		return sess.resync
	}
	if sess.err == nil {
		return errSessionEnded
	}
	return sess.err
}

// readLoop reads the messages of the source.
func (sess *backupSession) readLoop(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		t, payload, err := sess.conn.readFrame()
		if err != nil {
			if errors.Is(err, ErrInvalidFrame) {
				sess.conn.writeMessage(Error{Code: CodeProtocol, Message: "the frame is not a frame of the protocol"})
			}
			return err
		}
		switch t {
		case MsgBatch:
			if sess.b.isPaused() {
				// The backup is paused, the segments that arrive are
				// skipped. The source sends them again after the resume.
				continue
			}
			m, err := decodeBatch(payload)
			if err != nil {
				return err
			}
			if err := sess.applyBatch(ctx, m); err != nil {
				return err
			}
		case MsgSnapshotMeta:
			m, err := decodeSnapshotMeta(payload)
			if err != nil {
				return err
			}
			if err := sess.beginSnapshot(ctx, m); err != nil {
				return err
			}
		case MsgSnapshotData:
			if err := sess.writeSnapshotData(ctx, payload); err != nil {
				return err
			}
		case MsgHeartbeat:
			if _, err := decodeHeartbeat(payload); err != nil {
				return err
			}
			// The heartbeat is not answered. Both sides send their own,
			// an answer would only bounce between them.
		case MsgPaused:
			m, err := decodePaused(payload)
			if err != nil {
				return err
			}
			select {
			case sess.pausedCh <- m:
			default:
			}
		case MsgError:
			m, _ := decodeError(payload)
			return fmt.Errorf("protocol: the source sent an error: %s", m.Message)
		case MsgResyncRequired:
			m, _ := decodeResyncRequired(payload)
			sess.b.setSourceEpoch(m.Epoch)
			return fmt.Errorf("%w: the source asks for a new snapshot: %s", ErrResyncRequired, m.Reason)
		default:
			sess.conn.writeMessage(Error{Code: CodeProtocol, Message: fmt.Sprintf("the message %s is not expected", t)})
			return fmt.Errorf("%w: the message %s is not expected", ErrInvalidFrame, t)
		}
	}
}

// heartbeatLoop keeps the connection alive and holds the window of the
// source.
func (sess *backupSession) heartbeatLoop(ctx context.Context) error {
	ticker := time.NewTicker(sess.b.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sess.done:
			return sess.errLocked()
		case <-ticker.C:
			if err := sess.conn.writeMessage(Heartbeat{UnixMillis: uint64(time.Now().UnixMilli())}); err != nil {
				return err
			}
			if err := sess.conn.writeMessage(FlowControl{MaxUnackedBytes: uint64(sess.b.cfg.MaxUnackedBytes)}); err != nil {
				return err
			}
		}
	}
}

// beginSnapshot starts the installation of a snapshot.
//
// The manifest of the snapshot names the sequence number the snapshot is
// bound to and the size of its file. The bytes of the file follow in parts.
func (sess *backupSession) beginSnapshot(ctx context.Context, meta SnapshotMeta) error {
	if sess.b.snapSink == nil {
		return fmt.Errorf("%w: the backup does not take a snapshot", ErrInvalidMessage)
	}
	w, err := sess.b.snapSink.BeginSnapshot(ctx, meta)
	if err != nil {
		return err
	}
	sess.mu.Lock()
	sess.snapWriter = w
	sess.snapMeta = meta
	sess.snapBytes = 0
	sess.mu.Unlock()
	return nil
}

// writeSnapshotData writes a part of the file of the snapshot.
//
// The snapshot is installed when the last byte of the file arrived. The
// backup acknowledges the baseline sequence of the snapshot, the source
// continues with the transactions that follow it.
func (sess *backupSession) writeSnapshotData(ctx context.Context, data []byte) error {
	sess.mu.Lock()
	w := sess.snapWriter
	sess.mu.Unlock()
	if w == nil {
		return fmt.Errorf("%w: the bytes of the snapshot came before its manifest", ErrInvalidMessage)
	}
	n, err := w.Write(data)
	if err != nil {
		return err
	}
	sess.mu.Lock()
	sess.snapBytes += uint64(n)
	complete := sess.snapBytes == sess.snapMeta.Bytes
	sess.mu.Unlock()
	if !complete {
		return nil
	}
	if err := sess.b.snapSink.CommitSnapshot(ctx); err != nil {
		return err
	}
	sess.mu.Lock()
	sess.applied = sess.snapMeta.BaselineSeq
	sess.epoch = sess.snapMeta.Epoch
	sess.snapWriter = nil
	sess.mu.Unlock()
	sess.b.setLastAck(time.Now())
	return sess.conn.writeMessage(Ack{AppliedSeq: sess.appliedLocked(), Epoch: sess.epochLocked()})
}

// applyBatch applies the segments of a batch.
//
// Every segment is acknowledged when it is applied. A segment that was
// applied already is acknowledged again without being applied. The backup
// stops at the segment it is paused at: the segments that follow it are
// sent again after the resume.
func (sess *backupSession) applyBatch(ctx context.Context, b Batch) error {
	for _, raw := range b.Segments {
		if sess.b.isPaused() {
			return nil
		}
		// The payload of the frame holds the bytes of the segment, the
		// segment cannot reach into the next one.
		seg, err := segment.Read(bytes.NewReader(raw))
		if err != nil {
			sess.sendResync(ResyncCorrupt, sess.epochLocked(), 0)
			return fmt.Errorf("%w: the segment does not hold what it says: %v", ErrResyncRequired, err)
		}
		if seg.Header.DatabaseID != segment.DatabaseID(sess.b.cfg.DatabaseID) {
			sess.sendResync(ResyncIdentity, seg.Header.Epoch, seg.Header.SeqStart)
			return fmt.Errorf("%w: the segment belongs to another database", ErrResyncRequired)
		}
		if seg.Header.Epoch != sess.epochLocked() {
			sess.sendResync(ResyncEpochMismatch, seg.Header.Epoch, seg.Header.SeqStart)
			return fmt.Errorf("%w: the epoch of the segment is %d, the epoch of the backup is %d", ErrResyncRequired, seg.Header.Epoch, sess.epochLocked())
		}
		applied, err := sess.applySegment(ctx, seg)
		if err != nil {
			return err
		}
		// The transactions of the segment are durable before the backup
		// acknowledges them: the acknowledgement means that the sequence
		// number it names is persisted.
		if f, ok := sess.b.sink.(Flusher); ok {
			if err := f.Flush(ctx); err != nil {
				return err
			}
		}
		sess.b.setLastAck(time.Now())
		if err := sess.conn.writeMessage(Ack{AppliedSeq: applied, Epoch: sess.epochLocked()}); err != nil {
			return err
		}
	}
	return nil
}

// applySegment applies the transactions of a segment that were not applied
// yet.
//
// The transactions up to the applied sequence are skipped, a segment that was
// applied already is not applied again.
func (sess *backupSession) applySegment(ctx context.Context, seg *segment.Segment) (uint64, error) {
	applied := sess.appliedLocked()
	if seg.Header.SeqStart > applied+1 {
		sess.sendResync(ResyncGap, seg.Header.Epoch, seg.Header.SeqStart)
		return applied, fmt.Errorf("%w: the segment starts at %d, the backup applied %d", ErrResyncRequired, seg.Header.SeqStart, applied)
	}
	for _, tx := range seg.Txs {
		if tx.Seq <= applied {
			continue
		}
		if tx.Seq != applied+1 {
			sess.sendResync(ResyncGap, seg.Header.Epoch, tx.Seq)
			return applied, fmt.Errorf("%w: the transaction %d does not follow %d", ErrResyncRequired, tx.Seq, applied)
		}
		if err := sess.b.sink.Apply(ctx, tx); err != nil {
			return applied, err
		}
		applied = tx.Seq
	}
	sess.mu.Lock()
	sess.applied = applied
	sess.mu.Unlock()
	return applied, nil
}

// appliedLocked returns the sequence number the backup applied.
func (sess *backupSession) appliedLocked() uint64 {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.applied
}

// epochLocked returns the generation of the chain the backup is part of.
func (sess *backupSession) epochLocked() uint64 {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.epoch
}

// pause asks the source to stop pushing and waits for its answer.
func (sess *backupSession) pause(ctx context.Context, appliedSeq uint64) error {
	if err := sess.conn.writeMessage(Pause{AppliedSeq: appliedSeq}); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sess.done:
		return sess.errLocked()
	case <-sess.pausedCh:
		return nil
	}
}

// sendResync tells the source that a new snapshot is needed.
func (sess *backupSession) sendResync(reason ResyncReason, epoch, seq uint64) {
	sess.conn.writeMessage(ResyncRequired{Reason: reason, Epoch: epoch, Seq: seq})
}

// backoffDelay returns the delay before the next connection attempt.
//
// The delay doubles with every attempt and is capped at max. The jitter
// keeps the backups of a deployment from reconnecting at the same time.
func backoffDelay(min, max time.Duration, attempt int) time.Duration {
	d := min
	for i := 0; i < attempt && d < max; i++ {
		if d > max/2 {
			d = max
			break
		}
		d *= 2
	}
	if d > max {
		d = max
	}
	d += time.Duration(rand.Int64N(int64(d/2) + 1))
	if d > max {
		d = max
	}
	return d
}

// sleep waits for the duration or until the context is done.
func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
