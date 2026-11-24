package protocol

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/cnlangzi/sqlite-relay/segment"
)

// errSessionEnded reports that the session of a connection is over.
var errSessionEnded = errors.New("protocol: the session ended")

// pollInterval is the time the source waits before it asks the segment
// source for segments again.
const pollInterval = 50 * time.Millisecond

// Source accepts the connections of the backups and pushes the segments of
// the spool.
type Source struct {
	cfg Config
	src SegmentSource

	mu      sync.Mutex
	applied uint64
	lastErr error
}

// NewSource creates a source that pushes the segments of src.
func NewSource(cfg Config, src SegmentSource) (*Source, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if src == nil {
		return nil, errors.New("protocol: the segment source is nil")
	}
	return &Source{cfg: cfg, src: src}, nil
}

// AppliedSeq returns the sequence number the backup acknowledged last.
//
// It is the highest sequence number the backup applied and persisted. It is
// zero when the backup did not acknowledge anything yet.
func (s *Source) AppliedSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied
}

// LastError returns the error the last connection ended with.
func (s *Source) LastError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

// Serve accepts the connections of the backups.
//
// It runs one connection at a time. A connection that failed is ended and
// the next one is accepted. A connection that needs a new snapshot ends the
// serve with ErrResyncRequired.
func (s *Source) Serve(ctx context.Context, ln net.Listener) error {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = ln.Close()
		case <-stop:
		}
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		err = s.serveConn(ctx, c)
		if errors.Is(err, ErrResyncRequired) {
			return err
		}
		s.mu.Lock()
		s.lastErr = err
		s.mu.Unlock()
	}
}

// serveConn runs the session of one connection.
func (s *Source) serveConn(ctx context.Context, c net.Conn) error {
	sess := &sourceSession{
		src:  s,
		conn: newConn(c, s.cfg.MaxFrameBytes, s.cfg.IdleTimeout),
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
	defer func() { _ = sess.conn.Close() }()
	return sess.run(ctx)
}

// allowed reports whether the backup may connect.
func (s *Source) allowed(backupID string) bool {
	if len(s.cfg.AllowedBackups) == 0 {
		return true
	}
	return slices.Contains(s.cfg.AllowedBackups, backupID)
}

// setApplied records the sequence number the backup acknowledged.
func (s *Source) setApplied(seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = seq
}

// inflight is a segment the source sent and that the backup did not
// acknowledge yet.
type inflight struct {
	// seqEnd is the last sequence number of the segment.
	seqEnd uint64
	// bytes is the size of the segment.
	bytes int64
}

// sourceSession is the session of one connection of the source.
type sourceSession struct {
	src  *Source
	conn *conn

	mu        sync.Mutex
	applied   uint64
	epoch     uint64
	limit     int64
	unacked   int64
	pending   []inflight
	paused    bool
	pausedAck bool
	sent      uint64
	fetched   uint64
	lastSent  uint64
	err       error
	resync    error

	// resetQueue tells the send loop that the segments it fetched are
	// not the ones the source continues with.
	resetQueue bool

	cancel context.CancelFunc
	once   sync.Once
	wake   chan struct{}
	done   chan struct{}
}

// run performs the handshake and the session of the connection.
func (sess *sourceSession) run(ctx context.Context) error {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sess.mu.Lock()
	sess.cancel = cancel
	sess.mu.Unlock()

	// The reader waits for the peer, the connection is closed when the
	// session ends so that the wait does not hold the session up.
	go func() {
		<-sctx.Done()
		_ = sess.conn.Close()
	}()

	if err := sess.handshake(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		sess.finish(sess.readLoop(sctx))
	}()
	go func() {
		defer wg.Done()
		sess.finish(sess.sendLoop(sctx))
	}()
	go func() {
		defer wg.Done()
		sess.finish(sess.heartbeatLoop(sctx))
	}()
	wg.Wait()
	return sess.errLocked()
}

// finish records the error the session ended with and closes the connection.
func (sess *sourceSession) finish(err error) {
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
		_ = sess.conn.Close()
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
func (sess *sourceSession) errLocked() error {
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

// handshake reads the hello of the backup and answers it.
//
// Every check that fails ends the connection with an error message, no
// segment is pushed to a backup that did not prove itself.
func (sess *sourceSession) handshake() error {
	t, payload, err := sess.conn.readFrame()
	if err != nil {
		if errors.Is(err, ErrInvalidFrame) {
			sess.sendError(CodeProtocol, "the frame is not a frame of the protocol")
		}
		return err
	}
	if t != MsgHello {
		sess.sendError(CodeProtocol, "the first message is not a hello")
		return fmt.Errorf("%w: the first message is %s", ErrInvalidFrame, t)
	}
	hello, err := decodeHello(payload)
	if err != nil {
		sess.sendError(CodeProtocol, "the hello is malformed")
		return err
	}
	if hello.ProtocolVersion != ProtocolVersion {
		sess.sendError(CodeVersion, fmt.Sprintf("the protocol version %d is not supported", hello.ProtocolVersion))
		return fmt.Errorf("protocol: the protocol version %d is not supported", hello.ProtocolVersion)
	}
	token, err := LoadToken(sess.src.cfg.TokenFile)
	if err != nil {
		sess.sendError(CodeAuth, "the token could not be read")
		return err
	}
	if !macEqual(hello.MAC, clientMAC(token, hello.Nonce, hello.macPrefix())) {
		sess.sendError(CodeAuth, "the token does not match")
		return errors.New("protocol: the token does not match")
	}
	if !sess.src.allowed(hello.BackupID) {
		sess.sendError(CodeIdentity, "the backup is not allowed to connect")
		return fmt.Errorf("protocol: the backup %q is not allowed to connect", hello.BackupID)
	}
	if hello.DatabaseID != segment.DatabaseID(sess.src.cfg.DatabaseID) {
		sess.sendError(CodeIdentity, "the database does not match")
		return errors.New("protocol: the database does not match")
	}
	if hello.StreamID != sess.src.cfg.StreamID {
		sess.sendError(CodeIdentity, "the stream does not match")
		return errors.New("protocol: the stream does not match")
	}
	epoch := sess.src.src.Epoch()
	if hello.Epoch != epoch {
		sess.sendResync(ResyncEpochMismatch, epoch, hello.AppliedSeq)
		return fmt.Errorf("%w: the epoch of the backup is %d, the epoch of the source is %d", ErrResyncRequired, hello.Epoch, epoch)
	}
	if oldest := sess.src.src.OldestSeq(); oldest > hello.AppliedSeq+1 {
		sess.sendResync(ResyncGap, epoch, oldest)
		return fmt.Errorf("%w: the lowest sequence number of the source is %d, the backup applied %d", ErrResyncRequired, oldest, hello.AppliedSeq)
	}
	nonce, err := newNonce()
	if err != nil {
		return err
	}
	ack := HelloAck{
		ProtocolVersion: ProtocolVersion,
		Epoch:           epoch,
		ResumeSeq:       hello.AppliedSeq,
		Nonce:           nonce,
	}
	ack.MAC = serverMAC(token, hello.Nonce, nonce, ack.macPrefix())
	if err := sess.conn.writeMessage(ack); err != nil {
		return err
	}
	sess.mu.Lock()
	sess.applied = hello.AppliedSeq
	sess.epoch = epoch
	sess.sent = hello.AppliedSeq
	sess.fetched = hello.AppliedSeq
	sess.limit = sess.src.cfg.MaxUnackedBytes
	sess.mu.Unlock()
	return nil
}

// readLoop reads the messages of the backup.
func (sess *sourceSession) readLoop(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		t, payload, err := sess.conn.readFrame()
		if err != nil {
			if errors.Is(err, ErrInvalidFrame) {
				sess.sendError(CodeProtocol, "the frame is not a frame of the protocol")
			}
			return err
		}
		switch t {
		case MsgAck:
			m, err := decodeAck(payload)
			if err != nil {
				sess.sendError(CodeProtocol, "the acknowledgement is malformed")
				return err
			}
			sess.onAck(m)
		case MsgFlowControl:
			m, err := decodeFlowControl(payload)
			if err != nil {
				sess.sendError(CodeProtocol, "the flow control message is malformed")
				return err
			}
			sess.onFlowControl(m)
		case MsgPause:
			m, err := decodePause(payload)
			if err != nil {
				sess.sendError(CodeProtocol, "the pause is malformed")
				return err
			}
			sess.onPause(m)
		case MsgResume:
			m, err := decodeResume(payload)
			if err != nil {
				sess.sendError(CodeProtocol, "the resume is malformed")
				return err
			}
			sess.onResume(m)
		case MsgHeartbeat:
			if _, err := decodeHeartbeat(payload); err != nil {
				sess.sendError(CodeProtocol, "the heartbeat is malformed")
				return err
			}
			// The heartbeat is not answered. Both sides send their own,
			// an answer would only bounce between them.
		case MsgError:
			m, _ := decodeError(payload)
			return fmt.Errorf("protocol: the backup sent an error: %s", m.Message)
		case MsgResyncRequired:
			m, _ := decodeResyncRequired(payload)
			return fmt.Errorf("%w: the backup asks for a new snapshot: %s", ErrResyncRequired, m.Reason)
		default:
			sess.sendError(CodeProtocol, fmt.Sprintf("the message %s is not expected", t))
			return fmt.Errorf("%w: the message %s is not expected", ErrInvalidFrame, t)
		}
	}
}

// sendLoop pushes the segments of the source.
func (sess *sourceSession) sendLoop(ctx context.Context) error {
	// The queue holds the segments that were fetched and that were not
	// sent yet. They are fetched once and not once per batch.
	var queue []Segment
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		sess.mu.Lock()
		paused := sess.paused
		pausedAck := sess.pausedAck
		limit := sess.limit
		unacked := sess.unacked
		sent := sess.sent
		fetched := sess.fetched
		reset := sess.resetQueue
		sess.mu.Unlock()

		if paused {
			// The answer is sent here and not in the read loop: the send
			// loop is the only party that knows that no batch follows it.
			if !pausedAck {
				sess.mu.Lock()
				last := sess.lastSent
				sess.pausedAck = true
				sess.mu.Unlock()
				if err := sess.conn.writeMessage(Paused{LastSentSeq: last}); err != nil {
					return err
				}
			}
			if err := sess.wait(ctx, pollInterval); err != nil {
				return err
			}
			continue
		}
		if reset {
			// The backup asked the source to continue after a sequence
			// number, the segments that were fetched are fetched again.
			sess.mu.Lock()
			sess.resetQueue = false
			sess.mu.Unlock()
			queue = nil
			fetched = sent
		}

		// The batch never holds more than the window of the backup
		// allows, so that the bytes in flight stay below the limit.
		budget := sess.src.cfg.MaxBatchBytes
		if room := limit - unacked; room < budget {
			budget = room
		}
		if budget <= 0 {
			if err := sess.wait(ctx, pollInterval); err != nil {
				return err
			}
			continue
		}
		if len(queue) == 0 {
			segs, err := sess.src.src.SegmentsAfter(ctx, fetched, budget)
			if err != nil {
				return err
			}
			if len(segs) == 0 {
				if err := sess.wait(ctx, pollInterval); err != nil {
					return err
				}
				continue
			}
			queue = segs
			sess.mu.Lock()
			sess.fetched = segs[len(segs)-1].SeqEnd
			sess.mu.Unlock()
		}
		chosen, batch, err := packBatch(queue, budget, sess.src.cfg.MaxFrameBytes)
		if err != nil {
			return err
		}
		if err := sess.conn.writeMessage(batch); err != nil {
			return err
		}
		sess.advance(sent, chosen)
		queue = queue[len(chosen):]
	}
}

// advance records the segments that were sent.
//
// The cursor only moves when it did not move while the batch was written: a
// resume that arrived in the meantime names the sequence number the source
// continues after and it wins.
func (sess *sourceSession) advance(cursor uint64, chosen []Segment) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	moved := sess.sent != cursor
	for _, seg := range chosen {
		if !moved {
			sess.sent = seg.SeqEnd
		}
		sess.lastSent = seg.SeqEnd
		sess.pending = append(sess.pending, inflight{seqEnd: seg.SeqEnd, bytes: int64(len(seg.Bytes))})
		sess.unacked += int64(len(seg.Bytes))
	}
}

// heartbeatLoop keeps the connection alive.
func (sess *sourceSession) heartbeatLoop(ctx context.Context) error {
	ticker := time.NewTicker(sess.src.cfg.HeartbeatInterval)
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
		}
	}
}

// onAck records the sequence number the backup applied.
func (sess *sourceSession) onAck(m Ack) {
	if m.Epoch != sess.epoch {
		// The backup is part of another generation of the chain, the
		// sequence numbers of the two do not mean the same thing.
		sess.sendResync(ResyncEpochMismatch, m.Epoch, m.AppliedSeq)
		return
	}
	sess.mu.Lock()
	if m.AppliedSeq > sess.applied {
		sess.applied = m.AppliedSeq
	}
	sess.pending, sess.unacked = dropAcked(sess.pending, sess.unacked, sess.applied)
	applied := sess.applied
	sess.mu.Unlock()
	sess.src.setApplied(applied)
	sess.wakeup()
}

// onFlowControl records the limit of the bytes in flight.
//
// The limit is clamped, a peer that names a limit that does not fit into an
// int does not open the window of the source.
func (sess *sourceSession) onFlowControl(m FlowControl) {
	limit := int64(m.MaxUnackedBytes)
	if limit < 0 || limit > math.MaxInt32 {
		limit = math.MaxInt32
	}
	sess.mu.Lock()
	sess.limit = limit
	sess.mu.Unlock()
	sess.wakeup()
}

// onPause stops the push of segments.
//
// The answer is not sent here: the send loop is the only party that knows
// that no batch follows the answer.
func (sess *sourceSession) onPause(m Pause) {
	sess.mu.Lock()
	sess.paused = true
	sess.pausedAck = false
	sess.pending, sess.unacked = dropAcked(sess.pending, sess.unacked, m.AppliedSeq)
	sess.mu.Unlock()
	sess.wakeup()
}

// onResume starts the push of segments again.
func (sess *sourceSession) onResume(m Resume) {
	sess.mu.Lock()
	sess.paused = false
	sess.pausedAck = false
	sess.sent = m.AppliedSeq
	sess.fetched = m.AppliedSeq
	sess.resetQueue = true
	sess.pending, sess.unacked = dropAcked(sess.pending, sess.unacked, m.AppliedSeq)
	sess.mu.Unlock()
	sess.wakeup()
}

// dropAcked returns the segments that were not acknowledged yet and the
// number of bytes that are in flight.
func dropAcked(list []inflight, unacked int64, applied uint64) ([]inflight, int64) {
	keep := list[:0]
	for _, p := range list {
		if p.seqEnd <= applied {
			unacked -= p.bytes
			continue
		}
		keep = append(keep, p)
	}
	return keep, unacked
}

// wakeup tells the send loop that the state of the session changed.
func (sess *sourceSession) wakeup() {
	select {
	case sess.wake <- struct{}{}:
	default:
	}
}

// wait waits for the state of the session to change or for the timeout.
func (sess *sourceSession) wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sess.done:
		return sess.errLocked()
	case <-sess.wake:
		return nil
	case <-timer.C:
		return nil
	}
}

// sendError tells the peer that the connection is ended.
func (sess *sourceSession) sendError(code ErrorCode, msg string) {
	_ = sess.conn.writeMessage(Error{Code: code, Message: msg})
}

// sendResync tells the peer that a new snapshot is needed.
func (sess *sourceSession) sendResync(reason ResyncReason, epoch, seq uint64) {
	_ = sess.conn.writeMessage(ResyncRequired{Reason: reason, Epoch: epoch, Seq: seq})
}

// packBatch packs the segments into a batch.
//
// A batch holds at most MaxSegmentsPerBatch segments and its payload holds
// at most maxBatch bytes. A segment that does not fit into a batch is sent
// in a batch of its own. A segment that does not fit into a frame is an
// error, it is neither cut nor split.
func packBatch(segs []Segment, maxBatch, maxFrame int64) ([]Segment, Batch, error) {
	var chosen []Segment
	var raw [][]byte
	size := int64(0)
	for _, seg := range segs {
		if len(seg.Bytes) == 0 {
			return nil, Batch{}, fmt.Errorf("protocol: the segment %d holds no bytes", seg.SeqStart)
		}
		n := int64(4 + len(seg.Bytes))
		if n > maxFrame {
			return nil, Batch{}, fmt.Errorf("protocol: the segment %d holds %d bytes, a frame holds %d bytes", seg.SeqStart, len(seg.Bytes), maxFrame)
		}
		if len(chosen) > 0 && (len(chosen) >= MaxSegmentsPerBatch || size+n > maxBatch) {
			break
		}
		chosen = append(chosen, seg)
		raw = append(raw, seg.Bytes)
		size += n
	}
	return chosen, Batch{Segments: raw}, nil
}
