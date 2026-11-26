package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"slices"
	"testing"
	"time"
)

func TestSourceRejectsBadHandshake(t *testing.T) {
	cases := []struct {
		name  string
		hello func(t *testing.T, cfg Config) Hello
		code  ErrorCode
	}{
		{
			name: "a wrong token",
			hello: func(t *testing.T, cfg Config) Hello {
				return testHello(t, cfg, "the token of another deployment")
			},
			code: CodeAuth,
		},
		{
			name: "a backup that is not allowed",
			hello: func(t *testing.T, cfg Config) Hello {
				h := testHello(t, cfg, testToken)
				h.BackupID = "backup-2"
				return signHello(h, testToken)
			},
			code: CodeIdentity,
		},
		{
			name: "another database",
			hello: func(t *testing.T, cfg Config) Hello {
				h := testHello(t, cfg, testToken)
				h.DatabaseID[0] = 0xff
				return signHello(h, testToken)
			},
			code: CodeIdentity,
		},
		{
			name: "another stream",
			hello: func(t *testing.T, cfg Config) Hello {
				h := testHello(t, cfg, testToken)
				h.StreamID = 9
				return signHello(h, testToken)
			},
			code: CodeIdentity,
		},
		{
			name: "another protocol version",
			hello: func(t *testing.T, cfg Config) Hello {
				h := testHello(t, cfg, testToken)
				h.ProtocolVersion = ProtocolVersion + 1
				return signHello(h, testToken)
			},
			code: CodeVersion,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.AllowedBackups = []string{cfg.BackupID}
			src := &fakeSource{}
			addr, _, _ := startSource(t, cfg, src)

			cl := dialClient(t, addr)
			cl.write(c.hello(t, cfg))
			typ, payload, err := cl.read()
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if typ != MsgError {
				t.Fatalf("the first message is %s, want ERROR", typ)
			}
			m, err := decodeError(payload)
			if err != nil {
				t.Fatalf("decodeError: %v", err)
			}
			if m.Code != c.code {
				t.Errorf("the code is %s, want %s", m.Code, c.code)
			}
			types := []MessageType{typ}
			types = append(types, cl.readUntilClose()...)
			if slices.Contains(types, MsgBatch) {
				t.Error("the source pushed a segment to a backup that did not prove itself")
			}
		})
	}
}

func TestSourceRejectsAnInvalidFrame(t *testing.T) {
	badMagic := EncodeFrame(MsgHello, nil)
	badMagic[0] = 'X'
	var longLength [FrameHeaderSize]byte
	copy(longLength[0:4], FrameMagic)
	longLength[4] = FrameVersion
	longLength[5] = byte(MsgBatch)
	binary.BigEndian.PutUint32(longLength[8:12], uint32(MaxFrameBytes+1))

	cases := []struct {
		name  string
		frame []byte
		first bool
	}{
		{"a wrong magic", badMagic, true},
		{"a length that is too long", longLength[:], true},
		{"a wrong magic after the handshake", badMagic, false},
		{"a length that is too long after the handshake", longLength[:], false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := testConfig(t)
			src := &fakeSource{}
			addr, _, _ := startSource(t, cfg, src)

			cl := dialClient(t, addr)
			if !c.first {
				cl.write(testHello(t, cfg, testToken))
				if _, _, err := cl.read(); err != nil {
					t.Fatalf("read: %v", err)
				}
			}
			if _, err := cl.conn.Write(c.frame); err != nil {
				t.Fatalf("write: %v", err)
			}
			typ, _, err := cl.read()
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if typ != MsgError {
				t.Fatalf("the message is %s, want ERROR", typ)
			}
		})
	}
}

func TestSourceAnswersHello(t *testing.T) {
	cfg := testConfig(t)
	src := &fakeSource{}
	addr, _, _ := startSource(t, cfg, src)

	cl := dialClient(t, addr)
	hello := testHello(t, cfg, testToken)
	hello.AppliedSeq = 5
	cl.write(signHello(hello, testToken))

	typ, payload, err := cl.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgHelloAck {
		t.Fatalf("the first message is %s, want HELLO_ACK", typ)
	}
	ack, err := decodeHelloAck(payload)
	if err != nil {
		t.Fatalf("decodeHelloAck: %v", err)
	}
	if ack.ProtocolVersion != ProtocolVersion {
		t.Errorf("the protocol version is %d, want %d", ack.ProtocolVersion, ProtocolVersion)
	}
	if ack.ResumeSeq != hello.AppliedSeq {
		t.Errorf("the resume sequence is %d, want %d", ack.ResumeSeq, hello.AppliedSeq)
	}
	if !macEqual(ack.MAC, serverMAC([]byte(testToken), hello.Nonce, ack.Nonce, ack.macPrefix())) {
		t.Error("the answer of the source does not carry the right code")
	}
}

func TestSourceRemembersTheErrorOfTheLastConnection(t *testing.T) {
	cfg := testConfig(t)
	src := &fakeSource{}
	addr, source, _ := startSource(t, cfg, src)
	if source.LastError() != nil {
		t.Fatalf("the source holds the error %v before a connection", source.LastError())
	}

	cl := dialClient(t, addr)
	cl.write(testHello(t, cfg, "the token of another deployment"))
	cl.readUntilClose()
	if !waitFor(t, 5*time.Second, func() bool { return source.LastError() != nil }) {
		t.Fatal("the source did not remember the error of the connection")
	}
}

func TestSourceRemembersTheResyncOfTheBackup(t *testing.T) {
	cfg := testConfig(t)
	src := &fakeSource{}
	addr, source, _ := startSource(t, cfg, src)

	cl := dialClient(t, addr)
	cl.write(testHello(t, cfg, testToken))
	if _, _, err := cl.read(); err != nil {
		t.Fatalf("read: %v", err)
	}
	cl.write(ResyncRequired{Reason: ResyncGap, Epoch: 0, Seq: 3})
	if !waitFor(t, 5*time.Second, func() bool { return source.LastError() != nil }) {
		t.Fatal("the source did not remember the resync")
	}
	if !errors.Is(source.LastError(), ErrResyncRequired) {
		t.Errorf("the source holds the error %v, want %v", source.LastError(), ErrResyncRequired)
	}
}

func TestSourceDropsTheAcknowledgedSegments(t *testing.T) {
	sess := &sourceSession{src: &Source{}}
	sess.limit = 1000
	sess.sent = 10
	sess.pending = []inflight{{seqEnd: 10, bytes: 100}}
	sess.unacked = 100

	sess.onAck(Ack{AppliedSeq: 10})
	if sess.unacked != 0 {
		t.Errorf("the bytes in flight are %d, want 0", sess.unacked)
	}
	if len(sess.pending) != 0 {
		t.Errorf("the list of the segments in flight holds %d segments, want 0", len(sess.pending))
	}
}

func TestSourceResyncsOnEpochMismatch(t *testing.T) {
	cfg := testConfig(t)
	src := &fakeSource{}
	src.setEpoch(3)
	addr, source, _ := startSource(t, cfg, src)

	cl := dialClient(t, addr)
	cl.write(testHello(t, cfg, testToken))

	typ, payload, err := cl.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgResyncRequired {
		t.Fatalf("the message is %s, want RESYNC_REQUIRED", typ)
	}
	m, err := decodeResyncRequired(payload)
	if err != nil {
		t.Fatalf("decodeResyncRequired: %v", err)
	}
	if m.Reason != ResyncEpochMismatch {
		t.Errorf("the reason is %s, want the epoch mismatch", m.Reason)
	}
	// The source keeps serving: the backup asks for a snapshot in the hello
	// of its next connection.
	if !waitFor(t, 5*time.Second, func() bool { return source.LastError() != nil }) {
		t.Fatal("the source did not remember the resync")
	}
}

func TestSourceResyncsOnGap(t *testing.T) {
	cfg := testConfig(t)
	src := &fakeSource{}
	src.add(Segment{SeqStart: 5, SeqEnd: 6, Bytes: []byte("a segment")})
	addr, source, _ := startSource(t, cfg, src)

	cl := dialClient(t, addr)
	cl.write(testHello(t, cfg, testToken))

	typ, payload, err := cl.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgResyncRequired {
		t.Fatalf("the message is %s, want RESYNC_REQUIRED", typ)
	}
	m, err := decodeResyncRequired(payload)
	if err != nil {
		t.Fatalf("decodeResyncRequired: %v", err)
	}
	if m.Reason != ResyncGap {
		t.Errorf("the reason is %s, want the gap", m.Reason)
	}
	// The source keeps serving: the backup asks for a snapshot in the hello
	// of its next connection.
	if !waitFor(t, 5*time.Second, func() bool { return source.LastError() != nil }) {
		t.Fatal("the source did not remember the resync")
	}
}

func TestSourceStopsWhenTheWindowIsFull(t *testing.T) {
	cfg := testConfig(t)
	cfg.HeartbeatInterval = 10 * time.Second
	src := &fakeSource{}
	seg1 := Segment{SeqStart: 1, SeqEnd: 1, Bytes: testSegment(t, cfg.DatabaseID, 0, 1, 1)}
	seg2 := Segment{SeqStart: 2, SeqEnd: 2, Bytes: testSegment(t, cfg.DatabaseID, 0, 2, 1)}
	src.add(seg1, seg2)
	cfg.MaxBatchBytes = int64(len(seg1.Bytes)) + 4
	cfg.MaxUnackedBytes = int64(len(seg1.Bytes))
	addr, _, _ := startSource(t, cfg, src)

	cl := dialClient(t, addr)
	cl.write(testHello(t, cfg, testToken))
	typ, _, err := cl.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgHelloAck {
		t.Fatalf("the first message is %s, want HELLO_ACK", typ)
	}
	typ, payload, err := cl.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgBatch {
		t.Fatalf("the message is %s, want BATCH", typ)
	}
	batch, err := decodeBatch(payload)
	if err != nil {
		t.Fatalf("decodeBatch: %v", err)
	}
	if len(batch.Segments) != 1 {
		t.Fatalf("the batch holds %d segments, want 1", len(batch.Segments))
	}

	typ, _, err = cl.readWithin(300 * time.Millisecond)
	if err == nil {
		t.Fatalf("the source sent %s while the window was full", typ)
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("the error is %v, want a timeout", err)
	}

	cl.write(FlowControl{MaxUnackedBytes: uint64(2 * len(seg1.Bytes))})
	typ, payload, err = cl.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgBatch {
		t.Fatalf("the message is %s, want BATCH", typ)
	}
	batch, err = decodeBatch(payload)
	if err != nil {
		t.Fatalf("decodeBatch: %v", err)
	}
	if len(batch.Segments) != 1 {
		t.Fatalf("the batch holds %d segments, want 1", len(batch.Segments))
	}
	if !bytes.Equal(batch.Segments[0], seg2.Bytes) {
		t.Error("the batch holds another segment than the source")
	}
}

func TestSourceCursorDoesNotMoveOverAResume(t *testing.T) {
	sess := &sourceSession{src: &Source{}}
	sess.sent = 6
	chosen := []Segment{{SeqStart: 7, SeqEnd: 9, Bytes: []byte("a segment")}}

	// A resume that arrived while the batch was written names the sequence
	// number the source continues after, it wins over the batch.
	sess.onResume(Resume{AppliedSeq: 3})
	sess.advance(6, chosen)
	if sess.sent != 3 {
		t.Errorf("the cursor is %d, want 3", sess.sent)
	}
}

func TestSourceResumesAfterTheAppliedSequence(t *testing.T) {
	for range 10 {
		resumeAfterAppliedSequence(t)
	}
}

func resumeAfterAppliedSequence(t *testing.T) {
	t.Helper()
	cfg := testConfig(t)
	cfg.HeartbeatInterval = 10 * time.Second
	src := &fakeSource{}
	seg1 := Segment{SeqStart: 1, SeqEnd: 3, Bytes: testSegment(t, cfg.DatabaseID, 0, 1, 3)}
	seg2 := Segment{SeqStart: 4, SeqEnd: 6, Bytes: testSegment(t, cfg.DatabaseID, 0, 4, 3)}
	src.add(seg1, seg2)
	addr, _, _ := startSource(t, cfg, src)

	cl := dialClient(t, addr)
	cl.write(testHello(t, cfg, testToken))
	if _, _, err := cl.read(); err != nil {
		t.Fatalf("read: %v", err)
	}
	typ, payload, err := cl.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgBatch {
		t.Fatalf("the message is %s, want BATCH", typ)
	}
	batch, err := decodeBatch(payload)
	if err != nil {
		t.Fatalf("decodeBatch: %v", err)
	}
	if len(batch.Segments) != 2 {
		t.Fatalf("the batch holds %d segments, want 2", len(batch.Segments))
	}

	// The backup applied the first segment and asks the source to continue
	// after it.
	cl.write(Ack{AppliedSeq: 3})
	cl.write(Resume{AppliedSeq: 3})
	typ, payload, err = cl.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgBatch {
		t.Fatalf("the message is %s, want BATCH", typ)
	}
	batch, err = decodeBatch(payload)
	if err != nil {
		t.Fatalf("decodeBatch: %v", err)
	}
	if len(batch.Segments) != 1 {
		t.Fatalf("the batch holds %d segments, want 1", len(batch.Segments))
	}
	if !bytes.Equal(batch.Segments[0], seg2.Bytes) {
		t.Error("the source did not continue after the applied sequence")
	}
}

func TestSourceStopsOnPause(t *testing.T) {
	cfg := testConfig(t)
	cfg.HeartbeatInterval = 10 * time.Second
	src := &fakeSource{}
	seg1 := Segment{SeqStart: 1, SeqEnd: 3, Bytes: testSegment(t, cfg.DatabaseID, 0, 1, 3)}
	seg2 := Segment{SeqStart: 4, SeqEnd: 6, Bytes: testSegment(t, cfg.DatabaseID, 0, 4, 3)}
	src.add(seg1)
	addr, _, _ := startSource(t, cfg, src)

	cl := dialClient(t, addr)
	cl.write(testHello(t, cfg, testToken))
	if _, _, err := cl.read(); err != nil {
		t.Fatalf("read: %v", err)
	}
	typ, payload, err := cl.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgBatch {
		t.Fatalf("the message is %s, want BATCH", typ)
	}
	batch, err := decodeBatch(payload)
	if err != nil {
		t.Fatalf("decodeBatch: %v", err)
	}
	if len(batch.Segments) != 1 {
		t.Fatalf("the batch holds %d segments, want 1", len(batch.Segments))
	}

	cl.write(Pause{AppliedSeq: 3})
	typ, payload, err = cl.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgPaused {
		t.Fatalf("the message is %s, want PAUSED", typ)
	}
	paused, err := decodePaused(payload)
	if err != nil {
		t.Fatalf("decodePaused: %v", err)
	}
	if paused.LastSentSeq != seg1.SeqEnd {
		t.Errorf("the source names the sequence number %d, want %d", paused.LastSentSeq, seg1.SeqEnd)
	}

	// The source keeps the connection but stops pushing, even when a new
	// segment shows up.
	src.add(seg2)
	typ, _, err = cl.readWithin(300 * time.Millisecond)
	if err == nil {
		t.Fatalf("the source sent %s while it was paused", typ)
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("the error is %v, want a timeout", err)
	}

	cl.write(Resume{AppliedSeq: 3})
	typ, payload, err = cl.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgBatch {
		t.Fatalf("the message is %s, want BATCH", typ)
	}
	batch, err = decodeBatch(payload)
	if err != nil {
		t.Fatalf("decodeBatch: %v", err)
	}
	if len(batch.Segments) != 1 || !bytes.Equal(batch.Segments[0], seg2.Bytes) {
		t.Error("the source did not continue after the pause")
	}
}

func TestSourceReadsEverySegmentOnce(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	src := &fakeSource{}
	sink := &fakeSink{}
	addr, _, _ := startSource(t, cfg, src)
	startBackup(t, cfg, sink, addr)

	// The segments are small, so that every segment is a batch of its
	// own.
	var want int64
	for i := uint64(1); i <= 20; i++ {
		seg := Segment{SeqStart: i, SeqEnd: i, Bytes: testSegment(t, cfg.DatabaseID, 0, i, 1)}
		want += int64(len(seg.Bytes))
		src.add(seg)
	}
	if !waitFor(t, 10*time.Second, func() bool { return len(sink.seqs()) == 20 }) {
		t.Fatalf("the sink applied %v, want twenty transactions", sink.seqs())
	}
	if got := src.servedBytes(); got != want {
		t.Errorf("the source handed out %d bytes, want %d: a segment is read again", got, want)
	}
}

func TestSourceStopsWhenTheContextIsDone(t *testing.T) {
	cfg := testConfig(t)
	src := &fakeSource{}
	s, err := NewSource(cfg, src)
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Serve(ctx, ln) }()

	// A peer that connects and says nothing holds the session of the
	// source until the context is done.
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the serve ended with %v, want %v", err, context.Canceled)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the serve did not end")
	}
}

func TestSourceSendsNoBatchAfterThePaused(t *testing.T) {
	cfg := testConfig(t)
	cfg.HeartbeatInterval = 10 * time.Second
	src := &fakeSource{}
	seg1 := Segment{SeqStart: 1, SeqEnd: 1, Bytes: testSegment(t, cfg.DatabaseID, 0, 1, 1)}
	seg2 := Segment{SeqStart: 2, SeqEnd: 2, Bytes: testSegment(t, cfg.DatabaseID, 0, 2, 1)}
	src.add(seg1, seg2)
	// The window holds one segment, the source waits with the second one.
	cfg.MaxUnackedBytes = int64(len(seg1.Bytes))
	addr, _, _ := startSource(t, cfg, src)

	cl := dialClient(t, addr)
	cl.write(testHello(t, cfg, testToken))
	if _, _, err := cl.read(); err != nil {
		t.Fatalf("read: %v", err)
	}
	if _, _, err := cl.read(); err != nil {
		t.Fatalf("read: %v", err)
	}

	cl.write(Pause{AppliedSeq: 1})
	typ, _, err := cl.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgPaused {
		t.Fatalf("the message is %s, want PAUSED", typ)
	}

	// A bigger window does not make the source push while it is paused.
	cl.write(FlowControl{MaxUnackedBytes: 1 << 20})
	typ, _, err = cl.readWithin(300 * time.Millisecond)
	if err == nil {
		t.Fatalf("the source sent %s after the paused", typ)
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("the error is %v, want a timeout", err)
	}
}

func TestSourceKeepsTheResyncOverTheTransportError(t *testing.T) {
	// The peer may close the connection while the message that asks for
	// the snapshot is read, the write of the answer fails first. The
	// resync is the error that matters.
	local, remote := net.Pipe()
	defer remote.Close()
	sess := &sourceSession{
		src:  &Source{},
		conn: newConn(local, 1024, time.Second),
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
	sess.finish(io.EOF)
	sess.finish(fmt.Errorf("%w: the backup asks for a new snapshot", ErrResyncRequired))
	if !errors.Is(sess.errLocked(), ErrResyncRequired) {
		t.Errorf("the session ended with %v, want %v", sess.errLocked(), ErrResyncRequired)
	}
}

func TestSourceClampsTheWindowOfTheBackup(t *testing.T) {
	sess := &sourceSession{src: &Source{}}
	sess.onFlowControl(FlowControl{MaxUnackedBytes: math.MaxUint64})
	if sess.limit <= 0 {
		t.Errorf("the window is %d bytes, want a window that is open", sess.limit)
	}
}

func TestSourceClosesIdleConnection(t *testing.T) {
	cfg := testConfig(t)
	cfg.IdleTimeout = 200 * time.Millisecond
	cfg.HeartbeatInterval = 50 * time.Millisecond
	src := &fakeSource{}
	addr, _, _ := startSource(t, cfg, src)

	cl := dialClient(t, addr)
	cl.write(testHello(t, cfg, testToken))
	if _, _, err := cl.read(); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := cl.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set the read deadline: %v", err)
	}
	for {
		_, _, err := cl.read()
		if err == nil {
			continue
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatal("the connection stayed open")
		}
		return
	}
}

func TestSourceDoesNotAnswerHeartbeats(t *testing.T) {
	cfg := testConfig(t)
	cfg.HeartbeatInterval = 10 * time.Second
	src := &fakeSource{}
	addr, _, _ := startSource(t, cfg, src)

	cl := dialClient(t, addr)
	cl.write(testHello(t, cfg, testToken))
	if _, _, err := cl.read(); err != nil {
		t.Fatalf("read: %v", err)
	}
	// The peer sends its own heartbeats, an answer would only bounce
	// between the two sides.
	cl.write(Heartbeat{UnixMillis: 1})
	typ, _, err := cl.readWithin(300 * time.Millisecond)
	if err == nil {
		t.Fatalf("the source answered the heartbeat with %s", typ)
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("the error is %v, want a timeout", err)
	}
}

func TestSourceSendsHeartbeats(t *testing.T) {
	cfg := testConfig(t)
	cfg.HeartbeatInterval = 50 * time.Millisecond
	src := &fakeSource{}
	addr, _, _ := startSource(t, cfg, src)

	cl := dialClient(t, addr)
	cl.write(testHello(t, cfg, testToken))
	heartbeats := 0
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if err := cl.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
			t.Fatalf("set the read deadline: %v", err)
		}
		typ, _, err := cl.read()
		if err != nil {
			break
		}
		if typ == MsgHeartbeat {
			heartbeats++
		}
	}
	if heartbeats < 2 {
		t.Errorf("the source sent %d heartbeats, want at least 2", heartbeats)
	}
}

func TestPackBatchLimits(t *testing.T) {
	const maxBatch = 16 << 20
	const maxFrame = 64 << 20

	// A batch holds the segments up to the limit of the payload.
	segs := []Segment{
		{SeqStart: 1, SeqEnd: 1, Bytes: make([]byte, 8<<20)},
		{SeqStart: 2, SeqEnd: 2, Bytes: make([]byte, 8<<20)},
	}
	chosen, _, err := packBatch(segs, maxBatch, maxFrame)
	if err != nil {
		t.Fatalf("packBatch: %v", err)
	}
	if len(chosen) != 1 {
		t.Errorf("the batch holds %d segments, want 1", len(chosen))
	}

	// A batch holds at most 64 segments.
	segs = nil
	for i := range uint64(MaxSegmentsPerBatch + 1) {
		segs = append(segs, Segment{SeqStart: uint64(i) + 1, SeqEnd: uint64(i) + 1, Bytes: make([]byte, 16)})
	}
	chosen, _, err = packBatch(segs, maxBatch, maxFrame)
	if err != nil {
		t.Fatalf("packBatch: %v", err)
	}
	if len(chosen) != MaxSegmentsPerBatch {
		t.Errorf("the batch holds %d segments, want %d", len(chosen), MaxSegmentsPerBatch)
	}

	// A segment that is bigger than a batch is sent in a batch of its own.
	chosen, _, err = packBatch([]Segment{{SeqStart: 1, SeqEnd: 1, Bytes: make([]byte, maxBatch)}}, maxBatch, maxFrame)
	if err != nil {
		t.Fatalf("packBatch: %v", err)
	}
	if len(chosen) != 1 {
		t.Errorf("the batch holds %d segments, want 1", len(chosen))
	}

	// A segment that is bigger than a frame is an error.
	if _, _, err := packBatch([]Segment{{SeqStart: 1, SeqEnd: 1, Bytes: make([]byte, maxFrame)}}, maxBatch, maxFrame); err == nil {
		t.Error("a segment that does not fit into a frame was accepted")
	}
}

func TestPackBatch(t *testing.T) {
	seg := func(start uint64, size int) Segment {
		return Segment{SeqStart: start, SeqEnd: start, Bytes: make([]byte, size)}
	}
	segs := []Segment{}
	for i := range uint64(MaxSegmentsPerBatch + 1) {
		segs = append(segs, seg(i+1, 16))
	}
	chosen, batch, err := packBatch(segs, 1<<20, 1<<20)
	if err != nil {
		t.Fatalf("packBatch: %v", err)
	}
	if len(chosen) != MaxSegmentsPerBatch {
		t.Errorf("the batch holds %d segments, want %d", len(chosen), MaxSegmentsPerBatch)
	}
	if len(batch.Segments) != MaxSegmentsPerBatch {
		t.Errorf("the batch holds %d segments, want %d", len(batch.Segments), MaxSegmentsPerBatch)
	}

	// A batch that is full is split into more batches.
	chosen, _, err = packBatch([]Segment{seg(1, 600), seg(2, 600)}, 1000, 1<<20)
	if err != nil {
		t.Fatalf("packBatch: %v", err)
	}
	if len(chosen) != 1 {
		t.Errorf("the batch holds %d segments, want 1", len(chosen))
	}

	// A segment that does not fit into a batch is sent in a batch of its own.
	chosen, _, err = packBatch([]Segment{seg(1, 2000), seg(2, 16)}, 1000, 1<<20)
	if err != nil {
		t.Fatalf("packBatch: %v", err)
	}
	if len(chosen) != 1 {
		t.Fatalf("the batch holds %d segments, want 1", len(chosen))
	}
	if chosen[0].SeqStart != 1 {
		t.Errorf("the batch holds the segment %d, want the segment 1", chosen[0].SeqStart)
	}

	// A segment that does not fit into a frame is an error.
	if _, _, err := packBatch([]Segment{seg(1, 2048)}, 1<<20, 1024); err == nil {
		t.Fatal("a segment that does not fit into a frame was accepted")
	}

	// A segment without bytes is not a segment.
	if _, _, err := packBatch([]Segment{seg(1, 0)}, 1<<20, 1<<20); err == nil {
		t.Fatal("a segment without bytes was accepted")
	}
}

// TestTheSourceCountsItsSessions checks that the source tells whether a
// backup is on the line and how many sessions it accepted.
func TestTheSourceCountsItsSessions(t *testing.T) {
	cfg := testConfig(t)
	src := &fakeSource{}
	addr, source, _ := startSource(t, cfg, src)

	if source.Connected() {
		t.Error("the source is connected before a backup showed up")
	}
	if source.Sessions() != 0 {
		t.Errorf("the source accepted %d sessions, want 0", source.Sessions())
	}

	cl := dialClient(t, addr)
	cl.write(testHello(t, cfg, testToken))
	if _, _, err := cl.read(); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool { return source.Connected() }) {
		t.Fatal("the source does not see the backup")
	}
	if source.Sessions() != 1 {
		t.Errorf("the source accepted %d sessions, want 1", source.Sessions())
	}

	// The backup goes away.
	cl.conn.Close()
	if !waitFor(t, 5*time.Second, func() bool { return !source.Connected() }) {
		t.Fatal("the source still sees the backup")
	}

	// A backup that comes back is another session.
	cl2 := dialClient(t, addr)
	cl2.write(testHello(t, cfg, testToken))
	if _, _, err := cl2.read(); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool { return source.Sessions() == 2 }) {
		t.Errorf("the source accepted %d sessions, want 2", source.Sessions())
	}
}

// TestTheSourceCountsItsResyncs checks that the source counts the resyncs
// it asked the backup for.
func TestTheSourceCountsItsResyncs(t *testing.T) {
	cfg := testConfig(t)
	src := &fakeSource{}
	src.add(Segment{SeqStart: 5, SeqEnd: 6, Bytes: []byte("a segment")})
	addr, source, _ := startSource(t, cfg, src)

	cl := dialClient(t, addr)
	cl.write(testHello(t, cfg, testToken))
	if _, _, err := cl.read(); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool { return source.Resyncs() == 1 }) {
		t.Errorf("the source asked for %d resyncs, want 1", source.Resyncs())
	}
}
