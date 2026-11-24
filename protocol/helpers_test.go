package protocol

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/segment"
)

// testToken is the token of the tests.
const testToken = "the shared token of the tests"

// pageSize is the size of the pages the tests write.
const pageSize = 512

// fakeSource holds the segments a test pushes.
type fakeSource struct {
	mu       sync.Mutex
	epoch    uint64
	segments []Segment
}

// Epoch returns the generation of the chain.
func (f *fakeSource) Epoch() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.epoch
}

// OldestSeq returns the lowest sequence number the source holds.
func (f *fakeSource) OldestSeq() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.segments) == 0 {
		return 0
	}
	return f.segments[0].SeqStart
}

// SegmentsAfter returns the segments that hold a transaction after seq.
func (f *fakeSource) SegmentsAfter(ctx context.Context, seq uint64) ([]Segment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Segment
	for _, s := range f.segments {
		if s.SeqEnd <= seq {
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

// add puts segments into the source.
func (f *fakeSource) add(segs ...Segment) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.segments = append(f.segments, segs...)
}

// setEpoch sets the generation of the chain.
func (f *fakeSource) setEpoch(epoch uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.epoch = epoch
}

// fakeSink records the transactions the backup applied.
type fakeSink struct {
	mu      sync.Mutex
	applied uint64
	txs     []segment.Tx
	failSeq uint64
}

// Apply applies a transaction.
//
// The transaction of the sequence number in failSeq is not applied, the
// apply fails like a disk that does not accept the write. The failure
// happens once, the next try succeeds.
func (f *fakeSink) Apply(ctx context.Context, tx segment.Tx) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failSeq != 0 && tx.Seq == f.failSeq {
		f.failSeq = 0
		return errors.New("the sink does not accept the transaction")
	}
	f.txs = append(f.txs, tx)
	f.applied = tx.Seq
	return nil
}

// AppliedSeq returns the sequence number the sink applied.
func (f *fakeSink) AppliedSeq(ctx context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applied, nil
}

// setApplied sets the sequence number the sink holds.
func (f *fakeSink) setApplied(seq uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = seq
}

// seqs returns the sequence numbers of the applied transactions.
func (f *fakeSink) seqs() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]uint64, 0, len(f.txs))
	for _, tx := range f.txs {
		out = append(out, tx.Seq)
	}
	return out
}

// testPage returns a page image that differs for every sequence number.
func testPage(seq uint64) []byte {
	data := make([]byte, pageSize)
	for i := range data {
		data[i] = byte(seq + uint64(i))
	}
	return data
}

// testSegment returns the bytes of a segment that holds the given number of
// transactions.
func testSegment(t *testing.T, dbID string, epoch, seqStart uint64, txs int) []byte {
	t.Helper()
	var buf bytes.Buffer
	h := segment.Header{
		Version:    segment.Version,
		DatabaseID: segment.DatabaseID(dbID),
		Epoch:      epoch,
		SeqStart:   seqStart,
		PageSize:   pageSize,
	}
	w, err := segment.NewWriter(&buf, h)
	if err != nil {
		t.Fatalf("segment.NewWriter: %v", err)
	}
	for i := range txs {
		tx := segment.Tx{
			Seq:         seqStart + uint64(i),
			CommitPages: 4,
			Pages:       []segment.Page{{Pgno: 1, Data: testPage(seqStart + uint64(i))}},
		}
		if err := w.WriteTx(tx); err != nil {
			t.Fatalf("segment.WriteTx: %v", err)
		}
	}
	if _, err := w.Close(segment.Pos{Salt1: 1, Salt2: 2, Frame: seqStart, WALOffset: 4096}); err != nil {
		t.Fatalf("segment.Close: %v", err)
	}
	return buf.Bytes()
}

// testConfig returns the configuration of a test.
func testConfig(t *testing.T) Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(testToken+"\n"), 0o600); err != nil {
		t.Fatalf("write the token: %v", err)
	}
	return Config{
		TokenFile:  path,
		DatabaseID: "app.db",
		StreamID:   1,
		BackupID:   "backup-1",
	}
}

// startSource starts a source that serves its listener.
//
// It returns the address of the listener and the error the serve ended with.
func startSource(t *testing.T, cfg Config, src SegmentSource) (string, *Source, <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s, err := NewSource(cfg, src)
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Serve(ctx, ln)
	}()
	t.Cleanup(func() {
		cancel()
		ln.Close()
	})
	return ln.Addr().String(), s, errCh
}

// testClient is a peer that speaks the protocol by hand.
type testClient struct {
	t    *testing.T
	conn net.Conn
}

// dialClient opens a connection to the source.
func dialClient(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &testClient{t: t, conn: conn}
}

// write sends a message.
func (c *testClient) write(m message) {
	c.t.Helper()
	if err := c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		c.t.Fatalf("set the write deadline: %v", err)
	}
	if _, err := c.conn.Write(EncodeFrame(m.Type(), m.encode())); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

// read reads the next frame.
func (c *testClient) read() (MessageType, []byte, error) {
	return c.readWithin(5 * time.Second)
}

// readWithin reads the next frame and waits at most for the duration.
func (c *testClient) readWithin(d time.Duration) (MessageType, []byte, error) {
	c.t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(d)); err != nil {
		c.t.Fatalf("set the read deadline: %v", err)
	}
	return ReadFrame(c.conn, MaxFrameBytes)
}

// readUntilClose reads the frames until the peer closes the connection.
func (c *testClient) readUntilClose() []MessageType {
	c.t.Helper()
	var types []MessageType
	for {
		typ, _, err := c.read()
		if err != nil {
			return types
		}
		types = append(types, typ)
	}
}

// testHello returns a hello that proves that the sender knows the token.
func testHello(t *testing.T, cfg Config, token string) Hello {
	t.Helper()
	nonce, err := newNonce()
	if err != nil {
		t.Fatalf("newNonce: %v", err)
	}
	h := Hello{
		ProtocolVersion: ProtocolVersion,
		DatabaseID:      segment.DatabaseID(cfg.DatabaseID),
		StreamID:        cfg.StreamID,
		BackupID:        cfg.BackupID,
		Nonce:           nonce,
	}
	return signHello(h, token)
}

// signHello computes the code of a hello.
func signHello(h Hello, token string) Hello {
	h.MAC = clientMAC([]byte(token), h.Nonce, h.macPrefix())
	return h
}

// testServer is a source that a test drives by hand.
//
// It answers the hello of the backup and answers a pause with a paused, the
// batches are the ones the test sends.
type testServer struct {
	t     *testing.T
	ln    net.Listener
	cfg   Config
	token string

	mu       sync.Mutex
	hellos   []Hello
	times    []time.Time
	ackList  []Ack
	conns    []net.Conn
	received []MessageType

	// closeAfterHello ends the connection right after the hello, it
	// breaks the connection of the backup.
	closeAfterHello bool
}

// newTestServer starts a server that accepts the connections of the backup.
func newTestServer(t *testing.T, cfg Config) *testServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &testServer{t: t, ln: ln, cfg: cfg, token: testToken}
	t.Cleanup(func() { ln.Close() })
	go s.serve()
	return s
}

// addr returns the address of the server.
func (s *testServer) addr() string {
	return s.ln.Addr().String()
}

// serve accepts the connections of the backup.
func (s *testServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		go s.handle(conn)
	}
}

// handle answers the hello of the backup and reads its messages.
func (s *testServer) handle(conn net.Conn) {
	typ, payload, err := ReadFrame(conn, MaxFrameBytes)
	if err != nil {
		conn.Close()
		return
	}
	if typ != MsgHello {
		conn.Close()
		return
	}
	hello, err := decodeHello(payload)
	if err != nil {
		conn.Close()
		return
	}
	s.mu.Lock()
	s.hellos = append(s.hellos, hello)
	s.times = append(s.times, time.Now())
	closeAfterHello := s.closeAfterHello
	s.mu.Unlock()
	nonce, err := newNonce()
	if err != nil {
		conn.Close()
		return
	}
	ack := HelloAck{
		ProtocolVersion: ProtocolVersion,
		Epoch:           hello.Epoch,
		ResumeSeq:       hello.AppliedSeq,
		Nonce:           nonce,
	}
	ack.MAC = serverMAC([]byte(s.token), hello.Nonce, nonce, ack.macPrefix())
	if err := s.write(conn, MsgHelloAck, ack.encode()); err != nil {
		return
	}
	if closeAfterHello {
		conn.Close()
		return
	}
	for {
		typ, payload, err := ReadFrame(conn, MaxFrameBytes)
		if err != nil {
			return
		}
		s.mu.Lock()
		s.received = append(s.received, typ)
		if typ == MsgAck {
			m, err := decodeAck(payload)
			if err == nil {
				s.ackList = append(s.ackList, m)
			}
		}
		s.mu.Unlock()
		if typ == MsgPause {
			m, err := decodePause(payload)
			if err != nil {
				return
			}
			s.write(conn, MsgPaused, Paused{LastSentSeq: m.AppliedSeq}.encode())
		}
	}
}

// write sends a frame to the connection.
func (s *testServer) write(conn net.Conn, typ MessageType, payload []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	_, err := conn.Write(EncodeFrame(typ, payload))
	return err
}

// send sends a message to the backup.
func (s *testServer) send(m message) {
	s.t.Helper()
	s.mu.Lock()
	conns := append([]net.Conn(nil), s.conns...)
	s.mu.Unlock()
	if len(conns) == 0 {
		s.t.Fatal("the backup did not connect")
	}
	if err := s.write(conns[len(conns)-1], m.Type(), m.encode()); err != nil {
		s.t.Fatalf("write: %v", err)
	}
}

// closeConns closes the connections of the backup.
func (s *testServer) closeConns() {
	s.mu.Lock()
	conns := append([]net.Conn(nil), s.conns...)
	s.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

// helloCount returns the number of the hellos the server received.
func (s *testServer) helloCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.hellos)
}

// lastHello returns the hello the server received last.
func (s *testServer) lastHello() (Hello, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.hellos) == 0 {
		return Hello{}, false
	}
	return s.hellos[len(s.hellos)-1], true
}

// acks returns the acknowledgements the backup sent.
func (s *testServer) acks() []Ack {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Ack(nil), s.ackList...)
}

// helloTimes returns the times the server received the hellos.
func (s *testServer) helloTimes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.times...)
}

// types returns the messages the backup sent.
func (s *testServer) types() []MessageType {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]MessageType(nil), s.received...)
}

// waitFor waits until the condition holds or the timeout is over.
func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}
