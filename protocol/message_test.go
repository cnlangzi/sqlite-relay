package protocol

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

func TestHelloRoundTrip(t *testing.T) {
	m := Hello{
		ProtocolVersion: ProtocolVersion,
		DatabaseID:      [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		StreamID:        3,
		Epoch:           7,
		NeedsSnapshot:   true,
		BackupID:        "backup-1",
		AppliedSeq:      42,
		Nonce:           [32]byte{9: 1, 31: 2},
		MAC:             [32]byte{5: 3},
	}
	got, err := decodeHello(m.encode())
	if err != nil {
		t.Fatalf("decodeHello: %v", err)
	}
	if got.ProtocolVersion != m.ProtocolVersion {
		t.Errorf("the protocol version is %d, want %d", got.ProtocolVersion, m.ProtocolVersion)
	}
	if got.DatabaseID != m.DatabaseID {
		t.Errorf("the database ID is %x, want %x", got.DatabaseID, m.DatabaseID)
	}
	if got.StreamID != m.StreamID {
		t.Errorf("the stream ID is %d, want %d", got.StreamID, m.StreamID)
	}
	if got.Epoch != m.Epoch {
		t.Errorf("the epoch is %d, want %d", got.Epoch, m.Epoch)
	}
	if !got.NeedsSnapshot {
		t.Error("the hello does not ask for a snapshot")
	}
	if got.BackupID != m.BackupID {
		t.Errorf("the backup ID is %q, want %q", got.BackupID, m.BackupID)
	}
	if got.AppliedSeq != m.AppliedSeq {
		t.Errorf("the applied sequence is %d, want %d", got.AppliedSeq, m.AppliedSeq)
	}
	if got.Nonce != m.Nonce {
		t.Errorf("the nonce is %x, want %x", got.Nonce, m.Nonce)
	}
	if got.MAC != m.MAC {
		t.Errorf("the code is %x, want %x", got.MAC, m.MAC)
	}
}

func TestHelloRejectsLongBackupID(t *testing.T) {
	m := Hello{BackupID: string(bytes.Repeat([]byte("b"), MaxBackupIDLen+1))}
	if _, err := decodeHello(m.encode()); err == nil {
		t.Fatal("the hello with a long backup ID was accepted")
	}
}

func TestSnapshotMetaRoundTrip(t *testing.T) {
	m := SnapshotMeta{
		FormatVersion: 1,
		DatabaseID:    [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		StreamID:      3,
		Epoch:         7,
		BaselineSeq:   42,
		PageSize:      4096,
		Bytes:         8192,
		Checksum:      [32]byte{5: 3},
	}
	got, err := decodeSnapshotMeta(m.encode())
	if err != nil {
		t.Fatalf("decodeSnapshotMeta: %v", err)
	}
	if got.FormatVersion != m.FormatVersion {
		t.Errorf("the format version is %d, want %d", got.FormatVersion, m.FormatVersion)
	}
	if got.DatabaseID != m.DatabaseID {
		t.Errorf("the database ID is %x, want %x", got.DatabaseID, m.DatabaseID)
	}
	if got.StreamID != m.StreamID {
		t.Errorf("the stream ID is %d, want %d", got.StreamID, m.StreamID)
	}
	if got.Epoch != m.Epoch {
		t.Errorf("the epoch is %d, want %d", got.Epoch, m.Epoch)
	}
	if got.BaselineSeq != m.BaselineSeq {
		t.Errorf("the baseline sequence is %d, want %d", got.BaselineSeq, m.BaselineSeq)
	}
	if got.PageSize != m.PageSize {
		t.Errorf("the page size is %d, want %d", got.PageSize, m.PageSize)
	}
	if got.Bytes != m.Bytes {
		t.Errorf("the size is %d, want %d", got.Bytes, m.Bytes)
	}
	if got.Checksum != m.Checksum {
		t.Errorf("the checksum is %x, want %x", got.Checksum, m.Checksum)
	}
}

func TestHelloAckRoundTrip(t *testing.T) {
	m := HelloAck{
		ProtocolVersion: ProtocolVersion,
		Epoch:           11,
		ResumeSeq:       99,
		Nonce:           [32]byte{1: 1},
		MAC:             [32]byte{2: 2},
	}
	got, err := decodeHelloAck(m.encode())
	if err != nil {
		t.Fatalf("decodeHelloAck: %v", err)
	}
	if got.ProtocolVersion != m.ProtocolVersion {
		t.Errorf("the protocol version is %d, want %d", got.ProtocolVersion, m.ProtocolVersion)
	}
	if got.Epoch != m.Epoch {
		t.Errorf("the epoch is %d, want %d", got.Epoch, m.Epoch)
	}
	if got.ResumeSeq != m.ResumeSeq {
		t.Errorf("the resume sequence is %d, want %d", got.ResumeSeq, m.ResumeSeq)
	}
	if got.Nonce != m.Nonce {
		t.Errorf("the nonce is %x, want %x", got.Nonce, m.Nonce)
	}
	if got.MAC != m.MAC {
		t.Errorf("the code is %x, want %x", got.MAC, m.MAC)
	}
}

func TestBatchRoundTrip(t *testing.T) {
	m := Batch{Segments: [][]byte{
		[]byte("the first segment"),
		[]byte("the second segment"),
	}}
	got, err := decodeBatch(m.encode())
	if err != nil {
		t.Fatalf("decodeBatch: %v", err)
	}
	if len(got.Segments) != len(m.Segments) {
		t.Fatalf("the batch holds %d segments, want %d", len(got.Segments), len(m.Segments))
	}
	for i := range m.Segments {
		if !bytes.Equal(got.Segments[i], m.Segments[i]) {
			t.Errorf("the segment %d is %q, want %q", i, got.Segments[i], m.Segments[i])
		}
	}
}

func TestBatchRejectsAnEmptySegment(t *testing.T) {
	// A segment without bytes would not move the reader, the decoder has
	// to refuse it instead of walking over the payload for ever.
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := decodeBatch([]byte{0, 0, 0, 0}); err == nil {
			t.Error("a batch with an empty segment was accepted")
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the decoder of a batch with an empty segment did not return")
	}
}

func TestBatchRejectsALengthThatDoesNotFit(t *testing.T) {
	// The length of a segment is a u32, it does not fit into an int on
	// every platform. The decoder has to refuse it and not cut the
	// payload with it.
	if _, err := decodeBatch([]byte{0xff, 0xff, 0xff, 0xff}); err == nil {
		t.Fatal("a batch with a length that does not fit was accepted")
	}
}

func TestAckRoundTrip(t *testing.T) {
	m := Ack{AppliedSeq: 1234, Epoch: 5}
	got, err := decodeAck(m.encode())
	if err != nil {
		t.Fatalf("decodeAck: %v", err)
	}
	if got.AppliedSeq != m.AppliedSeq {
		t.Errorf("the applied sequence is %d, want %d", got.AppliedSeq, m.AppliedSeq)
	}
	if got.Epoch != m.Epoch {
		t.Errorf("the epoch is %d, want %d", got.Epoch, m.Epoch)
	}
}

func TestHeartbeatRoundTrip(t *testing.T) {
	m := Heartbeat{UnixMillis: 1700000000123}
	got, err := decodeHeartbeat(m.encode())
	if err != nil {
		t.Fatalf("decodeHeartbeat: %v", err)
	}
	if got.UnixMillis != m.UnixMillis {
		t.Errorf("the time is %d, want %d", got.UnixMillis, m.UnixMillis)
	}
}

func TestFlowControlRoundTrip(t *testing.T) {
	m := FlowControl{MaxUnackedBytes: 1 << 20}
	got, err := decodeFlowControl(m.encode())
	if err != nil {
		t.Fatalf("decodeFlowControl: %v", err)
	}
	if got.MaxUnackedBytes != m.MaxUnackedBytes {
		t.Errorf("the limit is %d, want %d", got.MaxUnackedBytes, m.MaxUnackedBytes)
	}
}

func TestPauseRoundTrip(t *testing.T) {
	m := Pause{AppliedSeq: 77}
	got, err := decodePause(m.encode())
	if err != nil {
		t.Fatalf("decodePause: %v", err)
	}
	if got.AppliedSeq != m.AppliedSeq {
		t.Errorf("the applied sequence is %d, want %d", got.AppliedSeq, m.AppliedSeq)
	}
}

func TestPausedRoundTrip(t *testing.T) {
	m := Paused{LastSentSeq: 88}
	got, err := decodePaused(m.encode())
	if err != nil {
		t.Fatalf("decodePaused: %v", err)
	}
	if got.LastSentSeq != m.LastSentSeq {
		t.Errorf("the last sequence number is %d, want %d", got.LastSentSeq, m.LastSentSeq)
	}
}

func TestResumeRoundTrip(t *testing.T) {
	m := Resume{AppliedSeq: 66}
	got, err := decodeResume(m.encode())
	if err != nil {
		t.Fatalf("decodeResume: %v", err)
	}
	if got.AppliedSeq != m.AppliedSeq {
		t.Errorf("the applied sequence is %d, want %d", got.AppliedSeq, m.AppliedSeq)
	}
}

func TestResyncRequiredRoundTrip(t *testing.T) {
	m := ResyncRequired{Reason: ResyncGap, Epoch: 3, Seq: 100}
	got, err := decodeResyncRequired(m.encode())
	if err != nil {
		t.Fatalf("decodeResyncRequired: %v", err)
	}
	if got.Reason != m.Reason {
		t.Errorf("the reason is %d, want %d", got.Reason, m.Reason)
	}
	if got.Epoch != m.Epoch {
		t.Errorf("the epoch is %d, want %d", got.Epoch, m.Epoch)
	}
	if got.Seq != m.Seq {
		t.Errorf("the sequence number is %d, want %d", got.Seq, m.Seq)
	}
}

func TestErrorRoundTrip(t *testing.T) {
	m := Error{Code: CodeAuth, Message: "the token does not match"}
	got, err := decodeError(m.encode())
	if err != nil {
		t.Fatalf("decodeError: %v", err)
	}
	if got.Code != m.Code {
		t.Errorf("the code is %d, want %d", got.Code, m.Code)
	}
	if got.Message != m.Message {
		t.Errorf("the message is %q, want %q", got.Message, m.Message)
	}
}

// TestMessageNoticesChangedByte decodes every message with one byte changed
// and reports whether the change was seen.
func TestMessageNoticesChangedByte(t *testing.T) {
	cases := []struct {
		name   string
		msg    message
		decode func([]byte) (message, error)
	}{
		{"HELLO", helloMessage(), func(b []byte) (message, error) { return decodeHello(b) }},
		{"HELLO_ACK", helloAckMessage(), func(b []byte) (message, error) { return decodeHelloAck(b) }},
		{"BATCH", Batch{Segments: [][]byte{[]byte("a segment")}}, func(b []byte) (message, error) { return decodeBatch(b) }},
		{"ACK", Ack{AppliedSeq: 5, Epoch: 2}, func(b []byte) (message, error) { return decodeAck(b) }},
		{"HEARTBEAT", Heartbeat{UnixMillis: 1700000000123}, func(b []byte) (message, error) { return decodeHeartbeat(b) }},
		{"FLOW_CONTROL", FlowControl{MaxUnackedBytes: 4096}, func(b []byte) (message, error) { return decodeFlowControl(b) }},
		{"PAUSE", Pause{AppliedSeq: 8}, func(b []byte) (message, error) { return decodePause(b) }},
		{"PAUSED", Paused{LastSentSeq: 9}, func(b []byte) (message, error) { return decodePaused(b) }},
		{"RESUME", Resume{AppliedSeq: 10}, func(b []byte) (message, error) { return decodeResume(b) }},
		{"RESYNC_REQUIRED", ResyncRequired{Reason: ResyncGap, Epoch: 1, Seq: 2}, func(b []byte) (message, error) { return decodeResyncRequired(b) }},
		{"ERROR", Error{Code: CodeProtocol, Message: "broken"}, func(b []byte) (message, error) { return decodeError(b) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			payload := c.msg.encode()
			for i := range payload {
				mutated := make([]byte, len(payload))
				copy(mutated, payload)
				mutated[i] ^= 0xff
				got, err := c.decode(mutated)
				if err != nil {
					continue
				}
				if !reflect.DeepEqual(got, c.msg) {
					return
				}
			}
			t.Errorf("the message %s did not notice a changed byte", c.name)
		})
	}
}

// FuzzDecodeMessages feeds the decoders with the payloads a peer may send.
//
// A payload that is not a message of the protocol has to be refused with an
// error and it may not panic.
func FuzzDecodeMessages(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("SQRL"))
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{0, 0, 0, 4, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, payload []byte) {
		_, _ = decodeHello(payload)
		_, _ = decodeHelloAck(payload)
		_, _ = decodeBatch(payload)
		_, _ = decodeAck(payload)
		_, _ = decodeHeartbeat(payload)
		_, _ = decodeFlowControl(payload)
		_, _ = decodePause(payload)
		_, _ = decodePaused(payload)
		_, _ = decodeResume(payload)
		_, _ = decodeResyncRequired(payload)
		_, _ = decodeError(payload)
	})
}

// helloMessage returns a hello with every field set to a distinct value.
func helloMessage() Hello {
	return Hello{
		ProtocolVersion: ProtocolVersion,
		DatabaseID:      [16]byte{1},
		StreamID:        2,
		Epoch:           3,
		BackupID:        "backup",
		AppliedSeq:      4,
		Nonce:           [32]byte{5},
		MAC:             [32]byte{6},
	}
}

// helloAckMessage returns a hello answer with every field set to a distinct
// value.
func helloAckMessage() HelloAck {
	return HelloAck{
		ProtocolVersion: ProtocolVersion,
		Epoch:           1,
		ResumeSeq:       2,
		Nonce:           [32]byte{3},
		MAC:             [32]byte{4},
	}
}
