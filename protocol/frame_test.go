package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestFrameLayout(t *testing.T) {
	payload := []byte("the payload of the frame")
	b := EncodeFrame(MsgBatch, payload)

	if len(b) != FrameHeaderSize+len(payload) {
		t.Fatalf("the frame is %d bytes, want %d", len(b), FrameHeaderSize+len(payload))
	}
	if string(b[0:4]) != "SQRL" {
		t.Errorf("the magic is %q, want %q", b[0:4], "SQRL")
	}
	if b[4] != 1 {
		t.Errorf("the version is %d, want 1", b[4])
	}
	if b[5] != byte(MsgBatch) {
		t.Errorf("the type is %d, want %d", b[5], MsgBatch)
	}
	if v := binary.BigEndian.Uint16(b[6:8]); v != 0 {
		t.Errorf("the flags are %d, want 0", v)
	}
	if v := binary.BigEndian.Uint32(b[8:12]); v != uint32(len(payload)) {
		t.Errorf("the length is %d, want %d", v, len(payload))
	}
	if !bytes.Equal(b[FrameHeaderSize:], payload) {
		t.Errorf("the payload is %q, want %q", b[FrameHeaderSize:], payload)
	}
}

func TestReadFrame(t *testing.T) {
	payload := []byte("the payload of the frame")
	got, data, err := ReadFrame(bytes.NewReader(EncodeFrame(MsgAck, payload)), MaxFrameBytes)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if got != MsgAck {
		t.Errorf("the type is %s, want ACK", got)
	}
	if !bytes.Equal(data, payload) {
		t.Errorf("the payload is %q, want %q", data, payload)
	}
}

func TestReadFrameRejectsBadMagic(t *testing.T) {
	b := EncodeFrame(MsgAck, nil)
	b[0] = 'X'
	_, _, err := ReadFrame(bytes.NewReader(b), MaxFrameBytes)
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("the error is %v, want %v", err, ErrInvalidFrame)
	}
}

func TestReadFrameRejectsBadVersion(t *testing.T) {
	b := EncodeFrame(MsgAck, nil)
	b[4] = 9
	_, _, err := ReadFrame(bytes.NewReader(b), MaxFrameBytes)
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("the error is %v, want %v", err, ErrInvalidFrame)
	}
}

func TestReadFrameRejectsLongPayload(t *testing.T) {
	var hdr [FrameHeaderSize]byte
	copy(hdr[0:4], FrameMagic)
	hdr[4] = FrameVersion
	hdr[5] = byte(MsgBatch)
	binary.BigEndian.PutUint32(hdr[8:12], uint32(MaxFrameBytes+1))
	_, _, err := ReadFrame(bytes.NewReader(hdr[:]), MaxFrameBytes)
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("the error is %v, want %v", err, ErrInvalidFrame)
	}
}

func TestReadFrameRejectsShortFrame(t *testing.T) {
	b := EncodeFrame(MsgAck, []byte("short"))
	_, _, err := ReadFrame(bytes.NewReader(b[:len(b)-2]), MaxFrameBytes)
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("the error is %v, want an end of file", err)
	}
}
