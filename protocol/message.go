package protocol

import (
	"encoding/binary"
	"fmt"
)

// The sizes of the fixed fields of the messages.
const (
	// macSize is the size of the message authentication code.
	macSize = 32
	// nonceSize is the size of the nonce of the handshake.
	nonceSize = 32
	// databaseIDSize is the size of the identifier of a database.
	databaseIDSize = 16
	// MaxBackupIDLen is the size of the identifier of a backup.
	MaxBackupIDLen = 64
)

// ErrorCode tells the peer why a connection is ended.
type ErrorCode uint8

// The error codes of the protocol.
const (
	// CodeProtocol reports a frame or a message that does not hold what it
	// says.
	CodeProtocol ErrorCode = 1
	// CodeAuth reports that the token does not match.
	CodeAuth ErrorCode = 2
	// CodeIdentity reports that the backup is not allowed to connect.
	CodeIdentity ErrorCode = 3
	// CodeVersion reports that the version of the protocol is not supported.
	CodeVersion ErrorCode = 4
)

// String returns the name of the code.
func (c ErrorCode) String() string {
	switch c {
	case CodeProtocol:
		return "protocol"
	case CodeAuth:
		return "authentication"
	case CodeIdentity:
		return "identity"
	case CodeVersion:
		return "version"
	}
	return fmt.Sprintf("ErrorCode(%d)", uint8(c))
}

// ResyncReason tells the peer why a new snapshot is needed.
type ResyncReason uint8

// The reasons of a resync.
const (
	// ResyncEpochMismatch reports that the peers are not part of the same
	// generation of the chain.
	ResyncEpochMismatch ResyncReason = 1
	// ResyncGap reports that a transaction of the chain is missing.
	ResyncGap ResyncReason = 2
	// ResyncCorrupt reports that a segment does not hold what it says.
	ResyncCorrupt ResyncReason = 3
	// ResyncIdentity reports that a segment belongs to another database.
	ResyncIdentity ResyncReason = 4
)

// String returns the name of the reason.
func (r ResyncReason) String() string {
	switch r {
	case ResyncEpochMismatch:
		return "the epoch does not match"
	case ResyncGap:
		return "the chain has a hole"
	case ResyncCorrupt:
		return "a segment does not hold what it says"
	case ResyncIdentity:
		return "a segment belongs to another database"
	}
	return fmt.Sprintf("ResyncReason(%d)", uint8(r))
}

// message is a message of the protocol.
//
// The interface is not exported: the set of the messages is closed, only
// the package encodes them.
type message interface {
	// Type returns the type of the message.
	Type() MessageType
	// encode returns the payload of the message.
	encode() []byte
}

// Hello opens a connection.
//
// The backup sends it. It names the backup, the database and the stream it
// wants to receive and the sequence number it applied and persisted. The
// nonce and the message authentication code prove that the backup knows the
// token.
type Hello struct {
	// ProtocolVersion is the version of the protocol the backup speaks.
	ProtocolVersion uint32
	// DatabaseID names the database the backup wants to receive.
	DatabaseID [databaseIDSize]byte
	// StreamID is the number of the stream the backup wants to receive.
	StreamID uint64
	// Epoch is the generation of the chain the backup is part of.
	Epoch uint64
	// BackupID names the backup.
	BackupID string
	// AppliedSeq is the highest sequence number the backup applied and
	// persisted.
	AppliedSeq uint64
	// Nonce is the challenge of the backup.
	Nonce [nonceSize]byte
	// MAC proves that the backup knows the token.
	MAC [macSize]byte
}

// Type returns the type of the message.
func (m Hello) Type() MessageType { return MsgHello }

// encode returns the payload of the message.
func (m Hello) encode() []byte {
	b := make([]byte, 0, 4+databaseIDSize+8+8+2+len(m.BackupID)+8+nonceSize+macSize)
	b = binary.BigEndian.AppendUint32(b, m.ProtocolVersion)
	b = append(b, m.DatabaseID[:]...)
	b = binary.BigEndian.AppendUint64(b, m.StreamID)
	b = binary.BigEndian.AppendUint64(b, m.Epoch)
	b = binary.BigEndian.AppendUint16(b, uint16(len(m.BackupID)))
	b = append(b, m.BackupID...)
	b = binary.BigEndian.AppendUint64(b, m.AppliedSeq)
	b = append(b, m.Nonce[:]...)
	b = append(b, m.MAC[:]...)
	return b
}

// macPrefix returns the bytes of the message the message authentication code
// covers.
func (m Hello) macPrefix() []byte {
	b := m.encode()
	return b[:len(b)-macSize]
}

// HelloAck answers a hello.
//
// The source sends it. It names the generation of the chain and the sequence
// number the source continues after.
type HelloAck struct {
	// ProtocolVersion is the version of the protocol the source speaks.
	ProtocolVersion uint32
	// Epoch is the generation of the chain of the source.
	Epoch uint64
	// ResumeSeq is the sequence number the source continues after.
	ResumeSeq uint64
	// Nonce is the challenge of the source.
	Nonce [nonceSize]byte
	// MAC proves that the source knows the token.
	MAC [macSize]byte
}

// Type returns the type of the message.
func (m HelloAck) Type() MessageType { return MsgHelloAck }

// encode returns the payload of the message.
func (m HelloAck) encode() []byte {
	b := make([]byte, 0, 4+8+8+nonceSize+macSize)
	b = binary.BigEndian.AppendUint32(b, m.ProtocolVersion)
	b = binary.BigEndian.AppendUint64(b, m.Epoch)
	b = binary.BigEndian.AppendUint64(b, m.ResumeSeq)
	b = append(b, m.Nonce[:]...)
	b = append(b, m.MAC[:]...)
	return b
}

// macPrefix returns the bytes of the message the message authentication code
// covers.
func (m HelloAck) macPrefix() []byte {
	b := m.encode()
	return b[:len(b)-macSize]
}

// Batch carries the segments the source pushes.
//
// The payload holds the segments one after the other, each of them with its
// own length. A batch carries no sequence numbers of its own, the header and
// the trailer of a segment name the range of sequence numbers it holds.
type Batch struct {
	// Segments holds the raw bytes of the segments.
	Segments [][]byte
}

// Type returns the type of the message.
func (m Batch) Type() MessageType { return MsgBatch }

// encode returns the payload of the message.
func (m Batch) encode() []byte {
	size := 0
	for _, s := range m.Segments {
		size += 4 + len(s)
	}
	b := make([]byte, 0, size)
	for _, s := range m.Segments {
		b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
		b = append(b, s...)
	}
	return b
}

// Ack carries the sequence number the backup applied.
//
// The acknowledgement means that the transactions up to the sequence number
// are applied and that the sequence number is persisted.
type Ack struct {
	// AppliedSeq is the highest sequence number the backup applied and
	// persisted.
	AppliedSeq uint64
	// Epoch is the generation of the chain the backup is part of.
	Epoch uint64
}

// Type returns the type of the message.
func (m Ack) Type() MessageType { return MsgAck }

// encode returns the payload of the message.
func (m Ack) encode() []byte {
	b := make([]byte, 0, 16)
	b = binary.BigEndian.AppendUint64(b, m.AppliedSeq)
	b = binary.BigEndian.AppendUint64(b, m.Epoch)
	return b
}

// Heartbeat keeps a connection alive.
//
// The message holds the time of the sender in milliseconds since the epoch.
type Heartbeat struct {
	// UnixMillis is the time of the sender in milliseconds since the epoch.
	UnixMillis uint64
}

// Type returns the type of the message.
func (m Heartbeat) Type() MessageType { return MsgHeartbeat }

// encode returns the payload of the message.
func (m Heartbeat) encode() []byte {
	b := make([]byte, 0, 8)
	b = binary.BigEndian.AppendUint64(b, m.UnixMillis)
	return b
}

// FlowControl carries the limit of the bytes the source may have in flight.
//
// The limit is absolute: it is the number of bytes the source may have sent
// and that the backup did not acknowledge yet.
type FlowControl struct {
	// MaxUnackedBytes is the limit of the bytes in flight.
	MaxUnackedBytes uint64
}

// Type returns the type of the message.
func (m FlowControl) Type() MessageType { return MsgFlowControl }

// encode returns the payload of the message.
func (m FlowControl) encode() []byte {
	b := make([]byte, 0, 8)
	b = binary.BigEndian.AppendUint64(b, m.MaxUnackedBytes)
	return b
}

// Pause asks the source to stop pushing.
//
// The source keeps the connection, it only stops sending segments.
type Pause struct {
	// AppliedSeq is the sequence number the backup applied and persisted.
	AppliedSeq uint64
}

// Type returns the type of the message.
func (m Pause) Type() MessageType { return MsgPause }

// encode returns the payload of the message.
func (m Pause) encode() []byte {
	b := make([]byte, 0, 8)
	b = binary.BigEndian.AppendUint64(b, m.AppliedSeq)
	return b
}

// Paused answers a pause.
//
// It names the last sequence number the source sent before it stopped.
type Paused struct {
	// LastSentSeq is the last sequence number the source sent.
	LastSentSeq uint64
}

// Type returns the type of the message.
func (m Paused) Type() MessageType { return MsgPaused }

// encode returns the payload of the message.
func (m Paused) encode() []byte {
	b := make([]byte, 0, 8)
	b = binary.BigEndian.AppendUint64(b, m.LastSentSeq)
	return b
}

// Resume tells the source to push again.
//
// The source continues after the sequence number the backup names.
type Resume struct {
	// AppliedSeq is the sequence number the backup applied and persisted.
	AppliedSeq uint64
}

// Type returns the type of the message.
func (m Resume) Type() MessageType { return MsgResume }

// encode returns the payload of the message.
func (m Resume) encode() []byte {
	b := make([]byte, 0, 8)
	b = binary.BigEndian.AppendUint64(b, m.AppliedSeq)
	return b
}

// ResyncRequired tells the peer that the chain is not continuous any more.
//
// The receiver needs a new snapshot, the increments of the chain cannot be
// applied any more.
type ResyncRequired struct {
	// Reason tells why the chain is broken.
	Reason ResyncReason
	// Epoch is the generation of the chain the sender knows.
	Epoch uint64
	// Seq is the sequence number the chain is broken at.
	Seq uint64
}

// Type returns the type of the message.
func (m ResyncRequired) Type() MessageType { return MsgResyncRequired }

// encode returns the payload of the message.
func (m ResyncRequired) encode() []byte {
	b := make([]byte, 0, 17)
	b = append(b, byte(m.Reason))
	b = binary.BigEndian.AppendUint64(b, m.Epoch)
	b = binary.BigEndian.AppendUint64(b, m.Seq)
	return b
}

// Error tells the peer that the connection is ended.
//
// The error carries a code and a message that names the reason.
type Error struct {
	// Code tells why the connection is ended.
	Code ErrorCode
	// Message names the reason of the error.
	Message string
}

// Type returns the type of the message.
func (m Error) Type() MessageType { return MsgError }

// encode returns the payload of the message.
func (m Error) encode() []byte {
	b := make([]byte, 0, 3+len(m.Message))
	b = append(b, byte(m.Code))
	b = binary.BigEndian.AppendUint16(b, uint16(len(m.Message)))
	b = append(b, m.Message...)
	return b
}

// reader reads the fields of a payload.
type reader struct {
	b   []byte
	err error
}

// newReader returns a reader for the payload.
func newReader(b []byte) *reader {
	return &reader{b: b}
}

// u8 reads an 8 bit field.
func (r *reader) u8() uint8 {
	if r.err != nil {
		return 0
	}
	if len(r.b) < 1 {
		r.err = fmt.Errorf("%w: the message is too short", ErrInvalidMessage)
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

// u16 reads a 16 bit field.
func (r *reader) u16() uint16 {
	if r.err != nil {
		return 0
	}
	if len(r.b) < 2 {
		r.err = fmt.Errorf("%w: the message is too short", ErrInvalidMessage)
		return 0
	}
	v := binary.BigEndian.Uint16(r.b)
	r.b = r.b[2:]
	return v
}

// u32 reads a 32 bit field.
func (r *reader) u32() uint32 {
	if r.err != nil {
		return 0
	}
	if len(r.b) < 4 {
		r.err = fmt.Errorf("%w: the message is too short", ErrInvalidMessage)
		return 0
	}
	v := binary.BigEndian.Uint32(r.b)
	r.b = r.b[4:]
	return v
}

// u64 reads a 64 bit field.
func (r *reader) u64() uint64 {
	if r.err != nil {
		return 0
	}
	if len(r.b) < 8 {
		r.err = fmt.Errorf("%w: the message is too short", ErrInvalidMessage)
		return 0
	}
	v := binary.BigEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v
}

// fixed reads a field of a fixed size.
func (r *reader) fixed(n int) []byte {
	if r.err != nil {
		return nil
	}
	if len(r.b) < n {
		r.err = fmt.Errorf("%w: the message is too short", ErrInvalidMessage)
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

// str reads a field that holds a string.
func (r *reader) str(max int) string {
	n := int(r.u16())
	if r.err != nil {
		return ""
	}
	if n > max {
		r.err = fmt.Errorf("%w: the string is %d bytes long, the limit is %d bytes", ErrInvalidMessage, n, max)
		return ""
	}
	return string(r.fixed(n))
}

// done reports whether the payload was read to its end.
func (r *reader) done() error {
	if r.err != nil {
		return r.err
	}
	if len(r.b) != 0 {
		return fmt.Errorf("%w: the message holds %d bytes too many", ErrInvalidMessage, len(r.b))
	}
	return nil
}

// decodeHello decodes the payload of a hello.
func decodeHello(payload []byte) (Hello, error) {
	var m Hello
	r := newReader(payload)
	m.ProtocolVersion = r.u32()
	copy(m.DatabaseID[:], r.fixed(databaseIDSize))
	m.StreamID = r.u64()
	m.Epoch = r.u64()
	m.BackupID = r.str(MaxBackupIDLen)
	m.AppliedSeq = r.u64()
	copy(m.Nonce[:], r.fixed(nonceSize))
	copy(m.MAC[:], r.fixed(macSize))
	if err := r.done(); err != nil {
		return m, err
	}
	return m, nil
}

// decodeHelloAck decodes the payload of a hello answer.
func decodeHelloAck(payload []byte) (HelloAck, error) {
	var m HelloAck
	r := newReader(payload)
	m.ProtocolVersion = r.u32()
	m.Epoch = r.u64()
	m.ResumeSeq = r.u64()
	copy(m.Nonce[:], r.fixed(nonceSize))
	copy(m.MAC[:], r.fixed(macSize))
	if err := r.done(); err != nil {
		return m, err
	}
	return m, nil
}

// decodeBatch decodes the payload of a batch.
func decodeBatch(payload []byte) (Batch, error) {
	var m Batch
	r := newReader(payload)
	for len(r.b) > 0 {
		n := int(r.u32())
		if r.err != nil {
			return m, r.err
		}
		if n == 0 {
			return m, fmt.Errorf("%w: the batch holds a segment without bytes", ErrInvalidMessage)
		}
		m.Segments = append(m.Segments, r.fixed(n))
	}
	if err := r.done(); err != nil {
		return m, err
	}
	return m, nil
}

// decodeAck decodes the payload of an acknowledgement.
func decodeAck(payload []byte) (Ack, error) {
	var m Ack
	r := newReader(payload)
	m.AppliedSeq = r.u64()
	m.Epoch = r.u64()
	if err := r.done(); err != nil {
		return m, err
	}
	return m, nil
}

// decodeHeartbeat decodes the payload of a heartbeat.
func decodeHeartbeat(payload []byte) (Heartbeat, error) {
	var m Heartbeat
	r := newReader(payload)
	m.UnixMillis = r.u64()
	if err := r.done(); err != nil {
		return m, err
	}
	return m, nil
}

// decodeFlowControl decodes the payload of a flow control message.
func decodeFlowControl(payload []byte) (FlowControl, error) {
	var m FlowControl
	r := newReader(payload)
	m.MaxUnackedBytes = r.u64()
	if err := r.done(); err != nil {
		return m, err
	}
	return m, nil
}

// decodePause decodes the payload of a pause.
func decodePause(payload []byte) (Pause, error) {
	var m Pause
	r := newReader(payload)
	m.AppliedSeq = r.u64()
	if err := r.done(); err != nil {
		return m, err
	}
	return m, nil
}

// decodePaused decodes the payload of the answer of a pause.
func decodePaused(payload []byte) (Paused, error) {
	var m Paused
	r := newReader(payload)
	m.LastSentSeq = r.u64()
	if err := r.done(); err != nil {
		return m, err
	}
	return m, nil
}

// decodeResume decodes the payload of a resume.
func decodeResume(payload []byte) (Resume, error) {
	var m Resume
	r := newReader(payload)
	m.AppliedSeq = r.u64()
	if err := r.done(); err != nil {
		return m, err
	}
	return m, nil
}

// decodeResyncRequired decodes the payload of a resync.
func decodeResyncRequired(payload []byte) (ResyncRequired, error) {
	var m ResyncRequired
	r := newReader(payload)
	m.Reason = ResyncReason(r.u8())
	m.Epoch = r.u64()
	m.Seq = r.u64()
	if err := r.done(); err != nil {
		return m, err
	}
	return m, nil
}

// decodeError decodes the payload of an error.
func decodeError(payload []byte) (Error, error) {
	var m Error
	r := newReader(payload)
	m.Code = ErrorCode(r.u8())
	m.Message = r.str(1024)
	if err := r.done(); err != nil {
		return m, err
	}
	return m, nil
}
