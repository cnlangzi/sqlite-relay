# Protocol

The protocol carries the segments of a spool to a backup peer. The backup
dials the source, both sides prove that they share the token of the
deployment and the source pushes the segments in the order of their sequence
numbers. The backup applies the transactions of a segment one by one and
acknowledges the sequence number it applied. A connection that breaks is
resumed at the sequence number the backup applied and persisted, so that no
segment is lost and no segment is applied twice.

The protocol does not apply anything itself and it does not write to the
primary database. The backup side of the protocol receives the segments and
hands the transactions of them to the sink that applies them, the source
side receives the segments of a spool and pushes them.

A backup that has no baseline asks for a snapshot in the hello of its
connection. The source produces one and sends it, the backup installs it and
the increments of the chain that follow the baseline of the snapshot bring
the backup up to date.

The transport is TCP. There is no TLS: the deployment is an internal network
and the shared token is the secret that both sides prove themselves with.

## The frame

Every message of the protocol travels in a frame:

```text
offset  size  field
0       4     magic "SQRL"
4       1     version
5       1     type
6       2     flags (0)
8       4     length (the size of the payload)
12      ...   payload
```

Every field is big endian, like the fields of a segment. The magic tells the
two sides that they speak the same protocol, the version names the format of
the frame and the length names the size of the payload. The flags are
reserved, they are zero and the receiver ignores them.

A frame whose magic is not `SQRL`, whose version is not the version of the
package or whose payload is bigger than 64MB is not a frame of the protocol.
The peer that reads it answers with an error and closes the connection.

The frame carries no checksum. TCP keeps the bytes of the stream intact and
a segment carries the checksums of its header, of its payload and of its
trailer.

## The messages

| Type | Direction | Payload |
|---|---|---|
| `HELLO` | Backup→Source | protocolVersion u32, databaseID 16B, streamID u64, epoch u64, needsSnapshot u8, backupID (u16 length + bytes, at most 64), appliedSeq u64, nonce 32B, mac 32B |
| `HELLO_ACK` | Source→Backup | protocolVersion u32, epoch u64, resumeSeq u64, nonce 32B, mac 32B |
| `BATCH` | Source→Backup | the segments, each of them with a u32 length that is not zero |
| `SNAPSHOT_META` | Source→Backup | formatVersion u32, databaseID 16B, streamID u64, epoch u64, baselineSeq u64, pageSize u32, bytes u64, checksum 32B |
| `SNAPSHOT_DATA` | Source→Backup | the bytes of a part of the file of the snapshot |
| `ACK` | Backup→Source | appliedSeq u64, epoch u64 |
| `HEARTBEAT` | both | unixMillis u64 |
| `FLOW_CONTROL` | Backup→Source | maxUnackedBytes u64 |
| `PAUSE` | Backup→Source | appliedSeq u64 |
| `PAUSED` | Source→Backup | lastSentSeq u64 |
| `RESUME` | Backup→Source | appliedSeq u64 |
| `RESYNC_REQUIRED` | both | reason u8, epoch u64, seq u64 |
| `ERROR` | both | code u8, message (u16 length + bytes) |

A batch carries no sequence numbers of its own. The header and the trailer of
a segment name the range of sequence numbers it holds, the epoch it belongs
to and the checksum of its payload, so the receiver learns everything it
needs from the segment itself.

A batch holds at most 64 segments and its payload holds at most 16MB. A
segment that does not fit into a batch is sent in a batch of its own. A
segment that does not fit into a frame is bigger than 64MB: that is a
mistake in the configuration, the source reports it and sends nothing. A
segment is never cut and never split.

## The handshake

The backup dials the source and sends the `HELLO`. It names the protocol
version, the database, the stream, the backup and the sequence number it
applied and persisted. The nonce and the message authentication code prove
that the backup knows the token.

The source answers with the `HELLO_ACK`. It names the protocol version, the
epoch of the chain and the sequence number it continues after. Its nonce and
its message authentication code prove that the source knows the token.

The code of the `HELLO` is

```text
HMAC-SHA256(token, "backup-hello" || nonce || the bytes of the HELLO before the code)
```

and the code of the `HELLO_ACK` is

```text
HMAC-SHA256(token, "source-hello" || the nonce of the HELLO || the nonce of the HELLO_ACK || the bytes of the HELLO_ACK before the code)
```

Both sides compute the code and compare it with the code they received.

The source checks the following, in this order:

1. The protocol version. A version it does not speak is answered with an
   error that carries the code `version`.
2. The message authentication code. A hello that does not match is answered
   with an error that carries the code `authentication`.
3. The identifier of the backup. A backup that is not on the list of the
   backups that may connect is answered with an error that carries the code
   `identity`.
4. The identifier of the database and the number of the stream. A hello that
   names another database or another stream is answered with an error that
   carries the code `identity`.
5. The epoch. A hello of another generation of the chain is answered with a
   `RESYNC_REQUIRED` and not with an error, see below.
6. The chain. A backup that needs a sequence number that is older than the
   oldest segment the source can still provide is answered with a
   `RESYNC_REQUIRED`.

A check that fails ends the connection. The source does not push a segment to
a backup that did not prove itself.

The epoch and the applied sequence of the backup come from its sink, not from
the configuration of the protocol. The two belong together: they are the two
halves of the position of the backup in the chain.

A hello that asks for a snapshot skips the checks of the epoch and of the
chain: the backup has no chain yet, there is nothing to compare its epoch and
its applied sequence with. The snapshot carries the epoch of the chain it is
bound to and the backup adopts it.

The token is read from its file again for every handshake, so that a token
that was rotated takes effect without a restart. There is no grace period
with two tokens: a connection that is open keeps the token it proved itself
with, a new connection has to use the token that the file holds.

## The push

The source sends the segments of the spool in the order of their sequence
numbers. A batch holds the segments one after the other, the receiver reads
them with the `segment` package.

The backup applies the transactions of a segment one by one and skips the
transactions it applied already. A transaction is applied when the sink
applied it and persisted the applied sequence. The sink is the part that
writes to the backup database, the protocol only asks it for the sequence
number it holds.

A sink that makes the applied transactions durable at the boundary of a
segment implements the `Flusher` interface. The protocol calls it before it
acknowledges the segment: the acknowledgement means that the transactions up
to the sequence number it names are applied and that the sequence number is
persisted.

The boundary of a segment and the boundary of the applied sequence do not
line up: a resumed connection may start in the middle of a segment. The
transactions up to the applied sequence are skipped, the rest of the segment
is applied. A segment that was applied already is not applied again, it is
acknowledged again.

The acknowledgement of the backup means that the transactions up to the
sequence number it names are applied and that the sequence number is
persisted. The source remembers it and calls it `AppliedSeq`.

## The snapshot

A backup without a baseline asks for a snapshot in the hello of its
connection. The source produces one and sends it: the `SNAPSHOT_META` names
the database, the generation of the chain, the sequence number of the last
transaction the snapshot holds, the size of the pages, the size of the file
and its SHA-256. The `SNAPSHOT_DATA` messages carry the bytes of the file,
four megabytes at a time. There is no length prefix: the size of the
payload of the frame is the size of the part.

The bytes of the snapshot are not bound by the window of the backup. The
window counts the segments of the chain that are in flight, a snapshot is a
file that is read once from the beginning to the end. The back pressure of
the transport is enough.

The backup installs the snapshot and acknowledges the baseline sequence of
the manifest. The source continues with the segments that hold the
transactions after the baseline.

The manifest is the link between the snapshot and the chain: the increments
of the chain that follow the baseline bring the database up to date without a
gap and without a repeat. The backup checks the size and the checksum of the
file against the manifest, the source checks the database and the epoch of
the manifest against its own.

A source that cannot produce a snapshot answers with an error. The backup
does not install anything and the session ends.

## The resume

There are two ways to continue a stream, they mean the same thing:

- The `HELLO` carries the sequence number the backup applied and persisted.
  The source continues after it.
- The `RESUME` of a connection that is open carries the sequence number the
  backup applied and persisted. The source continues after it and answers
  the `PAUSED` it sent before.

The source sends the segments that hold a transaction after the sequence
number it continues after. A segment that holds transactions before it is
sent again, the backup skips the transactions it applied already.

A lost acknowledgement does not trigger a retransmission. The connection
breaks, the backup reconnects with the sequence number it applied and
persisted and the source sends the segments after it again. The segments that
are sent twice are not applied twice.

The source that cannot provide the sequence number the backup needs sends a
`RESYNC_REQUIRED` and stops. The backup that receives a segment whose first
transaction is after the sequence number it applied sends a `RESYNC_REQUIRED`,
applies nothing and closes the connection. The same happens when the epoch
of a segment is not the epoch of the connection and when a segment does not
hold what it says: the chain of the segments is not continuous any more and
only a new snapshot brings the backup back.

## The flow control

The backup tells the source how many bytes it may have in flight with a
`FLOW_CONTROL`. The limit is absolute: it is the number of bytes the source
sent and that the backup did not acknowledge yet. The source stops sending
when the bytes in flight reach the limit and continues when the backup
acknowledges or when the backup sends a bigger limit.

The source has a limit of its own, 8MB by default. It is the limit that
applies until the backup sends a `FLOW_CONTROL`. A batch never holds more
than the window allows, so the bytes in flight stay below the limit. A
segment that is bigger than the window is sent anyway, it is not cut.

A limit of zero stops the source until the backup sends a bigger limit. A
limit that does not fit into an int is clamped, a peer cannot open the
window of the source by accident.

## The pause

`Pause` asks the source to stop pushing segments. The source keeps the
connection, it stops sending segments and answers with a `PAUSED` that names
the last sequence number it sent. `Pause` waits for that answer and it is
idempotent: a pause that is already running returns without an error.

The backup stops applying the segments as soon as the pause is asked for: the
batches that arrive while the pause runs are skipped and they are not
acknowledged. The source sends them again after the resume.

The answer comes from the side that sends the segments: no segment follows
the `PAUSED`. A caller that pauses before it reads the backup database may
rely on that.

The pause survives a broken connection. The backup pauses the source again
as soon as the connection is open, and a pause that is asked for while the
backup is not connected is remembered.

`Resume` tells the source to push again. The source continues after the
sequence number the backup names.

A pause stops the transfer of the segments, it does not stop the capture of
the primary database and it does not touch the spool. The backup side of the
database is the part that has to be paused as well, that is not the job of
the protocol.

## The heartbeat and the timeout

Both sides send a `HEARTBEAT` every 5 seconds. A heartbeat is not answered:
the peer sends its own, an answer would only bounce between the two sides. A
connection that stays silent for 30 seconds is closed: the peer is gone or the
network is broken. The idle timeout has to be at least twice the heartbeat
interval, a connection that dies before the first heartbeat is not a
connection.

## The epoch

The epoch names the generation of the chain. It belongs to the durable
applied sequence of the backup: a backup that changes the epoch of its
applied sequence needs a new snapshot, and a backup that keeps the epoch of a
snapshot takes the increments of that chain again.

The backup sends the epoch of its applied sequence in the hello. The epoch
comes from the sink of the backup, the configuration of the protocol does not
hold it: the state of the backup is the only place that knows it. The source
refuses a hello of another generation of the chain with a `RESYNC_REQUIRED`
that names the epoch of the source. `Backup.Epoch` returns that epoch, so
that the caller keeps it together with the applied sequence of the sink.

## The reconnection

The backup opens the connection, because the applied sequence and the pause
live on its side. A connection that breaks is opened again with a delay that
starts at one second, doubles with every attempt and stops at one minute.
The delay carries a jitter of up to half of the delay, so that the backups of
a deployment do not reconnect at the same time. The delay never grows beyond
the maximum. A connection that lived longer than a heartbeat starts the delay
over.

A backup that needs a new snapshot stops: the run ends with
`ErrResyncRequired` and the errors of the package can be matched with
`errors.Is`. The caller asks for a snapshot in the hello of the next
connection and the source sends it.

The source keeps serving after a resync. The backup that needs a new
baseline asks for one in the hello of its next connection, the source sends
it and the backup installs it over the database it had.

## The interfaces

The protocol package defines the interfaces of the two sides. The source side
takes the segments from a `SegmentSource` and the backup side hands the
transactions to a `SegmentSink`:

```go
type SegmentSource interface {
	Epoch() uint64
	OldestSeq() uint64
	SegmentsAfter(ctx context.Context, seq uint64, limit int64) ([]Segment, error)
}

type SegmentSink interface {
	Apply(ctx context.Context, tx segment.Tx) error
	AppliedSeq(ctx context.Context) (uint64, error)
	Epoch() uint64
}
```

A sink that makes the applied transactions durable at the boundary of a
segment implements the `Flusher` interface, a sink that takes a snapshot to
establish its baseline implements the `SnapshotSink` interface and the source
that produces the snapshot implements the `SnapshotProvider` interface:

```go
type Flusher interface {
	Flush(ctx context.Context) error
}

type SnapshotSink interface {
	NeedsSnapshot() bool
	BeginSnapshot(ctx context.Context, meta SnapshotMeta) (io.Writer, error)
	CommitSnapshot(ctx context.Context) error
}

type SnapshotProvider interface {
	Snapshot(ctx context.Context) (SnapshotMeta, io.ReadCloser, error)
}
```

`SpoolSource` in `spool_source.go` is the adapter of the spool. It takes the
segments that the spool published, it never sends a segment that waits for
the barrier of the spool.

The limit of `SegmentsAfter` names the number of bytes the source may send at
the moment. The source asks for the segments it can send, so that a segment
of the spool is read once and not once per batch.

The source accepts the connections:

```go
src, err := protocol.NewSourceWithSnapshot(cfg, protocol.NewSpoolSource(sp), producer)
err = src.Serve(ctx, listener)
```

and the backup dials the source:

```go
b, err := protocol.NewBackup(cfg, sink)
err = b.Run(ctx, "10.0.0.2:9000")
```

`Serve` runs one connection at a time. A connection that failed is ended and
the next one is accepted.

## The configuration

| Setting | Default | Meaning |
|---|---|---|
| `TokenFile` | | the file that holds the shared token |
| `DatabaseID` | | the name of the database the segments belong to |
| `StreamID` | | the number of the stream |
| `BackupID` | | the name of the backup, the source checks it against its list |
| `AllowedBackups` | | the backups that may connect, every backup when the list is empty |
| `HeartbeatInterval` | 5s | the time between two heartbeats |
| `IdleTimeout` | 30s | the time a connection may stay silent |
| `MaxBatchBytes` | 16MB | the size of the payload of a batch |
| `MaxFrameBytes` | 64MB | the size of the payload of a frame |
| `MaxUnackedBytes` | 8MB | the bytes the source may have in flight |
| `BackoffMin` | 1s | the shortest delay between two connection attempts |
| `BackoffMax` | 60s | the longest delay between two connection attempts |

## Not supported

### More than one backup

The source serves one connection at a time and the list of the backups that
may connect holds the names of the backups of the deployment. Several
backups that push at the same time and the distribution of the segments
among them are not part of the protocol.

### A spool that is broken

A spool whose chain is broken ends the session of every connection: the
segments of the spool cannot be read. The source keeps accepting and the
backup asks for a snapshot when the spool is reset.

### A backup that is behind the source

The source sends a `RESYNC_REQUIRED` when the backup asks for a sequence
number that the source cannot provide any more. The protocol does not take
the snapshot, the peer that owns the snapshot takes it.

### The transfer of a snapshot

The snapshot is transferred in one piece: a transfer that breaks starts over
with a new snapshot. The source produces the snapshot again for the backup
that asks for it, so there is no file that has to survive the connection.

### The data of the segments

The protocol carries the segments as they are. It does not read the pages of
the primary database and it does not write to the backup database, the sink
does that.

### A connection that is encrypted

There is no TLS and there is no certificate. The token is the secret of the
deployment.
