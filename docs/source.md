# Source

The source is the primary end of a relay. It holds the primary database, it
captures the committed transactions of the write-ahead log, it keeps them in a
local spool and it pushes the segments of the spool to the backup.

The source never waits for the backup. A business transaction does not wait
for the network, for the capture acknowledgement or for the backup
acknowledgement. When the spool is full the capture stands still, the
increments are not lost and the business keeps writing.

## The two sides of a relay

A relay has two sides that share one chain of segments.

The **primary side** runs the driver, the spool and the source runtime. It
holds the primary database and the spool of its increments. The driver reads
the committed transactions out of the write-ahead log, the runtime writes
them into the spool and the protocol server pushes the published segments to
the backup.

The **backup side** runs the backup runtime. It holds the backup database,
a plain SQLite file that is a copy of the primary database. It applies the
segments of the chain into that file and it acknowledges the sequence number
it applied. The backup side is described in [docs/backup.md](backup.md).

The two sides share the sequence number and the epoch of the chain.

- The **sequence number** is a number the spool hands out. It counts the
  transactions of the chain, it survives a restart of the source and it is
  the unit of both acknowledgements. On the primary side the sequence number
  names a transaction of the spool, on the backup side it names a transaction
  that is applied and persisted in the backup database.
- The **epoch** is the generation of the chain. The source raises it when the
  chain is reset, and the backup adopts the epoch of the baseline it
  installed. The two sides only compare sequence numbers of the same epoch:
  the source refuses a backup that belongs to another generation of the chain
  and it answers with a `ResyncRequired`.

A backup that is paused holds the backup database for the business. The
source keeps capturing the increments of the primary database while the
backup is paused, it keeps the segments in the spool and it does not push
them. The backup database is not written while the backup is paused, not by
the relay and not by a restart of the relay. The two sides of a paused relay
are described in [the pause and the resume](backup.md#the-pause-and-the-resume)
of the backup documentation.

## The configuration

```go
cfg := source.Config{
	Driver:      drv,
	Spool:       spool.Config{Root: "/var/lib/relay/spool"},
	Protocol:    protocol.Config{TokenFile: "/etc/relay/token", DatabaseID: "app.db", StreamID: 1},
	SnapshotDir: "/var/lib/relay/snapshots",
}
p, err := source.Open(ctx, cfg)
```

| Field | Meaning |
|---|---|
| `Driver` | the driver of the primary database, opened by the application |
| `Spool` | the settings of the local spool, the runtime opens the spool |
| `Protocol` | the settings of the connection to the backup |
| `PeerID` | the name of the peer the acknowledgement of the backup belongs to, `backup` by default |
| `SnapshotDir` | the directory the snapshots are written into |
| `ScanInterval` | the interval of the periodic scan, one second by default |
| `GCInterval` | the interval of the poll of the acknowledgement of the backup, five seconds by default |

The application opens the driver and it closes it. The runtime opens the
spool and it closes the spool: the name of the database and the name of the
peer of the spool are taken from the protocol configuration and from `PeerID`.

`Run` serves the listener of the backup and it starts the capture and the
collection of the segments. The listener is created by the application: the
protocol does not support an encrypted connection, an application that needs
one wraps the listener itself.

```go
ln, err := net.Listen("tcp", "127.0.0.1:9000")
if err != nil {
	return err
}
if err := p.Run(ctx, ln); err != nil {
	return err
}
```

## The capture

The capture is woken by the notifications of the driver and by the periodic
scan. Both are a hint that there is something to capture. The boundary of an
increment is decided by the bytes of the log and not by a notification: a
notification that is merged or lost costs a round, it never costs an
increment. The periodic scan finds the increments that the notifications did
not report.

A capture round runs in a fixed order:

1. The driver is scanned. The scan hands out the transactions that were
   committed since the last round.
2. Every transaction is appended to the spool. A segment that is full is
   closed and the transaction is written into the next one.
3. The segment is closed.
4. The barrier of the spool runs when it is due.
5. The durable acknowledgement of the spool is read and it is handed to the
   driver as the capture acknowledgement.

The spool hands out the sequence numbers. The sequence number of the driver
is not used: it does not survive a restart.

### The spool that is full

A spool that reached its limit refuses the transactions that do not fit into
it. The runtime keeps them: they are written into the spool as soon as it
has room again. While the spool is full the runtime does not scan the
driver, so that the backlog does not grow in the memory of the runtime, and
it does not hand a capture acknowledgement to the driver, so that the frames
of the log stay in the log.

The capacity and the age of the backlog are reported by the statistics of the
runtime.

### The capture that stands still

The producer of the snapshots suspends the capture while it takes a snapshot:
the snapshot and the drain that follows it have to see the same transactions.
`Suspend` waits for the round that is running to end and it keeps the
capture from starting a new one until the returned function is called.

## The acknowledgements

A relay has two acknowledgements and they never stand for each other.

The **capture acknowledgement** says that the increments are in the local
spool. It is handed to the driver as soon as the increments crossed the
persistence barrier of the spool. It does not wait for the backup: the
driver may write the frames of the log back into the primary database and the
WAL does not grow while the backup is away.

The **backup acknowledgement** says that the backup applied the increments and
persisted the sequence number. The runtime polls the protocol for it, writes
it into the spool and collects the segments it covers. The acknowledgement is
written before the segments are removed: a segment that is removed may not be
sent again.

## The restart

The segments of the spool are the truth, the state file of the spool is an
accelerator. The runtime does not keep a cursor in its memory: it reads the
durable acknowledgement of the spool when it starts and it continues from
that position.

The driver hands out the increments of the whole generation of the log again
after a restart. The runtime skips the increments that the spool holds
already: an increment of the same generation that was committed at or before
the position of the durable acknowledgement is in the spool. An increment of
another generation is new data.

The frames that the log held before the driver started belong to the
consumer: it holds the increments it has seen already. The driver does not
report them as lost when the log is reset, the consumer would ask for a new
snapshot for increments it holds already.

The segments that were not published when the runtime died are discarded with
the temporary directory of the spool. Their transactions are captured again
from the log.

## The chain that breaks

The chain is not continuous any more when the log was reset while the driver
had not captured all of its frames. The driver reports that a new snapshot is
needed and the runtime repairs the chain:

1. The spool is reset. The segments are gone, the epoch is raised and the
   sequence numbers start over.
2. A snapshot of the primary database is taken. It moves the capture cursor of
   the driver to the boundary of the snapshot, the capture continues after
   it.
3. The runtime reports that the backup needs a new baseline.

The session of the backup ends with the reset: the segments of the old chain
are gone and the sequence numbers of the new chain start over, a session that
waited for the segments of the old chain would wait for ever. The backup
connects again, the source tells it that a new baseline is needed and the
backup asks for a snapshot in the hello of its next connection. The source
produces one and the backup installs it over the old database.

The acknowledgement of the backup belongs to the chain it was given in. The
source does not report the sequence number of a chain that was reset: it
would collect the segments of the new chain before the backup applied them.

## The statistics

`Stats` reports the state of the runtime:

| Field | Meaning |
|---|---|
| `Driver` | the state of the driver: the generation of the log, the capture cursor, the acknowledged position, the checkpoint and the reclaim |
| `Spool` | the state of the spool: the segments, the bytes, the durable sequence, the acknowledged sequence, the age of the backlog and the warnings |
| `CapturedSeq` | the highest sequence number that is durable in the spool |
| `BackupSeq` | the highest sequence number the backup acknowledged |
| `LagSeq` | the number of the sequence numbers the backup did not acknowledge yet |
| `Lag` | the time since the last acknowledgement of the backup |
| `LastAck` | the time of the last acknowledgement of the backup |
| `SpoolFull` | the spool reached its limit and the capture stands still |
| `SnapshotRequired` | the chain is broken and the backup needs a new baseline |
| `SnapshotReason` | why the chain is broken |
| `LastError` | the description of the last failure |

## Not supported

### A primary database that is written by somebody else

The driver is the only component that may checkpoint the primary database. A
connection that checkpoints it behind the back of the driver makes the driver
fatal: the captured increments cannot be trusted any more.

### More than one backup per database

The protocol serves one backup at a time. The distribution of the segments of
the chain among several backups is not part of the protocol.

### An encrypted connection

The protocol does not support an encrypted connection. The application wraps
the listener when it needs one.
