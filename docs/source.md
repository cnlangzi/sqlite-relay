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

## The restart and the continuity

The chain of the segments has to cover the primary database: a transaction
that the chain does not hold is a transaction the backup does not get. The
write-ahead log is reset when the last connection of the primary database
closes. A reset starts a new generation of the log: the frame numbers start
over and the transactions that the chain does not hold are gone.

The runtime decides once whether the chain survived the reset. The decision
waits for the generation of the log: a log that does not exist yet has none.

- The durable acknowledgement of the spool is zero: the chain is empty, the
  runtime starts a new chain.
- The generation of the durable acknowledgement is the generation of the log:
  the log was not reset, the runtime continues where the chain ended.
- The generation of the log is another one and the marker of the last clean
  close describes the end of the chain: the log was reset after the runtime
  closed, the chain was complete then and the reset lost nothing. The chain
  continues across the generation.
- The generation of the log is another one and the marker describes another
  chain or there is none: the chain may have a hole. The runtime resets the
  chain, it takes a snapshot of the primary database and it tells the backup
  that it needs a new baseline.

The marker is the file `<root of the spool>/<database id>/peer.json`. It
holds the sequence number and the position of the end of the chain. The
runtime writes it when it closes and the chain ends where the capture cursor
of the driver ends: the chain covers the primary database then. A chain that
ends somewhere else may have a hole and no marker is written. The marker is
removed when the chain is reset and when the chain is not complete when the
runtime closes. The runtime reports a chain that is not complete in its
statistics.

The order of the shutdown decides whether the chain survives the restart:

1. The business stops writing to the primary database.
2. The runtime is closed. It writes the transactions that are still pending
   into the spool, it captures the transactions that were committed after the
   last round, it publishes the segments and it writes the marker.
3. The driver is closed. The application closes it, the runtime does not.

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

A log that was reset while the runtime was down breaks the chain as well: the
runtime judges the chain when it starts, see
[the restart and the continuity](#the-restart-and-the-continuity).

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

## Benchmarks

### Method

The benchmarks of the relay live in the `e2e` package and in the `spool`
package. The ones of the `e2e` package carry the build tag `e2e`: they start
the two sides of a relay in their own processes and they talk over a real TCP
connection. The target `make bench` runs them, one package after the other:

```
SQLITE_RELAY_E2E_ROOT=$PWD/.e2e-root go test -bench . -benchmem -run '^$' -tags e2e -p 1 ./e2e/ ./spool/
```

The three configurations are the primary database alone, the primary database
with the source runtime and the primary database with the source runtime and
a backup that applies the increments of the chain.

| Benchmark | Load | Configuration |
|---|---|---|
| `BenchmarkTheRelayUnderLoad` | one transaction per iteration | the three configurations |
| `BenchmarkTheCommitLatencyOfTheRelay` | one transaction per iteration | the three configurations |
| `BenchmarkTheStatementThroughputOfTheRelay` | one transaction per iteration: an insert, an update, a delete and a transaction of a hundred rows | the three configurations |
| `BenchmarkTheTransferOfTheSegments` | a backlog of 2000 transactions | relay with a backup |
| `BenchmarkTheApplyOfTheSegments` | the same backlog | relay with a backup |
| `BenchmarkTheCatchUpAfterADisconnect` | the same backlog, the backup is killed while it piles up | relay with a backup |
| `BenchmarkTheResourcesDuringTheBackupPause` | 50 transactions per second for four seconds | relay with a backup, the backup is paused |
| `BenchmarkTheSpoolSyncs` | transactions of 64 kilobytes | the three modes of the spool |

Measured on Linux/amd64 with an Intel Core i7-3537U CPU at 2.00 GHz, Go
1.27.0 and SQLite 3.54.1. The work directories of the benchmarks are in the
checkout, on the file system of the machine: the numbers of the transfer and
of the apply are the numbers of the syncs of that file system. The numbers of
one run are in the tables below: they depend on the machine and on the load of
the machine.

### The commit latency

The latency of the commits of the business. The benchmark commits one
transaction per iteration into a table with two columns.

| Configuration | mean (ns) | p50 (ns) | p95 (ns) | p99 (ns) |
|---|---|---|---|---|
| `no-relay` | 33291 | 30523 | 43127 | 54826 |
| `relay-without-backup` | 49495 | 43255 | 62781 | 118598 |
| `relay-with-backup` | 57902 | 44979 | 67809 | 262846 |

The source runtime costs thirteen microseconds on the median of the commits:
the WAL hook of the driver hands the notification to the capture and it
returns. The business does not wait for the capture, for the spool or for the
network.

The backup adds two microseconds to the median and it moves the tail of the
distribution: the ninety-ninth percentile of the commits with a backup is
six times their median. The backup applies the increments of the chain in
the same process and the pages it writes compete with the writes of the
business for the disk.

### The throughput of the statements

The number of the transactions per second of every statement of the business.
Every iteration is one transaction.

| Statement | `no-relay` | `relay-without-backup` | `relay-with-backup` |
|---|---|---|---|
| insert | 31001 | 19912 | 14558 |
| update | 28628 | 18337 | 15444 |
| delete | 13301 | 10089 | 8486 |
| a transaction of a hundred rows | 823 | 712 | 646 |

A transaction of a hundred rows takes 1.2 milliseconds: it costs thirty-eight
times a transaction of one row and it writes two and a half times as many
rows per second. The source runtime costs fifty-six percent of the time of a
transaction of one row and sixteen percent of the time of a transaction of a
hundred rows: the commit of a big transaction writes a hundred times more
pages and the capture of the source reads them all.

### The transfer of the segments

The source pushes the segments of the chain to the backup over TCP. The
backup is paused while the primary writes the backlog of 2000 transactions,
the source keeps the increments in its spool and it pushes them as fast as it
can after the resume.

| Metric | Value |
|---|---|
| bandwidth of the chain | 145312 B/s |
| average size of a segment | 22877 B |
| segments of the backlog | 366 |

The bandwidth is the number of the bytes of the chain per second, from the
resume until the backup holds every increment. The segments are small: a
capture round closes a segment and a segment holds the transactions of one
round.

The source publishes a segment and the backup makes the transactions of a
segment durable before it acknowledges them: the bandwidth of the chain is
the bandwidth of the syncs of the file system of the machine.

### The apply of the segments

The backup applies the increments of the chain one by one. The load is the
same backlog of 2000 transactions.

| Metric | Value |
|---|---|
| transactions applied per second | 42 |
| time to apply the backlog | 47.89 s |

The applier syncs the database of the backup and the state of the backup at
the boundary of every segment: the rate of the apply is the rate of the
syncs of the file system of the machine.

### The catch-up after a disconnect

The backup is killed and the primary writes the backlog of 2000 transactions
while it is away. The backup comes back and the measurement starts when the
backup is started again and ends when it holds every increment.

| Metric | Value |
|---|---|
| time of the catch-up | 47.94 s |
| transactions applied per second | 42 |
| bandwidth of the chain | 174594 B/s |

### The resources during the pause of the backup

The backup is paused and the source keeps working: it captures the increments
of the primary database and it writes them into its spool. The primary writes
50 transactions per second for four seconds. The numbers are the resources of
the process of the primary over the window of the pause.

| Metric | Value |
|---|---|
| time of the CPU | 203 ms |
| peak of the memory | 15104 kB |
| blocks read | 0 |
| blocks written | 5056 |
| time of the window | 4.193 s |

The source of a relay with a paused backup spends the CPU of the commit of
the business and of the capture: it writes the increments into its spool and
it does not push anything. The blocks it writes are the pages of the
transactions and the segments of the spool.

### The syncs of the spool

The number of the fsyncs the spool runs per megabyte it writes. The
transaction of the benchmark holds 64 kilobytes.

| Mode | fsyncs per MiB |
|---|---|
| `OFF` | 0 |
| `NORMAL` | 17.33 |
| `FULL` | 63.49 |

The barrier of `OFF` is the write of the operating system and it costs
nothing. The barrier of `NORMAL` is a batch of 32 segments: the segments of
the batch are synced one by one, the directory of the segments and the state
of the capture are synced once. The barrier of `FULL` syncs the segment, the
directory of the segments and the state of the capture on every transaction,
and it costs nearly four times as much as the barrier of `NORMAL`.

### The cost of the relay on the commit path

The benchmark commits one transaction per iteration into a table with two
columns.

| Configuration | ns/op | WAL bytes | WAL frames |
|---|---|---|---|
| `no-relay` | 31893 | 156733072 | 38042 |
| `relay-without-backup` | 49844 | 75676192 | 174 |
| `relay-with-backup` | 62763 | 68495032 | 307 |

The write-ahead log of the primary database grows with the number of the
commits in every configuration: SQLite does not shrink the log, it writes
the frames back into the database file and reuses the space of the log. The
source runtime keeps the content of the log smaller: it acknowledges the
frames it captured, the driver writes them back and the log starts over.

## The example

The directory `examples/relay` holds a program that shows how an application
uses the relay. It opens a primary database, it attaches the source runtime
to it, it connects a backup to the runtime, it writes rows, it waits for the
backup to catch up, it pauses the backup, it reads the database of the backup
while it is paused and it resumes the backup.

```
go run ./examples/relay
```

The program works in a temporary directory and removes it when it ends. The
flag `-dir` names a work directory that is kept, the flag `-addr` names the
address the primary listens on.

```
go run ./examples/relay -dir /tmp/relay
```

The two sides of the relay run in one program in the example. A deployment
runs the primary side with the application and the backup side on another
machine: the example is the configuration of the primary side with the backup
side next to it. The example prints the number of the rows of the primary
database and of the database of the backup after every step.

## Not supported

### A primary database that is written by somebody else

The driver is the only component that may checkpoint the primary database. A
connection that checkpoints it behind the back of the driver makes the driver
fatal: the captured increments cannot be trusted any more.

### Writing to the primary database while the runtime is closed

The chain of the segments has to cover the primary database when the runtime
closes. A transaction that is committed after the runtime closed is not in
the chain and the write-ahead log that holds it is reset when the driver
closes. The runtime cannot see it: the backup needs a new baseline.

### More than one backup per database

The protocol serves one backup at a time. The distribution of the segments of
the chain among several backups is not part of the protocol.

### An encrypted connection

The protocol does not support an encrypted connection. The application wraps
the listener when it needs one.
