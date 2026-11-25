# Primary Driver

The driver captures the committed changes of a SQLite database from its
write-ahead log. It hands the changes out as a stream of page level
increments and hands back a consistent copy of the database as a snapshot.

The driver owns every connection to the primary database inside the process.
It installs a WAL hook on each of them, reads the committed transactions out
of the WAL and keeps the WAL behind the acknowledged position, so that SQLite
cannot overwrite frames the consumer has not persisted yet.

The driver writes nothing into the primary database outside of the business
transactions. It has no table of its own, it replays no SQL and it adds no
write to the business path.

## Supported usage

### Opening the driver

```go
d, err := driver.Open(ctx, driver.Config{
    DSN: "file:/var/lib/app/app.db?_journal_mode=WAL",
})
if err != nil {
    return err
}
defer d.Close()
```

The DSN is a `mattn/go-sqlite3` DSN that points at a file on disk. A database
that lives in memory has no WAL and cannot be captured.

`Open` sets the invariants of every physical connection of the pool:
`journal_mode=WAL`, `wal_autocheckpoint=0`, a busy timeout and the WAL hook.
The autocheckpoint is switched off before the hook is installed, because
SQLite replaces a registered hook whenever the autocheckpoint is configured.
A connection that does not hold the invariants makes `Open` fail.

The business code uses `d.DB()` and nothing else. Every statement of the
business code goes through a connection of the driver.

### The capture loop

```go
for {
    select {
    case <-d.Events():
    case <-ticker.C:
    }
    batch, err := d.Scan(ctx)
    if err != nil {
        return err
    }
    for _, change := range batch.Changes {
        if err := spool.Append(change); err != nil {
            return err
        }
    }
    if len(batch.Changes) > 0 {
        if err := spool.Flush(ctx); err != nil {
            return err
        }
        if err := d.CaptureAck(ctx, batch.To); err != nil {
            return err
        }
    }
}
```

`Events` delivers a hint that the WAL moved. The hint may be dropped and
several commits may be reported as one, so the consumer has to look at the
batch and not at the notification. The periodic scan of the driver finds the
commits that no notification reported, so a dropped hint costs a delay and
not an increment.

`Scan` returns the transactions that were committed since the last call. A
change is always a whole transaction, and the batch holds all of them, so
the consumer may merge several transactions into one segment.

The page numbers of a change are in ascending order and `CommitPages` is the
size of the database in pages after the commit. A page beyond that size
belongs to a database that shrank and is not part of the change.

### The acknowledgement

`CaptureAck` says that the increments up to a position are on the spool. The
driver may only write the frames back into the database file that the
acknowledgement covers. As long as that holds, SQLite cannot reset the WAL
and overwrite frames that the consumer has not persisted yet.

The acknowledgement is about the local spool. It does not wait for the remote
side and it does not say that the remote side holds the data.

The acknowledged position has to be persisted. After a restart the driver
takes it in `Config.ResumePosition` and continues there. Without it the
driver starts at the beginning of the current WAL generation and hands out
the transactions that are still in the WAL. The consumer recognises the ones
it has seen already by their position.

A position belongs to the WAL generation it names. When SQLite resets the
WAL, the salt of the generation changes and the frame number starts over. A
position of an older generation does not say anything about the frames of the
current one. The driver forgets the acknowledged position when the
generation changes: the frames of the old generation are written back into
the database file, the new generation counts its frames from zero.

When SQLite resets the WAL while the driver did not capture all of its
frames, the driver reports `ErrSnapshotRequired`: the increments of the
frames that are gone are lost and the consumer needs a new baseline. The
frames that the WAL held before the driver started do not count: the
consumer holds the increments it has seen already.

### The WAL space

SQLite writes the frames of the WAL back into the database file and resets
the WAL when the whole log has been written back. The driver holds the
frames back until the consumer has acknowledged them, so the WAL grows while
the consumer is behind.

The driver gives the space back when the WAL is bigger than `ReclaimBytes`
or when `ReclaimInterval` has passed. It waits for the consumer to
acknowledge the frames of the WAL, takes the write lock and writes the whole
log back. The write lock stops the business writers for the time the
checkpoint takes. The stall stays below the busy timeout of the connections,
so a business write waits instead of failing.

`Stats` reports the stall and the outcome of the last reclaim.

`TruncateOnReclaim` lets the reclaim shrink the WAL file. SQLite does not
let a truncate run while the write lock is held, so a transaction that is
committed between the write lock being given back and the truncate is
written back without being acknowledged. The driver reads the frame count of
the WAL right before the truncate and gives up when the WAL holds more than
the acknowledged frames. The feature is off by default.

### Snapshots

```go
snap, err := d.Snapshot(ctx, "/var/lib/relay/snapshots/app.db")
if err != nil {
    return err
}
```

A snapshot is a SQLite database file that holds the state of the database at
the position the snapshot returns. The file is a plain database file that
SQLite opens as it is, without a second format in between.

The snapshot copies the database file and overlays the pages of the WAL that
are not in the file yet. It does not stop the business writes and it does
not wait for the WAL to be written back.

The snapshot is bound to a capture position. The increments that follow that
position bring the database up to date without a gap and without a repeat.
The consumer persists the position of the snapshot and hands it to the driver
as the `ResumePosition` after a restart.

A snapshot moves the capture cursor of the driver to the position of the
snapshot. The transactions between the cursor and the snapshot are part of
the snapshot and are not handed out again.

The driver retries a snapshot when the database file moved while it was
copied or when the result is not a valid database. A snapshot that does not
come out after `SnapshotRetries` attempts is an error.

### The quiesce gate

```go
if err := d.Quiesce(ctx); err != nil {
    return err
}
defer d.Resume()
```

`Quiesce` blocks new write transactions. A blocked writer waits for the
resume instead of failing with `SQLITE_BUSY`, so the window in which no new
write starts stays free of errors. `Quiesce` returns once the write
transactions that were open have finished.

The gate is applied when a statement runs and not when it is prepared, so a
statement that was prepared before the quiesce is held back as well. A
writer that waits for the resume follows the context of its caller and can
be cancelled.

`QuiesceWith` takes a `QuiesceOptions` and can block new read transactions
as well.

The snapshot path does not need the quiesce gate.

### The statistics

`Stats` reports the state of the driver: the generation of the log, the
capture cursor, the acknowledged position, the frames of the log, the
checkpoint, the reclaim and the last error.

| Field | Meaning |
|---|---|
| `Notifications` | the number of commit notifications the WAL hook delivered to the consumer |
| `DroppedNotifications` | the number of notifications that the full channel of the notifications did not take |
| `ScanCompensations` | the number of capture rounds that returned transactions that no notification announced |
| `CommitFrames` | the number of frames of the current generation of the log that are committed: the boundary of the last complete transaction |
| `WALFrames` | the number of frames SQLite reports for the log |

The counters tell how the commits of the primary database reached the
consumer. A notification that the full channel did not take costs a wake-up:
the periodic scan finds the commit anyway, and the round that found it is
counted as a scan compensation.

`CommitFrames` and `WALFrames` answer the same question from two sides.
`CommitFrames` is the boundary the driver read out of the bytes of the log
while it captured, `WALFrames` is the number of frames SQLite reports for
the log at the moment the statistics are taken. The two agree unless the log
moved between the two reads. The frames of a transaction that did not commit
are in neither of them.

## Not supported

The driver relies on being the only one that touches the primary database.
The following are outside of the contract. They are not intercepted, they
are not detected in time and they break the continuity of the increments.

### A connection of your own

A connection to the primary database that the driver did not open is not
managed. It has no WAL hook, it holds no invariant and the driver does not
know about it.

**Consequence**: the driver cannot tell that the database was checkpointed
by that connection. The driver notices when the number of frames SQLite
wrote back grows without the driver asking for it and stops with an error
that names `nBackfill`. The increments that were not acknowledged are lost.

### Another process on the same database file

Another process that writes to the same database file commits transactions
the driver does not see in its WAL hook. The periodic scan finds them, but
the driver cannot tell them apart from the transactions of the own process.

**Consequence**: the capture is not complete. A backup that is built from the
increments of the driver alone misses the transactions of the other process.

### A checkpoint from the outside

A checkpoint of the outside writes frames of the WAL back into the database
file that the driver has not acknowledged.

**Consequence**: the same as with a connection of your own. The driver
reports the condition and stops.

The driver stops for good when it finds an unmanaged checkpoint. The frames
that were written back without being acknowledged may be gone from the WAL,
so the increments that follow are not trustworthy any more and the consumer
needs a new snapshot.

### Changing the settings of a managed connection

`PRAGMA wal_autocheckpoint` on a connection of the driver replaces the WAL
hook of that connection with the automatic checkpoint of SQLite. The driver
reads the pragma back on its control connection and reports the condition.

**Consequence**: the automatic checkpoint writes frames back that the
consumer has not acknowledged. The driver puts the hook back, but the frames
that were written back in the meantime are gone from the WAL.

## Platform and versions

| | |
|---|---|
| Platform | Linux/amd64 |
| SQLite | 3.54.1, the version `mattn/go-sqlite3` v1.14.53 carries |
| WAL format | 3007000 |
| Go | 1.24 |

The driver builds its own copy of SQLite through the `mattn/go-sqlite3` fork
that carries the WAL hook. The import path stays `github.com/mattn/go-sqlite3`
and `go.mod` replaces it with the fork, so that a single SQLite ends up in
the binary.

The WAL format version is checked when the WAL header is read. A database
that uses another WAL format is rejected with an error.

## Benchmarks

### Method

The benchmarks run against a database on a tmpfs with the default page size
of 4096 bytes. Every transaction is a single `INSERT` into a table with one
text column.

```
go test -bench . -benchtime 3s -run '^$' ./driver/
```

Measured on Linux/amd64 with an Intel Core i7-3537U at 2.00 GHz, Go 1.27.0
and SQLite 3.54.1. The machine was busy while the numbers were taken, so
they are an upper bound.

### Results

| Benchmark | ns/op | |
|---|---|---|
| `BenchmarkWalHook` | 13.4 | the WAL hook of a commit |
| `BenchmarkCommit` | 28821 | 34697 tx/s, 4140 WAL bytes per transaction |
| `BenchmarkCaptureThroughput` | 116594 | 8577 tx/s |
| `BenchmarkScanEmpty` | 49431 | a capture round that finds nothing |
| `BenchmarkCaptureLatency` | 135498 | 135251 ns from the commit to the capture |
| `BenchmarkCheckpoint` | 135775 | |
| `BenchmarkSnapshot` | 3412584 | |

The WAL hook costs 13 nanoseconds and does not block the business write. It
only hands a notification to the consumer and returns. The hook does not
parse the WAL, it does not touch the disk and it does not wait for
anything.

A transaction that writes one page makes the WAL grow by 4140 bytes, one
frame header and one page. The WAL of a database that writes this many
transactions grows by 143 MB per second at the rate of the commit benchmark
and by 35 MB per second at the rate of the capture benchmark.

A capture round that finds nothing costs 49 microseconds. That is the price
the periodic scan pays for the promise that the driver finds a commit even
when no notification arrives.

A checkpoint writes the whole WAL back into the database file and costs 136
microseconds for a single frame.

### The delay of an increment

The delay between a commit and the backup that holds it has three parts:

| Part | Where it comes from | Measured |
|---|---|---|
| Capture delay | the notification of the WAL hook until `Scan` hands the transaction out | 84 us |
| Merge delay | the time the consumer waits to merge transactions into a segment | the linger of the consumer that fills the spool |
| Write delay | the time the spool needs to put the segment on the disk | the fsync of the spool |

The capture delay is the part the driver is responsible for. It is 84
microseconds at the rate of the benchmark, of which the commit itself is 29
microseconds.

The merge delay belongs to the consumer that fills the spool, the write delay
belongs to the spool. The capture delay is the input the linger of the
consumer is chosen against: a linger that is shorter than the capture delay
buys nothing, because the transaction is not captured yet when the segment is
closed.

The capture throughput of 9502 transactions per second and the WAL growth of
4140 bytes per transaction give the size of the WAL between two
checkpoints: the WAL grows by 35 MB per second, so the default threshold of
64 MiB is reached after about two seconds.

The thresholds of the driver follow from these numbers:

| Setting | Default | Why |
|---|---|---|
| `ScanInterval` | 1s | the capture delay of 66 us is far below the interval, the scan is not the bottleneck |
| `ReclaimBytes` | 64 MiB | at 35 MB per second the WAL is written back after about two seconds |
| `ReclaimInterval` | 5m | the lower bound for the space that is given back |
| `ReclaimTimeout` | 5s | the time the consumer has to acknowledge the frames that are held back |
