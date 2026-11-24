# Spool

The spool is the durable queue between the driver that captures the increments
of a primary database and the transport that carries them to a backup. It
writes the increments into immutable segment files, publishes the segments
that passed the persistence barrier and holds the durable capture
acknowledgement, so that the driver may write the frames of the write-ahead
log back into the database.

The spool writes nothing into the primary database. It has no table of its
own, it adds no write to the business path and it does not wait for the
network or for the remote side.

The spool is passive. It has no goroutine of its own, it times nothing and it
does not talk to the network. The caller drives it: it appends the
transactions it captured, it closes the segment when it wants the increments
to be merged and it runs the barrier when it wants them to be durable.

## The directory layout

```text
<Root>/<DatabaseID>/
  segments/%016d-%016d.seg   # the published segments (seqStart, seqEnd)
  tmp/%016d.tmp              # the segments that wait for the barrier
  state/stream.json          # the identity of the database, the epoch, the next seq
  state/capture.json         # the durable capture acknowledgement: seq and WAL position
  peers/<peer-id>.ack        # the acknowledgement of the backup: seq and epoch
```

The directory of a database is `<Root>/<DatabaseID>`. The name of the
database is also its identity: the identifier of the database is the first
sixteen bytes of the SHA-256 of its name, and it is written into the header of
every segment. A directory whose state file holds another identifier belongs
to another database and `Open` fails with `ErrDatabaseMismatch`.

The files of `tmp/` are the segments that were written but not published.
They are not trusted: `Open` removes them, the data of them is captured from
the write-ahead log again.

The files of `segments/` are published. A published segment is never changed.

## The segment format

A segment holds a range of the transactions of one database:

```text
[0, 64)               header
[64, 64+payload)      the transaction records
[64+payload, +64)     trailer
```

Every field is big endian and every checksum is a CRC32C (Castagnoli). The
checksum of the header covers the magic, the version and the flags, the
checksum of the trailer covers the whole trailer and the checksum of the
payload covers the records. The fields of the header that the checksum does
not reach are covered by the chain of the segments: the identity of the
database, the epoch and the sequence numbers are compared with the
neighbours when the segments are read. A transaction record holds the
sequence number of the transaction, the size of the database in pages after
the commit and the pages the transaction changed. The trailer closes the
segment: it names the last sequence number, the number of the transactions,
the number of the pages, the size of the payload, the checksum of the payload
and the position in the write-ahead log the last transaction was committed at.

A segment covers the sequence numbers `[seqStart, seqEnd]`. The size of the
database after the commit is kept per transaction, a segment may hold a
transaction that shrinks the database and one that grows it again.

The package `segment` reads and writes the format. The transport and the
backup read the segments with it.

## The modes

The mode of the spool is the persistence barrier. The names follow the ones of
the synchronous pragma of SQLite, but the unit of the barrier is a segment or
a batch of segments and not a transaction of the primary database. The mode
of the spool does not change the `PRAGMA synchronous` of the primary
database.

| Mode | The barrier is | The data that a crash may lose |
|---|---|---|
| `OFF` | the write of the operating system | the segments at the end of the queue |
| `NORMAL` | a sync of the files of a batch and of the directory that holds them | the segments that were not published yet |
| `FULL` | a sync of the file of the segment and of the directory that holds it | the segment that is written |

`OFF` is the default mode. It is the mode that writes the fastest.

Under `OFF` the capture acknowledgement only means that the operating system
accepted the write. It is not a promise that survives a power failure or a
crash of the operating system: the segments at the end of the queue may be
lost. When the driver already wrote the frames of the write-ahead log back
because of such an acknowledgement, the data of the lost segments is gone and
the only way back is a new snapshot. `OFF` must not be read as a mode that
survives a power failure.

Under `NORMAL` the segments are written and closed first, and the barrier
publishes them as one batch: every file of the batch is synced, the files are
renamed into `segments/` and the directory is synced once. The capture
acknowledgement only moves after the whole batch passed the barrier. A
segment that was written but not published is not visible to the transport
and may not be acknowledged by the backup.

The consumer runs the barrier when it wants the increments to be durable.
`Pending` reports when the thresholds of the configuration say that the
barrier is due: the interval and the number of bytes since the last
barrier. The consumer polls it and calls `Sync` when it is due.

Under `FULL` every segment passes the barrier on its own: the file is synced,
it is renamed into `segments/` and the directory is synced. The capture
acknowledgement moves after both.

## The handover with the driver

The driver hands out the transactions it captured. The consumer turns them
into the transactions of the spool and appends them:

```go
batch, err := d.Scan(ctx)
for _, change := range batch.Changes {
    if err := spool.Append(ctx, txOf(change)); err != nil {
        return err
    }
}
if err := spool.Flush(ctx); err != nil {
    return err
}
ack, err := spool.Durable()
if err != nil {
    return err
}
if err := d.CaptureAck(ctx, positionOf(ack.Pos)); err != nil {
    return err
}
```

`Append` writes the transaction into the segment that is open. The spool
hands out the sequence number, the sequence number of the driver is not used:
the sequence numbers of the spool survive a restart and the ones of the
driver do not.

`Flush` closes the segment. Under `OFF` and `FULL` the segment is published
and the durable capture acknowledgement moves to its last transaction. Under
`NORMAL` the segment waits in `tmp/` until `Sync` publishes it.

A segment that could not be published stays in the queue of the spool: the
next flush or the next barrier publishes it. The sequence numbers of the
chain stay continuous, so a segment that never reached the disk does not
leave a hole behind it.

`Durable` returns the durable capture acknowledgement: the sequence number of
the last transaction that is durable and the position in the write-ahead log
it was committed at. The driver may write the frames of the log up to that
position back into the database. The acknowledgement says nothing about the
remote side: it says that the local spool holds the increments.

The position of the acknowledgement always points at the commit boundary of a
whole transaction, because it is taken from the trailer of the last published
segment.

The spool does not import the driver and the driver does not import the
spool. The consumer holds both together.

## The sequence numbers, the epoch and the acknowledgements

Every transaction gets a sequence number. The numbers increase by one and
they survive a restart: the next number is the one after the last published
segment. The numbers are the unit of the acknowledgements and the unit the
garbage collection works in.

The epoch names the generation of the chain. A reset of the spool raises the
epoch and starts the sequence numbers over, so that the segments of the old
chain and the ones of the new chain are never mixed.

There are two acknowledgements:

- The **capture acknowledgement** is the durable state of the source side. It
  says that the increments up to a sequence number are on the disk and that
  the driver may write the frames of the log back. It is derived from the
  segments and never from a file.
- The **backup acknowledgement** says that the backup applied the segments up
  to a sequence number and persisted that number. It is the only thing that
  lets the source side remove a segment. It is written to
  `peers/<peer-id>.ack`.

A backup acknowledgement that goes back costs a repeated transmission and
nothing else. The source side never removes a segment the backup did not
acknowledge.

## The recovery

`Open` reads the segments and rebuilds the state of the spool from them. The
segments are the truth, the files in `state/` are accelerators: they are
rebuilt from the segments and a file that is ahead or behind them changes
nothing.

The scan does the following:

1. It creates the directories of the spool when they do not exist.
2. It checks the identifier of the database in `state/stream.json` and fails
   with `ErrDatabaseMismatch` when the directory belongs to another database.
3. It removes the files of `tmp/`. They were never published.
4. It reads every file of `segments/`, checks the magic, the version, the
   checksums, the size and the sequence numbers of each of them and sorts
   them by the sequence number of their first transaction.
5. It checks that the chain is continuous: the first transaction of a
   segment has to follow the last one of its predecessor, and all the
   segments have to belong to the same epoch.
6. It takes the durable capture acknowledgement from the last segment and the
   next sequence number from the sequence number after it.
7. It rewrites the files of `state/`.

A segment at the end of the chain that does not hold what it says is dropped:
the data of it is captured from the write-ahead log again and the durable
capture acknowledgement moves back to the segment before it. The sequence
numbers of the dropped segment are used again, so that the transaction that
is captured again carries the number it had.

A segment in the middle of the chain that is missing or that does not hold
what it says is a broken chain. `Open` fails with `ErrChainBroken`. The data
of the hole was durable once and the write-ahead log may have been written
back already, so it cannot be recovered: only a reset of the spool and a new
snapshot bring the queue back. The spool that comes with the error may be
used to look at the state and to reset the chain.

## The garbage collection

`GC` removes the segments the backup acknowledged:

- the sequence number of the last transaction of the segment is at or below
  the backup acknowledgement, and
- the segment is at least `Retention` old. The age of a segment is counted
  from the creation time in its header.

The capture acknowledgement never removes a segment. The data of a segment
the backup did not apply is still needed.

A backup that is paused or that lost the network stops the acknowledgement.
The segments that are not acknowledged stay on the disk and the spool runs
into its limit. That is the price of not losing an increment.

`Reset` empties the spool: it removes the segments and the temporary files,
raises the epoch, starts the sequence numbers over and clears the
acknowledgements. The chain that was reset is gone, the backup needs a new
snapshot.

`Reset` is also the way out of a broken chain: the spool that comes with the
error of `Open` may be reset, the epoch of the broken chain is raised above
it and the spool is usable again.

A collection that removed every segment takes the durable acknowledgement with
it: the segments are the truth and there is none of them left, so the
acknowledgement starts at zero again after a restart. The increments of the
segments that were collected are applied by the backup and are not captured
again.

## The capacity and the monitoring

`MaxBytes` is the limit of the spool. An append that would push the spool
past the limit fails with `ErrSpoolFull` and the transaction is not written.
The durable capture acknowledgement does not move, so the write-ahead log of
the primary keeps the increments and the driver holds it back. A spool
without a limit does not stop the primary on its own: a hidden limit would
stop the write-ahead log of the primary without anybody noticing.

`WarnBytes` is the size the spool warns about. The warning is a bit in
`Stats`, the writes go on.

`SegmentMaxBytes` is the size of a segment. An append that does not fit into
the segment that is open fails with `ErrSegmentFull` and the transaction is
not written. The caller closes the segment and retries the transaction, which
then lands in the next segment. A transaction that is bigger than the limit
on its own is written into a segment of its own, so that it cannot block the
queue.

`Stats` reports the mode, the epoch, the number of the published segments,
the number of bytes on the disk, the durable sequence number, the backup
acknowledgement, the number and the size of the segments the backup did not
acknowledge, the age of the oldest of them, the warning, the bytes that are
left on the file system and whether the chain is broken.

`Pending` reports the segments that wait for the barrier, their size, the
size of the segment that is open and the age of the oldest one. It also
reports whether the thresholds of the configuration say that the barrier is
due: the interval and the number of bytes since the last barrier. The
consumer decides when to run the barrier, the spool only tells it when the
thresholds are reached.

The high water mark of the write-ahead log is not part of the spool. It comes
from the statistics of the driver.

## Not supported

### More than one peer

The directory of the spool holds the acknowledgement of one peer. The
acknowledgements of several peers are not merged, and the garbage collection
does not wait for all of them.

### A segment that is changed after it was published

A published segment is immutable. Changing one breaks the chain and the
spool reports `ErrChainBroken` when it reads the segment.

### A spool that is used after it was closed

`Close` releases the file of the segment that is open and makes the spool
unusable. The transactions that were written into the segment that is open
are not published, the acknowledgement did not move and the write-ahead log
of the primary still holds them.

### A directory that another process writes

The spool owns its directory. Another process that writes segments into it
breaks the chain, and the segments that are written there are not covered by
any barrier.

### The data of the segments

The spool holds the pages of the increments. It does not hold the database
file, it does not apply anything and it does not know what the pages mean.
The backup applies the segments itself.

## Benchmarks

### Method

```
go test -bench . -run '^$' ./spool/
```

The benchmarks write into a temporary directory of the machine. The
transactions hold four pages of 512 bytes, every segment holds one
transaction. The numbers of the sync calls come from the seam of the spool
that the tests replace, the production code does not know about it.

Measured on darwin/amd64 with an Intel Core i5-1038NG7 at 2.00 GHz and Go
1.24. The file system of the machine is slow, the sync of a file takes
milliseconds.

### Results

| Benchmark | ns/op | | |
|---|---|---|---|
| `BenchmarkAppend` | 177848 | 11.70 MB/s | 2080 disk-B/op, 0.0076 fsync/op |
| `BenchmarkPublish/OFF` | 1540358 | | 0 fsync/op |
| `BenchmarkPublish/NORMAL` | 78971257 | | 4 fsync/op |
| `BenchmarkPublish/FULL` | 78170921 | | 4 fsync/op |
| `BenchmarkRecover` | 7396659 | 100 segments | the scan of the segments directory |

The modes `NORMAL` and `FULL` pay for their barrier: four sync calls per
published segment, two for the file and the directory of the segment and two
for the state files. The mode `OFF` never syncs and pays for the rename
instead.

The scan of the segments directory reads and checks every segment. The cost
grows with the number of the segments, not with their size.
