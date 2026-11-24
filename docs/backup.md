# Backup

The backup is the backup end of a relay. It holds a copy of the primary
database of the source: a plain SQLite file that the business may read while
the relay catches up with the increments of the chain.

The backup owns the file. While it is in sync the relay writes the file to
itself: it writes the pages of the transactions of the chain into it and it
makes it durable at the boundary of a segment. The relay holds no connection
to the file while it is in sync, it never opens it with SQLite and it leaves
no `-wal` and no `-shm` file behind.

The backup has no baseline when it starts: it asks the source for a snapshot
of the primary database, it installs it and the increments of the chain that
follow the baseline of the snapshot bring it up to date.

## The directory layout

```text
<Root>/<DatabaseID>/
  backup.db       # the backup database
  backup.db.new   # the snapshot that is on its way
  state.json      # the state of the backup
```

The directory of a database is `<Root>/<DatabaseID>`. The name of the database
is also its identity: the identifier of the database is the first sixteen
bytes of the SHA-256 of its name.

`backup.db` is the backup database. It is a plain SQLite file, there is no
second format and no auxiliary file beside `backup.db.new` and `state.json`.

`backup.db.new` holds the snapshot that is being received. It is not a
database until the installation is finished.

## The state file

The state file is the memory of the backup. It is written atomically: the
content goes into a temporary file in the same directory, the file is synced,
it is renamed over the target and the directory is synced. The temporary file
is removed when the write is done, it holds the state of the backup for the
time of the write.

```json
{
  "version": 1,
  "phase": "ready",
  "databaseID": "app.db",
  "streamID": 1,
  "epoch": 3,
  "appliedSeq": 42,
  "pageSize": 4096,
  "dbPath": "/var/lib/relay/app.db/backup.db",
  "snapshotBytes": 1048576,
  "snapshotSHA256": "…"
}
```

| Field | Meaning |
|---|---|
| `phase` | `empty`, `installing` or `ready` |
| `databaseID` | the name of the database the backup holds |
| `streamID` | the number of the stream the backup belongs to |
| `epoch` | the generation of the chain the backup is part of |
| `appliedSeq` | the sequence number the backup applied and persisted |
| `pageSize` | the size of the pages of the database |
| `dbPath` | the path of the file of the backup database |
| `snapshotBytes` | the size of the snapshot of the baseline |
| `snapshotSHA256` | the checksum of the snapshot of the baseline |

The epoch of the backup and the applied sequence belong together: they are
the two halves of the position of the backup in the chain of the source. The
source refuses a backup that belongs to another generation of the chain, and
the backup asks for a new snapshot when the chain of the source is not
continuous any more.

The phase tells what the backup holds:

- `empty` means that the backup has no baseline. It holds no database and it
  asks for a snapshot when it connects.
- `installing` means that a snapshot was on its way. The database of the
  backup is the old one until the installation is finished.
- `ready` means that the backup has a baseline and that it applies the
  increments of the chain to it.

## The snapshot and the baseline

The backup asks for a snapshot in the hello of its connection when it has no
baseline. The source answers with the manifest of the snapshot and with the
bytes of its file. The backup writes the bytes into `backup.db.new`, it
computes the SHA-256 of the file while it writes and it compares the size and
the checksum with the manifest when the last byte arrived. A snapshot that
does not match its manifest is discarded: the backup keeps the database it
had and the round fails.

The installation of a snapshot happens in two steps, so that a backup that
was killed in the middle of it holds either the old database or the new one:

1. The state file is written with the phase `installing`, the epoch, the
   baseline and the checksum of the snapshot.
2. The file of the snapshot replaces the backup database with a rename and
   the directory is synced.
3. The state file is written with the phase `ready` and the baseline as the
   applied sequence.

When the backup starts and the state says `installing`, it finishes the
installation: the temporary file is renamed over the backup database when it
is still there, and the checksum of the backup database is compared with the
state when it is gone. A backup database that does not match the state is
refused with `ErrSnapshotRequired`: the installation did not finish and only
a new snapshot brings the backup back.

The backup may check the integrity of the snapshot before it installs it. The
check runs `PRAGMA integrity_check` on the temporary file. It is off by
default.

The baseline of the snapshot is the sequence number of the last transaction of
the chain the snapshot holds. The increments that follow it are applied to
the backup, so that the chain continues without a gap and without a repeat.

## The increments

The backup applies the transactions of the chain while it is in sync. It does
not open the backup database with SQLite, it writes the pages of the
transactions into the file:

1. Every page of the transaction is written at its offset, `(pgno-1)*pageSize`.
2. The file is cut to the size the database has after the commit,
   `commitPages*pageSize`.
3. The header of page 1 is rewritten: the write version and the read version
   become 1 and the size of the database in the header becomes the number of
   pages the file holds.

The third step turns the backup into a plain database with a rollback
journal. The pages of the primary database carry the flags of the write-ahead
log, and a database with those flags leaves a `-wal` and a `-shm` file behind
and needs a SQLite that knows the path without them. The size of the database
in the header has to match the size of the file, otherwise a reader of the
backup sees a database that does not exist.

A transaction whose pages do not fit the page size of the backup is refused:
the page size of the backup comes from the snapshot and from the header of
the database, the segments of the chain have to agree with it.

## The durability

The backup persists the applied sequence at the boundary of a segment, not at
the boundary of a transaction. The file of the backup database is synced, the
state file is written with the new applied sequence and only then the
segment is acknowledged. The acknowledgement of the source means that the
transactions up to the sequence number it names are applied and that the
sequence number is persisted.

A backup that was killed after it wrote the pages of a segment and before it
wrote the state holds the pages of the segment but the state still names the
sequence number of the previous segment. The segment is applied again when
the backup starts. The replay is idempotent: the same pages are written into
the same file and the content does not change. No transaction is skipped.

## The pause and the resume

The backup can be paused. A paused backup does not touch its database, so
that a business program may read it.

`Pause` waits for the source to stop pushing the increments, it waits for the
segment that is applied to end, it makes the applied sequence durable and it
closes the file of the backup database. When the call returns, the backup
database is not written any more: the increments that the source captures in
the meantime pile up in the spool of the source.

`Resume` opens the file of the backup database again and tells the source to
continue after the sequence number the backup applied and persisted.

The rules of the service mode are:

- **Pause before the business opens the backup database.** The relay writes
  the file of the backup database while it is in sync and a reader that is
  open while the relay writes sees a torn page.
- **Close every connection of the business before the resume.** The relay
  writes the file of the backup database again after the resume. The relay
  cannot tell whether a connection of the business is open, it only sees the
  `-journal`, the `-wal` and the `-shm` file of the backup database. A
  leftover journal makes the resume fail with an error that names it.
- **The backup database is read only.** The business reads it, it does not
  write it. A business that needs to write takes a copy of the file and works
  on the copy: a file that was written by the business may not be resumed
  into the chain of the source, the pages of the backup and the pages of the
  chain would not agree any more.

The connection to the source stays open while the backup is paused. The source
sees that the backup is paused, it keeps the heartbeat running and it does
not push the increments.

## The chain that breaks

The chain of the source is not continuous any more when the source resets its
spool, when the source cannot provide the sequence number the backup needs
or when a segment does not hold what it says. The source answers with a
`ResyncRequired` and the session ends.

The backup asks for a new snapshot in the hello of its next connection, the
source produces one and the backup installs it over the old database. The
backup adopts the epoch of the new snapshot. The old database is not kept:
the backup holds the state of the chain it is part of.

The backup does not wait for a human: the source is at the other end of the
chain and a new baseline is one automatic step.

## The statistics

`Stats` reports the state of the backup: the applied sequence, the epoch, the
time of the last acknowledgement and the lag behind the source, the error of
the last failure, the time and the size of the last snapshot that was
installed and the number of the times the backup asked for a snapshot with
the reason of the last request.

## Not supported

### A business that writes the backup database

The backup database is a copy of the primary database of the source. A write
of the business is not part of the chain and it is lost with the next
snapshot. A business that writes takes a copy of the file.

### A backup that serves while it is in sync

The relay writes the file of the backup database while the backup is in sync.
A business that reads the file at the same time sees a torn page. The backup
has to be paused before the business reads it.

### More than one backup per database

The protocol serves one backup at a time. The distribution of the segments of
the chain among several backups is not part of the protocol.

### The versions of the backup database

The backup does not keep the old versions of the backup database. A snapshot
replaces the database of the backup, the old one is gone.

### The data of the segments

The backup writes the pages of the segments into the file of the backup
database. It does not read the pages of the primary database and it does not
write to the primary database.
