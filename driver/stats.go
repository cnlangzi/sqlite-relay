package driver

import "time"

// Notification tells the consumer that the WAL advanced.
//
// The notification is a wake-up hint. The frames it reports are the number of
// frames SQLite saw in the WAL and may be coalesced or dropped; the boundary
// of an increment is always decided by the WAL bytes.
type Notification struct {
	// DBName is the name of the database that was written. It is "main" for
	// the primary database.
	DBName string
	// Frames is the number of frames the WAL holds.
	Frames int
}

// CheckpointStats describes the last checkpoint the driver ran.
type CheckpointStats struct {
	// Mode is the checkpoint mode, "PASSIVE" or "TRUNCATE".
	Mode string
	// Busy reports whether the checkpoint could not run to completion.
	Busy bool
	// LogFrames is the number of frames the WAL holds.
	LogFrames int
	// Checkpointed is the number of frames SQLite has written back into
	// the database file, counted from the start of the WAL generation. It
	// is the state of the backfill, not the number of frames the last
	// checkpoint wrote.
	Checkpointed int
	// At is the time the checkpoint ran.
	At time.Time
	// LastError holds the message of the last failed checkpoint.
	LastError string
}

// ReclaimStats describes the last attempt to give the WAL space back.
type ReclaimStats struct {
	// At is the time of the attempt.
	At time.Time
	// Done reports whether the WAL space was reclaimed.
	Done bool
	// Reason explains why the reclaim did not happen.
	Reason string
	// Stall is the time the business writes were blocked.
	Stall time.Duration
	// Frames is the number of frames that were written back.
	Frames int
	// Truncated reports whether the WAL file was shrunk.
	Truncated bool
}

// Stats is a snapshot of the state of the driver.
type Stats struct {
	// Generation is the WAL generation the driver is reading.
	Generation Position
	// CaptureCursor is the position the driver has captured up to.
	CaptureCursor Position
	// AckPosition is the position the caller has acknowledged.
	AckPosition Position
	// BackfillFrames is the number of frames SQLite has written back into
	// the database file. A negative value means that SQLite could not
	// report the state of the WAL.
	BackfillFrames int
	// WALFrames is the number of frames SQLite reports for the WAL: the
	// frames that are committed in the current generation.
	WALFrames int
	// CommitFrames is the number of frames of the current WAL generation
	// that are committed: the boundary of the last complete transaction in
	// the log. The driver reads the boundary out of the WAL bytes while it
	// captures, WALFrames is the number of frames SQLite reports for the
	// same generation.
	CommitFrames uint64
	// WALBytes is the size of the WAL file.
	WALBytes int64
	// WALHighWater is the largest WAL file size the driver has seen.
	WALHighWater int64
	// Notifications is the number of commit notifications the WAL hook
	// delivered to the consumer.
	Notifications uint64
	// DroppedNotifications is the number of notifications that the full
	// channel of the notifications did not take.
	DroppedNotifications uint64
	// ScanCompensations is the number of capture rounds that returned
	// transactions that no notification announced. The periodic scan of the
	// driver found those transactions on its own.
	ScanCompensations uint64
	// Checkpoint describes the last checkpoint.
	Checkpoint CheckpointStats
	// Reclaim describes the last reclaim.
	Reclaim ReclaimStats
	// HookPanics is the number of panics that happened inside the WAL
	// hook. A panic is caught by the driver so that the commit it belongs
	// to still succeeds.
	HookPanics int
	// LastError is the last error the driver ran into.
	LastError string
}
