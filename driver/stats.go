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
	// Checkpointed is the number of frames the checkpoint wrote back into
	// the database file.
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
	// WALFrames is the number of frames the WAL holds.
	WALFrames int
	// WALBytes is the size of the WAL file.
	WALBytes int64
	// WALHighWater is the largest WAL file size the driver has seen.
	WALHighWater int64
	// Checkpoint describes the last checkpoint.
	Checkpoint CheckpointStats
	// Reclaim describes the last reclaim.
	Reclaim ReclaimStats
}
