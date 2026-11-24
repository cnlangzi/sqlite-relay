package driver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// readPin keeps a read transaction open on a dedicated connection.
//
// The read mark the pin occupies truncates the frame a checkpoint may write
// back into the database file. The mark is the number of frames the WAL held
// when the pin took its snapshot. The driver reads the frame count once more
// after the snapshot, which makes the recorded mark an upper bound of the
// mark the pin really took.
type readPin struct {
	tx   *sql.Tx
	mark uint64
}

// acquirePin starts a read transaction that holds the checkpoint back.
func (d *Driver) acquirePin(ctx context.Context) (*readPin, error) {
	tx, err := d.pinConn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("driver: start read pin: %w", err)
	}
	// The read has to touch the database, otherwise the connection does not
	// take a read mark at all.
	var n int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema").Scan(&n); err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("driver: take read mark: %w", err)
	}
	log, _, err := d.walFrames(ctx)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	if log < 0 {
		tx.Rollback()
		return nil, fmt.Errorf("driver: SQLite could not report the frame count of the WAL")
	}
	return &readPin{tx: tx, mark: uint64(log)}, nil
}

// release gives the read mark back.
func (p *readPin) release() {
	if p == nil {
		return
	}
	p.tx.Rollback()
}

// runCheckpoint runs a passive checkpoint.
//
// The backfill of a checkpoint may never pass the acknowledged position. As
// long as that holds, SQLite cannot reset the WAL and overwrite frames the
// consumer has not persisted yet.
//
// The checkpoint is bounded by the read mark of the pin. The pin is only
// released once the captured increments have been acknowledged up to its
// mark, so the backfill stays behind the acknowledged position.
func (d *Driver) runCheckpoint(ctx context.Context) (CheckpointStats, error) {
	d.ckptMu.Lock()
	defer d.ckptMu.Unlock()
	if d.ckptPaused {
		return d.checkpointStats(), nil
	}

	if d.pin == nil {
		pin, err := d.acquirePin(ctx)
		if err != nil {
			return d.checkpointStats(), err
		}
		d.pin = pin
	}
	if d.ackFrames() < d.pin.mark {
		// The consumer has not acknowledged the frames the pin holds back
		// yet. Writing them back would take the WAL ahead of the consumer.
		return d.checkpointStats(), nil
	}

	st, err := d.checkpoint(ctx, "PASSIVE")
	if err != nil {
		d.setCheckpointStats(st)
		return st, err
	}
	// The mark of the pin has been consumed. Releasing it lets the next
	// round take a pin with a higher mark, so that the backfill keeps up
	// with the consumer.
	d.releasePin()
	d.setCheckpointStats(st)
	return st, nil
}

// checkpoint runs a checkpoint of the given mode on the control connection.
//
// The control connection must not be inside a transaction, otherwise the
// pragma cannot run.
func (d *Driver) checkpoint(ctx context.Context, mode string) (CheckpointStats, error) {
	// The lock keeps the observation of the backfill count and the update
	// of the fence apart. Without it a reader could see the backfill count
	// of the running checkpoint and take it for a checkpoint of an
	// unmanaged connection.
	d.fenceMu.Lock()
	defer d.fenceMu.Unlock()

	var busy, log, ckpt int
	err := d.ctlConn.QueryRowContext(ctx, "PRAGMA main.wal_checkpoint("+mode+")").Scan(&busy, &log, &ckpt)
	st := CheckpointStats{
		Mode:         mode,
		Busy:         busy != 0,
		LogFrames:    log,
		Checkpointed: ckpt,
		At:           time.Now(),
	}
	if err != nil {
		st.LastError = err.Error()
		return st, fmt.Errorf("driver: wal_checkpoint(%s): %w", mode, err)
	}
	// The frames the checkpoint wrote back are the frames the driver is
	// responsible for. Anything beyond that came from a connection the
	// driver does not know about.
	d.noteBackfill(ckpt)
	return st, nil
}

// walFrames reports the number of frames the WAL holds and the number of
// frames SQLite has written back into the database file.
//
// The pragma does not change anything, it only reads the state of the
// wal-index. It reports a negative count when it could not read that state,
// which happens while another connection holds the lock the pragma needs.
func (d *Driver) walFrames(ctx context.Context) (log, ckpt int, err error) {
	d.fenceMu.Lock()
	defer d.fenceMu.Unlock()

	var busy int
	err = d.ctlConn.QueryRowContext(ctx, "PRAGMA main.wal_checkpoint(NOOP)").Scan(&busy, &log, &ckpt)
	if err != nil {
		return 0, 0, fmt.Errorf("driver: read wal state: %w", err)
	}
	// There is nothing to compare when the pragma could not read the state.
	if ckpt >= 0 {
		if err := d.checkBackfill(ckpt); err != nil {
			return log, ckpt, err
		}
	}
	return log, ckpt, nil
}

// walFrameCount returns the number of frames the WAL holds.
func (d *Driver) walFrameCount(ctx context.Context) (int, error) {
	log, _, err := d.walFrames(ctx)
	if err != nil {
		return 0, err
	}
	if log < 0 {
		return 0, fmt.Errorf("driver: SQLite could not report the frame count of the WAL")
	}
	return log, nil
}

// checkBackfill verifies that nobody checkpointed the database behind the
// back of the driver.
//
// The driver is the only component that may run a checkpoint. If the number
// of frames SQLite wrote back into the database file grows without the
// driver asking for it, some other connection checkpointed the database and
// the captured increments can no longer be trusted.
func (d *Driver) checkBackfill(observed int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.fenceSeen {
		// The first observation is the baseline. A database that was
		// checkpointed before the driver took it over already holds
		// written back frames.
		d.fenceSeen = true
		d.backfillFence = observed
		return nil
	}
	if observed < d.backfillFence {
		// The WAL was reset, the backfill count starts over.
		d.backfillFence = observed
		return nil
	}
	if observed > d.backfillFence {
		err := fmt.Errorf("driver: nBackfill advanced to %d without the driver running a checkpoint, an unmanaged connection checkpointed the database", observed)
		// The captured increments can no longer be trusted: the frames
		// behind the acknowledged position may have been written back.
		d.fatalErr = err
		return err
	}
	return nil
}

// noteBackfill records the frames the driver itself wrote back.
func (d *Driver) noteBackfill(frames int) {
	if frames < 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.fenceSeen || frames > d.backfillFence {
		d.fenceSeen = true
		d.backfillFence = frames
	}
}

// reclaimResult describes what a reclaim attempt did.
type reclaimResult struct {
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

// reclaim gives the WAL space back to SQLite.
//
// The sequence is:
//
//  1. Wait until the consumer has acknowledged every frame of the WAL. This
//     happens without the write lock, so the business writers are not held
//     back while the consumer catches up.
//  2. Take the write lock, which freezes the frame count and blocks the
//     business writers.
//  3. Wait until the consumer has acknowledged the frames of the frozen
//     WAL.
//  4. Write the whole WAL back into the database file.
//  5. Give the write lock back.
//
// SQLite resets the WAL on the next write, which reuses the space of the
// WAL file. The wait in step 3 is bounded by the busy timeout of the
// connections: a business writer has to wait for the write lock, and it may
// not fail because of the reclaim.
//
// The wait in step 1 is bounded by ReclaimTimeout: if the consumer does not
// catch up, the reclaim is given up and the WAL keeps growing until it does.
func (d *Driver) reclaim(ctx context.Context) (reclaimResult, error) {
	d.ckptMu.Lock()
	defer d.ckptMu.Unlock()
	if d.ckptPaused {
		return reclaimResult{Reason: "the checkpoint manager is paused"}, nil
	}

	start := time.Now()

	// The frames the consumer has to acknowledge before the WAL can be
	// written back. The business writers run while the consumer catches up.
	log, err := d.walFrameCount(ctx)
	if err != nil {
		return reclaimResult{Reason: "the frame count could not be read"}, err
	}
	if err := d.waitForAck(ctx, uint64(log), time.Now().Add(d.cfg.ReclaimTimeout), 10*time.Millisecond); err != nil {
		res := reclaimResult{
			Reason: fmt.Sprintf("the consumer acknowledged %d of %d frames in time", d.ackFrames(), log),
			Stall:  time.Since(start),
		}
		if errors.Is(err, errAckTimeout) {
			return res, nil
		}
		return res, err
	}

	if _, err := d.sealConn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return reclaimResult{Reason: "the write lock could not be taken"}, fmt.Errorf("driver: seal: %w", err)
	}
	sealed := true
	defer func() {
		if sealed {
			d.sealConn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		}
	}()

	// The frame count is frozen now that the write lock is held. A
	// transaction that was committed in between has to be acknowledged as
	// well.
	log, err = d.walFrameCount(ctx)
	if err != nil {
		return reclaimResult{Reason: "the frame count could not be read", Stall: time.Since(start)}, err
	}
	if err := d.waitForAck(ctx, uint64(log), time.Now().Add(d.stallBudget()), time.Millisecond); err != nil {
		res := reclaimResult{
			Reason: fmt.Sprintf("the consumer acknowledged %d of %d frames in time", d.ackFrames(), log),
			Stall:  time.Since(start),
		}
		if errors.Is(err, errAckTimeout) {
			return res, nil
		}
		return res, err
	}

	// The pin holds the checkpoint back, and the pin would keep the frames
	// from being written back that the consumer just acknowledged.
	d.releasePin()

	st, err := d.checkpoint(ctx, "PASSIVE")
	if err != nil {
		return reclaimResult{Reason: "the checkpoint failed", Stall: time.Since(start)}, err
	}

	// Give the write lock back. The next write resets the WAL and reuses
	// its space.
	if _, err := d.sealConn.ExecContext(ctx, "ROLLBACK"); err != nil {
		return reclaimResult{Reason: "the write lock could not be released", Stall: time.Since(start)}, fmt.Errorf("driver: unseal: %w", err)
	}
	sealed = false

	res := reclaimResult{Done: !st.Busy, Stall: time.Since(start), Frames: st.Checkpointed}
	if st.Busy {
		res.Reason = "the checkpoint could not write the whole WAL back"
	}

	if d.cfg.TruncateOnReclaim {
		truncated, err := d.truncate(ctx, uint64(log))
		if err != nil {
			res.Reason = err.Error()
		}
		res.Truncated = truncated
	}
	return res, nil
}

// stallBudget is the time the business writers may be held back by a
// reclaim.
//
// The writers wait for the write lock with their busy timeout. A reclaim
// that holds the lock longer than that makes a business write fail instead
// of making it wait, so the stall stays below the busy timeout.
func (d *Driver) stallBudget() time.Duration {
	budget := d.cfg.ReclaimTimeout
	if half := d.cfg.BusyTimeout / 2; half > 0 && half < budget {
		budget = half
	}
	return budget
}

// waitForAck waits until the consumer has acknowledged the given number of
// frames.
//
// The wait ends with an error when the deadline passes or the context is
// done.
func (d *Driver) waitForAck(ctx context.Context, frames uint64, deadline time.Time, interval time.Duration) error {
	for d.ackFrames() < frames {
		if time.Now().After(deadline) {
			return errAckTimeout
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
	return nil
}

// errAckTimeout reports that the consumer did not acknowledge the frames in
// time.
var errAckTimeout = errors.New("driver: the capture position was not acknowledged in time")

// truncate shrinks the WAL file.
//
// Truncating the WAL opens a new generation, so it may only happen when every
// frame of the WAL has been acknowledged. The read pin has to be released
// before, because truncating waits for the readers of the WAL.
//
// SQLite does not let a truncate run while the write lock is held, so the
// write lock of the reclaim is given back before. A transaction that is
// committed in between is written back without being acknowledged. The
// frame count is read right before the truncate to make that window as
// small as it can be.
func (d *Driver) truncate(ctx context.Context, acknowledged uint64) (bool, error) {
	log, err := d.walFrameCount(ctx)
	if err != nil {
		return false, err
	}
	if uint64(log) > acknowledged {
		return false, fmt.Errorf("driver: the WAL holds %d frames but only %d are acknowledged", log, acknowledged)
	}
	d.releasePin()

	st, err := d.checkpoint(ctx, "TRUNCATE")
	if err != nil {
		return false, err
	}
	if st.Busy {
		return false, fmt.Errorf("driver: the WAL could not be truncated, a reader is using it")
	}
	return true, nil
}

// ackFrames returns the number of frames the consumer has acknowledged in
// the current WAL generation.
//
// An acknowledgement of an older generation says nothing about the frames of
// the current one, so it counts as nothing.
func (d *Driver) ackFrames() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.ack.sameGeneration(d.cursor) {
		return 0
	}
	return d.ack.Frame
}

// checkpointStats returns the statistics of the last checkpoint.
func (d *Driver) checkpointStats() CheckpointStats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ckptStats
}

func (d *Driver) setCheckpointStats(st CheckpointStats) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ckptStats = st
}

// releasePin gives the read mark back.
//
// The caller has to hold the lock of the checkpoint manager.
func (d *Driver) releasePin() {
	d.pin.release()
	d.pin = nil
}
