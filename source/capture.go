package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cnlangzi/sqlite-relay/driver"
	"github.com/cnlangzi/sqlite-relay/spool"
)

// errSpoolFull reports that the spool reached its limit.
//
// The capture stands still until the spool has room again. The business
// keeps writing, the increments are not lost.
var errSpoolFull = errors.New("source: the spool is full, the capture stands still")

// captureLoop captures the increments of the primary database.
//
// The loop is woken by the notifications of the driver and by the periodic
// scan. Both are a hint that there is something to capture: the increments
// themselves are always complete, the boundary of an increment is decided by
// the bytes of the log and not by a notification. A notification that is
// merged or lost costs a round, it never costs an increment.
func (p *Peer) captureLoop(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.ScanInterval)
	defer ticker.Stop()
	events := p.drv.Events()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-events:
			if !ok {
				// The driver is closed, the periodic scan is the only
				// wake-up left.
				events = nil
			}
		case <-ticker.C:
		}
		err := p.captureRound(ctx)
		if err == nil {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		p.handleRoundError(ctx, err)
	}
}

// handleRoundError reacts to the error of a capture round.
//
// A chain that the driver could not capture is repaired: the spool starts a
// new chain and the backup gets a new baseline.
func (p *Peer) handleRoundError(ctx context.Context, err error) {
	if errors.Is(err, driver.ErrSnapshotRequired) || errors.Is(err, errChainBroken) {
		if rerr := p.recoverChain(ctx, err); rerr != nil {
			p.setError(rerr)
		}
		return
	}
	p.setError(err)
}

// captureRound runs one capture round.
//
// The round waits until the capture is not suspended any more. The suspend
// waits for the round that is running to end, so that no increment enters
// the spool while the capture stands still.
func (p *Peer) captureRound(ctx context.Context) error {
	for {
		p.gateMu.Lock()
		if p.suspends > 0 {
			ch := p.resumed
			p.gateMu.Unlock()
			select {
			case <-ch:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		end := make(chan struct{})
		p.roundEnd = end
		p.gateMu.Unlock()

		err := p.round(ctx)

		p.gateMu.Lock()
		p.roundEnd = nil
		p.gateMu.Unlock()
		close(end)
		return err
	}
}

// round captures the increments of the primary database and writes them
// into the spool.
//
// The order of the round is fixed: the increments are scanned, they are
// appended one by one, the segment is closed, the barrier runs when it is
// due and the durable acknowledgement of the spool is handed to the driver.
// The acknowledgement is only handed over when the increments crossed the
// persistence barrier of the spool: it does not wait for the backup.
func (p *Peer) round(ctx context.Context) error {
	// The transactions that did not fit into the spool are written first:
	// they are older than the ones a new scan brings.
	if err := p.writePending(ctx); err != nil {
		return err
	}
	full := p.hasPending()

	if !full {
		batch, err := p.drv.Scan(ctx)
		if err != nil {
			return err
		}
		// The chain has to cover the primary database before the increments
		// of a new generation of the log enter it: a chain with a hole
		// looks like a chain that continues.
		if err := p.checkContinuity(); err != nil {
			return err
		}
		rest, err := p.appendTo(ctx, p.transactionsOf(batch.Changes))
		p.setPending(rest, errors.Is(err, spool.ErrSpoolFull))
		if err != nil && !errors.Is(err, spool.ErrSpoolFull) {
			return err
		}
		full = len(rest) > 0
	}

	// The segment is closed even when the spool is full: a published
	// segment can be collected after the backup acknowledged it, an open
	// one can not.
	if err := p.sp.Flush(ctx); err != nil {
		return err
	}
	pending, err := p.sp.Pending()
	if err != nil {
		return err
	}
	if pending.Due {
		if err := p.sp.Sync(ctx); err != nil {
			return err
		}
	}
	if full {
		// The spool is full. The capture stands still and the business
		// keeps writing: the increments are captured again as soon as the
		// spool has room again. The frames of the log stay in the log.
		return nil
	}
	ack, err := p.sp.Durable()
	if err != nil {
		return err
	}
	if ack.Seq == 0 {
		// Nothing is durable yet, the driver keeps the frames of the log.
		return nil
	}
	return p.drv.CaptureAck(ctx, toDriverPos(ack.Pos))
}

// transactionsOf turns the changes of the driver into the transactions of
// the spool.
//
// The changes that the spool holds already are left out: after a restart the
// driver hands out the increments of the whole generation again and the
// spool tells which of them it holds.
func (p *Peer) transactionsOf(changes []driver.Change) []spool.Tx {
	out := make([]spool.Tx, 0, len(changes))
	for _, c := range changes {
		if p.holds(c.Pos) {
			continue
		}
		tx := spool.Tx{
			Pos: spool.Pos{
				Salt1:     c.Pos.Salt1,
				Salt2:     c.Pos.Salt2,
				Frame:     c.Pos.Frame,
				WALOffset: c.Pos.WALOffset,
			},
			CommitPages: c.CommitPages,
		}
		for _, page := range c.Pages {
			tx.Pages = append(tx.Pages, spool.Page{Pgno: page.Pgno, Data: page.Data})
		}
		out = append(out, tx)
	}
	return out
}

// holds reports whether the spool holds the increment at pos already.
//
// The position of the resume is the position of the last transaction that
// was durable when the runtime started. An increment of the same generation
// that was committed at or before it is in the spool already. An increment
// of another generation is new data.
func (p *Peer) holds(pos driver.Position) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.resumeSet {
		return false
	}
	if p.resumePos.Salt1 != pos.Salt1 || p.resumePos.Salt2 != pos.Salt2 {
		return false
	}
	return pos.Frame <= p.resumePos.Frame
}

// checkContinuity decides whether the chain of the segments still covers the
// primary database.
//
// The decision is made once: the generations that come after are the business
// of the driver, it reports the ones it cannot capture. The chain is broken
// when the log was reset while the runtime was down and the marker of the
// last clean close does not prove that the chain was complete then.
func (p *Peer) checkContinuity() error {
	p.mu.Lock()
	checked := p.continuityChecked
	p.mu.Unlock()
	if checked {
		return nil
	}

	ack, err := p.sp.Durable()
	if err != nil {
		return err
	}
	var marker *cleanMarker
	if ack.Seq > 0 {
		marker = readMarker(p.markerPath())
	}
	switch judgeChain(ack, p.drv.Stats().Generation, marker) {
	case verdictUnknown:
		return nil
	case verdictContinuous:
		p.setContinuityChecked()
		return nil
	default:
		return fmt.Errorf("%w: the log was reset and the chain does not prove that it covers the primary database", errChainBroken)
	}
}

// setContinuityChecked remembers that the continuity of the chain was
// decided.
func (p *Peer) setContinuityChecked() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.continuityChecked = true
}

// appendTo writes the transactions into the spool.
//
// The transactions that were not written are returned: they are written again
// when the spool has room again, or when the failure that stopped the write
// is gone. The driver does not hand them out a second time, they would be
// lost.
func (p *Peer) appendTo(ctx context.Context, txs []spool.Tx) ([]spool.Tx, error) {
	for i, tx := range txs {
		err := p.sp.Append(ctx, tx)
		if errors.Is(err, spool.ErrSegmentFull) {
			// The segment is full: it is closed and the transaction is
			// written into the next one.
			if ferr := p.sp.Flush(ctx); ferr != nil {
				return txs[i:], ferr
			}
			err = p.sp.Append(ctx, tx)
		}
		if err != nil {
			return txs[i:], err
		}
	}
	return nil, nil
}

// writePending writes the transactions that did not fit into the spool.
func (p *Peer) writePending(ctx context.Context) error {
	p.mu.Lock()
	pending := p.pending
	p.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	rest, err := p.appendTo(ctx, pending)
	p.setPending(rest, errors.Is(err, spool.ErrSpoolFull))
	if err != nil && !errors.Is(err, spool.ErrSpoolFull) {
		return err
	}
	return nil
}

// hasPending reports whether transactions wait for the spool.
func (p *Peer) hasPending() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pending) > 0
}

// setPending remembers the transactions that wait for the spool.
func (p *Peer) setPending(txs []spool.Tx, full bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending = txs
	p.spoolFull = full
	if full {
		p.lastErr = errSpoolFull
		return
	}
	if errors.Is(p.lastErr, errSpoolFull) {
		p.lastErr = nil
	}
}

// recoverChain brings the chain back after it was broken.
//
// The chain of the segments is gone: the spool is reset, the epoch is raised
// and the sequence numbers start over. The backup needs a new baseline, the
// snapshot of the primary database is the baseline of the chain that follows
// it. Taking the snapshot moves the capture cursor of the driver to the
// boundary of the snapshot, the capture continues after it.
func (p *Peer) recoverChain(ctx context.Context, cause error) error {
	if err := p.sp.Reset(ctx); err != nil {
		return err
	}
	// The marker describes a chain that is gone.
	if err := removeMarker(p.markerPath()); err != nil {
		return err
	}
	_, r, err := p.prod.Snapshot(ctx)
	if err != nil {
		return err
	}
	// The file of the snapshot is removed when its reader is closed.
	if _, err := io.Copy(io.Discard, r); err != nil {
		_ = r.Close()
		return err
	}
	if err := r.Close(); err != nil {
		return err
	}

	p.mu.Lock()
	p.snapshotRequired = true
	p.snapshotReason = cause.Error()
	// The acknowledgement of the backup belongs to the chain that is gone.
	p.backupAckSeq = 0
	// The position the runtime continued from belongs to the chain that is
	// gone.
	p.resumeSet = false
	p.continuityChecked = false
	p.mu.Unlock()
	return nil
}
