package source

import (
	"context"
	"time"
)

// gcLoop collects the segments the backup acknowledged.
func (p *Peer) gcLoop(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.GCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := p.collect(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			p.setError(err)
		}
	}
}

// collect writes the acknowledgement of the backup into the spool and
// collects the segments it covers.
//
// The acknowledgement is written before the segments are removed: a segment
// that is removed may not be sent again.
func (p *Peer) collect(ctx context.Context) error {
	ack := p.appliedSeq()
	stored, err := p.sp.BackupAck()
	if err != nil {
		return err
	}
	if stored > ack {
		ack = stored
	}
	if ack == p.backupSeq() {
		return nil
	}
	if err := p.sp.SetBackupAck(ctx, ack); err != nil {
		return err
	}
	p.setBackupSeq(ack)
	_, err = p.sp.GC(ctx)
	return err
}

// appliedSeq returns the sequence number the backup acknowledged.
//
// The protocol reports the number of the chain it serves. It is zero when
// the chain of the spool was reset: the number of the old chain says
// nothing about the segments of the new one and it may not be used to
// collect them.
func (p *Peer) appliedSeq() uint64 {
	seq := p.proto.AppliedSeq()
	p.mu.Lock()
	defer p.mu.Unlock()
	if seq != p.appliedSeen {
		p.appliedSeen = seq
		if seq > 0 {
			p.lastAck = time.Now()
			// The backup applies the increments of the chain of the spool:
			// it holds the new baseline.
			p.snapshotRequired = false
			p.snapshotReason = ""
		}
	}
	return seq
}

// setBackupSeq remembers the sequence number that was written into the
// acknowledgement of the spool.
func (p *Peer) setBackupSeq(seq uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.backupAckSeq = seq
}

// backupSeq returns the sequence number that was written into the
// acknowledgement of the spool.
func (p *Peer) backupSeq() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.backupAckSeq
}
