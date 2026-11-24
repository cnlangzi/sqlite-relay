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
	applied := p.appliedSeq()
	ack, err := p.sp.BackupAck()
	if err != nil {
		return err
	}
	if applied > ack {
		ack = applied
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
// The protocol remembers the last sequence number it saw, also from a chain
// that was reset. Such a number belongs to a chain that does not exist any
// more and it may not be written into the acknowledgement of the current
// chain: the segments of the current chain would be collected before the
// backup applied them. The number is only trusted when it changed since the
// chain was reset.
func (p *Peer) appliedSeq() uint64 {
	seq := p.proto.AppliedSeq()
	p.mu.Lock()
	defer p.mu.Unlock()
	if seq != p.appliedSeen {
		p.appliedSeen = seq
		p.appliedTrusted = true
		if seq > 0 {
			p.lastAck = time.Now()
		}
	}
	if !p.appliedTrusted {
		return 0
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
