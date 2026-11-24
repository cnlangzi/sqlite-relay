// Package backup is the backup end of a relay.
//
// The backup owns the backup database: a plain SQLite file that holds a copy
// of the primary database of the source. While the backup is in sync the
// relay holds the file to itself. It writes the pages of the transactions
// of the chain into the file, it does not open the file with SQLite and it
// leaves no -wal and no -shm file behind.
//
// The backup asks for a snapshot when it has no baseline. The source sends
// the snapshot, the backup installs it and the increments of the chain that
// follow the baseline of the snapshot bring the backup up to date.
//
// The backup can be paused. A paused backup does not touch its database any
// more, so that a business program may read it while the backup serves.
package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cnlangzi/sqlite-relay/protocol"
)

// The names of the files of a backup.
const (
	// dbName is the name of the file of the backup database.
	dbName = "backup.db"
	// stateName is the name of the file of the state of the backup.
	stateName = "state.json"
)

// The states of a backup.
const (
	// StateSyncing means that the backup applies the increments of the
	// chain.
	StateSyncing = "syncing"
	// StatePaused means that the backup does not touch its database.
	StatePaused = "paused"
)

// Config configures the backup end of a relay.
type Config struct {
	// Root is the directory the backups live in. Every database has its
	// own directory below it.
	Root string
	// Protocol holds the settings of the connection: the token file, the
	// name of the database, the number of the stream, the name of the
	// backup and the timeouts.
	Protocol protocol.Config
	// Verify checks the integrity of the backup database before a
	// snapshot is installed. It is off by default.
	Verify bool
}

// Stats is a snapshot of the state of a backup.
type Stats struct {
	// State is the state of the backup: syncing or paused.
	State string
	// AppliedSeq is the sequence number the backup applied and persisted.
	AppliedSeq uint64
	// Epoch is the generation of the chain the backup is part of.
	Epoch uint64
	// LastAck is the time of the last acknowledgement the backup sent.
	LastAck time.Time
	// Lag is the time since the last acknowledgement.
	Lag time.Duration
	// LastError is the error of the last failure.
	LastError string
	// LastSnapshot is the time of the last snapshot that was installed.
	LastSnapshot time.Time
	// LastSnapshotBytes is the size of the last snapshot that was
	// installed.
	LastSnapshotBytes int64
	// Snapshots is the number of the snapshots that were installed.
	Snapshots int
	// SnapshotRequired is the number of the times the backup asked for a
	// snapshot.
	SnapshotRequired int
	// SnapshotReason explains the last request for a snapshot.
	SnapshotReason string
}

// Peer is the backup end of a relay.
//
// The peer owns the backup database, the state of the backup and the
// connection to the source. It applies the increments of the chain, it
// installs the snapshot that establishes the baseline and it pauses the
// backup so that a business program may read the backup database.
type Peer struct {
	cfg Config
	dir string
	app *applier
	// proto is the connection to the source.
	proto *protocol.Backup

	mu sync.Mutex
	// paused reports that the backup does not touch its database.
	paused bool
	// lastErr is the error of the last failure.
	lastErr error
	// snapshotRequired is the number of the times the backup asked for a
	// snapshot.
	snapshotRequired int
	// snapshotReason explains the last request for a snapshot.
	snapshotReason string
}

// Open opens the backup of a database.
//
// The directory of the backup is created when it does not exist. The state
// of the backup is read and an installation that was interrupted is
// finished: the backup holds either the old database or the new one, never
// half a file.
//
// A backup without a baseline asks for a snapshot when it connects.
func Open(ctx context.Context, cfg Config) (*Peer, error) {
	if cfg.Root == "" {
		return nil, errors.New("backup: the root directory is empty")
	}
	if cfg.Protocol.DatabaseID == "" {
		return nil, errors.New("backup: the database ID is empty")
	}
	dir := filepath.Join(cfg.Root, cfg.Protocol.DatabaseID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("backup: create %s: %w", dir, err)
	}
	dbPath := filepath.Join(dir, dbName)
	statePath := filepath.Join(dir, stateName)

	st, err := readState(statePath)
	if err != nil {
		return nil, err
	}
	if st == nil {
		st = &state{
			Version:    stateVersion,
			Phase:      PhaseEmpty,
			DatabaseID: cfg.Protocol.DatabaseID,
			StreamID:   cfg.Protocol.StreamID,
			DBPath:     dbPath,
		}
	}
	if st.DatabaseID != "" && st.DatabaseID != cfg.Protocol.DatabaseID {
		return nil, fmt.Errorf("backup: the directory holds the backup of the database %q", st.DatabaseID)
	}
	if err := recover(dir, dbPath, statePath, st); err != nil {
		return nil, err
	}

	app := &applier{
		dir:       dir,
		dbPath:    dbPath,
		statePath: statePath,
		verify:    cfg.Verify,
		st:        st,
		applied:   st.AppliedSeq,
		pending:   st.AppliedSeq,
	}
	if st.Phase == PhaseReady {
		if err := app.open(); err != nil {
			return nil, err
		}
	}
	proto, err := protocol.NewBackup(cfg.Protocol, app)
	if err != nil {
		return nil, err
	}
	return &Peer{cfg: cfg, dir: dir, app: app, proto: proto}, nil
}

// Run dials the source and applies the increments it pushes.
//
// The run ends when the context is done. A chain that is not continuous any
// more does not end the run: the peer asks for a new snapshot and it opens
// the connection again.
func (p *Peer) Run(ctx context.Context, addr string) error {
	for {
		err := p.proto.Run(ctx, addr)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, protocol.ErrResyncRequired) {
			p.requireSnapshot(err)
			// The connection is opened again after a delay: the chain of
			// the source may not be continuous yet.
			if err := sleep(ctx, p.backoff()); err != nil {
				return err
			}
			continue
		}
		return err
	}
}

// backoff returns the delay before the next connection attempt.
func (p *Peer) backoff() time.Duration {
	d := p.cfg.Protocol.BackoffMin
	if d <= 0 {
		d = time.Second
	}
	return d
}

// sleep waits for the duration or until the context is done.
func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Pause stops the backup and hands the backup database to the business.
//
// The source stops pushing the increments of the chain, the applier stops at
// the boundary of a segment, the applied sequence number is persisted and
// the file of the backup database is closed. The backup database is not
// touched any more when the call returns.
//
// A pause that is already running returns without an error.
func (p *Peer) Pause(ctx context.Context) error {
	p.mu.Lock()
	if p.paused {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	applied, err := p.app.AppliedSeq(ctx)
	if err != nil {
		return err
	}
	if err := p.proto.Pause(ctx, applied); err != nil {
		p.setError(err)
		return err
	}
	if err := p.app.Pause(ctx); err != nil {
		p.setError(err)
		return err
	}
	p.mu.Lock()
	p.paused = true
	p.mu.Unlock()
	return nil
}

// Resume takes the backup database back and continues the chain.
//
// The file of the backup database is opened again and the source continues
// after the sequence number the backup applied and persisted. The business
// has to close its connections to the backup database before the resume.
//
// A resume that is already running returns without an error.
func (p *Peer) Resume(ctx context.Context) error {
	p.mu.Lock()
	if !p.paused {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	p.app.expectEpoch(p.proto.Epoch())
	if err := p.app.Resume(ctx); err != nil {
		p.setError(err)
		return err
	}
	applied, err := p.app.AppliedSeq(ctx)
	if err != nil {
		return err
	}
	if err := p.proto.Resume(ctx, applied); err != nil {
		p.setError(err)
		return err
	}
	p.mu.Lock()
	p.paused = false
	p.mu.Unlock()
	return nil
}

// Stats returns a snapshot of the state of the backup.
func (p *Peer) Stats() Stats {
	p.mu.Lock()
	state := StateSyncing
	if p.paused {
		state = StatePaused
	}
	st := Stats{
		State:            state,
		Epoch:            p.app.Epoch(),
		LastAck:          p.proto.LastAck(),
		SnapshotRequired: p.snapshotRequired,
		SnapshotReason:   p.snapshotReason,
	}
	if p.lastErr != nil {
		st.LastError = p.lastErr.Error()
	}
	p.mu.Unlock()
	if st.LastError == "" {
		if err := p.proto.LastError(); err != nil {
			st.LastError = err.Error()
		}
	}

	st.LastSnapshot, st.LastSnapshotBytes = p.app.snapshotStats()
	applied, err := p.app.AppliedSeq(context.Background())
	if err == nil {
		st.AppliedSeq = applied
	}
	if !st.LastAck.IsZero() {
		st.Lag = time.Since(st.LastAck)
	}
	return st
}

// requireSnapshot tells the applier that its baseline is not valid any more.
//
// The chain of the source is not continuous any more, only a new snapshot
// brings the backup back.
func (p *Peer) requireSnapshot(err error) {
	p.mu.Lock()
	p.snapshotRequired++
	p.snapshotReason = err.Error()
	p.lastErr = err
	p.mu.Unlock()
	p.app.requireSnapshot()
}

// setError remembers the error of the last failure.
func (p *Peer) setError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastErr = err
}
