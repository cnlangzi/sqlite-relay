//go:build e2e

package e2e

import (
	"testing"
)

// TestTheRelayWorksWithEveryPersistenceMode checks that the relay works
// with every combination of the persistence barriers.
//
// The barrier of the spool and the barrier of the primary database are
// independent of each other: every combination has to bring the backup in
// line with the primary, also after a restart of the primary.
func TestTheRelayWorksWithEveryPersistenceMode(t *testing.T) {
	modes := []string{"OFF", "NORMAL", "FULL"}
	synchronous := []string{"OFF", "NORMAL", "FULL"}
	for _, mode := range modes {
		for _, sync := range synchronous {
			t.Run(mode+"/"+sync, func(t *testing.T) {
				r := newRelay(t)
				// The segments of the chain are kept: the chain has to
				// survive the restart of the primary.
				r.writeSettings(settings{SpoolMode: mode, Synchronous: sync, Retention: 3600})
				r.startPrimary()
				r.createSchema()
				r.insert(100)
				r.startBackup()
				r.waitCatchUp()
				r.compare()

				// The primary is stopped and started again: the chain
				// survives the restart with every barrier.
				if err := r.primary.stop(); err != nil {
					t.Fatalf("the primary did not stop cleanly: %v", err)
				}
				r.startPrimary()
				r.insert(50)
				r.waitCatchUp()
				if st := r.backupStats(); st.Snapshots != 1 {
					t.Errorf("the backup installed %d snapshots, want 1", st.Snapshots)
				}
				r.compare()
			})
		}
	}
}
