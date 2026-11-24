package protocol

import (
	"bytes"
	"testing"
)

func TestConfigValidation(t *testing.T) {
	cfg := testConfig(t)
	if _, err := NewSource(cfg, &fakeSource{}); err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	if _, err := NewBackup(cfg, &fakeSink{}); err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	// The token file and the database have no default, a source and a
	// backup without them cannot work.
	cfg = testConfig(t)
	cfg.TokenFile = ""
	if _, err := NewSource(cfg, &fakeSource{}); err == nil {
		t.Error("a source without a token file was accepted")
	}
	cfg = testConfig(t)
	cfg.DatabaseID = ""
	if _, err := NewBackup(cfg, &fakeSink{}); err == nil {
		t.Error("a backup without a database was accepted")
	}

	// A name of a backup that does not fit into the hello is refused
	// instead of being cut.
	cfg = testConfig(t)
	cfg.BackupID = string(bytes.Repeat([]byte("b"), MaxBackupIDLen+1))
	if _, err := NewBackup(cfg, &fakeSink{}); err == nil {
		t.Error("a backup with a long name was accepted")
	}
}
