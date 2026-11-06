package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestIntakeOwnerCommandRequiresExplicitTransferAttestation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pg.json")
	if err := os.WriteFile(path, []byte(`{"postgres_dsn":"postgres://localhost/factory"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"transfer", "--config", path, "--source", "acme/issues", "--label", "Ready", "--kind", "poller", "--attest", ""},
		{"register", "--config", path, "--source", "acme/issues", "--label", "Ready", "--kind", "poller", "--attest", "not allowed"},
		{"inspect", "--config", path, "--source", "acme/issues", "--label", "Ready", "--kind", "poller"},
	} {
		var out bytes.Buffer
		if err := runIntakeOwnerCommand(args, &out); err == nil || err.Error() == "open canonical PostgreSQL job store" {
			t.Errorf("runIntakeOwnerCommand(%v) error=%v", args, err)
		}
	}
}
