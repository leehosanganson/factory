package factory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteStateReplacesStateAtomicallyWithPrivatePermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte(`{"old":true}`), 0o644); err != nil {
		t.Fatal(err)
	}

	state := &State{ID: "run-1", Status: "complete", Stages: []StageRecord{}}
	if err := writeState(dir, state); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("state permissions = %04o, want 0600", got)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got State
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("state file is not complete JSON: %v", err)
	}
	if got.ID != state.ID || got.Status != state.Status {
		t.Fatalf("state = %+v, want ID %q and status %q", got, state.ID, state.Status)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "state.json" {
		t.Errorf("state directory entries = %v, want only state.json", entries)
	}
}
