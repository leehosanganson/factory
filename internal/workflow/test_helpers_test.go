package workflow

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func resolvedTestPath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := ResolvedPath(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}
func sameResolvedTestPath(t *testing.T, left, right string) bool {
	t.Helper()
	return resolvedTestPath(t, left) == resolvedTestPath(t, right)
}
func workflowRunState(t *testing.T, stateDir string) State {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(stateDir, "runs"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected one workflow run, entries=%v err=%v", entries, err)
	}
	data, err := os.ReadFile(filepath.Join(stateDir, "runs", entries[0].Name(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	return state
}
func workflowRunStatus(t *testing.T, stateDir string) string {
	t.Helper()
	return workflowRunState(t, stateDir).Status
}
func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}
