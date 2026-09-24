package factory

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunnerRunContextStopsAgentWhenCanceled(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "slow-agent.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexec sleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := (Runner{Config: Config{Command: script}}).RunContext(ctx, "babysit", "prompt", "task", dir, filepath.Join(dir, "agent.log"))
	if err == nil {
		t.Fatal("canceled agent unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("canceled agent took %s to return", elapsed)
	}
}

func TestRunnerExpandsPlaceholdersAsDistinctArguments(t *testing.T) {
	dir := t.TempDir()
	capture := filepath.Join(dir, "capture")
	script := filepath.Join(dir, "agent.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$CAPTURE\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CAPTURE", capture)
	cfg := Config{Command: script, Args: []string{"{stage}", "{task}", "{system_prompt}", "{workdir}"}}
	log := filepath.Join(dir, "logs", "agent.log")
	if err := (Runner{Config: cfg}).Run("implement", "prompt; not shell", "task with spaces", dir, log); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(got), "\n"), "\n")
	want := []string{"implement", "task with spaces", "prompt; not shell", dir}
	if strings.Join(lines, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("argv = %#v, want %#v", lines, want)
	}
	if _, err := os.Stat(log); err != nil {
		t.Fatalf("log was not created: %v", err)
	}
}
