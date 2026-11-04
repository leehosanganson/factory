//go:build linux || darwin

package workflow

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRunnerRunContextStopsDescendantWhenCanceled(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	pidFile := filepath.Join(dir, "child.pid")
	marker := filepath.Join(dir, "survived")
	release := filepath.Join(dir, "release")
	script := filepath.Join(dir, "agent.sh")
	body := "#!/bin/sh\n" +
		"( while [ ! -e \"$CANCEL_RELEASE\" ]; do sleep 0.01; done; : > \"$CANCEL_MARKER\" ) & sleeper=$!\n" +
		"printf '%s %s\\n' \"$$\" \"$sleeper\" > \"$CANCEL_CHILD_PID\"\n" +
		": > \"$CANCEL_READY\"\n" +
		"wait\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CANCEL_READY", ready)
	t.Setenv("CANCEL_CHILD_PID", pidFile)
	t.Setenv("CANCEL_MARKER", marker)
	t.Setenv("CANCEL_RELEASE", release)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (Runner{Config: Config{Command: script, Args: []string{"{task}", "{system_prompt}"}}}).RunContext(ctx, "implement", "prompt", "task", dir, filepath.Join(dir, "agent.log"))
	}()
	defer killRecordedProcess(t, pidFile)
	waitForCancellationReadiness(t, ready, done)

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunContext() error = %v, want context.Canceled", err)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("RunContext() did not return promptly after cancellation")
	}

	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatalf("release descendant marker check: %v", err)
	}
	assertRunnerDescendantDidNotWriteMarker(t, marker)
}

func assertRunnerDescendantDidNotWriteMarker(t *testing.T, marker string) {
	t.Helper()
	deadline := time.NewTimer(500 * time.Millisecond)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Stat(marker); err == nil {
			t.Fatal("descendant survived cancellation and wrote its marker")
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("check descendant marker: %v", err)
		}
		select {
		case <-deadline.C:
			return
		case <-tick.C:
		}
	}
}

func waitForCancellationReadiness(t *testing.T, ready string, done <-chan error) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Stat(ready); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("check descendant readiness: %v", err)
		}
		select {
		case err := <-done:
			t.Fatalf("process returned before descendant became active: %v", err)
		case <-deadline.C:
			t.Fatal("descendant did not become active before the deadline")
		case <-tick.C:
		}
	}
}

func killRecordedProcess(t *testing.T, pidFile string) {
	t.Helper()
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return
	}
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return
	}
	// Kill the in-group shell too, so a failed assertion cannot leave its wait alive.
	for _, value := range fields {
		pid, err := strconv.Atoi(value)
		if err != nil || pid <= 0 {
			continue
		}
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
			t.Errorf("kill test descendant %d during cleanup: %v", pid, err)
		}
	}
}
