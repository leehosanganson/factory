//go:build linux || darwin

package factory

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestImplementationCommandCancellationKillsProcessGroup(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	pidFile := filepath.Join(dir, "command.pid")
	marker := filepath.Join(dir, "survived")
	release := filepath.Join(dir, "release")
	script := filepath.Join(dir, "agent.sh")
	body := "#!/bin/sh\n" +
		"printf '%s\\n' \"$$\" > \"$CANCEL_COMMAND_PID\"\n" +
		"( : > \"$CANCEL_COMMAND_READY\"; while [ ! -e \"$CANCEL_COMMAND_RELEASE\" ]; do sleep 0.01; done; : > \"$CANCEL_COMMAND_MARKER\" ) &\n" +
		"wait\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CANCEL_COMMAND_READY", ready)
	t.Setenv("CANCEL_COMMAND_PID", pidFile)
	t.Setenv("CANCEL_COMMAND_MARKER", marker)
	t.Setenv("CANCEL_COMMAND_RELEASE", release)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, err := implementationCommand(ctx, dir, script)
		done <- err
	}()
	defer func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			killImplementationCommandGroup(t, pidFile)
		}
		killImplementationCommandGroup(t, pidFile)
	}()

	waitForImplementationCommandReadiness(t, ready, done)
	cancel()

	select {
	case err := <-done:
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("implementationCommand() error = %v, want process signal error", err)
		}
		status, ok := exitErr.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			t.Fatalf("implementationCommand() exit status = %v, want SIGKILL", exitErr.Sys())
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("implementationCommand() did not return promptly after cancellation")
	}

	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatalf("release descendant marker check: %v", err)
	}
	assertImplementationDescendantDidNotWriteMarker(t, marker)
}

func assertImplementationDescendantDidNotWriteMarker(t *testing.T, marker string) {
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

func waitForImplementationCommandReadiness(t *testing.T, ready string, done <-chan error) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Stat(ready); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("check implementation command readiness: %v", err)
		}
		select {
		case err := <-done:
			t.Fatalf("implementation command returned before descendant became active: %v", err)
		case <-deadline.C:
			t.Fatal("implementation command descendant did not become active before the deadline")
		case <-tick.C:
		}
	}
}

func killImplementationCommandGroup(t *testing.T, pidFile string) {
	t.Helper()
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		t.Errorf("kill implementation command process group %d during cleanup: %v", pid, err)
	}
}
