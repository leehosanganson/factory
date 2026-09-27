package factory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAttachCancellationDetachesAndWorkerCanBeReattached(t *testing.T) {
	state := t.TempDir()
	target := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "attach-cancel", Type: implementationJobType, TaskDescription: "attach test", TargetPath: target, Status: "queued"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession("attach-cancel", "workflow", "queued"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJobLog("attach-cancel"); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "fake-agent")
	body := "#!/bin/sh\nsleep 0.3\ncase \"$2\" in *'Stage completed'*) echo PASS ;; *) echo agent-output ;; esac\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(t.TempDir(), "config", "factory")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"5s"}`, script, state)
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", filepath.Dir(configDir))

	workerDone := make(chan error, 1)
	go func() { workerDone <- RunJobWorker("attach-cancel", store.Root()) }()
	waitFor(t, 3*time.Second, func() bool {
		job, err := store.GetJob("attach-cancel")
		return err == nil && job.Status == "running"
	})

	logPath, err := store.JobLogPath("attach-cancel")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("live progress\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var firstAttach synchronizedBuffer
	attachDone := make(chan error, 1)
	go func() { attachDone <- AttachJob(ctx, store, "attach-cancel", &firstAttach) }()
	waitFor(t, time.Second, func() bool { return strings.Contains(firstAttach.String(), "live progress") })
	cancel()
	select {
	case err := <-attachDone:
		if err != nil {
			t.Fatalf("detaching observer: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("attach did not detach after cancellation")
	}
	job, err := store.GetJob("attach-cancel")
	if err != nil || job.Status != "running" {
		t.Fatalf("detaching changed worker lifecycle: status=%q err=%v", job.Status, err)
	}
	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatalf("worker failed after observer detached: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not finish after observer detached")
	}
	job, err = store.GetJob("attach-cancel")
	if err != nil || job.Status != "complete" {
		t.Fatalf("worker lifecycle after detach: status=%q err=%v", job.Status, err)
	}

	var reattached bytes.Buffer
	if err := AttachJob(context.Background(), store, "attach-cancel", &reattached); err != nil {
		t.Fatalf("reattach: %v", err)
	}
	if !strings.Contains(reattached.String(), "live progress") {
		t.Fatalf("reattached transcript omitted prior worker output: %q", reattached.String())
	}
	if !strings.HasPrefix(reattached.String(), firstAttach.String()) {
		t.Fatalf("reattach did not replay transcript from the beginning: first=%q replay=%q", firstAttach.String(), reattached.String())
	}
}

type synchronizedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *synchronizedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(data)
}

func (b *synchronizedBuffer) ReadFrom(reader io.Reader) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.ReadFrom(reader)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

func TestAttachSurfacesFailedAndStoppedLifecycle(t *testing.T) {
	for _, status := range []string{"failed", "stopped", "interrupted"} {
		t.Run(status, func(t *testing.T) {
			store := newTestJobStore(t)
			if err := store.CreateJob(JobRecord{ID: "terminal", Type: implementationJobType, Status: status}); err != nil {
				t.Fatal(err)
			}
			if err := store.CreateJobLog("terminal"); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			err := AttachJob(context.Background(), store, "terminal", &out)
			if err == nil || !strings.Contains(err.Error(), status) {
				t.Fatalf("attach terminal status error = %v, want status %q", err, status)
			}
		})
	}
}

func TestMonitorEventAppendFailureDoesNotChangeCanonicalMonitorRecord(t *testing.T) {
	state := t.TempDir()
	root, err := JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	id := "20260518T120016-0123456789ab"
	monitor := &monitorJob{ID: id, RepoRoot: t.TempDir(), Status: "running", LastEvent: "previous event"}
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TargetPath: monitor.RepoRoot, Status: "running", Monitor: monitor}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, monitorSessionID, "running"); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, id)
	oldAppend := monitorSessionLogAppend
	monitorSessionLogAppend = func(*JobStore, string, []byte) error { return errors.New("injected session log failure") }
	t.Cleanup(func() { monitorSessionLogAppend = oldAppend })
	monitor.Status = "complete"
	err = monitorEvent(dir, monitor, "PR merged")
	if err == nil || !strings.Contains(err.Error(), "injected session log failure") {
		t.Fatalf("monitorEvent error=%v", err)
	}
	persisted, err := readMonitorJob(dir)
	if err != nil || persisted.Status != "running" || persisted.LastEvent != "previous event" {
		t.Fatalf("monitor record changed: %+v err=%v", persisted, err)
	}
	events, err := store.SessionEvents(id, monitorSessionID)
	if err != nil || len(events) != 0 {
		t.Fatalf("event persisted despite log failure: %+v err=%v", events, err)
	}
}

func TestAttachMonitorUsesCanonicalMonitorRecordAndSessionLog(t *testing.T) {
	state := t.TempDir()
	root, err := JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "20260518T120012-0123456789ab"
	target := t.TempDir()
	monitor := &monitorJob{ID: id, RepoRoot: target, Status: "complete", LastEvent: "PR is merged; monitoring stopped."}
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TargetPath: target, Status: "complete", Monitor: monitor}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, monitorSessionID, "complete"); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendSessionLog(id, monitorSessionID, []byte("canonical monitor transcript\n")); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := AttachJob(context.Background(), store, id, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "canonical monitor transcript\n" {
		t.Fatalf("attach output=%q", got)
	}
	out.Reset()
	if err := JobCommand([]string{"logs", id}, Config{StateDir: state}, t.TempDir(), nil, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "canonical monitor transcript\n" {
		t.Fatalf("logs output=%q", got)
	}
}

func TestAttachMonitorIgnoresOldMonitorRootAndActionsLog(t *testing.T) {
	state := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	const id = "20260518T120013-0123456789ab"
	target := t.TempDir()
	monitor := &monitorJob{ID: id, RepoRoot: target, Status: "complete"}
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TargetPath: target, Status: "complete", Monitor: monitor}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, monitorSessionID, "complete"); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendSessionLog(id, monitorSessionID, []byte("canonical session\n")); err != nil {
		t.Fatal(err)
	}
	oldDir := filepath.Join(state, "factory", "jobs", id)
	if err := os.MkdirAll(oldDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oldDir, "actions.log"), []byte("old transcript\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := AttachJob(context.Background(), store, id, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "canonical session\n" {
		t.Fatalf("old monitor root was read: %q", got)
	}
}

func TestCanonicalJobStatusesDefineTerminalOutcomes(t *testing.T) {
	for _, status := range []string{"complete", "closed", "failed", "stopped", "cancelled", "interrupted"} {
		if !isTerminalStatus(status) {
			t.Errorf("canonical final status %q must be terminal", status)
		}
	}
	for _, status := range []string{"queued", "running", "recoverable_failure", "completed", "merged"} {
		if isTerminalStatus(status) {
			t.Errorf("noncanonical or resumable status %q must not be terminal", status)
		}
	}
}

func TestAttachRecoverableFailureReturnsWhenMonitorWorkerExited(t *testing.T) {
	store := newTestJobStore(t)
	const id = "20260518T120017-0123456789ab"
	monitor := &monitorJob{ID: id, Status: "recoverable_failure"}
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, Status: "recoverable_failure", Monitor: monitor}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, monitorSessionID, "recoverable_failure"); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	started := time.Now()
	err := AttachJob(context.Background(), store, id, &out)
	if err == nil || !strings.Contains(err.Error(), "resumable") || !strings.Contains(err.Error(), "factory monitor reset "+id) {
		t.Fatalf("recoverable attach error = %v, want clear reset guidance", err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("attach waited %s for an exited recoverable worker", elapsed)
	}
	if isTerminalStatus("recoverable_failure") {
		t.Fatal("recoverable_failure must remain resumable to generic lifecycle observers so reset can proceed")
	}
}

func TestJobLogsFollowRecoverableFailureReturnsResetGuidance(t *testing.T) {
	state := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	const id = "20260518T120015-0123456789ab"
	monitor := &monitorJob{ID: id, Status: "recoverable_failure"}
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, Status: "recoverable_failure", Monitor: monitor}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, monitorSessionID, "recoverable_failure"); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendSessionLog(id, monitorSessionID, []byte("snapshot failed\n")); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	started := time.Now()
	err = JobCommandContext(context.Background(), []string{"logs", id, "--follow"}, Config{StateDir: state}, t.TempDir(), nil, &out)
	if err == nil || !strings.Contains(err.Error(), "resumable") || !strings.Contains(err.Error(), "factory monitor reset "+id) {
		t.Fatalf("recoverable logs follow error = %v, want clear reset guidance", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("logs follow waited %s for an exited recoverable worker", elapsed)
	}
	job, err := store.GetJob(id)
	if err != nil || job.Status != "recoverable_failure" || isTerminalStatus(job.Status) {
		t.Fatalf("follow changed resumable lifecycle: job=%+v err=%v", job, err)
	}
}

func TestFollowLogDrainsBytesAppendedByTerminalCallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "terminal.log")
	if err := os.WriteFile(path, []byte("before\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	called := false
	var out bytes.Buffer
	err := followLog(context.Background(), path, func() (bool, error) {
		if !called {
			called = true
			file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
			if err != nil {
				return false, err
			}
			_, writeErr := file.WriteString("final bytes\n")
			closeErr := file.Close()
			if writeErr != nil {
				return false, writeErr
			}
			if closeErr != nil {
				return false, closeErr
			}
		}
		return true, nil
	}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "before\nfinal bytes\n" {
		t.Fatalf("terminal log output = %q, want final appended bytes", got)
	}
}

func TestJobLogsFollowStreamsUntilPersistedTerminalStatus(t *testing.T) {
	state := t.TempDir()
	root, err := JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "follow-logs", Type: implementationJobType, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJobLog("follow-logs"); err != nil {
		t.Fatal(err)
	}
	path, err := store.JobLogPath("follow-logs")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(250 * time.Millisecond)
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return
		}
		_, _ = file.WriteString("second\n")
		_ = file.Close()
		_, _ = store.UpdateJob("follow-logs", func(job *JobRecord) error { job.Status = "complete"; return nil })
	}()
	var out bytes.Buffer
	if err := JobCommandContext(context.Background(), []string{"logs", "follow-logs", "--follow"}, Config{StateDir: state}, t.TempDir(), nil, &out); err != nil {
		t.Fatalf("logs follow: %v", err)
	}
	if got := out.String(); got != "first\nsecond\n" {
		t.Fatalf("followed log output = %q, want each appended line once", got)
	}
}
