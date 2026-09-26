package factory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	store, err := NewJobStore(filepath.Join(state, "factory", "jobs", "v2"))
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

func TestBabysitEventAppendFailurePreventsLegacyAndV2Updates(t *testing.T) {
	state := t.TempDir()
	legacyDir := filepath.Join(state, "factory", "jobs", "20260518T120016-0123456789ab")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	id := filepath.Base(legacyDir)
	target := t.TempDir()
	legacy := &babysitJob{ID: id, RepoRoot: target, Status: "running", LastEvent: "previous event"}
	if err := writeBabysitJob(legacyDir, legacy); err != nil {
		t.Fatal(err)
	}
	cfg := Config{StateDir: state}
	if err := createMonitorJobRecord(cfg, legacy); err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(filepath.Join(state, "factory", "jobs", "v2"))
	if err != nil {
		t.Fatal(err)
	}
	oldOpen := babysitLogOpenFile
	babysitLogOpenFile = func(string, int, os.FileMode) (*os.File, error) {
		return nil, errors.New("injected actions log failure")
	}
	t.Cleanup(func() { babysitLogOpenFile = oldOpen })

	legacy.Status = "completed"
	err = babysitEvent(legacyDir, legacy, "PR merged")
	if err == nil || !strings.Contains(err.Error(), "injected actions log failure") {
		t.Fatalf("babysitEvent error = %v, want append failure", err)
	}
	persisted, err := readBabysitJob(legacyDir)
	if err != nil || persisted.Status != "running" || persisted.LastEvent != "previous event" {
		t.Fatalf("legacy state changed after log append failure: %+v err=%v", persisted, err)
	}
	v2, err := store.GetJob(id)
	if err != nil || v2.Status != "queued" {
		t.Fatalf("v2 status changed after log append failure: %+v err=%v", v2, err)
	}
	session, err := store.GetSession(id, monitorSessionID)
	if err != nil || session.Status != "queued" {
		t.Fatalf("v2 session changed after log append failure: %+v err=%v", session, err)
	}
	events, err := store.SessionEvents(id, monitorSessionID)
	if err != nil || len(events) != 0 {
		t.Fatalf("v2 event persisted after append failure: %+v err=%v", events, err)
	}
}

func TestAttachMonitorDrainsTerminalLegacyEventBeforeStatusTransition(t *testing.T) {
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
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TargetPath: target, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, monitorSessionID, "running"); err != nil {
		t.Fatal(err)
	}
	legacyDir := filepath.Join(filepath.Dir(store.Root()), id)
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := &babysitJob{ID: id, RepoRoot: target, Status: "running"}
	if err := writeBabysitJob(legacyDir, legacy); err != nil {
		t.Fatal(err)
	}

	const finalMessage = "PR is merged; monitoring stopped."
	legacy.Status = "completed"
	if err := babysitEvent(legacyDir, legacy, finalMessage); err != nil {
		t.Fatalf("record terminal babysit event: %v", err)
	}

	job, err := store.GetJob(id)
	if err != nil || job.Status != "merged" {
		t.Fatalf("terminal monitor status = %q, err=%v", job.Status, err)
	}
	session, err := store.GetSession(id, monitorSessionID)
	if err != nil || session.Status != "merged" {
		t.Fatalf("terminal monitor session status = %q, err=%v", session.Status, err)
	}
	events, err := store.SessionEvents(id, monitorSessionID)
	if err != nil || len(events) != 1 || events[0].Message != finalMessage {
		t.Fatalf("persisted terminal monitor event = %+v, err=%v", events, err)
	}
	if !events[0].At.Before(job.EndedAt) {
		t.Fatalf("terminal event time %s was not persisted before job terminal transition %s", events[0].At, job.EndedAt)
	}

	var output bytes.Buffer
	if err := AttachJob(context.Background(), store, id, &output); err != nil {
		t.Fatalf("attach completed monitor: %v", err)
	}
	if !strings.Contains(output.String(), finalMessage) {
		t.Fatalf("attach omitted final legacy event %q: %q", finalMessage, output.String())
	}
}

func TestAttachMonitorUsesMonitorSessionLog(t *testing.T) {
	state := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "jobs", "v2"))
	if err != nil {
		t.Fatal(err)
	}
	const id = "20260518T120013-0123456789ab"
	target := t.TempDir()
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TargetPath: target, Status: "complete"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, monitorSessionID, "complete"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJobLog(id); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendSessionLog(id, monitorSessionID, []byte("monitor transcript\n")); err != nil {
		t.Fatal(err)
	}
	legacyDir := filepath.Join(filepath.Dir(store.Root()), id)
	if err := os.Mkdir(legacyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := &babysitJob{ID: id, RepoRoot: target, Status: "completed"}
	if err := writeBabysitJob(legacyDir, legacy); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "actions.log"), []byte("legacy actions transcript\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	jobLog, err := store.JobLogPath(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(jobLog, []byte("wrong worker log\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := AttachJob(context.Background(), store, id, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "legacy actions transcript\n" {
		t.Fatalf("monitor attach output = %q, want validated legacy actions transcript", got)
	}
	out.Reset()
	if err := JobCommand([]string{"logs", id}, Config{StateDir: state}, t.TempDir(), nil, &out); err != nil {
		t.Fatalf("monitor logs command: %v", err)
	}
	if got := out.String(); got != "legacy actions transcript\n" {
		t.Fatalf("monitor logs output = %q, want same validated legacy transcript as attach", got)
	}
	out.Reset()
	if err := JobCommand([]string{"logs", id, "--follow"}, Config{StateDir: state}, t.TempDir(), nil, &out); err != nil {
		t.Fatalf("monitor logs follow: %v", err)
	}
	if got := out.String(); got != "legacy actions transcript\n" {
		t.Fatalf("monitor logs follow output = %q, want same validated legacy transcript as attach", got)
	}
}

func TestAttachMonitorFallsBackAndRejectsUnsafeLegacyActionLog(t *testing.T) {
	for _, test := range []struct {
		name         string
		setup        func(*testing.T, string)
		wantFallback bool
	}{
		{name: "missing legacy directory", wantFallback: true},
		{name: "missing actions log", setup: func(t *testing.T, dir string) {
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
		}, wantFallback: true},
		{name: "symlink actions log", setup: func(t *testing.T, dir string) {
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			outside := filepath.Join(t.TempDir(), "outside.log")
			if err := os.WriteFile(outside, []byte("outside\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(dir, "actions.log")); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestJobStore(t)
			const id = "20260518T120014-0123456789ab"
			target := t.TempDir()
			if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TargetPath: target, Status: "complete"}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.CreateSession(id, monitorSessionID, "complete"); err != nil {
				t.Fatal(err)
			}
			if err := store.AppendSessionLog(id, monitorSessionID, []byte("v2 session fallback\n")); err != nil {
				t.Fatal(err)
			}
			legacyDir := filepath.Join(filepath.Dir(store.Root()), id)
			if test.setup != nil {
				test.setup(t, legacyDir)
			}
			var out bytes.Buffer
			if test.setup != nil {
				if _, err := os.Lstat(filepath.Join(legacyDir, "actions.log")); errors.Is(err, os.ErrNotExist) {
					legacy := &babysitJob{ID: id, RepoRoot: target, Status: "completed"}
					if err := writeBabysitJob(legacyDir, legacy); err != nil {
						t.Fatal(err)
					}
				}
			}
			err := AttachJob(context.Background(), store, id, &out)
			if test.wantFallback {
				if err != nil || out.String() != "v2 session fallback\n" {
					t.Fatalf("attach fallback output=%q err=%v", out.String(), err)
				}
			} else if err == nil {
				t.Fatalf("attach with unsafe legacy log succeeded: %q", out.String())
			} else {
				out.Reset()
				state := filepath.Dir(filepath.Dir(filepath.Dir(store.Root())))
				if err := JobCommand([]string{"logs", id}, Config{StateDir: state}, t.TempDir(), nil, &out); err == nil {
					t.Fatalf("logs command accepted unsafe legacy log: %q", out.String())
				}
			}
		})
	}
}

func TestAttachRecoverableFailureReturnsWhenMonitorWorkerExited(t *testing.T) {
	store := newTestJobStore(t)
	const id = "20260518T120017-0123456789ab"
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, Status: "recoverable_failure"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, monitorSessionID, "recoverable_failure"); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	started := time.Now()
	err := AttachJob(context.Background(), store, id, &out)
	if err == nil || !strings.Contains(err.Error(), "resumable") || !strings.Contains(err.Error(), "factory babysit reset "+id) {
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
	store, err := NewJobStore(filepath.Join(state, "factory", "jobs", "v2"))
	if err != nil {
		t.Fatal(err)
	}
	const id = "20260518T120015-0123456789ab"
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, Status: "recoverable_failure"}); err != nil {
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
	if err == nil || !strings.Contains(err.Error(), "resumable") || !strings.Contains(err.Error(), "factory babysit reset "+id) {
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
