package factory

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestJobWatchRequiresIDsAndReportsMissingIDs(t *testing.T) {
	state := t.TempDir()
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "empty", args: []string{"watch"}, want: "usage: factory job watch <id>..."},
		{name: "missing", args: []string{"watch", "does-not-exist"}, want: "watch job does-not-exist"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := JobCommand(tc.args, Config{StateDir: state}, t.TempDir(), nil, &out)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("watch error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestJobWatchDeduplicatesPreservingOrderAndStopsWhenAllTerminal(t *testing.T) {
	state := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	createWatchJob(t, store, "first", "complete")
	createWatchJob(t, store, "second", "failed")
	var out bytes.Buffer
	if err := JobCommandContext(context.Background(), []string{"watch", "second", "first", "second"}, Config{StateDir: state}, t.TempDir(), nil, &out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if strings.Count(text, "Job second") != 1 || strings.Count(text, "Job first") != 1 {
		t.Fatalf("watch output did not deduplicate IDs: %q", text)
	}
	if strings.Index(text, "Job second") > strings.Index(text, "Job first") {
		t.Fatalf("watch output lost requested order: %q", text)
	}
}

func TestJobWatchRefreshesStatusAndActivityWithoutPrintingLogs(t *testing.T) {
	store := newWatchTestStore(t)
	createWatchJob(t, store, "changing", "running")
	logPath, err := store.JobLogPath("changing")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("SECRET_WORKER_LOG_CONTENT\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sessionPath, err := store.SessionLogPath("changing", "workflow")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sessionPath, []byte("SECRET_SESSION_LOG_CONTENT\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var out safeWatchBuffer
	done := make(chan error, 1)
	go func() {
		done <- watchJobs(context.Background(), store, []string{"changing"}, &out, false, 5*time.Millisecond)
	}()
	waitFor(t, time.Second, func() bool { return strings.Contains(out.String(), "Status: running") })
	if err := store.AppendSessionEventDetails("changing", "workflow", SessionEvent{Type: "stage.completed", Message: "new latest activity", At: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdateJob("changing", func(job *JobRecord) error { job.Status = "complete"; return nil }); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("watch did not stop after observing terminal status")
	}
	text := out.String()
	if !strings.Contains(text, "Status: complete") || !strings.Contains(text, "stage.completed: new latest activity") {
		t.Fatalf("watch did not refresh selected job status/activity: %q", text)
	}
	if strings.Contains(text, "SECRET_WORKER_LOG_CONTENT") || strings.Contains(text, "SECRET_SESSION_LOG_CONTENT") {
		t.Fatalf("watch leaked worker/session logs: %q", text)
	}
}

func TestJobWatchCancellationDoesNotStopWorker(t *testing.T) {
	store := newWatchTestStore(t)
	createWatchJob(t, store, "still-running", "running")
	ctx, cancel := context.WithCancel(context.Background())
	var out safeWatchBuffer
	done := make(chan error, 1)
	go func() { done <- watchJobs(ctx, store, []string{"still-running"}, &out, false, time.Hour) }()
	waitFor(t, time.Second, func() bool { return strings.Contains(out.String(), "Job still-running") })
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("watch did not exit after context cancellation")
	}
	job, err := store.GetJob("still-running")
	if err != nil || job.Status != "running" {
		t.Fatalf("watch cancellation changed worker status: job=%+v err=%v", job, err)
	}
	if store.StopRequested("still-running") {
		t.Fatal("watch cancellation requested worker stop")
	}
	if _, err := os.Stat(filepath.Join(store.Root(), "still-running", "cancel.request")); !os.IsNotExist(err) {
		t.Fatalf("watch created cancellation request: stat err=%v", err)
	}
}

func TestJobWatchPlainAndTerminalSnapshots(t *testing.T) {
	jobs := []watchedJob{{ID: "job-1", Status: "running", Activity: "stage.started: work"}}
	for _, tc := range []struct {
		name     string
		terminal bool
		contains string
		omits    string
	}{
		{name: "plain", contains: "Selected jobs", omits: "\033[H"},
		{name: "terminal", terminal: true, contains: "\033[H\033[2J", omits: "\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := writeJobWatchSnapshot(&out, jobs, tc.terminal); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), tc.contains) || strings.Contains(out.String(), tc.omits) {
				t.Fatalf("snapshot=%q; contains %q and omits %q", out.String(), tc.contains, tc.omits)
			}
		})
	}
}

type safeWatchBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *safeWatchBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(data)
}

func (b *safeWatchBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

func newWatchTestStore(t *testing.T) *JobStore {
	t.Helper()
	store, err := NewJobStore(filepath.Join(t.TempDir(), "jobs"))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func createWatchJob(t *testing.T, store *JobStore, id, status string) {
	t.Helper()
	if err := store.CreateJob(JobRecord{ID: id, Type: implementationJobType, TargetPath: t.TempDir(), Status: status}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, "workflow", status); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJobLog(id); err != nil {
		t.Fatal(err)
	}
}
