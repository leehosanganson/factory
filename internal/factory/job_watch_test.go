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
	if err := store.AppendSessionEventDetails("changing", "workflow", SessionEvent{Type: "stage.started", Message: "stage=implement /private/01-implement.log", At: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendSessionEventDetails("changing", "workflow", SessionEvent{Type: "stage.status", Message: "stage=implement 2026-01-02T03:04:05Z checking focused tests\x1b[31m", At: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}

	var out safeWatchBuffer
	done := make(chan error, 1)
	go func() {
		done <- watchJobs(context.Background(), store, []string{"changing"}, &out, false, 5*time.Millisecond)
	}()
	waitFor(t, time.Second, func() bool {
		return strings.Contains(out.String(), "Status: running") && strings.Contains(out.String(), "implement · checking focused tests")
	})
	if err := store.AppendSessionEventDetails("changing", "workflow", SessionEvent{Type: "stage.status", Message: "stage=implement 2026-01-02T03:04:06Z running focused verifications", At: time.Now().UTC()}); err != nil {
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
	if !strings.Contains(text, "Status: complete") || !strings.Contains(text, "implement · running focused verifications") {
		t.Fatalf("watch did not show current stage and safe persisted progress: %q", text)
	}
	if strings.Contains(text, "01-implement.log") || strings.Contains(text, "2026-01-02") || strings.Contains(text, "stage.status") || strings.ContainsAny(text, "\x1b\r") {
		t.Fatalf("watch exposed event internals or unsafe data: %q", text)
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

func TestJobWatchNextActionHintsArePhaseAwareAndDoNotEchoTaskContent(t *testing.T) {
	cases := []struct {
		name   string
		job    JobRecord
		want   string
		secret string
	}{
		{
			name:   "pending monitor proposal",
			job:    JobRecord{ID: "pending", Type: monitorJobType, Status: "running", TaskDescription: "secret task", Monitor: &monitorJob{Status: "running", Phase: "approval_pending", PendingSignature: "sig", Proposal: "secret proposal"}},
			want:   "factory monitor get pending --details`, then `factory monitor approve pending` or `factory monitor reject pending",
			secret: "secret",
		},
		{
			name: "recoverable monitor",
			job:  JobRecord{ID: "recover", Type: monitorJobType, Status: "recoverable_failure", Monitor: &monitorJob{Status: "recoverable_failure"}},
			want: "confirming the monitor worker stopped",
		},
		{
			name: "failed implementation",
			job:  JobRecord{ID: "failed", Type: implementationJobType, Status: "failed"},
			want: "factory job get failed --details` and `factory job logs failed",
		},
		{
			name: "completed implementation",
			job:  JobRecord{ID: "done", Type: implementationJobType, Status: "complete", Worktree: "/tmp/worktree"},
			want: "review the recorded worktree diff",
		},
		{
			name: "active implementation",
			job:  JobRecord{ID: "active", Type: implementationJobType, Status: "running"},
			want: "No action needed; let the job continue",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hint := jobWatchNextAction(tc.job)
			if !strings.Contains(hint, tc.want) {
				t.Fatalf("next action = %q, want %q", hint, tc.want)
			}
			if tc.secret != "" && strings.Contains(hint, tc.secret) {
				t.Fatalf("next action exposed task/proposal content: %q", hint)
			}
		})
	}
}

func TestJobWatchIncludesMonitorPhaseFreshnessAndBoundedEvents(t *testing.T) {
	store := newWatchTestStore(t)
	checkedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	monitor := &monitorJob{
		ID: "monitor", Status: "running", Phase: "approval_pending", LatestCheckAt: checkedAt,
		LatestCheckResult: "1 passed, 0 failed, 1 pending",
		RecentEvents:      []monitorTraceEvent{{At: checkedAt, Phase: "approval_pending", Message: "Agent requested approval"}},
	}
	if err := store.CreateJob(JobRecord{ID: "monitor", Type: monitorJobType, TargetPath: t.TempDir(), Status: "running", Monitor: monitor}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession("monitor", monitorSessionID, "running"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var out safeWatchBuffer
	done := make(chan error, 1)
	go func() { done <- watchJobs(ctx, store, []string{"monitor"}, &out, false, time.Millisecond) }()
	waitFor(t, time.Second, func() bool { return strings.Contains(out.String(), "Agent requested approval") })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{"Monitor phase: approval_pending", "Latest successful PR/check query:", checkedAt.Format(time.RFC3339), "1 passed, 0 failed, 1 pending", "Recent monitor events:", "Agent requested approval"} {
		if !strings.Contains(text, want) {
			t.Errorf("watch output missing %q: %s", want, text)
		}
	}
}

func TestJobWatchMonitorRecentEventWriterErrorIsReturned(t *testing.T) {
	job := watchedJob{ID: "monitor", Status: "running", Phase: "polling", RecentEvents: []monitorTraceEvent{{Phase: "polling", Message: "query complete"}}}
	if err := writeJobWatchSnapshot(&failingMonitorWriter{}, []watchedJob{job}, false); err == nil {
		t.Fatal("job watch swallowed recent-event output error")
	}
}

func TestJobWatchPlainAndTerminalSnapshots(t *testing.T) {
	jobs := []watchedJob{{ID: "job-1", Status: "running", Activity: "stage.started: work", NextAction: "No action needed; let the job continue."}}
	for _, tc := range []struct {
		name     string
		terminal bool
		contains string
		omits    string
	}{
		{name: "plain", contains: "Next action: No action needed", omits: "\033[H"},
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

func (b *safeWatchBuffer) WriteString(s string) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.WriteString(s)
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
