package factory

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestJobTraceCountsStatusAttemptsAndTracksOnlyLivePiSubprocesses(t *testing.T) {
	state := t.TempDir()
	root, err := JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "trace-job", Type: implementationJobType, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession("trace-job", "workflow", "running"); err != nil {
		t.Fatal(err)
	}
	for _, event := range []SessionEvent{
		{Type: "status.started", Invocation: "one"},
		{Type: "status.completed", Invocation: "one", Outcome: "error"},
		{Type: "status.started", Invocation: "two"},
		{Type: "status.completed", Invocation: "two", Outcome: "timeout"},
		{Type: "status.started", Invocation: "three"},
		{Type: "status.completed", Invocation: "three", Outcome: "success", Summary: "checking tests"},
	} {
		if err := store.AppendSessionEventDetails("trace-job", "workflow", event); err != nil {
			t.Fatal(err)
		}
	}
	observer := jobProcessObserver(store, "trace-job", "workflow")
	observer("/usr/bin/pi", 0, true)
	observer("custom-agent", 12345, true)
	observer("pi", 0, true)
	observer("pi", 0, false)
	observer("pi", currentProcessID(), true)
	if got := summarizeJobTrace(store, JobRecord{ID: "trace-job", Type: implementationJobType}).ActivePi; got != 0 {
		t.Fatalf("arbitrary current process counted as Pi: %d", got)
	}
	if err := store.AppendSessionEventDetails("trace-job", "workflow", SessionEvent{Type: "status.started", Invocation: "four"}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendSessionEventDetails("trace-job", "workflow", SessionEvent{Type: "status.completed", Invocation: "four", Outcome: "canceled"}); err != nil {
		t.Fatal(err)
	}
	summary := summarizeJobTrace(store, JobRecord{ID: "trace-job", Type: implementationJobType})
	if summary.StatusCalls != 4 {
		t.Fatalf("status invocation count = %d, want 4", summary.StatusCalls)
	}
	if summary.ActivePi != 0 {
		t.Fatalf("arbitrary current process counted as Pi: %d", summary.ActivePi)
	}
	if !strings.Contains(summary.Activity, "status.completed") {
		t.Fatalf("latest activity = %q, want latest recorded status result", summary.Activity)
	}
	var cli bytes.Buffer
	if err := JobCommand([]string{"get", "trace-job"}, Config{StateDir: state}, t.TempDir(), nil, &cli); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cli.String(), "Status calls: 4") || !strings.Contains(cli.String(), "direct process observations only") {
		t.Fatalf("job get omitted trace or observation caveat: %q", cli.String())
	}
	cli.Reset()
	if err := JobCommand([]string{"list"}, Config{StateDir: state}, t.TempDir(), nil, &cli); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cli.String(), "STATUS CALLS") || !strings.Contains(cli.String(), " 4 ") {
		t.Fatalf("job list omitted status invocation count: %q", cli.String())
	}
	observer("pi", currentProcessID(), false)
	cmd := exec.CommandContext(context.Background(), "sleep", "30")
	configureProcessCancellation(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	observer("pi", cmd.Process.Pid, true)
	if got := summarizeJobTrace(store, JobRecord{ID: "trace-job", Type: implementationJobType}).ActivePi; got != 1 {
		t.Fatalf("live direct Pi process in its own process group counted %d times, want one", got)
	}
	observer("pi", cmd.Process.Pid, false)
	if got := summarizeJobTrace(store, JobRecord{ID: "trace-job", Type: implementationJobType}).ActivePi; got != 0 {
		t.Fatalf("completed direct subprocess still counted active: %d", got)
	}
}

func TestStatusLifecyclePersistsFailureTimeoutAndCancellationWithoutFailingPrimary(t *testing.T) {
	base := t.TempDir()
	store, err := NewJobStore(filepath.Join(base, "jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "status-outcomes", Type: implementationJobType, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession("status-outcomes", "workflow", "running"); err != nil {
		t.Fatal(err)
	}
	startedPrimary := make(chan struct{})
	callsDone := make(chan struct{})
	calls := 0
	workflow := Workflow{
		Agent:  &blockingStatusTestAgent{started: startedPrimary, release: make(chan struct{})},
		Config: Config{StateDir: filepath.Join(base, "state")}, Out: io.Discard, Workdir: t.TempDir(),
		Stages: []string{"implement"}, statusInterval: time.Millisecond,
		Observer: statusJobObserver{JobSessionObserver: JobSessionObserver{Store: store, JobID: "status-outcomes", SessionID: "workflow"}, store: store, jobID: "status-outcomes", sessionID: "workflow"},
		statusCall: func(ctx context.Context, _, _, _, _ string) (string, error) {
			calls++
			switch calls {
			case 1:
				return "ignored", errors.New("status unavailable")
			case 2:
				return "", context.DeadlineExceeded
			default:
				close(callsDone)
				<-ctx.Done()
				return "", ctx.Err()
			}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- workflow.RunContext(ctx, "trace status outcomes") }()
	select {
	case <-startedPrimary:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("primary did not start")
	}
	select {
	case <-callsDone:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("status calls did not reach cancellation case")
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("primary workflow cancellation was not returned")
	}
	events, err := store.SessionEvents("status-outcomes", "workflow")
	if err != nil {
		t.Fatal(err)
	}
	outcomes := map[string]int{}
	for _, event := range events {
		if event.Type == "status.completed" {
			outcomes[event.Outcome]++
			if event.Outcome != "success" && event.Summary != "" {
				t.Fatalf("unsuccessful status event leaked a summary: %+v", event)
			}
		}
	}
	for _, outcome := range []string{"error", "timeout", "canceled"} {
		if outcomes[outcome] != 1 {
			t.Fatalf("status outcome %q occurred %d times; all outcomes=%v", outcome, outcomes[outcome], outcomes)
		}
	}
	if got := summarizeJobTrace(store, JobRecord{ID: "status-outcomes", Type: implementationJobType}).StatusCalls; got != 3 {
		t.Fatalf("status call count = %d, want three attempts", got)
	}
}

func currentProcessID() int { return os.Getpid() }

func TestLatestActivityTruncationPreservesUTF8(t *testing.T) {
	base := t.TempDir()
	store, err := NewJobStore(filepath.Join(base, "jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "utf8-activity", Type: implementationJobType, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession("utf8-activity", "workflow", "running"); err != nil {
		t.Fatal(err)
	}
	message := strings.Repeat("a", 101) + "é" + strings.Repeat("b", 20)
	if err := store.AppendSessionEventDetails("utf8-activity", "workflow", SessionEvent{Type: "stage.updated", Message: message}); err != nil {
		t.Fatal(err)
	}
	activity := summarizeJobTrace(store, JobRecord{ID: "utf8-activity", Type: implementationJobType}).Activity
	if !utf8.ValidString(activity) {
		t.Fatalf("latest activity is invalid UTF-8 after truncation: %q", activity)
	}
	if !strings.HasSuffix(activity, "… ("+time.Now().Local().Format("15:04:05")+")") {
		t.Fatalf("truncated latest activity lost its marker or timestamp: %q", activity)
	}
}

func TestRunnerReportsPiProcessStartAndCompletion(t *testing.T) {
	dir := t.TempDir()
	pi := filepath.Join(dir, "pi")
	if err := os.WriteFile(pi, []byte("#!/bin/sh\nexec sleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(filepath.Join(dir, "jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "runner-pi", Type: implementationJobType, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession("runner-pi", "workflow", "running"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = (Runner{Config: Config{Command: pi, Args: []string{"{task}", "{system_prompt}"}}, ProcessObserver: jobProcessObserver(store, "runner-pi", "workflow")}).RunContext(ctx, "implement", "prompt", "task", dir, filepath.Join(dir, "agent.log"))
	if err == nil {
		t.Fatal("expected configured Pi subprocess cancellation")
	}
	events, err := store.SessionEvents("runner-pi", "workflow")
	if err != nil {
		t.Fatal(err)
	}
	started, completed := false, false
	for _, event := range events {
		if event.Type == "agent.started" && event.Command == "pi" && event.PID > 0 {
			started = true
		}
		if event.Type == "agent.completed" && event.Command == "pi" && event.PID > 0 {
			completed = true
		}
	}
	if !started || !completed {
		t.Fatalf("Pi process lifecycle events missing: %+v", events)
	}
	if got := summarizeJobTrace(store, JobRecord{ID: "runner-pi", Type: implementationJobType}).ActivePi; got != 0 {
		t.Fatalf("finished Pi subprocess remains active in summary: %d", got)
	}
}

func TestUUIDv4AndLegacyIdentifiersRemainValid(t *testing.T) {
	id, err := newUUIDv4()
	if err != nil {
		t.Fatal(err)
	}
	if !monitorIDPattern.MatchString(id) {
		t.Fatalf("generated UUIDv4 %q does not match monitor ID format", id)
	}
	for _, value := range []string{id, "20260518T120005-0123456789ab", "legacy-job_01"} {
		if err := validateStoredID(value); err != nil {
			t.Errorf("stored ID %q rejected: %v", value, err)
		}
	}
	if !monitorIDPattern.MatchString("20260518T120005-0123456789ab") {
		t.Fatal("legacy monitor ID no longer matches")
	}
	if monitorIDPattern.MatchString("../escape") || monitorIDPattern.MatchString("b0d9a6b6-675c-411b-72f6-1b5050b4fd91") {
		t.Fatal("monitor ID validator accepted traversal or a non-v4 UUID")
	}
	if !strings.Contains(id, "-") || len(id) != 36 {
		t.Fatalf("malformed UUIDv4: %q", id)
	}
}
