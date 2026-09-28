package factory

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

type blockingStatusTestAgent struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (a *blockingStatusTestAgent) Run(string, string, string, string, string) error {
	return errors.New("context-aware execution required")
}

func (a *blockingStatusTestAgent) RunWithContext(ctx context.Context, _, _, _, _, logPath string) error {
	a.once.Do(func() { close(a.started) })
	select {
	case <-a.release:
		return os.WriteFile(logPath, []byte("primary finished\n"), 0o600)
	case <-ctx.Done():
		return ctx.Err()
	}
}

type fakeAgent struct {
	outputs map[string][]string
	calls   []string
	tasks   []string
}

func (f *fakeAgent) Run(stage, prompt, task, workdir, logPath string) error {
	_, err := f.run(stage, task, logPath)
	return err
}

func (f *fakeAgent) RunWithContext(_ context.Context, stage, prompt, task, workdir, logPath string) error {
	_, err := f.run(stage, task, logPath)
	return err
}

func (f *fakeAgent) RunWithOutputContext(_ context.Context, stage, prompt, task, workdir, logPath string) (string, error) {
	return f.run(stage, task, logPath)
}

func (f *fakeAgent) run(stage, task, logPath string) (string, error) {
	f.calls = append(f.calls, stage)
	f.tasks = append(f.tasks, task)
	queue := f.outputs[stage]
	output := "PASS\n"
	if len(queue) > 0 {
		output = queue[0]
		f.outputs[stage] = queue[1:]
	}
	if err := os.WriteFile(logPath, []byte(output), 0o600); err != nil {
		return "", err
	}
	if strings.HasPrefix(output, "ERROR:") {
		return output, errors.New(strings.TrimPrefix(output, "ERROR:"))
	}
	return output, nil
}

func TestSecondaryStatusRunsDuringActiveStageAndPersistsSanitizedUpdate(t *testing.T) {
	base := t.TempDir()
	store, err := NewJobStore(filepath.Join(base, "jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "status-job", Type: implementationJobType, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession("status-job", "workflow", "running"); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	statusCalled := make(chan struct{})
	agent := &blockingStatusTestAgent{started: started, release: make(chan struct{})}
	var output strings.Builder
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: filepath.Join(base, "state")}, Out: &output, Workdir: t.TempDir(),
		Stages: []string{"implement"}, statusInterval: 100 * time.Millisecond,
		Observer: JobSessionObserver{Store: store, JobID: "status-job", SessionID: "workflow"},
		statusCall: func(ctx context.Context, _, _, _, _ string) (string, error) {
			select {
			case <-started:
			case <-ctx.Done():
				return "", ctx.Err()
			}
			select {
			case <-statusCalled:
			default:
				close(statusCalled)
			}
			return " progress\x1b[31m status " + strings.Repeat("x", secondaryStatusLimit+10), nil
		},
	}
	workflowDone := make(chan error, 1)
	go func() { workflowDone <- workflow.Run("task") }()
	select {
	case <-statusCalled:
	case <-time.After(time.Second):
		t.Fatal("secondary status call did not run during the active stage")
	}
	close(agent.release)
	if err := <-workflowDone; err != nil {
		t.Fatal(err)
	}
	events, err := store.SessionEvents("status-job", "workflow")
	if err != nil {
		t.Fatal(err)
	}
	statusStarted, statusCompleted := false, false
	for _, event := range events {
		if event.Type == "status.started" {
			statusStarted = true
		}
		if event.Type == "status.completed" && event.Outcome == "success" && event.Summary != "" {
			statusCompleted = true
			if strings.ContainsAny(event.Summary, "\x1b\r\n") || len(event.Summary) > secondaryStatusLimit {
				t.Fatalf("status summary was not bounded/sanitized: %q", event.Summary)
			}
		}
	}
	if !statusStarted || !statusCompleted {
		t.Fatalf("status call lifecycle was not persisted: events=%+v output=%q", events, output.String())
	}
	if got := summarizeJobTrace(store, JobRecord{ID: "status-job", Type: implementationJobType}).StatusCalls; got != 1 {
		t.Fatalf("status invocation count = %d, want one", got)
	}
}

func TestSecondaryStatusCancellationStopsObserverAndPrimary(t *testing.T) {
	base := t.TempDir()
	primaryStarted := make(chan struct{})
	observerStarted := make(chan struct{})
	observerCanceled := make(chan struct{})
	agent := &blockingStatusTestAgent{started: primaryStarted, release: make(chan struct{})}
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: filepath.Join(base, "state")}, Out: io.Discard, Workdir: t.TempDir(),
		Stages: []string{"implement"}, statusInterval: time.Millisecond,
		statusCall: func(ctx context.Context, _, _, _, _ string) (string, error) {
			close(observerStarted)
			<-ctx.Done()
			close(observerCanceled)
			return "", ctx.Err()
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	workflowDone := make(chan error, 1)
	go func() { workflowDone <- workflow.RunContext(ctx, "cancel task") }()
	select {
	case <-primaryStarted:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("primary stage was not started")
	}
	select {
	case <-observerStarted:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("observer was not started")
	}
	cancel()
	select {
	case err := <-workflowDone:
		if err == nil || !strings.Contains(err.Error(), "canceled") {
			t.Fatalf("workflow cancellation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("workflow did not join canceled status invocation")
	}
	select {
	case <-observerCanceled:
	case <-time.After(time.Second):
		t.Fatal("status invocation did not receive cancellation")
	}
}

func TestSecondaryStatusErrorsAreNonFatalAndDoesNotStartAfterPrimary(t *testing.T) {
	base := t.TempDir()
	calls := 0
	workflow := Workflow{
		Agent: &fakeAgent{outputs: map[string][]string{}}, Config: Config{StateDir: filepath.Join(base, "state")}, Out: io.Discard, Workdir: t.TempDir(),
		Stages: []string{"implement"}, statusInterval: time.Millisecond,
		statusCall: func(context.Context, string, string, string, string) (string, error) {
			calls++
			return "ignored", errors.New("observer failed")
		},
	}
	if err := workflow.Run("quick task"); err != nil {
		t.Fatalf("status failure affected primary stage: %v", err)
	}
	if calls != 0 {
		t.Fatalf("status invocation launched after the short primary stage finished: %d", calls)
	}
}

func TestWorkflowOrderGatesAndOutOfTreePersistence(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	target := t.TempDir()
	agent := &fakeAgent{outputs: map[string][]string{}}
	var output strings.Builder
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader("yes\nyes\nyes\nyes\n"), Out: &output, Workdir: target, Gate: true}
	if err := workflow.Run("make a useful change"); err != nil {
		t.Fatal(err)
	}
	want := []string{"requirements", "implement", "review", "document"}
	if strings.Join(agent.calls, ",") != strings.Join(want, ",") {
		t.Fatalf("workflow calls = %v, want %v", agent.calls, want)
	}
	entries, err := os.ReadDir(filepath.Join(stateDir, "runs"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("run state not found outside target: entries=%v err=%v", entries, err)
	}
	runDir := filepath.Join(stateDir, "runs", entries[0].Name())
	stateBytes, err := os.ReadFile(filepath.Join(runDir, "state.json"))
	if err != nil || !strings.Contains(string(stateBytes), `"status": "complete"`) {
		t.Fatalf("state is not complete: %s (%v)", stateBytes, err)
	}
	if entries, err := os.ReadDir(target); err != nil || len(entries) != 0 {
		t.Fatalf("workflow wrote to target directory: %v, %v", entries, err)
	}
}

func TestWorkflowRejectsNestedStateBeforeCreatingRun(t *testing.T) {
	target := t.TempDir()
	state := filepath.Join(target, "workflow-state")
	agent := &fakeAgent{outputs: map[string][]string{}}
	var output strings.Builder
	workflow := Workflow{Agent: agent, Config: Config{StateDir: state}, In: strings.NewReader(""), Out: &output, Workdir: target}
	if err := workflow.Run("must not start"); err == nil || !strings.Contains(err.Error(), "must be outside target") {
		t.Fatalf("nested state error = %v, want containment rejection", err)
	}
	if len(agent.calls) != 0 {
		t.Fatalf("agents started before rejecting nested state: %v", agent.calls)
	}
	if _, err := os.Stat(filepath.Join(state, "runs")); !os.IsNotExist(err) {
		t.Fatalf("nested run state was created before rejection: err=%v", err)
	}
}

func TestWorkflowRejectsStateRootSymlinkIntoTarget(t *testing.T) {
	target := t.TempDir()
	inside := filepath.Join(target, "actual-state")
	if err := os.Mkdir(inside, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(target, "state-alias")
	if err := os.Symlink(inside, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	workflow := Workflow{Agent: &fakeAgent{outputs: map[string][]string{}}, Config: Config{StateDir: alias}, In: strings.NewReader(""), Out: io.Discard, Workdir: target}
	if err := workflow.Run("must not start"); err == nil || !strings.Contains(err.Error(), "must be outside target") {
		t.Fatalf("symlinked nested state error = %v, want containment rejection", err)
	}
	if _, err := os.Stat(filepath.Join(inside, "runs")); !os.IsNotExist(err) {
		t.Fatalf("state was created through target symlink before rejection: err=%v", err)
	}
}

func TestWorkflowAcceptsExternalStateRoot(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "target")
	state := filepath.Join(base, "state")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	agent := &fakeAgent{outputs: map[string][]string{}}
	workflow := Workflow{Agent: agent, Config: Config{StateDir: state}, In: strings.NewReader("yes\nyes\nyes\nyes\n"), Out: io.Discard, Workdir: target, Gate: true}
	if err := workflow.Run("task"); err != nil {
		t.Fatalf("external state directory should be accepted: %v", err)
	}
	if len(agent.calls) == 0 {
		t.Fatal("external state workflow did not start")
	}
	if _, err := os.Stat(filepath.Join(state, "runs")); err != nil {
		t.Fatalf("custom state directory must retain its runs subdirectory semantics: %v", err)
	}
}

func TestWorkflowDoesNotRequireAgentStdoutProtocol(t *testing.T) {
	agent := &legacyProtocolAgent{}
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: filepath.Join(t.TempDir(), "state")},
		In: strings.NewReader(""), Out: io.Discard, Workdir: t.TempDir(), Stages: []string{"requirements"},
	}
	if err := workflow.Run("task"); err != nil {
		t.Fatalf("successful stage agent should pass without evaluator stdout protocol: %v", err)
	}
	if agent.calls != 1 {
		t.Fatalf("workflow made %d agent calls, want exactly one stage invocation", agent.calls)
	}
}

type legacyProtocolAgent struct {
	calls int
}

func (a *legacyProtocolAgent) Run(_, _, _, _, logPath string) error {
	a.calls++
	return os.WriteFile(logPath, []byte("PASS\ncombined output cannot prove stdout provenance\n"), 0o600)
}

func (a *legacyProtocolAgent) RunWithContext(_ context.Context, stage, prompt, task, workdir, logPath string) error {
	return a.Run(stage, prompt, task, workdir, logPath)
}

func TestWorkflowStageExecutionBudgetStopsActiveAgentWithoutRetry(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	agent := &contextWaitingAgent{}
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader(strings.Repeat("yes\\n", 3)),
		Out: io.Discard, Workdir: t.TempDir(), Stages: []string{"requirements"}, stageBudgetLimit: 40 * time.Millisecond,
	}
	started := time.Now()
	err := workflow.Run("task")
	if !errors.Is(err, errStageExecutionBudgetExhausted) {
		t.Fatalf("workflow error = %v, want stage execution budget exhaustion", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("stage budget cancellation took %s, want prompt cancellation", elapsed)
	}
	state := workflowRunState(t, stateDir)
	if len(state.Stages) != 1 || state.Stages[0].Status != "failed" {
		t.Fatalf("budget-exhausted stage history = %+v, want one failed stage", state.Stages)
	}
}

type contextWaitingAgent struct{}

func (contextWaitingAgent) Run(string, string, string, string, string) error { return nil }

func (contextWaitingAgent) RunWithContext(ctx context.Context, stage, prompt, task, workdir, logPath string) error {
	<-ctx.Done()
	return ctx.Err()
}

type delayedRetryAgent struct {
	calls int
}

func (a *delayedRetryAgent) Run(string, string, string, string, string) error { return nil }

func (a *delayedRetryAgent) RunWithContext(ctx context.Context, _, _, _, _, _ string) error {
	a.calls++
	timer := time.NewTimer(65 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return errors.New("transient process failure")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestWorkflowStageBudgetAppliesToSingleInvocation(t *testing.T) {
	agent := &delayedRetryAgent{}
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: filepath.Join(t.TempDir(), "state")},
		In: strings.NewReader("yes\n"), Out: io.Discard, Workdir: t.TempDir(),
		Stages: []string{"requirements"}, stageBudgetLimit: 100 * time.Millisecond,
	}
	err := workflow.Run("task")
	if err == nil || !strings.Contains(err.Error(), "transient process failure") {
		t.Fatalf("workflow error = %v, want the single invocation failure", err)
	}
	if agent.calls != 1 {
		t.Fatalf("agent calls = %d, want exactly one invocation", agent.calls)
	}
}

func TestWorkflowStagePassesWithoutStdoutProtocolEvaluation(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "agent.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf 'stage output\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(dir, "state")
	workflow := Workflow{
		Agent:  Runner{Config: Config{Command: script, Args: []string{"{stage}", "{task}", "{system_prompt}"}}},
		Config: Config{StateDir: stateDir}, In: strings.NewReader(""), Out: io.Discard,
		Workdir: t.TempDir(), Stages: []string{"requirements"},
	}
	if err := workflow.Run("task"); err != nil {
		t.Fatalf("successful stage should not require evaluator output: %v", err)
	}
	state := workflowRunState(t, stateDir)
	if len(state.Stages) != 1 || state.Stages[0].Status != "passed" {
		t.Fatalf("successful stage history = %+v, want one passed invocation", state.Stages)
	}
	entries, err := os.ReadDir(filepath.Join(stateDir, "runs"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("run entries = %v, error = %v", entries, err)
	}
	runDir := filepath.Join(stateDir, "runs", entries[0].Name())
	if _, err := os.Stat(filepath.Join(runDir, "01-requirements.log")); err != nil {
		t.Fatalf("stage transcript missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(runDir, "01-evaluate-requirements.log")); !os.IsNotExist(err) {
		t.Fatalf("separate evaluator transcript exists: %v", err)
	}
}

func TestWorkflowRunsStageAgentWithoutEvaluatorTask(t *testing.T) {
	agent := &fakeAgent{outputs: map[string][]string{}}
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: filepath.Join(t.TempDir(), "state")},
		In: strings.NewReader(""), Out: io.Discard, Workdir: t.TempDir(), Stages: []string{"review"},
	}
	if err := workflow.Run("verify the requested behavior"); err != nil {
		t.Fatal(err)
	}
	if len(agent.calls) != 1 || agent.calls[0] != "review" {
		t.Fatalf("workflow calls = %v, want only the review stage agent", agent.calls)
	}
	if !strings.Contains(agent.tasks[0], "verify the requested behavior") {
		t.Fatalf("stage task did not preserve original request: %q", agent.tasks[0])
	}
}

func TestWorkflowPersistsVersionedEventsWithoutObserver(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	var output strings.Builder
	workflow := Workflow{
		Agent: &fakeAgent{outputs: map[string][]string{}}, Config: Config{StateDir: stateDir},
		In: strings.NewReader(""), Out: &output, Workdir: t.TempDir(), Stages: []string{"requirements"},
	}
	if err := workflow.Run("task"); err != nil {
		t.Fatal(err)
	}

	var runDir string
	for _, line := range strings.Split(output.String(), "\n") {
		if strings.HasPrefix(line, "Run: ") {
			runDir = strings.TrimPrefix(line, "Run: ")
			break
		}
	}
	if runDir == "" {
		t.Fatalf("workflow output did not expose run directory: %s", output.String())
	}
	file, err := os.Open(filepath.Join(runDir, "workflow-events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	type eventRecord struct {
		Version   int       `json:"version"`
		Timestamp time.Time `json:"timestamp"`
		RunID     string    `json:"runID"`
		Type      string    `json:"type"`
	}
	wantRunID := filepath.Base(runDir)
	wantTypes := map[string]bool{
		"workflow.transition": false,
		"stage.started":       false,
		"stage.completed":     false,
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if strings.Contains(strings.ToLower(scanner.Text()), "attempt") {
			t.Fatalf("new workflow event contains attempt field: %s", scanner.Text())
		}
		var record eventRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("parse workflow event JSONL record %q: %v", scanner.Text(), err)
		}
		if record.Version != 1 || record.RunID != wantRunID || record.Timestamp.IsZero() {
			t.Errorf("event missing versioned run metadata: %+v", record)
		}
		if _, ok := wantTypes[record.Type]; ok {
			wantTypes[record.Type] = true
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	for eventType, found := range wantTypes {
		if !found {
			t.Errorf("private workflow JSONL missing %q event", eventType)
		}
	}
	info, err := os.Stat(filepath.Join(runDir, "workflow-events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("workflow event log permissions = %o, want 600", info.Mode().Perm())
	}
}

func TestWorkflowInitialObserverFailurePersistsFailedLocally(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	observerErr := errors.New("observer unavailable")
	observerCalls := 0
	var output strings.Builder
	workflow := Workflow{
		Agent: &fakeAgent{outputs: map[string][]string{}}, Config: Config{StateDir: stateDir},
		In: strings.NewReader(""), Out: &output, Workdir: t.TempDir(),
		Observer: WorkflowObserverFunc(func(WorkflowEvent) error {
			observerCalls++
			if observerCalls == 1 {
				return observerErr
			}
			return nil
		}),
	}
	if err := workflow.Run("task"); !errors.Is(err, observerErr) {
		t.Fatalf("workflow error = %v, want observer failure %v", err, observerErr)
	}
	if observerCalls != 1 {
		t.Fatalf("observer called %d times, want initial event only", observerCalls)
	}

	var runDir string
	for _, line := range strings.Split(output.String(), "\n") {
		if strings.HasPrefix(line, "Run: ") {
			runDir = strings.TrimPrefix(line, "Run: ")
			break
		}
	}
	if runDir == "" {
		t.Fatalf("workflow output did not expose run directory: %s", output.String())
	}
	state, err := os.ReadFile(filepath.Join(runDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(state), `"status": "failed"`) {
		t.Fatalf("initial observer failure left state non-failed: %s", state)
	}

	file, err := os.Open(filepath.Join(runDir, "workflow-events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var transitions []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("parse workflow event JSONL record %q: %v", scanner.Text(), err)
		}
		if record.Type == "workflow.transition" {
			transitions = append(transitions, record.Message)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(transitions, ",") != "running,failed" {
		t.Fatalf("local transition records = %v, want [running failed]", transitions)
	}
}

func TestWorkflowObserverPersistsSingleInvocationFailureLifecycle(t *testing.T) {
	base := t.TempDir()
	store, err := NewJobStore(filepath.Join(base, "jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "job", Type: "implementation", Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession("job", "session", "running"); err != nil {
		t.Fatal(err)
	}
	agent := &fakeAgent{outputs: map[string][]string{"implement": {"ERROR: transient", "success"}}}
	var output strings.Builder
	var runDir string
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: filepath.Join(base, "state")}, In: strings.NewReader(""),
		Out: &output, Workdir: t.TempDir(), Stages: []string{"implement"},
		Observer:   JobSessionObserver{Store: store, JobID: "job", SessionID: "session"},
		RunCreated: func(dir, _ string) { runDir = dir },
	}
	if err := workflow.Run("task"); err == nil {
		t.Fatal("failed stage invocation should fail workflow")
	}
	events, err := store.SessionEvents("job", "session")
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for _, event := range events {
		types = append(types, event.Type)
	}
	want := []string{"workflow.transition", "stage.started", "stage.failed", "workflow.transition"}
	if strings.Join(types, ",") != strings.Join(want, ",") {
		t.Fatalf("persisted event types = %v, want %v", types, want)
	}
	if len(agent.calls) != 1 || !strings.Contains(events[1].Message, "stage=implement") || !strings.Contains(events[2].Message, "stage=implement") {
		t.Fatalf("failed stage must have one invocation and no retry event: calls=%v events=%+v", agent.calls, events)
	}
	for _, event := range events {
		if event.Type == "retry" || strings.Contains(strings.ToLower(event.Message), "attempt") {
			t.Fatalf("new session event contains retry/attempt details: %+v", event)
		}
	}
	if runDir == "" {
		t.Fatal("workflow did not expose the resolved run directory")
	}
	localEvents, err := os.ReadFile(filepath.Join(runDir, "workflow-events.jsonl"))
	if err != nil {
		t.Fatalf("configured observer run has no local JSONL event log: %v", err)
	}
	if count := strings.Count(strings.TrimSpace(string(localEvents)), "\n") + 1; count != len(events) {
		t.Fatalf("local JSONL event count = %d, observer event count = %d", count, len(events))
	}
}

func TestWorkflowPersistsOrderedStageHistoryWithoutAttemptFields(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	agent := &fakeAgent{outputs: map[string][]string{}}
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader(""), Out: io.Discard,
		Workdir: t.TempDir(), Stages: []string{"requirements", "implement"},
	}
	if err := workflow.Run("task"); err != nil {
		t.Fatal(err)
	}

	state := workflowRunState(t, stateDir)
	if state.StageHistoryVersion != 1 || state.Status != "complete" {
		t.Fatalf("history version/status = %d/%q, want 1/complete", state.StageHistoryVersion, state.Status)
	}
	if state.Stage != "implement" {
		t.Fatalf("legacy stage = %q, want final stage implement", state.Stage)
	}
	if len(state.Stages) != 2 {
		t.Fatalf("stage history has %d records, want 2: %+v", len(state.Stages), state.Stages)
	}
	wantNames := []string{"requirements", "implement"}
	for i, record := range state.Stages {
		if record.Name != wantNames[i] || record.Status != "passed" || record.StartedAt.IsZero() || record.EndedAt.IsZero() {
			t.Errorf("stage record %d = %+v, want ordered passed stage with timestamps", i, record)
		}
	}
	entries, err := os.ReadDir(filepath.Join(stateDir, "runs"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("run entries=%v err=%v", entries, err)
	}
	encoded, err := os.ReadFile(filepath.Join(stateDir, "runs", entries[0].Name(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "attempt") {
		t.Fatalf("new stage state contains attempt fields: %s", encoded)
	}
}

func TestWorkflowStageHistoryRecordsSingleInvocationFailure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		outputs    map[string][]string
		input      string
		wantStatus string
		wantStage  string
	}{
		{name: "invocation failure", outputs: map[string][]string{"requirements": {"ERROR: unavailable"}}, input: "yes\n", wantStatus: "failed", wantStage: "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stateDir := filepath.Join(t.TempDir(), "state")
			workflow := Workflow{
				Agent: &fakeAgent{outputs: tc.outputs}, Config: Config{StateDir: stateDir}, In: strings.NewReader(tc.input),
				Out: io.Discard, Workdir: t.TempDir(), Gate: true, Stages: []string{"requirements"},
			}
			err := workflow.Run("task")
			if tc.wantStatus == "failed" && err == nil {
				t.Fatal("failed stage must return an error")
			}
			state := workflowRunState(t, stateDir)
			if state.Status != tc.wantStatus || len(state.Stages) != 1 {
				t.Fatalf("run state = %+v, want status %q and one stage", state, tc.wantStatus)
			}
			stage := state.Stages[0]
			if stage.Status != tc.wantStage || stage.EndedAt.IsZero() {
				t.Fatalf("stage history = %+v, want status %q and end timestamp", stage, tc.wantStage)
			}
		})
	}
}

func workflowRunState(t *testing.T, stateDir string) State {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(stateDir, "runs"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected one workflow run, entries=%v err=%v", entries, err)
	}
	data, err := os.ReadFile(filepath.Join(stateDir, "runs", entries[0].Name(), "state.json"))
	if err != nil {
		t.Fatalf("read workflow state: %v", err)
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("decode workflow state: %v", err)
	}
	return state
}

func TestWorkflowSuccessfulStageReportsCompletionWithoutEvaluator(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	var output strings.Builder
	workflow := Workflow{Agent: &fakeAgent{outputs: map[string][]string{}}, Config: Config{StateDir: stateDir}, In: strings.NewReader(""), Out: &output, Workdir: t.TempDir(), Stages: []string{"requirements"}}
	if err := workflow.Run("task"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "requirements completed") {
		t.Fatalf("successful stage completion not reported: %s", output.String())
	}
	if strings.Contains(output.String(), "Evaluation:") {
		t.Fatalf("workflow reported an evaluator verdict: %s", output.String())
	}
	if status := workflowRunStatus(t, stateDir); status != "complete" {
		t.Fatalf("successful run status = %q, want complete", status)
	}
}

func TestWorkflowCancellationBeforeFirstAttemptPersistsInterruptedRun(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	workflow := Workflow{
		Agent: &fakeAgent{outputs: map[string][]string{}}, Config: Config{StateDir: stateDir},
		In: strings.NewReader(""), Out: io.Discard, Workdir: t.TempDir(), Stages: []string{"requirements"},
	}
	if err := workflow.RunContext(ctx, "task"); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled workflow error = %v, want context.Canceled", err)
	}

	state := workflowRunState(t, stateDir)
	if state.Status != "interrupted" || state.UpdatedAt.IsZero() {
		t.Fatalf("pre-canceled run state = %+v, want interrupted status and persisted timestamp", state)
	}
	if state.Stage != "" || len(state.Stages) != 0 {
		t.Fatalf("pre-canceled workflow created stage history before its first attempt: %+v", state)
	}
}

func TestWorkflowCancellationStopsAgentAndPersistsInterruptedStatus(t *testing.T) {
	tmp := t.TempDir()
	stateDir := filepath.Join(tmp, "state")
	workdir := filepath.Join(tmp, "work")
	if err := os.Mkdir(workdir, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(tmp, "agent-started")
	script := filepath.Join(tmp, "slow-agent.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf started > "+marker+"\nexec sleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	agent := Runner{Config: Config{Command: script, Args: []string{"{task}", "{system_prompt}"}}}
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader(""), Out: io.Discard, Workdir: workdir, Stages: []string{"requirements"}}
	result := make(chan error, 1)
	started := time.Now()
	go func() { result <- workflow.RunContext(ctx, "task") }()
	deadline := time.After(3 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("agent process did not start")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "workflow interrupted") {
			t.Fatalf("canceled workflow error = %v, want clear context-canceled interruption", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled workflow did not stop its active agent promptly")
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("canceled workflow took %s to return", elapsed)
	}
	state := workflowRunState(t, stateDir)
	if state.Status != "interrupted" {
		t.Fatalf("canceled run status = %q, want interrupted", state.Status)
	}
	if state.Stage != "requirements" || len(state.Stages) != 1 {
		t.Fatalf("canceled run legacy/current stage history = %q/%+v, want requirements and one record", state.Stage, state.Stages)
	}
	stage := state.Stages[0]
	if stage.Name != "requirements" || stage.Status != "interrupted" || stage.StartedAt.IsZero() || stage.EndedAt.IsZero() || !stage.EndedAt.After(stage.StartedAt) {
		t.Fatalf("canceled stage record = %+v, want interrupted status and ordered timestamps", stage)
	}
}

func TestWorkflowCancellationAfterStageRejectionPersistsInterruptedStatus(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := cancelOnPromptReader{cancel: cancel}
	workflow := Workflow{
		Agent:  &fakeAgent{outputs: map[string][]string{"requirements": {"PASS"}}},
		Config: Config{StateDir: stateDir}, In: reader, Out: io.Discard,
		Workdir: t.TempDir(), Gate: true, Stages: []string{"requirements"},
	}
	if err := workflow.RunContext(ctx, "task"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled rejected stage error = %v, want context.Canceled", err)
	}
	if status := workflowRunStatus(t, stateDir); status != "interrupted" {
		t.Fatalf("canceled rejected stage status = %q, want interrupted", status)
	}
}

type cancelOnPromptReader struct {
	cancel context.CancelFunc
}

func (r cancelOnPromptReader) Read([]byte) (int, error) { return 0, io.EOF }

func (r cancelOnPromptReader) ReadLineContext(context.Context) (string, error) {
	r.cancel()
	return "no\n", nil
}

func TestWorkflowPromptCancellationInterruptsApproval(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outputs map[string][]string
	}{
		{name: "approval"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stateDir := filepath.Join(t.TempDir(), "state")
			reader := &waitingContextLineReader{started: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			workflow := Workflow{
				Agent: &fakeAgent{outputs: tc.outputs}, Config: Config{StateDir: stateDir},
				In: reader, Out: io.Discard, Workdir: t.TempDir(), Gate: true, Stages: []string{"requirements"},
			}
			result := make(chan error, 1)
			go func() { result <- workflow.RunContext(ctx, "task") }()
			select {
			case <-reader.started:
			case <-time.After(3 * time.Second):
				cancel()
				t.Fatal("workflow did not reach its gate prompt")
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled gate error = %v, want context.Canceled", err)
				}
			case <-time.After(time.Second):
				t.Fatal("workflow remained blocked after prompt context cancellation")
			}
			if status := workflowRunStatus(t, stateDir); status != "interrupted" {
				t.Fatalf("canceled gate status = %q, want interrupted", status)
			}
		})
	}
}

type waitingContextLineReader struct {
	started chan struct{}
}

func (r *waitingContextLineReader) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (r *waitingContextLineReader) ReadLineContext(ctx context.Context) (string, error) {
	close(r.started)
	<-ctx.Done()
	return "", ctx.Err()
}

func TestWorkflowFailsClosedForNonContextAgentWhenCancellationIsPossible(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	agent := &nonContextProtocolAgent{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader(""),
		Out: io.Discard, Workdir: t.TempDir(), Stages: []string{"requirements"},
	}
	if err := workflow.RunContext(ctx, "task"); err == nil || !strings.Contains(err.Error(), "does not support context-aware execution") {
		t.Fatalf("non-context agent error = %v, want fail-closed context contract error", err)
	}
	if agent.calls != 0 {
		t.Fatalf("non-context agent was invoked %d times despite cancellable context", agent.calls)
	}
	if status := workflowRunStatus(t, stateDir); status != "failed" {
		t.Fatalf("non-context agent run status = %q, want failed", status)
	}
}

type nonContextProtocolAgent struct{ calls int }

func (a *nonContextProtocolAgent) Run(_, _, _, _, _ string) error {
	a.calls++
	return nil
}

func TestWorkflowPromptLoadErrorPersistsFailedStatus(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	workflow := Workflow{Agent: &fakeAgent{outputs: map[string][]string{}}, Config: Config{StateDir: stateDir}, In: strings.NewReader(""), Out: io.Discard, Workdir: t.TempDir(), Stages: []string{"invalid"}}
	if err := workflow.Run("task"); err == nil {
		t.Fatal("missing prompt should fail workflow")
	}
	if status := workflowRunStatus(t, stateDir); status != "failed" {
		t.Fatalf("prompt load failure status = %q, want failed", status)
	}
}

func workflowRunStatus(t *testing.T, stateDir string) string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(stateDir, "runs"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected one workflow run, entries=%v err=%v", entries, err)
	}
	state, err := os.ReadFile(filepath.Join(stateDir, "runs", entries[0].Name(), "state.json"))
	if err != nil {
		t.Fatalf("read workflow state: %v", err)
	}
	for _, status := range []string{"complete", "failed", "stopped", "interrupted", "running"} {
		if strings.Contains(string(state), `"status": "`+status+`"`) {
			return status
		}
	}
	t.Fatalf("workflow state has unknown status: %s", state)
	return ""
}

func TestWorkflowFailureStopsBeforeFollowingStagesWithoutPrompt(t *testing.T) {
	agent := &fakeAgent{outputs: map[string][]string{"requirements": {"ERROR: transient", "PASS\n"}}}
	var output strings.Builder
	stateDir := filepath.Join(t.TempDir(), "state")
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader(""), Out: &output, Workdir: t.TempDir()}
	if err := workflow.Run("task"); err == nil {
		t.Fatal("failed stage invocation should fail the workflow")
	}
	if got, want := strings.Join(agent.calls, ","), "requirements"; got != want {
		t.Fatalf("workflow calls = %s, want only the failed stage", got)
	}
	if strings.Contains(output.String(), "Type exactly yes") || strings.Contains(output.String(), "Retry this stage?") || strings.Contains(output.String(), "Retrying") {
		t.Fatalf("ungated failure prompted for approval or retry: %s", output.String())
	}
	if status := workflowRunStatus(t, stateDir); status != "failed" {
		t.Fatalf("workflow status = %q, want failed", status)
	}
}

func TestWorkflowGateDoesNotRetryInvocationFailure(t *testing.T) {
	agent := &fakeAgent{outputs: map[string][]string{"requirements": {"ERROR: transient", "success"}}}
	var output strings.Builder
	stateDir := filepath.Join(t.TempDir(), "state")
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader("yes\nyes\n"), Out: &output, Workdir: t.TempDir(), Gate: true, Stages: []string{"requirements"}}
	if err := workflow.Run("task"); err == nil || !strings.Contains(err.Error(), "requirements agent failed") {
		t.Fatalf("failed invocation should fail workflow: %v", err)
	}
	if len(agent.calls) != 1 {
		t.Fatalf("agent calls = %v, want exactly one invocation", agent.calls)
	}
	if status := workflowRunStatus(t, stateDir); status != "failed" {
		t.Fatalf("failed invocation status = %q, want failed", status)
	}
	if strings.Contains(output.String(), "Retry this stage?") || strings.Contains(output.String(), "Retrying") || strings.Contains(output.String(), "Type exactly yes") {
		t.Fatalf("failed stage produced retry prompt or feedback: %s", output.String())
	}
}

func TestWorkflowRequiresApprovalAtEverySuccessfulGate(t *testing.T) {
	agent := &fakeAgent{outputs: map[string][]string{}}
	stateDir := filepath.Join(t.TempDir(), "state")
	var output strings.Builder
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader("yes\nno\n"), Out: &output, Workdir: t.TempDir(), Gate: true}
	if err := workflow.Run("task"); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(agent.calls, ","), "requirements,implement"; got != want {
		t.Fatalf("stopped workflow calls = %s, want %s", got, want)
	}
	if status := workflowRunStatus(t, stateDir); status != "stopped" {
		t.Fatalf("declined stage approval status = %q, want stopped", status)
	}
}

type panickingAgent struct{ panicStage bool }

func (a panickingAgent) Run(string, string, string, string, string) error {
	if a.panicStage {
		panic("stage panic")
	}
	return nil
}
func (a panickingAgent) RunWithContext(context.Context, string, string, string, string, string) error {
	if a.panicStage {
		panic("stage panic")
	}
	return nil
}
func (panickingAgent) RunWithOutputContext(context.Context, string, string, string, string, string) (string, error) {
	panic("unexpected output-context invocation")
}

func TestWorkflowRestoresProgressOnAgentPanic(t *testing.T) {
	for _, stage := range []string{"requirements"} {
		t.Run(stage, func(t *testing.T) {
			t.Setenv("TERM", "xterm")
			t.Setenv("COLUMNS", "80")
			t.Setenv("LINES", "24")
			stateDir := filepath.Join(t.TempDir(), "state")
			var output strings.Builder
			workflow := Workflow{
				Agent: panickingAgent{panicStage: stage == "requirements"}, Config: Config{StateDir: stateDir}, In: strings.NewReader(""),
				Out: &output, Workdir: t.TempDir(), Terminal: true, Stages: []string{"requirements"},
			}
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("workflow should preserve and re-panic agent failure")
					}
				}()
				_ = workflow.Run("task")
			}()
			got := output.String()
			if !strings.Contains(got, "\033[?25h\033[?1049l") {
				t.Fatalf("%s panic left terminal modes altered: %q", stage, got)
			}
			if strings.Count(got, "\033[?1049h") != strings.Count(got, "\033[?1049l") {
				t.Fatalf("%s panic did not balance alternate-screen entry/exit: %q", stage, got)
			}
		})
	}
}

func TestWorkflowSanitizesFailedStageLogAndAgentErrorOnStdout(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	agent := &fakeAgent{outputs: map[string][]string{"requirements": {
		"ERROR:\x1b]0;hidden title\a\x1b[31mfailed\x1b[0m\x00",
		"ERROR:retry", "ERROR:retry", "ERROR:retry",
	}}}
	var output strings.Builder
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader(""), Out: &output, Workdir: t.TempDir(), Stages: []string{"requirements"}}
	if err := workflow.Run("task"); err == nil {
		t.Fatal("failed agent invocation must return an error")
	}
	text := output.String()
	if strings.ContainsAny(stripProgressANSI(text), "\x1b\a\x00") || strings.Contains(text, "hidden title") || !strings.Contains(text, "failed") {
		t.Fatalf("failed agent output was not safely presented: %q", text)
	}
	if status := workflowRunStatus(t, stateDir); status != "failed" {
		t.Fatalf("failed stage status = %q, want failed", status)
	}
}

func TestWorkflowAgentInvocationErrorFailsWithoutRetry(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	agent := &fakeAgent{outputs: map[string][]string{"requirements": {"ERROR: unavailable", "ERROR: unavailable", "ERROR: unavailable", "ERROR: unavailable"}}}
	var output strings.Builder
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader("yes\nyes\nyes\n"), Out: &output, Workdir: t.TempDir(), Stages: []string{"requirements"}}
	if err := workflow.Run("task"); err == nil || !strings.Contains(err.Error(), "requirements agent failed") {
		t.Fatalf("agent invocation error should be returned, got %v", err)
	}
	if len(agent.calls) != 1 {
		t.Fatalf("agent calls = %d, want one invocation", len(agent.calls))
	}
	if !strings.Contains(output.String(), "Agent failed:") || !strings.Contains(output.String(), "unavailable") {
		t.Fatalf("agent failure details missing: %s", output.String())
	}
	if status := workflowRunStatus(t, stateDir); status != "failed" {
		t.Fatalf("terminal agent error status = %q, want failed", status)
	}
}

func TestReadLogBoundsLargeDisplayToRecentTailAndRetainsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.log")
	content := "earliest-only diagnostic\n" + strings.Repeat("routine diagnostic\n", evaluatorOutputLimit*4) + "useful final diagnostic\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	got := readLog(path)
	if len(got) > evaluatorOutputLimit {
		t.Fatalf("displayed log length=%d exceeds limit %d", len(got), evaluatorOutputLimit)
	}
	if !strings.HasPrefix(got, "… log truncated; showing recent output …\n") {
		t.Fatalf("large log display omitted truncation marker: %q", got[:min(len(got), 80)])
	}
	if !strings.Contains(got, "useful final diagnostic") {
		t.Fatalf("large log display omitted recent diagnostic: %q", got)
	}
	if strings.Contains(got, "earliest-only diagnostic") {
		t.Fatalf("large log display retained old output instead of showing the tail")
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(len(content)) {
		t.Fatalf("full log size=%d, want %d bytes retained on disk", info.Size(), len(content))
	}
}

func TestBoundedOutputStripsTerminalControlsAndBoundsUTF8Bytes(t *testing.T) {
	output := "\x1b[31mfinding\x1b[0m\n\x1b]0;hostile title\a" + strings.Repeat("界", evaluatorOutputLimit)
	bounded := boundedOutput(output, 64)
	if strings.ContainsAny(bounded, "\x1b\a\r") || strings.Contains(bounded, "hostile title") {
		t.Fatalf("terminal control sequence survived sanitization: %q", bounded)
	}
	if !strings.HasPrefix(bounded, "finding\n") || !strings.Contains(bounded, "output truncated") || len(bounded) > 64 || !utf8.ValidString(bounded) {
		t.Fatalf("output was not safely bounded in bytes: len=%d output=%q", len(bounded), bounded)
	}
}

func TestWorkflowAgentFailureDoesNotRetryOrPrompt(t *testing.T) {
	outputs := []string{"ERROR: unavailable", "ERROR: must not be called", "ERROR: must not be called", "ERROR: must not be called"}
	agent := &fakeAgent{outputs: map[string][]string{"requirements": outputs}}
	stateDir := filepath.Join(t.TempDir(), "state")
	var output strings.Builder
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader(strings.Repeat("yes\n", 3)), Out: &output, Workdir: t.TempDir(), Gate: true, Stages: []string{"requirements"}}
	if err := workflow.Run("task"); err == nil || !strings.Contains(err.Error(), "requirements agent failed") {
		t.Fatalf("failed invocation must fail workflow, got %v", err)
	}
	if len(agent.calls) != 1 {
		t.Fatalf("agent calls = %d, want exactly one invocation", len(agent.calls))
	}
	if status := workflowRunStatus(t, stateDir); status != "failed" {
		t.Fatalf("exhausted agent status = %q, want failed", status)
	}
	if strings.Contains(output.String(), "Retry this stage?") || strings.Contains(output.String(), "Retrying") || strings.Contains(output.String(), "Attempt:") {
		t.Fatalf("retry feedback or attempt UI remains: %s", output.String())
	}
}

func TestWorkflowFailureIgnoresLegacyRetryInputAndFailsRun(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	agent := &fakeAgent{outputs: map[string][]string{"requirements": {"ERROR: unavailable"}}}
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader("no\n"), Out: io.Discard, Workdir: t.TempDir(), Gate: true, Stages: []string{"requirements"}}
	if err := workflow.Run("task"); err == nil {
		t.Fatal("invocation failure must fail without asking for retry approval")
	}
	if status := workflowRunStatus(t, stateDir); status != "failed" {
		t.Fatalf("failed invocation status = %q, want failed", status)
	}
}

func TestWorkflowFailedStageIsInvokedOnceWithoutRetryEventsOrPrompt(t *testing.T) {
	outputs := []string{"ERROR: unavailable", "ERROR: must not execute", "ERROR: must not execute", "ERROR: must not execute"}
	agent := &fakeAgent{outputs: map[string][]string{"requirements": outputs}}
	var output strings.Builder
	stateDir := filepath.Join(t.TempDir(), "state")
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader("yes\nyes\nyes\n"), Out: &output, Workdir: t.TempDir(), Gate: true, Stages: []string{"requirements"}}
	if err := workflow.Run("task"); err == nil || !strings.Contains(err.Error(), "requirements agent failed") {
		t.Fatalf("agent invocation error should fail workflow, got %v", err)
	}
	if len(agent.calls) != 1 {
		t.Fatalf("failed requirements ran %d times, want exactly one invocation", len(agent.calls))
	}
	for _, forbidden := range []string{"Retry this stage?", "Type exactly yes to retry", "Retrying", "Attempt:"} {
		if strings.Contains(output.String(), forbidden) {
			t.Errorf("failure output contains removed retry UI %q: %s", forbidden, output.String())
		}
	}
	if status := workflowRunStatus(t, stateDir); status != "failed" {
		t.Fatalf("terminal agent error status = %q, want failed", status)
	}
}

type retryFeedbackAgent struct {
	calls []struct {
		stage string
		task  string
	}
}

func (a *retryFeedbackAgent) RunWithOutputContext(_ context.Context, stage, prompt, task, workdir, logPath string) (string, error) {
	a.calls = append(a.calls, struct {
		stage string
		task  string
	}{stage: stage, task: task})
	output := "stage output\n"
	var err error
	if stage == "requirements" && len(a.calls) == 1 {
		output = "stage output from failed attempt\n"
		err = errors.New("stage process exited nonzero")
	}
	if writeErr := os.WriteFile(logPath, []byte(output), 0o600); writeErr != nil {
		return "", writeErr
	}
	return output, err
}

func (a *retryFeedbackAgent) Run(stage, prompt, task, workdir, logPath string) error {
	_, err := a.RunWithOutputContext(context.Background(), stage, prompt, task, workdir, logPath)
	return err
}

func (a *retryFeedbackAgent) RunWithContext(ctx context.Context, stage, prompt, task, workdir, logPath string) error {
	_, err := a.RunWithOutputContext(ctx, stage, prompt, task, workdir, logPath)
	return err
}

func TestWorkflowFailureDoesNotPassRetryFeedbackOrInvokeNextStage(t *testing.T) {
	agent := &retryFeedbackAgent{}
	state := filepath.Join(t.TempDir(), "state")
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: state}, In: strings.NewReader("yes\nyes\n"),
		Out: io.Discard, Workdir: t.TempDir(), Gate: true,
	}
	if err := workflow.Run("preserve original requested task"); err == nil {
		t.Fatal("failed invocation must fail workflow")
	}
	if len(agent.calls) != 1 || agent.calls[0].stage != "requirements" {
		t.Fatalf("failed workflow calls = %+v, want one requirements invocation", agent.calls)
	}
	for _, forbidden := range []string{"Previous stage attempt failed", "Agent error:", "stage output from failed attempt"} {
		if strings.Contains(agent.calls[0].task, forbidden) {
			t.Errorf("first invocation received retry feedback %q: %s", forbidden, agent.calls[0].task)
		}
	}
}
