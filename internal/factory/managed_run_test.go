package factory

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestManagedLivenessUsesHeartbeatAndProcessEvidence(t *testing.T) {
	now := time.Now()
	valid := managedOwner{PID: 123, StartedAt: now.Add(-time.Minute), HeartbeatAt: now.Add(-time.Second)}
	for _, tc := range []struct {
		name  string
		owner managedOwner
		now   time.Time
		want  string
	}{
		{name: "fresh owner heartbeat", owner: valid, now: now, want: "heartbeat_fresh"},
		{name: "stale owner heartbeat", owner: managedOwner{PID: valid.PID, StartedAt: valid.StartedAt, HeartbeatAt: now.Add(-managedOwnerFreshness - time.Second)}, now: now, want: "heartbeat_stale"},
		{name: "future-dated heartbeat", owner: managedOwner{PID: valid.PID, StartedAt: valid.StartedAt, HeartbeatAt: now.Add(time.Second)}, now: now, want: "unknown"},
		{name: "invalid owner", owner: managedOwner{PID: 0, StartedAt: now, HeartbeatAt: now}, now: now, want: "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := managedLiveness(tc.owner, tc.now); got != tc.want {
				t.Fatalf("managedLiveness() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRunCommandListsShowsEventsAndCooperativelyStopsOnlyManagedRuns(t *testing.T) {
	stateRoot := filepath.Join(t.TempDir(), "state")
	runsRoot, err := StateRoot(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	runDir, state, err := createRun(runsRoot, t.TempDir(), "gated task")
	if err != nil {
		t.Fatal(err)
	}
	state.Managed = true
	if err := writeState(runDir, state); err != nil {
		t.Fatal(err)
	}
	if err := writeManagedOwner(runDir, managedOwner{PID: os.Getpid()}); err != nil {
		t.Fatal(err)
	}
	if err := persistWorkflowEvent(runDir, WorkflowEvent{RunID: state.ID, Type: "stage.started", Stage: "requirements"}); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	cfg := Config{StateDir: stateRoot}
	if err := RunCommand(context.Background(), []string{"list"}, cfg, &out); err != nil || !strings.Contains(out.String(), "ID") || !strings.Contains(out.String(), "LIVENESS") || !strings.Contains(out.String(), state.ID) || !strings.Contains(out.String(), "heartbeat_fresh") {
		t.Fatalf("list output=%q err=%v", out.String(), err)
	}
	out.Reset()
	if err := RunCommand(context.Background(), []string{"show", state.ID}, cfg, &out); err != nil || !strings.Contains(out.String(), "Liveness: heartbeat_fresh") || strings.Contains(out.String(), "Description:") {
		t.Fatalf("show output=%q err=%v", out.String(), err)
	}
	out.Reset()
	if err := RunCommand(context.Background(), []string{"get", state.ID, "--details"}, cfg, &out); err != nil || !strings.Contains(out.String(), "Description: gated task") || !strings.Contains(out.String(), "workflow-events.jsonl path:") || !strings.Contains(out.String(), `"stage":"requirements"`) {
		t.Fatalf("detailed get output=%q err=%v", out.String(), err)
	}
	out.Reset()
	if err := RunCommand(context.Background(), []string{"events", state.ID}, cfg, &out); err != nil || !strings.Contains(out.String(), `"stage":"requirements"`) {
		t.Fatalf("events output=%q err=%v", out.String(), err)
	}
	state.Status = "complete"
	if err := writeState(runDir, state); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := RunCommand(context.Background(), []string{"events", state.ID, "--follow"}, cfg, &out); err != nil || !strings.Contains(out.String(), `"stage":"requirements"`) {
		t.Fatalf("followed events output=%q err=%v", out.String(), err)
	}
	state.Status = "running"
	if err := writeState(runDir, state); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := RunCommand(context.Background(), []string{"stop", state.ID}, cfg, &out); err != nil {
		t.Fatal(err)
	}
	if !managedStopRequested(runDir) {
		t.Fatal("stop command did not persist its cooperative request")
	}
	if err := RunCommand(context.Background(), []string{"show", "unmanaged"}, cfg, &out); err == nil {
		t.Fatal("run control accepted an unmanaged or missing run")
	}
}

func TestManagedWorkflowStartupFailurePersistsFailedOutcome(t *testing.T) {
	stateRoot := t.TempDir()
	workdir := t.TempDir()
	var runDir string
	workflow := Workflow{
		Agent: &fakeAgent{outputs: map[string][]string{}}, Config: Config{StateDir: stateRoot},
		In: strings.NewReader(""), Out: io.Discard, Workdir: workdir,
		Gate: true, Managed: true,
		RunCreated: func(dir, _ string) {
			runDir = dir
			if err := os.Mkdir(filepath.Join(dir, "owner.json"), 0o700); err != nil {
				t.Errorf("block managed owner record: %v", err)
			}
		},
	}

	err := workflow.Run("managed task")
	if err == nil || !strings.Contains(err.Error(), "owner.json") {
		t.Fatalf("workflow error = %v, want preserved managed startup owner write failure", err)
	}
	if runDir == "" {
		t.Fatal("workflow did not create a persisted run")
	}
	state, stateErr := readManagedState(runDir)
	if stateErr != nil || !state.Managed || state.Status != "failed" {
		t.Fatalf("persisted state = %+v, err=%v; want managed failed state", state, stateErr)
	}

	file, err := os.Open(filepath.Join(runDir, "workflow-events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var terminal *workflowEventRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event workflowEventRecord
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatalf("parse workflow event %q: %v", scanner.Text(), err)
		}
		if event.Type == "workflow.transition" && event.Message == "failed" {
			terminal = &event
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if terminal == nil || terminal.Outcome != "failure" {
		t.Fatalf("terminal failure event = %+v, want workflow failure outcome", terminal)
	}
}

func TestManagedRunStopRequestCancelsContext(t *testing.T) {
	runDir := t.TempDir()
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	managed, err := startManagedRun(parent, runDir)
	if err != nil {
		t.Fatal(err)
	}
	defer managed.close()
	if err := requestManagedStop(runDir); err != nil {
		t.Fatal(err)
	}
	select {
	case <-managed.ctx.Done():
		if !errors.Is(managed.ctx.Err(), context.Canceled) {
			t.Fatalf("managed context error = %v, want cancellation", managed.ctx.Err())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("managed run did not observe durable stop request")
	}
	if !managedStopRequested(runDir) {
		t.Fatal("cooperative stop request was not retained durably")
	}
	managed.close()
}

func TestGatedWorkflowRunStopCancelsApprovalCooperatively(t *testing.T) {
	stateRoot := t.TempDir()
	workdir := t.TempDir()
	prompted := make(chan struct{})
	workflow := Workflow{
		Agent: &fakeAgent{outputs: map[string][]string{}}, Config: Config{StateDir: stateRoot},
		In: managedBlockingReader{prompted: prompted}, Out: io.Discard, Workdir: workdir,
		Stages: []string{"requirements"}, Gate: true, Managed: true,
	}
	result := make(chan error, 1)
	go func() { result <- workflow.Run("gated task") }()
	select {
	case <-prompted:
	case <-time.After(3 * time.Second):
		t.Fatal("gated workflow did not reach approval")
	}
	runsRoot, err := StateRoot(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(runsRoot)
	if err != nil || len(entries) != 1 {
		t.Fatalf("run records = %v err=%v", entries, err)
	}
	var out bytes.Buffer
	if err := RunCommand(context.Background(), []string{"stop", entries[0].Name()}, Config{StateDir: stateRoot}, &out); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("stopped gated workflow error = %v, want context cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gated workflow remained blocked after cooperative stop")
	}
	state, err := readManagedState(filepath.Join(runsRoot, entries[0].Name()))
	if err != nil || state.Status != "stopped" {
		t.Fatalf("stopped gated run state=%+v err=%v", state, err)
	}
}

type managedBlockingReader struct{ prompted chan struct{} }

func (r managedBlockingReader) Read([]byte) (int, error) {
	return 0, errors.New("unexpected non-context read")
}

func (r managedBlockingReader) ReadLineContext(ctx context.Context) (string, error) {
	select {
	case <-r.prompted:
	default:
		close(r.prompted)
	}
	<-ctx.Done()
	return "", ctx.Err()
}

func TestManagedWorkflowRequiresApprovalGate(t *testing.T) {
	workflow := Workflow{Managed: true, Config: Config{}, Workdir: t.TempDir(), In: strings.NewReader(""), Out: io.Discard}
	if err := workflow.Run("task"); err == nil || !strings.Contains(err.Error(), "reserved for gated") {
		t.Fatalf("ungated managed workflow error = %v", err)
	}
}

func TestRunListEmptyIsHumanReadable(t *testing.T) {
	var out bytes.Buffer
	if err := RunCommand(context.Background(), []string{"list"}, Config{StateDir: t.TempDir()}, &out); err != nil {
		t.Fatal(err)
	}
	if out.String() != "No gated runs.\n" {
		t.Fatalf("empty run list output=%q", out.String())
	}
	out.Reset()
	if err := RunCommand(context.Background(), []string{"list"}, Config{StateDir: t.TempDir()}, &out); err != nil {
		t.Fatal(err)
	}
	if out.String() != "No gated runs.\n" {
		t.Fatalf("run list containing only unmanaged state output=%q", out.String())
	}
}

func TestRunCommandsRejectNonManagedRun(t *testing.T) {
	root := filepath.Join(t.TempDir(), "runs")
	dir, _, err := createRun(root, t.TempDir(), "ordinary run")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := RunCommand(context.Background(), []string{"show", filepath.Base(dir)}, Config{StateDir: filepath.Dir(root)}, &out); err == nil || !strings.Contains(err.Error(), "not a gated, manageable run") {
		t.Fatalf("show unmanaged run error = %v", err)
	}
}
