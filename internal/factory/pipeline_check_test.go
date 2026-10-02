package factory

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const pipelineCheckTestMode = "FACTORY_PIPELINE_CHECK_TEST_MODE"

var pipelineCheckArgument string

func init() {
	flag.StringVar(&pipelineCheckArgument, "test.pipeline-check-argument", "", "pipeline check test argument")
}

func TestPipelineCheckProcessHelper(t *testing.T) {
	mode := os.Getenv(pipelineCheckTestMode)
	if mode == "" {
		return
	}
	if mode == "literal" {
		fmt.Print(pipelineCheckArgument)
		os.Exit(0)
	}
	if mode == "cwd" {
		workdir, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		fmt.Print(workdir)
		os.Exit(0)
	}
	fmt.Print(os.Getenv("FACTORY_PIPELINE_CHECK_STDOUT"))
	fmt.Fprint(os.Stderr, os.Getenv("FACTORY_PIPELINE_CHECK_STDERR"))
	if mode == "sleep" {
		if marker := os.Getenv("FACTORY_PIPELINE_CHECK_STARTED"); marker != "" {
			_ = os.WriteFile(marker, []byte("started"), 0o600)
		}
		time.Sleep(10 * time.Second)
	}
	if mode == "fail" {
		os.Exit(7)
	}
	os.Exit(0)
}

func TestWorkflowRunsConfiguredChecksAndPersistsCompleteLogs(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	workdir := t.TempDir()
	stdout := strings.Repeat("stdout evidence\n", 1200)
	stderr := strings.Repeat("stderr evidence\n", 1200)
	t.Setenv(pipelineCheckTestMode, "success")
	t.Setenv("FACTORY_PIPELINE_CHECK_STDOUT", stdout)
	t.Setenv("FACTORY_PIPELINE_CHECK_STDERR", stderr)

	workflow := pipelineCheckWorkflow(t, stateDir, workdir)
	if err := workflow.Run("run configured checks"); err != nil {
		t.Fatal(err)
	}

	state, runDir := readPipelineCheckState(t, stateDir)
	if state.Status != "complete" {
		t.Fatalf("status = %q, want complete", state.Status)
	}
	if len(state.Checks) != 1 {
		t.Fatalf("check results = %+v, want one result", state.Checks)
	}
	result := state.Checks[0]
	if result.ExitCode != 0 || !result.StartedAt.Before(result.EndedAt) {
		t.Fatalf("check result metadata = %+v", result)
	}
	assertPipelineCheckEvents(t, runDir, []workflowEventRecord{
		{Type: "check.started", Outcome: "", ExitCode: nil, Command: result.Command, Transcript: result.Log, StartedAt: timePointer(result.StartedAt)},
		{Type: "check.completed", Outcome: "success", ExitCode: intPointer(0), Command: result.Command, Transcript: result.Log, StartedAt: timePointer(result.StartedAt), EndedAt: timePointer(result.EndedAt)},
	})
	executable := resolvedTestPath(t, os.Args[0])
	if len(result.Command) != 2 || resolvedTestPath(t, result.Command[0]) != executable || result.Command[1] != "-test.run=^TestPipelineCheckProcessHelper$" {
		t.Fatalf("recorded argv = %q, want executable and helper test args", result.Command)
	}
	logData, err := os.ReadFile(filepath.Join(runDir, result.Log))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(logData), stdout+stderr; got != want {
		t.Fatalf("check log length/content mismatch: got %d bytes, want %d", len(got), len(want))
	}
	if _, err := os.Stat(filepath.Join(workdir, result.Log)); !os.IsNotExist(err) {
		t.Fatalf("check log was not kept external to workdir: %v", err)
	}
}

func TestWorkflowCheckStartFailurePersistsResultAndFailsRun(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	workflow := pipelineCheckWorkflow(t, stateDir, t.TempDir())
	workflow.Config.PipelineChecks = [][]string{{filepath.Join(t.TempDir(), "missing-check-executable")}}

	err := workflow.Run("check start failure")
	if err == nil || !strings.Contains(err.Error(), "exit code -1") {
		t.Fatalf("Run() error = %v, want process start failure with exit code -1", err)
	}
	state, runDir := readPipelineCheckState(t, stateDir)
	if state.Status != "failed" || len(state.Checks) != 1 || state.Checks[0].ExitCode != -1 {
		t.Fatalf("failed check state = %+v, want persisted start failure", state)
	}
	assertPipelineCheckEvents(t, runDir, []workflowEventRecord{
		{Type: "check.started", Command: state.Checks[0].Command, Transcript: state.Checks[0].Log, StartedAt: timePointer(state.Checks[0].StartedAt)},
		{Type: "check.completed", Outcome: "failure", ExitCode: intPointer(-1), Command: state.Checks[0].Command, Transcript: state.Checks[0].Log, StartedAt: timePointer(state.Checks[0].StartedAt), EndedAt: timePointer(state.Checks[0].EndedAt)},
	})
}

func TestWorkflowFailedCheckPersistsResultAndFailsRun(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	t.Setenv(pipelineCheckTestMode, "fail")
	t.Setenv("FACTORY_PIPELINE_CHECK_STDOUT", "check output\n")
	t.Setenv("FACTORY_PIPELINE_CHECK_STDERR", "check error\n")

	err := pipelineCheckWorkflow(t, stateDir, t.TempDir()).Run("failing check")
	if err == nil || !strings.Contains(err.Error(), "exit code 7") {
		t.Fatalf("Run() error = %v, want check failure with exit code 7", err)
	}
	state, runDir := readPipelineCheckState(t, stateDir)
	transcriptPath := resolvedTestPath(t, filepath.Join(runDir, "pipeline-check-01.log"))
	if !strings.Contains(err.Error(), transcriptPath) {
		t.Fatalf("Run() error = %v, want full transcript path %q", err, transcriptPath)
	}
	if state.Status != "failed" {
		t.Fatalf("status = %q, want failed", state.Status)
	}
	if state.Status == "complete" {
		t.Fatal("run must not complete after a failed check")
	}
	if len(state.Checks) != 1 || state.Checks[0].ExitCode != 7 {
		t.Fatalf("failed check result = %+v", state.Checks)
	}
	assertPipelineCheckEvents(t, runDir, []workflowEventRecord{
		{Type: "check.started", Command: state.Checks[0].Command, Transcript: state.Checks[0].Log, StartedAt: timePointer(state.Checks[0].StartedAt)},
		{Type: "check.completed", Outcome: "failure", ExitCode: intPointer(7), Command: state.Checks[0].Command, Transcript: state.Checks[0].Log, StartedAt: timePointer(state.Checks[0].StartedAt), EndedAt: timePointer(state.Checks[0].EndedAt)},
	})
	logData, err := os.ReadFile(filepath.Join(runDir, state.Checks[0].Log))
	if err != nil || string(logData) != "check output\ncheck error\n" {
		t.Fatalf("failed check transcript = %q, %v", logData, err)
	}
}

func TestWorkflowPipelineCheckOpenFailureCompletesLifecycle(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	openErr := errors.New("transcript unavailable")
	oldOpen := pipelineCheckOpenFile
	pipelineCheckOpenFile = func(string) (*os.File, error) { return nil, openErr }
	t.Cleanup(func() { pipelineCheckOpenFile = oldOpen })
	var observed []WorkflowEvent
	workflow := pipelineCheckWorkflow(t, stateDir, t.TempDir())
	workflow.Observer = WorkflowObserverFunc(func(event WorkflowEvent) error {
		if strings.HasPrefix(event.Type, "check.") {
			observed = append(observed, event)
		}
		return nil
	})

	err := workflow.Run("open failure")
	if !errors.Is(err, openErr) {
		t.Fatalf("Run() error = %v, want transcript open failure", err)
	}
	state, runDir := readPipelineCheckState(t, stateDir)
	if state.Status != "failed" || len(state.Checks) != 1 || state.Checks[0].ExitCode != -1 {
		t.Fatalf("failed check state = %+v, want persisted failure result", state)
	}
	assertPipelineCheckEvents(t, runDir, []workflowEventRecord{
		{Type: "check.started", Command: state.Checks[0].Command, Transcript: state.Checks[0].Log, StartedAt: timePointer(state.Checks[0].StartedAt)},
		{Type: "check.completed", Outcome: "failure", ExitCode: intPointer(-1), Command: state.Checks[0].Command, Transcript: state.Checks[0].Log, StartedAt: timePointer(state.Checks[0].StartedAt), EndedAt: timePointer(state.Checks[0].EndedAt)},
	})
	if len(observed) != 2 || observed[0].Type != "check.started" || observed[1].Type != "check.completed" || observed[1].Outcome != "failure" || observed[1].ExitCode == nil || *observed[1].ExitCode != -1 {
		t.Fatalf("observer check lifecycle = %+v, want started and failed completion", observed)
	}
}

func TestWorkflowPipelineCheckCloseFailurePersistsFailedResult(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	closeErr := errors.New("transcript close failed")
	oldClose := pipelineCheckCloseFile
	pipelineCheckCloseFile = func(file *os.File) error {
		if err := file.Close(); err != nil {
			return err
		}
		return closeErr
	}
	t.Cleanup(func() { pipelineCheckCloseFile = oldClose })
	t.Setenv(pipelineCheckTestMode, "success")
	var observed []WorkflowEvent
	workflow := pipelineCheckWorkflow(t, stateDir, t.TempDir())
	workflow.Observer = WorkflowObserverFunc(func(event WorkflowEvent) error {
		if strings.HasPrefix(event.Type, "check.") {
			observed = append(observed, event)
		}
		return nil
	})

	err := workflow.Run("close failure")
	if !errors.Is(err, closeErr) {
		t.Fatalf("Run() error = %v, want transcript close failure", err)
	}
	state, runDir := readPipelineCheckState(t, stateDir)
	if state.Status != "failed" || len(state.Checks) != 1 || state.Checks[0].ExitCode != -1 {
		t.Fatalf("failed check state = %+v, want nonzero failure result", state)
	}
	assertPipelineCheckEvents(t, runDir, []workflowEventRecord{
		{Type: "check.started", Command: state.Checks[0].Command, Transcript: state.Checks[0].Log, StartedAt: timePointer(state.Checks[0].StartedAt)},
		{Type: "check.completed", Outcome: "failure", ExitCode: intPointer(-1), Command: state.Checks[0].Command, Transcript: state.Checks[0].Log, StartedAt: timePointer(state.Checks[0].StartedAt), EndedAt: timePointer(state.Checks[0].EndedAt)},
	})
	if len(observed) != 2 || observed[1].Type != "check.completed" || observed[1].Outcome != "failure" || observed[1].ExitCode == nil || *observed[1].ExitCode != -1 {
		t.Fatalf("observer check lifecycle = %+v, want failed completion", observed)
	}
}

func TestWorkflowRunsChecksInConfiguredOrder(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	t.Setenv(pipelineCheckTestMode, "literal")
	workflow := pipelineCheckWorkflow(t, stateDir, t.TempDir())
	executable := workflow.Config.PipelineChecks[0][0]
	workflow.Config.PipelineChecks = [][]string{
		{executable, "-test.run=^TestPipelineCheckProcessHelper$", "-test.pipeline-check-argument=first"},
		{executable, "-test.run=^TestPipelineCheckProcessHelper$", "-test.pipeline-check-argument=second"},
	}
	if err := workflow.Run("ordered checks"); err != nil {
		t.Fatal(err)
	}
	state, runDir := readPipelineCheckState(t, stateDir)
	if len(state.Checks) != 2 || state.Checks[0].Command[len(state.Checks[0].Command)-1] != "-test.pipeline-check-argument=first" || state.Checks[1].Command[len(state.Checks[1].Command)-1] != "-test.pipeline-check-argument=second" {
		t.Fatalf("checks were not recorded in configured order: %+v", state.Checks)
	}
	checkEvents := readPipelineCheckEvents(t, runDir)
	var lifecycle []workflowEventRecord
	for _, event := range checkEvents {
		if event.Type == "check.started" || event.Type == "check.completed" {
			lifecycle = append(lifecycle, event)
		}
	}
	if len(lifecycle) != 4 || lifecycle[0].Type != "check.started" || lifecycle[0].Command[len(lifecycle[0].Command)-1] != "-test.pipeline-check-argument=first" || lifecycle[1].Type != "check.completed" || lifecycle[2].Type != "check.started" || lifecycle[2].Command[len(lifecycle[2].Command)-1] != "-test.pipeline-check-argument=second" || lifecycle[3].Type != "check.completed" {
		t.Fatalf("structured check events are not ordered around configured checks: %+v", lifecycle)
	}
	for index, want := range []string{"first", "second"} {
		data, err := os.ReadFile(filepath.Join(runDir, state.Checks[index].Log))
		if err != nil || string(data) != want {
			t.Fatalf("check %d transcript = %q, %v; want %q", index+1, data, err, want)
		}
	}
}

func TestWorkflowRunsChecksInTargetWorkdir(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	workdir := t.TempDir()
	t.Setenv(pipelineCheckTestMode, "cwd")
	workflow := pipelineCheckWorkflow(t, stateDir, workdir)
	if err := workflow.Run("target workdir"); err != nil {
		t.Fatal(err)
	}
	state, runDir := readPipelineCheckState(t, stateDir)
	data, err := os.ReadFile(filepath.Join(runDir, state.Checks[0].Log))
	if err != nil || string(data) != resolvedTestPath(t, workdir) {
		t.Fatalf("check workdir = %q, %v; want %q", data, err, resolvedTestPath(t, workdir))
	}
}

func TestWorkflowCanceledCheckPersistsResultAndInterruptsRun(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	t.Setenv(pipelineCheckTestMode, "sleep")
	t.Setenv("FACTORY_PIPELINE_CHECK_STDOUT", "started\n")
	t.Setenv("FACTORY_PIPELINE_CHECK_STDERR", "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	marker := filepath.Join(t.TempDir(), "check-started")
	t.Setenv("FACTORY_PIPELINE_CHECK_STARTED", marker)
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(marker); err == nil {
				cancel()
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		cancel()
	}()

	err := pipelineCheckWorkflow(t, stateDir, t.TempDir()).RunContext(ctx, "canceled check")
	if err == nil || !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("RunContext() error = %v, want interruption", err)
	}
	state, runDir := readPipelineCheckState(t, stateDir)
	if state.Status != "interrupted" {
		t.Fatalf("status = %q, want interrupted", state.Status)
	}
	if state.Status == "complete" {
		t.Fatal("run must not complete after a canceled check")
	}
	if len(state.Checks) != 1 || state.Checks[0].ExitCode != -1 {
		t.Fatalf("canceled check result = %+v", state.Checks)
	}
	assertPipelineCheckEvents(t, runDir, []workflowEventRecord{
		{Type: "check.started", Command: state.Checks[0].Command, Transcript: state.Checks[0].Log, StartedAt: timePointer(state.Checks[0].StartedAt)},
		{Type: "check.completed", Outcome: "canceled", ExitCode: intPointer(-1), Command: state.Checks[0].Command, Transcript: state.Checks[0].Log, StartedAt: timePointer(state.Checks[0].StartedAt), EndedAt: timePointer(state.Checks[0].EndedAt)},
	})
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("check did not start before cancellation: %v", err)
	}
	logData, err := os.ReadFile(filepath.Join(runDir, state.Checks[0].Log))
	if err != nil || string(logData) != "started\n" {
		t.Fatalf("canceled check transcript = %q, %v", logData, err)
	}
}

func TestPipelineCheckArgumentsAreNotInterpretedByShell(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "should-not-exist")
	literal := "$(touch " + marker + ")"
	workflow := pipelineCheckWorkflow(t, filepath.Join(t.TempDir(), "state"), t.TempDir())
	workflow.Config.PipelineChecks = [][]string{{os.Args[0], "-test.run=^TestPipelineCheckProcessHelper$", "-test.pipeline-check-argument=" + literal}}
	t.Setenv(pipelineCheckTestMode, "literal")
	if err := workflow.Run("literal argv"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("argument was interpreted as shell syntax: %v", err)
	}
	_, runDir := readPipelineCheckState(t, workflow.Config.StateDir)
	entries, err := os.ReadDir(filepath.Join(workflow.Config.StateDir, "runs"))
	if err != nil {
		t.Fatal(err)
	}
	var state State
	data, err := os.ReadFile(filepath.Join(runDir, "state.json"))
	if err != nil || json.Unmarshal(data, &state) != nil {
		t.Fatalf("read state: %v", err)
	}
	logData, err := os.ReadFile(filepath.Join(runDir, state.Checks[0].Log))
	if err != nil || strings.TrimSpace(string(logData)) != literal {
		t.Fatalf("literal argument transcript = %q, %v (runs=%d)", logData, err, len(entries))
	}
}

func TestRunPipelineChecksWithFakeRunner(t *testing.T) {
	failure := errors.New("check failed")
	startupFailure := errors.New("executable not found")
	cases := []struct {
		name        string
		run         func(context.Context) (int, error)
		wantCode    int
		wantOutcome string
		wantErr     string
		wantIs      error
	}{
		{
			name:     "success",
			run:      func(context.Context) (int, error) { return 0, nil },
			wantCode: 0, wantOutcome: "success",
		},
		{
			name:     "nonzero result",
			run:      func(context.Context) (int, error) { return 9, failure },
			wantCode: 9, wantOutcome: "failure", wantErr: "exit code 9", wantIs: failure,
		},
		{
			name:     "runner startup failure",
			run:      func(context.Context) (int, error) { return -1, startupFailure },
			wantCode: -1, wantOutcome: "failure", wantErr: "exit code -1", wantIs: startupFailure,
		},
		{
			name: "cancellation",
			run: func(ctx context.Context) (int, error) {
				return -1, ctx.Err()
			},
			wantCode: -1, wantOutcome: "canceled", wantErr: "interrupted", wantIs: context.Canceled,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runDir := t.TempDir()
			workdir := filepath.Join(t.TempDir(), "target")
			if err := os.Mkdir(workdir, 0o700); err != nil {
				t.Fatal(err)
			}
			args := []string{"check-tool", "--label", "value with spaces"}
			ctx := context.Background()
			if tc.name == "cancellation" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				t.Cleanup(cancel)
				originalRun := tc.run
				tc.run = func(context.Context) (int, error) {
					cancel()
					return originalRun(ctx)
				}
			}
			runner := &fakePipelineCheckRunner{
				output: "runner transcript\n",
				run:    tc.run,
			}
			state := State{ID: "test-run"}
			var events []WorkflowEvent
			err := runPipelineChecks(ctx, [][]string{args}, workdir, runDir, &state, func(event WorkflowEvent) error {
				events = append(events, event)
				return nil
			}, runner)

			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("runPipelineChecks() error = %v, want nil", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) || !errors.Is(err, tc.wantIs) {
				t.Fatalf("runPipelineChecks() error = %v, want %q wrapping %v", err, tc.wantErr, tc.wantIs)
			}
			if runner.workdir != workdir || strings.Join(runner.args, "\x00") != strings.Join(args, "\x00") {
				t.Fatalf("runner received workdir=%q args=%q, want %q %q", runner.workdir, runner.args, workdir, args)
			}
			if len(state.Checks) != 1 || state.Checks[0].ExitCode != tc.wantCode || strings.Join(state.Checks[0].Command, "\x00") != strings.Join(args, "\x00") {
				t.Fatalf("persisted check result = %+v", state.Checks)
			}
			if len(events) != 2 || events[0].Type != "check.started" || events[1].Type != "check.completed" || events[1].Outcome != tc.wantOutcome || events[1].ExitCode == nil || *events[1].ExitCode != tc.wantCode {
				t.Fatalf("check lifecycle events = %+v", events)
			}
			logData, readErr := os.ReadFile(filepath.Join(runDir, state.Checks[0].Log))
			if readErr != nil || string(logData) != runner.output {
				t.Fatalf("check transcript = %q, %v", logData, readErr)
			}
		})
	}
}

func TestRunPipelineChecksForwardsExactArgumentsAndWorkdir(t *testing.T) {
	workdir := filepath.Join(t.TempDir(), "selected-working-directory")
	args := []string{"tool path", "argument with spaces", "$(not-a-shell-expression)"}
	runner := &fakePipelineCheckRunner{output: "forwarded output\n", run: func(context.Context) (int, error) { return 0, nil }}
	runDir := t.TempDir()
	state := State{ID: "forwarding-run"}
	if err := runPipelineChecks(context.Background(), [][]string{args}, workdir, runDir, &state, func(WorkflowEvent) error { return nil }, runner); err != nil {
		t.Fatal(err)
	}
	if runner.workdir != workdir || strings.Join(runner.args, "\x00") != strings.Join(args, "\x00") {
		t.Fatalf("runner received workdir=%q args=%q, want %q %q", runner.workdir, runner.args, workdir, args)
	}
	args[1] = "mutated after invocation"
	if runner.args[1] != "argument with spaces" || state.Checks[0].Command[1] != "argument with spaces" {
		t.Fatalf("forwarded command aliases caller input: runner=%q state=%q", runner.args, state.Checks[0].Command)
	}
	logData, err := os.ReadFile(filepath.Join(runDir, state.Checks[0].Log))
	if err != nil || string(logData) != runner.output {
		t.Fatalf("runner output transcript = %q, %v", logData, err)
	}
}

type fakePipelineCheckRunner struct {
	workdir string
	args    []string
	output  string
	run     func(context.Context) (int, error)
}

func (r *fakePipelineCheckRunner) Run(ctx context.Context, workdir string, args []string, output io.Writer, env []string) (int, error) {
	r.workdir = workdir
	r.args = append([]string(nil), args...)
	if _, err := io.WriteString(output, r.output); err != nil {
		return -1, err
	}
	return r.run(ctx)
}

func pipelineCheckWorkflow(t *testing.T, stateDir, workdir string) Workflow {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	workflow := Workflow{
		Agent: &fakeAgent{outputs: map[string][]string{}},
		Config: Config{
			StateDir:       stateDir,
			PipelineChecks: [][]string{{executable, "-test.run=^TestPipelineCheckProcessHelper$"}},
		},
		In: strings.NewReader(""), Out: io.Discard, Workdir: workdir,
		Stages: []string{"requirements"},
	}
	return workflow
}

func intPointer(value int) *int { return &value }

func timePointer(value time.Time) *time.Time { return &value }

func assertPipelineCheckEvents(t *testing.T, runDir string, want []workflowEventRecord) {
	t.Helper()
	got := readPipelineCheckEvents(t, runDir)
	var lifecycle []workflowEventRecord
	for _, event := range got {
		if event.Type == "check.started" || event.Type == "check.completed" {
			lifecycle = append(lifecycle, event)
		}
	}
	if len(lifecycle) != len(want) {
		t.Fatalf("check lifecycle event count = %d, want %d: %+v", len(lifecycle), len(want), lifecycle)
	}
	for i := range want {
		actual := lifecycle[i]
		expected := want[i]
		if actual.Type != expected.Type || actual.Outcome != expected.Outcome || actual.Transcript != expected.Transcript || strings.Join(actual.Command, "\x00") != strings.Join(expected.Command, "\x00") || actual.StartedAt == nil || (expected.StartedAt != nil && !actual.StartedAt.Equal(*expected.StartedAt)) {
			t.Errorf("check event %d metadata = %+v, want %+v", i, actual, expected)
		}
		endedAtMatches := actual.EndedAt != nil && (expected.EndedAt == nil || actual.EndedAt.Equal(*expected.EndedAt))
		if expected.Type == "check.started" && (actual.ExitCode != nil || actual.EndedAt != nil) {
			t.Errorf("check start contains completion-only fields: %+v", actual)
		}
		if expected.Type == "check.completed" && (actual.ExitCode == nil || expected.ExitCode == nil || *actual.ExitCode != *expected.ExitCode || !endedAtMatches || !actual.StartedAt.Before(*actual.EndedAt)) {
			t.Errorf("check completion timing/outcome = %+v, want %+v", actual, expected)
		}
		if actual.Message != "" {
			t.Errorf("structured check event leaked message/output: %+v", actual)
		}
	}
	data, err := os.ReadFile(filepath.Join(runDir, "workflow-events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "check output") || strings.Contains(string(data), "check error") {
		t.Fatalf("workflow events leaked check output: %s", data)
	}
}

func readPipelineCheckEvents(t *testing.T, runDir string) []workflowEventRecord {
	t.Helper()
	file, err := os.Open(filepath.Join(runDir, "workflow-events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var records []workflowEventRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record workflowEventRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("decode workflow event %q: %v", scanner.Text(), err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return records
}

func readPipelineCheckState(t *testing.T, stateDir string) (State, string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(stateDir, "runs"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("run entries = %v, %v; want one", entries, err)
	}
	runDir := filepath.Join(stateDir, "runs", entries[0].Name())
	data, err := os.ReadFile(filepath.Join(runDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	return state, runDir
}
