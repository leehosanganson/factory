package factory

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fakeAgent struct {
	outputs map[string][]string
	calls   []string
	tasks   []string
}

func (f *fakeAgent) Run(stage, prompt, task, workdir, logPath string) error {
	f.calls = append(f.calls, stage)
	f.tasks = append(f.tasks, task)
	queue := f.outputs[stage]
	output := "PASS\n"
	if len(queue) > 0 {
		output = queue[0]
		f.outputs[stage] = queue[1:]
	}
	if err := os.WriteFile(logPath, []byte(output), 0o600); err != nil {
		return err
	}
	if strings.HasPrefix(output, "ERROR:") {
		return errors.New(strings.TrimPrefix(output, "ERROR:"))
	}
	return nil
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
	want := []string{"requirements", "evaluate", "implement", "evaluate", "review", "evaluate", "document", "evaluate"}
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

func TestWorkflowSkipsSuccessfulStageAndRetryGatesByDefault(t *testing.T) {
	agent := &fakeAgent{outputs: map[string][]string{"evaluate": {"FAIL\nmissing requirement", "PASS\n", "PASS\n", "PASS\n", "PASS\n"}}}
	var output strings.Builder
	workflow := Workflow{Agent: agent, Config: Config{StateDir: filepath.Join(t.TempDir(), "state")}, In: strings.NewReader(""), Out: &output, Workdir: t.TempDir()}
	if err := workflow.Run("task"); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(agent.calls, ","), "requirements,evaluate,requirements,evaluate,implement,evaluate,review,evaluate,document,evaluate"; got != want {
		t.Fatalf("default ungated workflow calls = %s, want %s", got, want)
	}
	if strings.Contains(output.String(), "Type exactly yes") {
		t.Fatalf("default workflow prompted for approval: %s", output.String())
	}
}

func TestWorkflowRetriesWithGateEnabledOnlyAfterApproval(t *testing.T) {
	agent := &fakeAgent{outputs: map[string][]string{"evaluate": {"FAIL\nmissing requirement", "PASS\n"}}}
	var output strings.Builder
	workflow := Workflow{Agent: agent, Config: Config{StateDir: filepath.Join(t.TempDir(), "state")}, In: strings.NewReader("no\n"), Out: &output, Workdir: t.TempDir(), Gate: true}
	if err := workflow.Run("task"); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(agent.calls, ","), "requirements,evaluate"; got != want {
		t.Fatalf("declined retry calls = %s, want %s", got, want)
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
	if got, want := strings.Join(agent.calls, ","), "requirements,evaluate,implement,evaluate"; got != want {
		t.Fatalf("stopped workflow calls = %s, want %s", got, want)
	}
}

func TestWorkflowRetriesEvaluatorFailureAtMostFourAttempts(t *testing.T) {
	outputs := []string{"FAIL\nnot done", "FAIL\nnot done", "FAIL\nnot done", "FAIL\nnot done"}
	agent := &fakeAgent{outputs: map[string][]string{"evaluate": outputs}}
	// Four evaluator failures, each authorized for retry except the final attempt.
	input := strings.Repeat("yes\n", 3) + "yes\nno\n"
	workflow := Workflow{Agent: agent, Config: Config{StateDir: filepath.Join(t.TempDir(), "state")}, In: strings.NewReader(input), Out: io.Discard, Workdir: t.TempDir(), Gate: true}
	if err := workflow.Run("task"); err != nil {
		t.Fatal(err)
	}
	if len(agent.calls) != 8 {
		t.Fatalf("calls = %d, expected four stage/evaluator pairs", len(agent.calls))
	}
}

func TestWorkflowRetriesFailedStageAndStopsAfterFourAttempts(t *testing.T) {
	outputs := []string{"ERROR: unavailable", "ERROR: unavailable", "ERROR: unavailable", "ERROR: unavailable"}
	agent := &fakeAgent{outputs: map[string][]string{"requirements": outputs}}
	var output strings.Builder
	workflow := Workflow{Agent: agent, Config: Config{StateDir: filepath.Join(t.TempDir(), "state")}, In: strings.NewReader("yes\nyes\nyes\n"), Out: &output, Workdir: t.TempDir(), Gate: true}
	if err := workflow.Run("task"); err != nil {
		t.Fatal(err)
	}
	if len(agent.calls) != 4 {
		t.Fatalf("failed requirements ran %d times, want four maximum attempts", len(agent.calls))
	}
	for _, stage := range agent.calls {
		if stage != "requirements" {
			t.Fatalf("advanced after failed requirements: %v", agent.calls)
		}
	}
	for _, want := range []string{"requirements attempt 1/4 failed in", "Retry this stage?", "Type exactly yes to retry", "log:"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("failure/retry progress missing %q: %s", want, output.String())
		}
	}
}

type retryFeedbackAgent struct {
	calls []struct {
		stage string
		task  string
	}
	firstEvaluatorError bool
}

func (a *retryFeedbackAgent) Run(stage, prompt, task, workdir, logPath string) error {
	a.calls = append(a.calls, struct {
		stage string
		task  string
	}{stage: stage, task: task})
	output := "PASS\n"
	var err error
	if stage == "requirements" && len(a.calls) == 1 {
		output = "stage output from failed attempt\n"
		err = errors.New("stage process exited nonzero")
	} else if stage == "evaluate" && len(a.calls) == 3 {
		if a.firstEvaluatorError {
			output = "evaluator diagnostic output\n"
			err = errors.New("evaluator process exited nonzero")
		} else {
			output = "FAIL\nfix the missing acceptance criterion\n"
		}
	}
	if writeErr := os.WriteFile(logPath, []byte(output), 0o600); writeErr != nil {
		return writeErr
	}
	return err
}

func TestRetryPassesPriorStageFailureAndEvaluatorFindingsIntoTask(t *testing.T) {
	for _, evaluatorError := range []bool{false, true} {
		name := "evaluator findings"
		if evaluatorError {
			name = "evaluator error"
		}
		t.Run(name, func(t *testing.T) {
			agent := &retryFeedbackAgent{firstEvaluatorError: evaluatorError}
			state := filepath.Join(t.TempDir(), "state")
			var output strings.Builder
			workflow := Workflow{
				Agent:   agent,
				Config:  Config{StateDir: state},
				In:      strings.NewReader("yes\nyes\nyes\nyes\nyes\nyes\n"),
				Out:     &output,
				Workdir: t.TempDir(),
				Gate:    true,
			}
			if err := workflow.Run("preserve original requested task"); err != nil {
				t.Fatal(err)
			}
			if len(agent.calls) < 5 || agent.calls[0].stage != "requirements" || agent.calls[1].stage != "requirements" || agent.calls[3].stage != "requirements" {
				t.Fatalf("expected stage failure, retry, evaluator rejection, and retry; calls=%+v", agent.calls)
			}
			stageRetryTask := agent.calls[1].task
			for _, want := range []string{"Target repository:", "Original task:\npreserve original requested task", "stage output from failed attempt", "Previous stage attempt failed", "Agent error: stage process exited nonzero"} {
				if !strings.Contains(stageRetryTask, want) {
					t.Errorf("stage retry task missing %q:\n%s", want, stageRetryTask)
				}
			}
			evaluatorRetryTask := agent.calls[3].task
			for _, want := range []string{"Target repository:", "Original task:\npreserve original requested task", "Feedback from the previous attempt", "Evaluator rejected the previous attempt", "Stage output log:", "Evaluator output/findings:"} {
				if !strings.Contains(evaluatorRetryTask, want) {
					t.Errorf("evaluator retry task missing %q:\n%s", want, evaluatorRetryTask)
				}
			}
			if evaluatorError {
				for _, want := range []string{"evaluator diagnostic output", "Evaluator error: evaluator process exited nonzero"} {
					if !strings.Contains(evaluatorRetryTask, want) {
						t.Errorf("evaluator error context missing %q: %s", want, evaluatorRetryTask)
					}
				}
			} else if !strings.Contains(evaluatorRetryTask, "fix the missing acceptance criterion") {
				t.Errorf("evaluator findings missing from retry context: %s", evaluatorRetryTask)
			}
		})
	}
}

func TestEvaluatorRequiresExactFirstNonEmptyLine(t *testing.T) {
	for _, tc := range []struct {
		output string
		want   bool
	}{
		{"\nPASS\nmore", true},
		{" PASS\n", false},
		{"PASS extra\n", false},
		{"FAIL\nPASS", false},
		{"\n", false},
	} {
		if got := evaluatorPassed(tc.output); got != tc.want {
			t.Errorf("evaluatorPassed(%q) = %v, want %v", tc.output, got, tc.want)
		}
	}
}
