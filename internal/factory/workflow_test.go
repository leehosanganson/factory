package factory

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

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

func TestWorkflowFailsClosedForAgentWithoutStdoutProtocol(t *testing.T) {
	agent := &legacyProtocolAgent{}
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: filepath.Join(t.TempDir(), "state")},
		In: strings.NewReader(""), Out: io.Discard, Workdir: t.TempDir(), Stages: []string{"requirements"},
	}
	if err := workflow.Run("task"); err == nil || !strings.Contains(err.Error(), "does not provide stdout protocol output") {
		t.Fatalf("legacy agent error = %v, want fail-closed stdout protocol error", err)
	}
	if agent.calls != 4 {
		t.Fatalf("workflow made %d evaluator calls, want four retries", agent.calls)
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

func TestWorkflowEvaluatorUsesStdoutProtocolAndRetainsCombinedLog(t *testing.T) {
	for _, tc := range []struct {
		name     string
		stdout   string
		wantPass bool
	}{
		{name: "PASS despite stderr warning", stdout: "PASS\n", wantPass: true},
		{name: "FAIL on stdout", stdout: "FAIL\n"},
		{name: "malformed stdout", stdout: " PASS\nPASS\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			script := filepath.Join(dir, "agent.sh")
			body := "#!/bin/sh\nif [ \"$1\" = evaluate ]; then printf '%s\\n' '[pi-web-access] Dynamic tool activation requires Pi 0.86.1 or newer; web tools remain eagerly available.' >&2; printf '%s' \"$EVAL_OUTPUT\"; else printf 'stage output\\n'; fi\n"
			if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("EVAL_OUTPUT", tc.stdout)
			stateDir := filepath.Join(dir, "state")
			var output strings.Builder
			workflow := Workflow{
				Agent:  Runner{Config: Config{Command: script, Args: []string{"{stage}", "{task}", "{system_prompt}"}}},
				Config: Config{StateDir: stateDir}, In: strings.NewReader(""), Out: &output,
				Workdir: t.TempDir(), Stages: []string{"requirements"},
			}
			err := workflow.Run("task")
			if (err == nil) != tc.wantPass {
				t.Fatalf("workflow error = %v, want pass=%v", err, tc.wantPass)
			}
			entries, readErr := os.ReadDir(filepath.Join(stateDir, "runs"))
			if readErr != nil || len(entries) != 1 {
				t.Fatalf("run directory entries=%v err=%v", entries, readErr)
			}
			evaluatorLog := filepath.Join(stateDir, "runs", entries[0].Name(), "01-evaluate-requirements.log")
			log, readErr := os.ReadFile(evaluatorLog)
			if readErr != nil {
				t.Fatal(readErr)
			}
			warning := "[pi-web-access] Dynamic tool activation requires Pi 0.86.1 or newer; web tools remain eagerly available."
			if !strings.HasPrefix(string(log), warning) || !strings.Contains(string(log), strings.TrimSpace(tc.stdout)) {
				t.Fatalf("evaluator combined log omitted or reordered streams: %q", log)
			}
		})
	}
}

func TestWorkflowEvaluatorTaskExplainsRunningStateAndPreservesStrictStdoutProtocol(t *testing.T) {
	agent := &fakeAgent{outputs: map[string][]string{}}
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: filepath.Join(t.TempDir(), "state")},
		In: strings.NewReader(""), Out: io.Discard, Workdir: t.TempDir(), Stages: []string{"review"},
	}
	if err := workflow.Run("verify the requested behavior"); err != nil {
		t.Fatal(err)
	}
	if len(agent.calls) != 2 || agent.calls[1] != "evaluate" {
		t.Fatalf("workflow calls = %v, want one stage followed by its evaluator", agent.calls)
	}
	task := agent.tasks[1]
	for _, want := range []string{
		"The stage agent already exited successfully; otherwise this evaluator would not run.",
		"The workflow state remains status=running during evaluation by design, until evaluator acceptance is recorded.",
		"The evaluator process being active is expected.",
		"Do not count status=running or this evaluator being active alone as failure or incompleteness.",
		"Verify the requested work and stage output on substance against the original task and workflow requirements; keep those content and quality checks strict.",
		"put PASS as the first non-empty stdout line only if the stage succeeded and materially satisfies its requirements; otherwise put FAIL first.",
	} {
		if !strings.Contains(task, want) {
			t.Errorf("generated evaluator task missing %q:\n%s", want, task)
		}
	}
	for _, tc := range []struct {
		output string
		want   bool
	}{
		{output: "PASS\\nfindings", want: false},
		{output: "PASS\nfindings", want: true},
		{output: " PASS\n", want: false},
		{output: "FAIL\nPASS", want: false},
	} {
		if got := evaluatorPassed(tc.output); got != tc.want {
			t.Errorf("evaluatorPassed(%q) = %v, want %v", tc.output, got, tc.want)
		}
	}
}

func TestWorkflowSuccessfulEvaluationPrintsQuietVerdictAndLogPath(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	agent := &fakeAgent{outputs: map[string][]string{"evaluate": {"PASS\nprivate evaluator details"}}}
	var output strings.Builder
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader(""), Out: &output, Workdir: t.TempDir(), Stages: []string{"requirements"}}
	if err := workflow.Run("task"); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if !strings.Contains(text, "Evaluation: PASS — requirements; log:") {
		t.Fatalf("PASS verdict with evaluator log path missing: %s", text)
	}
	if strings.Contains(text, "private evaluator details") {
		t.Fatalf("successful evaluator output should remain quiet: %s", text)
	}
	if status := workflowRunStatus(t, stateDir); status != "complete" {
		t.Fatalf("successful run status = %q, want complete", status)
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
	if status := workflowRunStatus(t, stateDir); status != "interrupted" {
		t.Fatalf("canceled run status = %q, want interrupted", status)
	}
}

func TestWorkflowCancellationAfterStageRejectionPersistsInterruptedStatus(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := cancelOnPromptReader{cancel: cancel}
	workflow := Workflow{
		Agent:  &fakeAgent{outputs: map[string][]string{"evaluate": {"FAIL\nmissing requirement"}}},
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

func TestWorkflowPromptCancellationInterruptsApprovalAndRetry(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outputs map[string][]string
	}{
		{name: "approval"},
		{name: "retry", outputs: map[string][]string{"evaluate": {"FAIL\nmissing requirement"}}},
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
	stateDir := filepath.Join(t.TempDir(), "state")
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader("no\n"), Out: &output, Workdir: t.TempDir(), Gate: true}
	if err := workflow.Run("task"); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(agent.calls, ","), "requirements,evaluate"; got != want {
		t.Fatalf("declined retry calls = %s, want %s", got, want)
	}
	if status := workflowRunStatus(t, stateDir); status != "stopped" {
		t.Fatalf("declined retry status = %q, want stopped", status)
	}
	if !strings.Contains(output.String(), "Evaluation: FAIL — requirements") || !strings.Contains(output.String(), "missing requirement") || !strings.Contains(output.String(), "Evaluator log:") {
		t.Fatalf("failure findings/log not reported: %s", output.String())
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
	panic("evaluator panic")
}

func TestWorkflowRestoresProgressOnAgentAndEvaluatorPanic(t *testing.T) {
	for _, stage := range []string{"requirements", "evaluate"} {
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
	if strings.ContainsAny(text, "\x1b\a\x00") || strings.Contains(text, "hidden title") || !strings.Contains(text, "failed") {
		t.Fatalf("failed agent output was not safely presented: %q", text)
	}
	if status := workflowRunStatus(t, stateDir); status != "failed" {
		t.Fatalf("failed stage status = %q, want failed", status)
	}
}

func TestEvaluatorProtocolUsesRawLogBeforeTerminalSanitization(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	agent := &fakeAgent{outputs: map[string][]string{"evaluate": {
		"\x1b[31mPASS\x1b[0m\\nlooks good",
		"\x1b[31mPASS\x1b[0m\\nlooks good",
		"\x1b[31mPASS\x1b[0m\\nlooks good",
		"\x1b[31mPASS\x1b[0m\\nlooks good",
	}}}
	var output strings.Builder
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader(""), Out: &output, Workdir: t.TempDir(), Stages: []string{"requirements"}}
	if err := workflow.Run("task"); err == nil {
		t.Fatal("terminal styling around PASS must not satisfy the raw evaluator protocol")
	}
	if strings.ContainsAny(output.String(), "\x1b") {
		t.Fatalf("evaluator output exposed terminal controls: %q", output.String())
	}
	if status := workflowRunStatus(t, stateDir); status != "failed" {
		t.Fatalf("invalid evaluator protocol status = %q, want failed", status)
	}
}

func TestWorkflowEvaluatorInvocationErrorIsReportedAndFailsRun(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	agent := &fakeAgent{outputs: map[string][]string{"evaluate": {"ERROR: evaluator unavailable", "ERROR: evaluator unavailable", "ERROR: evaluator unavailable", "ERROR: evaluator unavailable"}}}
	var output strings.Builder
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader("yes\nyes\nyes\n"), Out: &output, Workdir: t.TempDir(), Stages: []string{"requirements"}}
	if err := workflow.Run("task"); err == nil || !strings.Contains(err.Error(), "evaluator failed after attempt 4/4") {
		t.Fatalf("exhausted evaluator invocation errors should be returned, got %v", err)
	}
	if !strings.Contains(output.String(), "Evaluation: ERROR — requirements") || !strings.Contains(output.String(), "evaluator unavailable") || !strings.Contains(output.String(), "Evaluator log:") {
		t.Fatalf("evaluator error details/log missing: %s", output.String())
	}
	if status := workflowRunStatus(t, stateDir); status != "failed" {
		t.Fatalf("terminal evaluator error status = %q, want failed", status)
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

func TestWorkflowRetriesEvaluatorFailureAtMostFourAttempts(t *testing.T) {
	outputs := []string{"FAIL\nnot done", "FAIL\nnot done", "FAIL\nnot done", "FAIL\nnot done"}
	agent := &fakeAgent{outputs: map[string][]string{"evaluate": outputs}}
	// Four evaluator failures, each authorized for retry except the final attempt.
	input := strings.Repeat("yes\n", 3) + "yes\nno\n"
	stateDir := filepath.Join(t.TempDir(), "state")
	var output strings.Builder
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader(input), Out: &output, Workdir: t.TempDir(), Gate: true}
	if err := workflow.Run("task"); err == nil || !strings.Contains(err.Error(), "evaluator rejected the stage after attempt 4/4") {
		t.Fatalf("exhausted evaluator rejections must fail the workflow, got %v", err)
	}
	if len(agent.calls) != 8 {
		t.Fatalf("calls = %d, expected four stage/evaluator pairs", len(agent.calls))
	}
	if status := workflowRunStatus(t, stateDir); status != "failed" {
		t.Fatalf("exhausted evaluator rejection status = %q, want failed", status)
	}
	if !strings.Contains(output.String(), "Retrying requirements: evaluator rejected the stage; starting attempt 2/4") {
		t.Fatalf("retry reason and next attempt missing: %s", output.String())
	}
}

func TestWorkflowDeclinedAgentRetryStopsWithoutFailingRun(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	agent := &fakeAgent{outputs: map[string][]string{"requirements": {"ERROR: unavailable"}}}
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader("no\n"), Out: io.Discard, Workdir: t.TempDir(), Gate: true, Stages: []string{"requirements"}}
	if err := workflow.Run("task"); err != nil {
		t.Fatalf("declined retry should stop, not fail, the run: %v", err)
	}
	if status := workflowRunStatus(t, stateDir); status != "stopped" {
		t.Fatalf("declined agent retry status = %q, want stopped", status)
	}
}

func TestWorkflowRetriesFailedStageAndStopsAfterFourAttempts(t *testing.T) {
	outputs := []string{"ERROR: unavailable", "ERROR: unavailable", "ERROR: unavailable", "ERROR: unavailable"}
	agent := &fakeAgent{outputs: map[string][]string{"requirements": outputs}}
	var output strings.Builder
	stateDir := filepath.Join(t.TempDir(), "state")
	workflow := Workflow{Agent: agent, Config: Config{StateDir: stateDir}, In: strings.NewReader("yes\nyes\nyes\n"), Out: &output, Workdir: t.TempDir(), Gate: true}
	if err := workflow.Run("task"); err == nil || !strings.Contains(err.Error(), "agent failed after attempt 4/4") {
		t.Fatalf("exhausted agent errors should be returned, got %v", err)
	}
	if len(agent.calls) != 4 {
		t.Fatalf("failed requirements ran %d times, want four maximum attempts", len(agent.calls))
	}
	for _, stage := range agent.calls {
		if stage != "requirements" {
			t.Fatalf("advanced after failed requirements: %v", agent.calls)
		}
	}
	for _, want := range []string{"requirements attempt 1/4 failed in", "Retry this stage?", "Type exactly yes to retry", "log:", "Retrying requirements: agent invocation failed", "starting attempt 2/4", "maximum 4 attempts"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("failure/retry progress missing %q: %s", want, output.String())
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
	firstEvaluatorError bool
}

func (a *retryFeedbackAgent) RunWithOutputContext(_ context.Context, stage, prompt, task, workdir, logPath string) (string, error) {
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
