package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/leehosanganson/factory/internal/factory"
)

func TestHelpAndBabysitUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := run([]string{"-h"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "factory pipeline") || !strings.Contains(out.String(), "factory clean") || !strings.Contains(out.String(), "factory babysit") || !strings.Contains(out.String(), "Alias for factory pipeline") || !strings.Contains(out.String(), "babysit approve <id>") || !strings.Contains(out.String(), "make clean") || !strings.Contains(out.String(), "Fallback\ndestination: origin/<branch>") || !strings.Contains(out.String(), "only if that remote branch does not already exist") || !strings.Contains(out.String(), "Pristine mode") || !strings.Contains(out.String(), "dirty safe mode") || !strings.Contains(out.String(), "does not stage, commit, or push") || !strings.Contains(out.String(), "does not require an") {
		t.Fatalf("help missing babysit commands: %s", out.String())
	}
	if err := run([]string{"babysit"}, strings.NewReader(""), &out, &errOut); err == nil || !strings.Contains(err.Error(), "usage: factory babysit") {
		t.Fatalf("babysit should show usage: err=%v", err)
	}
}

func TestWorkflowApprovalCancellationWithPipeLeavesInputOpen(t *testing.T) {
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readEnd.Close()
	defer writeEnd.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stateDir := filepath.Join(t.TempDir(), "state")
	prompted := make(chan struct{})
	workflow := factory.Workflow{
		Agent: passingTestAgent{}, Config: factory.Config{StateDir: stateDir},
		In: &contextStdin{file: readEnd, ctx: ctx}, Out: promptSignalWriter{prompted: prompted},
		Workdir: t.TempDir(), Gate: true, Stages: []string{"requirements"},
	}
	result := make(chan error, 1)
	go func() { result <- workflow.RunContext(ctx, "task") }()
	select {
	case <-prompted:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("workflow did not reach approval prompt")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled workflow error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("workflow did not return promptly after approval cancellation")
	}
	entries, err := os.ReadDir(filepath.Join(stateDir, "runs"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected persisted workflow state, entries=%v err=%v", entries, err)
	}
	state, err := os.ReadFile(filepath.Join(stateDir, "runs", entries[0].Name(), "state.json"))
	if err != nil || !strings.Contains(string(state), `"status": "interrupted"`) {
		t.Fatalf("canceled approval state = %s, %v; want interrupted", state, err)
	}
	if _, err := writeEnd.Write([]byte("yes\n")); err != nil {
		t.Fatalf("canceled prompt closed stdin: %v", err)
	}
}

type passingTestAgent struct{}

func (passingTestAgent) Run(_, _, _, _, logPath string) error {
	return os.WriteFile(logPath, []byte("PASS\n"), 0o600)
}

func (a passingTestAgent) RunWithContext(_ context.Context, stage, prompt, task, workdir, logPath string) error {
	return a.Run(stage, prompt, task, workdir, logPath)
}

type promptSignalWriter struct {
	prompted chan struct{}
}

func (w promptSignalWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "Type exactly yes") {
		select {
		case <-w.prompted:
		default:
			close(w.prompted)
		}
	}
	return io.Discard.Write(p)
}

func TestForegroundContextStopsOnInterruptAndCanBeReleased(t *testing.T) {
	ctx, stop := foregroundContext()
	// Canceling the context releases signal.NotifyContext's signal resources.
	stop()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("stopped foreground context error = %v, want canceled", ctx.Err())
	}
}

func TestForegroundAgentUsesSignalContextForContextAwareRunner(t *testing.T) {
	signalCtx, cancel := context.WithCancel(context.Background())
	cancel()
	agent := foregroundAgent{ctx: signalCtx, agent: cancelAwareTestAgent{}}
	err := agent.RunWithContext(context.Background(), "review", "prompt", "task", ".", "log")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("foreground agent error = %v, want signal context cancellation", err)
	}
}

func TestForegroundAgentFailsClosedWithoutStdoutAwareEvaluator(t *testing.T) {
	legacy := &legacyForegroundAgent{}
	wrapped := foregroundAgent{ctx: context.Background(), agent: legacy}
	workflow := factory.Workflow{
		Agent: wrapped, Config: factory.Config{StateDir: filepath.Join(t.TempDir(), "state")},
		In: strings.NewReader(""), Out: io.Discard, Workdir: t.TempDir(), Stages: []string{"requirements"},
	}
	if err := workflow.Run("task"); err == nil || !strings.Contains(err.Error(), "does not provide stdout protocol output") {
		t.Fatalf("workflow error = %v, want fail-closed stdout protocol error", err)
	}
	if legacy.calls != 4 {
		t.Fatalf("legacy agent calls = %d, want four stage attempts and no evaluator invocation", legacy.calls)
	}
}

type legacyForegroundAgent struct {
	calls int
}

func (a *legacyForegroundAgent) Run(_, _, _, _, logPath string) error {
	a.calls++
	return os.WriteFile(logPath, []byte("PASS\nstderr-only protocol text\n"), 0o600)
}

func (a *legacyForegroundAgent) RunWithContext(_ context.Context, stage, prompt, task, workdir, logPath string) error {
	return a.Run(stage, prompt, task, workdir, logPath)
}

type cancelAwareTestAgent struct{}

func (cancelAwareTestAgent) Run(string, string, string, string, string) error {
	return errors.New("non-context runner called")
}

func (cancelAwareTestAgent) RunWithContext(ctx context.Context, _, _, _, _, _ string) error {
	return ctx.Err()
}

func TestNonInteractivePipelineAndBareAliasAreRejected(t *testing.T) {
	var errs []error
	for _, args := range [][]string{{"pipeline"}, nil} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out, errOut bytes.Buffer
			err := run(args, strings.NewReader("task\n"), &out, &errOut)
			if err == nil || !strings.Contains(err.Error(), "interactive terminal") {
				t.Fatalf("non-terminal invocation error = %v", err)
			}
			errs = append(errs, err)
		})
	}
	if len(errs) != 2 || errs[0].Error() != errs[1].Error() {
		t.Fatalf("bare invocation and explicit pipeline should use the same workflow: errors=%v", errs)
	}
}

func TestTaskEntryPromptsWithOutputGuidanceWhenInitiallyEmpty(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("implement the result\n.\n"))
	var output bytes.Buffer
	task, err := readTask(reader, &output)
	if err != nil {
		t.Fatal(err)
	}
	if task != "implement the result" {
		t.Fatalf("task = %q, want entered task", task)
	}
	if !strings.Contains(output.String(), "Tip:") || !strings.Contains(output.String(), "how it should be verified") {
		t.Fatalf("empty-entry guidance missing: %q", output.String())
	}
}

func TestCLIParsesGateOptionAcrossSubcommands(t *testing.T) {
	for _, tc := range []struct {
		args []string
		gate bool
		task string
	}{
		{args: []string{"pipeline", "--gate", "do", "work"}, gate: true, task: "do work"},
		{args: []string{"--gate", "do", "work"}, gate: true, task: "do work"},
		{args: []string{"pipeline", "do", "work"}, gate: false, task: "do work"},
	} {
		gotGate, gotTask, err := parseGate(tc.args[1:])
		if tc.args[0] == "--gate" {
			gotGate, gotTask, err = parseGate(tc.args)
		}
		if err != nil || gotGate != tc.gate || strings.Join(gotTask, " ") != tc.task {
			t.Fatalf("CLI parse %v = (%v, %v, %v)", tc.args, gotGate, gotTask, err)
		}
	}
	if gate, task, err := parseGate([]string{"--gate"}); err != nil || !gate || len(task) != 0 {
		t.Fatalf("clean gate parse = (%v, %v, %v)", gate, task, err)
	}
}

func TestGateParsingSupportsPipelineAndBareAliasForms(t *testing.T) {
	for _, args := range [][]string{{"--gate", "a", "task"}, {"a", "--gate", "task"}} {
		gate, task, err := parseGate(args)
		if err != nil || !gate || strings.Join(task, " ") != "a task" {
			t.Fatalf("parseGate(%v) = (%v, %v, %v)", args, gate, task, err)
		}
	}
	if _, _, err := parseGate([]string{"--gate", "--gate"}); err == nil {
		t.Fatal("duplicate --gate accepted")
	}
}

func TestPipelineArgumentsBecomeTaskWithoutPrompt(t *testing.T) {
	var out bytes.Buffer
	var gotTask string
	var gotInput io.Reader
	run := func(task string, input io.Reader) error {
		gotTask = task
		gotInput = input
		return nil
	}
	if err := runPipelineTask([]string{"implement", "a", "small", "feature"}, strings.NewReader("unused input"), &out, run); err != nil {
		t.Fatal(err)
	}
	if gotTask != "implement a small feature" {
		t.Fatalf("workflow task = %q, want joined arguments", gotTask)
	}
	if out.Len() != 0 {
		t.Fatalf("argument-based task unexpectedly prompted: %q", out.String())
	}
	if got, err := io.ReadAll(gotInput); err != nil || string(got) != "unused input" {
		t.Fatalf("workflow input = %q, %v; argument-based execution must preserve stdin for approval gates", got, err)
	}
}

func TestBabysitListAndDescribePersistedMetadata(t *testing.T) {
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	_ = factory.DefaultConfig()
	jobRoot := filepath.Join(state, "factory", "jobs")
	id := "20260518T120000-0123456789ab"
	dir := filepath.Join(jobRoot, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	metadata := `{"id":"` + id + `","description":"watch task","repo":"owner/repo","pr":12,"status":"running","created_at":"2026-05-18T12:00:00Z","updated_at":"2026-05-18T12:00:00Z"}`
	if err := os.WriteFile(filepath.Join(dir, "job.json"), []byte(metadata), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "actions.log"), []byte("[now] worker started\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if err := run([]string{"babysit", "list"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), id) || !strings.Contains(out.String(), "owner/repo") {
		t.Fatalf("list omitted persisted job: %s", out.String())
	}
	out.Reset()
	if err := run([]string{"babysit", "describe", id}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "watch task") || !strings.Contains(out.String(), "worker started") {
		t.Fatalf("describe omitted metadata/log: %s", out.String())
	}
	if err := run([]string{"babysit", "describe", "../escape"}, strings.NewReader(""), &out, &errOut); err == nil {
		t.Fatal("unsafe job id accepted")
	}
}
