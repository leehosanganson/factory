package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/leehosanganson/factory/internal/factory"
)

func TestPrivateJobWorkerInvocationRequiresStoreRoot(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := run([]string{"__job-worker", "job-id"}, strings.NewReader(""), &out, &errOut); err == nil || !strings.Contains(err.Error(), "invalid private worker invocation") {
		t.Fatalf("worker invocation without store root error=%v", err)
	}
	if err := run([]string{"__job-worker", "job-id", "relative-root"}, strings.NewReader(""), &out, &errOut); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("worker invocation with invalid root error=%v", err)
	}
}

func TestHelpAndMonitorUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := run([]string{"-h"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	help := strings.Join(strings.Fields(strings.ToLower(out.String())), " ")
	for _, want := range []string{
		"factory implement", "factory tidy", "factory monitor", "factory pipeline", "factory clean", "factory babysit",
		"factory run list", "factory run stop", "factory job start implementation", "factory job start monitor",
		"factory monitor list", "factory babysit", "factory run events", "factory job attach", "-h, --help",
		"babysit", "make clean", "origin/<branch>", "only if that remote branch does not already exist",
		"pristine mode", "dirty safe mode", "does not stage, commit, or push", "does not require an upstream or origin",
		"not an independent correctness evaluation", "each agent stage runs once", "30-minute active agent-execution budget",
		"no overall job deadline", "agent_timeout (default 60m)", "exact lowercase yes", "optional placeholders: {workdir} and {stage}",
		"approval requires exact lowercase y", "three automatic actions", "isolated worktree", "does not merge the pr", "not forcibly killed", "recoverable failure",
	} {
		if !strings.Contains(help, want) {
			t.Errorf("help missing %q: %s", want, out.String())
		}
	}
	if strings.Contains(help, "retry") {
		t.Fatalf("help must not make retry claims: %s", out.String())
	}
	if err := run([]string{"job", "start", "clean", "unsupported"}, strings.NewReader(""), &out, &errOut); err == nil || !strings.Contains(err.Error(), "unsupported job type") {
		t.Fatalf("clean job type should remain unsupported: err=%v", err)
	}
	for _, command := range []string{"monitor", "babysit"} {
		err := run([]string{command}, strings.NewReader(""), &out, &errOut)
		if err == nil || !strings.Contains(err.Error(), "usage: factory monitor <description>") || !strings.Contains(err.Error(), "reset <id>") {
			t.Fatalf("%s should show canonical monitor usage including reset: err=%v", command, err)
		}
	}
}

func TestHelpFormattingWrapsAndAlignsAtRequestedWidth(t *testing.T) {
	var out bytes.Buffer
	printHelpWithOptions(&out, 48, false, false)
	text := out.String()
	if strings.Contains(text, "\033[") {
		t.Fatal("buffer help must never contain ANSI styling")
	}
	for i, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
		if len([]rune(line)) > 48 {
			t.Errorf("line %d exceeds configured width (%d): %q", i+1, len([]rune(line)), line)
		}
	}
	if !strings.Contains(text, "factory implement") || !strings.Contains(text, "factory tidy") {
		t.Fatalf("help omitted canonical commands: %s", text)
	}
	var descriptionColumns []int
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "  factory ") && strings.Contains(line, "Show this help.") {
			descriptionColumns = append(descriptionColumns, strings.Index(line, "Show this help."))
		}
	}
	if len(descriptionColumns) != 1 || descriptionColumns[0] < 20 {
		t.Fatalf("help command row is not aligned into usage and description columns: %s", text)
	}
	if !strings.Contains(text, "factory implement add a small feature") || !strings.Contains(text, "Dirty safe mode") {
		t.Fatalf("help missing examples or safety detail: %s", text)
	}
}

func TestHelpLongUsageStacksDescriptionAtDefaultWidth(t *testing.T) {
	for _, command := range []helpCommand{
		{
			usage: "factory implement [--gate] [description...]",
			desc:  "Start an implementation job and attach to its output (alias: factory pipeline).",
		},
		{
			usage: "factory job start implementation <description>",
			desc:  "Start a detached implementation job.",
		},
	} {
		t.Run(command.usage, func(t *testing.T) {
			var output bytes.Buffer
			printHelpCommands(&output, []helpCommand{command}, 80)
			got := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")

			usageLines := wrapHelpText(command.usage, 40)
			descLines := wrapHelpText(command.desc, 36)
			want := make([]string, 0, len(usageLines)+len(descLines))
			for _, line := range usageLines {
				want = append(want, "  "+line)
			}
			for _, line := range descLines {
				want = append(want, "    "+line)
			}
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Fatalf("long usage and description should be stacked at width 80:\n got: %q\nwant: %q", got, want)
			}
		})
	}
}

func TestHelpOutputFitsNarrowWidths(t *testing.T) {
	for _, width := range []int{24, 39} {
		t.Run(fmt.Sprintf("width-%d", width), func(t *testing.T) {
			var out bytes.Buffer
			printHelpWithOptions(&out, width, false, false)
			for lineNumber, line := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
				if cells := helpTextWidth(line); cells > width {
					t.Errorf("line %d has %d terminal cells, exceeding width %d: %q", lineNumber+1, cells, width, line)
				}
			}
			if !strings.Contains(out.String(), "    Show this help.") {
				t.Errorf("width %d command descriptions did not stack below usage", width)
			}
		})
	}
}

func TestHelpTextWrappingUsesTerminalCellsForUnicode(t *testing.T) {
	commands := []helpCommand{{usage: "factory 界 command", desc: "e\u0301界 description with Unicode"}}
	for _, width := range []int{24, 39} {
		var output bytes.Buffer
		printHelpCommands(&output, commands, width)
		for lineNumber, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
			if cells := helpTextWidth(line); cells > width {
				t.Errorf("Unicode command line %d has %d cells, exceeding width %d: %q", lineNumber+1, cells, width, line)
			}
		}
	}
	for _, tc := range []struct {
		text  string
		width int
	}{
		{text: "界界界", width: 4},
		{text: "e\u0301e\u0301e\u0301", width: 2},
		{text: "界 e\u0301 界", width: 3},
	} {
		lines := wrapHelpText(tc.text, tc.width)
		if strings.Join(lines, "") == "" {
			t.Fatalf("wrapHelpText(%q, %d) returned no text", tc.text, tc.width)
		}
		for _, line := range lines {
			if cells := helpTextWidth(line); cells > tc.width {
				t.Errorf("wrapped line %q has %d cells, exceeding width %d", line, cells, tc.width)
			}
		}
	}
}

func TestHelpWidthPrefersTerminalThenColumnsThenDefault(t *testing.T) {
	calls := 0
	query := func() (int, error) {
		calls++
		return 62, nil
	}
	if got := helpWidthFromEnvironment("91", query); got != 62 || calls != 1 {
		t.Fatalf("terminal width=%d queries=%d, want 62 and one query", got, calls)
	}
	if got := helpWidthFromEnvironment("invalid", func() (int, error) { return 0, errors.New("not a tty") }); got != 80 {
		t.Fatalf("fallback width=%d, want 80", got)
	}
	if got := helpWidthFromEnvironment("91", func() (int, error) { return 0, errors.New("not a tty") }); got != 91 {
		t.Fatalf("environment fallback width=%d, want 91", got)
	}
}

func TestHelpStylingRequiresTTYColorAndWideEnoughTerminal(t *testing.T) {
	for _, tc := range []struct {
		name     string
		width    int
		terminal bool
		noColor  bool
		wantANSI bool
	}{
		{name: "wide tty", width: 80, terminal: true, wantANSI: true},
		{name: "buffer", width: 80, wantANSI: false},
		{name: "pipe", width: 80, terminal: false, wantANSI: false},
		{name: "color disabled", width: 80, terminal: true, noColor: true, wantANSI: false},
		{name: "narrow tty", width: 39, terminal: true, wantANSI: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			printHelpWithOptions(&out, tc.width, tc.terminal, tc.noColor)
			got := strings.Contains(out.String(), "\033[")
			if got != tc.wantANSI {
				t.Fatalf("ANSI present=%t, want %t", got, tc.wantANSI)
			}
		})
	}
}

func TestRunCommandDispatchesGatedRunControls(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	var out, errOut bytes.Buffer
	if err := run([]string{"run", "list"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatalf("run list dispatch: %v", err)
	}
	if out.String() != "No gated runs.\n" {
		t.Fatalf("run list output=%q", out.String())
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

func TestForegroundAgentHonorsSuppliedContextBudget(t *testing.T) {
	started := make(chan struct{})
	suppliedCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	signalCtx := context.Background()
	agent := foregroundAgent{ctx: signalCtx, agent: waitingContextAgent{started: started}}

	err := agent.RunWithContext(suppliedCtx, "review", "prompt", "task", ".", "log")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("foreground agent error = %v, want cancellation from supplied budget", err)
	}
	select {
	case <-started:
	default:
		t.Fatal("context-aware runner was not started")
	}
	if signalCtx.Err() != nil {
		t.Fatalf("supplied budget cancellation affected outer signal context: %v", signalCtx.Err())
	}
}

type waitingContextAgent struct {
	started chan struct{}
}

func (a waitingContextAgent) Run(string, string, string, string, string) error {
	return errors.New("non-context runner called")
}

func (a waitingContextAgent) RunWithContext(ctx context.Context, _, _, _, _, _ string) error {
	close(a.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestForegroundAgentAllowsStageWithoutStdoutProtocol(t *testing.T) {
	legacy := &legacyForegroundAgent{}
	wrapped := foregroundAgent{ctx: context.Background(), agent: legacy}
	workflow := factory.Workflow{
		Agent: wrapped, Config: factory.Config{StateDir: filepath.Join(t.TempDir(), "state")},
		In: strings.NewReader(""), Out: io.Discard, Workdir: t.TempDir(), Stages: []string{"requirements"},
	}
	if err := workflow.Run("task"); err != nil {
		t.Fatalf("successful stage should not require evaluator stdout protocol: %v", err)
	}
	if legacy.calls != 1 {
		t.Fatalf("legacy agent calls = %d, want one stage invocation", legacy.calls)
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

func TestNonInteractiveImplementPipelineAndBareAliasAreRejected(t *testing.T) {
	var errs []error
	for _, args := range [][]string{{"implement"}, {"pipeline"}, nil} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out, errOut bytes.Buffer
			err := run(args, strings.NewReader("task\n"), &out, &errOut)
			if err == nil || !strings.Contains(err.Error(), "interactive terminal") || !strings.Contains(err.Error(), "factory implement") {
				t.Fatalf("non-terminal invocation error = %v", err)
			}
			errs = append(errs, err)
		})
	}
	if len(errs) != 3 || errs[0].Error() != errs[1].Error() || errs[1].Error() != errs[2].Error() {
		t.Fatalf("bare invocation, implement, and pipeline should use the same workflow: errors=%v", errs)
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

func TestImplementAndPipelineDefaultToAttachedImplementationJob(t *testing.T) {
	script := filepath.Join(t.TempDir(), "fake-agent")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 0.1\ncase \"$2\" in *'Stage completed'*) echo PASS ;; *) echo agent-output ;; esac\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "factory")
	projectRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/factory")
	build.Dir = projectRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build factory: %v\n%s", err, output)
	}
	for _, name := range []string{"implement", "pipeline"} {
		t.Run(name, func(t *testing.T) {
			state := t.TempDir()
			target := t.TempDir()
			configDir := filepath.Join(t.TempDir(), "config", "factory")
			if err := os.MkdirAll(configDir, 0o700); err != nil {
				t.Fatal(err)
			}
			config := fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"5s"}`, script, state)
			if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(binary, name, "complete", "the", "task")
			command.Dir = target
			command.Env = append(os.Environ(), "XDG_CONFIG_HOME="+filepath.Dir(configDir), "XDG_STATE_HOME="+state)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("%s command: %v\n%s", name, err, output)
			}
			if !strings.Contains(string(output), "Started pipeline job ") || !strings.Contains(string(output), "requirements completed") {
				t.Fatalf("%s did not attach and stream worker output: %q", name, output)
			}
			storeRoot, err := factory.JobStateRoot(state)
			if err != nil {
				t.Fatal(err)
			}
			store, err := factory.NewJobStore(storeRoot)
			if err != nil {
				t.Fatal(err)
			}
			jobs, err := store.ListJobs()
			if err != nil || len(jobs) != 1 || jobs[0].Status != "complete" || jobs[0].TaskDescription != "complete the task" || jobs[0].TargetPath != target {
				t.Fatalf("%s job records = %+v, err=%v", name, jobs, err)
			}
		})
	}
}

func TestCleanAndTidyRejectTaskArgumentsIdentically(t *testing.T) {
	var errors []string
	for _, command := range []string{"tidy", "clean"} {
		command := command
		t.Run(command, func(t *testing.T) {
			var out, errOut bytes.Buffer
			err := run([]string{command, "unexpected"}, strings.NewReader(""), &out, &errOut)
			if err == nil || err.Error() != "factory tidy accepts only --gate" {
				t.Fatalf("%s argument error = %v", command, err)
			}
			errors = append(errors, err.Error())
		})
	}
	if len(errors) != 2 || errors[0] != errors[1] {
		t.Fatalf("tidy and clean should share routing and argument validation: %v", errors)
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
	legacyList := out.String()
	if !strings.Contains(legacyList, id) || !strings.Contains(legacyList, "owner/repo") {
		t.Fatalf("list omitted persisted job: %s", legacyList)
	}
	out.Reset()
	if err := run([]string{"monitor", "list"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if out.String() != legacyList {
		t.Fatalf("monitor list differs from legacy babysit list: monitor=%q babysit=%q", out.String(), legacyList)
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
