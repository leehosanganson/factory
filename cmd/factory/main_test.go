package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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

func TestVersionCommandReportsLinkedVersion(t *testing.T) {
	originalVersion := version
	t.Cleanup(func() { version = originalVersion })

	for _, tc := range []struct {
		name    string
		version string
	}{
		{name: "development default", version: "dev"},
		{name: "release linker value", version: "v1.2.3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			version = tc.version
			var out, errOut bytes.Buffer
			if err := run([]string{"version"}, strings.NewReader(""), &out, &errOut); err != nil {
				t.Fatal(err)
			}
			if out.String() != tc.version+"\n" {
				t.Fatalf("version output = %q, want %q", out.String(), tc.version+"\n")
			}
			if errOut.Len() != 0 {
				t.Fatalf("version wrote unexpected stderr: %q", errOut.String())
			}
		})
	}
}

func TestRootHelpAliasesAreConciseAndConsistent(t *testing.T) {
	var out, errOut bytes.Buffer
	var rootHelp string
	for _, args := range [][]string{{"-h"}, {"--help"}, {"help"}} {
		out.Reset()
		if err := run(args, strings.NewReader(""), &out, &errOut); err != nil {
			t.Fatal(err)
		}
		text := strings.Join(strings.Fields(strings.ToLower(out.String())), " ")
		for _, want := range []string{"usage: factory", "software factory workflows", "factory implement", "factory tidy", "factory monitor", "factory job", "factory run", "--gate", "factory version"} {
			if !strings.Contains(text, want) {
				t.Errorf("root %v help missing %q: %s", args, want, out.String())
			}
		}
		for _, omitted := range []string{"full command overview", "origin/<branch>", "dirty safe mode", "30-minute active", "optional placeholders", "approval requires exact", "isolated worktree", "factory job start implementation", "execution and safeguards", "configuration and limits", "factory pipeline", "factory clean", "alias: pipeline", "alias: clean"} {
			if strings.Contains(text, omitted) {
				t.Errorf("root %v help includes full-overview detail %q: %s", args, omitted, out.String())
			}
		}
		if rootHelp != "" && text != rootHelp {
			t.Errorf("root help aliases differ:\n%q\n%q", rootHelp, text)
		}
		rootHelp = text
	}
	out.Reset()
	printRootHelpWithOptions(&out, 80, false, false)
	if !strings.Contains(out.String(), "Usage:") || strings.Contains(out.String(), "full command overview") {
		t.Fatalf("concise root help rendering is incorrect: %s", out.String())
	}
	out.Reset()
	if err := run([]string{"implement", "help"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Implement workflow") || strings.Contains(out.String(), "Execution and safeguards") {
		t.Fatalf("workflow help should remain concise: %s", out.String())
	}
	out.Reset()
	if err := run([]string{"tidy", "help"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Tidy workflow") {
		t.Fatalf("tidy help missing canonical workflow: %s", out.String())
	}
	out.Reset()
	if err := run([]string{"job", "help"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "factory job start implementation") || !strings.Contains(out.String(), "factory job start tidy") || strings.Contains(out.String(), "full command overview") {
		t.Fatalf("command-specific help should remain concise: %s", out.String())
	}
	if err := run([]string{"job", "start", "clean", "unsupported"}, strings.NewReader(""), &out, &errOut); err == nil || !strings.Contains(err.Error(), "unsupported job type") {
		t.Fatalf("clean job type should remain unsupported: err=%v", err)
	}
	err := run([]string{"monitor"}, strings.NewReader(""), &out, &errOut)
	if err == nil || !strings.Contains(err.Error(), "usage: factory monitor <description>") || !strings.Contains(err.Error(), "reset <id>") {
		t.Fatalf("monitor should show canonical usage including reset: err=%v", err)
	}
	if err := run([]string{"babysit"}, strings.NewReader(""), &out, &errOut); err == nil || !strings.Contains(err.Error(), `unknown command "babysit"`) {
		t.Fatalf("removed public babysit command should be rejected: err=%v", err)
	}
}

func TestCommandHelpRoutesBeforeConfigAndWorkflowDispatch(t *testing.T) {
	configHome := t.TempDir()
	factoryConfig := filepath.Join(configHome, "factory")
	if err := os.MkdirAll(factoryConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(factoryConfig, "config.json"), []byte("not valid config"), 0o600); err != nil {
		t.Fatal(err)
	}
	stateHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("XDG_STATE_HOME", stateHome)

	for _, tc := range []struct {
		name string
		args []string
		want []string
		omit []string
	}{
		{name: "implement", args: []string{"implement", "--help"}, want: []string{"Implement workflow", "factory implement"}, omit: []string{"factory pipeline", "Examples:", "Ctrl-C", "interactive terminal"}},
		{name: "tidy focused", args: []string{"tidy", "--help"}, want: []string{"Tidy workflow", "factory tidy"}, omit: []string{"factory clean", "Detached jobs", "factory job", "Monitor management", "Dirty safe mode", "make clean"}},
		{name: "job subcommand", args: []string{"job", "start", "--help"}, want: []string{"Detached jobs", "factory job start implementation", "factory job start tidy", "factory job start monitor"}, omit: []string{"factory run", "Monitor management", "Example:"}},
		{name: "job get canonical", args: []string{"job", "get", "--help"}, want: []string{"factory job get", "--details", "metadata"}, omit: []string{"factory job start", "factory run", "factory job show"}},
		{name: "run subcommand", args: []string{"run", "events", "--help"}, want: []string{"Gated runs", "factory run events"}, omit: []string{"factory job", "Monitor management", "Example:"}},
		{name: "run get canonical", args: []string{"run", "get", "--help"}, want: []string{"factory run get", "--details", "metadata"}, omit: []string{"factory run show"}},
		{name: "monitor canonical", args: []string{"monitor", "get", "--help"}, want: []string{"factory monitor get", "--details", "proposals"}, omit: []string{"factory monitor describe"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			if err := run(tc.args, strings.NewReader(""), &out, &errOut); err != nil {
				t.Fatalf("help with invalid config: %v", err)
			}
			text := strings.Join(strings.Fields(out.String()), " ")
			if errOut.Len() != 0 {
				t.Fatalf("help wrote stderr: %q", errOut.String())
			}
			for _, want := range tc.want {
				if !strings.Contains(text, want) {
					t.Errorf("help missing %q: %s", want, text)
				}
			}
			for _, omitted := range tc.omit {
				if strings.Contains(text, omitted) {
					t.Errorf("focused help unexpectedly includes %q: %s", omitted, text)
				}
			}
		})
	}
	if err := os.Remove(filepath.Join(factoryConfig, "config.json")); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"tidy", "--help"}, {"job", "list", "--help"}, {"run", "list", "--help"}, {"monitor", "list", "--help"}} {
		var out bytes.Buffer
		if err := run(args, strings.NewReader(""), &out, io.Discard); err != nil || out.Len() == 0 {
			t.Errorf("help %v with no config: output=%q err=%v", args, out.String(), err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(stateHome, "factory"))
	if err == nil && len(entries) != 0 {
		t.Fatalf("help started work or created state: %v", entries)
	}
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("inspect state after help: %v", err)
	}
}

func TestRemovedAliasesFailBeforeHelpOrCommandDispatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "pipeline", args: []string{"pipeline", "do work"}, want: `unknown command "pipeline"`},
		{name: "pipeline help flag", args: []string{"pipeline", "--help"}, want: `unknown command "pipeline"`},
		{name: "pipeline help word", args: []string{"pipeline", "help"}, want: `unknown command "pipeline"`},
		{name: "clean", args: []string{"clean"}, want: `unknown command "clean"`},
		{name: "clean help flag", args: []string{"clean", "--help"}, want: `unknown command "clean"`},
		{name: "clean help word", args: []string{"clean", "help"}, want: `unknown command "clean"`},
		{name: "job show", args: []string{"job", "show", "job-id"}, want: `unknown job subcommand "show"`},
		{name: "job show help flag", args: []string{"job", "show", "--help"}, want: `unknown job subcommand "show"`},
		{name: "job show help word", args: []string{"job", "show", "help"}, want: `unknown job subcommand "show"`},
		{name: "run show", args: []string{"run", "show", "run-id"}, want: `unknown run subcommand "show"`},
		{name: "run show help flag", args: []string{"run", "show", "--help"}, want: `unknown run subcommand "show"`},
		{name: "run show help word", args: []string{"run", "show", "help"}, want: `unknown run subcommand "show"`},
		{name: "monitor describe", args: []string{"monitor", "describe", "job-id"}, want: `unknown monitor subcommand "describe"`},
		{name: "monitor describe help flag", args: []string{"monitor", "describe", "--help"}, want: `unknown monitor subcommand "describe"`},
		{name: "monitor describe help word", args: []string{"monitor", "describe", "help"}, want: `unknown monitor subcommand "describe"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			err := run(tc.args, strings.NewReader(""), &out, &errOut)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run(%v) error = %v, want %q", tc.args, err, tc.want)
			}
			if out.Len() != 0 {
				t.Fatalf("rejected alias printed help or output: %q", out.String())
			}
		})
	}
}

func TestCanonicalManagementCommandsStillRoute(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	for _, tc := range []struct {
		args []string
		want string
	}{
		{args: []string{"job", "list"}, want: "No jobs."},
		{args: []string{"run", "list"}, want: "No gated runs."},
		{args: []string{"monitor", "list"}, want: "No monitor jobs."},
	} {
		var out, errOut bytes.Buffer
		if err := run(tc.args, strings.NewReader(""), &out, &errOut); err != nil {
			t.Errorf("run(%v): %v", tc.args, err)
			continue
		}
		if !strings.Contains(out.String(), tc.want) {
			t.Errorf("run(%v) output = %q, want %q", tc.args, out.String(), tc.want)
		}
	}
}

func TestRootHelpFormattingWrapsAtRequestedWidth(t *testing.T) {
	var out bytes.Buffer
	for _, width := range []int{48, 24} {
		out.Reset()
		printRootHelpWithOptions(&out, width, false, false)
		text := out.String()
		if strings.Contains(text, "\033[") {
			t.Fatal("buffer help must never contain ANSI styling")
		}
		for i, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
			if helpTextWidth(line) > width {
				t.Errorf("line %d exceeds configured width (%d): %q", i+1, width, line)
			}
		}
	}
	text := out.String()
	for _, want := range []string{"Usage:", "factory implement", "factory tidy", "factory help"} {
		if !strings.Contains(text, want) {
			t.Errorf("concise help omitted %q: %s", want, text)
		}
	}
	for _, omitted := range []string{"factory implement add a small feature", "Dirty safe mode", "Execution and safeguards"} {
		if strings.Contains(text, omitted) {
			t.Errorf("concise help unexpectedly includes %q: %s", omitted, text)
		}
	}
}

func TestHelpOutputFitsNarrowWidths(t *testing.T) {
	for _, width := range []int{24, 39} {
		t.Run(fmt.Sprintf("width-%d", width), func(t *testing.T) {
			var out bytes.Buffer
			printRootHelpWithOptions(&out, width, false, false)
			for lineNumber, line := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
				if cells := helpTextWidth(line); cells > width {
					t.Errorf("line %d has %d terminal cells, exceeding width %d: %q", lineNumber+1, cells, width, line)
				}
			}
			if !strings.Contains(out.String(), "Usage:") || !strings.Contains(out.String(), "factory implement") {
				t.Errorf("width %d concise help omitted usage or workflow", width)
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
			printRootHelpWithOptions(&out, tc.width, tc.terminal, tc.noColor)
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

func TestGatedImplementRejectsNonInteractiveBeforeAgentOrRunState(t *testing.T) {
	stateDir := t.TempDir()
	configDir := filepath.Join(t.TempDir(), "config")
	factoryConfigDir := filepath.Join(configDir, "factory")
	if err := os.MkdirAll(factoryConfigDir, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "agent-invoked")
	agent := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\ntouch \"$AGENT_MARKER\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q}`, agent, stateDir)
	if err := os.WriteFile(filepath.Join(factoryConfigDir, "config.json"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", configDir)
	t.Setenv("AGENT_MARKER", marker)

	tests := []struct {
		name           string
		stdinTerminal  bool
		stdoutTerminal bool
	}{
		{name: "stdin is not a terminal", stdoutTerminal: true},
		{name: "stdout is not a terminal", stdinTerminal: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			readEnd, writeEnd, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { readEnd.Close(); writeEnd.Close() })
			terminalCheck := func(file *os.File) bool {
				if file == readEnd {
					return tc.stdinTerminal
				}
				if file == os.Stdout {
					return tc.stdoutTerminal
				}
				return false
			}
			err = runPipelineContextWithTerminalCheck(context.Background(), []string{"do the work"}, true, readEnd, os.Stdout, terminalCheck)
			if err == nil || !strings.Contains(err.Error(), "factory implement --gate requires an interactive terminal for approvals") {
				t.Fatalf("gated implementation error = %v", err)
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("agent invoked before terminal check: stat marker error=%v", err)
			}
			if _, err := os.Stat(filepath.Join(stateDir, "runs")); !os.IsNotExist(err) {
				t.Fatalf("gated implementation created run state before terminal check: stat error=%v", err)
			}
		})
	}
}

func TestRemovedPipelineAliasIsRejectedWhileBareInvocationStillRunsImplement(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := run([]string{"pipeline"}, strings.NewReader("task\n"), &out, &errOut); err == nil || !strings.Contains(err.Error(), `unknown command "pipeline"`) {
		t.Fatalf("pipeline alias error = %v", err)
	}

	var errs []error
	for _, args := range [][]string{{"implement"}, nil} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out, errOut bytes.Buffer
			err := run(args, strings.NewReader("task\n"), &out, &errOut)
			if err == nil || !strings.Contains(err.Error(), "interactive terminal") || !strings.Contains(err.Error(), "factory implement") {
				t.Fatalf("non-terminal invocation error = %v", err)
			}
			errs = append(errs, err)
		})
	}
	if len(errs) != 2 || errs[0].Error() != errs[1].Error() {
		t.Fatalf("bare invocation and implement should use the same workflow: errors=%v", errs)
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
		{args: []string{"implement", "--gate", "do", "work"}, gate: true, task: "do work"},
		{args: []string{"--gate", "do", "work"}, gate: true, task: "do work"},
		{args: []string{"implement", "do", "work"}, gate: false, task: "do work"},
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
		t.Fatalf("tidy gate parse = (%v, %v, %v)", gate, task, err)
	}
}

func TestGateParsingSupportsImplementAndBareForms(t *testing.T) {
	for _, args := range [][]string{{"--gate", "a", "task"}, {"a", "--gate", "task"}} {
		gate, task, err := parseGate(args)
		if err != nil || !gate || strings.Join(task, " ") != "a task" {
			t.Fatalf("parseGate(%v) = (%v, %v, %v)", args, gate, task, err)
		}
	}
	if _, _, err := parseGate([]string{"--gate", "--gate"}); err == nil {
		t.Fatal("duplicate --gate accepted")
	}
	for _, args := range [][]string{{"--detach", "-d"}, {"-d", "--detach"}} {
		if _, _, _, err := parseWorkflowOptions(args); err == nil {
			t.Fatalf("duplicate detach flags accepted: %v", args)
		}
	}
	for _, args := range [][]string{{"implement", "--gate", "--detach"}, {"tidy", "-d", "--gate"}} {
		gate, detach, _, err := parseWorkflowOptions(args[1:])
		if err != nil || !gate || !detach {
			t.Fatalf("options %v parse = gate=%v detach=%v err=%v", args, gate, detach, err)
		}
	}
}

func TestGateDetachConflictIsRejectedBeforeConfigLoading(t *testing.T) {
	configHome := t.TempDir()
	factoryConfig := filepath.Join(configHome, "factory")
	if err := os.MkdirAll(factoryConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(factoryConfig, "config.json"), []byte("not valid config"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", configHome)

	var output, errOutput bytes.Buffer
	err := run([]string{"--gate", "-d", "x"}, strings.NewReader(""), &output, &errOutput)
	if err == nil || !strings.Contains(err.Error(), "factory implement --gate cannot be combined with --detach") {
		t.Fatalf("gate/detach conflict = %v, want conflict validation error", err)
	}
	if strings.Contains(err.Error(), "config") || strings.Contains(err.Error(), "invalid") {
		t.Fatalf("configuration was loaded before conflict validation: %v", err)
	}
}

func TestDetachHelpAndGateConflictValidation(t *testing.T) {
	for _, command := range []string{"implement", "tidy"} {
		var output, errors bytes.Buffer
		if err := run([]string{command, "--help"}, strings.NewReader(""), &output, &errors); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(output.String(), "--detach") || !strings.Contains(output.String(), "-d") {
			t.Errorf("%s help omitted detach aliases: %q", command, output.String())
		}
	}
	for _, args := range [][]string{
		{"implement", "--gate", "--detach", "work"},
		{"implement", "-d", "--gate", "work"},
		{"--gate", "-d", "work"},
		{"tidy", "--gate", "--detach"},
		{"tidy", "--detach", "--gate"},
	} {
		var output, errors bytes.Buffer
		err := run(args, strings.NewReader(""), &output, &errors)
		if err == nil || !strings.Contains(err.Error(), "cannot be combined") {
			t.Errorf("CLI accepted conflicting flags %v: %v", args, err)
		}
	}
}

func TestImplementDefaultsToAttachedImplementationJob(t *testing.T) {
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
	for _, name := range []string{"implement"} {
		t.Run(name, func(t *testing.T) {
			state := t.TempDir()
			target, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
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

func TestDetachedImplementCLIReturnsBeforeWorkerCompletes(t *testing.T) {
	state := t.TempDir()
	configHome := t.TempDir()
	factoryConfig := filepath.Join(configHome, "factory")
	if err := os.MkdirAll(factoryConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 0.4\nprintf 'PASS\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(factoryConfig, "config.json"), []byte(fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"5s"}`, script, state)), 0o600); err != nil {
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
	target, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	command := exec.Command(binary, "implement", "-d", "test", "detached")
	command.Dir = target
	command.Env = append(os.Environ(), "XDG_CONFIG_HOME="+configHome, "XDG_STATE_HOME="+state)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("detached implement: %v\n%s", err, output)
	}
	if time.Since(started) > 300*time.Millisecond || !strings.Contains(string(output), "Started implementation job") || strings.Contains(string(output), "requirements completed") {
		t.Fatalf("detached command did not return promptly with job ID: duration=%s output=%q", time.Since(started), output)
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
	if err != nil || len(jobs) != 1 || jobs[0].Type != "implementation" || jobs[0].TaskDescription != "test detached" {
		t.Fatalf("detached CLI job state=%+v err=%v", jobs, err)
	}
}

func TestDetachedTidyCLIUsesDefaultDescriptionAndNeverPublishes(t *testing.T) {
	state := t.TempDir()
	configHome := t.TempDir()
	factoryConfig := filepath.Join(configHome, "factory")
	if err := os.MkdirAll(factoryConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	agent := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\nprintf 'PASS\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(factoryConfig, "config.json"), []byte(fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"5s"}`, agent, state)), 0o600); err != nil {
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

	target := t.TempDir()
	git := func(args ...string) []byte {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = target
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
		return output
	}
	git("init", "-q")
	git("config", "user.name", "Factory Test")
	git("config", "user.email", "factory-test@example.invalid")
	if err := os.WriteFile(filepath.Join(target, "tracked.txt"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", "tracked.txt")
	git("commit", "-qm", "baseline")
	initialHead := strings.TrimSpace(string(git("rev-parse", "HEAD")))
	if err := os.WriteFile(filepath.Join(target, "preexisting.txt"), []byte("keep me\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fakeBin := t.TempDir()
	if err := os.WriteFile(filepath.Join(fakeBin, "make"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(binary, "tidy", "--detach")
	command.Dir = target
	command.Env = append(os.Environ(), "XDG_CONFIG_HOME="+configHome, "XDG_STATE_HOME="+state, "PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("detached tidy CLI: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "Started tidy job ") || !strings.Contains(string(output), "remain unpublished") {
		t.Fatalf("CLI did not report tidy dispatch and nonpublishing mode: %q", output)
	}
	storeRoot, err := factory.JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := factory.NewJobStore(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	var jobs []factory.JobRecord
	waitUntil := time.Now().Add(10 * time.Second)
	for time.Now().Before(waitUntil) {
		jobs, err = store.ListJobs()
		if err == nil && len(jobs) == 1 && jobs[0].Status == "complete" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil || len(jobs) != 1 || jobs[0].Type != "tidy" || jobs[0].Status != "complete" || jobs[0].TaskDescription != "Review, fix, document, and verify the target repository without publishing changes." {
		t.Fatalf("detached tidy job record = %+v err=%v", jobs, err)
	}
	if got := strings.TrimSpace(string(git("rev-parse", "HEAD"))); got != initialHead {
		t.Fatalf("detached tidy published a commit: HEAD=%s want=%s", got, initialHead)
	}
	if got, err := os.ReadFile(filepath.Join(target, "preexisting.txt")); err != nil || string(got) != "keep me\n" {
		t.Fatalf("detached tidy did not preserve existing work: %q err=%v", got, err)
	}
	if status := strings.TrimSpace(string(git("status", "--porcelain"))); status == "" {
		t.Fatal("detached tidy unexpectedly cleaned or committed the pre-existing change")
	}
}

func TestTidyRejectsTaskArgumentsUnlessDetached(t *testing.T) {
	var out, errOut bytes.Buffer
	err := run([]string{"tidy", "unexpected"}, strings.NewReader(""), &out, &errOut)
	if err == nil || !strings.Contains(err.Error(), "factory tidy accepts only --gate") {
		t.Fatalf("tidy argument error = %v", err)
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

func TestMonitorListAndGetPersistedMetadata(t *testing.T) {
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	id := "20260518T120000-0123456789ab"
	jobRoot, err := factory.JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := factory.NewJobStore(jobRoot)
	if err != nil {
		t.Fatal(err)
	}
	var job factory.JobRecord
	metadata := `{"id":"` + id + `","type":"monitor","task_description":"watch task","status":"running","monitor":{"id":"` + id + `","description":"watch task","repo":"owner/repo","pr":12,"head_branch":"feature","status":"running","created_at":"2026-05-18T12:00:00Z","updated_at":"2026-05-18T12:00:00Z"}}`
	if err := json.Unmarshal([]byte(metadata), &job); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(job); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, "monitor", "running"); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendSessionLog(id, "monitor", []byte("[now] worker started\n")); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if err := run([]string{"monitor", "list"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	monitorList := out.String()
	if !strings.Contains(monitorList, id) || !strings.Contains(monitorList, "owner/repo") {
		t.Fatalf("list omitted persisted job: %s", monitorList)
	}
	out.Reset()
	if err := run([]string{"monitor", "get", id, "--details"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "watch task") || !strings.Contains(out.String(), "worker started") {
		t.Fatalf("get omitted metadata/log: %s", out.String())
	}
	if err := run([]string{"monitor", "get", "../escape"}, strings.NewReader(""), &out, &errOut); err == nil {
		t.Fatal("unsafe job id accepted")
	}
}
