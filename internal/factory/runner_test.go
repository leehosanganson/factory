package factory

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunnerRunContextStopsAgentWhenCanceled(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "slow-agent.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexec sleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := (Runner{Config: Config{Command: script, Args: []string{"{task}", "{system_prompt}"}}}).RunContext(ctx, "monitor", "prompt", "task", dir, filepath.Join(dir, "agent.log"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled agent error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("canceled agent took %s to return", elapsed)
	}
}

func TestRunnerRunWithContextPreservesConfiguredTimeout(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "slow-agent.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexec sleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := time.Now()
	err := (Runner{Config: Config{Command: script, Args: []string{"{task}", "{system_prompt}"}, AgentTimeout: "100ms"}}).RunWithContext(ctx, "implement", "prompt", "task", dir, filepath.Join(dir, "agent.log"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed-out agent error = %v, want configured context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("timed-out agent took %s to return", elapsed)
	}
}

func TestRunnerRunUsesConfiguredTimeout(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "slow-agent.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexec sleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err := (Runner{Config: Config{Command: script, Args: []string{"{task}", "{system_prompt}"}, AgentTimeout: "100ms"}}).Run("implement", "prompt", "task", dir, filepath.Join(dir, "agent.log"))
	if err == nil {
		t.Fatal("agent exceeded configured timeout without error")
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("timed-out agent took %s to return", elapsed)
	}
}

func TestRunnerRunContextUsesOnlyCallerContext(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "short-agent.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 0.15\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := (Runner{Config: Config{Command: script, Args: []string{"{task}", "{system_prompt}"}, AgentTimeout: "10ms"}}).RunContext(ctx, "implement", "prompt", "task", dir, filepath.Join(dir, "agent.log"))
	if err != nil {
		t.Fatalf("RunContext applied configured timeout instead of caller context: %v", err)
	}
}

func TestProgressFiltersOnlyKnownWarningAndLeavesTranscriptRaw(t *testing.T) {
	dir := t.TempDir()
	warning := "[pi-web-access] Dynamic tool activation requires Pi 0.86.1 or newer; web tools remain eagerly available."
	script := filepath.Join(dir, "agent.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s\\n' "+shellQuote(warning)+" >&2\nprintf '%s\\n' 'other activity'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "agent.log")
	if err := (Runner{Config: Config{Command: script, Args: []string{"{task}", "{system_prompt}"}}}).RunContext(context.Background(), "implement", "prompt", "task", dir, logPath); err != nil {
		t.Fatal(err)
	}
	rendered := strings.Join(readProgressLog(logPath), "\\n")
	if strings.Contains(rendered, "Dynamic tool activation") || strings.Contains(rendered, "[pi-web-access]") || !strings.Contains(rendered, "other activity") {
		t.Fatalf("rendered activity not precisely filtered: %q", rendered)
	}
	stored, err := os.ReadFile(logPath)
	if err != nil || !strings.Contains(string(stored), warning) {
		t.Fatalf("raw transcript did not preserve warning: %q err=%v", stored, err)
	}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func TestConfiguredStatusAdapterAddsNoToolsOnlyForPi(t *testing.T) {
	for _, tc := range []struct {
		command string
		want    bool
	}{
		{command: "pi", want: true},
		{command: "/opt/bin/pi", want: true},
		{command: "custom-agent", want: false},
	} {
		got := configuredSecondaryStatusCall(Config{Command: tc.command, Args: []string{"-p", "--append-system-prompt", "{system_prompt}", "{task}"}}, t.TempDir())
		statusConfig := secondaryStatusConfig(Config{Command: tc.command, Args: []string{"-p", "--append-system-prompt", "{system_prompt}", "{task}"}})
		has := false
		for _, arg := range statusConfig.Args {
			has = has || arg == "--no-tools"
		}
		if has != tc.want || got == nil {
			t.Errorf("status adapter command %q no-tools=%v want %v", tc.command, has, tc.want)
		}
		if tc.want && statusConfig.Args[0] != "--no-tools" {
			t.Errorf("Pi status call did not prepend --no-tools: %#v", statusConfig.Args)
		}
		if !tc.want && strings.Join(statusConfig.Args, " ") != "-p --append-system-prompt {system_prompt} {task}" {
			t.Errorf("custom adapter arguments were changed: %#v", statusConfig.Args)
		}
	}
}

func TestRunnerReturnsStdoutProtocolAndLogsBothStreams(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "agent.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s\\n' '[pi-web-access] Dynamic tool activation requires Pi 0.86.1 or newer; web tools remain eagerly available.' >&2\nprintf 'PASS\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "agent.log")
	stdout, err := (Runner{Config: Config{Command: script, Args: []string{"{task}", "{system_prompt}"}}}).RunWithOutputContext(context.Background(), "implement", "prompt", "task", dir, logPath)
	if err != nil {
		t.Fatal(err)
	}
	if stdout != "PASS\n" {
		t.Fatalf("protocol stdout = %q, want PASS line", stdout)
	}
	log, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	warning := "[pi-web-access] Dynamic tool activation requires Pi 0.86.1 or newer; web tools remain eagerly available."
	if !strings.HasPrefix(string(log), warning) || !strings.Contains(string(log), "PASS") {
		t.Fatalf("combined log must retain stderr warning first and stdout PASS: %q", log)
	}
}

func TestRunnerBoundsProtocolCaptureAndPreservesFirstNonemptyLine(t *testing.T) {
	for _, tc := range []struct {
		name     string
		body     string
		want     string
		wantPass bool
		wantErr  bool
	}{
		{
			name: "large empty prefix before PASS",
			body: "head -c 1048576 /dev/zero | tr '\\000' ' '; printf '\\nPASS\\n'",
			want: "PASS\n", wantPass: true,
		},
		{
			name:     "oversized first nonempty line before PASS",
			body:     "printf 'FAIL'; head -c 1048576 /dev/zero | tr '\\000' 'x'; printf '\\nPASS\\n'",
			wantPass: false,
			wantErr:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			script := filepath.Join(dir, "agent.sh")
			if err := os.WriteFile(script, []byte("#!/bin/sh\n"+tc.body+"\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			logPath := filepath.Join(dir, "agent.log")
			stdout, err := (Runner{Config: Config{Command: script, Args: []string{"{task}", "{system_prompt}"}}}).RunWithOutputContext(context.Background(), "implement", "prompt", "task", dir, logPath)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "exceeds") {
					t.Fatalf("oversized protocol error = %v, want bounded-capture rejection", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if len(stdout) > stdoutProtocolCaptureLimit {
				t.Fatalf("protocol capture length=%d exceeds limit %d", len(stdout), stdoutProtocolCaptureLimit)
			}
			if got := stdoutFirstLinePasses(stdout); got != tc.wantPass {
				t.Fatalf("captured protocol passed=%v, want %v; output length=%d", got, tc.wantPass, len(stdout))
			}
			if tc.want != "" && stdout != tc.want {
				t.Fatalf("captured protocol=%q, want %q", stdout, tc.want)
			}
			info, err := os.Stat(logPath)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() < 1<<20 {
				t.Fatalf("combined log size=%d, want full output >= 1 MiB", info.Size())
			}
		})
	}
}

func TestRunnerCapturesAndValidatesPlanSizedProtocolWithoutChangingWhitespace(t *testing.T) {
	plan := `{"subtasks":[{"id":"model","task":"Implement the model","files":["internal/model.go"],"depends_on":[]},{"id":"tests","task":"Add model tests","files":["internal/model_test.go"],"depends_on":[]}]}`
	const trailingWhitespaceBytes = 12 * 1024
	protocol := plan + strings.Repeat(" ", trailingWhitespaceBytes)
	if len(protocol) <= 8*1024 || len(protocol) > maxSubtaskPlanBytes {
		t.Fatalf("test protocol size=%d, want between 8 KiB and %d bytes", len(protocol), maxSubtaskPlanBytes)
	}

	for _, tc := range []struct {
		name       string
		output     string
		wantError  bool
		wantOutput string
	}{
		{name: "within plan limit", output: protocol, wantOutput: protocol},
		{name: "over plan limit", output: protocol + strings.Repeat("x", maxSubtaskPlanBytes), wantError: true, wantOutput: protocol + strings.Repeat("x", maxSubtaskPlanBytes-len(protocol))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			script := filepath.Join(dir, "agent.sh")
			if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' '"+tc.output+"'\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			stdout, err := (Runner{Config: Config{Command: script, Args: []string{"{task}", "{system_prompt}"}}}).RunWithOutputContext(context.Background(), "plan", "prompt", "task", dir, filepath.Join(dir, "agent.log"))
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "exceeds") {
					t.Fatalf("over-limit protocol error = %v, want bounded-capture rejection", err)
				}
			} else if err != nil {
				t.Fatalf("within-limit protocol rejected: %v", err)
			}
			if stdout != tc.wantOutput {
				t.Fatalf("captured protocol length=%d, want exact length=%d", len(stdout), len(tc.wantOutput))
			}
			if len(stdout) > maxSubtaskPlanBytes {
				t.Fatalf("captured protocol length=%d exceeds limit %d", len(stdout), maxSubtaskPlanBytes)
			}
			if !tc.wantError {
				if _, err := validateImplementationPlan([]byte(stdout)); err != nil {
					t.Fatalf("captured protocol failed plan validation: %v", err)
				}
				if !strings.HasSuffix(stdout, strings.Repeat(" ", trailingWhitespaceBytes)) {
					t.Fatal("captured protocol did not preserve trailing JSON whitespace")
				}
			}
		})
	}
}

func TestRunnerExpandsPlaceholdersAsDistinctArguments(t *testing.T) {
	dir := t.TempDir()
	capture := filepath.Join(dir, "capture")
	script := filepath.Join(dir, "agent.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$CAPTURE\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CAPTURE", capture)
	cfg := Config{Command: script, Args: []string{"{stage}", "{task}", "{system_prompt}", "{workdir}"}}
	log := filepath.Join(dir, "logs", "agent.log")
	if err := (Runner{Config: cfg}).Run("implement", "prompt; not shell", "task with spaces", dir, log); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(got), "\n"), "\n")
	want := []string{"implement", "task with spaces", "prompt; not shell", dir}
	if strings.Join(lines, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("argv = %#v, want %#v", lines, want)
	}
	if _, err := os.Stat(log); err != nil {
		t.Fatalf("log was not created: %v", err)
	}
}
