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
	err := (Runner{Config: Config{Command: script, Args: []string{"{task}", "{system_prompt}"}}}).RunContext(ctx, "babysit", "prompt", "task", dir, filepath.Join(dir, "agent.log"))
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
			if err != nil {
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
