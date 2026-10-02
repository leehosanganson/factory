package factory

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type seamCheckRunner struct {
	calls int
	env   []string
	write string
	run   func(context.Context) (int, error)
}

func (r *seamCheckRunner) Run(ctx context.Context, _ string, _ []string, output io.Writer, env []string) (int, error) {
	r.calls++
	r.env = append([]string(nil), env...)
	if _, err := io.WriteString(output, r.write); err != nil {
		return -1, err
	}
	if r.run != nil {
		return r.run(ctx)
	}
	return 0, nil
}

func TestWorkflowSharesBoundedCaptureAcrossStagesAndChecksWithoutStoppingChecks(t *testing.T) {
	root := t.TempDir()
	var privateLog strings.Builder
	capture := NewBoundedOutputWriter(context.Background(), &privateLog, 32)
	check := &seamCheckRunner{write: "check output that exceeds whatever remains"}
	workflow := Workflow{
		Agent:  &fakeAgent{outputs: map[string][]string{}},
		Config: Config{StateDir: filepath.Join(root, "state"), PipelineChecks: [][]string{{"check-a"}, {"check-b"}}},
		Out:    io.Discard, Workdir: filepath.Join(root, "work"), Stages: []string{"requirements"},
		OutputWriter: capture,
		ExecutionHook: func(_ context.Context, _, _, _, _, _ string, output io.Writer) error {
			_, err := io.WriteString(output, "stage output before checks")
			return err
		},
		PipelineCheckRunner: check,
		PipelineCheckEnv:    []string{"PATH=/bin", "LANG=C"},
	}
	if err := os.Mkdir(workflow.Workdir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := workflow.Run("run shared output"); err != nil {
		t.Fatal(err)
	}
	if check.calls != 2 {
		t.Fatalf("checks invoked %d times, want both checks after output limit", check.calls)
	}
	if err := capture.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := capture.String(); len(capture.Bytes()) > 32 || !capture.Truncated() || !strings.HasPrefix(got, "stage output") || !strings.Contains(got, "[output truncated]") {
		t.Fatalf("shared capture = %q (%d bytes), truncated=%v", got, len(capture.Bytes()), capture.Truncated())
	}
	if strings.Join(check.env, "\x00") != "PATH=/bin\x00LANG=C" {
		t.Fatalf("check env = %q, want explicit override", check.env)
	}
}

func TestWorkflowCheckRunnerReceivesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	check := &seamCheckRunner{run: func(ctx context.Context) (int, error) {
		cancel()
		<-ctx.Done()
		return -1, ctx.Err()
	}}
	root := t.TempDir()
	workflow := Workflow{
		Agent: &fakeAgent{outputs: map[string][]string{}}, Config: Config{StateDir: filepath.Join(root, "state"), PipelineChecks: [][]string{{"check"}}},
		Out: io.Discard, Workdir: filepath.Join(root, "work"), Stages: []string{"requirements"}, PipelineCheckRunner: check,
	}
	if err := os.Mkdir(workflow.Workdir, 0o700); err != nil {
		t.Fatal(err)
	}
	err := workflow.RunContext(ctx, "cancel check")
	if err == nil || !errors.Is(err, context.Canceled) || check.calls != 1 {
		t.Fatalf("RunContext() = %v, calls=%d; want canceled check failure", err, check.calls)
	}
}

func TestWorkflowSeamDoesNotPublishWithoutPublicationApprovalPath(t *testing.T) {
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"config", "user.email", "test@example.com"}, {"config", "user.name", "Test"}} {
		if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(repo, "tracked"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("git", "-C", repo, "add", "tracked").CombinedOutput(); err != nil {
		t.Fatalf("git add: %v: %s", err, output)
	}
	if output, err := exec.Command("git", "-C", repo, "commit", "-q", "-m", "baseline").CombinedOutput(); err != nil {
		t.Fatalf("git commit: %v: %s", err, output)
	}
	before, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	workflow := Workflow{
		Agent: &fakeAgent{outputs: map[string][]string{}}, Config: Config{StateDir: filepath.Join(root, "state")},
		Out: io.Discard, Workdir: repo, Stages: []string{"requirements"},
		ExecutionHook: func(_ context.Context, _, _, _, _, _ string, _ io.Writer) error {
			return os.WriteFile(filepath.Join(repo, "new-file"), []byte("stage result\n"), 0o600)
		},
	}
	if err := workflow.Run("do not publish"); err != nil {
		t.Fatal(err)
	}
	after, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil || string(after) != string(before) {
		t.Fatalf("workflow changed repository HEAD from %q to %q: %v", strings.TrimSpace(string(before)), strings.TrimSpace(string(after)), err)
	}
}

func TestProcessPipelineCheckRunnerUsesOnlyExplicitEnvironment(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "check.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s|%s|%s\\n' \"$PATH\" \"$LANG\" \"$REST_SERVER_API_KEY\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REST_SERVER_API_KEY", "must-not-leak")
	var output strings.Builder
	code, err := (processPipelineCheckRunner{}).Run(context.Background(), dir, []string{script}, &output, []string{"PATH=/bin", "LANG=C"})
	if err != nil || code != 0 {
		t.Fatalf("check run = %d, %v", code, err)
	}
	if got := strings.TrimSpace(output.String()); got != "/bin|C|" {
		t.Fatalf("check environment output = %q, want allowlisted values and no inherited key", got)
	}
}

func TestRunnerEnvironmentOverrideDoesNotInheritSecrets(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "agent.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s|%s|%s' \"$PATH\" \"$LANG\" \"$REST_SERVER_API_KEY\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REST_SERVER_API_KEY", "must-not-leak")
	var output strings.Builder
	runner := Runner{Config: Config{Command: script, Args: []string{"{task}", "{system_prompt}"}}, Env: []string{"PATH=/bin", "LANG=C"}, OutputWriter: &output}
	if err := runner.RunWithContext(context.Background(), "implement", "prompt", "task", dir, filepath.Join(dir, "agent.log")); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(output.String()); got != "/bin|C|" {
		t.Fatalf("child environment output = %q, want allowlisted values and no inherited key", got)
	}
}
