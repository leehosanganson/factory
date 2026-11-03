package factory

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/leehosanganson/factory/internal/workflow"
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
func (f *fakeAgent) WithOutputWriter(output io.Writer) workflow.Agent {
	return writerAgent{agent: f, output: output}
}

type writerAgent struct {
	agent  *fakeAgent
	output io.Writer
}

func (w writerAgent) Run(stage, prompt, task, workdir, logPath string) error {
	return w.agent.Run(stage, prompt, task, workdir, logPath)
}
func (w writerAgent) RunWithContext(ctx context.Context, stage, prompt, task, workdir, logPath string) error {
	text, err := w.agent.RunWithOutputContext(ctx, stage, prompt, task, workdir, logPath)
	if text != "" {
		_, writeErr := io.WriteString(w.output, text)
		if err == nil {
			err = writeErr
		}
	}
	return err
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

type blockingStatusTestAgent struct {
	started, release chan struct{}
	once             sync.Once
}

func (a *blockingStatusTestAgent) Run(string, string, string, string, string) error {
	return errors.New("context-aware execution required")
}
func (a *blockingStatusTestAgent) RunWithContext(ctx context.Context, _, _, _, _, logPath string) error {
	a.once.Do(func() { close(a.started) })
	select {
	case <-a.release:
		return os.WriteFile(logPath, []byte("primary finished\n"), 0o600)
	case <-ctx.Done():
		return ctx.Err()
	}
}
func workflowRunState(t *testing.T, stateDir string) workflow.State {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(stateDir, "runs"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected one workflow run, entries=%v err=%v", entries, err)
	}
	data, err := os.ReadFile(filepath.Join(stateDir, "runs", entries[0].Name(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state workflow.State
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	return state
}
func workflowRunStatus(t *testing.T, stateDir string) string {
	return workflowRunState(t, stateDir).Status
}
func intPointer(v int) *int              { return &v }
func timePointer(v time.Time) *time.Time { return &v }
func readPipelineCheckEvents(t *testing.T, dir string) []workflow.WorkflowEventRecord {
	t.Helper()
	file, err := os.Open(filepath.Join(dir, "workflow-events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var records []workflow.WorkflowEventRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record workflow.WorkflowEventRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return records
}
func assertPipelineCheckEvents(t *testing.T, dir string, want []workflow.WorkflowEventRecord) {
	t.Helper()
	got := readPipelineCheckEvents(t, dir)
	var lifecycle []workflow.WorkflowEventRecord
	for _, event := range got {
		if event.Type == "check.started" || event.Type == "check.completed" {
			lifecycle = append(lifecycle, event)
		}
	}
	if len(lifecycle) != len(want) {
		t.Fatalf("check lifecycle count=%d want=%d", len(lifecycle), len(want))
	}
	for i := range want {
		if lifecycle[i].Type != want[i].Type || lifecycle[i].Outcome != want[i].Outcome || lifecycle[i].Transcript != want[i].Transcript || strings.Join(lifecycle[i].Command, "\x00") != strings.Join(want[i].Command, "\x00") {
			t.Errorf("event %d=%+v want %+v", i, lifecycle[i], want[i])
		}
	}
}
func initTestGitRepo(t *testing.T) string {
	t.Helper()
	dir := resolvedTestPath(t, t.TempDir())
	for _, args := range [][]string{{"init", "-q", dir}, {"-C", dir, "config", "user.email", "factory-test@example.invalid"}, {"-C", dir, "config", "user.name", "Factory Test"}} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v failed: %v: %s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("ignored.out\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"-C", dir, "add", "README.md", ".gitignore"}, {"-C", dir, "commit", "-q", "-m", "initial"}} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v failed: %v: %s", args, err, output)
		}
	}
	return dir
}
