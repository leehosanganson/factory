package factory

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type processCall struct {
	Stage   string `json:"stage"`
	Task    string `json:"task"`
	Workdir string `json:"workdir"`
}

// TestWorkflowIntegrationHelper is invoked as a child process by the integration test.
func TestWorkflowIntegrationHelper(t *testing.T) {
	if os.Getenv("FACTORY_INTEGRATION_HELPER") != "1" {
		return
	}
	args := os.Args
	separator := -1
	for i, arg := range args {
		if arg == "--" {
			separator = i
			break
		}
	}
	if separator < 0 || len(args) != separator+5 {
		t.Fatalf("expected stage, task, workdir, and system prompt arguments, got %q", args)
	}
	call := processCall{Stage: args[separator+1], Task: args[separator+2], Workdir: args[separator+3]}
	tracePath := os.Getenv("FACTORY_INTEGRATION_TRACE")
	file, err := os.OpenFile(tracePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(file).Encode(call); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if call.Stage == "requirements" {
		countPath := os.Getenv("FACTORY_INTEGRATION_RETRY_COUNT")
		count := 0
		data, err := os.ReadFile(countPath)
		if err == nil {
			if _, err := fmt.Sscanf(string(data), "%d", &count); err != nil {
				t.Fatal(err)
			}
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		count++
		if err := os.WriteFile(countPath, []byte(fmt.Sprint(count)), 0o600); err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			t.Fatalf("first stage invocation should fail workflow")
		}
	}
}

func TestWorkflowIntegrationFailsAfterOneRunnerInvocationAndKeepsArtifactsOutsideTarget(t *testing.T) {
	base := resolvedTestPath(t, t.TempDir())
	target := filepath.Join(base, "target")
	state := filepath.Join(base, "state")
	actualState := filepath.Join(base, "actual-state")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(actualState, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(actualState, state); err != nil {
		t.Skipf("state directory symlinks unavailable: %v", err)
	}
	trace := filepath.Join(base, "processes.jsonl")
	counter := filepath.Join(base, "invocation-count")
	t.Setenv("FACTORY_INTEGRATION_HELPER", "1")
	t.Setenv("FACTORY_INTEGRATION_TRACE", trace)
	t.Setenv("FACTORY_INTEGRATION_RETRY_COUNT", counter)
	marker := filepath.Join(target, "INJECTED")
	task := "task with spaces; touch " + marker
	cfg := Config{
		Command:  os.Args[0],
		Args:     []string{"-test.run=^TestWorkflowIntegrationHelper$", "--", "{stage}", "{task}", "{workdir}", "{system_prompt}"},
		StateDir: state,
	}
	var output strings.Builder
	workflow := Workflow{
		Agent:   Runner{Config: cfg},
		Config:  cfg,
		In:      strings.NewReader("yes\nyes\nyes\nyes\nyes\n"),
		Out:     &output,
		Workdir: target,
		Gate:    true,
	}
	if err := workflow.Run(task); err == nil || !strings.Contains(err.Error(), "requirements agent failed") {
		t.Fatalf("failed stage invocation should end workflow: %v", err)
	}

	calls := readIntegrationCalls(t, trace)
	if len(calls) != 1 || calls[0].Stage != "requirements" {
		t.Fatalf("process calls = %+v, want one requirements invocation and no follow-up stages", calls)
	}
	if !strings.Contains(calls[0].Task, "Target repository: "+resolvedTestPath(t, target)) || !strings.Contains(calls[0].Task, task) {
		t.Errorf("requirements did not receive target context and original task: %q", calls[0].Task)
	}
	resolvedState, err := resolvedPath(state)
	if err != nil {
		t.Fatalf("resolve external state directory: %v", err)
	}
	if sameResolvedTestPath(t, calls[0].Workdir, target) || !isWithin(resolvedState, resolvedTestPath(t, calls[0].Workdir)) {
		t.Errorf("requirements workdir = %q, want a run directory under external state %q", calls[0].Workdir, resolvedState)
	}
	if count, err := os.ReadFile(counter); err != nil || strings.TrimSpace(string(count)) != "1" {
		t.Errorf("requirements agent invocation count = %q, err=%v", count, err)
	}
	stateEntries, err := os.ReadDir(filepath.Join(state, "runs"))
	if err != nil || len(stateEntries) != 1 {
		t.Fatalf("expected persisted run outside target: entries=%v err=%v", stateEntries, err)
	}
	runDir := filepath.Join(state, "runs", stateEntries[0].Name())
	if _, err := os.Stat(filepath.Join(runDir, "01-requirements.log")); err != nil {
		t.Errorf("single stage log missing from external state: %v", err)
	}
	if _, err := os.Stat(filepath.Join(runDir, "02-requirements.log")); !os.IsNotExist(err) {
		t.Errorf("unexpected retry log exists: %v", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Errorf("task text was interpreted as shell syntax: marker stat error=%v", err)
	}
	if entries, err := os.ReadDir(target); err != nil || len(entries) != 0 {
		t.Errorf("workflow artifacts entered target: entries=%v err=%v", entries, err)
	}
}

func readIntegrationCalls(t *testing.T, path string) []processCall {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var calls []processCall
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var call processCall
		if err := json.Unmarshal(scanner.Bytes(), &call); err != nil {
			t.Fatal(err)
		}
		calls = append(calls, call)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return calls
}
