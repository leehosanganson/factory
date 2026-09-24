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
	if call.Stage == "evaluate" && strings.Contains(call.Task, "Stage completed: requirements") {
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
			fmt.Println("FAIL\nintegration evaluator finding: requirements incomplete")
			return
		}
	}
	if call.Stage == "evaluate" {
		fmt.Println("PASS")
	}
}

func TestWorkflowIntegrationUsesRunnerRetriesAndKeepsArtifactsOutsideTarget(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "target")
	state := filepath.Join(base, "state")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	trace := filepath.Join(base, "processes.jsonl")
	counter := filepath.Join(base, "retry-count")
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
	if err := workflow.Run(task); err != nil {
		t.Fatal(err)
	}

	calls := readIntegrationCalls(t, trace)
	wantStages := []string{"requirements", "evaluate", "requirements", "evaluate", "implement", "evaluate", "review", "evaluate", "document", "evaluate"}
	if len(calls) != len(wantStages) {
		t.Fatalf("process calls = %d, want %d; calls=%+v", len(calls), len(wantStages), calls)
	}
	for i, call := range calls {
		if call.Stage != wantStages[i] {
			t.Fatalf("process call %d stage = %q, want %q", i, call.Stage, wantStages[i])
		}
		if !strings.Contains(call.Task, task) {
			t.Errorf("task placeholder was not preserved for %s: %q", call.Stage, call.Task)
		}
	}
	if !strings.Contains(calls[0].Task, "Target repository: "+target) || !strings.Contains(calls[0].Task, task) {
		t.Errorf("requirements did not receive target context and original task: %q", calls[0].Task)
	}
	if !strings.Contains(calls[2].Task, task) || !strings.Contains(calls[2].Task, "integration evaluator finding: requirements incomplete") || !strings.Contains(calls[2].Task, "Evaluator output/findings:") {
		t.Errorf("second requirements attempt did not receive original task and first evaluator feedback: %q", calls[2].Task)
	}
	if calls[0].Workdir == target || !isWithin(state, calls[0].Workdir) {
		t.Errorf("requirements workdir = %q, want a run directory under external state %q", calls[0].Workdir, state)
	}
	for i, call := range calls[1:] {
		wantWorkdir := target
		if call.Stage == "requirements" {
			wantWorkdir = calls[0].Workdir
		}
		if call.Workdir != wantWorkdir {
			t.Errorf("process call %d (%s) workdir = %q, want %q", i+1, call.Stage, call.Workdir, wantWorkdir)
		}
	}
	if count, err := os.ReadFile(counter); err != nil || strings.TrimSpace(string(count)) != "2" {
		t.Errorf("requirements evaluator retry count = %q, err=%v", count, err)
	}
	stateEntries, err := os.ReadDir(filepath.Join(state, "runs"))
	if err != nil || len(stateEntries) != 1 {
		t.Fatalf("expected persisted run outside target: entries=%v err=%v", stateEntries, err)
	}
	runDir := filepath.Join(state, "runs", stateEntries[0].Name())
	if _, err := os.Stat(filepath.Join(runDir, "02-evaluate-requirements.log")); err != nil {
		t.Errorf("retry evaluator log missing from external state: %v", err)
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
