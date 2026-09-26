package factory

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestValidateImplementationPlanEnforcesDAGAndNonOverlappingBoundedScopes(t *testing.T) {
	valid := `{"subtasks":[{"id":"model","task":"Implement the model","files":["internal/model.go"],"depends_on":[]},{"id":"tests","task":"Add model tests","files":["internal/model_test.go"],"depends_on":["model"]},{"id":"docs","task":"Document behavior","files":["docs/model.md"],"depends_on":[]}]}`
	plan, err := validateImplementationPlan([]byte(valid))
	if err != nil {
		t.Fatalf("valid dependency plan rejected: %v", err)
	}
	waves, err := implementationWaves(plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(waves) != 2 || len(waves[0]) != 2 || len(waves[1]) != 1 || waves[1][0] != 1 {
		t.Fatalf("dependency waves = %v, want docs/model together then tests", waves)
	}

	for _, tc := range []struct {
		name string
		json string
		want string
	}{
		{"cycle", `{"subtasks":[{"id":"a","task":"A","files":["a.go"],"depends_on":["b"]},{"id":"b","task":"B","files":["b.go"],"depends_on":["a"]}]}`, "cycle"},
		{"unknown dependency", `{"subtasks":[{"id":"a","task":"A","files":["a.go"],"depends_on":["missing"]},{"id":"b","task":"B","files":["b.go"],"depends_on":[]}]}`, "unknown"},
		{"overlap", `{"subtasks":[{"id":"a","task":"A","files":["pkg/a.go"],"depends_on":[]},{"id":"b","task":"B","files":["pkg/a.go"],"depends_on":[]}]}`, "overlaps"},
		{"directory overlap", `{"subtasks":[{"id":"a","task":"A","files":["pkg"],"depends_on":[]},{"id":"b","task":"B","files":["pkg/a.go"],"depends_on":[]}]}`, "overlap"},
		{"path traversal", `{"subtasks":[{"id":"a","task":"A","files":["../outside"],"depends_on":[]},{"id":"b","task":"B","files":["b.go"],"depends_on":[]}]}`, "path"},
		{"extra fields", `{"subtasks":[{"id":"a","task":"A","files":["a.go"],"depends_on":[],"command":"rm -rf /"},{"id":"b","task":"B","files":["b.go"],"depends_on":[]}]}`, "unknown field"},
		{"duplicate JSON keys", `{"subtasks":[{"id":"a","task":"A","files":["a.go"],"files":["escape.go"],"depends_on":[]},{"id":"b","task":"B","files":["b.go"],"depends_on":[]}]}`, "duplicate"},
		{"too many", oversizedPlanJSON(), "2 to 8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := validateImplementationPlan([]byte(tc.json)); err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.want)) {
				t.Fatalf("invalid plan error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func oversizedPlanJSON() string {
	var subtasks []plannedSubtask
	for i := 0; i < maxParallelSubtasks+1; i++ {
		subtasks = append(subtasks, plannedSubtask{ID: fmt.Sprintf("task-%d", i), Task: "do work", Files: []string{fmt.Sprintf("file-%d.go", i)}})
	}
	data, _ := json.Marshal(implementationPlan{Subtasks: subtasks})
	return string(data)
}

func TestParallelImplementationRequiresCleanGitRootAndFallsBackOtherwise(t *testing.T) {
	dir := initTestGitRepo(t)
	root, eligible, err := parallelImplementationEligible(dir)
	if err != nil || !eligible || root != dir {
		t.Fatalf("clean git repository eligibility = %q, %v, %v; want eligible root", root, eligible, err)
	}
	if _, eligible, err := parallelImplementationEligible(filepath.Dir(dir)); err != nil || eligible {
		t.Fatalf("parent directory eligibility = %v, %v; want fallback", eligible, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "dirty.txt"), []byte("external work"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, eligible, err := parallelImplementationEligible(dir); err != nil || eligible {
		t.Fatalf("dirty repository eligibility = %v, %v; want fallback", eligible, err)
	}
	if _, eligible, err := parallelImplementationEligible(t.TempDir()); err != nil || eligible {
		t.Fatalf("non-Git directory eligibility = %v, %v; want fallback", eligible, err)
	}
}

func TestWorkflowParallelImplementationUsesIsolatedWorktreesAndPersistsOutcomes(t *testing.T) {
	dir := initTestGitRepo(t)
	stateDir := filepath.Join(t.TempDir(), "state")
	agent := &parallelWorkflowAgent{}
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: stateDir, ParallelImplementation: &ParallelImplementationConfig{Enabled: true, MaxConcurrency: 2}},
		In: strings.NewReader(""), Out: ioDiscard{}, Workdir: dir, Stages: []string{"implement"},
	}
	if err := workflow.Run("implement two independent changes"); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&agent.maxActive); got != 2 {
		t.Fatalf("maximum simultaneous implementation agents = %d, want bounded parallelism of 2", got)
	}
	if got := atomic.LoadInt32(&agent.plannerCalls); got != 1 {
		t.Fatalf("planner called %d times, want one structured plan", got)
	}
	for path, want := range map[string]string{"feature-one.txt": "one", "feature-two.txt": "two"} {
		data, err := os.ReadFile(filepath.Join(dir, path))
		if err != nil || string(data) != want {
			t.Fatalf("integrated file %s = %q, %v; want %q", path, data, err, want)
		}
	}
	state := workflowRunState(t, stateDir)
	if state.SubtaskPlanStatus != "completed" || len(state.Subtasks) != 2 {
		t.Fatalf("persisted plan status/subtasks = %q/%+v", state.SubtaskPlanStatus, state.Subtasks)
	}
	for _, record := range state.Subtasks {
		if record.Status != "integrated" || record.StartedAt.IsZero() || record.EndedAt.IsZero() || record.Log == "" || record.Worktree == "" {
			t.Errorf("subtask outcome not fully persisted: %+v", record)
		}
		if _, err := os.Stat(record.Worktree); !os.IsNotExist(err) {
			t.Errorf("isolated worktree was not removed: %s, err=%v", record.Worktree, err)
		}
	}
	for _, event := range []string{"subtask.plan", "subtask.started", "subtask.completed", "subtask.integrated"} {
		data, err := os.ReadFile(filepath.Join(stateDir, "runs", state.ID, "workflow-events.jsonl"))
		if err != nil || !strings.Contains(string(data), `"type":"`+event+`"`) {
			t.Errorf("persisted events missing %s: %v", event, err)
		}
	}
}

func TestParallelImplementationDependencyWaitsForIntegration(t *testing.T) {
	dir := initTestGitRepo(t)
	stateDir := filepath.Join(t.TempDir(), "state")
	agent := &parallelWorkflowAgent{dependencyPlan: true}
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: stateDir, ParallelImplementation: &ParallelImplementationConfig{Enabled: true}},
		In: strings.NewReader(""), Out: ioDiscard{}, Workdir: dir, Stages: []string{"implement"},
	}
	if err := workflow.Run("implement dependent changes"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "feature-two.txt"))
	if err != nil || string(data) != "derived from one" {
		t.Fatalf("dependent output = %q, %v; dependency output was not integrated first", data, err)
	}
	if got := atomic.LoadInt32(&agent.maxActive); got != 1 {
		t.Fatalf("dependent subtasks ran with %d simultaneous agents, want sequential dependency waves", got)
	}
}

func TestParallelImplementationFallsBackToSingleAgentForDirtyRepository(t *testing.T) {
	dir := initTestGitRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "preexisting.txt"), []byte("user work"), 0o600); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(t.TempDir(), "state")
	agent := &parallelWorkflowAgent{}
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: stateDir, ParallelImplementation: &ParallelImplementationConfig{Enabled: true}},
		In: strings.NewReader(""), Out: ioDiscard{}, Workdir: dir, Stages: []string{"implement"},
	}
	if err := workflow.Run("task"); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&agent.plannerCalls) != 0 {
		t.Fatal("planner ran in dirty checkout instead of preserving single-agent fallback")
	}
	if got := atomic.LoadInt32(&agent.singleCalls); got != 1 {
		t.Fatalf("single-agent implementation calls = %d, want 1", got)
	}
	if state := workflowRunState(t, stateDir); len(state.Subtasks) != 0 {
		t.Fatalf("dirty checkout unexpectedly persisted subtasks: %+v", state.Subtasks)
	}
}

func TestParallelImplementationRejectsUndeclaredChangesBeforeIntegration(t *testing.T) {
	dir := initTestGitRepo(t)
	stateDir := filepath.Join(t.TempDir(), "state")
	agent := &parallelWorkflowAgent{writeUndeclared: true}
	workflow := Workflow{
		Agent: agent, Config: Config{StateDir: stateDir, ParallelImplementation: &ParallelImplementationConfig{Enabled: true}},
		In: strings.NewReader(""), Out: ioDiscard{}, Workdir: dir, Stages: []string{"implement"},
	}
	if err := workflow.Run("do scoped changes"); err == nil || !strings.Contains(err.Error(), "undeclared file") {
		t.Fatalf("undeclared path workflow error = %v, want scope rejection", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "agent-escape.txt")); !os.IsNotExist(err) {
		t.Fatalf("undeclared agent output was integrated: err=%v", err)
	}
	state := workflowRunState(t, stateDir)
	if state.Status != "failed" || state.SubtaskPlanStatus != "failed" {
		t.Fatalf("scope violation status/plan status = %q/%q", state.Status, state.SubtaskPlanStatus)
	}
}

func TestParallelImplementationBoundsConcurrencyAcrossFourTasks(t *testing.T) {
	dir := initTestGitRepo(t)
	agent := &parallelWorkflowAgent{fourTaskPlan: true}
	workflow := Workflow{Agent: agent, Config: Config{StateDir: filepath.Join(t.TempDir(), "state"), ParallelImplementation: &ParallelImplementationConfig{Enabled: true, MaxConcurrency: 2}}, In: strings.NewReader(""), Out: ioDiscard{}, Workdir: dir, Stages: []string{"implement"}}
	if err := workflow.Run("implement four changes"); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&agent.maxActive); got != 2 {
		t.Fatalf("maximum simultaneous workers = %d, want exactly configured cap 2 with four ready tasks", got)
	}
}

func TestParallelImplementationRejectsIgnoredUndeclaredOutput(t *testing.T) {
	dir := initTestGitRepo(t)
	agent := &parallelWorkflowAgent{writeIgnored: true}
	workflow := Workflow{Agent: agent, Config: Config{StateDir: filepath.Join(t.TempDir(), "state"), ParallelImplementation: &ParallelImplementationConfig{Enabled: true}}, In: strings.NewReader(""), Out: ioDiscard{}, Workdir: dir, Stages: []string{"implement"}}
	if err := workflow.Run("do scoped changes"); err == nil || !strings.Contains(err.Error(), "ignored output") {
		t.Fatalf("ignored output error = %v, want ignored output rejection", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "feature-one.txt")); !os.IsNotExist(err) {
		t.Fatalf("target changed despite ignored-output failure: %v", err)
	}
}

func TestParallelImplementationRejectsUnsafeDeclaredTypes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		create func(string) error
	}{
		{name: "symlink", create: func(path string) error { return os.Symlink("README.md", path) }},
		{name: "directory", create: func(path string) error { return os.Mkdir(path, 0o700) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "output")
			if err := tc.create(path); err != nil {
				t.Fatal(err)
			}
			if err := stageSubtaskFiles(t.TempDir(), root, []string{"output"}); err == nil {
				t.Fatal("unsafe declared type was staged")
			}
		})
	}
}

func TestParallelImplementationPreservesModeChanges(t *testing.T) {
	dir := initTestGitRepo(t)
	agent := &parallelWorkflowAgent{modeChange: true}
	workflow := Workflow{Agent: agent, Config: Config{StateDir: filepath.Join(t.TempDir(), "state"), ParallelImplementation: &ParallelImplementationConfig{Enabled: true}}, In: strings.NewReader(""), Out: ioDiscard{}, Workdir: dir, Stages: []string{"implement"}}
	if err := workflow.Run("make the first output executable"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, "feature-one.txt"))
	if err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("integrated mode = %v, %v; want 0755", info, err)
	}
}

func TestParallelImplementationLaterWaveFailureLeavesTargetUnchanged(t *testing.T) {
	dir := initTestGitRepo(t)
	before := gitWorktreeSnapshot(t, dir)
	agent := &parallelWorkflowAgent{laterFail: true}
	workflow := Workflow{Agent: agent, Config: Config{StateDir: filepath.Join(t.TempDir(), "state"), ParallelImplementation: &ParallelImplementationConfig{Enabled: true}}, In: strings.NewReader(""), Out: ioDiscard{}, Workdir: dir, Stages: []string{"implement"}}
	if err := workflow.Run("fail in second wave"); err == nil {
		t.Fatal("later wave failure unexpectedly succeeded")
	}
	if after := gitWorktreeSnapshot(t, dir); after != before {
		t.Fatalf("target changed on failed later wave: before %q, after %q", before, after)
	}
}

func TestParallelImplementationCancelsSiblingOnWorkerError(t *testing.T) {
	dir := initTestGitRepo(t)
	agent := &parallelWorkflowAgent{cancelSibling: true}
	workflow := Workflow{Agent: agent, Config: Config{StateDir: filepath.Join(t.TempDir(), "state"), ParallelImplementation: &ParallelImplementationConfig{Enabled: true, MaxConcurrency: 2}}, In: strings.NewReader(""), Out: ioDiscard{}, Workdir: dir, Stages: []string{"implement"}}
	if err := workflow.Run("cancel sibling after first failure"); err == nil {
		t.Fatal("worker failure unexpectedly succeeded")
	}
	if got := atomic.LoadInt32(&agent.siblingCanceled); got < 1 {
		t.Fatalf("cancel-aware sibling exits = %d, want cancellation after first worker error", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "feature-one.txt")); !os.IsNotExist(err) {
		t.Fatalf("target changed despite worker failure: %v", err)
	}
}

func gitWorktreeSnapshot(t *testing.T, dir string) string {
	t.Helper()
	cmd := exec.Command("git", "-C", dir, "status", "--porcelain=v1", "--untracked-files=all", "--ignored=matching", "-z")
	status, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	var result strings.Builder
	result.Write(status)
	for _, path := range []string{"feature-one.txt", "feature-two.txt", "ignored.out"} {
		full := filepath.Join(dir, path)
		info, err := os.Lstat(full)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(full)
		if err == nil {
			fmt.Fprintf(&result, "%s:%#o:%s", path, info.Mode().Perm(), data)
		}
	}
	return result.String()
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }

type parallelWorkflowAgent struct {
	active          int32
	maxActive       int32
	plannerCalls    int32
	singleCalls     int32
	writeUndeclared bool
	writeIgnored    bool
	modeChange      bool
	dependencyPlan  bool
	fourTaskPlan    bool
	laterFail       bool
	cancelSibling   bool
	siblingCanceled int32
}

func (a *parallelWorkflowAgent) Run(stage, prompt, task, workdir, logPath string) error {
	_, err := a.RunWithOutputContext(context.Background(), stage, prompt, task, workdir, logPath)
	return err
}

func (a *parallelWorkflowAgent) RunWithContext(ctx context.Context, stage, prompt, task, workdir, logPath string) error {
	_, err := a.RunWithOutputContext(ctx, stage, prompt, task, workdir, logPath)
	return err
}

func (a *parallelWorkflowAgent) RunWithOutputContext(ctx context.Context, stage, prompt, task, workdir, logPath string) (string, error) {
	if stage == "plan" && strings.Contains(task, "Create an implementation plan") {
		atomic.AddInt32(&a.plannerCalls, 1)
		plan := `{"subtasks":[{"id":"one","task":"write feature one","files":["feature-one.txt"],"depends_on":[]},{"id":"two","task":"write feature two","files":["feature-two.txt"],"depends_on":[]}]}`
		if a.fourTaskPlan {
			plan = `{"subtasks":[{"id":"one","task":"write feature-one.txt","files":["feature-one.txt"],"depends_on":[]},{"id":"two","task":"write feature-two.txt","files":["feature-two.txt"],"depends_on":[]},{"id":"three","task":"write feature-three.txt","files":["feature-three.txt"],"depends_on":[]},{"id":"four","task":"write feature-four.txt","files":["feature-four.txt"],"depends_on":[]}]}`
		}
		if a.laterFail {
			plan = `{"subtasks":[{"id":"one","task":"write feature one","files":["feature-one.txt"],"depends_on":[]},{"id":"two","task":"write feature two","files":["feature-two.txt"],"depends_on":["one"]}]}`
		}
		if a.dependencyPlan {
			plan = `{"subtasks":[{"id":"one","task":"write feature one","files":["feature-one.txt"],"depends_on":[]},{"id":"two","task":"read feature one and derive feature two","files":["feature-two.txt"],"depends_on":["one"]}]}`
		}
		if err := os.WriteFile(logPath, []byte(plan), 0o600); err != nil {
			return "", err
		}
		return plan, nil
	}
	if stage == "implement" {
		gitMetadata, err := os.Stat(filepath.Join(workdir, ".git"))
		if err == nil && gitMetadata.IsDir() {
			atomic.AddInt32(&a.singleCalls, 1)
			if err := os.WriteFile(logPath, []byte("implemented\n"), 0o600); err != nil {
				return "", err
			}
			return "", nil
		}
		active := atomic.AddInt32(&a.active, 1)
		defer atomic.AddInt32(&a.active, -1)
		for {
			previous := atomic.LoadInt32(&a.maxActive)
			if active <= previous || atomic.CompareAndSwapInt32(&a.maxActive, previous, active) {
				break
			}
		}
		if a.cancelSibling && strings.Contains(task, "feature one") {
			deadline := time.Now().Add(time.Second)
			for atomic.LoadInt32(&a.active) < 2 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			return "", fmt.Errorf("deliberate worker failure")
		}
		wait := 50 * time.Millisecond
		if a.cancelSibling && strings.Contains(task, "feature two") {
			wait = time.Second
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			if a.cancelSibling && strings.Contains(task, "feature two") {
				atomic.AddInt32(&a.siblingCanceled, 1)
			}
			return "", ctx.Err()
		}
		name, contents := "feature-one.txt", "one"
		if strings.HasPrefix(task, "write feature-") {
			name = strings.TrimPrefix(task, "write ")
			contents = strings.TrimSuffix(strings.TrimPrefix(name, "feature-"), ".txt")
		}
		if strings.Contains(task, "two") {
			name, contents = "feature-two.txt", "two"
			if strings.Contains(task, "read feature one") {
				dependency, err := os.ReadFile(filepath.Join(workdir, "feature-one.txt"))
				if err != nil {
					return "", fmt.Errorf("integrated dependency unavailable: %w", err)
				}
				contents = "derived from " + string(dependency)
			}
		}
		if a.laterFail && strings.Contains(task, "feature two") {
			return "", fmt.Errorf("later wave failed")
		}
		if err := os.WriteFile(filepath.Join(workdir, name), []byte(contents), 0o600); err != nil {
			return "", err
		}
		if a.modeChange && name == "feature-one.txt" {
			if err := os.Chmod(filepath.Join(workdir, name), 0o755); err != nil {
				return "", err
			}
		}
		if a.writeIgnored {
			if err := os.WriteFile(filepath.Join(workdir, "ignored.out"), []byte("ignored"), 0o600); err != nil {
				return "", err
			}
		}
		if a.writeUndeclared {
			if err := os.WriteFile(filepath.Join(workdir, "agent-escape.txt"), []byte("escape"), 0o600); err != nil {
				return "", err
			}
		}
	}
	if err := os.WriteFile(logPath, []byte("PASS\n"), 0o600); err != nil {
		return "", err
	}
	return "PASS\n", nil
}

func initTestGitRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, args := range [][]string{{"init", "-q", dir}, {"-C", dir, "config", "user.email", "factory-test@example.invalid"}, {"-C", dir, "config", "user.name", "Factory Test"}} {
		cmd := exec.Command("git", args...)
		if output, err := cmd.CombinedOutput(); err != nil {
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
		cmd := exec.Command("git", args...)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v failed: %v: %s", args, err, output)
		}
	}
	return dir
}
