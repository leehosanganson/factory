package restworker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/leehosanganson/factory/internal/factory"
	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restprovider"
	"github.com/leehosanganson/factory/internal/restserver"
	"github.com/leehosanganson/factory/internal/restworkspace"
)

const (
	executorJobID      = "01234567-89ab-4cde-8fab-0123456789ab"
	restJobOutputLimit = 16 << 20
)

type testWorkspaceManager struct {
	root      string
	workspace restworkspace.Workspace
	createdID string
	markedID  string
	markErr   error
	createErr error
	createFn  func(context.Context) (restworkspace.Workspace, error)
}

func (m *testWorkspaceManager) RepositoryRoot() string { return m.root }

func (m *testWorkspaceManager) VerifyResultDirectory(path string) error {
	if filepath.Clean(path) != path || filepath.Base(path) != executorJobID || path != filepath.Dir(m.workspace.WorktreePath) {
		return errors.New("invalid result directory")
	}
	return nil
}

func (m *testWorkspaceManager) CreateContext(ctx context.Context, id string) (restworkspace.Workspace, error) {
	m.createdID = id
	if m.createFn != nil {
		return m.createFn(ctx)
	}
	if m.createErr != nil {
		return restworkspace.Workspace{}, m.createErr
	}
	return m.workspace, nil
}

func (m *testWorkspaceManager) MarkSucceeded(id string) error {
	m.markedID = id
	return m.markErr
}

func TestProviderExecutionCannotMarkWorkspaceCompleteBeforeOutcomePersistence(t *testing.T) {
	executor, manager, _ := newExecutorFixture(t, "10s", restJobOutputLimit, &executorTestAgent{})
	executor.provider = testJobPublisher(func(context.Context, restprovider.PublishRequest) (restprovider.Outcome, error) {
		return restprovider.Outcome{}, nil
	})
	if err := executor.CompleteResult(context.Background(), executorJob("protect provider side effect")); err == nil {
		t.Fatal("provider-configured job marked complete without persisted outcome")
	}
	if manager.markedID != "" {
		t.Fatalf("workspace marked complete prematurely: %q", manager.markedID)
	}
	job := executorJob("complete after durable provider outcome")
	job.Provider = &restjobs.ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 7, URL: "https://github.com/acme/widget/pull/7", Branch: "factory/job/01234567-89ab-4cde-8fab-0123456789ab", Commit: "abc123", State: "open"}
	if err := executor.CompleteResult(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if manager.markedID != executorJobID {
		t.Fatalf("workspace completion after outcome persistence = %q", manager.markedID)
	}
}

type testJobPublisher func(context.Context, restprovider.PublishRequest) (restprovider.Outcome, error)

func (f testJobPublisher) Publish(ctx context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
	return f(ctx, request)
}
func (f testJobPublisher) Ping(context.Context) error { return nil }

type executorTestAgent struct {
	calls       []string
	output      io.Writer
	failAt      string
	block       bool
	outputBytes int
}

func (a *executorTestAgent) WithOutputWriter(output io.Writer) factory.Agent {
	return a
}

func (a *executorTestAgent) Run(stage, prompt, task, workdir, log string) error {
	return a.RunWithContext(context.Background(), stage, prompt, task, workdir, log)
}

func (a *executorTestAgent) RunWithContext(ctx context.Context, stage, _, _, _, logPath string) error {
	a.calls = append(a.calls, stage)
	if _, err := os.Stat(logPath); err == nil {
		return errors.New("unexpected per-stage transcript")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	size := a.outputBytes
	if size == 0 {
		size = 16
	}
	if a.output != nil {
		_, _ = io.WriteString(a.output, strings.Repeat(stage[:1], size))
	}
	if a.block {
		<-ctx.Done()
		return ctx.Err()
	}
	if a.failAt == stage {
		return errors.New("private agent failure detail")
	}
	return nil
}

func newExecutorFixture(t *testing.T, timeout string, outputLimit int, agent *executorTestAgent, checks ...[]string) (*FactoryExecutor, *testWorkspaceManager, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"config", "user.email", "test@example.com"}, {"config", "user.name", "REST executor test"}} {
		if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(repo, "tracked"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "tracked"}, {"commit", "-q", "-m", "baseline"}} {
		if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	jobDir := filepath.Join(root, executorJobID)
	if err := os.Mkdir(jobDir, 0o700); err != nil {
		t.Fatal(err)
	}
	worktree := filepath.Join(jobDir, "worktree")
	if output, err := exec.Command("git", "-C", repo, "worktree", "add", "--detach", worktree, "HEAD").CombinedOutput(); err != nil {
		t.Fatalf("create test worktree: %v: %s", err, output)
	}
	state := filepath.Join(jobDir, "state")
	outputDir := filepath.Join(jobDir, "output")
	for _, dir := range []string{state, outputDir} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	manager := &testWorkspaceManager{root: repo, workspace: restworkspace.Workspace{
		JobID: executorJobID, WorktreePath: worktree, StatePath: state, OutputPath: outputDir,
	}}
	server := restserver.DefaultConfig()
	server.Repositories = map[string]string{"trusted": repo}
	server.Harness = restserver.HarnessConfig{Executable: "rest-agent", Args: []string{"{task}", "{system_prompt}"}}
	server.APIKeyFile = filepath.Join(root, "api-key")
	server.Limits.JobTimeout = timeout
	server.Limits.HarnessOutput = outputLimit
	workflow := factory.Config{
		Command: "placeholder", Args: []string{"{task}", "{system_prompt}"}, StateDir: state,
		PipelineChecks: checks, AutoPublish: true,
	}
	executor, err := NewFactoryExecutor(FactoryExecutorConfig{
		Server: server, Workflow: workflow,
		Workspaces: map[string]WorkspaceManager{"trusted": manager},
	})
	if err != nil {
		t.Fatalf("NewFactoryExecutor() error = %v", err)
	}
	executor.agent = func(_ factory.Config, _ []string, output io.Writer) factory.Agent {
		agent.output = output
		return agent
	}
	return executor, manager, repo
}

func executorJob(task string) restjobs.Snapshot {
	return restjobs.Snapshot{ID: executorJobID, Request: restjobs.Request{Repository: "trusted", Task: task}}
}

func TestNewLocalJobManagerUsesServerRegistryLimits(t *testing.T) {
	config := restserver.DefaultConfig()
	config.Repositories = map[string]string{"trusted": t.TempDir()}
	config.Harness = restserver.HarnessConfig{Executable: "agent", Args: []string{"{task}", "{system_prompt}"}}
	config.APIKeyFile = filepath.Join(t.TempDir(), "api-key")
	config.Limits.Workers = 1
	config.Limits.QueueCapacity = 2
	config.Limits.MaxRecords = 2
	config.Limits.MaxEventsPerJob = 3
	config.Limits.TaskBytes = 5
	manager, err := NewLocalJobManager(config)
	if err != nil {
		t.Fatalf("NewLocalJobManager() error = %v", err)
	}
	first, _, err := manager.Admit("first", restjobs.Request{Repository: "trusted", Task: "12345"})
	if err != nil {
		t.Fatalf("admit max-sized task: %v", err)
	}
	if _, _, err := manager.Admit("too-long", restjobs.Request{Repository: "trusted", Task: "123456"}); !errors.Is(err, restjobs.ErrInvalidInput) {
		t.Fatalf("admit over-limit task error = %v, want ErrInvalidInput", err)
	}
	if err := manager.AddEvent(first.ID, "safe", ""); err != nil {
		t.Fatalf("AddEvent(): %v", err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatalf("claim first record: %v", err)
	}
	if err := manager.Finish(first.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatalf("finish first record: %v", err)
	}
	if _, _, err := manager.Admit("second", restjobs.Request{Repository: "trusted", Task: "other"}); err != nil {
		t.Fatalf("admit second registry record: %v", err)
	}
	if _, _, err := manager.Admit("third", restjobs.Request{Repository: "trusted", Task: "third"}); err != nil {
		t.Fatalf("admit at record cap should evict oldest terminal record: %v", err)
	}
	if _, err := manager.Get(first.ID); !errors.Is(err, restjobs.ErrNotFound) {
		t.Fatalf("oldest terminal record was not evicted: %v", err)
	}
}

func TestNewLocalJobManagerReturnsConfiguredSQLiteOpenFailure(t *testing.T) {
	config := restserver.DefaultConfig()
	config.Repositories = map[string]string{"trusted": t.TempDir()}
	config.Harness = restserver.HarnessConfig{Executable: "agent", Args: []string{"{task}", "{system_prompt}"}}
	config.APIKeyFile = filepath.Join(t.TempDir(), "api-key")
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: "relative.db"}
	if _, err := NewLocalJobManager(config); err == nil {
		t.Fatal("configured SQLite open failure silently fell back to memory")
	}
}

func TestNewFactoryExecutorRequiresWorkspaceForMatchingTrustedRepository(t *testing.T) {
	validManager := &testWorkspaceManager{root: t.TempDir()}
	server := restserver.DefaultConfig()
	server.Repositories = map[string]string{"trusted": validManager.root}
	server.Harness = restserver.HarnessConfig{Executable: "agent", Args: []string{"{task}", "{system_prompt}"}}
	server.APIKeyFile = filepath.Join(t.TempDir(), "api-key")
	workflow := factory.Config{Command: "agent", Args: []string{"{task}", "{system_prompt}"}}
	if _, err := NewFactoryExecutor(FactoryExecutorConfig{Server: server, Workflow: workflow}); err == nil {
		t.Fatal("NewFactoryExecutor() accepted a missing workspace manager")
	}
}

func TestFactoryExecutorRunsStagesAndChecksBeforeMarkingSuccessWithoutPublishing(t *testing.T) {
	agent := &executorTestAgent{}
	check := filepath.Join(t.TempDir(), "check.sh")
	if err := os.WriteFile(check, []byte("#!/bin/sh\nprintf 'check output'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	agent.outputBytes = 5 << 20
	executor, workspace, repo := newExecutorFixture(t, "10s", restJobOutputLimit, agent, []string{check})
	before, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.Execute(context.Background(), executorJob("implement feature")); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got, want := strings.Join(agent.calls, ","), "requirements,implement,review,document"; got != want {
		t.Fatalf("stage order = %q, want %q", got, want)
	}
	if workspace.createdID != executorJobID || workspace.markedID != executorJobID {
		t.Fatalf("workspace IDs = create %q, success %q", workspace.createdID, workspace.markedID)
	}
	capture, err := os.ReadFile(filepath.Join(workspace.workspace.OutputPath, "workflow.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(capture) > restJobOutputLimit || !strings.Contains(string(capture), "output truncated") {
		t.Fatalf("job-wide capture length=%d, content=%q", len(capture), capture)
	}
	after, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil || string(after) != string(before) {
		t.Fatalf("executor published or changed checkout HEAD: before=%q after=%q err=%v", before, after, err)
	}
}

func TestFactoryExecutorRetainsWorkspaceUnmarkedOnStageOrCheckFailure(t *testing.T) {
	for _, test := range []struct {
		name  string
		stage string
		check bool
	}{
		{name: "agent stage", stage: "review"},
		{name: "verification check", check: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			agent := &executorTestAgent{failAt: test.stage}
			checks := [][]string(nil)
			if test.check {
				check := filepath.Join(t.TempDir(), "fail-check.sh")
				if err := os.WriteFile(check, []byte("#!/bin/sh\nprintf secret-on-stdout\nexit 9\n"), 0o700); err != nil {
					t.Fatal(err)
				}
				checks = [][]string{{check}}
			}
			executor, manager, _ := newExecutorFixture(t, "2s", 256, agent, checks...)
			err := executor.Execute(context.Background(), executorJob("do work"))
			if !errors.Is(err, errExecutionFailed) || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "private") {
				t.Fatalf("Execute() error = %v, want sanitized failure", err)
			}
			if manager.markedID != "" {
				t.Fatalf("failed workspace marked successful: %q", manager.markedID)
			}
			if _, err := os.Stat(manager.workspace.WorktreePath); err != nil {
				t.Fatalf("failed workspace was not retained: %v", err)
			}
		})
	}
}

func TestFactoryExecutorBoundsCombinedStageAndCheckFilesAndRecordsTruncation(t *testing.T) {
	checkRoot := t.TempDir()
	checkMarker := filepath.Join(checkRoot, "check-finished")
	check := filepath.Join(checkRoot, "large-check.sh")
	if err := os.WriteFile(check, []byte("#!/bin/sh\ndd if=/dev/zero bs=1048576 count=5 2>/dev/null | tr '\\000' c\ntouch '"+checkMarker+"'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	agent := &executorTestAgent{outputBytes: 5 << 20}
	executor, manager, _ := newExecutorFixture(t, "20s", restJobOutputLimit, agent, []string{check})
	var observed []factory.WorkflowEvent
	executor.observer = factory.WorkflowObserverFunc(func(event factory.WorkflowEvent) error {
		observed = append(observed, event)
		return nil
	})
	if err := executor.Execute(context.Background(), executorJob("sensitive user task")); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := strings.Join(agent.calls, ","); got != "requirements,implement,review,document" {
		t.Fatalf("stages stopped after reaching the output cap: %q", got)
	}
	if _, err := os.Stat(checkMarker); err != nil {
		t.Fatalf("check did not finish draining after output cap: %v", err)
	}
	var retained int64
	for _, root := range []string{manager.workspace.StatePath, manager.workspace.OutputPath} {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				retained += info.Size()
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if retained > restJobOutputLimit+(256<<10) {
		t.Fatalf("output/state retained %d bytes; cap plus metadata allowance is %d", retained, restJobOutputLimit+(256<<10))
	}
	outputInfo, err := os.Stat(filepath.Join(manager.workspace.OutputPath, "workflow.log"))
	if err != nil || outputInfo.Size() != restJobOutputLimit {
		t.Fatalf("combined output file size=%v err=%v, want exact configured cap %d", outputInfo, err, restJobOutputLimit)
	}
	statePaths, err := filepath.Glob(filepath.Join(manager.workspace.StatePath, "runs", "*", "state.json"))
	if err != nil || len(statePaths) != 1 {
		t.Fatalf("state paths=%q err=%v", statePaths, err)
	}
	stateData, err := os.ReadFile(statePaths[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(stateData), "[omitted]") || strings.Contains(string(stateData), strings.Repeat("r", 128)) || strings.Contains(string(stateData), strings.Repeat("c", 128)) {
		t.Fatal("workflow state duplicated task or stage output")
	}
	var state struct {
		OutputTruncated bool `json:"output_truncated"`
	}
	if err := json.Unmarshal(stateData, &state); err != nil || !state.OutputTruncated {
		t.Fatalf("state truncation metadata=%+v err=%v", state, err)
	}
	eventsData, err := os.ReadFile(filepath.Join(filepath.Dir(statePaths[0]), "workflow-events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(eventsData), `"type":"output.truncated"`) || !strings.Contains(string(eventsData), `"truncated":true`) || strings.Contains(string(eventsData), strings.Repeat("r", 128)) || strings.Contains(string(eventsData), strings.Repeat("c", 128)) {
		t.Fatalf("workflow events lack safe truncation metadata or include output: %s", eventsData)
	}
	foundAPIEvent := false
	for _, event := range observed {
		if event.Type == "output.truncated" && event.Truncated && event.Message == "combined workflow output cap reached" {
			foundAPIEvent = true
		}
		if strings.Contains(event.Message, "sensitive user task") || strings.Contains(event.Message, strings.Repeat("r", 128)) || strings.Contains(event.Message, strings.Repeat("c", 128)) {
			t.Fatalf("observer event exposed task/output data: %+v", event)
		}
	}
	if !foundAPIEvent {
		t.Fatalf("WorkflowObserver did not receive safe truncation event: %+v", observed)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(statePaths[0]), "task.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("task transcript was retained: %v", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(statePaths[0]), "01-requirements.log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stage transcript was retained: %v", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(statePaths[0]), "pipeline-check-01.log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("check transcript was retained: %v", err)
	}
}

func TestFactoryExecutorUsesConfiguredSharedOutputLimitAndDrainsStagesAndChecks(t *testing.T) {
	checkRoot := t.TempDir()
	checkOne := filepath.Join(checkRoot, "check-one.sh")
	checkTwo := filepath.Join(checkRoot, "check-two.sh")
	checkTwoMarker := filepath.Join(checkRoot, "check-two-finished")
	if err := os.WriteFile(checkOne, []byte("#!/bin/sh\nprintf 'check-one-output\\n'\ndd if=/dev/zero bs=4096 count=1 2>/dev/null\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(checkTwo, []byte("#!/bin/sh\nprintf 'check-two-output\\n'\ntouch '"+checkTwoMarker+"'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	agent := &executorTestAgent{outputBytes: 2}
	const configuredLimit = 64
	executor, manager, _ := newExecutorFixture(t, "5s", configuredLimit, agent, []string{checkOne}, []string{checkTwo})
	if err := executor.Execute(context.Background(), executorJob("capture shared output")); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := strings.Join(agent.calls, ","); got != "requirements,implement,review,document" {
		t.Fatalf("stages = %q", got)
	}
	if _, err := os.Stat(checkTwoMarker); err != nil {
		t.Fatalf("later check was not drained to completion: %v", err)
	}
	capture, err := os.ReadFile(filepath.Join(manager.workspace.OutputPath, "workflow.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(capture) != configuredLimit || !strings.Contains(string(capture), "rr") || !strings.Contains(string(capture), "check-one-output") || !strings.Contains(string(capture), "output truncated") {
		t.Fatalf("shared output length=%d content=%q, want configured cap and check output", len(capture), capture)
	}
}

func TestFactoryExecutorUsesOneDeadlineForWorkspaceAndAllStages(t *testing.T) {
	t.Run("workspace creation", func(t *testing.T) {
		agent := &executorTestAgent{}
		executor, manager, _ := newExecutorFixture(t, "20ms", 128, agent)
		manager.createFn = func(ctx context.Context) (restworkspace.Workspace, error) {
			<-ctx.Done()
			return restworkspace.Workspace{}, ctx.Err()
		}
		started := time.Now()
		err := executor.Execute(context.Background(), executorJob("wait"))
		if !errors.Is(err, errExecutionFailed) || time.Since(started) < 15*time.Millisecond || manager.markedID != "" {
			t.Fatalf("workspace timeout result=%v elapsed=%s marked=%q", err, time.Since(started), manager.markedID)
		}
	})
	t.Run("stage execution", func(t *testing.T) {
		agent := &executorTestAgent{block: true}
		executor, manager, _ := newExecutorFixture(t, "100ms", restJobOutputLimit, agent)
		if err := executor.Execute(context.Background(), executorJob("wait")); !errors.Is(err, errExecutionFailed) {
			t.Fatalf("Execute() error = %v, want sanitized timeout", err)
		}
		if manager.markedID != "" || len(agent.calls) != 1 {
			t.Fatalf("timed-out workflow marked=%q stages=%v", manager.markedID, agent.calls)
		}
	})
}

func TestFactoryExecutorRepositoryValidationUsesJobContext(t *testing.T) {
	executor, manager, _ := newExecutorFixture(t, "5s", restJobOutputLimit, &executorTestAgent{})
	started := make(chan struct{})
	executor.validateRepositoryRoot = func(ctx context.Context, _ string) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- executor.Execute(ctx, executorJob("cancel root validation")) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("repository validation did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, errExecutionFailed) || strings.Contains(err.Error(), "context") {
			t.Fatalf("Execute() error = %v, want sanitized failure", err)
		}
	case <-time.After(time.Second):
		t.Fatal("job cancellation did not stop repository validation")
	}
	if manager.createdID != "" {
		t.Fatalf("workspace created after canceled validation: %q", manager.createdID)
	}
}

func TestNewFactoryExecutorBoundsStartupRepositoryValidation(t *testing.T) {
	executor, _, _ := newExecutorFixture(t, "5s", restJobOutputLimit, &executorTestAgent{})
	started := make(chan struct{})
	config := FactoryExecutorConfig{
		Server: executor.server, Workflow: executor.workflow, Workspaces: executor.workspaces,
		ValidateRepositoryRoot: func(ctx context.Context, _ string) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	const validationTimeout = 20 * time.Millisecond
	start := time.Now()
	_, err := newFactoryExecutor(context.Background(), config, validationTimeout)
	if err == nil || strings.Contains(err.Error(), "context") || strings.Contains(err.Error(), executor.server.Repositories["trusted"]) {
		t.Fatalf("constructor error = %v, want sanitized timeout", err)
	}
	if elapsed := time.Since(start); elapsed < validationTimeout || elapsed > time.Second {
		t.Fatalf("startup validation elapsed %s, want bounded near %s", elapsed, validationTimeout)
	}
	select {
	case <-started:
	default:
		t.Fatal("startup repository validation did not start")
	}
}

func TestNewFactoryExecutorContextPropagatesShutdownCancellation(t *testing.T) {
	executor, _, _ := newExecutorFixture(t, "5s", restJobOutputLimit, &executorTestAgent{})
	started := make(chan struct{})
	config := FactoryExecutorConfig{
		Server: executor.server, Workflow: executor.workflow, Workspaces: executor.workspaces,
		ValidateRepositoryRoot: func(ctx context.Context, _ string) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := NewFactoryExecutorContext(ctx, config)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("startup repository validation did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil || strings.Contains(err.Error(), "context") || strings.Contains(err.Error(), executor.server.Repositories["trusted"]) {
			t.Fatalf("constructor error = %v, want sanitized cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown cancellation did not stop startup validation")
	}
}

func TestFactoryExecutorRejectsUntrustedAliasAndNonExactCheckoutBeforeWorkspace(t *testing.T) {
	agent := &executorTestAgent{}
	executor, manager, repo := newExecutorFixture(t, "1s", restJobOutputLimit, agent)
	for _, job := range []restjobs.Snapshot{
		{ID: executorJobID, Request: restjobs.Request{Repository: "unknown", Task: "do work"}},
		{ID: executorJobID, Request: restjobs.Request{Repository: "trusted", Task: "do work"}},
	} {
		if job.Request.Repository == "trusted" {
			executor.server.Repositories["trusted"] = filepath.Dir(repo)
		}
		if err := executor.Execute(context.Background(), job); !errors.Is(err, errExecutionFailed) {
			t.Fatalf("Execute(%q) error = %v, want sanitized rejection", job.Request.Repository, err)
		}
		if manager.createdID != "" {
			t.Fatalf("workspace created for invalid repository selection: %q", manager.createdID)
		}
	}
}

func TestAllowlistedEnvironmentDropsAPIKeysAndSecretShapedNames(t *testing.T) {
	got := allowlistedEnvironment([]string{
		"PATH=/bin", "HOME=/home/worker", "LANG=C", "LC_ALL=C.UTF-8", "XDG_STATE_HOME=/private/state",
		"HTTPS_PROXY=http://proxy", "REST_SERVER_API_KEY=secret", "API_TOKEN=secret", "DB_PASSWORD=secret",
		"MY_SECRET_FLAG=secret", "FACTORY_MODE=production",
	})
	joined := strings.Join(got, "\n")
	for _, want := range []string{"PATH=/bin", "HOME=/home/worker", "LANG=C", "LC_ALL=C.UTF-8", "XDG_STATE_HOME=/private/state", "HTTPS_PROXY=http://proxy"} {
		if !strings.Contains(joined, want) {
			t.Errorf("allowlisted environment missing %q: %q", want, got)
		}
	}
	for _, forbidden := range []string{"API_KEY", "API_TOKEN", "PASSWORD", "SECRET", "FACTORY_MODE"} {
		if strings.Contains(joined, forbidden) {
			t.Errorf("environment leaked excluded variable %q: %q", forbidden, got)
		}
	}
}

func TestFactoryExecutorPassesOnlyAllowlistedEnvironmentToHarness(t *testing.T) {
	t.Setenv("REST_SERVER_API_KEY", "must-not-leak")
	t.Setenv("REST_TEST_SECRET", "must-not-leak")
	agentScript := filepath.Join(t.TempDir(), "agent.sh")
	envFile := filepath.Join(t.TempDir(), "agent-env")
	if err := os.WriteFile(agentScript, []byte("#!/bin/sh\nprintf '%s|%s|%s|%s' \"$PATH\" \"$LANG\" \"$REST_SERVER_API_KEY\" \"$REST_TEST_SECRET\" > \""+envFile+"\"\nhead -c 5242880 /dev/zero | tr '\\000' x\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	executor, manager, _ := newExecutorFixture(t, "5s", restJobOutputLimit, &executorTestAgent{})
	executor.server.Harness.Executable = agentScript
	executor.env = allowlistedEnvironment(append(os.Environ(), "REST_SERVER_API_KEY=must-not-leak", "REST_TEST_SECRET=must-not-leak"))
	executor.agent = func(config factory.Config, env []string, output io.Writer) factory.Agent {
		return factory.Runner{Config: config, Env: env, OutputWriter: output}
	}
	if err := executor.Execute(context.Background(), executorJob("check environment")); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	got, err := os.ReadFile(envFile)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(string(got), "|")
	if len(parts) != 4 || parts[0] == "" || parts[1] == "" || parts[2] != "" || parts[3] != "" {
		t.Fatalf("harness environment = %q, want basic environment and no credentials", got)
	}
	if manager.markedID != executorJobID {
		t.Fatalf("job did not complete after safe harness invocation: %q", manager.markedID)
	}
	capture, err := os.ReadFile(filepath.Join(manager.workspace.OutputPath, "workflow.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(capture) > restJobOutputLimit || !strings.Contains(string(capture), "output truncated") {
		t.Fatalf("subprocess output capture length=%d, truncated=%v", len(capture), strings.Contains(string(capture), "output truncated"))
	}
}

func TestFactoryExecutorMarksARealWorkspaceOnlyAfterCompleteWorkflow(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"config", "user.email", "test@example.com"}, {"config", "user.name", "REST real workspace test"}} {
		if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(repo, "tracked"), []byte("baseline\\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "tracked"}, {"commit", "-q", "-m", "baseline"}} {
		if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	serverRoot := filepath.Join(root, "server")
	if err := os.Mkdir(serverRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	workspaces, err := restworkspace.New(restworkspace.Config{RepositoryRoot: repo, ResultsRoot: filepath.Join(serverRoot, "results")})
	if err != nil {
		t.Fatal(err)
	}
	stageFile := filepath.Join(root, "stages")
	checkFile := filepath.Join(root, "check.sh")
	marker := filepath.Join(serverRoot, "results", executorJobID, ".completion.json")
	if err := os.WriteFile(checkFile, []byte("#!/bin/sh\ntest ! -e \""+marker+"\"\nprintf checks-passed\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	agentScript := filepath.Join(root, "agent.sh")
	if err := os.WriteFile(agentScript, []byte("#!/bin/sh\nprintf 'ran\\n' >> \""+stageFile+"\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	server := restserver.DefaultConfig()
	server.Repositories = map[string]string{"trusted": repo}
	server.Harness = restserver.HarnessConfig{Executable: agentScript, Args: []string{"{task}", "{system_prompt}"}}
	server.APIKeyFile = filepath.Join(root, "api-key")
	server.Limits.JobTimeout = "5s"
	workflowConfig := factory.DefaultConfig()
	workflowConfig.PipelineChecks = [][]string{{checkFile}}
	executor, err := NewFactoryExecutor(FactoryExecutorConfig{
		Server: server, Workflow: workflowConfig, Workspaces: map[string]WorkspaceManager{"trusted": workspaces},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.Execute(context.Background(), executorJob("real worktree")); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	stageData, err := os.ReadFile(stageFile)
	if err != nil || strings.TrimSpace(string(stageData)) != "ran\nran\nran\nran" {
		t.Fatalf("real harness stages = %q, err=%v", stageData, err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("successful job completion marker missing: %v", err)
	}
}

func TestFactoryExecutorDoesNotReportSuccessWhenCompletionMarkerFails(t *testing.T) {
	agent := &executorTestAgent{}
	executor, manager, _ := newExecutorFixture(t, "1s", restJobOutputLimit, agent)
	manager.markErr = errors.New("marker filesystem detail")
	if err := executor.Execute(context.Background(), executorJob("complete work")); !errors.Is(err, errExecutionFailed) {
		t.Fatalf("Execute() error = %v, want sanitized marker failure", err)
	}
	if manager.markedID != executorJobID {
		t.Fatalf("completion marker attempted for %q, want %q", manager.markedID, executorJobID)
	}
}
