package restworker

import (
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

	"github.com/leehosanganson/factory/internal/jobs"
	"github.com/leehosanganson/factory/internal/provider"
	"github.com/leehosanganson/factory/internal/server"
	"github.com/leehosanganson/factory/internal/workflow"
	"github.com/leehosanganson/factory/internal/workspace"
)

const (
	executorJobID      = "01234567-89ab-4cde-8fab-0123456789ab"
	restJobOutputLimit = 16 << 20
)

type testWorkspaceManager struct {
	root       string
	workspace  restworkspace.Workspace
	createdID  string
	markedID   string
	markErr    error
	createErr  error
	createFn   func(context.Context) (restworkspace.Workspace, error)
	revision   restworkspace.SourceRevision
	captureErr error
}

func (m *testWorkspaceManager) RepositoryRoot() string { return m.root }

func (m *testWorkspaceManager) VerifyResultDirectory(path string) error {
	if filepath.Clean(path) != path || (filepath.Base(path) != executorJobID && filepath.Base(filepath.Dir(filepath.Dir(path))) != executorJobID) {
		return errors.New("invalid result directory")
	}
	return nil
}

func (m *testWorkspaceManager) CaptureContext(context.Context) (restworkspace.SourceRevision, error) {
	return m.revision, m.captureErr
}

func (m *testWorkspaceManager) CreateRevisionContext(ctx context.Context, id string, revision restworkspace.SourceRevision) (restworkspace.Workspace, error) {
	m.createdID = id
	if m.createFn != nil {
		return m.createFn(ctx)
	}
	if m.createErr != nil {
		return restworkspace.Workspace{}, m.createErr
	}
	workspace := m.workspace
	workspace.SourceRevision = revision
	return workspace, nil
}

func (m *testWorkspaceManager) CreateAttemptRevisionContext(ctx context.Context, id string, generation int64, revision restworkspace.SourceRevision) (restworkspace.Workspace, error) {
	m.createdID = id
	if generation == 0 {
		return m.CreateRevisionContext(ctx, id, revision)
	}
	if m.createFn != nil {
		return m.createFn(ctx)
	}
	if m.createErr != nil {
		return restworkspace.Workspace{}, m.createErr
	}
	workspace := m.workspace
	workspace.Generation = generation
	workspace.WorktreePath = filepath.Join(filepath.Dir(workspace.WorktreePath), "attempts", fmt.Sprint(generation), "worktree")
	workspace.StatePath = filepath.Join(filepath.Dir(workspace.WorktreePath), "state")
	workspace.OutputPath = filepath.Join(filepath.Dir(workspace.WorktreePath), "output")
	workspace.SourceRevision = revision
	if err := os.MkdirAll(filepath.Dir(workspace.WorktreePath), 0o700); err != nil {
		return restworkspace.Workspace{}, err
	}
	if output, err := exec.Command("git", "-C", m.root, "worktree", "add", "--detach", workspace.WorktreePath, "HEAD").CombinedOutput(); err != nil {
		return restworkspace.Workspace{}, fmt.Errorf("create attempt fixture worktree: %v: %s", err, output)
	}
	for _, path := range []string{workspace.StatePath, workspace.OutputPath} {
		if err := os.Mkdir(path, 0o700); err != nil {
			return restworkspace.Workspace{}, err
		}
	}
	m.workspace = workspace
	return workspace, nil
}

func (m *testWorkspaceManager) RetainedWorkspaceForGeneration(id string, generation int64) (restworkspace.Workspace, error) {
	workspace := m.workspace
	workspace.Generation = generation
	workspace.WorktreePath = filepath.Join(filepath.Dir(workspace.WorktreePath), "attempts", fmt.Sprint(generation), "worktree")
	workspace.StatePath = filepath.Join(filepath.Dir(workspace.WorktreePath), "state")
	workspace.OutputPath = filepath.Join(filepath.Dir(workspace.WorktreePath), "output")
	return workspace, nil
}

func (m *testWorkspaceManager) MarkSucceeded(id string) error {
	m.markedID = id
	return m.markErr
}

func TestProviderReconciliationUsesExactGenerationAndLegacyWorkspace(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"config", "user.email", "test@example.com"}, {"config", "user.name", "Reconcile Test"}} {
		if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(repo, "tracked"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	exec.Command("git", "-C", repo, "add", "tracked").Run()
	if output, err := exec.Command("git", "-C", repo, "commit", "-q", "-m", "base").CombinedOutput(); err != nil {
		t.Fatalf("commit source: %v: %s", err, output)
	}
	manager, err := restworkspace.New(restworkspace.Config{RepositoryRoot: repo, ResultsRoot: filepath.Join(root, "server", "results")})
	if err != nil {
		t.Fatal(err)
	}
	server := restserver.DefaultConfig()
	server.Repositories = map[string]string{"trusted": repo}
	server.Harness = restserver.HarnessConfig{Executable: "agent", Args: []string{"{task}", "{system_prompt}"}}
	server.APIKeyFile = filepath.Join(root, "api-key")
	server.Limits.JobTimeout = "10s"
	executor, err := NewFactoryExecutor(FactoryExecutorConfig{Server: server, Workflow: workflow.DefaultConfig(), Workspaces: map[string]WorkspaceManager{"trusted": manager}})
	if err != nil {
		t.Fatal(err)
	}
	executor.server.Provider.Backend = "github"
	executor.server.Provider.Repositories = map[string]string{"trusted": "acme/widget"}
	executor.server.Provider.BaseBranch = "main"
	publish := func(t *testing.T, generation int64, legacy bool) (string, string) {
		t.Helper()
		var workspace restworkspace.Workspace
		if legacy {
			workspace, err = manager.Create(executorJobID)
		} else {
			var revision restworkspace.SourceRevision
			revision, err = manager.CaptureContext(context.Background())
			if err == nil {
				workspace, err = manager.CreateAttemptRevisionContext(context.Background(), executorJobID, generation, revision)
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, config := range [][2]string{{"user.email", "test@example.com"}, {"user.name", "Reconcile Test"}} {
			if output, err := exec.Command("git", "-C", workspace.WorktreePath, "config", config[0], config[1]).CombinedOutput(); err != nil {
				t.Fatalf("configure attempt %s: %v: %s", config[0], err, output)
			}
		}
		if err := os.WriteFile(filepath.Join(workspace.WorktreePath, "change"), []byte("change\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		branch := "factory/job/" + executorJobID
		for _, args := range [][]string{{"add", "change"}, {"commit", "-q", "-m", "change"}, {"switch", "-c", branch}} {
			if output, err := exec.Command("git", append([]string{"-C", workspace.WorktreePath}, args...)...).CombinedOutput(); err != nil {
				t.Fatalf("git %v: %v: %s", args, err, output)
			}
		}
		commit, err := exec.Command("git", "-C", workspace.WorktreePath, "rev-parse", "HEAD").Output()
		if err != nil {
			t.Fatal(err)
		}
		return workspace.WorktreePath, strings.TrimSpace(string(commit))
	}
	legacyPath, legacyCommit := publish(t, 0, true)
	attemptPath, commit := publish(t, 2, false)
	job := executorJob("read-only reconciliation")
	job.Status = restjobs.StatusFailed
	attempt := restjobs.ProviderAttempt{Generation: 2, Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + executorJobID, Commit: commit}
	var calls int
	reconciler := testJobReconciler(func(_ context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
		calls++
		if request.Worktree != attemptPath {
			t.Errorf("lookup worktree = %q, want exact generation path %q", request.Worktree, attemptPath)
		}
		return restprovider.Outcome{Provider: "github", Repository: "acme/widget", Number: 7, URL: "https://github.com/acme/widget/pull/7", Branch: attempt.Branch, Commit: commit, State: "open"}, nil
	})
	if _, err := executor.ReconcileInterruptedProvider(context.Background(), job, attempt, reconciler); err != nil || calls != 1 {
		t.Fatalf("generation reconciliation = %v calls=%d", err, calls)
	}
	missing := attempt
	missing.Generation = 3
	if _, err := executor.ReconcileInterruptedProvider(context.Background(), job, missing, reconciler); err == nil || calls != 1 {
		t.Fatalf("missing generation fell back: err=%v lookup calls=%d", err, calls)
	}
	legacy := attempt
	legacy.Generation = 0
	legacy.Commit = legacyCommit
	if _, err := executor.ReconcileInterruptedProvider(context.Background(), job, legacy, testJobReconciler(func(_ context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
		if request.Worktree != legacyPath {
			t.Errorf("legacy lookup worktree = %q, want %q", request.Worktree, legacyPath)
		}
		return restprovider.Outcome{Provider: "github", Repository: "acme/widget", Number: 8, URL: "https://github.com/acme/widget/pull/8", Branch: legacy.Branch, Commit: legacy.Commit, State: "open"}, nil
	})); err != nil {
		t.Fatalf("legacy reconciliation failed: %v", err)
	}
}

type testJobReconciler func(context.Context, restprovider.PublishRequest) (restprovider.Outcome, error)

func (f testJobReconciler) Reconcile(ctx context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
	return f(ctx, request)
}

func TestFactoryExecutorCancellationOutcomePreservesUncertainProviderAttempt(t *testing.T) {
	executor := &FactoryExecutor{providerAttempts: make(map[string]bool)}
	if got := executor.CancellationOutcome("before-attempt"); got != restjobs.StatusCanceled {
		t.Fatalf("cancellation before provider attempt = %q, want canceled", got)
	}
	executor.providerAttempts["uncertain-attempt"] = true
	if got := executor.CancellationOutcome("uncertain-attempt"); got != restjobs.StatusFailed {
		t.Fatalf("cancellation after provider attempt = %q, want failed for operator investigation", got)
	}
}

func TestFactoryExecutorAndWorkerPersistUncertainProviderAttempt(t *testing.T) {
	databaseDir, err := os.MkdirTemp(t.TempDir(), "sqlite-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(databaseDir, 0o700); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(databaseDir, "jobs.db")
	store, err := restjobs.OpenSQLiteStore(database, restjobs.Config{
		QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	job, _, err := store.Admit("uncertain-publish", restjobs.Request{Repository: "trusted", Task: "publish verified change"})
	if err != nil {
		t.Fatal(err)
	}

	agent := &executorTestAgent{createFileDuringStage: "implement"}
	executor, _, repo := newExecutorFixture(t, "10s", restJobOutputLimit, agent)
	canonicalDatabaseDir, err := filepath.EvalSymlinks(databaseDir)
	if err != nil {
		t.Fatal(err)
	}
	resultsRoot := filepath.Join(canonicalDatabaseDir, "results")
	if err := os.Mkdir(resultsRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	workspaceManager, err := restworkspace.New(restworkspace.Config{RepositoryRoot: repo, ResultsRoot: resultsRoot})
	if err != nil {
		t.Fatal(err)
	}
	executor.workspaces["trusted"] = workspaceManager
	executor.server.Provider.Repositories = map[string]string{"trusted": "acme/widget"}
	executor.server.Provider.BaseBranch = "main"
	executor.recordProviderAttempt = store.RecordProviderAttemptForAttempt
	executor.markProviderAttemptUncertain = store.MarkProviderAttemptUncertainForAttempt
	executor.recordVerificationEvidence = store.RecordVerificationEvidenceForAttempt
	attemptBeforePublish := make(chan restjobs.ProviderAttempt, 1)
	executor.provider = testJobPublisher(func(_ context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
		attempt, err := store.ProviderAttempt(request.JobID)
		if err != nil {
			t.Errorf("ProviderAttempt(%q) during Publish = %v", request.JobID, err)
		} else {
			attemptBeforePublish <- attempt
		}
		return restprovider.Outcome{}, fmt.Errorf("%w: simulated lost response", restprovider.ErrUncertain)
	})

	coordinator, err := New(store, executor, CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := coordinator.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	}()

	failed := waitForStatus(t, store, job.ID, restjobs.StatusFailed)
	if failed.Provider != nil {
		t.Fatalf("uncertain publication recorded provider outcome: %+v", failed.Provider)
	}
	select {
	case attempt := <-attemptBeforePublish:
		if attempt.Generation != 1 || attempt.Provider != "github" || attempt.Repository != "acme/widget" || attempt.Branch != "factory/job/"+job.ID || attempt.Commit == "" || attempt.Uncertain {
			t.Fatalf("persisted provider attempt visible during Publish = %+v", attempt)
		}
	case <-time.After(time.Second):
		t.Fatal("Publisher.Publish did not observe the persisted provider attempt")
	}
	persisted, err := store.ProviderAttempt(job.ID)
	if err != nil || !persisted.Uncertain {
		t.Fatalf("provider attempt after ErrUncertain = %+v, %v; want marked uncertain", persisted, err)
	}
	inspected, err := store.Get(job.ID)
	if err != nil || inspected.Status != restjobs.StatusFailed || inspected.Provider != nil {
		t.Fatalf("uncertain job inspection = %+v, %v; want failed with no provider outcome", inspected, err)
	}
	history, err := store.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history.Events) == 0 || history.Events[len(history.Events)-1].Type != string(restjobs.StatusFailed) {
		t.Fatalf("uncertain job history = %+v, want terminal failed event", history.Events)
	}
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
	if manager.markedID != "" {
		t.Fatalf("provider completion attempted legacy cleanup marker: %q", manager.markedID)
	}
}

type testJobPublisher func(context.Context, restprovider.PublishRequest) (restprovider.Outcome, error)

func (f testJobPublisher) Publish(ctx context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
	return f(ctx, request)
}
func (f testJobPublisher) Ping(context.Context) error { return nil }

type executorTestAgent struct {
	calls                 []string
	output                io.Writer
	failAt                string
	block                 bool
	outputBytes           int
	createFileDuringStage string
}

func (a *executorTestAgent) WithOutputWriter(output io.Writer) workflow.Agent {
	return a
}

func (a *executorTestAgent) Run(stage, prompt, task, workdir, log string) error {
	return a.RunWithContext(context.Background(), stage, prompt, task, workdir, log)
}

func (a *executorTestAgent) RunWithContext(ctx context.Context, stage, _, _, workdir, logPath string) error {
	a.calls = append(a.calls, stage)
	if stage == a.createFileDuringStage {
		if err := os.WriteFile(filepath.Join(workdir, "provider-change"), []byte("verified\n"), 0o600); err != nil {
			return err
		}
	}
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
	workflowConfig := workflow.Config{
		Command: "placeholder", Args: []string{"{task}", "{system_prompt}"}, StateDir: state,
		PipelineChecks: checks, AutoPublish: true,
	}
	executor, err := NewFactoryExecutor(FactoryExecutorConfig{
		Server: server, Workflow: workflowConfig,
		Workspaces: map[string]WorkspaceManager{"trusted": manager},
	})
	if err != nil {
		t.Fatalf("NewFactoryExecutor() error = %v", err)
	}
	executor.agent = func(_ workflow.Config, _ []string, output io.Writer) workflow.Agent {
		agent.output = output
		return agent
	}
	return executor, manager, repo
}

func executorJob(task string) restjobs.Snapshot {
	return restjobs.Snapshot{ID: executorJobID, Request: restjobs.Request{Repository: "trusted", Task: task}}
}

func executorAttempt() restjobs.AttemptHandle {
	return restjobs.AttemptHandle{JobID: executorJobID, Generation: 1, OwnerToken: strings.Repeat("a", 64)}
}

func TestFactoryExecutorRejectsProviderExecutionWithoutAttemptBeforeSideEffects(t *testing.T) {
	agent := &executorTestAgent{}
	executor, workspace, _ := newExecutorFixture(t, "10s", restJobOutputLimit, agent)
	var providerCalls int
	executor.provider = testJobPublisher(func(context.Context, restprovider.PublishRequest) (restprovider.Outcome, error) {
		providerCalls++
		return restprovider.Outcome{}, nil
	})

	if _, err := executor.ExecuteWithResult(context.Background(), executorJob("must not start")); !errors.Is(err, errExecutionFailed) {
		t.Fatalf("ExecuteWithResult() error = %v, want execution failure", err)
	}
	if len(agent.calls) != 0 || workspace.createdID != "" || providerCalls != 0 {
		t.Fatalf("provider execution without attempt caused side effects: stages=%v workspace=%q provider calls=%d", agent.calls, workspace.createdID, providerCalls)
	}
}

func TestFactoryExecutorLegacyEntriesRejectPersistenceWithoutAttemptBeforeSideEffects(t *testing.T) {
	for _, entry := range []struct {
		name string
		run  func(*FactoryExecutor) error
	}{
		{name: "Execute", run: func(executor *FactoryExecutor) error {
			return executor.Execute(context.Background(), executorJob("must not start"))
		}},
		{name: "ExecuteWithResult", run: func(executor *FactoryExecutor) error {
			_, err := executor.ExecuteWithResult(context.Background(), executorJob("must not start"))
			return err
		}},
	} {
		t.Run(entry.name, func(t *testing.T) {
			agent := &executorTestAgent{}
			executor, workspace, _ := newExecutorFixture(t, "10s", restJobOutputLimit, agent)
			var callbacks int
			executor.recordVerificationEvidence = func(restjobs.AttemptHandle, restjobs.VerificationEvidence) error {
				callbacks++
				return nil
			}
			if err := entry.run(executor); !errors.Is(err, errExecutionFailed) {
				t.Fatalf("legacy direct entry error = %v, want execution failure", err)
			}
			if len(agent.calls) != 0 || workspace.createdID != "" || callbacks != 0 {
				t.Fatalf("legacy entry caused workflow side effects: stages=%v workspace=%q persistence callbacks=%d", agent.calls, workspace.createdID, callbacks)
			}
		})
	}
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

func TestNewLocalJobManagerReturnsConfiguredDurableStoreOpenFailures(t *testing.T) {
	for _, persistence := range []restserver.PersistenceConfig{
		{Backend: restserver.PersistenceBackendSQLite, Path: "relative.db"},
		{Backend: restserver.PersistenceBackendPostgres, DSN: "postgres://factory:credential-secret@127.0.0.1:1/factory?connect_timeout=1"},
	} {
		t.Run(persistence.Backend, func(t *testing.T) {
			config := restserver.DefaultConfig()
			config.Repositories = map[string]string{"trusted": t.TempDir()}
			config.Harness = restserver.HarnessConfig{Executable: "agent", Args: []string{"{task}", "{system_prompt}"}}
			config.APIKeyFile = filepath.Join(t.TempDir(), "api-key")
			config.Persistence = persistence
			if _, err := NewLocalJobManager(config); err == nil {
				t.Fatal("configured durable-store failure silently fell back to memory")
			} else if strings.Contains(err.Error(), "credential-secret") {
				t.Fatalf("store open error leaked DSN credentials: %v", err)
			}
		})
	}
}

func TestNewFactoryExecutorRequiresWorkspaceForMatchingTrustedRepository(t *testing.T) {
	validManager := &testWorkspaceManager{root: t.TempDir()}
	server := restserver.DefaultConfig()
	server.Repositories = map[string]string{"trusted": validManager.root}
	server.Harness = restserver.HarnessConfig{Executable: "agent", Args: []string{"{task}", "{system_prompt}"}}
	server.APIKeyFile = filepath.Join(t.TempDir(), "api-key")
	workflowConfig := workflow.Config{Command: "agent", Args: []string{"{task}", "{system_prompt}"}}
	if _, err := NewFactoryExecutor(FactoryExecutorConfig{Server: server, Workflow: workflowConfig}); err == nil {
		t.Fatal("NewFactoryExecutor() accepted a missing workspace manager")
	}
}

func TestFactoryExecutorRejectsUncapturableSourceBeforeWorkspaceOrWorkflow(t *testing.T) {
	agent := &executorTestAgent{}
	executor, manager, _ := newExecutorFixture(t, "10s", restJobOutputLimit, agent)
	manager.captureErr = errors.New("unsupported source")
	if err := executor.Execute(context.Background(), executorJob("do not run")); !errors.Is(err, errExecutionFailed) {
		t.Fatalf("Execute() error = %v, want sanitized execution failure", err)
	}
	if manager.createdID != "" || len(agent.calls) != 0 {
		t.Fatalf("unsupported source caused side effects: workspace=%q stages=%v", manager.createdID, agent.calls)
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
	var evidence restjobs.VerificationEvidence
	executor.recordVerificationEvidence = func(attempt restjobs.AttemptHandle, recorded restjobs.VerificationEvidence) error {
		if attempt != executorAttempt() {
			t.Fatalf("evidence attempt = %+v", attempt)
		}
		evidence = recorded
		return nil
	}
	before, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.ExecuteWithAttempt(context.Background(), executorJob("implement feature"), executorAttempt()); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got, want := strings.Join(agent.calls, ","), "requirements,implement,review,document"; got != want {
		t.Fatalf("stage order = %q, want %q", got, want)
	}
	if workspace.createdID != executorJobID || workspace.markedID != "" {
		t.Fatalf("attempt workspace IDs = create %q, cleanup marker %q; want created without marker", workspace.createdID, workspace.markedID)
	}
	if len(evidence.Checks) != 1 || evidence.Checks[0] != (restjobs.VerificationCheck{Name: "check-01", Outcome: restjobs.VerificationPassed}) || len(evidence.Limitations) != 1 || evidence.Limitations[0] != restjobs.LimitationAgentNotVerdict {
		t.Fatalf("recorded verification evidence = %+v", evidence)
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

func TestFactoryExecutorRecordsNoConfiguredCheckLimitation(t *testing.T) {
	agent := &executorTestAgent{}
	executor, _, _ := newExecutorFixture(t, "10s", restJobOutputLimit, agent)
	var evidence restjobs.VerificationEvidence
	executor.recordVerificationEvidence = func(_ restjobs.AttemptHandle, recorded restjobs.VerificationEvidence) error {
		evidence = recorded
		return nil
	}
	if err := executor.ExecuteWithAttempt(context.Background(), executorJob("do work"), executorAttempt()); err != nil {
		t.Fatalf("ExecuteWithAttempt() error = %v", err)
	}
	if len(evidence.Checks) != 0 || len(evidence.Limitations) != 2 || evidence.Limitations[0] != restjobs.LimitationAgentNotVerdict || evidence.Limitations[1] != restjobs.LimitationNoChecksConfigured {
		t.Fatalf("no-check verification evidence = %+v", evidence)
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
			var evidence restjobs.VerificationEvidence
			executor.recordVerificationEvidence = func(_ restjobs.AttemptHandle, recorded restjobs.VerificationEvidence) error {
				evidence = recorded
				return nil
			}
			err := executor.ExecuteWithAttempt(context.Background(), executorJob("do work"), executorAttempt())
			if !errors.Is(err, errExecutionFailed) || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "private") {
				t.Fatalf("Execute() error = %v, want sanitized failure", err)
			}
			if test.check && (len(evidence.Checks) != 1 || evidence.Checks[0] != (restjobs.VerificationCheck{Name: "check-01", Outcome: restjobs.VerificationFailed})) {
				t.Fatalf("failed check evidence = %+v", evidence)
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
	var evidence restjobs.VerificationEvidence
	executor.recordVerificationEvidence = func(_ restjobs.AttemptHandle, recorded restjobs.VerificationEvidence) error {
		evidence = recorded
		return nil
	}
	var observed []workflow.WorkflowEvent
	executor.observer = workflow.WorkflowObserverFunc(func(event workflow.WorkflowEvent) error {
		observed = append(observed, event)
		return nil
	})
	if err := executor.ExecuteWithAttempt(context.Background(), executorJob("sensitive user task"), executorAttempt()); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := strings.Join(agent.calls, ","); got != "requirements,implement,review,document" {
		t.Fatalf("stages stopped after reaching the output cap: %q", got)
	}
	if _, err := os.Stat(checkMarker); err != nil {
		t.Fatalf("check did not finish draining after output cap: %v", err)
	}
	if len(evidence.Checks) != 1 || evidence.Checks[0].Outcome != restjobs.VerificationPassed {
		t.Fatalf("check completion evidence = %+v", evidence)
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
	if err := os.WriteFile(agentScript, []byte("#!/bin/sh\nprintf '%s|%s|%s|%s' \"$PATH\" \"$LANG\" \"$REST_SERVER_API_KEY\" \"$REST_TEST_SECRET\" > \""+envFile+"\"\nhead -c 131072 /dev/zero | tr '\\000' x\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	executor, manager, _ := newExecutorFixture(t, "5s", 64<<10, &executorTestAgent{})
	executor.server.Harness.Executable = agentScript
	executor.env = allowlistedEnvironment(append(os.Environ(), "REST_SERVER_API_KEY=must-not-leak", "REST_TEST_SECRET=must-not-leak"))
	executor.agent = func(config workflow.Config, env []string, output io.Writer) workflow.Agent {
		return workflow.Runner{Config: config, Env: env, OutputWriter: output}
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
	if len(capture) > 64<<10 || !strings.Contains(string(capture), "output truncated") {
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
	workflowConfig := workflow.DefaultConfig()
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

func TestFactoryExecutorSuccessDoesNotDependOnAttemptCleanupMarker(t *testing.T) {
	agent := &executorTestAgent{}
	executor, manager, _ := newExecutorFixture(t, "1s", restJobOutputLimit, agent)
	manager.markErr = errors.New("marker filesystem detail")
	if err := executor.ExecuteWithAttempt(context.Background(), executorJob("complete work"), executorAttempt()); err != nil {
		t.Fatalf("ExecuteWithAttempt() failed due to cleanup marker: %v", err)
	}
	if manager.markedID != "" {
		t.Fatalf("attempt workspace unexpectedly got a cleanup marker: %q", manager.markedID)
	}
}
