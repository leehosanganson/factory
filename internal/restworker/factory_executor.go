package restworker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/leehosanganson/factory/internal/factory"
	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restprovider"
	"github.com/leehosanganson/factory/internal/restserver"
	"github.com/leehosanganson/factory/internal/restworkspace"
)

const startupRepositoryValidationTimeout = 10 * time.Second

var errExecutionFailed = errors.New("REST job execution failed")

// WorkspaceManager is the per-repository workspace lifecycle used by an executor.
type WorkspaceManager interface {
	RepositoryRoot() string
	CreateContext(context.Context, string) (restworkspace.Workspace, error)
	VerifyResultDirectory(string) error
	MarkSucceeded(string) error
}

// FactoryExecutorConfig contains trusted server settings and non-public Factory
// workflow settings such as verification checks and prompt overrides.
type FactoryExecutorConfig struct {
	Server                       restserver.Config
	Workflow                     factory.Config
	Workspaces                   map[string]WorkspaceManager
	WorkflowObserver             factory.WorkflowObserver
	RecordVerificationEvidence   func(string, restjobs.VerificationEvidence) error
	Provider                     restprovider.Publisher
	RecordProviderAttempt        func(string, restjobs.ProviderAttempt) error
	MarkProviderAttemptUncertain func(string) error

	// ValidateRepositoryRoot replaces Git-backed repository validation when set.
	// It must honor ctx; this seam is intended for tests and controlled callers.
	ValidateRepositoryRoot func(context.Context, string) error
}

// FactoryExecutor adapts one REST job to Factory's requirements, implement,
// review, and document workflow. It never publishes changes.
type FactoryExecutor struct {
	server                       restserver.Config
	workflow                     factory.Config
	workspaces                   map[string]WorkspaceManager
	timeout                      time.Duration
	env                          []string
	agent                        func(factory.Config, []string, io.Writer) factory.Agent
	validateRepositoryRoot       func(context.Context, string) error
	observer                     factory.WorkflowObserver
	recordVerificationEvidence   func(string, restjobs.VerificationEvidence) error
	provider                     restprovider.Publisher
	recordProviderAttempt        func(string, restjobs.ProviderAttempt) error
	markProviderAttemptUncertain func(string) error
}

// NewFactoryExecutor validates the trusted configuration and builds the
// multi-stage executor. Repository aliases must each have a configured workspace
// manager; managers must be constructed for the corresponding trusted root.
func NewFactoryExecutor(config FactoryExecutorConfig) (*FactoryExecutor, error) {
	return NewFactoryExecutorContext(context.Background(), config)
}

// NewFactoryExecutorContext constructs an executor while bounding startup Git
// validation by both the caller's lifecycle context and a fixed timeout.
func NewFactoryExecutorContext(ctx context.Context, config FactoryExecutorConfig) (*FactoryExecutor, error) {
	if ctx == nil {
		return nil, errors.New("invalid repository validation context")
	}
	validationCtx, cancelValidation := context.WithTimeout(ctx, startupRepositoryValidationTimeout)
	defer cancelValidation()
	return newFactoryExecutor(validationCtx, config, startupRepositoryValidationTimeout)
}

func newFactoryExecutor(ctx context.Context, config FactoryExecutorConfig, validationTimeout time.Duration) (*FactoryExecutor, error) {
	if err := config.Server.Validate(); err != nil {
		return nil, fmt.Errorf("invalid REST executor configuration")
	}
	if err := config.Workflow.Validate(); err != nil {
		return nil, fmt.Errorf("invalid Factory workflow configuration")
	}
	timeout, err := time.ParseDuration(config.Server.Limits.JobTimeout)
	if err != nil || timeout <= 0 {
		return nil, errors.New("invalid REST job timeout")
	}
	if validationTimeout <= 0 {
		return nil, errors.New("invalid repository validation timeout")
	}
	validateRoot := config.ValidateRepositoryRoot
	if validateRoot == nil {
		validateRoot = validateExactRepositoryRoot
	}
	validationCtx, cancelValidation := context.WithTimeout(ctx, validationTimeout)
	defer cancelValidation()
	workspaces := make(map[string]WorkspaceManager, len(config.Workspaces))
	for alias, workspace := range config.Workspaces {
		root := config.Server.Repositories[alias]
		if root == "" || workspace == nil || validateRoot(validationCtx, root) != nil || validationCtx.Err() != nil || workspace.RepositoryRoot() != root {
			return nil, errors.New("workspace managers must match configured repository aliases and roots")
		}
		workspaces[alias] = workspace
	}
	for alias := range config.Server.Repositories {
		if workspaces[alias] == nil {
			return nil, errors.New("workspace manager missing for configured repository")
		}
	}
	return &FactoryExecutor{
		server: cloneServerConfig(config.Server), workflow: cloneWorkflowConfig(config.Workflow), workspaces: workspaces,
		timeout: timeout, env: allowlistedEnvironment(os.Environ()), validateRepositoryRoot: validateRoot, observer: config.WorkflowObserver, recordVerificationEvidence: config.RecordVerificationEvidence, provider: config.Provider, recordProviderAttempt: config.RecordProviderAttempt, markProviderAttemptUncertain: config.MarkProviderAttemptUncertain,
		agent: func(workflow factory.Config, env []string, output io.Writer) factory.Agent {
			return factory.Runner{Config: workflow, Env: env, OutputWriter: output, DisableTranscript: true}
		},
	}, nil
}

// NewLocalJobManager builds the selected job store from validated trusted
// server settings. A configured SQLite failure is returned directly; there is
// no fallback to volatile memory.
func NewLocalJobManager(config restserver.Config) (restjobs.Store, error) {
	return newLocalJobManager(config, false)
}

// NewLocalServerJobManager opens the configured store with server ownership
// semantics, preventing a second process from running startup recovery against
// a database owned by a live REST server.
func NewLocalServerJobManager(config restserver.Config) (restjobs.Store, error) {
	return newLocalJobManager(config, true)
}

func newLocalJobManager(config restserver.Config, server bool) (restjobs.Store, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.New("invalid REST job manager configuration")
	}
	limits := restjobs.Config{
		QueueCapacity: config.Limits.QueueCapacity, MaxConcurrentJobs: config.Limits.Workers,
		MaxRecords: config.Limits.MaxRecords, MaxEventsPerJob: config.Limits.MaxEventsPerJob,
		MaxTaskBytes: config.Limits.TaskBytes, RegistryBytes: config.Limits.RegistryBytes,
	}
	if config.Persistence.Backend == restserver.PersistenceBackendSQLite {
		if server {
			return restjobs.OpenSQLiteServerStore(config.Persistence.Path, limits)
		}
		return restjobs.OpenSQLiteStore(config.Persistence.Path, limits)
	}
	return restjobs.NewManager(limits)
}

// Execute bounds workspace provisioning, every workflow stage, and configured
// checks with one job-wide deadline. All returned errors are intentionally
// generic because lower layers can include paths, command output, or secrets.
func (e *FactoryExecutor) RequiresProviderOutcome() bool { return e.provider != nil }

func (e *FactoryExecutor) CompleteResult(ctx context.Context, job restjobs.Snapshot) error {
	if ctx == nil || ctx.Err() != nil || e.provider == nil || job.Provider == nil {
		return errExecutionFailed
	}
	workspaceManager := e.workspaces[job.Request.Repository]
	if workspaceManager == nil || workspaceManager.MarkSucceeded(job.ID) != nil {
		return errExecutionFailed
	}
	return nil
}

func (e *FactoryExecutor) ReconcileProvider(ctx context.Context, job restjobs.Snapshot, attempt restjobs.ProviderAttempt, reconciler restprovider.Reconciler) (restjobs.ProviderOutcome, error) {
	if job.Status != restjobs.StatusFailed {
		return restjobs.ProviderOutcome{}, errExecutionFailed
	}
	return e.ReconcileInterruptedProvider(ctx, job, attempt, reconciler)
}

func (e *FactoryExecutor) ReconcileInterruptedProvider(ctx context.Context, job restjobs.Snapshot, attempt restjobs.ProviderAttempt, reconciler restprovider.Reconciler) (restjobs.ProviderOutcome, error) {
	if ctx == nil || ctx.Err() != nil || job.ID == "" || (job.Status != restjobs.StatusFailed && job.Status != restjobs.StatusRunning) || reconciler == nil || attempt.Provider != "github" || attempt.Repository != e.server.Provider.Repositories[job.Request.Repository] || attempt.Branch != restprovider.JobBranch(job.ID) {
		return restjobs.ProviderOutcome{}, errExecutionFailed
	}
	workspaceManager := e.workspaces[job.Request.Repository]
	retained, ok := workspaceManager.(interface {
		RetainedWorkspace(string) (restworkspace.Workspace, error)
	})
	if !ok || workspaceManager == nil || e.validateRepositoryRoot(ctx, e.server.Repositories[job.Request.Repository]) != nil {
		return restjobs.ProviderOutcome{}, errExecutionFailed
	}
	workspace, err := retained.RetainedWorkspace(job.ID)
	if err != nil {
		return restjobs.ProviderOutcome{}, errExecutionFailed
	}
	branch, err := providerGitCommand(ctx, workspace.WorktreePath, "branch", "--show-current").Output()
	if err != nil || strings.TrimSpace(string(branch)) != attempt.Branch {
		return restjobs.ProviderOutcome{}, errExecutionFailed
	}
	commit, err := providerGitCommand(ctx, workspace.WorktreePath, "rev-parse", "--verify", "HEAD").Output()
	if err != nil || strings.TrimSpace(string(commit)) != attempt.Commit {
		return restjobs.ProviderOutcome{}, errExecutionFailed
	}
	published, err := reconciler.Reconcile(ctx, restprovider.PublishRequest{
		JobID: job.ID, Repository: attempt.Repository, Worktree: workspace.WorktreePath,
		Branch: attempt.Branch, Commit: attempt.Commit, Title: job.Request.Task,
		Summary: "Factory completed configured workflow and verification checks.", BaseBranch: e.server.Provider.BaseBranch,
	})
	if err != nil || published.Provider != attempt.Provider || published.Repository != attempt.Repository || published.Branch != attempt.Branch || published.Commit != attempt.Commit || published.State != "open" {
		return restjobs.ProviderOutcome{}, errExecutionFailed
	}
	return restjobs.ProviderOutcome{Provider: published.Provider, Repository: published.Repository, Number: published.Number, URL: published.URL, Branch: published.Branch, Commit: published.Commit, State: published.State}, nil
}

func (e *FactoryExecutor) Execute(ctx context.Context, job restjobs.Snapshot) error {
	_, err := e.ExecuteWithResult(ctx, job)
	return err
}

func (e *FactoryExecutor) ExecuteWithResult(ctx context.Context, job restjobs.Snapshot) (*restjobs.ProviderOutcome, error) {
	if ctx == nil {
		return nil, errExecutionFailed
	}
	jobCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	if job.ID == "" || job.Request.Task == "" {
		return nil, errExecutionFailed
	}
	evidence := restjobs.VerificationEvidence{
		Checks:      make([]restjobs.VerificationCheck, len(e.workflow.PipelineChecks)),
		Limitations: []string{restjobs.LimitationAgentNotVerdict},
	}
	for index := range evidence.Checks {
		evidence.Checks[index] = restjobs.VerificationCheck{Name: fmt.Sprintf("check-%02d", index+1), Outcome: restjobs.VerificationNotCompleted}
	}
	if len(evidence.Checks) == 0 {
		evidence.Limitations = append(evidence.Limitations, restjobs.LimitationNoChecksConfigured)
	}
	if e.recordVerificationEvidence != nil && e.recordVerificationEvidence(job.ID, evidence) != nil {
		return nil, errExecutionFailed
	}
	root, ok := e.server.Repositories[job.Request.Repository]
	workspaceManager := e.workspaces[job.Request.Repository]
	if !ok || workspaceManager == nil || e.validateRepositoryRoot(jobCtx, root) != nil {
		return nil, errExecutionFailed
	}
	if err := jobCtx.Err(); err != nil {
		return nil, errExecutionFailed
	}
	workspace, err := workspaceManager.CreateContext(jobCtx, job.ID)
	if err != nil || jobCtx.Err() != nil || validateWorkspace(workspaceManager, job.ID, workspace) != nil {
		return nil, errExecutionFailed
	}
	outputFile, err := os.OpenFile(filepath.Join(workspace.OutputPath, "workflow.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, errExecutionFailed
	}
	defer outputFile.Close()
	capture := factory.NewBoundedOutputWriter(jobCtx, outputFile, e.server.Limits.HarnessOutput)
	workflowConfig := e.workflow
	workflowConfig.Command = e.server.Harness.Executable
	workflowConfig.Args = append([]string(nil), e.server.Harness.Args...)
	workflowConfig.PipelineChecks = cloneChecks(e.workflow.PipelineChecks)
	workflowConfig.StateDir = workspace.StatePath
	workflowConfig.AutoPublish = false
	completedChecks := 0
	observer := factory.WorkflowObserverFunc(func(event factory.WorkflowEvent) error {
		if event.Type == "check.completed" && completedChecks < len(evidence.Checks) {
			outcome := restjobs.VerificationFailed
			switch event.Outcome {
			case "success":
				outcome = restjobs.VerificationPassed
			case "canceled":
				outcome = restjobs.VerificationCanceled
			}
			evidence.Checks[completedChecks].Outcome = outcome
			completedChecks++
			if e.recordVerificationEvidence != nil && e.recordVerificationEvidence(job.ID, evidence) != nil {
				return errExecutionFailed
			}
		}
		if e.observer != nil {
			return e.observer.ObserveWorkflowEvent(event)
		}
		return nil
	})
	workflow := factory.Workflow{
		Agent: e.agent(workflowConfig, append([]string(nil), e.env...), capture), Config: workflowConfig,
		In: strings.NewReader(""), Out: io.Discard, Workdir: workspace.WorktreePath,
		Stages: []string{"requirements", "implement", "review", "document"},
		Gate:   false, RequireComplete: true, OutputWriter: capture, DisableTranscripts: true,
		DisableStateTask: true, PipelineCheckEnv: append([]string(nil), e.env...), Observer: observer,
	}
	runErr := workflow.RunContext(jobCtx, job.Request.Task)
	flushErr := capture.Flush()
	closeErr := outputFile.Close()
	if runErr != nil || flushErr != nil || closeErr != nil || jobCtx.Err() != nil {
		return nil, errExecutionFailed
	}
	var providerOutcome *restjobs.ProviderOutcome
	if e.provider != nil {
		commit, err := prepareProviderBranch(jobCtx, workspace.WorktreePath, job.ID)
		if err != nil {
			return nil, errExecutionFailed
		}
		branch := restprovider.JobBranch(job.ID)
		repository := e.server.Provider.Repositories[job.Request.Repository]

		if e.recordProviderAttempt != nil && e.recordProviderAttempt(job.ID, restjobs.ProviderAttempt{Provider: "github", Repository: repository, Branch: branch, Commit: commit}) != nil {
			return nil, errExecutionFailed
		}
		published, err := e.provider.Publish(jobCtx, restprovider.PublishRequest{JobID: job.ID, Repository: repository, Worktree: workspace.WorktreePath, Branch: branch, Commit: commit, Title: job.Request.Task, Summary: "Factory completed configured workflow and verification checks.", BaseBranch: e.server.Provider.BaseBranch})
		if err != nil {
			if errors.Is(err, restprovider.ErrUncertain) && e.markProviderAttemptUncertain != nil {
				_ = e.markProviderAttemptUncertain(job.ID)
			}
			return nil, errExecutionFailed
		}
		providerOutcome = &restjobs.ProviderOutcome{Provider: published.Provider, Repository: published.Repository, Number: published.Number, URL: published.URL, Branch: published.Branch, Commit: published.Commit, State: published.State}
	}
	if e.provider == nil {
		if err := workspaceManager.MarkSucceeded(job.ID); err != nil {
			return nil, errExecutionFailed
		}
	}
	return providerOutcome, nil
}

func providerGitCommand(ctx context.Context, worktree string, args ...string) *exec.Cmd {
	gitArgs := append([]string{"-C", worktree, "-c", "core.hooksPath=/dev/null"}, args...)
	cmd := exec.CommandContext(ctx, "git", gitArgs...)
	cmd.Env = allowlistedEnvironment(os.Environ())
	cmd.Env = append(cmd.Env, "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	return cmd
}

func prepareProviderBranch(ctx context.Context, worktree, jobID string) (string, error) {
	if ctx == nil || ctx.Err() != nil || !restprovider.JobIDValid(jobID) || !filepath.IsAbs(worktree) || filepath.Clean(worktree) != worktree {
		return "", errors.New("invalid provider worktree")
	}
	reported, err := providerGitCommand(ctx, worktree, "rev-parse", "--show-toplevel").Output()
	if err != nil {
		reported, err = providerGitCommand(ctx, worktree, "rev-parse", "--show-cdup").Output()
		if err != nil || len(reported) != 0 {
			return "", errors.New("provider worktree is invalid")
		}
	} else if strings.TrimSpace(string(reported)) != worktree {
		return "", errors.New("provider worktree is invalid")
	}
	if err := providerGitCommand(ctx, worktree, "switch", "--detach", "HEAD").Run(); err != nil {
		return "", errors.New("provider worktree could not be detached safely")
	}
	branch := restprovider.JobBranch(jobID)
	if err := providerGitCommand(ctx, worktree, "check-ref-format", "--branch", branch).Run(); err != nil {
		return "", errors.New("provider branch is invalid")
	}
	if err := providerGitCommand(ctx, worktree, "show-ref", "--verify", "--quiet", "refs/heads/"+branch).Run(); err == nil {
		return "", errors.New("provider job branch already exists")
	}
	if err := ctx.Err(); err != nil {
		return "", errors.New("provider job was canceled")
	}
	if err := providerGitCommand(ctx, worktree, "switch", "-c", branch).Run(); err != nil {
		return "", errors.New("create provider job branch")
	}
	if err := providerGitCommand(ctx, worktree, "add", "--all").Run(); err != nil {
		return "", errors.New("stage verified provider changes")
	}
	if err := providerGitCommand(ctx, worktree, "diff", "--cached", "--quiet").Run(); err == nil {
		return "", errors.New("provider worktree contains no changes")
	}
	commit := providerGitCommand(ctx, worktree, "-c", "core.hooksPath=/dev/null", "-c", "user.name=Factory", "-c", "user.email=factory@localhost", "commit", "--no-gpg-sign", "-m", "Factory job "+jobID)
	if err := commit.Run(); err != nil {
		return "", errors.New("commit verified provider changes")
	}
	head, err := providerGitCommand(ctx, worktree, "rev-parse", "--verify", "HEAD").Output()
	if err != nil {
		return "", errors.New("read provider commit")
	}
	value := strings.TrimSpace(string(head))
	if len(value) < 7 || len(value) > 64 || strings.Trim(value, "0123456789abcdefABCDEF") != "" {
		return "", errors.New("provider commit identity is invalid")
	}
	return value, nil
}

func validateWorkspace(manager WorkspaceManager, jobID string, workspace restworkspace.Workspace) error {
	if workspace.JobID != jobID || !filepath.IsAbs(workspace.WorktreePath) || !filepath.IsAbs(workspace.StatePath) || !filepath.IsAbs(workspace.OutputPath) {
		return errors.New("workspace paths are invalid")
	}
	resultDir := filepath.Dir(workspace.StatePath)
	if filepath.Clean(resultDir) != resultDir || workspace.WorktreePath != filepath.Join(resultDir, "worktree") || workspace.StatePath != filepath.Join(resultDir, "state") || workspace.OutputPath != filepath.Join(resultDir, "output") {
		return errors.New("workspace paths are not the expected job layout")
	}
	return manager.VerifyResultDirectory(resultDir)
}

func cloneServerConfig(config restserver.Config) restserver.Config {
	repositories := config.Repositories
	config.Repositories = make(map[string]string, len(repositories))
	for alias, root := range repositories {
		config.Repositories[alias] = root
	}
	config.Harness.Args = append([]string(nil), config.Harness.Args...)
	providerRepositories := config.Provider.Repositories
	config.Provider.Repositories = make(map[string]string, len(providerRepositories))
	for alias, repository := range providerRepositories {
		config.Provider.Repositories[alias] = repository
	}
	return config
}

func cloneWorkflowConfig(config factory.Config) factory.Config {
	config.Args = append([]string(nil), config.Args...)
	config.PipelineChecks = cloneChecks(config.PipelineChecks)
	if config.ParallelImplementation != nil {
		parallel := *config.ParallelImplementation
		config.ParallelImplementation = &parallel
	}
	return config
}

func cloneChecks(checks [][]string) [][]string {
	result := make([][]string, len(checks))
	for index, check := range checks {
		result[index] = append([]string(nil), check...)
	}
	return result
}

func validateExactRepositoryRoot(ctx context.Context, root string) error {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return errors.New("invalid repository root")
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return errors.New("invalid repository root")
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != root {
		return errors.New("invalid repository root")
	}
	command := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "--show-toplevel")
	output, err := command.Output()
	reportedRoot := strings.TrimSuffix(string(output), "\n")
	if err != nil || reportedRoot != root || strings.Contains(reportedRoot, "\n") {
		return errors.New("configured path is not the exact Git checkout root")
	}
	return nil
}

func allowlistedEnvironment(source []string) []string {
	allowed := make(map[string]string)
	for _, entry := range source {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || name == "" || secretEnvironmentName(name) {
			continue
		}
		upper := strings.ToUpper(name)
		if name == "PATH" || name == "HOME" || name == "LANG" || strings.HasPrefix(upper, "LC_") || strings.HasPrefix(upper, "XDG_") || isProxyEnvironmentName(upper) {
			allowed[name] = value
		}
	}
	result := make([]string, 0, len(allowed))
	for _, name := range []string{"PATH", "HOME", "LANG"} {
		if value, ok := allowed[name]; ok {
			result = append(result, name+"="+value)
			delete(allowed, name)
		}
	}
	for name, value := range allowed {
		result = append(result, name+"="+value)
	}
	return result
}

func isProxyEnvironmentName(name string) bool {
	switch name {
	case "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY":
		return true
	default:
		return false
	}
}

func secretEnvironmentName(name string) bool {
	upper := strings.ToUpper(name)
	for _, marker := range []string{"KEY", "SECRET", "TOKEN", "PASSWORD", "CREDENTIAL", "AUTH"} {
		if strings.Contains(upper, marker) {
			return true
		}
	}
	return false
}
