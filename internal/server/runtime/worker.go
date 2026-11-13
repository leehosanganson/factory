package runtime

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/leehosanganson/factory/internal/jobs"
	"github.com/leehosanganson/factory/internal/provider"
	"github.com/leehosanganson/factory/internal/server"
	"github.com/leehosanganson/factory/internal/worker"
	"github.com/leehosanganson/factory/internal/workflow"
	"github.com/leehosanganson/factory/internal/workspace"
)

// RunWorker starts a worker-only process. It never loads API principals or
// binds an HTTP listener; PostgreSQL is required to share claims with a server.
func RunWorker(ctx context.Context, config restserver.Config) error {
	if ctx == nil {
		return errors.New("worker context must not be nil")
	}
	if err := config.Validate(); err != nil && !(config.Execution != nil && config.Execution.Mode == "kubernetes" && strings.Contains(err.Error(), "Kubernetes execution is not implemented")) {
		return fmt.Errorf("invalid REST worker config: %w", err)
	}
	if config.Execution != nil && config.Execution.Mode == "kubernetes" {
		return errors.New("Kubernetes execution is not implemented")
	}
	if config.Persistence.Backend != restserver.PersistenceBackendPostgres {
		return errors.New("factory worker requires PostgreSQL persistence")
	}
	if config.WorkerController == nil {
		return errors.New("factory worker requires worker_controller configuration")
	}
	if err := config.ValidateRepositoryRoots(); err != nil {
		return err
	}
	workerID := config.WorkerID
	if workerID == "" {
		workerID = fmt.Sprintf("factory-%d", os.Getpid())
	}
	bootID := processBootID()
	controller, err := newWorkerSessionClient(*config.WorkerController)
	if err != nil {
		return err
	}
	defer controller.close()
	limits := restjobs.Config{QueueCapacity: config.Limits.QueueCapacity, MaxConcurrentJobs: config.Limits.Workers, MaxRecords: config.Limits.MaxRecords, MaxEventsPerJob: config.Limits.MaxEventsPerJob, MaxTaskBytes: config.Limits.TaskBytes, RegistryBytes: config.Limits.RegistryBytes}
	store, err := restjobs.OpenPostgresWorkerStore(config.Persistence.DSN, limits, workerID, bootID)
	if err != nil {
		return err
	}
	defer store.CloseStore()
	if err := store.Ping(ctx); err != nil {
		return errors.New("PostgreSQL worker store is unavailable")
	}
	var publisher restprovider.Publisher
	if config.Provider.Backend == "github" {
		token, err := restserver.LoadProviderToken(config.Provider.TokenFile)
		if err != nil {
			return err
		}
		publisher = restprovider.NewGitHubPublisherWithTokenPush(token.Value(), &http.Client{Timeout: 20 * time.Second}, restprovider.GitPushBranchWithToken)
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = publisher.Ping(probeCtx)
		cancel()
		if err != nil {
			return errors.New("configured GitHub provider is unavailable")
		}
	}
	resultsBase, err := serverResultsBase()
	if err != nil {
		return err
	}
	workspaces := make(map[string]restworker.WorkspaceManager, len(config.Repositories))
	for alias, root := range config.Repositories {
		workspace, err := restworkspace.New(restworkspace.Config{RepositoryRoot: root, ResultsRoot: filepath.Join(resultsBase, aliasDirectory(alias), "results")})
		if err != nil {
			return fmt.Errorf("initialize workspace for repository %q: %w", alias, err)
		}
		workspaces[alias] = workspace
	}
	workflowConfig := workflow.DefaultConfig()
	workflowConfig.AutoPublish = false
	workflowConfig.WorktreeParent = ""
	workflowConfig.PipelineChecks = cloneVerificationChecks(config.VerificationChecks)
	if config.ParallelSubtasks != nil && config.ParallelSubtasks.Enabled {
		maxConcurrency := 4
		if config.ParallelSubtasks.MaxConcurrency != nil {
			maxConcurrency = *config.ParallelSubtasks.MaxConcurrency
		}
		workflowConfig.ParallelImplementation = &workflow.ParallelImplementationConfig{Enabled: true, MaxConcurrency: maxConcurrency}
	}
	executor, err := restworker.NewFactoryExecutorContext(ctx, restworker.FactoryExecutorConfig{
		Server: config, Workflow: workflowConfig, Workspaces: workspaces, Provider: publisher,
		RecordProviderAttempt:        store.RecordProviderAttemptForAttempt,
		MarkProviderAttemptUncertain: store.MarkProviderAttemptUncertainForAttempt,
		RecordVerificationEvidence:   store.RecordVerificationEvidenceForAttempt,
		RecordProgress:               store.RecordProgressForAttempt,
	})
	if err != nil {
		return fmt.Errorf("initialize REST workflow executor: %w", err)
	}
	registrationCtx, cancelRegistration := context.WithTimeout(ctx, workerSessionRequestTimeout)
	err = controller.register(registrationCtx, bootID)
	cancelRegistration()
	if err != nil {
		return err
	}
	coordinator, err := restworker.New(store, executor, restworker.CoordinatorConfig{Workers: config.Limits.Workers, WorkerID: workerID, BootID: bootID})
	if err != nil {
		return err
	}
	heartbeatCtx, stopHeartbeats := context.WithCancel(ctx)
	heartbeatFailure := make(chan struct{}, 1)
	heartbeatDone := controller.workerSessionHeartbeat(heartbeatCtx, func() {
		coordinator.StopClaims()
		heartbeatFailure <- struct{}{}
	})
	failed := false
	select {
	case <-ctx.Done():
	case <-heartbeatFailure:
		failed = true
	}
	stopHeartbeats()
	<-heartbeatDone
	coordinator.StopClaims()
	shutdownCtx, cancel := newWorkerShutdownContext(failed, 0)
	defer cancel()
	shutdownErr := coordinator.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		return fmt.Errorf("shut down REST worker: %w", shutdownErr)
	}
	if failed {
		return errors.New("worker controller heartbeat failed; claims stopped")
	}
	return nil
}

func newWorkerShutdownContext(heartbeatFailed bool, timeout time.Duration) (context.Context, context.CancelFunc) {
	if heartbeatFailed {
		return context.WithCancel(context.Background())
	}
	return newShutdownContext(timeout)
}
