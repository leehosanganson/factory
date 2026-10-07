package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/leehosanganson/factory/internal/factory"
	"github.com/leehosanganson/factory/internal/restapi"
	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restprovider"
	"github.com/leehosanganson/factory/internal/restserver"
	"github.com/leehosanganson/factory/internal/restworker"
	"github.com/leehosanganson/factory/internal/restworkspace"
)

const (
	defaultShutdownTimeout = 15 * time.Second
	sweepInterval          = time.Hour
	sweepTimeout           = 30 * time.Second
	maxHeaderBytes         = 1 << 20
)

// runtimeOptions contains injectable seams for runtime lifecycle tests.
type runtimeOptions struct {
	Listen          func(string, string) (net.Listener, error)
	OpenStore       func(restserver.Config) (restjobs.Store, error)
	Executor        restworker.Executor
	Publisher       restprovider.Publisher
	ResultsBase     string
	ShutdownTimeout time.Duration
	Log             *log.Logger
	WorkspaceGit    func(context.Context, string, ...string) error
	WorkspaceNow    func() time.Time
	SweepInterval   time.Duration
}

type sqliteProviderReconciliationStore interface {
	Get(string) (restjobs.Snapshot, error)
	ProviderAttempt(string) (restjobs.ProviderAttempt, error)
	ReconcileProviderOutcome(string, restjobs.ProviderOutcome) error
	RecoveryNeeded(string) bool
}

type sqliteInterruptedDispositionStore interface {
	ResolveInterrupted(string, restjobs.InterruptedDisposition) error
	RecoveryNeeded(string) bool
}

// Run starts the REST server and blocks until ctx is canceled or serving fails.
func closeLocalStore(store restjobs.Store) {
	if closer, ok := store.(interface{ CloseStore() error }); ok {
		_ = closer.CloseStore()
	} else {
		store.Close()
	}
}

func Run(ctx context.Context, config restserver.Config) error {
	return run(ctx, config, runtimeOptions{})
}

func run(ctx context.Context, config restserver.Config, options runtimeOptions) error {
	if ctx == nil {
		return errors.New("server context must not be nil")
	}
	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid REST server config: %w", err)
	}
	key, err := restserver.LoadAPIKey(config.APIKeyFile)
	if err != nil {
		return err
	}
	openStore := options.OpenStore
	if openStore == nil {
		openStore = restworker.NewLocalServerJobManager
	}
	store, err := openStore(config)
	if err != nil {
		return err
	}
	publisher := options.Publisher
	if publisher == nil && config.Provider.Backend == "github" {
		token, err := restserver.LoadProviderToken(config.Provider.TokenFile)
		if err != nil {
			closeLocalStore(store)
			return err
		}
		publisher = restprovider.NewGitHubPublisherWithTokenPush(token.Value(), &http.Client{Timeout: 20 * time.Second}, restprovider.GitPushBranchWithToken)
		probeCtx, cancelProbe := context.WithTimeout(ctx, 5*time.Second)
		probeErr := publisher.Ping(probeCtx)
		cancelProbe()
		if probeErr != nil {
			closeLocalStore(store)
			return errors.New("configured GitHub provider is unavailable")
		}
	} else if publisher != nil {
		probeCtx, cancelProbe := context.WithTimeout(ctx, 5*time.Second)
		probeErr := publisher.Ping(probeCtx)
		cancelProbe()
		if probeErr != nil {
			closeLocalStore(store)
			return errors.New("configured GitHub provider is unavailable")
		}
	}
	manager, ok := store.(restjobs.Manager)
	if !ok {
		closeLocalStore(store)
		return errors.New("configured job store does not provide worker lifecycle operations")
	}
	closeStore := func() {
		if closer, ok := store.(interface{ CloseStore() error }); ok {
			_ = closer.CloseStore()
		} else {
			store.Close()
		}
	}
	// Repository validation must complete before any listener is opened.
	if err := config.ValidateRepositoryRoots(); err != nil {
		closeStore()
		return err
	}
	logger := options.Log
	if logger == nil {
		logger = log.Default()
	}
	resultsBase := options.ResultsBase
	if resultsBase == "" {
		resultsBase, err = serverResultsBase()
		if err != nil {
			closeStore()
			return err
		}
	}
	workspaces := make(map[string]restworker.WorkspaceManager, len(config.Repositories))
	workspaceManagers := make(map[string]*restworkspace.Manager, len(config.Repositories))
	for alias, root := range config.Repositories {
		workspace, err := restworkspace.New(restworkspace.Config{
			RepositoryRoot: root,
			ResultsRoot:    filepath.Join(resultsBase, aliasDirectory(alias), "results"),
			RunGit:         options.WorkspaceGit,
			Now:            options.WorkspaceNow,
		})
		if err != nil {
			closeStore()
			return fmt.Errorf("initialize workspace for repository %q: %w", alias, err)
		}
		workspaces[alias] = workspace
		workspaceManagers[alias] = workspace
	}
	workflowConfig := factory.DefaultConfig()
	workflowConfig.AutoPublish = false
	workflowConfig.WorktreeParent = ""
	workflowConfig.PipelineChecks = cloneVerificationChecks(config.VerificationChecks)
	workflowConfig.ParallelImplementation = nil
	executor := options.Executor
	if executor == nil {
		var recordProviderAttempt func(string, restjobs.ProviderAttempt) error
		var markProviderAttemptUncertain func(string) error
		if sqliteStore, ok := store.(*restjobs.SQLiteStore); ok {
			recordProviderAttempt = sqliteStore.RecordProviderAttempt
			markProviderAttemptUncertain = sqliteStore.MarkProviderAttemptUncertain
		}
		executor, err = restworker.NewFactoryExecutorContext(ctx, restworker.FactoryExecutorConfig{
			Server: config, Workflow: workflowConfig, Workspaces: workspaces, Provider: publisher,
			RecordProviderAttempt: recordProviderAttempt, MarkProviderAttemptUncertain: markProviderAttemptUncertain,
			RecordVerificationEvidence: manager.RecordVerificationEvidence,
		})
		if err != nil {
			closeStore()
			return fmt.Errorf("initialize REST workflow executor: %w", err)
		}
	}
	var ready atomic.Bool
	var readyCheck func(context.Context) error
	if probe, ok := store.(interface{ Ping(context.Context) error }); ok {
		readyCheck = probe.Ping
	}
	apiHandler, err := restapi.New(manager, key, restapi.Config{
		MaxRequestBodyBytes: config.Limits.RequestBodyBytes,
		MaxTaskBytes:        config.Limits.TaskBytes,
		RepositoryAliases:   repositoryAliases(config.Repositories),
		ResolveInterrupted: func(requestCtx context.Context, id string, disposition restjobs.InterruptedDisposition) error {
			if err := requestCtx.Err(); err != nil {
				return restjobs.ErrInvalidTransition
			}
			sqliteStore, ok := store.(sqliteInterruptedDispositionStore)
			if !ok {
				return restjobs.ErrInvalidTransition
			}
			if !sqliteStore.RecoveryNeeded(id) {
				return restjobs.ErrInvalidTransition
			}
			return sqliteStore.ResolveInterrupted(id, disposition)
		},
		ReconcileProvider: func(requestCtx context.Context, id string) error {
			sqliteStore, ok := store.(sqliteProviderReconciliationStore)
			if !ok || publisher == nil {
				return restjobs.ErrInvalidTransition
			}
			reconciler, ok := publisher.(restprovider.Reconciler)
			if !ok {
				return restjobs.ErrInvalidTransition
			}
			factoryExecutor, ok := executor.(*restworker.FactoryExecutor)
			if !ok {
				return restjobs.ErrInvalidTransition
			}
			job, getErr := sqliteStore.Get(id)
			if getErr != nil {
				return getErr
			}
			attempt, attemptErr := sqliteStore.ProviderAttempt(id)
			if errors.Is(attemptErr, restjobs.ErrInvalidTransition) && job.Provider != nil {
				attempt = restjobs.ProviderAttempt{Provider: job.Provider.Provider, Repository: job.Provider.Repository, Branch: job.Provider.Branch, Commit: job.Provider.Commit}
				attemptErr = nil
			}
			if errors.Is(attemptErr, restjobs.ErrInvalidTransition) {
				return restjobs.ErrInvalidTransition
			}
			if attemptErr != nil {
				return attemptErr
			}
			if job.Status == restjobs.StatusSucceeded && job.Provider != nil && job.Provider.Provider == attempt.Provider && job.Provider.Repository == attempt.Repository && job.Provider.Branch == attempt.Branch && job.Provider.Commit == attempt.Commit {
				return nil
			}
			if job.Status != restjobs.StatusFailed && (job.Status != restjobs.StatusRunning || !sqliteStore.RecoveryNeeded(id)) {
				return restjobs.ErrInvalidTransition
			}
			outcome, reconcileErr := factoryExecutor.ReconcileInterruptedProvider(requestCtx, job, attempt, reconciler)
			if reconcileErr != nil {
				return restjobs.ErrInvalidTransition
			}
			if err := sqliteStore.ReconcileProviderOutcome(id, outcome); err != nil {
				return restjobs.ErrInvalidTransition
			}
			return nil
		},
		Ready: func() bool { return ready.Load() },
		ReadyError: func(requestCtx context.Context) error {
			if !ready.Load() {
				return errors.New("server not initialized")
			}
			probeCtx, cancel := context.WithTimeout(requestCtx, 2*time.Second)
			defer cancel()
			if readyCheck != nil {
				if err := readyCheck(probeCtx); err != nil {
					return err
				}
			}
			if publisher != nil {
				if err := publisher.Ping(probeCtx); err != nil {
					return err
				}
			}
			return nil
		},
	})
	if err != nil {
		closeStore()
		return err
	}

	startupSweepCtx, cancelStartupSweep := context.WithTimeout(ctx, sweepTimeout)
	for _, workspace := range workspaceManagers {
		if report := workspace.SweepContext(startupSweepCtx); len(report.Errors) > 0 {
			logCleanup(logger, len(report.Errors))
		}
		if startupSweepCtx.Err() != nil {
			break
		}
	}
	cancelStartupSweep()
	sweepCtx, cancelSweep := context.WithCancel(context.Background())
	defer cancelSweep()
	sweepDone := make(chan struct{})
	go sweepWorkspaces(sweepCtx, sweepDone, workspaceManagers, logger, options.SweepInterval)

	coordinator, err := restworker.New(manager, executor, restworker.CoordinatorConfig{Workers: config.Limits.Workers})
	if err != nil {
		closeStore()
		return err
	}
	server := &http.Server{
		Handler:           apiHandler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    maxHeaderBytes,
	}
	listen := options.Listen
	if listen == nil {
		listen = net.Listen
	}
	listener, err := listen("tcp", config.ListenAddress)
	if err != nil {
		shutdownCtx, cancel := newShutdownContext(options.ShutdownTimeout)
		defer cancel()
		cancelSweep()
		_ = waitForSweep(shutdownCtx, sweepDone)
		_ = coordinator.Shutdown(shutdownCtx)
		closeStore()
		return fmt.Errorf("listen for REST server: %w", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	ready.Store(true)
	logger.Printf("REST server ready; persistence backend: %s", config.Persistence.Backend)

	var serveErr error
	select {
	case <-ctx.Done():
	case err := <-serveDone:
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr = fmt.Errorf("serve REST API: %w", err)
		}
	}
	shutdownCtx, cancel := newShutdownContext(options.ShutdownTimeout)
	defer cancel()
	ready.Store(false)
	manager.Close()
	cancelSweep()
	sweepErr := waitForSweep(shutdownCtx, sweepDone)
	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		_ = server.Close()
	}
	workerErr := coordinator.Shutdown(shutdownCtx)
	closeStore()
	if serveErr != nil {
		return serveErr
	}
	if shutdownErr != nil {
		return fmt.Errorf("shut down REST HTTP server: %w", shutdownErr)
	}
	if workerErr != nil {
		return fmt.Errorf("shut down REST workers: %w", workerErr)
	}
	if sweepErr != nil {
		return fmt.Errorf("stop REST workspace sweeper: %w", sweepErr)
	}
	return nil
}

func newShutdownContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = defaultShutdownTimeout
	}
	return context.WithTimeout(context.Background(), timeout)
}

func waitForSweep(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func cloneVerificationChecks(checks [][]string) [][]string {
	cloned := make([][]string, len(checks))
	for i, check := range checks {
		cloned[i] = append([]string(nil), check...)
	}
	return cloned
}

func serverResultsBase() (string, error) {
	stateHome := os.Getenv("XDG_STATE_HOME")
	if stateHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("find state directory: %w", err)
		}
		stateHome = filepath.Join(home, ".local", "state")
	}
	if !filepath.IsAbs(stateHome) {
		return "", errors.New("XDG_STATE_HOME must be absolute")
	}
	return filepath.Join(stateHome, "factory", "rest-server"), nil
}

func aliasDirectory(alias string) string {
	digest := sha256.Sum256([]byte(alias))
	return hex.EncodeToString(digest[:])
}

func repositoryAliases(repositories map[string]string) map[string]struct{} {
	aliases := make(map[string]struct{}, len(repositories))
	for alias := range repositories {
		aliases[alias] = struct{}{}
	}
	return aliases
}

func sweepWorkspaces(ctx context.Context, done chan<- struct{}, managers map[string]*restworkspace.Manager, logger *log.Logger, interval time.Duration) {
	defer close(done)
	if interval <= 0 {
		interval = sweepInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweepCtx, cancel := context.WithTimeout(ctx, sweepTimeout)
			for _, manager := range managers {
				if report := manager.SweepContext(sweepCtx); len(report.Errors) > 0 {
					logCleanup(logger, len(report.Errors))
				}
				if sweepCtx.Err() != nil {
					break
				}
			}
			cancel()
		}
	}
}

func logCleanup(logger *log.Logger, count int) {
	if logger != nil {
		logger.Printf("REST workspace cleanup retained or skipped %d item(s)", count)
	}
}
