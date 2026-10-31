package runtime

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/leehosanganson/factory/internal/factory"
	"github.com/leehosanganson/factory/internal/restapi"
	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restprovider"
	"github.com/leehosanganson/factory/internal/restserver"
	"github.com/leehosanganson/factory/internal/restweb"
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
	ListenWorker    func(string, string, *tls.Config) (net.Listener, error)
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

type providerReconciliationStore interface {
	Get(string) (restjobs.Snapshot, error)
	ProviderAttempt(string) (restjobs.ProviderAttempt, error)
	ReconcileProviderOutcome(string, restjobs.ProviderOutcome) error
	RecoveryNeeded(string) bool
}

type interruptedDispositionStore interface {
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
	if err := config.ValidateServerAuth(); err != nil {
		return fmt.Errorf("invalid REST server config: %w", err)
	}
	principals, err := restserver.LoadPrincipalResolver(config)
	if err != nil {
		return err
	}
	key := principals.APIKey()
	openStore := options.OpenStore
	if openStore == nil {
		openStore = restworker.NewLocalServerJobManager
	}
	store, err := openStore(config)
	if err != nil {
		return err
	}
	workersEnabled := config.WorkersEnabledValue()
	workerID := config.WorkerID
	if workerID == "" {
		workerID = fmt.Sprintf("factory-%d", os.Getpid())
	}
	bootID := processBootID()
	if workersEnabled {
		if identityStore, ok := store.(interface{ SetWorkerIdentity(string, string) error }); ok {
			if err := identityStore.SetWorkerIdentity(workerID, bootID); err != nil {
				closeLocalStore(store)
				return err
			}
		}
	}
	publisher := options.Publisher
	if publisher == nil && config.Provider.Backend == "github" && workersEnabled {
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
	} else if publisher != nil && workersEnabled {
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
	if workersEnabled {
		if err := config.ValidateRepositoryRoots(); err != nil {
			closeStore()
			return err
		}
	}
	logger := options.Log
	if logger == nil {
		logger = log.Default()
	}
	resultsBase := options.ResultsBase
	if workersEnabled && resultsBase == "" {
		resultsBase, err = serverResultsBase()
		if err != nil {
			closeStore()
			return err
		}
	}
	workspaces := make(map[string]restworker.WorkspaceManager, len(config.Repositories))
	workspaceManagers := make(map[string]*restworkspace.Manager, len(config.Repositories))
	if workersEnabled {
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
	}
	workflowConfig := factory.DefaultConfig()
	workflowConfig.AutoPublish = false
	workflowConfig.WorktreeParent = ""
	workflowConfig.PipelineChecks = cloneVerificationChecks(config.VerificationChecks)
	if config.ParallelSubtasks != nil && config.ParallelSubtasks.Enabled {
		maxConcurrency := 4
		if config.ParallelSubtasks.MaxConcurrency != nil {
			maxConcurrency = *config.ParallelSubtasks.MaxConcurrency
		}
		workflowConfig.ParallelImplementation = &factory.ParallelImplementationConfig{Enabled: true, MaxConcurrency: maxConcurrency}
	}
	executor := options.Executor
	if workersEnabled && executor == nil {
		executor, err = restworker.NewFactoryExecutorContext(ctx, restworker.FactoryExecutorConfig{
			Server: config, Workflow: workflowConfig, Workspaces: workspaces, Provider: publisher,
			RecordProviderAttempt:        manager.RecordProviderAttemptForAttempt,
			MarkProviderAttemptUncertain: manager.MarkProviderAttemptUncertainForAttempt,
			RecordVerificationEvidence:   manager.RecordVerificationEvidenceForAttempt,
			RecordProgress:               manager.RecordProgressForAttempt,
		})
		if err != nil {
			closeStore()
			return fmt.Errorf("initialize REST workflow executor: %w", err)
		}
	}
	var ready atomic.Bool
	var readyCheck func(context.Context) error
	var coordinator *restworker.Coordinator
	if probe, ok := store.(interface{ Ping(context.Context) error }); ok {
		readyCheck = probe.Ping
	}
	var reconcileProvider func(context.Context, string) error
	if workersEnabled {
		reconcileProvider = func(requestCtx context.Context, id string) error {
			recoveryStore, ok := store.(providerReconciliationStore)
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
			job, getErr := recoveryStore.Get(id)
			if getErr != nil {
				return getErr
			}
			attempt, attemptErr := recoveryStore.ProviderAttempt(id)
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
			if job.Status != restjobs.StatusFailed && (job.Status != restjobs.StatusRunning || !recoveryStore.RecoveryNeeded(id)) {
				return restjobs.ErrInvalidTransition
			}
			outcome, reconcileErr := factoryExecutor.ReconcileInterruptedProvider(requestCtx, job, attempt, reconciler)
			if reconcileErr != nil {
				return restjobs.ErrInvalidTransition
			}
			if err := recoveryStore.ReconcileProviderOutcome(id, outcome); err != nil {
				return restjobs.ErrInvalidTransition
			}
			return nil
		}
	}
	dashboardOperatorID := ""
	if config.WebUI.Enabled {
		dashboardOperatorID = config.WebUI.OperatorID
	}
	var webhook restapi.GitHubWebhookConfig
	if config.GitHubWebhook.Enabled() {
		if _, ok := store.(restjobs.GitHubIssueWebhookStore); !ok {
			closeStore()
			return errors.New("configured job store does not support GitHub webhook admission")
		}
		secret, secretErr := restserver.LoadWebhookSecret(config.GitHubWebhook.SecretFile)
		if secretErr != nil {
			closeStore()
			return errors.New("configured GitHub webhook secret is unavailable")
		}
		webhook = restapi.GitHubWebhookConfig{
			Secret: secret.Value(), TriggerLabel: config.GitHubWebhook.TriggerLabel,
			Sources: cloneStringMap(config.GitHubWebhook.Sources), Owners: cloneStringMap(config.GitHubWebhook.Owners),
		}
	}
	var workerHandler http.Handler
	var workerTLS *tls.Config
	if config.WorkerListener != nil {
		sessionStore, ok := store.(restjobs.WorkerSessionStore)
		if !ok {
			closeStore()
			return errors.New("configured SQL store does not support worker sessions")
		}
		workerHandler, err = restserver.NewWorkerListenerHandler(*config.WorkerListener, sessionStore, func() bool {
			if !ready.Load() {
				return false
			}
			if readyCheck == nil {
				return true
			}
			probeCtx, cancelProbe := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelProbe()
			return readyCheck(probeCtx) == nil
		}, logger)
		if err != nil {
			closeStore()
			return err
		}
		workerTLS, err = restserver.NewWorkerListenerTLS(*config.WorkerListener)
		if err != nil {
			closeStore()
			return err
		}
	}
	apiHandler, err := restapi.New(manager, key, restapi.Config{
		MaxRequestBodyBytes: config.Limits.RequestBodyBytes,
		MaxTaskBytes:        config.Limits.TaskBytes,
		RepositoryAliases:   repositoryAliases(config.Repositories),
		DashboardOperatorID: dashboardOperatorID,
		GitHubWebhook:       webhook,
		CancelJob: func(requestCtx context.Context, id string) (restjobs.Snapshot, error) {
			if err := requestCtx.Err(); err != nil {
				return restjobs.Snapshot{}, restjobs.ErrInvalidTransition
			}
			if coordinator == nil {
				return manager.Cancel(id)
			}
			return coordinator.RequestCancel(id)
		},
		ResolveInterrupted: func(requestCtx context.Context, id string, disposition restjobs.InterruptedDisposition) error {
			if err := requestCtx.Err(); err != nil {
				return restjobs.ErrInvalidTransition
			}
			recoveryStore, ok := store.(interruptedDispositionStore)
			if !ok {
				return restjobs.ErrInvalidTransition
			}
			if !recoveryStore.RecoveryNeeded(id) {
				return restjobs.ErrInvalidTransition
			}
			return recoveryStore.ResolveInterrupted(id, disposition)
		},
		ReconcileProvider: reconcileProvider,
		Ready:             func() bool { return ready.Load() },
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

	if workersEnabled {
		coordinator, err = restworker.New(manager, executor, restworker.CoordinatorConfig{Workers: config.Limits.Workers, WorkerID: workerID, BootID: bootID})
		if err != nil {
			closeStore()
			return err
		}
	}
	httpHandler := http.Handler(apiHandler)
	if config.WebUI.Enabled {
		mux := http.NewServeMux()
		mux.Handle("/healthz", apiHandler)
		mux.Handle("/readyz", apiHandler)
		mux.Handle("/v1/", apiHandler)
		mux.Handle("/", restweb.New(apiHandler, config.WebUI.OperatorID))
		httpHandler = mux
	}
	server := &http.Server{
		Handler:           httpHandler,
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
		if coordinator != nil {
			_ = coordinator.Shutdown(shutdownCtx)
		}
		closeStore()
		if errors.Is(err, syscall.EADDRINUSE) {
			return errors.New("listen for REST server: address already in use")
		}
		return errors.New("listen for REST server")
	}
	var workerListener net.Listener
	var workerServer *http.Server
	if workerHandler != nil {
		listenWorker := options.ListenWorker
		if listenWorker == nil {
			listenWorker = func(network, address string, tlsConfig *tls.Config) (net.Listener, error) {
				listener, err := net.Listen(network, address)
				if err != nil {
					return nil, err
				}
				return tls.NewListener(listener, tlsConfig), nil
			}
		}
		workerListener, err = listenWorker("tcp", config.WorkerListener.ListenAddress, workerTLS)
		if err != nil {
			_ = listener.Close()
			cancelSweep()
			shutdownCtx, cancel := newShutdownContext(options.ShutdownTimeout)
			defer cancel()
			_ = waitForSweep(shutdownCtx, sweepDone)
			if coordinator != nil {
				_ = coordinator.Shutdown(shutdownCtx)
			}
			closeStore()
			return errors.New("listen for worker identity API")
		}
		workerServer = &http.Server{Handler: workerHandler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: maxHeaderBytes}
	}
	serveDone := make(chan error, 2)
	go func() { serveDone <- server.Serve(listener) }()
	if workerServer != nil {
		go func() { serveDone <- workerServer.Serve(workerListener) }()
	}
	ready.Store(true)
	logger.Printf("REST server ready; persistence backend: %s", config.Persistence.Backend)

	var serveErr error
	select {
	case <-ctx.Done():
	case err := <-serveDone:
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr = errors.New("serve REST listener")
		}
	}
	shutdownCtx, cancel := newShutdownContext(options.ShutdownTimeout)
	defer cancel()
	ready.Store(false)
	if workersEnabled {
		manager.Close()
	}
	cancelSweep()
	sweepErr := waitForSweep(shutdownCtx, sweepDone)
	var httpServers []*http.Server
	httpServers = append(httpServers, server)
	if workerServer != nil {
		httpServers = append(httpServers, workerServer)
	}
	httpShutdownErrors := shutdownHTTPServers(shutdownCtx, httpServers...)
	var shutdownErr, workerShutdownErr error
	if len(httpShutdownErrors) > 0 {
		shutdownErr = httpShutdownErrors[0]
	}
	if len(httpShutdownErrors) > 1 {
		workerShutdownErr = httpShutdownErrors[1]
	}
	var workerErr error
	if coordinator != nil {
		workerErr = coordinator.Shutdown(shutdownCtx)
	}
	closeStore()
	if serveErr != nil {
		return serveErr
	}
	if shutdownErr != nil {
		return fmt.Errorf("shut down REST HTTP server: %w", shutdownErr)
	}
	if workerShutdownErr != nil {
		return fmt.Errorf("shut down worker identity API: %w", workerShutdownErr)
	}
	if workerErr != nil {
		return fmt.Errorf("shut down REST workers: %w", workerErr)
	}
	if sweepErr != nil {
		return fmt.Errorf("stop REST workspace sweeper: %w", sweepErr)
	}
	return nil
}

func cloneStringMap(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func processBootID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("boot-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value[:])
}

func principalsConfigured(config restserver.Config) bool {
	if len(config.Principals) == 0 {
		return false
	}
	for _, principal := range config.Principals {
		if principal.Enabled && principal.Role == "operator" {
			return true
		}
	}
	return false
}

func shutdownHTTPServers(ctx context.Context, servers ...*http.Server) []error {
	errorsByServer := make([]error, len(servers))
	var wg sync.WaitGroup
	for i, server := range servers {
		wg.Add(1)
		go func(i int, server *http.Server) {
			defer wg.Done()
			errorsByServer[i] = server.Shutdown(ctx)
			if errorsByServer[i] != nil {
				_ = server.Close()
			}
		}(i, server)
	}
	wg.Wait()
	return errorsByServer
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
