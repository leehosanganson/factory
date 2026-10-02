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
	"sync"
	"sync/atomic"
	"time"

	"github.com/leehosanganson/factory/internal/factory"
	"github.com/leehosanganson/factory/internal/restapi"
	"github.com/leehosanganson/factory/internal/restserver"
	"github.com/leehosanganson/factory/internal/restworker"
	"github.com/leehosanganson/factory/internal/restworkspace"
)

const (
	defaultShutdownTimeout = 15 * time.Second
	sweepInterval          = time.Hour
	maxHeaderBytes         = 1 << 20
)

// runtimeOptions contains injectable seams for runtime lifecycle tests.
type runtimeOptions struct {
	Listen          func(string, string) (net.Listener, error)
	Executor        restworker.Executor
	ResultsBase     string
	ShutdownTimeout time.Duration
	Log             *log.Logger
}

// Run starts the REST server and blocks until ctx is canceled or serving fails.
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
	// Repository validation must complete before any listener is opened.
	if err := config.ValidateRepositoryRoots(); err != nil {
		return err
	}
	key, err := restserver.LoadAPIKey(config.APIKeyFile)
	if err != nil {
		return err
	}
	manager, err := restworker.NewLocalJobManager(config)
	if err != nil {
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
			return err
		}
	}
	workspaces := make(map[string]restworker.WorkspaceManager, len(config.Repositories))
	workspaceManagers := make(map[string]*restworkspace.Manager, len(config.Repositories))
	for alias, root := range config.Repositories {
		workspace, err := restworkspace.New(restworkspace.Config{
			RepositoryRoot: root,
			ResultsRoot:    filepath.Join(resultsBase, aliasDirectory(alias), "results"),
		})
		if err != nil {
			return fmt.Errorf("initialize workspace for repository %q: %w", alias, err)
		}
		workspaces[alias] = workspace
		workspaceManagers[alias] = workspace
	}
	workflowConfig := factory.DefaultConfig()
	workflowConfig.AutoPublish = false
	workflowConfig.WorktreeParent = ""
	workflowConfig.PipelineChecks = nil
	workflowConfig.ParallelImplementation = nil
	executor := options.Executor
	if executor == nil {
		executor, err = restworker.NewFactoryExecutorContext(ctx, restworker.FactoryExecutorConfig{
			Server: config, Workflow: workflowConfig, Workspaces: workspaces,
		})
		if err != nil {
			return fmt.Errorf("initialize REST workflow executor: %w", err)
		}
	}
	var ready atomic.Bool
	apiHandler, err := restapi.New(manager, key, restapi.Config{
		MaxRequestBodyBytes: config.Limits.RequestBodyBytes,
		MaxTaskBytes:        config.Limits.TaskBytes,
		RepositoryAliases:   repositoryAliases(config.Repositories),
		Ready:               ready.Load,
	})
	if err != nil {
		return err
	}

	for _, workspace := range workspaceManagers {
		if report := workspace.Sweep(); len(report.Errors) > 0 {
			logCleanup(logger, len(report.Errors))
		}
	}
	stopSweep := make(chan struct{})
	sweepDone := make(chan struct{})
	go sweepWorkspaces(stopSweep, sweepDone, workspaceManagers, logger)
	var stopSweepOnce sync.Once
	stopSweeper := func() {
		stopSweepOnce.Do(func() {
			close(stopSweep)
			<-sweepDone
		})
	}
	defer stopSweeper()

	coordinator, err := restworker.New(manager, executor, restworker.CoordinatorConfig{Workers: config.Limits.Workers})
	if err != nil {
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
		shutdownWorkers(coordinator, options.ShutdownTimeout)
		return fmt.Errorf("listen for REST server: %w", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	ready.Store(true)

	var serveErr error
	select {
	case <-ctx.Done():
	case err := <-serveDone:
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr = fmt.Errorf("serve REST API: %w", err)
		}
	}
	ready.Store(false)
	manager.Close()
	stopSweeper()
	shutdownTimeout := options.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = defaultShutdownTimeout
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		_ = server.Close()
	}
	workerErr := coordinator.Shutdown(shutdownCtx)
	if serveErr != nil {
		return serveErr
	}
	if shutdownErr != nil {
		return fmt.Errorf("shut down REST HTTP server: %w", shutdownErr)
	}
	if workerErr != nil {
		return fmt.Errorf("shut down REST workers: %w", workerErr)
	}
	return nil
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

func sweepWorkspaces(stop <-chan struct{}, done chan<- struct{}, managers map[string]*restworkspace.Manager, logger *log.Logger) {
	defer close(done)
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			for _, manager := range managers {
				if report := manager.Sweep(); len(report.Errors) > 0 {
					logCleanup(logger, len(report.Errors))
				}
			}
		}
	}
}

func logCleanup(logger *log.Logger, count int) {
	if logger != nil {
		logger.Printf("REST workspace cleanup retained or skipped %d item(s)", count)
	}
}

func shutdownWorkers(coordinator *restworker.Coordinator, timeout time.Duration) {
	if timeout <= 0 {
		timeout = defaultShutdownTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = coordinator.Shutdown(ctx)
}
