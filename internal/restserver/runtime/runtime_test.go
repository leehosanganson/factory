package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restprovider"
	"github.com/leehosanganson/factory/internal/restserver"
	"github.com/leehosanganson/factory/internal/restworker"
	"github.com/leehosanganson/factory/internal/restworkspace"
)

type runtimeExecutor func(context.Context, restjobs.Snapshot) error

func (f runtimeExecutor) Execute(ctx context.Context, request restjobs.Snapshot) error {
	return f(ctx, request)
}

type readinessStore struct {
	restjobs.Store
	up atomic.Bool
}

type closeTrackingStore struct {
	restjobs.Store
	closed bool
}

func (s *closeTrackingStore) CloseStore() error {
	s.closed = true
	if closer, ok := s.Store.(interface{ CloseStore() error }); ok {
		return closer.CloseStore()
	}
	s.Store.Close()
	return nil
}

func (s *readinessStore) Ping(ctx context.Context) error {
	if !s.up.Load() {
		return errors.New("store unavailable")
	}
	return s.Store.(interface{ Ping(context.Context) error }).Ping(ctx)
}

type readinessPublisher struct {
	checks atomic.Int32
	up     atomic.Bool
}

func (p *readinessPublisher) Ping(context.Context) error {
	p.checks.Add(1)
	if !p.up.Load() {
		return errors.New("provider unavailable")
	}
	return nil
}

func (p *readinessPublisher) Publish(context.Context, restprovider.PublishRequest) (restprovider.Outcome, error) {
	return restprovider.Outcome{}, errors.New("not used in readiness test")
}

func TestRunWorkersDisabledListenerFailureClosesStoreAndSanitizesError(t *testing.T) {
	for _, backend := range []string{restserver.PersistenceBackendSQLite, restserver.PersistenceBackendPostgres} {
		t.Run(backend, func(t *testing.T) {
			config, _ := runtimeFixture(t)
			disabled := false
			config.WorkersEnabled = &disabled
			if backend == restserver.PersistenceBackendSQLite {
				stateDir := filepath.Join(t.TempDir(), "private-state")
				if err := os.Mkdir(stateDir, 0o700); err != nil {
					t.Fatal(err)
				}
				config.Persistence = restserver.PersistenceConfig{Backend: backend, Path: filepath.Join(stateDir, "jobs.db")}
			} else {
				config.Persistence = restserver.PersistenceConfig{Backend: backend, DSN: "postgres://factory:private-password@db.example/factory"}
			}
			var underlying restjobs.Store
			var err error
			if backend == restserver.PersistenceBackendSQLite {
				underlying, err = restworker.NewLocalServerJobManager(config)
			} else {
				underlying, err = restjobs.NewManager(restjobs.Config{
					QueueCapacity: config.Limits.QueueCapacity, MaxConcurrentJobs: config.Limits.Workers,
					MaxRecords: config.Limits.MaxRecords, MaxEventsPerJob: config.Limits.MaxEventsPerJob,
					MaxTaskBytes: config.Limits.TaskBytes, RegistryBytes: config.Limits.RegistryBytes,
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			store := &closeTrackingStore{Store: underlying}
			const privateListenError = "bind 127.0.0.1:8080: private-listener-detail"
			gotErr := run(context.Background(), config, runtimeOptions{
				OpenStore: func(restserver.Config) (restjobs.Store, error) { return store, nil },
				Listen:    func(string, string) (net.Listener, error) { return nil, errors.New(privateListenError) },
			})
			if gotErr == nil || gotErr.Error() != "listen for REST server" {
				t.Fatalf("listener failure = %v, want generic sanitized listen error", gotErr)
			}
			if strings.Contains(gotErr.Error(), "private") {
				t.Fatalf("listener error leaked details: %v", gotErr)
			}
			if !store.closed {
				t.Fatal("configured store was not closed after listener failure")
			}
		})
	}
}

func TestRunListenerAddressInUseUsesFixedDiagnosticAndClosesStore(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()

	config, _ := runtimeFixture(t)
	underlying, err := restjobs.NewManager(restjobs.Config{
		QueueCapacity: config.Limits.QueueCapacity, MaxConcurrentJobs: config.Limits.Workers,
		MaxRecords: config.Limits.MaxRecords, MaxEventsPerJob: config.Limits.MaxEventsPerJob,
		MaxTaskBytes: config.Limits.TaskBytes, RegistryBytes: config.Limits.RegistryBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &closeTrackingStore{Store: underlying}
	gotErr := run(context.Background(), config, runtimeOptions{
		OpenStore: func(restserver.Config) (restjobs.Store, error) { return store, nil },
		Listen: func(network, _ string) (net.Listener, error) {
			return net.Listen(network, occupied.Addr().String())
		},
	})
	if gotErr == nil || gotErr.Error() != "listen for REST server: address already in use" {
		t.Fatalf("listener failure = %v, want fixed address-in-use diagnostic", gotErr)
	}
	for _, address := range []string{occupied.Addr().String(), config.ListenAddress} {
		if strings.Contains(gotErr.Error(), address) {
			t.Fatalf("listener diagnostic disclosed address %q: %v", address, gotErr)
		}
	}
	if !store.closed {
		t.Fatal("configured store was not closed after listener failure")
	}
}

func TestRunWithWorkersDisabledDoesNotExecuteOrLoadProvider(t *testing.T) {
	config, key := runtimeFixture(t)
	disabled := false
	config.WorkersEnabled = &disabled
	config.Provider = restserver.ProviderConfig{Backend: "github", TokenFile: "/missing/provider-token", BaseBranch: "main", Repositories: map[string]string{"trusted": "acme/widget"}}
	listenerReady := make(chan net.Listener, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, config, runtimeOptions{Listen: func(network, _ string) (net.Listener, error) {
			l, err := net.Listen(network, "127.0.0.1:0")
			if err == nil {
				listenerReady <- l
			}
			return l, err
		}})
	}()
	listener := <-listenerReady
	waitForStatus(t, "http://"+listener.Addr().String()+"/readyz", http.StatusOK)
	request, _ := http.NewRequest(http.MethodPost, "http://"+listener.Addr().String()+"/v1/jobs", strings.NewReader(`{"repository":"trusted","task":"must remain queued"}`))
	request.Header.Set("Authorization", "Bearer "+key)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "workers-disabled")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var admitted struct {
		Job restjobs.Snapshot `json:"job"`
	}
	decodeErr := json.NewDecoder(response.Body).Decode(&admitted)
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted || decodeErr != nil {
		t.Fatalf("admission status=%d err=%v", response.StatusCode, decodeErr)
	}
	reconcile, err := http.NewRequest(http.MethodPost, "http://"+listener.Addr().String()+"/v1/jobs/"+admitted.Job.ID+"/reconcile", nil)
	if err != nil {
		t.Fatal(err)
	}
	reconcile.Header.Set("Authorization", "Bearer "+key)
	reconcileResponse, err := http.DefaultClient.Do(reconcile)
	if err != nil {
		t.Fatal(err)
	}
	reconcileBody, readErr := io.ReadAll(reconcileResponse.Body)
	reconcileResponse.Body.Close()
	if readErr != nil || reconcileResponse.StatusCode != http.StatusConflict || !strings.Contains(string(reconcileBody), `"code":"reconciliation_unavailable"`) {
		t.Fatalf("workers-disabled reconciliation status=%d body=%s err=%v", reconcileResponse.StatusCode, reconcileBody, readErr)
	}
	time.Sleep(100 * time.Millisecond)
	get, err := http.NewRequest(http.MethodGet, "http://"+listener.Addr().String()+"/v1/jobs/"+admitted.Job.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	get.Header.Set("Authorization", "Bearer "+key)
	response, err = http.DefaultClient.Do(get)
	if err != nil {
		t.Fatal(err)
	}
	var current restjobs.Snapshot
	decodeErr = json.NewDecoder(response.Body).Decode(&current)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || decodeErr != nil || current.Status != restjobs.StatusQueued {
		t.Fatalf("workers-disabled job=(%+v status=%d decode=%v)", current, response.StatusCode, decodeErr)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRunDashboardRoutesAreIntegratedWithoutChangingAPIListener(t *testing.T) {
	config, _ := runtimeFixture(t)
	config.WebUI = restserver.WebUIConfig{Enabled: true, OperatorID: "runtime-operator"}
	listenerReady := make(chan net.Listener, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, config, runtimeOptions{Listen: func(network, _ string) (net.Listener, error) {
			listener, err := net.Listen(network, "127.0.0.1:0")
			if err == nil {
				listenerReady <- listener
			}
			return listener, err
		}})
	}()
	var listener net.Listener
	select {
	case listener = <-listenerReady:
	case err := <-done:
		t.Fatalf("server exited before listener startup: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not start a listener")
	}
	defer listener.Close()
	baseURL := "http://" + listener.Addr().String()
	for path, wantType := range map[string]string{"/": "text/html", "/ui/assets/dashboard.js": "text/javascript", "/healthz": "application/json"} {
		response, err := http.Get(baseURL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || !strings.Contains(response.Header.Get("Content-Type"), wantType) {
			t.Errorf("GET %s status=%d content-type=%q", path, response.StatusCode, response.Header.Get("Content-Type"))
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func TestRunNamedPrincipalAndLegacyBearerCredentialsAuthenticate(t *testing.T) {
	config, legacyToken := runtimeFixture(t)
	resolver, err := restserver.LoadPrincipalResolver(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{legacyToken, "runtime-named-operator-token"} {
		if !resolver.APIKey().Authenticate(token) {
			t.Errorf("runtime resolver rejected configured credential")
		}
	}
	if resolver.APIKey().Authenticate("unconfigured-runtime-token") {
		t.Fatal("runtime resolver accepted unconfigured credential")
	}
	if principal, ok := resolver.Resolve("runtime-named-operator-token"); !ok || principal.ID != "runtime-operator" || principal.Role != "operator" {
		t.Fatalf("runtime named credential resolved to %+v, %v", principal, ok)
	}
}

func TestRunReadinessRecoversAfterProviderOutageAndBlocksAdmission(t *testing.T) {
	config, key := runtimeFixture(t)
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(t.TempDir(), "jobs.db")}
	jobConfig := restjobs.Config{
		QueueCapacity: config.Limits.QueueCapacity, MaxConcurrentJobs: config.Limits.Workers,
		MaxRecords: config.Limits.MaxRecords, MaxEventsPerJob: config.Limits.MaxEventsPerJob,
		MaxTaskBytes: config.Limits.TaskBytes, RegistryBytes: config.Limits.RegistryBytes,
	}
	stateDir := filepath.Join(t.TempDir(), "private-state")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	jobStore, err := restjobs.OpenSQLiteStore(filepath.Join(stateDir, "jobs.db"), jobConfig)
	if err != nil {
		t.Fatal(err)
	}
	store := &readinessStore{Store: jobStore}
	store.up.Store(true)
	publisher := &readinessPublisher{}
	publisher.up.Store(true)
	var executions atomic.Int32
	listenerReady := make(chan net.Listener, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, config, runtimeOptions{
			OpenStore: func(restserver.Config) (restjobs.Store, error) { return store, nil },
			Publisher: publisher,
			Executor: runtimeExecutor(func(context.Context, restjobs.Snapshot) error {
				executions.Add(1)
				return nil
			}),
			Listen: func(network, _ string) (net.Listener, error) {
				listener, err := net.Listen(network, "127.0.0.1:0")
				if err == nil {
					listenerReady <- listener
				}
				return listener, err
			},
		})
	}()
	var listener net.Listener
	select {
	case listener = <-listenerReady:
	case err := <-done:
		t.Fatalf("server exited before listener startup: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("server did not start a listener")
	}
	baseURL := "http://" + listener.Addr().String()
	client := &http.Client{Timeout: 2 * time.Second}
	getStatus := func(path string) int {
		t.Helper()
		response, err := client.Get(baseURL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer response.Body.Close()
		return response.StatusCode
	}
	submitStatus := func(token, idempotencyKey string) int {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs", strings.NewReader(`{"repository":"trusted","task":"must not queue"}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", idempotencyKey)
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode
	}
	if got := getStatus("/healthz"); got != http.StatusOK {
		t.Fatalf("health status=%d, want %d", got, http.StatusOK)
	}
	if got := getStatus("/readyz"); got != http.StatusOK {
		t.Fatalf("initial readiness=%d, want %d", got, http.StatusOK)
	}
	store.up.Store(false)
	if got := getStatus("/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("readiness during store outage=%d, want %d", got, http.StatusServiceUnavailable)
	}
	if got := submitStatus("runtime-named-operator-token", "named-outage-request"); got != http.StatusServiceUnavailable {
		t.Fatalf("named operator admission during store outage=%d, want %d", got, http.StatusServiceUnavailable)
	}

	if got := submitStatus(key, "legacy-outage-request"); got != http.StatusServiceUnavailable {
		t.Fatalf("legacy admission during store outage=%d, want %d", got, http.StatusServiceUnavailable)
	}
	store.up.Store(true)
	if got := getStatus("/readyz"); got != http.StatusOK {
		t.Fatalf("readiness after store recovery=%d, want %d", got, http.StatusOK)
	}
	publisher.up.Store(false)
	if got := getStatus("/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("readiness during provider outage=%d, want %d", got, http.StatusServiceUnavailable)
	}
	if got := submitStatus(key, "provider-outage-request"); got != http.StatusServiceUnavailable {
		t.Fatalf("admission during provider outage=%d, want %d", got, http.StatusServiceUnavailable)
	}
	publisher.up.Store(true)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && getStatus("/readyz") != http.StatusOK {
		time.Sleep(10 * time.Millisecond)
	}
	if got := getStatus("/readyz"); got != http.StatusOK {
		t.Fatalf("readiness after provider recovery=%d, want %d; probes=%d", got, http.StatusOK, publisher.checks.Load())
	}
	if got := submitStatus("unconfigured-runtime-token", "unknown-request"); got != http.StatusUnauthorized {
		t.Fatalf("unconfigured bearer status=%d, want %d", got, http.StatusUnauthorized)
	}
	if got := submitStatus("runtime-named-operator-token", "recovered-request"); got != http.StatusAccepted {
		t.Fatalf("named operator admission after provider recovery=%d, want %d", got, http.StatusAccepted)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && executions.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if executions.Load() != 1 {
		t.Fatalf("job execution count=%d, want one legacy-key admission after recovery", executions.Load())
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server shutdown error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func TestRunMemoryModeRejectsInterruptedDispositionEndpoint(t *testing.T) {
	config, key := runtimeFixture(t)
	listenerReady := make(chan net.Listener, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, config, runtimeOptions{
			ResultsBase: testResultsBase(t),
			Listen: func(network, _ string) (net.Listener, error) {
				listener, err := net.Listen(network, "127.0.0.1:0")
				if err == nil {
					listenerReady <- listener
				}
				return listener, err
			},
		})
	}()
	listener := <-listenerReady
	baseURL := "http://" + listener.Addr().String()
	waitForStatus(t, baseURL+"/readyz", http.StatusOK)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs/00000000-0000-4000-8000-000000000000/disposition/failed", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusConflict || !strings.Contains(string(body), `"reconciliation_not_allowed"`) {
		t.Fatalf("memory-mode disposition status=%d body=%s err=%v", response.StatusCode, body, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func TestRunLogsSelectedPersistenceAfterListenerReadiness(t *testing.T) {
	for _, test := range []struct {
		name    string
		backend string
	}{
		{name: "memory defaults", backend: restserver.PersistenceBackendMemory},
		{name: "selected SQLite", backend: restserver.PersistenceBackendSQLite},
		{name: "selected PostgreSQL", backend: restserver.PersistenceBackendPostgres},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, key := runtimeFixture(t)
			providerTokenPath := filepath.Join(t.TempDir(), "private-provider-token-path")
			if err := os.WriteFile(providerTokenPath, []byte("private-provider-token-secret"), 0o600); err != nil {
				t.Fatal(err)
			}
			if test.backend == restserver.PersistenceBackendSQLite {
				config.Persistence = restserver.PersistenceConfig{
					Backend: test.backend,
					Path:    filepath.Join(t.TempDir(), "private-sqlite-path", "jobs.db"),
				}
				config.Provider = restserver.ProviderConfig{
					Backend: "github", TokenFile: providerTokenPath,
					BaseBranch: "main", Repositories: map[string]string{"trusted": "owner/private-config-repository"},
				}
				if err := os.MkdirAll(filepath.Dir(config.Persistence.Path), 0o700); err != nil {
					t.Fatal(err)
				}
			} else if test.backend == restserver.PersistenceBackendPostgres {
				config.Persistence = restserver.PersistenceConfig{
					Backend: test.backend,
					DSN:     "postgres://factory:private-dsn-secret@localhost/factory?sslmode=disable",
				}
			}
			resultsBase := testResultsBase(t)
			var output bytes.Buffer
			listenerReady := make(chan net.Listener, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- run(ctx, config, runtimeOptions{
					Log: log.New(&output, "", 0),
					OpenStore: func(serverConfig restserver.Config) (restjobs.Store, error) {
						if test.backend != restserver.PersistenceBackendPostgres {
							return restworker.NewLocalServerJobManager(serverConfig)
						}
						return restjobs.NewManager(restjobs.Config{
							QueueCapacity: serverConfig.Limits.QueueCapacity, MaxConcurrentJobs: serverConfig.Limits.Workers,
							MaxRecords: serverConfig.Limits.MaxRecords, MaxEventsPerJob: serverConfig.Limits.MaxEventsPerJob,
							MaxTaskBytes: serverConfig.Limits.TaskBytes, RegistryBytes: serverConfig.Limits.RegistryBytes,
						})
					},
					Publisher:   readyPublisherForRuntimeTest(),
					ResultsBase: resultsBase,
					Listen: func(network, _ string) (net.Listener, error) {
						listener, err := net.Listen(network, "127.0.0.1:0")
						if err == nil {
							listenerReady <- listener
						}
						return listener, err
					},
				})
			}()
			var listener net.Listener
			select {
			case listener = <-listenerReady:
			case err := <-done:
				t.Fatalf("server exited before listener startup: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("server did not start a listener")
			}
			response, err := http.Get("http://" + listener.Addr().String() + "/healthz")
			if err != nil {
				t.Fatalf("GET /healthz: %v", err)
			}
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusOK || string(body) != "{\"status\":\"ok\"}\n" {
				t.Fatalf("health response = %d %q", response.StatusCode, body)
			}
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("server shutdown error: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("server did not shut down")
			}

			want := "REST server ready; persistence backend: " + test.backend + "\n"
			if got := output.String(); got != want {
				t.Fatalf("startup log = %q, want exactly %q", got, want)
			}
			for _, secret := range []string{key, "private-provider-token-secret", "private-dsn-secret", config.Persistence.DSN, config.Persistence.Path, providerTokenPath, "owner/private-config-repository", config.APIKeyFile, config.Harness.Executable, config.Repositories["trusted"], config.ListenAddress} {
				if secret != "" && strings.Contains(output.String(), secret) {
					t.Errorf("startup log disclosed %q: %q", secret, output.String())
				}
			}
		})
	}
}

func readyPublisherForRuntimeTest() *readinessPublisher {
	publisher := &readinessPublisher{}
	publisher.up.Store(true)
	return publisher
}

func TestRunAuthenticatedSubmissionLifecycleAndShutdown(t *testing.T) {
	config, key := runtimeFixture(t)
	listenerReady := make(chan net.Listener, 1)
	var calls atomic.Int32
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, config, runtimeOptions{
			ResultsBase: testResultsBase(t),
			Executor: runtimeExecutor(func(ctx context.Context, request restjobs.Snapshot) error {
				calls.Add(1)
				if request.Request.Task != "review change" || request.Request.Repository != "trusted" {
					return errors.New("unexpected request")
				}
				close(started)
				<-ctx.Done()
				return ctx.Err()
			}),
			Listen: func(network, address string) (net.Listener, error) {
				listener, err := net.Listen(network, "127.0.0.1:0")
				if err == nil {
					listenerReady <- listener
				}
				return listener, err
			},
		})
	}()
	var listener net.Listener
	select {
	case listener = <-listenerReady:
	case err := <-done:
		t.Fatalf("server exited before listener startup: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("server did not start a listener")
	}
	baseURL := "http://" + listener.Addr().String()
	waitForStatus(t, baseURL+"/readyz", http.StatusOK)
	client := &http.Client{Timeout: 2 * time.Second}
	request := func(path, authorization string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, baseURL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	unauthorized := request("/v1/jobs", "")
	unauthorized.Body.Close()
	if unauthorized.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated job request status=%d", unauthorized.StatusCode)
	}
	body := `{"repository":"trusted","task":"review change"}`
	submit := func() (int, map[string]json.RawMessage) {
		req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "stable-key")
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var value map[string]json.RawMessage
		if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, value
	}
	status, first := submit()
	if status != http.StatusAccepted {
		t.Fatalf("submission status=%d body=%v", status, first)
	}
	var firstResponse struct {
		Job struct {
			ID string `json:"id"`
		} `json:"job"`
		Replayed bool `json:"replayed"`
	}
	if err := json.Unmarshal(first["job"], &firstResponse.Job); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(first["replayed"], &firstResponse.Replayed); err != nil {
		t.Fatal(err)
	}
	if firstResponse.Job.ID == "" || firstResponse.Replayed {
		t.Fatalf("first admission response=%v", first)
	}
	listRequest, err := http.NewRequest(http.MethodGet, baseURL+"/v1/jobs?limit=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	listRequest.Header.Set("Authorization", "Bearer "+key)
	listResponse, err := client.Do(listRequest)
	if err != nil {
		t.Fatal(err)
	}
	var listed restjobs.JobPage
	listBody, readErr := io.ReadAll(listResponse.Body)
	listResponse.Body.Close()
	if readErr != nil || json.Unmarshal(listBody, &listed) != nil || listResponse.StatusCode != http.StatusOK || len(listed.Jobs) != 1 || listed.Jobs[0].ID != firstResponse.Job.ID || strings.Contains(string(listBody), "review change") {
		t.Fatalf("runtime list response status=%d body=%s err=%v", listResponse.StatusCode, listBody, readErr)
	}
	status, replay := submit()
	var wasReplayed bool
	if err := json.Unmarshal(replay["replayed"], &wasReplayed); err != nil {
		t.Fatal(err)
	}
	if status != http.StatusAccepted || !wasReplayed {
		t.Fatalf("idempotent replay status=%d response=%v", status, replay)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("executor did not start")
	}
	job := request("/v1/jobs/"+firstResponse.Job.ID, "Bearer "+key)
	var snapshot restjobs.Snapshot
	if err := json.NewDecoder(job.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	job.Body.Close()
	if job.StatusCode != http.StatusOK || snapshot.Status != restjobs.StatusRunning {
		t.Fatalf("job status=%d snapshot=%+v", job.StatusCode, snapshot)
	}
	history := request("/v1/jobs/"+firstResponse.Job.ID+"/history", "Bearer "+key)
	var events restjobs.History
	if err := json.NewDecoder(history.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	history.Body.Close()
	if history.StatusCode != http.StatusOK || len(events.Events) < 2 {
		t.Fatalf("history status=%d events=%+v", history.StatusCode, events)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server shutdown error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down")
	}
	if calls.Load() != 1 {
		t.Fatalf("executor calls=%d, want exactly one idempotent execution", calls.Load())
	}
	if _, err := http.Get(baseURL + "/readyz"); err == nil {
		t.Fatal("server still accepted connections after shutdown")
	}
}

func TestRunExecutesFactoryStagesInPrivateWorkspace(t *testing.T) {
	config, key := runtimeFixture(t)
	root := config.Repositories["trusted"]
	callLog := filepath.Join(t.TempDir(), "harness-calls")
	harness := filepath.Join(t.TempDir(), "fake-harness.sh")
	script := "#!/bin/sh\nprintf '%s\\n' \"$PWD\" >> \"" + callLog + "\"\n"
	if err := os.WriteFile(harness, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(harness, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{task}", "{system_prompt}"}}
	checkMarker := filepath.Join(t.TempDir(), "configured-check-ran")
	check := filepath.Join(t.TempDir(), "verification-check.sh")
	if err := os.WriteFile(check, []byte("#!/bin/sh\ntest \"$1\" = \"--literal argument\" && touch \"$2\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	config.VerificationChecks = [][]string{{check, "--literal argument", checkMarker}}
	resultsBase := testResultsBase(t)
	listenerReady := make(chan net.Listener, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, config, runtimeOptions{
			ResultsBase: resultsBase,
			Listen: func(network, _ string) (net.Listener, error) {
				listener, err := net.Listen(network, "127.0.0.1:0")
				if err == nil {
					listenerReady <- listener
				}
				return listener, err
			},
		})
	}()
	var listener net.Listener
	select {
	case listener = <-listenerReady:
	case err := <-done:
		t.Fatalf("server exited before listener startup: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("server did not start a listener")
	}
	baseURL := "http://" + listener.Addr().String()
	waitForStatus(t, baseURL+"/readyz", http.StatusOK)
	requestBody := `{"repository":"trusted","task":"execute runtime integration"}`
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs", strings.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "real-workflow")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var admitted struct {
		Job restjobs.Snapshot `json:"job"`
	}
	if response.StatusCode != http.StatusAccepted || json.NewDecoder(response.Body).Decode(&admitted) != nil {
		response.Body.Close()
		t.Fatalf("job submission status=%d", response.StatusCode)
	}
	response.Body.Close()
	deadline := time.Now().Add(5 * time.Second)
	var snapshot restjobs.Snapshot
	for time.Now().Before(deadline) {
		pollRequest, err := http.NewRequest(http.MethodGet, baseURL+"/v1/jobs/"+admitted.Job.ID, nil)
		if err != nil {
			t.Fatal(err)
		}
		pollRequest.Header.Set("Authorization", "Bearer "+key)
		poll, err := http.DefaultClient.Do(pollRequest)
		if err == nil {
			_ = json.NewDecoder(poll.Body).Decode(&snapshot)
			poll.Body.Close()
			if snapshot.Status.Terminal() {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if snapshot.Status != restjobs.StatusSucceeded {
		t.Fatalf("runtime job status = %q, want succeeded", snapshot.Status)
	}
	calls, err := os.ReadFile(callLog)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(calls)), "\n")
	if len(lines) != 4 {
		t.Fatalf("Factory harness stage calls = %d, want requirements/implement/review/document", len(lines))
	}
	if _, err := os.Stat(checkMarker); err != nil {
		t.Fatalf("operator-configured verification argv was not invoked: %v", err)
	}
	resultRoot := filepath.Join(resultsBase, aliasDirectory("trusted"), "results", admitted.Job.ID)
	for index, workdir := range lines {
		want := filepath.Join(resultRoot, "worktree")
		if index == 0 {
			if filepath.Dir(filepath.Dir(filepath.Dir(workdir))) != resultRoot || filepath.Base(filepath.Dir(workdir)) != "runs" {
				t.Fatalf("requirements stage did not run under isolated workflow state: %q", workdir)
			}
			continue
		}
		if workdir != want || workdir == root {
			t.Fatalf("harness stage %d ran in %q, want isolated path %q", index, workdir, want)
		}
	}
	if _, err := os.Stat(filepath.Join(resultRoot, ".completion.json")); err != nil {
		t.Fatalf("successful workspace has no protected completion marker: %v", err)
	}
	if _, err := os.Stat(filepath.Join(resultRoot, "output", "workflow.log")); err != nil {
		t.Fatalf("private workflow output is missing: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func TestRunCancellationDoesNotWaitBeyondDeadlineForBlockedWorkspaceGit(t *testing.T) {
	config, _ := runtimeFixture(t)
	resultsBase := testResultsBase(t)
	resultsRoot := filepath.Join(resultsBase, aliasDirectory("trusted"), "results")
	var sweepNow atomic.Int64
	sweepNow.Store(time.Now().UTC().UnixNano())
	workspace, err := restworkspace.New(restworkspace.Config{
		RepositoryRoot: config.Repositories["trusted"], ResultsRoot: resultsRoot,
		Now: func() time.Time { return time.Unix(0, sweepNow.Load()).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	const id = "01234567-89ab-4cde-8fab-0123456789ab"
	if _, err := workspace.Create(id); err != nil {
		t.Fatal(err)
	}
	if err := workspace.MarkSucceeded(id); err != nil {
		t.Fatal(err)
	}
	enteredGit := make(chan struct{})
	releaseGit := make(chan struct{})
	listenerReady := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	shutdownBound := 200 * time.Millisecond
	go func() {
		done <- run(ctx, config, runtimeOptions{
			ResultsBase: resultsBase, ShutdownTimeout: shutdownBound,
			WorkspaceNow: func() time.Time { return time.Unix(0, sweepNow.Load()).UTC() }, SweepInterval: 10 * time.Millisecond,
			WorkspaceGit: func(runCtx context.Context, root string, args ...string) error {
				if len(args) >= 2 && args[0] == "worktree" && args[1] == "remove" {
					close(enteredGit)
					select {
					case <-runCtx.Done():
						return runCtx.Err()
					case <-releaseGit:
						return errors.New("released fake Git")
					}
				}
				command := exec.CommandContext(runCtx, "git", append([]string{"-C", root}, args...)...)
				return command.Run()
			},
			Listen: func(network, _ string) (net.Listener, error) {
				listener, err := net.Listen(network, "127.0.0.1:0")
				if err == nil {
					close(listenerReady)
				}
				return listener, err
			},
		})
	}()
	select {
	case <-listenerReady:
	case <-time.After(3 * time.Second):
		cancel()
		close(releaseGit)
		t.Fatal("server listener did not start")
	}
	sweepNow.Add(int64(25 * time.Hour))
	select {
	case <-enteredGit:
	case <-time.After(3 * time.Second):
		cancel()
		close(releaseGit)
		t.Fatal("hourly sweep did not reach blocked workspace Git cleanup")
	}
	started := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() after cancellation = %v", err)
		}
		if elapsed := time.Since(started); elapsed > shutdownBound+100*time.Millisecond {
			t.Fatalf("Run() cancellation took %s, beyond shared shutdown bound %s", elapsed, shutdownBound)
		}
	case <-time.After(shutdownBound + time.Second):
		close(releaseGit)
		t.Fatal("Run() blocked beyond configured shutdown bound")
	}
	close(releaseGit)
	if _, err := os.Stat(filepath.Join(resultsRoot, id)); err != nil {
		t.Fatalf("canceled cleanup removed workspace: %v", err)
	}
}

func TestRunRecordsExecutorFailureWithoutExposingDetails(t *testing.T) {
	config, key := runtimeFixture(t)
	listenerReady := make(chan net.Listener, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, config, runtimeOptions{
			ResultsBase: testResultsBase(t),
			Executor: runtimeExecutor(func(context.Context, restjobs.Snapshot) error {
				return errors.New("private /path and credential-secret")
			}),
			Listen: func(network, _ string) (net.Listener, error) {
				listener, err := net.Listen(network, "127.0.0.1:0")
				if err == nil {
					listenerReady <- listener
				}
				return listener, err
			},
		})
	}()
	listener := <-listenerReady
	baseURL := "http://" + listener.Addr().String()
	waitForStatus(t, baseURL+"/readyz", http.StatusOK)
	request, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs", strings.NewReader(`{"repository":"trusted","task":"fail privately"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+key)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "failure")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var admitted struct {
		Job restjobs.Snapshot `json:"job"`
	}
	if response.StatusCode != http.StatusAccepted || json.NewDecoder(response.Body).Decode(&admitted) != nil {
		response.Body.Close()
		t.Fatalf("job submission status=%d", response.StatusCode)
	}
	response.Body.Close()
	deadline := time.Now().Add(3 * time.Second)
	failed := false
	for time.Now().Before(deadline) {
		pollRequest, err := http.NewRequest(http.MethodGet, baseURL+"/v1/jobs/"+admitted.Job.ID, nil)
		if err != nil {
			t.Fatal(err)
		}
		pollRequest.Header.Set("Authorization", "Bearer "+key)
		poll, err := http.DefaultClient.Do(pollRequest)
		if err == nil {
			var snapshot restjobs.Snapshot
			_ = json.NewDecoder(poll.Body).Decode(&snapshot)
			poll.Body.Close()
			if snapshot.Status.Terminal() {
				if snapshot.Status != restjobs.StatusFailed {
					t.Fatalf("failed executor status=%q", snapshot.Status)
				}
				failed = true
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !failed {
		t.Fatal("executor failure did not reach a terminal failed status")
	}
	historyRequest, err := http.NewRequest(http.MethodGet, baseURL+"/v1/jobs/"+admitted.Job.ID+"/history", nil)
	if err != nil {
		t.Fatal(err)
	}
	historyRequest.Header.Set("Authorization", "Bearer "+key)
	history, err := http.DefaultClient.Do(historyRequest)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(history.Body)
	history.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if history.StatusCode != http.StatusOK || strings.Contains(string(data), "credential-secret") || strings.Contains(string(data), "/path") || !strings.Contains(string(data), `"failed"`) {
		t.Fatalf("sanitized failure history status=%d body=%s", history.StatusCode, data)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func TestRunConfiguredPostgresStartupFailureDoesNotOpenListenerOrLeakDSN(t *testing.T) {
	config, _ := runtimeFixture(t)
	config.Persistence = restserver.PersistenceConfig{
		Backend: restserver.PersistenceBackendPostgres,
		DSN:     "postgres://factory:credential-secret@127.0.0.1:1/factory?connect_timeout=1",
	}
	listenerCalled := false
	var output bytes.Buffer
	err := run(context.Background(), config, runtimeOptions{
		Log: log.New(&output, "", 0),
		Listen: func(string, string) (net.Listener, error) {
			listenerCalled = true
			return nil, errors.New("listener should not be reached")
		},
	})
	if err == nil || listenerCalled {
		t.Fatalf("PostgreSQL startup error=%v listener called=%v; configured storage failure must abort before accepting jobs", err, listenerCalled)
	}
	if output.Len() != 0 {
		t.Fatalf("startup log before listener readiness = %q, want none", output.String())
	}
	if strings.Contains(err.Error(), "credential-secret") || strings.Contains(output.String(), "credential-secret") {
		t.Fatalf("PostgreSQL startup leaked DSN credentials: error=%v log=%q", err, output.String())
	}
}

func TestRunConfiguredSQLiteStartupFailureDoesNotOpenListener(t *testing.T) {
	config, _ := runtimeFixture(t)
	config.Persistence = restserver.PersistenceConfig{
		Backend: restserver.PersistenceBackendSQLite,
		Path:    filepath.Join(t.TempDir(), "missing-parent", "jobs.db"),
	}
	listenerCalled := false
	var output bytes.Buffer
	err := run(context.Background(), config, runtimeOptions{
		Log: log.New(&output, "", 0),
		Listen: func(string, string) (net.Listener, error) {
			listenerCalled = true
			return nil, errors.New("listener should not be reached")
		},
	})
	if err == nil || listenerCalled {
		t.Fatalf("SQLite startup error=%v listener called=%v; configured storage failure must abort before accepting jobs", err, listenerCalled)
	}
	if output.Len() != 0 {
		t.Fatalf("startup log before listener readiness = %q, want none", output.String())
	}
}

func TestRunValidatesRepositoriesBeforeOpeningListener(t *testing.T) {
	config, _ := runtimeFixture(t)
	config.Repositories["trusted"] = filepath.Join(t.TempDir(), "missing")
	called := false
	err := run(context.Background(), config, runtimeOptions{Listen: func(string, string) (net.Listener, error) {
		called = true
		return nil, errors.New("listener should not be reached")
	}})
	if err == nil || called {
		t.Fatalf("startup error=%v listener called=%v", err, called)
	}
}

func TestRunRequiresProtectedServerStateParent(t *testing.T) {
	config, _ := runtimeFixture(t)
	base := filepath.Join(t.TempDir(), "shared")
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatal(err)
	}
	unsafeServerParent := filepath.Join(base, aliasDirectory("trusted"))
	if err := os.Mkdir(unsafeServerParent, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unsafeServerParent, 0o755); err != nil {
		t.Fatal(err)
	}
	// Existing broad parent directories are not silently chmodded or trusted
	// for cleanup; the workspace package rejects them at construction.
	called := false
	err := run(context.Background(), config, runtimeOptions{
		ResultsBase: base,
		Listen: func(string, string) (net.Listener, error) {
			called = true
			return nil, errors.New("listener should not be reached")
		},
	})
	if err == nil || called {
		t.Fatalf("startup error=%v listener called=%v", err, called)
	}
	info, err := os.Stat(unsafeServerParent)
	if err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("existing broad parent permissions changed: info=%v err=%v", info, err)
	}
}

func testResultsBase(t *testing.T) string {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(base, "state", "factory", "rest-server")
}

func runtimeFixture(t *testing.T) (restserver.Config, string) {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "repo")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"config", "user.email", "test@example.com"}, {"config", "user.name", "REST server test"}} {
		if output, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "tracked"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "tracked"}, {"commit", "-q", "-m", "baseline"}} {
		if output, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	key := "runtime-test-key"
	keyPath := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(keyPath, []byte(key), 0o600); err != nil {
		t.Fatal(err)
	}
	config := restserver.DefaultConfig()
	config.Repositories = map[string]string{"trusted": root}
	config.Harness = restserver.HarnessConfig{Executable: "pi", Args: []string{"{system_prompt}", "{task}"}}
	config.APIKeyFile = keyPath
	const operatorToken = "runtime-named-operator-token"
	digest := sha256.Sum256([]byte(operatorToken))
	verifierPath := filepath.Join(t.TempDir(), "operator-verifier.json")
	verifier, err := json.Marshal(struct {
		Version    int `json:"version"`
		Principals []struct {
			ID          string `json:"id"`
			TokenSHA256 string `json:"token_sha256"`
		} `json:"principals"`
	}{Version: 1, Principals: []struct {
		ID          string `json:"id"`
		TokenSHA256 string `json:"token_sha256"`
	}{{ID: "runtime-operator", TokenSHA256: hex.EncodeToString(digest[:])}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(verifierPath, verifier, 0o600); err != nil {
		t.Fatal(err)
	}
	config.Principals = []restserver.PrincipalConfig{{ID: "runtime-operator", Role: "operator", TokenFile: verifierPath, Enabled: true}}
	return config, key
}

func waitForStatus(t *testing.T, url string, status int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(url)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == status {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server did not return status %d at %s", status, url)
}
