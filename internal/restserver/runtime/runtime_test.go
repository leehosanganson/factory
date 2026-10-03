package runtime

import (
	"bytes"
	"context"
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
	submitStatus := func() int {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs", strings.NewReader(`{"repository":"trusted","task":"must not queue"}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+key)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "outage-request")
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
	if got := submitStatus(); got != http.StatusServiceUnavailable {
		t.Fatalf("admission during store outage=%d, want %d", got, http.StatusServiceUnavailable)
	}
	store.up.Store(true)
	if got := getStatus("/readyz"); got != http.StatusOK {
		t.Fatalf("readiness after store recovery=%d, want %d", got, http.StatusOK)
	}
	publisher.up.Store(false)
	if got := getStatus("/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("readiness during provider outage=%d, want %d", got, http.StatusServiceUnavailable)
	}
	if got := submitStatus(); got != http.StatusServiceUnavailable {
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
	if got := submitStatus(); got != http.StatusAccepted {
		t.Fatalf("admission after provider recovery=%d, want %d", got, http.StatusAccepted)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && executions.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if executions.Load() != 1 {
		t.Fatalf("job execution count=%d, want one admitted job after recovery", executions.Load())
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
			}
			resultsBase := testResultsBase(t)
			var output bytes.Buffer
			listenerReady := make(chan net.Listener, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- run(ctx, config, runtimeOptions{
					Log:         log.New(&output, "", 0),
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
			for _, secret := range []string{key, "private-provider-token-secret", config.Persistence.Path, providerTokenPath, "owner/private-config-repository", config.APIKeyFile, config.Harness.Executable, config.Repositories["trusted"], config.ListenAddress} {
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
