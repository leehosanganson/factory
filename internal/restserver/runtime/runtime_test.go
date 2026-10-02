package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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
	"github.com/leehosanganson/factory/internal/restserver"
)

type runtimeExecutor func(context.Context, restjobs.Snapshot) error

func (f runtimeExecutor) Execute(ctx context.Context, request restjobs.Snapshot) error {
	return f(ctx, request)
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
			ResultsBase: filepath.Join(t.TempDir(), "state", "factory", "rest-server"),
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
	listener := <-listenerReady
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
	resultsBase := filepath.Join(t.TempDir(), "state", "factory", "rest-server")
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
	listener := <-listenerReady
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

func TestRunRecordsExecutorFailureWithoutExposingDetails(t *testing.T) {
	config, key := runtimeFixture(t)
	listenerReady := make(chan net.Listener, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, config, runtimeOptions{
			ResultsBase: filepath.Join(t.TempDir(), "state", "factory", "rest-server"),
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
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatal(err)
	}
	unsafeServerParent := filepath.Join(base, aliasDirectory("trusted"))
	if err := os.Mkdir(unsafeServerParent, 0o755); err != nil {
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

func runtimeFixture(t *testing.T) (restserver.Config, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "repo")
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
