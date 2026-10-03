package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restprovider"
	"github.com/leehosanganson/factory/internal/restserver"
	"github.com/leehosanganson/factory/internal/restworker"
)

func TestRESTServerProcessHelper(t *testing.T) {
	if os.Getenv("FACTORY_E2E_HELPER") != "1" {
		return
	}
	config, err := restserver.LoadConfig(os.Getenv("FACTORY_E2E_CONFIG"))
	if err != nil {
		os.Exit(11)
	}
	var publisher restprovider.Publisher = &e2ePublisher{path: os.Getenv("FACTORY_E2E_OUTCOMES")}
	if os.Getenv("FACTORY_E2E_PROVIDER_MODE") == "uncertain" {
		publisher = &uncertainE2EPublisher{path: os.Getenv("FACTORY_E2E_OUTCOMES")}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	listen := func(string, string) (net.Listener, error) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(os.Getenv("FACTORY_E2E_READY"), []byte(listener.Addr().String()), 0o600); err != nil {
			_ = listener.Close()
			return nil, err
		}
		return listener, nil
	}
	if err := run(ctx, config, runtimeOptions{Publisher: publisher, Listen: listen}); err != nil {
		os.Exit(12)
	}
	os.Exit(0)
}

type e2ePublisher struct{ path string }

type uncertainE2EPublisher struct{ path string }

type uncertainProviderRecord struct {
	JobID      string               `json:"job_id"`
	Repository string               `json:"repository"`
	Branch     string               `json:"branch"`
	Commit     string               `json:"commit"`
	Outcome    restprovider.Outcome `json:"outcome"`
	Creates    int                  `json:"creates"`
	Attempts   int                  `json:"attempts"`
}

func (p *uncertainE2EPublisher) Ping(context.Context) error { return nil }

func (p *uncertainE2EPublisher) Reconcile(ctx context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
	if err := ctx.Err(); err != nil {
		return restprovider.Outcome{}, err
	}
	data, err := os.ReadFile(p.path + ".uncertain.json")
	if err != nil {
		return restprovider.Outcome{}, fmt.Errorf("fake provider unavailable")
	}
	var record uncertainProviderRecord
	if err := json.Unmarshal(data, &record); err != nil || record.JobID != request.JobID || record.Repository != request.Repository || record.Branch != request.Branch || record.Commit != request.Commit {
		return restprovider.Outcome{}, fmt.Errorf("fake provider identity mismatch")
	}
	mode, _ := os.ReadFile(p.path + ".reconcile-mode")
	switch strings.TrimSpace(string(mode)) {
	case "outage", "missing", "multiple", "malformed":
		return restprovider.Outcome{}, fmt.Errorf("fake provider could not confirm PR")
	case "mismatch":
		record.Outcome.Commit = "different-commit"
	case "repository":
		record.Outcome.Repository = "other/repository"
	case "branch":
		record.Outcome.Branch = "other-branch"
	}
	return record.Outcome, nil
}

func (p *uncertainE2EPublisher) Publish(ctx context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
	path := p.path + ".uncertain.json"
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		branch, branchErr := exec.CommandContext(ctx, "git", "-C", request.Worktree, "branch", "--show-current").Output()
		commit, commitErr := exec.CommandContext(ctx, "git", "-C", request.Worktree, "rev-parse", "HEAD").Output()
		if branchErr != nil || strings.TrimSpace(string(branch)) != request.Branch || commitErr != nil || strings.TrimSpace(string(commit)) != request.Commit {
			return restprovider.Outcome{}, fmt.Errorf("uncertain provider request identity mismatch")
		}
		record := uncertainProviderRecord{
			JobID: request.JobID, Repository: request.Repository, Branch: request.Branch, Commit: request.Commit,
			Outcome: restprovider.Outcome{Provider: "github", Repository: request.Repository, Number: 71, URL: fmt.Sprintf("https://github.com/%s/pull/71", request.Repository), Branch: request.Branch, Commit: request.Commit, State: "open"},
			Creates: 1, Attempts: 1,
		}
		data, err = json.Marshal(record)
		if err != nil {
			return restprovider.Outcome{}, err
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			return restprovider.Outcome{}, err
		}
		<-ctx.Done()
		return restprovider.Outcome{}, fmt.Errorf("%w: fake response was lost after create", restprovider.ErrUncertain)
	}
	if err != nil {
		return restprovider.Outcome{}, err
	}
	var record uncertainProviderRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return restprovider.Outcome{}, err
	}
	if record.JobID != request.JobID || record.Repository != request.Repository || record.Branch != request.Branch || record.Commit != request.Commit {
		return restprovider.Outcome{}, fmt.Errorf("uncertain provider identity mismatch")
	}
	record.Attempts++
	data, err = json.Marshal(record)
	if err != nil {
		return restprovider.Outcome{}, err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return restprovider.Outcome{}, err
	}
	return record.Outcome, nil
}

func (p *e2ePublisher) Ping(context.Context) error { return nil }

func (p *e2ePublisher) Publish(ctx context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
	if request.JobID == "" {
		return restprovider.Outcome{}, fmt.Errorf("missing job identity")
	}
	started, err := os.OpenFile(p.path+".started", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return restprovider.Outcome{}, err
	}
	if _, err := started.WriteString(request.JobID + "\n"); err != nil {
		_ = started.Close()
		return restprovider.Outcome{}, err
	}
	if err := started.Close(); err != nil {
		return restprovider.Outcome{}, err
	}
	if err := ctx.Err(); err != nil {
		return restprovider.Outcome{}, err
	}
	branch, err := exec.CommandContext(ctx, "git", "-C", request.Worktree, "branch", "--show-current").Output()
	if err != nil || strings.TrimSpace(string(branch)) != request.Branch {
		return restprovider.Outcome{}, fmt.Errorf("published branch mismatch")
	}
	commit, err := exec.CommandContext(ctx, "git", "-C", request.Worktree, "rev-parse", "HEAD").Output()
	if err != nil || strings.TrimSpace(string(commit)) != request.Commit {
		return restprovider.Outcome{}, fmt.Errorf("published commit mismatch")
	}
	number := len(request.JobID)
	outcome := restprovider.Outcome{Provider: "github", Repository: request.Repository, Number: number, URL: fmt.Sprintf("https://github.com/%s/pull/%d", request.Repository, number), Branch: request.Branch, Commit: request.Commit, State: "open"}
	file, err := os.OpenFile(p.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return restprovider.Outcome{}, err
	}
	encodeErr := json.NewEncoder(file).Encode(struct {
		JobID      string `json:"job_id"`
		Repository string `json:"repository"`
		Worktree   string `json:"worktree"`
		Branch     string `json:"branch"`
		Commit     string `json:"commit"`
	}{request.JobID, request.Repository, request.Worktree, request.Branch, request.Commit})
	closeErr := file.Close()
	if encodeErr != nil {
		return restprovider.Outcome{}, encodeErr
	}
	if closeErr != nil {
		return restprovider.Outcome{}, closeErr
	}
	return outcome, nil
}

func TestRESTServerProcessRejectsUnavailableSQLiteBeforeListening(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level REST E2E")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	config, _ := runtimeFixture(t)
	config.Persistence = restserver.PersistenceConfig{
		Backend: restserver.PersistenceBackendSQLite,
		Path:    filepath.Join(t.TempDir(), "missing-parent", "jobs.db"),
	}
	configPath := filepath.Join(t.TempDir(), "server.json")
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	readyPath := filepath.Join(t.TempDir(), "listener")
	logPath := filepath.Join(t.TempDir(), "server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
	child.Env = append(os.Environ(), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath, "FACTORY_E2E_OUTCOMES="+filepath.Join(t.TempDir(), "outcomes.jsonl"))
	child.Stdout, child.Stderr = logFile, logFile
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	select {
	case err := <-done:
		if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 12 {
			t.Fatalf("server process exit=%v, want configured SQLite startup failure", err)
		}
	case <-time.After(10 * time.Second):
		_ = child.Process.Kill()
		<-done
		t.Fatal("server process did not fail promptly with unavailable configured SQLite")
	}
	if _, err := os.Stat(readyPath); !os.IsNotExist(err) {
		t.Fatalf("server published a listener despite unavailable configured SQLite: stat err=%v", err)
	}
}

func TestRESTServerProcessRejectsAndRetriesAtQueueCapacity(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level REST E2E")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	config, apiKey := runtimeFixture(t)
	config.Limits.Workers = 1
	config.Limits.QueueCapacity = 1
	providerToken := filepath.Join(t.TempDir(), "provider-token")
	if err := os.WriteFile(providerToken, []byte("test-provider-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	config.Provider = restserver.ProviderConfig{Backend: "github", TokenFile: providerToken, BaseBranch: "main", Repositories: map[string]string{"trusted": "acme/widget"}}
	barrier := filepath.Join(t.TempDir(), "worker-started")
	harness := filepath.Join(t.TempDir(), "capacity-harness.sh")
	script := fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = hold-worker ] && [ ! -e '%s' ]; then touch '%s'; sleep 2; fi\nprintf 'verified\\n' >> result.txt\n", barrier, barrier)
	if err := os.WriteFile(harness, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{task}", "{system_prompt}"}}
	configPath := filepath.Join(t.TempDir(), "server.json")
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	readyPath := filepath.Join(t.TempDir(), "listener")
	logPath := filepath.Join(t.TempDir(), "server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
	child.Env = append(os.Environ(), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath, "FACTORY_E2E_OUTCOMES="+filepath.Join(t.TempDir(), "outcomes.jsonl"))
	child.Stdout, child.Stderr = logFile, logFile
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	defer func() {
		if child.Process == nil {
			return
		}
		_ = child.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = child.Process.Kill()
			<-done
		}
	}()
	baseURL := waitForProcessURL(t, done, readyPath, logPath)
	client := &http.Client{Timeout: 3 * time.Second}
	first, err := submitProcessJob(client, baseURL, apiKey, "trusted", "hold-worker", "capacity-first")
	if err != nil || first.StatusCode != http.StatusAccepted || first.ID == "" {
		t.Fatalf("worker job admission=%+v err=%v", first, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(barrier); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(barrier); err != nil {
		t.Fatalf("worker did not start before filling queue: %v", err)
	}
	second, err := submitProcessJob(client, baseURL, apiKey, "trusted", "queued-job", "capacity-second")
	if err != nil || second.StatusCode != http.StatusAccepted || second.ID == "" {
		t.Fatalf("queued job admission=%+v err=%v", second, err)
	}
	full, err := submitProcessJob(client, baseURL, apiKey, "trusted", "retry-after-capacity", "capacity-retry")
	if err != nil || full.StatusCode != http.StatusServiceUnavailable || full.ErrorCode != "queue_full" || full.ID != "" {
		t.Fatalf("full-queue admission=%+v err=%v, want an unrecorded queue_full rejection", full, err)
	}
	request, err := http.NewRequest(http.MethodGet, baseURL+"/v1/operations", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+apiKey)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var summary restjobs.OperationalSummary
	decodeErr := json.NewDecoder(response.Body).Decode(&summary)
	_ = response.Body.Close()
	if decodeErr != nil || response.StatusCode != http.StatusOK || summary.Running != 1 || summary.Queued != 1 || summary.QueueCapacity != 1 || !summary.QueueSaturated {
		t.Fatalf("operations while full: status=%d summary=%+v decodeErr=%v", response.StatusCode, summary, decodeErr)
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job := getProcessJob(t, client, baseURL, first.ID, apiKey)
		if job.Status == restjobs.StatusSucceeded {
			break
		}
		if job.Status.Terminal() {
			t.Fatalf("running job ended unexpectedly: %+v", job)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if job := getProcessJob(t, client, baseURL, first.ID, apiKey); job.Status != restjobs.StatusSucceeded {
		t.Fatalf("worker did not complete and release capacity: %+v", job)
	}
	waitForProcessJob(t, client, baseURL, second.ID, apiKey)
	retried, err := submitProcessJob(client, baseURL, apiKey, "trusted", "retry-after-capacity", "capacity-retry")
	if err != nil || retried.StatusCode != http.StatusAccepted || retried.ID == "" || retried.Replayed {
		t.Fatalf("same-key retry after capacity freed=%+v err=%v, want new admission", retried, err)
	}
}

func TestRESTServerProcessRunsConcurrentSQLiteJobsThroughProvider(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level REST E2E")
	}
	config, apiKey := runtimeFixture(t)
	root := filepath.Dir(config.Repositories["trusted"])
	secondRepo := filepath.Join(root, "second-repository")
	if err := os.Mkdir(secondRepo, 0o700); err != nil {
		t.Fatal(err)
	}
	initE2ERepository(t, secondRepo)
	config.Repositories = map[string]string{"first": config.Repositories["trusted"], "second": secondRepo}
	config.Limits.Workers = 2
	config.Limits.QueueCapacity = 4
	config.Limits.MaxRecords = 8
	stateDir := testResultsBase(t)
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(stateDir, "jobs.db")}
	providerToken := filepath.Join(t.TempDir(), "provider-token")
	if err := os.WriteFile(providerToken, []byte("test-provider-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	config.Provider = restserver.ProviderConfig{Backend: "github", TokenFile: providerToken, BaseBranch: "main", Repositories: map[string]string{"first": "acme/first", "second": "acme/second"}}
	barrier := filepath.Join(t.TempDir(), "barrier")
	if err := os.Mkdir(barrier, 0o700); err != nil {
		t.Fatal(err)
	}
	harness := filepath.Join(t.TempDir(), "concurrent-harness.sh")
	script := fmt.Sprintf("#!/bin/sh\ncase \"$1\" in fail-*) exit 42;; cancel-*) touch '%s/cancel-started'; sleep 30; exit 0;; esac\nmkdir '%s/'\"$1\"\nsleep 0.2\nprintf 'implemented\\n' >> \"$PWD/result.txt\"\n", barrier, barrier)
	if err := os.WriteFile(harness, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{task}", "{system_prompt}"}}
	configPath := filepath.Join(t.TempDir(), "server.json")
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	readyDir, outcomeDir := t.TempDir(), t.TempDir()
	readyPath := filepath.Join(readyDir, "listener")
	outcomesPath := filepath.Join(outcomeDir, "outcomes.jsonl")
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
	child.Env = append(os.Environ(), "XDG_STATE_HOME="+filepath.Dir(filepath.Dir(stateDir)), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath, "FACTORY_E2E_OUTCOMES="+outcomesPath)
	childLogPath := filepath.Join(t.TempDir(), "server.log")
	childLog, err := os.Create(childLogPath)
	if err != nil {
		t.Fatal(err)
	}
	defer childLog.Close()
	child.Stdout, child.Stderr = childLog, childLog
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	childDone := make(chan error, 1)
	go func() { childDone <- child.Wait() }()
	defer func() {
		if child.Process != nil {
			_ = child.Process.Signal(syscall.SIGTERM)
			select {
			case <-childDone:
			case <-time.After(2 * time.Second):
				_ = child.Process.Kill()
				<-childDone
			}
		}
	}()
	baseURL := waitForProcessURL(t, childDone, readyPath, childLogPath)
	client := &http.Client{Timeout: 3 * time.Second}
	type response struct {
		status int
		body   struct {
			Job struct {
				ID string `json:"id"`
			} `json:"job"`
			Replayed bool `json:"replayed"`
		}
		err error
	}
	results := make(chan response, 2)
	for _, item := range []struct{ alias, task, key string }{{"first", "task-first", "key-first"}, {"second", "task-second", "key-second"}} {
		item := item
		go func() {
			body := strings.NewReader(fmt.Sprintf(`{"repository":%q,"task":%q}`, item.alias, item.task))
			req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs", body)
			if err != nil {
				results <- response{err: err}
				return
			}
			req.Header.Set("Authorization", "Bearer "+apiKey)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", item.key)
			resp, err := client.Do(req)
			if err != nil {
				results <- response{err: err}
				return
			}
			defer resp.Body.Close()
			var result response
			result.status = resp.StatusCode
			result.err = json.NewDecoder(resp.Body).Decode(&result.body)
			results <- result
		}()
	}
	ids := make([]string, 0, 2)
	for range 2 {
		result := <-results
		if result.err != nil || result.status != http.StatusAccepted || result.body.Replayed {
			t.Fatalf("admission status=%d replayed=%v err=%v", result.status, result.body.Replayed, result.err)
		}
		ids = append(ids, result.body.Job.ID)
	}
	if ids[0] == ids[1] {
		t.Fatalf("independent submissions shared job id %q", ids[0])
	}
	jobIDs := make(map[string]string, len(ids))
	requestIDs := make(map[string]string, len(ids))
	for _, id := range ids {
		request := getProcessJob(t, client, baseURL, id, apiKey)
		if request.Status != restjobs.StatusQueued && request.Status != restjobs.StatusRunning {
			t.Fatalf("job became terminal before polling: %+v", request)
		}
		expected := map[string]string{"task-first": "first", "task-second": "second"}[request.Request.Task]
		if expected == "" || request.Request.Repository != expected {
			t.Fatalf("accepted job request is not isolated: %+v", request.Request)
		}
		if request.Request.Task != "task-first" && request.Request.Task != "task-second" {
			t.Fatalf("unexpected task in inspectable result: %q", request.Request.Task)
		}
		jobIDs[request.Request.Task] = id
		requestIDs[request.Request.Task] = request.Request.Repository
	}
	if len(jobIDs) != 2 {
		t.Fatalf("request results did not preserve separate jobs: %v", jobIDs)
	}
	for _, id := range ids {
		waitForProcessJob(t, client, baseURL, id, apiKey)
		history := getProcessHistory(t, client, baseURL, id, apiKey)
		if len(history.Events) < 3 || history.Events[0].Type != "queued" || history.Events[1].Type != "running" {
			t.Fatalf("job history is not inspectable: %+v", history)
		}
	}
	expectedAfterRestart := getProcessJob(t, client, baseURL, jobIDs["task-first"], apiKey)
	startedData, err := os.ReadFile(outcomesPath + ".started")
	if err != nil {
		t.Fatal(err)
	}
	if attempts := strings.Fields(string(startedData)); len(attempts) != 2 {
		t.Fatalf("provider attempts=%d, want one per successful job", len(attempts))
	}
	failed, err := submitProcessJob(client, baseURL, apiKey, "first", "fail-workflow", "key-failure")
	if err != nil || failed.StatusCode != http.StatusAccepted || failed.ID == "" {
		t.Fatalf("failure admission: job=%+v err=%v", failed, err)
	}
	waitForProcessFailure(t, client, baseURL, failed.ID, apiKey)
	duplicate, err := submitProcessJob(client, baseURL, apiKey, "first", "task-first", "key-first")
	if err != nil || duplicate.StatusCode != http.StatusAccepted || !duplicate.Replayed || duplicate.ID != jobIDs["task-first"] {
		t.Fatalf("same-key retry did not return original job: %+v err=%v", duplicate, err)
	}
	entries, err := os.ReadDir(barrier)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("independent workflows did not overlap: %v", entries)
	}
	canceled, err := submitProcessJob(client, baseURL, apiKey, "first", "cancel-shutdown", "key-canceled")
	if err != nil || canceled.StatusCode != http.StatusAccepted || canceled.ID == "" {
		t.Fatalf("cancellation admission: job=%+v err=%v", canceled, err)
	}
	cancelStarted := filepath.Join(barrier, "cancel-started")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(cancelStarted); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(cancelStarted); err != nil {
		t.Fatalf("cancellation workflow did not start: %v", err)
	}
	if err := child.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-childDone; err != nil {
		t.Fatalf("server process exit: %v", err)
	}
	child.Process = nil

	createsBeforeRestart, err := os.ReadFile(outcomesPath)
	if err != nil {
		t.Fatal(err)
	}
	startedBeforeRestart, err := os.ReadFile(outcomesPath + ".started")
	if err != nil {
		t.Fatal(err)
	}
	restartedReady := filepath.Join(readyDir, "listener-restarted")
	restartedLogPath := filepath.Join(t.TempDir(), "server-restarted.log")
	restartedLog, err := os.Create(restartedLogPath)
	if err != nil {
		t.Fatal(err)
	}
	restartedChild := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
	restartedChild.Env = append(os.Environ(), "XDG_STATE_HOME="+filepath.Dir(filepath.Dir(stateDir)), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+restartedReady, "FACTORY_E2E_OUTCOMES="+outcomesPath)
	restartedChild.Stdout, restartedChild.Stderr = restartedLog, restartedLog
	if err := restartedChild.Start(); err != nil {
		_ = restartedLog.Close()
		t.Fatal(err)
	}
	restartedDone := make(chan error, 1)
	restartedExited := make(chan struct{})
	go func() {
		defer close(restartedExited)
		restartedDone <- restartedChild.Wait()
		_ = restartedLog.Close()
	}()
	restartedStopped := false
	stopRestarted := func() {
		if restartedStopped {
			return
		}
		restartedStopped = true
		_ = restartedChild.Process.Signal(syscall.SIGTERM)
		select {
		case <-restartedExited:
		case <-time.After(2 * time.Second):
			_ = restartedChild.Process.Kill()
			<-restartedExited
		}
	}
	defer stopRestarted()
	restartedURL := waitForProcessURL(t, restartedDone, restartedReady, restartedLogPath)
	replayed, err := submitProcessJob(client, restartedURL, apiKey, "first", "task-first", "key-first")
	if err != nil || replayed.StatusCode != http.StatusAccepted || !replayed.Replayed || replayed.ID != expectedAfterRestart.ID {
		stopRestarted()
		t.Fatalf("HTTP idempotency retry after process restart=%+v err=%v, want original job %q", replayed, err, expectedAfterRestart.ID)
	}
	restartedJob := getProcessJob(t, client, restartedURL, replayed.ID, apiKey)
	if restartedJob.ID != expectedAfterRestart.ID || restartedJob.Status != expectedAfterRestart.Status || restartedJob.Status != restjobs.StatusSucceeded || restartedJob.Provider == nil || expectedAfterRestart.Provider == nil || *restartedJob.Provider != *expectedAfterRestart.Provider {
		stopRestarted()
		t.Fatalf("HTTP replay changed successful provider job: before=%+v after=%+v", expectedAfterRestart, restartedJob)
	}
	createsAfterReplay, err := os.ReadFile(outcomesPath)
	if err != nil {
		t.Fatal(err)
	}
	startedAfterReplay, err := os.ReadFile(outcomesPath + ".started")
	if err != nil {
		t.Fatal(err)
	}
	if string(createsAfterReplay) != string(createsBeforeRestart) || string(startedAfterReplay) != string(startedBeforeRestart) {
		t.Fatalf("HTTP replay caused fake-provider create: before=%q after=%q", createsBeforeRestart, createsAfterReplay)
	}
	if err := restartedChild.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-restartedDone; err != nil {
		restartedStopped = true
		t.Fatalf("restarted server process exit: %v", err)
	}
	restartedStopped = true
	persisted, err := restworker.NewLocalJobManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer persisted.Close()
	for _, id := range ids {
		job, err := persisted.Get(id)
		if err != nil || job.Status != restjobs.StatusSucceeded || job.Provider == nil {
			t.Fatalf("durable outcome job=%+v err=%v", job, err)
		}
		history, err := persisted.History(id)
		if err != nil || len(history.Events) < 3 || history.Events[len(history.Events)-1].Type != string(restjobs.StatusSucceeded) {
			t.Fatalf("terminal provider outcome history=%+v err=%v", history, err)
		}
		key := map[string]string{jobIDs["task-first"]: "key-first", jobIDs["task-second"]: "key-second"}[id]
		replayed, duplicate, err := persisted.Admit(key, job.Request)
		if err != nil || !duplicate || replayed.ID != id {
			t.Fatalf("restart idempotency replay=(%+v,%v,%v)", replayed, duplicate, err)
		}
	}
	canceledJob, err := persisted.Get(canceled.ID)
	if err != nil || canceledJob.Status != restjobs.StatusCanceled || canceledJob.Provider != nil {
		t.Fatalf("shutdown-canceled durable job=%+v err=%v", canceledJob, err)
	}
	canceledHistory, err := persisted.History(canceled.ID)
	if err != nil || len(canceledHistory.Events) < 3 || canceledHistory.Events[len(canceledHistory.Events)-1].Type != string(restjobs.StatusCanceled) {
		t.Fatalf("shutdown-canceled history=%+v err=%v", canceledHistory, err)
	}
	canceledResult := filepath.Join(stateDir, aliasDirectory("first"), "results", canceled.ID)
	if _, err := os.Stat(filepath.Join(canceledResult, "worktree")); err != nil {
		t.Fatalf("shutdown-canceled workspace was not retained: %v", err)
	}
	if _, err := os.Stat(filepath.Join(canceledResult, ".completion.json")); !os.IsNotExist(err) {
		t.Fatalf("shutdown-canceled workspace has completion marker: %v", err)
	}
	type providerRecord struct {
		JobID      string `json:"job_id"`
		Repository string `json:"repository"`
		Worktree   string `json:"worktree"`
		Branch     string `json:"branch"`
		Commit     string `json:"commit"`
	}
	var records []providerRecord
	outcomeData, err := os.ReadFile(outcomesPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(outcomeData)), "\n") {
		var record providerRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	if len(records) != 2 || records[0].Worktree == records[1].Worktree || records[0].Branch == records[1].Branch {
		t.Fatalf("provider calls do not reflect isolated jobs: %+v", records)
	}
	for _, record := range records {
		if record.Commit == "" || !strings.HasPrefix(record.Branch, "factory/job/") {
			t.Fatalf("incomplete provider request: %+v", record)
		}
		expectedTask := map[string]string{jobIDs["task-first"]: "task-first", jobIDs["task-second"]: "task-second"}[record.JobID]
		wantRepository := map[string]string{"task-first": "acme/first", "task-second": "acme/second"}[expectedTask]
		if expectedTask == "" || record.Repository != wantRepository || requestIDs[expectedTask] == "" {
			t.Fatalf("provider repository does not match request alias: %+v", record)
		}
	}
}

func TestRESTServerProcessRecoversUncertainProviderCreateWithoutDuplicate(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level REST E2E")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	config, apiKey := runtimeFixture(t)
	stateDir := testResultsBase(t)
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(stateDir, "jobs.db")}
	providerToken := filepath.Join(t.TempDir(), "provider-token")
	if err := os.WriteFile(providerToken, []byte("test-provider-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	config.Provider = restserver.ProviderConfig{Backend: "github", TokenFile: providerToken, BaseBranch: "main", Repositories: map[string]string{"trusted": "acme/widget"}}
	config.Limits.JobTimeout = "2s"
	harness := filepath.Join(t.TempDir(), "harness.sh")
	harnessCount := filepath.Join(t.TempDir(), "harness-count")
	harnessScript := fmt.Sprintf("#!/bin/sh\nprintf 'run\\n' >> %q\nprintf 'verified\\n' >> result.txt\n", harnessCount)
	if err := os.WriteFile(harness, []byte(harnessScript), 0o700); err != nil {
		t.Fatal(err)
	}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{task}", "{system_prompt}"}}
	configPath := filepath.Join(t.TempDir(), "server.json")
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	outcomesPath := filepath.Join(t.TempDir(), "provider-outcomes.jsonl")
	startServer := func() (*exec.Cmd, <-chan error, string) {
		t.Helper()
		readyPath := filepath.Join(t.TempDir(), "listener")
		logPath := filepath.Join(t.TempDir(), "server.log")
		logFile, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
		child.Env = append(os.Environ(), "XDG_STATE_HOME="+filepath.Dir(filepath.Dir(stateDir)), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath, "FACTORY_E2E_OUTCOMES="+outcomesPath, "FACTORY_E2E_PROVIDER_MODE=uncertain")
		child.Stdout, child.Stderr = logFile, logFile
		if err := child.Start(); err != nil {
			_ = logFile.Close()
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			done <- child.Wait()
			_ = logFile.Close()
		}()
		return child, done, waitForProcessURL(t, done, readyPath, logPath)
	}

	child, childDone, baseURL := startServer()
	defer func() {
		if child.Process == nil {
			return
		}
		_ = child.Process.Signal(syscall.SIGTERM)
		select {
		case <-childDone:
		case <-time.After(2 * time.Second):
			_ = child.Process.Kill()
			<-childDone
		}
	}()
	client := &http.Client{Timeout: 10 * time.Second}
	admitted, err := submitProcessJob(client, baseURL, apiKey, "trusted", "publish with uncertain response", "uncertain-create-key")
	if err != nil || admitted.StatusCode != http.StatusAccepted || admitted.ID == "" {
		t.Fatalf("uncertain-create admission=%+v err=%v", admitted, err)
	}
	recordPath := outcomesPath + ".uncertain.json"
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(recordPath); err == nil {
			break
		}
		job := getProcessJob(t, client, baseURL, admitted.ID, apiKey)
		if job.Status.Terminal() {
			t.Fatalf("job became terminal before fake provider create; job=%+v history=%+v", job, getProcessHistory(t, client, baseURL, admitted.ID, apiKey))
		}
		select {
		case err := <-childDone:
			t.Fatalf("server exited before provider create became uncertain: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(recordPath); err != nil {
		job := getProcessJob(t, client, baseURL, admitted.ID, apiKey)
		t.Fatalf("fake provider did not record accepted PR create: %v; job=%+v history=%+v", err, job, getProcessHistory(t, client, baseURL, admitted.ID, apiKey))
	}
	waitForProcessFailure(t, client, baseURL, admitted.ID, apiKey)
	recordData, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	var record uncertainProviderRecord
	if err := json.Unmarshal(recordData, &record); err != nil {
		t.Fatal(err)
	}
	if record.JobID != admitted.ID || record.Creates != 1 || record.Attempts != 1 || record.Outcome.Number != 71 {
		t.Fatalf("fake provider create record=%+v", record)
	}
	harnessRuns, err := os.ReadFile(harnessCount)
	if err != nil || len(harnessRuns) == 0 {
		t.Fatalf("harness run count unavailable: %q %v", harnessRuns, err)
	}
	initialHarnessRuns := string(harnessRuns)
	failed := getProcessJob(t, client, baseURL, admitted.ID, apiKey)
	if failed.Status != restjobs.StatusFailed || failed.Provider != nil {
		t.Fatalf("job reported success despite ambiguous provider timeout: %+v", failed)
	}
	failedHistory := getProcessHistory(t, client, baseURL, admitted.ID, apiKey)
	if len(failedHistory.Events) < 3 || failedHistory.Events[len(failedHistory.Events)-1].Type != string(restjobs.StatusFailed) {
		t.Fatalf("ambiguous provider timeout evidence was not retained: %+v", failedHistory)
	}
	workspace := filepath.Join(stateDir, aliasDirectory("trusted"), "results", admitted.ID)
	if _, err := os.Stat(filepath.Join(workspace, "worktree")); err != nil {
		t.Fatalf("ambiguous provider workspace was not retained before restart: %v", err)
	}
	if err := child.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-childDone; err != nil {
		t.Fatalf("server shutdown after provider timeout: %v", err)
	}
	child.Process = nil

	persisted, err := restworker.NewLocalJobManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer persisted.Close()
	persistedJob, err := persisted.Get(admitted.ID)
	if err != nil || persistedJob.Status != restjobs.StatusFailed || persistedJob.Provider != nil {
		t.Fatalf("timed-out job was reported successful or lost: %+v err=%v", persistedJob, err)
	}
	persistedHistory, err := persisted.History(admitted.ID)
	if err != nil || len(persistedHistory.Events) < 3 || persistedHistory.Events[len(persistedHistory.Events)-1].Type != string(restjobs.StatusFailed) {
		t.Fatalf("timed-out provider evidence was not durable: history=%+v err=%v", persistedHistory, err)
	}
	recovery, err := persisted.Recover(context.Background())
	if err != nil || len(recovery.Terminal) != 1 || recovery.Terminal[0].ID != admitted.ID {
		t.Fatalf("timed-out job recovery classification=%+v err=%v", recovery, err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "worktree")); err != nil {
		t.Fatalf("uncertain provider workspace was not retained: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace, ".completion.json")); !os.IsNotExist(err) {
		t.Fatalf("uncertain provider workspace has completion marker: %v", err)
	}

	// Starting a fresh server must keep the failed job inspectable and must not
	// replay a provider write without explicit reconciliation.
	restarted, restartedDone, restartedURL := startServer()
	defer func() {
		if restarted.Process == nil {
			return
		}
		_ = restarted.Process.Signal(syscall.SIGTERM)
		select {
		case <-restartedDone:
		case <-time.After(2 * time.Second):
			_ = restarted.Process.Kill()
			<-restartedDone
		}
	}()
	restartedJob := getProcessJob(t, client, restartedURL, admitted.ID, apiKey)
	if restartedJob.Status != restjobs.StatusFailed || restartedJob.Provider != nil {
		t.Fatalf("restart changed unresolved provider job: %+v", restartedJob)
	}
	time.Sleep(100 * time.Millisecond)
	recordData, err = os.ReadFile(recordPath)
	if err != nil || json.Unmarshal(recordData, &record) != nil || record.Creates != 1 || record.Attempts != 1 {
		t.Fatalf("restart blindly retried unresolved provider write: record=%+v err=%v", record, err)
	}
	if err := restarted.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-restartedDone; err != nil {
		t.Fatalf("restarted server shutdown: %v", err)
	}
	restarted.Process = nil

	// Explicit operator action reconciles by the persisted identity through the
	// restarted process, records a single audit event, and never calls Publish.
	restarted, restartedDone, restartedURL = startServer()
	operatorReconcile := func(auth string) (int, string) {
		req, requestErr := http.NewRequest(http.MethodPost, restartedURL+"/v1/jobs/"+admitted.ID+"/reconcile", nil)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		if auth != "" {
			req.Header.Set("Authorization", "Bearer "+auth)
		}
		resp, requestErr := client.Do(req)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	if code, _ := operatorReconcile(""); code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated reconcile status=%d", code)
	}
	for _, mode := range []string{"mismatch", "repository", "branch", "missing", "multiple", "outage", "malformed"} {
		if err := os.WriteFile(outcomesPath+".reconcile-mode", []byte(mode), 0o600); err != nil {
			t.Fatal(err)
		}
		if code, body := operatorReconcile(apiKey); code != http.StatusConflict || strings.Contains(body, "fake provider") || strings.Contains(body, "provider-test-token") {
			t.Fatalf("%s reconciliation status=%d body=%s", mode, code, body)
		}
		unchanged := getProcessJob(t, client, restartedURL, admitted.ID, apiKey)
		if unchanged.Status != restjobs.StatusFailed || unchanged.Provider != nil {
			t.Fatalf("%s reconciliation changed job: %+v", mode, unchanged)
		}
		if _, err := os.Stat(filepath.Join(workspace, "worktree")); err != nil {
			t.Fatalf("%s reconciliation removed evidence: %v", mode, err)
		}
	}
	if err := os.Remove(outcomesPath + ".reconcile-mode"); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	unlockSQLite := lockSQLiteWrites(t, config.Persistence.Path)
	locked := true
	defer func() {
		if locked {
			unlockSQLite()
		}
	}()
	if code, body := operatorReconcile(apiKey); code != http.StatusConflict || strings.Contains(body, "SQLite") || strings.Contains(body, "provider-test-token") {
		t.Fatalf("reconciliation during SQLite write outage status=%d body=%s", code, body)
	}
	unlockSQLite()
	locked = false
	outageJob := getProcessJob(t, client, restartedURL, admitted.ID, apiKey)
	if outageJob.Status != restjobs.StatusFailed || outageJob.Provider != nil {
		t.Fatalf("SQLite write outage changed job: %+v", outageJob)
	}
	outageHistory := getProcessHistory(t, client, restartedURL, admitted.ID, apiKey)
	for _, event := range outageHistory.Events {
		if event.Type == "provider_reconciled" {
			t.Fatalf("SQLite write outage recorded successful reconciliation: %+v", outageHistory)
		}
	}
	providerAttemptStore, ok := persisted.(*restjobs.SQLiteStore)
	if !ok {
		t.Fatalf("persisted store type=%T, want SQLiteStore", persisted)
	}
	attemptDuringOutage, err := providerAttemptStore.ProviderAttempt(admitted.ID)
	if err != nil || !attemptDuringOutage.Uncertain {
		t.Fatalf("SQLite write outage lost uncertain provider attempt: %+v err=%v", attemptDuringOutage, err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "worktree")); err != nil {
		t.Fatalf("SQLite write outage removed retained workspace: %v", err)
	}
	if currentRuns, err := os.ReadFile(harnessCount); err != nil || string(currentRuns) != initialHarnessRuns {
		t.Fatalf("SQLite write outage reran harness: before=%q after=%q err=%v", initialHarnessRuns, currentRuns, err)
	}
	recordData, err = os.ReadFile(recordPath)
	if err != nil || json.Unmarshal(recordData, &record) != nil || record.Creates != 1 || record.Attempts != 1 {
		t.Fatalf("SQLite write outage reran provider write: record=%+v err=%v", record, err)
	}
	if err := os.Remove(outcomesPath + ".reconcile-mode"); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if code, body := operatorReconcile(apiKey); code != http.StatusOK || !strings.Contains(body, `"status":"succeeded"`) {
		t.Fatalf("authenticated reconcile status=%d body=%s", code, body)
	}
	if code, body := operatorReconcile(apiKey); code != http.StatusOK || !strings.Contains(body, `"status":"succeeded"`) {
		t.Fatalf("repeated reconcile status=%d body=%s", code, body)
	}
	reconciled := getProcessJob(t, client, restartedURL, admitted.ID, apiKey)
	if reconciled.Status != restjobs.StatusSucceeded || reconciled.Provider == nil || reconciled.Provider.Number != record.Outcome.Number {
		t.Fatalf("reconciled job=%+v", reconciled)
	}
	reconciledHistory := getProcessHistory(t, client, restartedURL, admitted.ID, apiKey)
	reconciledEvents := 0
	for _, event := range reconciledHistory.Events {
		if event.Type == "provider_reconciled" {
			reconciledEvents++
		}
	}
	if reconciledEvents != 1 {
		t.Fatalf("reconciliation audit count=%d history=%+v", reconciledEvents, reconciledHistory)
	}
	recordData, err = os.ReadFile(recordPath)
	if err != nil || json.Unmarshal(recordData, &record) != nil || record.Creates != 1 || record.Attempts != 1 {
		t.Fatalf("reconciliation reran publish or duplicated create: record=%+v err=%v", record, err)
	}
	harnessRuns, err = os.ReadFile(harnessCount)
	if err != nil || string(harnessRuns) != initialHarnessRuns {
		t.Fatalf("reconciliation reran harness: before=%q after=%q err=%v", initialHarnessRuns, harnessRuns, err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "worktree")); err != nil {
		t.Fatalf("reconciliation removed retained workspace: %v", err)
	}
	if err := restarted.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-restartedDone; err != nil {
		t.Fatalf("reconciled server shutdown: %v", err)
	}
	restarted.Process = nil
}

func lockSQLiteWrites(t *testing.T, path string) func() {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+url.PathEscape(filepath.ToSlash(path))+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open SQLite lock connection: %v", err)
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(context.Background())
	if err != nil {
		_ = db.Close()
		t.Fatalf("get SQLite lock connection: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		_ = conn.Close()
		_ = db.Close()
		t.Fatalf("acquire SQLite immediate write lock: %v", err)
	}
	return func() {
		t.Helper()
		if _, err := conn.ExecContext(context.Background(), "ROLLBACK"); err != nil {
			t.Errorf("release SQLite immediate write lock: %v", err)
		}
		if err := conn.Close(); err != nil {
			t.Errorf("close SQLite lock connection: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Errorf("close SQLite lock database: %v", err)
		}
	}
}

func initE2ERepository(t *testing.T, root string) {
	t.Helper()
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"config", "user.name", "E2E"}, {"config", "user.email", "e2e@localhost"}} {
		if output, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "README.md"}, {"commit", "-q", "-m", "baseline"}} {
		if output, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
}

func waitForProcessURL(t *testing.T, childDone <-chan error, readyPath, logPath string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(readyPath); err == nil {
			return "http://" + strings.TrimSpace(string(data))
		}
		select {
		case err := <-childDone:
			t.Fatalf("REST server helper exited early: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	logData, _ := os.ReadFile(logPath)
	t.Fatalf("REST server process did not become ready; childState=%v log=%s", childDone, logData)
	return ""
}

type submittedProcessJob struct {
	ID         string `json:"id"`
	Replayed   bool   `json:"replayed"`
	ErrorCode  string
	StatusCode int
}

func submitProcessJob(client *http.Client, baseURL, apiKey, alias, task, key string) (submittedProcessJob, error) {
	body := strings.NewReader(fmt.Sprintf(`{"repository":%q,"task":%q}`, alias, task))
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs", body)
	if err != nil {
		return submittedProcessJob{}, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := client.Do(req)
	if err != nil {
		return submittedProcessJob{}, err
	}
	defer resp.Body.Close()
	var result struct {
		Job struct {
			ID string `json:"id"`
		} `json:"job"`
		Replayed bool `json:"replayed"`
		Error    struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return submittedProcessJob{}, err
	}
	return submittedProcessJob{ID: result.Job.ID, Replayed: result.Replayed, ErrorCode: result.Error.Code, StatusCode: resp.StatusCode}, nil
}

func waitForProcessFailure(t *testing.T, client *http.Client, baseURL, id, apiKey string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job := getProcessJob(t, client, baseURL, id, apiKey)
		if job.Status.Terminal() {
			if job.Status != restjobs.StatusFailed || job.Provider != nil {
				t.Fatalf("failed workflow result=%+v", job)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not fail before timeout", id)
}

func getProcessHistory(t *testing.T, client *http.Client, baseURL, id, apiKey string) restjobs.History {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/jobs/"+id+"/history", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("job history status=%d", resp.StatusCode)
	}
	var history restjobs.History
	if err := json.NewDecoder(resp.Body).Decode(&history); err != nil {
		t.Fatal(err)
	}
	return history
}

func getProcessJob(t *testing.T, client *http.Client, baseURL, id, apiKey string) restjobs.Snapshot {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/jobs/"+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("job inspect status=%d", resp.StatusCode)
	}
	var job restjobs.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	return job
}

func waitForProcessJob(t *testing.T, client *http.Client, baseURL, id, apiKey string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/jobs/"+id, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := client.Do(req)
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		var job restjobs.Snapshot
		decodeErr := json.NewDecoder(resp.Body).Decode(&job)
		_ = resp.Body.Close()
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if job.Status.Terminal() {
			if job.Status != restjobs.StatusSucceeded || job.Provider == nil || job.Provider.URL == "" {
				t.Fatalf("job %s ended without durable provider outcome: %+v", id, job)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish before timeout", id)
}
