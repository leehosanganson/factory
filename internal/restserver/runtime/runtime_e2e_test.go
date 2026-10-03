package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
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
	publisher := &e2ePublisher{path: os.Getenv("FACTORY_E2E_OUTCOMES")}
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
	script := fmt.Sprintf("#!/bin/sh\ncase \"$1\" in fail-*) exit 42;; esac\nmkdir '%s/'\"$1\"\nsleep 0.2\nprintf 'implemented\\n' >> \"$PWD/result.txt\"\n", barrier)
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
	child.Env = append(os.Environ(), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath, "FACTORY_E2E_OUTCOMES="+outcomesPath)
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
	if err := child.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-childDone; err != nil {
		t.Fatalf("server process exit: %v", err)
	}
	child.Process = nil
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
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return submittedProcessJob{}, err
	}
	return submittedProcessJob{ID: result.Job.ID, Replayed: result.Replayed, StatusCode: resp.StatusCode}, nil
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
