package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/leehosanganson/factory/internal/restapi"
	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restserver"
)

func TestRESTOperationsProcessIsReadOnlyAndReturnsCuratedSummary(t *testing.T) {
	manager, err := restjobs.NewManager(restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	job, _, err := manager.Admit("private-idempotency-key", restjobs.Request{Repository: "widget", Task: "PRIVATE_TASK /private/task/path"})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(job.ID, "private-event", "PRIVATE_EVENT provider-secret /private/workspace"); err != nil {
		t.Fatal(err)
	}
	const token = "isolated-operations-token"
	tokenPath := filepath.Join(t.TempDir(), "server-token")
	if err := os.WriteFile(tokenPath, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := restserver.LoadAPIKey(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	var ready atomic.Bool
	ready.Store(true)
	handler, err := restapi.New(manager, key, restapi.Config{
		MaxRequestBodyBytes: 2 << 20,
		MaxTaskBytes:        256 << 10,
		RepositoryAliases:   map[string]struct{}{"widget": {}},
		Ready:               ready.Load,
		ReadyError:          func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()

	root := t.TempDir()
	binary := filepath.Join(root, "factory")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	credentialPath := filepath.Join(root, "client-token")
	if err := os.WriteFile(credentialPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "rest-client.json")
	config := `{"base_url":"` + server.URL + `","token_file":"client-token"}`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) (string, string, error) {
		t.Helper()
		command := exec.Command(binary, args...)
		var stdout, stderr bytes.Buffer
		command.Stdout, command.Stderr = &stdout, &stderr
		err := command.Run()
		return stdout.String(), stderr.String(), err
	}
	decodeOne := func(output string, target any) {
		t.Helper()
		decoder := json.NewDecoder(strings.NewReader(output))
		if err := decoder.Decode(target); err != nil {
			t.Fatalf("decode JSON output %q: %v", output, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			t.Fatalf("expected exactly one JSON value, extra=%v err=%v output=%q", trailing, err, output)
		}
	}
	before, err := manager.OperationalSummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err := run("rest", "operations", "--config", configPath)
	if err != nil || stderr != "" {
		t.Fatalf("operations text: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	if stdout != "Operations snapshot: 1 retained job(s), 1/10 records, queue 1/4 (saturated: false), queued 1, running 0, succeeded 0, failed 0, canceled 0, recovery needed 0.\n" || strings.Contains(stdout, "PRIVATE_TASK") || strings.Contains(stdout, "provider-secret") || strings.Contains(stdout, "/private/") || strings.Contains(stdout, job.ID) {
		t.Fatalf("operations text is not the expected safe aggregate: %q", stdout)
	}
	stdout, stderr, err = run("rest", "operations", "--config", configPath, "--json")
	if err != nil || stderr != "" {
		t.Fatalf("operations JSON: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	var output struct {
		SchemaVersion int `json:"schema_version"`
		Result        struct {
			Operations map[string]json.RawMessage `json:"operations"`
		} `json:"result"`
	}
	decodeOne(stdout, &output)
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(stdout), &envelope); err != nil || len(envelope) != 2 {
		t.Fatalf("JSON envelope fields = %v, err=%v", envelope, err)
	}
	var resultFields map[string]json.RawMessage
	if err := json.Unmarshal(envelope["result"], &resultFields); err != nil || len(resultFields) != 1 {
		t.Fatalf("JSON result fields = %v, err=%v", resultFields, err)
	}
	if output.SchemaVersion != 1 {
		t.Fatalf("schema_version = %d, want 1", output.SchemaVersion)
	}
	allowed := map[string]bool{"retained_records": true, "record_limit": true, "queue_capacity": true, "queued": true, "running": true, "succeeded": true, "failed": true, "canceled": true, "queue_saturated": true, "recovery_needed": true}
	if len(output.Result.Operations) != len(allowed) {
		t.Fatalf("operations fields = %v", output.Result.Operations)
	}
	for name := range output.Result.Operations {
		if !allowed[name] {
			t.Errorf("unexpected operations JSON field %q", name)
		}
	}
	for _, secret := range []string{job.ID, "private-idempotency-key", "PRIVATE_TASK", "PRIVATE_EVENT", "provider-secret", "/private/", token} {
		if strings.Contains(stdout, secret) {
			t.Errorf("operations JSON leaked %q: %s", secret, stdout)
		}
	}
	var summary restjobs.OperationalSummary
	if err := json.Unmarshal([]byte(mustRaw(output.Result.Operations)), &summary); err != nil {
		t.Fatal(err)
	}
	if summary != before {
		t.Fatalf("returned aggregate = %+v, before=%+v", summary, before)
	}
	if _, err := manager.Get(job.ID); err != nil {
		t.Fatalf("read-only command changed job state: %v", err)
	}
	after, err := manager.OperationalSummary(t.Context())
	if err != nil || after != before {
		t.Fatalf("operations changed manager state: before=%+v after=%+v err=%v", before, after, err)
	}
	mu.Lock()
	gotRequests := append([]string(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 2 || gotRequests[0] != "GET /v1/operations" || gotRequests[1] != "GET /v1/operations" {
		t.Fatalf("requests = %v, want only two GET /v1/operations calls", gotRequests)
	}

	if err := os.WriteFile(credentialPath, []byte("wrong-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err = run("rest", "operations", "--config", configPath)
	if err == nil || stdout != "" || !strings.Contains(stderr, "401") || !strings.Contains(stderr, "token configuration") || strings.Contains(stderr, "wrong-token") {
		t.Fatalf("sanitized auth failure: err=%v stdout=%q stderr=%q", err, stdout, stderr)
	}
	if err := os.WriteFile(credentialPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ready.Store(false)
	stdout, stderr, err = run("rest", "operations", "--config", configPath)
	if err == nil || stdout != "" || !strings.Contains(stderr, "503") || !strings.Contains(stderr, "not ready") || strings.Contains(stderr, token) || strings.Contains(stderr, "/private/") {
		t.Fatalf("sanitized readiness failure: err=%v stdout=%q stderr=%q", err, stdout, stderr)
	}
	mu.Lock()
	gotRequests = append([]string(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 4 || gotRequests[2] != "GET /v1/operations" || gotRequests[3] != "GET /v1/operations" {
		t.Fatalf("requests = %v, want only GET /v1/operations", gotRequests)
	}
}

func mustRaw(fields map[string]json.RawMessage) []byte {
	value, _ := json.Marshal(fields)
	return value
}

func TestRESTClientProcessSubmitsInspectsListsAndCancels(t *testing.T) {
	manager, err := restjobs.NewManager(restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	const token = "isolated-test-token"
	tokenPath := filepath.Join(t.TempDir(), "server-token")
	if err := os.WriteFile(tokenPath, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := restserver.LoadAPIKey(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := restapi.New(manager, key, restapi.Config{
		MaxRequestBodyBytes: 2 << 20,
		MaxTaskBytes:        256 << 10,
		RepositoryAliases:   map[string]struct{}{"widget": {}},
		Ready:               func() bool { return true },
		ReadyError:          func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	root := t.TempDir()
	binary := filepath.Join(root, "factory")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	credentialPath := filepath.Join(root, "client-token")
	if err := os.WriteFile(credentialPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "rest-client.json")
	config := `{"base_url":"` + server.URL + `","token_file":"client-token"}`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) (string, string, error) {
		t.Helper()
		command := exec.Command(binary, args...)
		var stdout, stderr bytes.Buffer
		command.Stdout, command.Stderr = &stdout, &stderr
		err := command.Run()
		return stdout.String(), stderr.String(), err
	}
	stdout, stderr, err := run("rest", "submit", "--config", configPath, "--repository", "widget", "--idempotency-key", "integration-key", "--json", "private task text")
	if err != nil || stderr != "" {
		t.Fatalf("submit: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	var submission struct {
		SchemaVersion int `json:"schema_version"`
		Submission    struct {
			Job struct {
				ID     string `json:"id"`
				Status string `json:"status"`
			} `json:"job"`
		} `json:"submission"`
	}
	if err := json.Unmarshal([]byte(stdout), &submission); err != nil {
		t.Fatalf("decode submit JSON %q: %v", stdout, err)
	}
	id := submission.Submission.Job.ID
	if submission.SchemaVersion != 1 || id == "" || submission.Submission.Job.Status != "queued" {
		t.Fatalf("submission = %+v", submission)
	}
	if strings.Contains(stdout, "private task text") || strings.Contains(stdout, token) {
		t.Fatalf("JSON output disclosed task/token: %q", stdout)
	}
	for _, args := range [][]string{{"get", "--config", configPath, "--json", id}, {"list", "--config", configPath, "--json"}} {
		stdout, stderr, err = run(append([]string{"rest"}, args...)...)
		if err != nil || stderr != "" || !json.Valid([]byte(stdout)) {
			t.Fatalf("inspect %v: err=%v stderr=%q stdout=%q", args, err, stderr, stdout)
		}
	}
	stdout, stderr, err = run("rest", "cancel", "--config", configPath, "--json", id)
	if err != nil || stderr != "" || !json.Valid([]byte(stdout)) {
		t.Fatalf("cancel: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	if !strings.Contains(stdout, "canceled") {
		t.Fatalf("cancel response = %q", stdout)
	}
	stdout, stderr, err = run("rest", "watch", "--config", configPath, "--json", id)
	if err != nil || stderr != "" || !json.Valid([]byte(stdout)) {
		t.Fatalf("watch terminal job: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	if strings.Contains(stdout, "private task text") || strings.Contains(stdout, token) {
		t.Fatalf("watch JSON disclosed task/token: %q", stdout)
	}
	if err := os.WriteFile(credentialPath, []byte("wrong-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err = run("rest", "get", "--config", configPath, id)
	if err == nil || !strings.Contains(stderr, "401") || !strings.Contains(stderr, "token configuration") || strings.Contains(stderr, "wrong-token") || stdout != "" {
		t.Fatalf("sanitized auth failure: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
}
