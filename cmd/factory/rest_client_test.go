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

func TestRESTDispositionCommandRequiresConfirmationAndPostsOnlyDocumentedRoute(t *testing.T) {
	const token = "isolated-disposition-token"
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests = append(requests, r.Method+" "+r.URL.Path+" body="+string(body)+" auth="+r.Header.Get("Authorization"))
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"code":"unauthenticated","message":"PRIVATE_SERVER_DETAIL"}`)
			return
		}
		if r.Method != http.MethodPost || len(body) != 0 {
			t.Errorf("recovery request method/body = %s %q", r.Method, body)
		}
		if r.URL.Path == "/v1/jobs/eligible-id/disposition/failed" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"id":"eligible-id","status":"failed","request":{"task":"PRIVATE_TASK"},"provider":{"url":"PRIVATE_PROVIDER"},"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}`)
			return
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"code":"reconciliation_not_allowed","message":"PRIVATE_SERVER_DETAIL"}`)
	}))
	defer server.Close()

	root := t.TempDir()
	credentialPath := filepath.Join(root, "client-token")
	if err := os.WriteFile(credentialPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "rest-client.json")
	if err := os.WriteFile(configPath, []byte(`{"base_url":"`+server.URL+`","token_file":"client-token"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "factory")
	if output, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	run := func(args ...string) (string, string, error) {
		t.Helper()
		command := exec.Command(binary, args...)
		var stdout, stderr bytes.Buffer
		command.Stdout, command.Stderr = &stdout, &stderr
		err := command.Run()
		return stdout.String(), stderr.String(), err
	}

	for _, args := range [][]string{
		{"rest", "disposition", "--config", configPath, "failed", "eligible-id"},
		{"rest", "disposition", "--config", configPath, "--confirm", "succeeded", "eligible-id"},
		{"rest", "disposition", "--config", configPath, "--confirm", "failed"},
		{"rest", "disposition", "--config", configPath, "failed", "eligible-id", "--confirm=false"},
	} {
		stdout, stderr, err := run(args...)
		if err == nil || stdout != "" || !strings.Contains(stderr, "usage:") {
			t.Fatalf("invalid disposition arguments %v: err=%v stdout=%q stderr=%q", args, err, stdout, stderr)
		}
	}
	if len(requests) != 0 {
		t.Fatalf("invalid arguments sent HTTP requests: %v", requests)
	}

	stdout, stderr, err := run("rest", "disposition", "--config", configPath, "--confirm", "--json", "failed", "eligible-id")
	if err != nil || stderr != "" {
		t.Fatalf("disposition: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	var envelope struct {
		SchemaVersion int `json:"schema_version"`
		Result        struct {
			Job map[string]json.RawMessage `json:"job"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(stdout), &envelope); err != nil || envelope.SchemaVersion != 1 || len(envelope.Result.Job) != 4 {
		t.Fatalf("disposition JSON schema = %+v, err=%v output=%q", envelope, err, stdout)
	}
	for _, secret := range []string{"PRIVATE_TASK", "PRIVATE_PROVIDER", token, "PRIVATE_SERVER_DETAIL"} {
		if strings.Contains(stdout, secret) || strings.Contains(stderr, secret) {
			t.Errorf("disposition leaked %q: stdout=%q stderr=%q", secret, stdout, stderr)
		}
	}
	stdout, stderr, err = run("rest", "disposition", "--config", configPath, "--confirm", "canceled", "other-id")
	if err == nil || stdout != "" || !strings.Contains(stderr, "409") || strings.Contains(stderr, "PRIVATE_SERVER_DETAIL") {
		t.Fatalf("sanitized ineligible disposition: err=%v stdout=%q stderr=%q", err, stdout, stderr)
	}
	if err := os.WriteFile(credentialPath, []byte("wrong-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err = run("rest", "disposition", "--config", configPath, "--confirm", "failed", "eligible-id")
	if err == nil || stdout != "" || !strings.Contains(stderr, "401") || strings.Contains(stderr, "wrong-token") || strings.Contains(stderr, "PRIVATE_SERVER_DETAIL") {
		t.Fatalf("sanitized auth failure: err=%v stdout=%q stderr=%q", err, stdout, stderr)
	}
	if len(requests) != 3 {
		t.Fatalf("requests = %v; want only successful, conflict, and unauthenticated recovery calls", requests)
	}
	for i, request := range requests {
		if !strings.HasPrefix(request, "POST /v1/jobs/") || !strings.Contains(request, " body= auth=") || strings.Contains(request, "PRIVATE") {
			t.Errorf("request %d was not an authenticated bodyless documented POST: %q", i, request)
		}
	}
}

func TestRESTReconcileCommandUsesOnlyReadOnlyBodylessRoute(t *testing.T) {
	const token = "isolated-reconcile-token"
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests = append(requests, r.Method+" "+r.URL.Path+" body="+string(body))
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"code":"unauthenticated","message":"PRIVATE_SERVER_DETAIL"}`)
			return
		}
		if r.Method != http.MethodPost || len(body) != 0 {
			t.Errorf("reconcile request method/body = %s %q", r.Method, body)
		}
		if r.URL.Path == "/v1/jobs/reconciled-id/reconcile" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"id":"reconciled-id","status":"succeeded","request":{"task":"PRIVATE_TASK"},"provider":{"url":"PRIVATE_PROVIDER"},"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}`)
			return
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"code":"reconciliation_not_allowed","message":"PRIVATE_SERVER_DETAIL"}`)
	}))
	defer server.Close()

	root := t.TempDir()
	credentialPath := filepath.Join(root, "client-token")
	if err := os.WriteFile(credentialPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "rest-client.json")
	if err := os.WriteFile(configPath, []byte(`{"base_url":"`+server.URL+`","token_file":"client-token"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "factory")
	if output, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	run := func(args ...string) (string, string, error) {
		t.Helper()
		command := exec.Command(binary, args...)
		var stdout, stderr bytes.Buffer
		command.Stdout, command.Stderr = &stdout, &stderr
		err := command.Run()
		return stdout.String(), stderr.String(), err
	}
	for _, args := range [][]string{
		{"rest", "reconcile", "--config", configPath},
		{"rest", "reconcile", "--config", configPath, "--confirm", "reconciled-id"},
	} {
		stdout, stderr, err := run(args...)
		if err == nil || stdout != "" || !strings.Contains(stderr, "usage:") {
			t.Fatalf("invalid reconcile arguments %v: err=%v stdout=%q stderr=%q", args, err, stdout, stderr)
		}
	}
	if len(requests) != 0 {
		t.Fatalf("invalid reconcile arguments sent HTTP requests: %v", requests)
	}
	stdout, stderr, err := run("rest", "reconcile", "--config", configPath, "--json", "reconciled-id")
	if err != nil || stderr != "" {
		t.Fatalf("reconcile: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	var result struct {
		SchemaVersion int `json:"schema_version"`
		Result        struct {
			Job map[string]json.RawMessage `json:"job"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(stdout), &result); err != nil || result.SchemaVersion != 1 || len(result.Result.Job) != 4 {
		t.Fatalf("reconcile JSON = %+v err=%v output=%q", result, err, stdout)
	}
	for _, secret := range []string{"PRIVATE_TASK", "PRIVATE_PROVIDER", token} {
		if strings.Contains(stdout, secret) {
			t.Errorf("reconcile leaked %q: %s", secret, stdout)
		}
	}
	stdout, stderr, err = run("rest", "reconcile", "--config", configPath, "ineligible-id")
	if err == nil || stdout != "" || !strings.Contains(stderr, "409") || strings.Contains(stderr, "PRIVATE_SERVER_DETAIL") {
		t.Fatalf("sanitized ineligible reconciliation: err=%v stdout=%q stderr=%q", err, stdout, stderr)
	}
	if err := os.WriteFile(credentialPath, []byte("wrong-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err = run("rest", "reconcile", "--config", configPath, "reconciled-id")
	if err == nil || stdout != "" || !strings.Contains(stderr, "401") || strings.Contains(stderr, "wrong-token") || strings.Contains(stderr, "PRIVATE_SERVER_DETAIL") {
		t.Fatalf("sanitized auth failure: err=%v stdout=%q stderr=%q", err, stdout, stderr)
	}
	if len(requests) != 3 || requests[0] != "POST /v1/jobs/reconciled-id/reconcile body=" || requests[1] != "POST /v1/jobs/ineligible-id/reconcile body=" || requests[2] != "POST /v1/jobs/reconciled-id/reconcile body=" {
		t.Fatalf("requests = %v", requests)
	}
}

func TestRESTRecoveryCommandsRespectSQLiteEligibilityAndPreserveOtherJobs(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	databasePath := filepath.Join(root, "jobs.db")
	store, err := restjobs.OpenSQLiteStore(databasePath, restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 4, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	noAttempt, _, err := store.Admit("no-attempt-key", restjobs.Request{Repository: "widget", Task: "PRIVATE_TASK"})
	if err != nil {
		t.Fatal(err)
	}
	withAttempt, _, err := store.Admit("provider-attempt-key", restjobs.Request{Repository: "widget", Task: "PRIVATE_TASK"})
	if err != nil {
		t.Fatal(err)
	}
	unrelated, _, err := store.Admit("unrelated-key", restjobs.Request{Repository: "widget", Task: "UNRELATED_PRIVATE_TASK"})
	if err != nil {
		t.Fatal(err)
	}
	canceled, _, err := store.Admit("canceled-key", restjobs.Request{Repository: "widget", Task: "PRIVATE_TASK"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if claimed, err := store.ClaimNext(); err != nil || claimed.ID != unrelated.ID {
		t.Fatalf("claim unrelated job=(%+v,%v)", claimed, err)
	}
	if err := store.Finish(unrelated.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if claimed, err := store.ClaimNext(); err != nil || claimed.ID != canceled.ID {
		t.Fatalf("claim canceled job=(%+v,%v)", claimed, err)
	}
	attempt := restjobs.ProviderAttempt{Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + withAttempt.ID, Commit: "abc1234"}
	if err := store.RecordProviderAttempt(withAttempt.ID, attempt); err != nil {
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	store, err = restjobs.OpenSQLiteStore(databasePath, restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 4, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()

	const token = "isolated-sqlite-recovery-token"
	tokenPath := filepath.Join(root, "server-token")
	if err := os.WriteFile(tokenPath, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := restserver.LoadAPIKey(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	var lookups, providerWrites atomic.Int32
	handler, err := restapi.New(store, key, restapi.Config{
		MaxRequestBodyBytes: 2 << 20,
		MaxTaskBytes:        4096,
		RepositoryAliases:   map[string]struct{}{"widget": {}},
		Ready:               func() bool { return true },
		ReadyError:          func(context.Context) error { return nil },
		ResolveInterrupted: func(_ context.Context, id string, disposition restjobs.InterruptedDisposition) error {
			if !store.RecoveryNeeded(id) {
				return restjobs.ErrInvalidTransition
			}
			return store.ResolveInterrupted(id, disposition)
		},
		ReconcileProvider: func(_ context.Context, id string) error {
			lookups.Add(1)
			job, err := store.Get(id)
			if err != nil {
				return err
			}
			gotAttempt, err := store.ProviderAttempt(id)
			if err != nil || job.Status != restjobs.StatusRunning || !store.RecoveryNeeded(id) {
				return restjobs.ErrInvalidTransition
			}
			outcome := restjobs.ProviderOutcome{Provider: gotAttempt.Provider, Repository: gotAttempt.Repository, Number: 42, URL: "https://github.com/acme/widget/pull/42", Branch: gotAttempt.Branch, Commit: gotAttempt.Commit, State: "open"}
			return store.ReconcileProviderOutcome(id, outcome)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	clientToken := filepath.Join(root, "client-token")
	if err := os.WriteFile(clientToken, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "rest-client.json")
	if err := os.WriteFile(configPath, []byte(`{"base_url":"`+server.URL+`","token_file":"client-token"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "factory")
	if output, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	run := func(args ...string) (string, string, error) {
		t.Helper()
		command := exec.Command(binary, args...)
		var stdout, stderr bytes.Buffer
		command.Stdout, command.Stderr = &stdout, &stderr
		err := command.Run()
		return stdout.String(), stderr.String(), err
	}

	stdout, stderr, err := run("rest", "disposition", "--config", configPath, "--confirm", "failed", noAttempt.ID)
	if err != nil || stderr != "" || !strings.Contains(stdout, "failed") || strings.Contains(stdout, "PRIVATE") {
		t.Fatalf("eligible SQLite failed disposition: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	stdout, stderr, err = run("rest", "disposition", "--config", configPath, "--confirm", "canceled", canceled.ID)
	if err != nil || stderr != "" || !strings.Contains(stdout, "canceled") || strings.Contains(stdout, "PRIVATE") {
		t.Fatalf("eligible SQLite canceled disposition: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	stdout, stderr, err = run("rest", "disposition", "--config", configPath, "--confirm", "canceled", withAttempt.ID)
	if err == nil || stdout != "" || !strings.Contains(stderr, "409") || strings.Contains(stderr, "PRIVATE") {
		t.Fatalf("provider-attempt disposition should conflict: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	stdout, stderr, err = run("rest", "reconcile", "--config", configPath, withAttempt.ID)
	if err != nil || stderr != "" || !strings.Contains(stdout, "succeeded") || strings.Contains(stdout, "PRIVATE") {
		t.Fatalf("eligible SQLite reconciliation: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	if got := lookups.Load(); got != 1 {
		t.Fatalf("read-only provider lookups=%d, want one explicit reconciliation lookup", got)
	}
	if got := providerWrites.Load(); got != 0 {
		t.Fatalf("provider writes=%d, want none", got)
	}
	for id, want := range map[string]restjobs.Status{noAttempt.ID: restjobs.StatusFailed, canceled.ID: restjobs.StatusCanceled, withAttempt.ID: restjobs.StatusSucceeded, unrelated.ID: restjobs.StatusSucceeded} {
		job, err := store.Get(id)
		if err != nil || job.Status != want {
			t.Errorf("job %s status=(%s,%v), want %s", id, job.Status, err, want)
		}
	}
	if got, err := store.History(unrelated.ID); err != nil || len(got.Events) != 3 {
		t.Errorf("unrelated job history changed: (%+v,%v)", got, err)
	}
	if summary, err := store.OperationalSummary(t.Context()); err != nil || summary.Queued != 0 || summary.Failed != 1 || summary.Canceled != 1 || summary.Succeeded != 2 || summary.RecoveryNeeded != 0 {
		t.Errorf("post-recovery summary=(%+v,%v)", summary, err)
	}

	memory, err := restjobs.NewManager(restjobs.Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 4, MaxEventsPerJob: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer memory.Close()
	memoryJob, _, err := memory.Admit("memory-key", restjobs.Request{Repository: "widget", Task: "MEMORY_PRIVATE_TASK"})
	if err != nil {
		t.Fatal(err)
	}
	memoryHandler, err := restapi.New(memory, key, restapi.Config{
		MaxRequestBodyBytes: 2 << 20,
		MaxTaskBytes:        4096,
		RepositoryAliases:   map[string]struct{}{"widget": {}},
		Ready:               func() bool { return true },
		ReadyError:          func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	memoryServer := httptest.NewServer(memoryHandler)
	defer memoryServer.Close()
	if err := os.WriteFile(configPath, []byte(`{"base_url":"`+memoryServer.URL+`","token_file":"client-token"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"rest", "disposition", "--config", configPath, "--confirm", "failed", memoryJob.ID},
		{"rest", "reconcile", "--config", configPath, memoryJob.ID},
	} {
		stdout, stderr, err := run(args...)
		if err == nil || stdout != "" || !strings.Contains(stderr, "409") || strings.Contains(stderr, "MEMORY_PRIVATE_TASK") {
			t.Errorf("memory recovery request %v: err=%v stdout=%q stderr=%q", args, err, stdout, stderr)
		}
	}
	if got, err := memory.Get(memoryJob.ID); err != nil || got.Status != restjobs.StatusQueued {
		t.Errorf("memory recovery changed job=(%+v,%v)", got, err)
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
