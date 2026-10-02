package restapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restserver"
)

const testAPIKey = "handler-test-secret"

func newTestHandler(t *testing.T, manager restjobs.Manager, ready func() bool) *Handler {
	t.Helper()
	key, err := restserver.LoadAPIKey(writeAPIKey(t, testAPIKey))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(manager, key, Config{
		MaxRequestBodyBytes: 256,
		MaxTaskBytes:        128,
		RepositoryAliases:   map[string]struct{}{"widget": {}},
		Ready:               ready,
		ReadyError:          func() error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func newTestManager(t *testing.T, config restjobs.Config) *restjobs.LocalManager {
	t.Helper()
	manager, err := restjobs.NewManager(config)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func managerConfig(queue, records, events int) restjobs.Config {
	return restjobs.Config{QueueCapacity: queue, MaxConcurrentJobs: 1, MaxRecords: records, MaxEventsPerJob: events}
}

func writeAPIKey(t *testing.T, value string) string {
	t.Helper()
	path := t.TempDir() + "/api-key"
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func request(handler http.Handler, method, path, body string, authenticated bool) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if authenticated {
		r.Header.Set("Authorization", "Bearer "+testAPIKey)
	}
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, r)
	return recorder
}

func TestNotReadyRejectsJobOperationsBeforeAuthentication(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 2))
	handler := newTestHandler(t, manager, func() bool { return false })
	for _, tc := range []struct {
		method string
		path   string
	}{
		{method: http.MethodPost, path: "/v1/jobs"},
		{method: http.MethodGet, path: "/v1/jobs/unknown"},
	} {
		response := request(handler, tc.method, tc.path, `{"repository":"widget","task":"task"}`, false)
		if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"not_ready"`) {
			t.Errorf("%s %s while not ready = %d %s", tc.method, tc.path, response.Code, response.Body.String())
		}
	}
}

func TestHealthAndReadinessAreMinimalAndUnauthenticated(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 2))
	ready := false
	handler := newTestHandler(t, manager, func() bool { return ready })
	for _, tc := range []struct {
		path string
		want int
		body string
	}{{"/healthz", 200, `{"status":"ok"}`}, {"/readyz", 503, `{"status":"not_ready"}`}} {
		t.Run(tc.path, func(t *testing.T) {
			response := request(handler, http.MethodGet, tc.path, "", false)
			if response.Code != tc.want || strings.TrimSpace(response.Body.String()) != tc.body {
				t.Fatalf("response = %d %s", response.Code, response.Body.String())
			}
		})
	}
	ready = true
	response := request(handler, http.MethodGet, "/readyz", "", false)
	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != `{"status":"ready"}` {
		t.Fatalf("ready response = %d %s", response.Code, response.Body.String())
	}
	if response := request(newTestHandler(t, manager, nil), http.MethodGet, "/readyz", "", false); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil readiness callback status = %d", response.Code)
	}
}

func TestAuthenticationPrecedesJobPathResolution(t *testing.T) {
	handler := newTestHandler(t, newTestManager(t, managerConfig(2, 2, 2)), func() bool { return true })
	for _, path := range []string{"/v1/jobs", "/v1/jobs/unknown", "/v1/jobs/unknown/nope"} {
		response := request(handler, http.MethodGet, path, "", false)
		if response.Code != http.StatusUnauthorized {
			t.Errorf("GET %s unauthenticated = %d, want 401", path, response.Code)
		}
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	r.Header.Add("Authorization", "Bearer "+testAPIKey)
	r.Header.Add("Authorization", "Bearer second")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, r)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("duplicate Authorization headers status = %d", response.Code)
	}
}

func TestAdmissionReplayConflictAndQueueFull(t *testing.T) {
	manager := newTestManager(t, managerConfig(1, 2, 4))
	handler := newTestHandler(t, manager, nil)
	first := postJob(handler, `{"repository":"widget","task":"Fix the queue.","issue":12}`, "retry-1")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first admission = %d %s", first.Code, first.Body.String())
	}
	var accepted struct {
		Job      restjobs.Snapshot `json:"job"`
		Replayed bool              `json:"replayed"`
		Links    linksDTO          `json:"links"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.Replayed || accepted.Job.Request.Issue == nil || *accepted.Job.Request.Issue != 12 || accepted.Links.Self != "/v1/jobs/"+accepted.Job.ID || accepted.Links.History != accepted.Links.Self+"/history" {
		t.Fatalf("unexpected admission DTO: %+v", accepted)
	}
	replay := postJob(handler, `{"repository":"widget","task":"Fix the queue.","issue":12}`, "retry-1")
	var replayDTO struct {
		Job      restjobs.Snapshot `json:"job"`
		Replayed bool              `json:"replayed"`
	}
	if replay.Code != http.StatusAccepted || json.Unmarshal(replay.Body.Bytes(), &replayDTO) != nil || !replayDTO.Replayed || replayDTO.Job.ID != accepted.Job.ID {
		t.Fatalf("replay = %d %s", replay.Code, replay.Body.String())
	}
	conflict := postJob(handler, `{"repository":"widget","task":"Different request"}`, "retry-1")
	assertError(t, conflict, http.StatusConflict, "idempotency_conflict")
	full := postJob(handler, `{"repository":"widget","task":"Another task"}`, "retry-2")
	assertError(t, full, http.StatusServiceUnavailable, "queue_full")
}

func TestAdmissionRegistryFull(t *testing.T) {
	manager := newTestManager(t, managerConfig(1, 1, 4))
	first, _, err := manager.Admit("key-1", restjobs.Request{Repository: "widget", Task: "active"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, nil)
	response := postJob(handler, `{"repository":"widget","task":"cannot fit"}`, "key-2")
	assertError(t, response, http.StatusServiceUnavailable, "registry_full")
	if _, err := manager.Get(first.ID); err != nil {
		t.Fatalf("existing active job lost: %v", err)
	}
}

func TestRequestSchemaValidationAndBodyBounds(t *testing.T) {
	handler := newTestHandler(t, newTestManager(t, managerConfig(4, 4, 4)), nil)
	cases := []struct {
		name, body, contentType string
		status                  int
		code                    string
	}{
		{"issue URL", `{"repository":"widget","task":"valid","issue":"https://github.com/org/repo/issues/1"}`, "application/json", 400, "invalid_request"},
		{"nonpositive issue", `{"repository":"widget","task":"valid","issue":0}`, "application/json", 400, "invalid_request"},
		{"unknown alias", `{"repository":"secret-path","task":"valid"}`, "application/json", 400, "invalid_request"},
		{"blank task", `{"repository":"widget","task":"  "}`, "application/json", 400, "invalid_request"},
		{"unknown field", `{"repository":"widget","task":"valid","command":"oops"}`, "application/json", 400, "unknown_field"},
		{"duplicate field", `{"repository":"widget","repository":"widget","task":"valid"}`, "application/json", 400, "malformed_json"},
		{"nested duplicate", `{"repository":"widget","task":"valid","issue":1,"x":{"a":1,"a":2}}`, "application/json", 400, "malformed_json"},
		{"trailing value", `{"repository":"widget","task":"valid"} {}`, "application/json", 400, "malformed_json"},
		{"malformed", `{"repository":`, "application/json", 400, "malformed_json"},
		{"invalid utf8", string([]byte{'{', '"', 'r', 'e', 'p', 'o', 's', 'i', 't', 'o', 'r', 'y', '"', ':', '"', 0xff, '"', '}'}), "application/json", 400, "invalid_request"},
		{"unsupported media", `{"repository":"widget","task":"valid"}`, "text/plain", 415, "unsupported_media_type"},
		{"oversized", `{"repository":"widget","task":"` + strings.Repeat("x", 230) + `"}`, "application/json", 413, "body_too_large"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(tc.body))
			r.Header.Set("Authorization", "Bearer "+testAPIKey)
			r.Header.Set("Idempotency-Key", "key-"+tc.name)
			r.Header.Set("Content-Type", tc.contentType)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, r)
			assertError(t, response, tc.status, tc.code)
		})
	}
	missingHeader := postJobWithHeaders(handler, `{"repository":"widget","task":"valid"}`, "")
	assertError(t, missingHeader, 400, "invalid_request")
	duplicateHeaderRequest := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(`{"repository":"widget","task":"valid"}`))
	duplicateHeaderRequest.Header.Set("Authorization", "Bearer "+testAPIKey)
	duplicateHeaderRequest.Header.Add("Idempotency-Key", "one")
	duplicateHeaderRequest.Header.Add("Idempotency-Key", "two")
	duplicateHeaderRequest.Header.Set("Content-Type", "application/json")
	duplicateHeaderResponse := httptest.NewRecorder()
	handler.ServeHTTP(duplicateHeaderResponse, duplicateHeaderRequest)
	assertError(t, duplicateHeaderResponse, 400, "invalid_request")
}

func TestTaskLimitIsEnforcedBeforeAdmission(t *testing.T) {
	handler := newTestHandler(t, newTestManager(t, managerConfig(4, 4, 4)), nil)
	response := postJob(handler, `{"repository":"widget","task":"`+strings.Repeat("x", 129)+`"}`, "too-long-task")
	assertError(t, response, 400, "invalid_request")
}

func TestAPIAndManagerAcceptMaximumTaskBytes(t *testing.T) {
	const taskLimit = 256 << 10
	manager := newTestManager(t, restjobs.Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 2, MaxTaskBytes: taskLimit})
	key, err := restserver.LoadAPIKey(writeAPIKey(t, testAPIKey))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(manager, key, Config{MaxRequestBodyBytes: 512 << 10, MaxTaskBytes: taskLimit, RepositoryAliases: map[string]struct{}{"widget": {}}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(struct {
		Repository string `json:"repository"`
		Task       string `json:"task"`
	}{Repository: "widget", Task: strings.Repeat("x", taskLimit)})
	if err != nil {
		t.Fatal(err)
	}
	response := postJob(handler, string(body), "maximum-task")
	if response.Code != http.StatusAccepted {
		t.Fatalf("maximum task request status = %d body=%s", response.Code, response.Body.String())
	}
	overLimit, err := json.Marshal(struct {
		Repository string `json:"repository"`
		Task       string `json:"task"`
	}{Repository: "widget", Task: strings.Repeat("x", taskLimit+1)})
	if err != nil {
		t.Fatal(err)
	}
	assertError(t, postJob(handler, string(overLimit), "over-maximum-task"), http.StatusBadRequest, "invalid_request")
}

func TestStatusHistoryTruncationAndSafeErrors(t *testing.T) {
	manager := newTestManager(t, managerConfig(4, 4, 3))
	snapshot, _, err := manager.Admit("status-key", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(snapshot.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(snapshot.ID, "failed", "/host/path secret stack"); err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, nil)
	status := request(handler, http.MethodGet, "/v1/jobs/"+snapshot.ID, "", true)
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"task":"task"`) {
		t.Fatalf("status response = %d %s", status.Code, status.Body.String())
	}
	history := request(handler, http.MethodGet, "/v1/jobs/"+snapshot.ID+"/history", "", true)
	var historyDTO restjobs.History
	if history.Code != http.StatusOK || json.Unmarshal(history.Body.Bytes(), &historyDTO) != nil || !historyDTO.Truncated || len(historyDTO.Events) != 3 {
		t.Fatalf("history response = %d %s", history.Code, history.Body.String())
	}
	if historyDTO.Events[0].Type != "running" || historyDTO.Events[1].Type != "succeeded" || historyDTO.Events[2].Type != "failed" || historyDTO.Events[2].Message != "" {
		t.Fatalf("retained history was not safely filtered: %+v", historyDTO.Events)
	}
	for _, forbidden := range []string{"/host/path", "secret", "stack"} {
		if strings.Contains(history.Body.String(), forbidden) {
			t.Fatalf("history exposed %q: %s", forbidden, history.Body.String())
		}
	}
	missing := request(handler, http.MethodGet, "/v1/jobs/missing", "", true)
	assertError(t, missing, 404, "not_found")

	failing := newTestHandler(t, errorManager{Manager: manager, getErr: errors.New("raw /host/path secret stack")}, nil)
	internal := request(failing, http.MethodGet, "/v1/jobs/"+snapshot.ID, "", true)
	assertError(t, internal, 500, "internal_error")
	for _, forbidden := range []string{"raw", "/host/path", "secret", "stack"} {
		if strings.Contains(internal.Body.String(), forbidden) {
			t.Fatalf("internal error leaked %q: %s", forbidden, internal.Body.String())
		}
	}
}

func TestClosedManagerReturnsStableUnavailableResponse(t *testing.T) {
	manager := newTestManager(t, managerConfig(1, 1, 1))
	manager.Close()
	handler := newTestHandler(t, manager, func() bool { return true })
	response := postJob(handler, `{"repository":"widget","task":"task"}`, "key")
	assertError(t, response, http.StatusServiceUnavailable, "server_shutting_down")
}

func TestMethodsPathsAndAllowHeaders(t *testing.T) {
	handler := newTestHandler(t, newTestManager(t, managerConfig(4, 4, 4)), nil)
	for _, tc := range []struct {
		method, path, allow string
		status              int
		auth                bool
	}{
		{http.MethodPost, "/healthz", "GET", 405, false},
		{http.MethodPost, "/readyz", "GET", 405, false},
		{http.MethodGet, "/v1/jobs", "POST", 405, true},
		{http.MethodPost, "/v1/jobs/id", "GET", 405, true},
		{http.MethodPost, "/v1/jobs/id/history", "GET", 405, true},
		{http.MethodGet, "/v1/jobs/id/unknown", "", 404, true},
		{http.MethodGet, "/v1/jobsx", "", 404, false},
		{http.MethodGet, "/v1/jobs/%69d", "", 404, true},
	} {
		t.Run(fmt.Sprintf("%s %s", tc.method, tc.path), func(t *testing.T) {
			response := request(handler, tc.method, tc.path, "", tc.auth)
			if response.Code != tc.status {
				t.Fatalf("status = %d, body %s", response.Code, response.Body.String())
			}
			if tc.allow != "" && response.Header().Get("Allow") != tc.allow {
				t.Fatalf("Allow = %q, want %q", response.Header().Get("Allow"), tc.allow)
			}
		})
	}
}

type errorManager struct {
	restjobs.Manager
	getErr error
}

func (m errorManager) Get(id string) (restjobs.Snapshot, error) { return restjobs.Snapshot{}, m.getErr }

func postJob(handler http.Handler, body, key string) *httptest.ResponseRecorder {
	return postJobWithHeaders(handler, body, key)
}

func postJobWithHeaders(handler http.Handler, body, key string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+testAPIKey)
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	r.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, r)
	return response
}

func assertError(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	var envelope struct {
		Error errorDTO `json:"error"`
	}
	if response.Code != status || json.Unmarshal(response.Body.Bytes(), &envelope) != nil || envelope.Error.Code != code || envelope.Error.Message == "" {
		t.Fatalf("error response = %d %s, want %d %s", response.Code, response.Body.String(), status, code)
	}
}

func TestReadinessCanChangeWithAtomicCallback(t *testing.T) {
	var ready atomic.Bool
	handler := newTestHandler(t, newTestManager(t, managerConfig(1, 1, 1)), ready.Load)
	if got := request(handler, http.MethodGet, "/readyz", "", false).Code; got != 503 {
		t.Fatalf("initial readiness = %d", got)
	}
	ready.Store(true)
	if got := request(handler, http.MethodGet, "/readyz", "", false).Code; got != 200 {
		t.Fatalf("updated readiness = %d", got)
	}
}
