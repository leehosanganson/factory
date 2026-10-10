package restapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
		ReadyError:          func(context.Context) error { return nil },
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
		{method: http.MethodGet, path: "/v1/jobs"},
	} {
		response := request(handler, tc.method, tc.path, `{"repository":"widget","task":"task"}`, false)
		if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"not_ready"`) {
			t.Errorf("%s %s while not ready = %d %s", tc.method, tc.path, response.Code, response.Body.String())
		}
	}
}

func TestListJobsRemainsAvailableAtAdmissionCapacity(t *testing.T) {
	manager := newTestManager(t, managerConfig(1, 1, 2))
	job, _, err := manager.Admit("list-at-capacity", restjobs.Request{Repository: "widget", Task: "private"})
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	response := request(handler, http.MethodGet, "/v1/jobs", "", true)
	var page restjobs.JobPage
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &page) != nil || len(page.Jobs) != 1 || page.Jobs[0].ID != job.ID {
		t.Fatalf("listing at full admission capacity status=%d body=%s", response.Code, response.Body.String())
	}
	if _, _, err := manager.Admit("over-capacity", restjobs.Request{Repository: "widget", Task: "another"}); !errors.Is(err, restjobs.ErrQueueFull) {
		t.Fatalf("admission at full capacity error=%v, want ErrQueueFull", err)
	}
}

func TestListJobsMapsStoreCapacityAndInternalErrorsWithoutLeakage(t *testing.T) {
	manager := newTestManager(t, managerConfig(1, 1, 2))
	readyHandler := newTestHandler(t, errorManager{Manager: manager, listErr: restjobs.ErrRegistryFull}, func() bool { return true })
	response := request(readyHandler, http.MethodGet, "/v1/jobs", "", true)
	assertError(t, response, http.StatusServiceUnavailable, "registry_full")
	failing := newTestHandler(t, errorManager{Manager: manager, listErr: errors.New("private /host/path provider secret")}, func() bool { return true })
	response = request(failing, http.MethodGet, "/v1/jobs", "", true)
	assertError(t, response, http.StatusInternalServerError, "internal_error")
	for _, forbidden := range []string{"private", "/host/path", "provider", "secret"} {
		if strings.Contains(response.Body.String(), forbidden) {
			t.Fatalf("listing error leaked %q: %s", forbidden, response.Body.String())
		}
	}
}

func TestReadinessBoundsConcurrentChecksAndRejectsExcessPromptly(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 2))
	key, err := restserver.LoadAPIKey(writeAPIKey(t, testAPIKey))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, readinessConcurrencyLimit)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	handler, err := New(manager, key, Config{
		MaxRequestBodyBytes: 256,
		MaxTaskBytes:        128,
		RepositoryAliases:   map[string]struct{}{"widget": {}},
		Ready:               func() bool { return true },
		ReadyError: func(context.Context) error {
			inFlight := active.Add(1)
			defer active.Add(-1)
			for prior := maximum.Load(); inFlight > prior && !maximum.CompareAndSwap(prior, inFlight); prior = maximum.Load() {
			}
			started <- struct{}{}
			<-release
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	admitted := make(chan *httptest.ResponseRecorder, readinessConcurrencyLimit)
	for range readinessConcurrencyLimit {
		go func() { admitted <- request(handler, http.MethodGet, "/readyz", "", false) }()
	}
	for range readinessConcurrencyLimit {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("readiness check did not start")
		}
	}

	for _, tc := range []struct {
		method string
		path   string
		body   string
		want   string
	}{
		{method: http.MethodGet, path: "/readyz", want: `{"status":"not_ready"}`},
		{method: http.MethodGet, path: "/v1/jobs/unknown", want: `{"error":{"code":"not_ready","message":"The server is not accepting requests."}}`},
	} {
		startedAt := time.Now()
		response := request(handler, tc.method, tc.path, tc.body, false)
		if response.Code != http.StatusServiceUnavailable || strings.TrimSpace(response.Body.String()) != tc.want {
			t.Errorf("excess %s %s response=%d %q", tc.method, tc.path, response.Code, response.Body.String())
		}
		if elapsed := time.Since(startedAt); elapsed > 100*time.Millisecond {
			t.Errorf("excess %s %s waited %s for readiness capacity", tc.method, tc.path, elapsed)
		}
	}
	if got := maximum.Load(); got != readinessConcurrencyLimit {
		t.Fatalf("maximum concurrent checks=%d, want bounded at %d", got, readinessConcurrencyLimit)
	}
	close(release)
	for range readinessConcurrencyLimit {
		response := <-admitted
		if response.Code != http.StatusOK {
			t.Errorf("admitted readiness status=%d, want 200", response.Code)
		}
	}
}

func TestReadinessCheckUsesRequestContextAndReleasesCapacity(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 2))
	handler := newTestHandler(t, manager, func() bool { return true })
	started := make(chan struct{})
	handler.config.ReadyError = func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	response := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, req)
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("readiness check did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled readiness handler did not return")
	}
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("canceled readiness status=%d, want 503", response.Code)
	}
	handler.config.ReadyError = func(context.Context) error { return nil }
	recovered := request(handler, http.MethodGet, "/readyz", "", false)
	if recovered.Code != http.StatusOK {
		t.Fatalf("readiness after cancellation status=%d, want 200", recovered.Code)
	}
}

func TestRoutesWithoutReadinessCallbackSkipReadinessChecks(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 2))
	handler := newTestHandler(t, manager, nil)
	var readinessChecks atomic.Int32
	handler.config.ReadyError = func(context.Context) error {
		readinessChecks.Add(1)
		return errors.New("unexpected readiness check")
	}
	response := request(handler, http.MethodGet, "/v1/jobs/unknown", "", false)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("route without readiness callback status=%d body=%s, want 401", response.Code, response.Body.String())
	}
	if got := readinessChecks.Load(); got != 0 {
		t.Fatalf("readiness checks without Ready callback=%d, want 0", got)
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

func TestInterruptedDispositionRouteIsAuthenticatedBodylessAndBounded(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("interrupted-disposition", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	called := atomic.Int32{}
	handler := newTestHandler(t, manager, func() bool { return true })
	handler.config.ResolveInterrupted = func(_ context.Context, id string, disposition restjobs.InterruptedDisposition) error {
		called.Add(1)
		if id != job.ID || disposition != restjobs.InterruptedDispositionFailed {
			return restjobs.ErrInvalidTransition
		}
		return nil
	}
	path := "/v1/jobs/" + job.ID + "/disposition/failed"
	if response := request(handler, http.MethodPost, path, "", false); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated disposition status=%d", response.Code)
	}
	if response := request(handler, http.MethodPost, path, `{"status":"succeeded"}`, true); response.Code != http.StatusBadRequest {
		t.Fatalf("body-bearing disposition status=%d body=%s", response.Code, response.Body.String())
	}
	for _, invalid := range []string{"succeeded", "running", "unknown"} {
		if response := request(handler, http.MethodPost, "/v1/jobs/"+job.ID+"/disposition/"+invalid, "", true); response.Code != http.StatusBadRequest {
			t.Errorf("invalid disposition %q status=%d body=%s", invalid, response.Code, response.Body.String())
		}
	}
	response := request(handler, http.MethodPost, path, "", true)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"queued"`) {
		t.Fatalf("valid disposition response=%d body=%s", response.Code, response.Body.String())
	}
	if called.Load() != 1 {
		t.Fatalf("disposition callback count=%d, want one callback for bodyless valid action", called.Load())
	}
	if response := request(handler, http.MethodGet, path, "", true); response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("disposition GET status=%d", response.Code)
	}
}

func TestQueuedJobCancellationIsAuthenticatedAndTerminalizesBeforeExecution(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("cancel-queued", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	path := "/v1/jobs/" + job.ID + "/cancel"
	if response := request(handler, http.MethodPost, path, "", false); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated cancellation status=%d", response.Code)
	}
	if response := request(handler, http.MethodPost, path, ` `, true); response.Code != http.StatusBadRequest {
		t.Fatalf("whitespace-body cancellation status=%d", response.Code)
	}
	response := request(handler, http.MethodPost, path, "", true)
	if response.Code != http.StatusOK {
		t.Fatalf("queued cancellation status=%d body=%s, want 200", response.Code, response.Body.String())
	}
	var canceled restjobs.Snapshot
	if err := json.Unmarshal(response.Body.Bytes(), &canceled); err != nil {
		t.Fatal(err)
	}
	if canceled.ID != job.ID || canceled.Status != restjobs.StatusCanceled {
		t.Fatalf("queued cancellation response=%+v, want same job canceled", canceled)
	}
	if _, err := manager.ClaimNext(); !errors.Is(err, restjobs.ErrNoQueuedJobs) {
		t.Fatalf("claim after queued cancellation=%v, want no queued jobs", err)
	}
	history, err := manager.History(job.ID)
	if err != nil || len(history.Events) != 2 || history.Events[1].Type != string(restjobs.StatusCanceled) {
		t.Fatalf("queued cancellation history=%+v err=%v", history, err)
	}
	if response := request(handler, http.MethodPost, path, "", true); response.Code != http.StatusConflict {
		t.Fatalf("repeat terminal cancellation status=%d body=%s, want 409", response.Code, response.Body.String())
	}
	if response := request(handler, http.MethodGet, path, "", true); response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("cancellation GET status=%d", response.Code)
	}
}

func TestCancellationRouteRepeatsPendingRequestWithoutDuplicateEvent(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("cancel-repeat", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	path := "/v1/jobs/" + job.ID + "/cancel"
	for range 2 {
		response := request(handler, http.MethodPost, path, "", true)
		if response.Code != http.StatusAccepted || !strings.Contains(response.Body.String(), `"cancellation_requested":true`) {
			t.Fatalf("pending cancellation response=%d body=%s, want 202 with pending flag", response.Code, response.Body.String())
		}
	}
	history, err := manager.History(job.ID)
	if err != nil || len(history.Events) != 3 || history.Events[2].Type != "cancel_requested" {
		t.Fatalf("repeated pending cancellation history=%+v err=%v", history, err)
	}
}

func TestCancellationRouteIsBodylessAndRejectsIneligibleJobsWithoutMutation(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("cancel-ineligible", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(job.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	path := "/v1/jobs/" + job.ID + "/cancel"
	if response := request(handler, http.MethodPost, path, `{"status":"canceled"}`, true); response.Code != http.StatusBadRequest {
		t.Fatalf("body-bearing cancellation status=%d body=%s", response.Code, response.Body.String())
	}
	response := request(handler, http.MethodPost, path, "", true)
	assertError(t, response, http.StatusConflict, "job_not_cancelable")
	got, err := manager.Get(job.ID)
	if err != nil || got.Status != restjobs.StatusSucceeded {
		t.Fatalf("terminal job mutated after rejected cancellation: %+v err=%v", got, err)
	}
}

func TestProviderReconciliationRouteRequiresAuthenticationAndEligibility(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("reconcile-route", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	called := atomic.Int32{}
	handler := newTestHandler(t, manager, func() bool { return true })
	handler.config.ReconcileProvider = func(context.Context, string) error {
		called.Add(1)
		return restjobs.ErrInvalidTransition
	}
	if response := request(handler, http.MethodPost, "/v1/jobs/"+job.ID+"/reconcile", "", false); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated reconciliation status=%d", response.Code)
	}
	response := request(handler, http.MethodPost, "/v1/jobs/"+job.ID+"/reconcile", "", true)
	assertError(t, response, http.StatusConflict, "reconciliation_not_allowed")
	if called.Load() != 1 || strings.Contains(response.Body.String(), "secret") {
		t.Fatalf("reconcile callback count=%d response=%s", called.Load(), response.Body.String())
	}
	if response := request(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/reconcile", "", true); response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("reconcile GET status=%d", response.Code)
	}
	if response := request(handler, http.MethodPost, "/v1/jobs/"+job.ID+"/reconcile", `{"number":1}`, true); response.Code != http.StatusBadRequest {
		t.Fatalf("body-bearing reconcile status=%d", response.Code)
	}
}

func TestListJobsIsAuthenticatedBoundedAndReadOnly(t *testing.T) {
	manager := newTestManager(t, managerConfig(3, 4, 8))
	first, _, err := manager.Admit("list-first", restjobs.Request{Repository: "widget", Task: "must never appear in list"})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := manager.Admit("list-second", restjobs.Request{Repository: "widget", Task: "another private task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(first.ID, restjobs.StatusFailed); err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	if response := request(handler, http.MethodGet, "/v1/jobs?limit=1", "", false); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated list status=%d body=%s", response.Code, response.Body.String())
	}
	response := request(handler, http.MethodGet, "/v1/jobs?limit=1", "", true)
	if response.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", response.Code, response.Body.String())
	}
	var page restjobs.JobPage
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 1 || page.Jobs[0].ID != first.ID || page.Jobs[0].Status != restjobs.StatusFailed || !page.HasMore || page.NextSequence == 0 || page.SnapshotSequence == 0 {
		t.Fatalf("first page=%+v want oldest retained record and cursor", page)
	}
	if strings.Contains(response.Body.String(), "must never appear") || strings.Contains(response.Body.String(), "another private task") || strings.Contains(response.Body.String(), "request") || strings.Contains(response.Body.String(), "provider") {
		t.Fatalf("list exposed payload or provider information: %s", response.Body.String())
	}
	third, _, err := manager.Admit("list-third", restjobs.Request{Repository: "widget", Task: "arrived between pages"})
	if err != nil {
		t.Fatal(err)
	}
	path := fmt.Sprintf("/v1/jobs?limit=1&after=%d&snapshot=%d", page.NextSequence, page.SnapshotSequence)
	response = request(handler, http.MethodGet, path, "", true)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), second.ID) || strings.Contains(response.Body.String(), third.ID) || strings.Contains(response.Body.String(), "arrived between pages") {
		t.Fatalf("continuation status=%d body=%s; must include second and exclude later admission", response.Code, response.Body.String())
	}
	before, err := manager.Get(second.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.History(second.ID); err != nil {
		t.Fatal(err)
	}
	beforeList, err := manager.ListJobs(context.Background(), 0, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	continuation := fmt.Sprintf("/v1/jobs?limit=1&after=%d&snapshot=%d", page.NextSequence, page.SnapshotSequence)
	if got := request(handler, http.MethodGet, continuation, "", true); got.Code != http.StatusOK {
		t.Fatalf("read-only continuation status=%d body=%s", got.Code, got.Body.String())
	}
	afterList, err := manager.ListJobs(context.Background(), 0, 0, 100)
	if err != nil || len(beforeList.Jobs) != len(afterList.Jobs) {
		t.Fatalf("listing changed retained record set: before=%+v after=%+v err=%v", beforeList, afterList, err)
	}
	after, err := manager.Get(second.ID)
	if err != nil || before.Status != after.Status || !before.UpdatedAt.Equal(after.UpdatedAt) {
		t.Fatalf("listing mutated job state: before=%+v after=%+v err=%v", before, after, err)
	}
	for _, invalid := range []string{"0", "-1", "101", "nope"} {
		if got := request(handler, http.MethodGet, "/v1/jobs?limit="+invalid, "", true); got.Code != http.StatusBadRequest {
			t.Errorf("invalid limit %q status=%d body=%s", invalid, got.Code, got.Body.String())
		}
	}
	for _, query := range []string{"after=1", "limit=1&after=2&snapshot=1", "limit=1&unknown=x", "limit=1&limit=2"} {
		if got := request(handler, http.MethodGet, "/v1/jobs?"+query, "", true); got.Code != http.StatusBadRequest {
			t.Errorf("invalid query %q status=%d body=%s", query, got.Code, got.Body.String())
		}
	}
	if got := request(handler, http.MethodDelete, "/v1/jobs", "", true); got.Code != http.StatusMethodNotAllowed || got.Header().Get("Allow") != "GET, POST" {
		t.Fatalf("collection DELETE status=%d allow=%q", got.Code, got.Header().Get("Allow"))
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

func TestOperationsSummaryIsAuthenticatedBoundedAndSanitized(t *testing.T) {
	manager := newTestManager(t, managerConfig(1, 3, 4))
	running, _, err := manager.Admit("summary-1", restjobs.Request{Repository: "widget", Task: "top-secret task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	queued, _, err := manager.Admit("summary-2", restjobs.Request{Repository: "widget", Task: "another hidden task"})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(running.ID, "secret", "private event transcript"); err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	if response := request(handler, http.MethodGet, "/v1/operations", "", false); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated operations status=%d, body=%s", response.Code, response.Body.String())
	}
	response := request(handler, http.MethodGet, "/v1/operations", "", true)
	if response.Code != http.StatusOK {
		t.Fatalf("operations response = %d %s", response.Code, response.Body.String())
	}
	var summary restjobs.OperationalSummary
	if err := json.Unmarshal(response.Body.Bytes(), &summary); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"retained_records": true, "record_limit": true, "queue_capacity": true,
		"queued": true, "running": true, "succeeded": true, "failed": true,
		"canceled": true, "queue_saturated": true, "recovery_needed": true,
	}
	for field := range fields {
		if !allowed[field] {
			t.Errorf("operations response contains unexpected field %q", field)
		}
	}
	if len(fields) != len(allowed) {
		t.Errorf("operations response fields = %v, want exactly %d count-only fields", fields, len(allowed))
	}
	if summary.RetainedRecords != 2 || summary.Queued != 1 || summary.Running != 1 || summary.RecoveryNeeded != 0 || !summary.QueueSaturated {
		t.Fatalf("operations summary = %+v", summary)
	}
	for _, secret := range []string{running.ID, queued.ID, "top-secret task", "another hidden task", "private event transcript", "summary-1"} {
		if strings.Contains(response.Body.String(), secret) {
			t.Errorf("operations response leaked %q: %s", secret, response.Body.String())
		}
	}
}

func TestOperationsSummaryStoreFailureIsSanitized(t *testing.T) {
	manager := errorSummaryManager{Manager: newTestManager(t, managerConfig(1, 1, 1)), err: errors.New("/private/store.db credential=never-log")}
	handler := newTestHandler(t, manager, func() bool { return true })
	response := request(handler, http.MethodGet, "/v1/operations", "", true)
	assertError(t, response, http.StatusInternalServerError, "internal_error")
	if strings.Contains(response.Body.String(), "private/store.db") || strings.Contains(response.Body.String(), "never-log") {
		t.Fatalf("operations error leaked internal details: %s", response.Body.String())
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
		{http.MethodDelete, "/v1/jobs", "GET, POST", 405, true},
		{http.MethodPost, "/v1/operations", "GET", 405, true},
		{http.MethodPost, "/v1/jobs/id", "GET", 405, true},
		{http.MethodPost, "/v1/jobs/id/history", "GET", 405, true},
		{http.MethodGet, "/v1/jobs/id/unknown", "", 404, true},
		{http.MethodGet, "/v1/jobsx", "", 404, false},
		{http.MethodGet, "/v1/operations/extra", "", 404, true},
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
	getErr  error
	listErr error
}

func (m errorManager) Get(id string) (restjobs.Snapshot, error) { return restjobs.Snapshot{}, m.getErr }
func (m errorManager) ListJobs(context.Context, uint64, uint64, int) (restjobs.JobPage, error) {
	if m.listErr != nil {
		return restjobs.JobPage{}, m.listErr
	}
	return m.Manager.ListJobs(context.Background(), 0, 0, 1)
}

type errorSummaryManager struct {
	restjobs.Manager
	err error
}

func (m errorSummaryManager) OperationalSummary(context.Context) (restjobs.OperationalSummary, error) {
	return restjobs.OperationalSummary{}, m.err
}

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
