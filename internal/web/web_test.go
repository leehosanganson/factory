package restweb

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/leehosanganson/factory/internal/api"
	"github.com/leehosanganson/factory/internal/jobs"
	"github.com/leehosanganson/factory/internal/server"
)

func testWebHandler(t *testing.T) http.Handler {
	t.Helper()
	keyPath := t.TempDir() + "/key"
	if err := os.WriteFile(keyPath, []byte("legacy-web-test-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("web-test-token"))
	verifierPath := t.TempDir() + "/operator.json"
	verifier, _ := json.Marshal(map[string]any{"version": 1, "principals": []any{map[string]string{"id": "admin", "token_sha256": hex.EncodeToString(digest[:])}}})
	if err := os.WriteFile(verifierPath, verifier, 0o600); err != nil {
		t.Fatal(err)
	}
	resolver, err := restserver.LoadPrincipalResolver(restserver.Config{APIKeyFile: keyPath, Principals: []restserver.PrincipalConfig{{ID: "admin", Role: "operator", TokenFile: verifierPath, Enabled: true}}})
	if err != nil {
		t.Fatal(err)
	}
	key := resolver.APIKey()
	manager, err := restjobs.NewManager(restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 4, MaxEventsPerJob: 8})
	if err != nil {
		t.Fatal(err)
	}
	api, err := restapi.New(manager, key, restapi.Config{MaxRequestBodyBytes: 4096, MaxTaskBytes: 2048, RepositoryAliases: map[string]struct{}{"widget": {}}, DashboardOperatorID: "admin", Ready: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	return New(api, "admin")
}

func TestDashboardScheduleViewRendersBoundedScheduleSummaries(t *testing.T) {
	handler := testWebHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/ui/assets/dashboard.js", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("dashboard JavaScript status=%d", response.Code)
	}
	for _, want := range []string{
		`api("/v1/schedules")`,
		`api("/v1/threads", { method: "POST"`,
		`inspectThread(currentThread)`,
		`threadAttempt.fingerprint !== fingerprint`,
		`currentThread = response.thread.id`,
		"/v1/schedules/${encodeURIComponent(schedule.id)}/runs?limit=5",
		`pending_occurrence_at`,
		`skipped_occurrences`,
		`schedule-run-link`,
		`new Date(schedule.pending_occurrence_at).toISOString()`,
		`refreshSchedules();`,
		`if (schedulesRefreshing) { scheduleRefreshPending = true; return; }`,
		`const generation = ++scheduleRefreshGeneration;`,
		`if (!isCurrent()) return;`,
		`schedulesRefreshing = false;`,
		`generation === scheduleRefreshGeneration && !byId("dashboard").classList.contains("hidden")`,
		`if (scheduleRefreshPending) {`,
		`if (!byId("dashboard").classList.contains("hidden")) refreshSchedules();`,
		` scheduleRefreshGeneration++; scheduleRefreshPending = false;`,
		`if (!byId("dashboard").classList.contains("hidden")) { refreshJobs(); refreshSchedules(); }`,
		`byId("refresh").addEventListener("click", () => { refreshJobs(); refreshSchedules(); if (currentThread) inspectThread(currentThread); });`,
	} {
		if !strings.Contains(response.Body.String(), want) {
			t.Errorf("dashboard JavaScript missing schedule behavior %q", want)
		}
	}
	if strings.Contains(response.Body.String(), `schedule.task`) {
		t.Fatal("dashboard JavaScript reads schedule task text")
	}
}

func TestDashboardSelectedJobScrollRespectsReducedMotionPreference(t *testing.T) {
	handler := testWebHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/ui/assets/dashboard.js", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("dashboard JavaScript status=%d", response.Code)
	}
	for _, want := range []string{
		`window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth"`,
		`panel.scrollIntoView({ behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth", block: "start" })`,
	} {
		if !strings.Contains(response.Body.String(), want) {
			t.Errorf("dashboard JavaScript missing selected-job scroll behavior %q", want)
		}
	}
}

func TestDashboardAssetsAreEmbeddedAndHardened(t *testing.T) {
	handler := testWebHandler(t)
	for _, path := range []string{"/", "/ui/assets/dashboard.css", "/ui/assets/dashboard.js"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK || recorder.Body.Len() == 0 {
			t.Errorf("GET %s status=%d body=%q", path, recorder.Code, recorder.Body.String())
		}
		if path == "/" {
			for _, want := range []string{`id="thread-form"`, `id="thread-repository"`, `id="thread-task"`, `id="thread-result"`} {
				if !strings.Contains(recorder.Body.String(), want) {
					t.Errorf("dashboard HTML missing Thread UI element %s", want)
				}
			}
		}
		for _, header := range []string{"Content-Security-Policy", "X-Content-Type-Options", "X-Frame-Options", "Referrer-Policy", "Cache-Control"} {
			if recorder.Header().Get(header) == "" {
				t.Errorf("GET %s omitted %s", path, header)
			}
		}
	}
}

func TestOperatorSessionUsesHttpOnlyCookieAndCSRFProtectedProxy(t *testing.T) {
	handler := testWebHandler(t)
	login := httptest.NewRequest(http.MethodPost, "/ui/login", strings.NewReader(url.Values{"token": {"web-test-token"}}.Encode()))
	login.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	login.Header.Set("Origin", "https://factory.example")
	login.Host = "factory.example"
	loginRecorder := httptest.NewRecorder()
	handler.ServeHTTP(loginRecorder, login)
	if loginRecorder.Code != http.StatusOK {
		t.Fatalf("login status=%d body=%s", loginRecorder.Code, loginRecorder.Body.String())
	}
	cookie := loginRecorder.Result().Cookies()[0]
	if !cookie.HttpOnly || !cookie.Secure || cookie.SameSite != http.SameSiteStrictMode || strings.Contains(cookie.Value, "web-test-token") {
		t.Fatalf("unsafe session cookie: %+v", cookie)
	}
	var sessionData struct {
		CSRF string `json:"csrf"`
	}
	if err := json.Unmarshal(loginRecorder.Body.Bytes(), &sessionData); err != nil || sessionData.CSRF == "" {
		t.Fatalf("login CSRF response=%s err=%v", loginRecorder.Body.String(), err)
	}

	request := httptest.NewRequest(http.MethodPost, "/ui/api/v1/jobs", strings.NewReader(`{"repository":"widget","task":"safe test task"}`))
	request.Host = "factory.example"
	request.Header.Set("Origin", "https://factory.example")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "web-ui-test-key")
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("missing CSRF status=%d body=%s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/ui/api/v1/threads", strings.NewReader(`{"repository":"widget","task":"safe thread task"}`))
	request.Host = "factory.example"
	request.Header.Set("Origin", "https://factory.example")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "web-thread-create")
	request.AddCookie(cookie)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("Thread create missing CSRF status=%d body=%s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "/ui/api/v1/threads", strings.NewReader(`{"repository":"widget","task":"safe thread task"}`))
	request.Host = "factory.example"
	request.Header.Set("Origin", "https://evil.example")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "web-thread-wrong-origin")
	request.Header.Set("X-CSRF-Token", sessionData.CSRF)
	request.AddCookie(cookie)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("Thread create wrong Origin status=%d body=%s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/ui/api/v1/threads", strings.NewReader(`{"repository":"widget","task":"safe thread task"}`))
	request.Host = "factory.example"
	request.Header.Set("Origin", "https://factory.example")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "web-thread-create")
	request.Header.Set("X-CSRF-Token", sessionData.CSRF)
	request.AddCookie(cookie)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || strings.Contains(response.Body.String(), "safe thread task") {
		t.Fatalf("Thread create status=%d body=%s", response.Code, response.Body.String())
	}
	var created struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
		Job struct {
			ID string `json:"id"`
		} `json:"job"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil || created.Thread.ID == "" || created.Job.ID == "" {
		t.Fatalf("Thread response=%s err=%v", response.Body.String(), err)
	}
	inspect := httptest.NewRequest(http.MethodGet, "/ui/api/v1/threads/"+created.Thread.ID, nil)
	inspect.AddCookie(cookie)
	inspectResponse := httptest.NewRecorder()
	handler.ServeHTTP(inspectResponse, inspect)
	if inspectResponse.Code != http.StatusOK || !strings.Contains(inspectResponse.Body.String(), created.Job.ID) || strings.Contains(inspectResponse.Body.String(), "safe thread task") {
		t.Fatalf("Thread inspect status=%d body=%s", inspectResponse.Code, inspectResponse.Body.String())
	}
	retry := httptest.NewRequest(http.MethodPost, "/ui/api/v1/threads", strings.NewReader(`{"repository":"widget","task":"safe thread task"}`))
	retry.Host = "factory.example"
	retry.Header.Set("Origin", "https://factory.example")
	retry.Header.Set("Content-Type", "application/json")
	retry.Header.Set("Idempotency-Key", "web-thread-create")
	retry.Header.Set("X-CSRF-Token", sessionData.CSRF)
	retry.AddCookie(cookie)
	retryResponse := httptest.NewRecorder()
	handler.ServeHTTP(retryResponse, retry)
	if retryResponse.Code != http.StatusAccepted || !strings.Contains(retryResponse.Body.String(), created.Thread.ID) || !strings.Contains(retryResponse.Body.String(), `"replayed":true`) {
		t.Fatalf("Thread idempotent retry status=%d body=%s", retryResponse.Code, retryResponse.Body.String())
	}
	invalid := httptest.NewRequest(http.MethodPost, "/ui/api/v1/threads", strings.NewReader(`{"repository":"not-allowed","task":"sentinel error task"}`))
	invalid.Host = "factory.example"
	invalid.Header.Set("Origin", "https://factory.example")
	invalid.Header.Set("Content-Type", "application/json")
	invalid.Header.Set("Idempotency-Key", "web-thread-invalid")
	invalid.Header.Set("X-CSRF-Token", sessionData.CSRF)
	invalid.AddCookie(cookie)
	invalidResponse := httptest.NewRecorder()
	handler.ServeHTTP(invalidResponse, invalid)
	if invalidResponse.Code != http.StatusBadRequest || strings.Contains(invalidResponse.Body.String(), "sentinel error task") {
		t.Fatalf("Thread sanitized error status=%d body=%s", invalidResponse.Code, invalidResponse.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/ui/api/v1/jobs", strings.NewReader(`{"repository":"widget","task":"safe test task"}`))
	request.Host = "factory.example"
	request.Header.Set("Origin", "https://factory.example")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "web-ui-test-key")
	request.Header.Set("X-CSRF-Token", sessionData.CSRF)
	request.AddCookie(cookie)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || strings.Contains(response.Body.String(), "web-test-token") {
		t.Fatalf("CSRF-authenticated admission status=%d body=%s", response.Code, response.Body.String())
	}

	for _, tc := range []struct {
		method string
		path   string
		want   int
	}{{http.MethodGet, "/ui/api/v1/schedules", http.StatusServiceUnavailable}, {http.MethodGet, "/ui/api/v1/schedules/example/runs?limit=5", http.StatusServiceUnavailable}, {http.MethodPost, "/ui/api/v1/schedules", http.StatusNotFound}, {http.MethodPatch, "/ui/api/v1/schedules/example", http.StatusNotFound}, {http.MethodGet, "/ui/api/v1/schedules/example/history", http.StatusNotFound}, {http.MethodGet, "/ui/api/v1/threads", http.StatusNotFound}, {http.MethodPost, "/ui/api/v1/threads/id/jobs", http.StatusNotFound}, {http.MethodPost, "/ui/api/v1/threads/id/complete", http.StatusNotFound}, {http.MethodGet, "/ui/api/v1/threads/id/extra", http.StatusNotFound}, {http.MethodPost, "/ui/api/v1/threads?foo=bar", http.StatusBadRequest}, {http.MethodGet, "/ui/api/v1/threads/id?foo=bar", http.StatusNotFound}} {
		proxyRequest := httptest.NewRequest(tc.method, tc.path, nil)
		proxyRequest.Host = "factory.example"
		proxyRequest.Header.Set("Origin", "https://factory.example")
		proxyRequest.Header.Set("X-CSRF-Token", sessionData.CSRF)
		proxyRequest.AddCookie(cookie)
		proxyResponse := httptest.NewRecorder()
		handler.ServeHTTP(proxyResponse, proxyRequest)
		if proxyResponse.Code != tc.want {
			t.Errorf("proxy %s %s status=%d body=%s; want %d", tc.method, tc.path, proxyResponse.Code, proxyResponse.Body.String(), tc.want)
		}
	}
}

func TestDashboardRejectsLoginWithoutOriginAndDisallowsQuerySecrets(t *testing.T) {
	handler := testWebHandler(t)
	request := httptest.NewRequest(http.MethodPost, "/ui/login", strings.NewReader("token=web-test-token"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("login without HTTPS origin status=%d", response.Code)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/ui/session?token=web-test-token", nil))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("UI query string status=%d", response.Code)
	}
}

func TestDashboardRequiresConfiguredNamedOperatorAndScopesProxy(t *testing.T) {
	handler := testWebHandler(t)
	for _, token := range []string{"wrong", "legacy-web-test-token"} {
		login := httptest.NewRequest(http.MethodPost, "/ui/login", strings.NewReader("token="+url.QueryEscape(token)))
		login.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		login.Header.Set("Origin", "https://factory.example")
		login.Host = "factory.example"
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, login)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("non-configured-operator login token rejected with status=%d", response.Code)
		}
	}
	for _, path := range []string{"/ui/api/v1/operations", "/ui/api/v1/jobs/one/reconcile", "/ui/api/v1/jobs/one/events/anything", "/ui/api/v1/jobs/one?cursor=secret", "/ui/api/v1/schedules/one/history", "/ui/api/v1/schedules/one/runs/extra"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.AddCookie(&http.Cookie{Name: cookieName, Value: "forged"})
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusUnauthorized && recorder.Code != http.StatusNotFound {
			t.Errorf("untrusted proxy path %s status=%d", path, recorder.Code)
		}
	}
	for _, tc := range []struct {
		method string
		path   string
		query  string
	}{{http.MethodPost, "/ui/api/v1/schedules", ""}, {http.MethodGet, "/ui/api/v1/schedules/one/runs", "before_sequence=2"}} {
		path := tc.path
		if tc.query != "" {
			path += "?" + tc.query
		}
		request := httptest.NewRequest(tc.method, path, nil)
		request.AddCookie(&http.Cookie{Name: cookieName, Value: "forged"})
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusUnauthorized && recorder.Code != http.StatusNotFound {
			t.Errorf("restricted proxy route %s %s status=%d", tc.method, tc.path, recorder.Code)
		}
	}
}
