package restweb

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/leehosanganson/factory/internal/api"
	"github.com/leehosanganson/factory/internal/jobs"
	"github.com/leehosanganson/factory/internal/server"
)

func testWebHandler(t *testing.T) http.Handler {
	t.Helper()
	manager, err := restjobs.NewManager(restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 4, MaxEventsPerJob: 8})
	if err != nil {
		t.Fatal(err)
	}
	return testWebHandlerWithManager(t, manager)
}

func testWebHandlerWithManager(t *testing.T, manager restjobs.Manager) http.Handler {
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
	api, err := restapi.New(manager, key, restapi.Config{MaxRequestBodyBytes: 4096, MaxTaskBytes: 2048, RepositoryAliases: map[string]struct{}{"widget": {}}, DashboardOperatorID: "admin", Ready: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	return New(api, "admin")
}

func TestDashboardScheduleCreateUsesSameOriginCSRFAndIdempotency(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := restjobs.OpenSQLiteStore(filepath.Join(dir, "dashboard.db"), restjobs.Config{QueueCapacity: 8, MaxConcurrentJobs: 2, MaxRecords: 16, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	handler := testWebHandlerWithManager(t, store)

	login := httptest.NewRequest(http.MethodPost, "/ui/login", strings.NewReader(url.Values{"token": {"web-test-token"}}.Encode()))
	login.Host = "factory.example"
	login.Header.Set("Origin", "https://factory.example")
	login.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	loginResponse := httptest.NewRecorder()
	handler.ServeHTTP(loginResponse, login)
	if loginResponse.Code != http.StatusOK {
		t.Fatalf("login status=%d body=%s", loginResponse.Code, loginResponse.Body.String())
	}
	cookie := loginResponse.Result().Cookies()[0]
	var sessionData struct {
		CSRF string `json:"csrf"`
	}
	if err := json.Unmarshal(loginResponse.Body.Bytes(), &sessionData); err != nil || sessionData.CSRF == "" {
		t.Fatalf("login response=%s err=%v", loginResponse.Body.String(), err)
	}

	create := func(key, task string, csrf, origin string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/ui/api/v1/schedules", strings.NewReader(`{"cron":"*/5 * * * *","repository":"widget","task":"`+task+`"}`))
		request.Host = "factory.example"
		request.Header.Set("Origin", origin)
		request.Header.Set("X-CSRF-Token", csrf)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", key)
		request.AddCookie(cookie)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	if rejected := create("schedule-no-csrf", "private scheduled task", "", "https://factory.example"); rejected.Code != http.StatusForbidden {
		t.Fatalf("missing CSRF status=%d body=%s", rejected.Code, rejected.Body.String())
	}
	if rejected := create("schedule-wrong-origin", "private scheduled task", sessionData.CSRF, "https://evil.example"); rejected.Code != http.StatusForbidden {
		t.Fatalf("wrong Origin status=%d body=%s", rejected.Code, rejected.Body.String())
	}
	first := create("dashboard-create-retry", "private scheduled task", sessionData.CSRF, "https://factory.example")
	if first.Code != http.StatusCreated || strings.Contains(first.Body.String(), "private scheduled task") || strings.Contains(first.Body.String(), `"task"`) || strings.Contains(first.Body.String(), `"repository"`) || strings.Contains(first.Body.String(), `"owner"`) {
		t.Fatalf("create status=%d body=%s", first.Code, first.Body.String())
	}
	var created struct {
		Schedule struct {
			ID string `json:"id"`
		} `json:"schedule"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &created); err != nil || created.Schedule.ID == "" {
		t.Fatalf("create response=%s err=%v", first.Body.String(), err)
	}
	stored, err := store.GetSchedule(created.Schedule.ID)
	if err != nil || stored.Owner != "admin" || stored.Task != "private scheduled task" {
		t.Fatalf("stored schedule=%+v err=%v", stored, err)
	}
	replay := create("dashboard-create-retry", "private scheduled task", sessionData.CSRF, "https://factory.example")
	if replay.Code != http.StatusOK || !strings.Contains(replay.Body.String(), created.Schedule.ID) || !strings.Contains(replay.Body.String(), `"replayed":true`) || strings.Contains(replay.Body.String(), "private scheduled task") {
		t.Fatalf("replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	for _, method := range []string{http.MethodPatch, http.MethodDelete} {
		request := httptest.NewRequest(method, "/ui/api/v1/schedules/"+created.Schedule.ID, nil)
		request.AddCookie(cookie)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Errorf("%s schedule status=%d body=%s", method, response.Code, response.Body.String())
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
		if path == "/" && (strings.Contains(recorder.Body.String(), "<script>") || strings.Contains(recorder.Body.String(), "<style") || strings.Contains(recorder.Body.String(), "style=")) {
			t.Error("dashboard entry page contains inline script or style content")
		}
		if path == "/ui/assets/dashboard.js" && (strings.Contains(recorder.Body.String(), "sourceMappingURL") || strings.Contains(recorder.Body.String(), "react-refresh") || strings.Contains(recorder.Body.String(), "vite/client")) {
			t.Error("dashboard JavaScript contains development or source-map output")
		}
		if path == "/ui/assets/dashboard.js" && recorder.Header().Get("Content-Type") != "text/javascript; charset=utf-8" {
			t.Errorf("dashboard JavaScript content type=%q", recorder.Header().Get("Content-Type"))
		}
		if path == "/ui/assets/dashboard.css" && recorder.Header().Get("Content-Type") != "text/css; charset=utf-8" {
			t.Errorf("dashboard CSS content type=%q", recorder.Header().Get("Content-Type"))
		}
		for _, header := range []string{"Content-Security-Policy", "X-Content-Type-Options", "X-Frame-Options", "Referrer-Policy", "Cache-Control"} {
			if recorder.Header().Get(header) == "" {
				t.Errorf("GET %s omitted %s", path, header)
			}
		}
		if recorder.Header().Get("Content-Security-Policy") != "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'" {
			t.Errorf("GET %s content security policy=%q", path, recorder.Header().Get("Content-Security-Policy"))
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
	appendRequest := httptest.NewRequest(http.MethodPost, "/ui/api/v1/threads/"+created.Thread.ID+"/jobs", strings.NewReader(`{"repository":"widget","task":"private follow-up task"}`))
	appendRequest.Host = "factory.example"
	appendRequest.Header.Set("Origin", "https://factory.example")
	appendRequest.Header.Set("Content-Type", "application/json")
	appendRequest.Header.Set("Idempotency-Key", "web-thread-append")
	appendRequest.Header.Set("X-CSRF-Token", sessionData.CSRF)
	appendRequest.AddCookie(cookie)
	appendResponse := httptest.NewRecorder()
	handler.ServeHTTP(appendResponse, appendRequest)
	if appendResponse.Code != http.StatusAccepted || strings.Contains(appendResponse.Body.String(), "private follow-up task") || strings.Contains(appendResponse.Body.String(), `"request"`) || strings.Contains(appendResponse.Body.String(), `"provider"`) {
		t.Fatalf("Thread append status=%d body=%s", appendResponse.Code, appendResponse.Body.String())
	}
	var appended struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
		Job struct {
			ID string `json:"id"`
		} `json:"job"`
		Replayed bool `json:"replayed"`
	}
	if err := json.Unmarshal(appendResponse.Body.Bytes(), &appended); err != nil || appended.Thread.ID != created.Thread.ID || appended.Job.ID == "" || appended.Replayed {
		t.Fatalf("Thread append response=%s err=%v", appendResponse.Body.String(), err)
	}
	appendRetry := httptest.NewRequest(http.MethodPost, "/ui/api/v1/threads/"+created.Thread.ID+"/jobs", strings.NewReader(`{"repository":"widget","task":"private follow-up task"}`))
	appendRetry.Host = "factory.example"
	appendRetry.Header = appendRequest.Header.Clone()
	appendRetry.AddCookie(cookie)
	appendRetryResponse := httptest.NewRecorder()
	handler.ServeHTTP(appendRetryResponse, appendRetry)
	if appendRetryResponse.Code != http.StatusAccepted || !strings.Contains(appendRetryResponse.Body.String(), appended.Job.ID) || !strings.Contains(appendRetryResponse.Body.String(), `"replayed":true`) {
		t.Fatalf("Thread append retry status=%d body=%s", appendRetryResponse.Code, appendRetryResponse.Body.String())
	}
	completeRequest := httptest.NewRequest(http.MethodPost, "/ui/api/v1/threads/"+created.Thread.ID+"/complete", nil)
	completeRequest.Host = "factory.example"
	completeRequest.Header.Set("Origin", "https://factory.example")
	completeRequest.Header.Set("X-CSRF-Token", sessionData.CSRF)
	completeRequest.AddCookie(cookie)
	completeResponse := httptest.NewRecorder()
	handler.ServeHTTP(completeResponse, completeRequest)
	if completeResponse.Code != http.StatusOK || strings.Contains(completeResponse.Body.String(), `"completed_by"`) || strings.Contains(completeResponse.Body.String(), `"provider"`) || !strings.Contains(completeResponse.Body.String(), `"completed_at"`) {
		t.Fatalf("Thread completion status=%d body=%s", completeResponse.Code, completeResponse.Body.String())
	}
	closedAppend := httptest.NewRequest(http.MethodPost, "/ui/api/v1/threads/"+created.Thread.ID+"/jobs", strings.NewReader(`{"repository":"widget","task":"after close"}`))
	closedAppend.Host = appendRequest.Host
	closedAppend.Header = appendRequest.Header.Clone()
	closedAppend.Header.Set("Idempotency-Key", "web-thread-closed-append")
	closedAppend.AddCookie(cookie)
	closedResponse := httptest.NewRecorder()
	handler.ServeHTTP(closedResponse, closedAppend)
	if closedResponse.Code != http.StatusConflict || !strings.Contains(closedResponse.Body.String(), "thread_closed") {
		t.Fatalf("closed Thread append status=%d body=%s", closedResponse.Code, closedResponse.Body.String())
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
	}{{http.MethodGet, "/ui/api/v1/schedules", http.StatusServiceUnavailable}, {http.MethodGet, "/ui/api/v1/schedules/example/runs?limit=5", http.StatusServiceUnavailable}, {http.MethodPost, "/ui/api/v1/schedules", http.StatusServiceUnavailable}, {http.MethodPatch, "/ui/api/v1/schedules/example", http.StatusNotFound}, {http.MethodGet, "/ui/api/v1/schedules/example/history", http.StatusNotFound}, {http.MethodGet, "/ui/api/v1/threads", http.StatusNotFound}, {http.MethodGet, "/ui/api/v1/threads/id/jobs", http.StatusNotFound}, {http.MethodGet, "/ui/api/v1/threads/id/complete", http.StatusNotFound}, {http.MethodPost, "/ui/api/v1/threads/id/extra", http.StatusNotFound}, {http.MethodPost, "/ui/api/v1/threads/id/jobs?key=x", http.StatusBadRequest}, {http.MethodPost, "/ui/api/v1/threads/id/complete?key=x", http.StatusBadRequest}, {http.MethodPost, "/ui/api/v1/threads?foo=bar", http.StatusBadRequest}, {http.MethodGet, "/ui/api/v1/threads/id?foo=bar", http.StatusNotFound}} {
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
	}{{http.MethodPatch, "/ui/api/v1/schedules/one", ""}, {http.MethodDelete, "/ui/api/v1/schedules/one", ""}, {http.MethodGet, "/ui/api/v1/schedules/one/runs", "before_sequence=2"}} {
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
