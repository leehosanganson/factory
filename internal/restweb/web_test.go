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

	"github.com/leehosanganson/factory/internal/restapi"
	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restserver"
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

func TestDashboardAssetsAreEmbeddedAndHardened(t *testing.T) {
	handler := testWebHandler(t)
	for _, path := range []string{"/", "/ui/assets/dashboard.css", "/ui/assets/dashboard.js"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK || recorder.Body.Len() == 0 {
			t.Errorf("GET %s status=%d body=%q", path, recorder.Code, recorder.Body.String())
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
	for _, path := range []string{"/ui/api/v1/operations", "/ui/api/v1/jobs/one/reconcile", "/ui/api/v1/jobs/one/events/anything", "/ui/api/v1/jobs/one?cursor=secret"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.AddCookie(&http.Cookie{Name: cookieName, Value: "forged"})
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusUnauthorized && recorder.Code != http.StatusNotFound {
			t.Errorf("untrusted proxy path %s status=%d", path, recorder.Code)
		}
	}
}
