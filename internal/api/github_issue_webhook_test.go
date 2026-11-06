package restapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/leehosanganson/factory/internal/jobs"
	"github.com/leehosanganson/factory/internal/server"
)

const webhookTestSecret = "test-only-github-webhook-secret-over-32-bytes"

func configuredGitHubWebhook(t *testing.T, manager restjobs.Manager, maxBody, maxTask int) *Handler {
	t.Helper()
	key, err := restserver.LoadAPIKey(writeAPIKey(t, testAPIKey))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(manager, key, Config{
		MaxRequestBodyBytes: maxBody,
		MaxTaskBytes:        maxTask,
		RepositoryAliases:   map[string]struct{}{"widget": {}},
		Ready:               func() bool { return true },
		ReadyError:          func(context.Context) error { return nil },
		GitHubWebhook: GitHubWebhookConfig{
			Secret:       []byte(webhookTestSecret),
			TriggerLabel: "factory:ready",
			Sources:      map[string]string{"acme/issues": "widget"},
			Owners:       map[string]string{"acme/issues": "issue-owner"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func newSQLiteWebhookHandler(t *testing.T, maxBody, maxTask int) (*restjobs.SQLiteStore, *Handler) {
	t.Helper()
	store, err := restjobs.OpenSQLiteStore(filepath.Join(webhookPrivateDir(t), "webhook.db"), restjobs.Config{QueueCapacity: 8, MaxConcurrentJobs: 2, MaxRecords: 16, MaxEventsPerJob: 8, MaxTaskBytes: maxTask})
	if err != nil {
		t.Fatal(err)
	}
	return store, configuredGitHubWebhook(t, store, maxBody, maxTask)
}

func webhookPrivateDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func githubWebhookRequest(handler http.Handler, body, delivery, event string, signed bool) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, githubIssueWebhookPath, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-GitHub-Delivery", delivery)
	request.Header.Set("X-GitHub-Event", event)
	if signed {
		mac := hmac.New(sha256.New, []byte(webhookTestSecret))
		_, _ = mac.Write([]byte(body))
		request.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	} else {
		request.Header.Set("X-Hub-Signature-256", "sha256="+strings.Repeat("0", sha256.Size*2))
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestGitHubIssueWebhookAdmissionOnSQLiteFailsWithoutPartialRows(t *testing.T) {
	manager, handler := newSQLiteWebhookHandler(t, 4096, 2048)
	defer manager.CloseStore()
	body := `{"action":"labeled","label":{"name":"factory:ready"},"issue":{"number":42,"title":"Fix issue","body":"Do work","state":"open","labels":[{"name":"factory:ready"}]},"repository":{"full_name":"acme/issues"}}`
	response := githubWebhookRequest(handler, body, "sqlite-delivery", "issues", true)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "webhook_unavailable") {
		t.Fatalf("SQLite webhook response=%d %s, want unavailable", response.Code, response.Body.String())
	}
	jobs, err := manager.ListJobs(context.Background(), 0, 0, 20)
	if err != nil || len(jobs.Jobs) != 0 {
		t.Fatalf("SQLite API webhook admission left work: jobs=%+v err=%v", jobs, err)
	}
}

func TestGitHubIssueWebhookAuthenticatesRawBytesHeadersAndBodyLimit(t *testing.T) {
	manager, handler := newSQLiteWebhookHandler(t, 200, 128)
	defer manager.CloseStore()
	body := `{"action":"labeled","label":{"name":"factory:ready"},"issue":{"number":42,"title":"Fix","body":"","state":"open","labels":[{"name":"factory:ready"}]},"repository":{"full_name":"acme/issues"}}`
	if response := githubWebhookRequest(handler, body, "bad-signature", "issues", false); response.Code != http.StatusUnauthorized {
		t.Fatalf("invalid signature status=%d body=%s", response.Code, response.Body.String())
	}
	for name, mutate := range map[string]func(*http.Request){
		"event":               func(r *http.Request) { r.Header.Set("X-GitHub-Event", "issue_comment") },
		"missing delivery":    func(r *http.Request) { r.Header.Del("X-GitHub-Delivery") },
		"duplicate signature": func(r *http.Request) { r.Header.Add("X-Hub-Signature-256", r.Header.Get("X-Hub-Signature-256")) },
	} {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, githubIssueWebhookPath, strings.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("X-GitHub-Delivery", "bad-header-"+name)
			r.Header.Set("X-GitHub-Event", "issues")
			mac := hmac.New(sha256.New, []byte(webhookTestSecret))
			_, _ = mac.Write([]byte(body))
			r.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
			mutate(r)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, r)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("invalid header status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	oversized := `{"action":"labeled","issue":{"number":42,"title":"` + strings.Repeat("x", 300) + `"},"repository":{"full_name":"acme/issues"}}`
	if response := githubWebhookRequest(handler, oversized, "oversized", "issues", true); response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body status=%d body=%s", response.Code, response.Body.String())
	}
	for _, body := range []string{
		`{"action":"labeled","action":"opened","issue":{"number":42},"repository":{"full_name":"acme/issues"}}`,
		`{"action":"labeled","issue":{"number":42,"title":"x","labels":[{"name":"x","name":"factory:ready"}]},"repository":{"full_name":"acme/issues"}}`,
		`{"action":"labeled"} {}`,
		`{"action":`,
	} {
		if response := githubWebhookRequest(handler, body, "malformed-"+string(rune('a'+len(body)%20)), "issues", true); response.Code != http.StatusBadRequest {
			t.Fatalf("malformed/ambiguous body status=%d body=%s payload=%s", response.Code, response.Body.String(), body)
		}
	}
	if response := githubWebhookRequest(handler, strings.Replace(body, `"factory:ready"`, `"Factory:Ready"`, 1), "upper-label", "issues", true); response.Code != http.StatusAccepted || !bytes.Contains(response.Body.Bytes(), []byte("ignored")) {
		t.Fatalf("non-exact trigger label was not ignored: %d %s", response.Code, response.Body.String())
	}
}

func TestGitHubIssueWebhookIgnoresWrongActionLabelSourceClosedAndPullRequest(t *testing.T) {
	manager, handler := newSQLiteWebhookHandler(t, 4096, 2048)
	defer manager.CloseStore()
	base := `{"action":"%s","label":{"name":"%s"},"issue":{"number":42,"title":"Issue","body":"body","state":"%s","labels":[{"name":"%s"}]%s},"repository":{"full_name":"%s"}}`
	for index, body := range []string{
		fmtWebhook(base, "opened", "factory:ready", "open", "factory:ready", "", "acme/issues"),
		fmtWebhook(base, "labeled", "other", "open", "other", "", "acme/issues"),
		fmtWebhook(base, "labeled", "factory:ready", "open", "factory:ready", "", "foreign/repo"),
		fmtWebhook(base, "labeled", "factory:ready", "closed", "factory:ready", "", "acme/issues"),
		fmtWebhook(base, "labeled", "factory:ready", "open", "factory:ready", `,"pull_request":{}`, "acme/issues"),
	} {
		response := githubWebhookRequest(handler, body, "ignored-"+string(rune('a'+index)), "issues", true)
		if response.Code != http.StatusAccepted || !bytes.Contains(response.Body.Bytes(), []byte("ignored")) {
			t.Errorf("ineligible event admission=%d %s, want ignored", response.Code, response.Body.String())
		}
	}
	jobs, err := manager.ListJobs(context.Background(), 0, 0, 20)
	if err != nil || len(jobs.Jobs) != 0 {
		t.Fatalf("ineligible event admitted work: %+v %v", jobs, err)
	}
}

func fmtWebhook(format, action, eventLabel, state, issueLabel, extra, source string) string {
	return fmt.Sprintf(format, action, eventLabel, state, issueLabel, extra, source)
}

func TestGitHubIssueWebhookOnSQLiteDoesNotPersistDeliveryOrAdmitWork(t *testing.T) {
	manager, handler := newSQLiteWebhookHandler(t, 4096, 2048)
	defer manager.CloseStore()
	body := `{"action":"labeled","label":{"name":"factory:ready"},"issue":{"number":42,"title":"Fix","state":"open","labels":[{"name":"factory:ready"}]},"repository":{"full_name":"acme/issues"}}`
	for _, delivery := range []string{"stable-delivery", "retry-delivery"} {
		response := githubWebhookRequest(handler, body, delivery, "issues", true)
		if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "webhook_unavailable") {
			t.Fatalf("SQLite webhook status=%d body=%s, want unavailable", response.Code, response.Body.String())
		}
	}
	jobs, err := manager.ListJobs(context.Background(), 0, 0, 20)
	if err != nil || len(jobs.Jobs) != 0 {
		t.Fatalf("SQLite webhook admitted jobs: %+v %v", jobs, err)
	}
}
