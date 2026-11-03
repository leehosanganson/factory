package restclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIssueProvenanceUsesScopedRouteAndDecodesOnlyProjection(t *testing.T) {
	const token = "provenance-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/jobs/job-42/issue-provenance" {
			t.Errorf("request = %s %s, want scoped provenance GET", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+token {
			t.Errorf("authorization = %q", got)
		}
		_, _ = w.Write([]byte(`{"job_id":"job-42","issue_provenance":{"tracker":"github","source_repository":"acme/widget","issue_number":42,"version":"` + strings.Repeat("a", 64) + `","url":"https://github.com/acme/widget/issues/42","title":"Bounded title"}}`))
	}))
	defer server.Close()

	result, err := New(Config{BaseURL: server.URL, Token: token}).IssueProvenance(context.Background(), "job-42")
	if err != nil {
		t.Fatal(err)
	}
	if result.JobID != "job-42" || result.Provenance.IssueNumber != 42 || result.Provenance.Title != "Bounded title" {
		t.Fatalf("provenance result = %+v", result)
	}
}

func TestListForPrincipalUsesOpaqueCursorAndDecodesSafePage(t *testing.T) {
	const token = "submitter-list-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/jobs" {
			t.Errorf("request = %s %s, want GET /v1/jobs", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+token {
			t.Errorf("authorization = %q", got)
		}
		if got := r.URL.Query().Get("limit"); got != "3" {
			t.Errorf("limit = %q, want 3", got)
		}
		if got := r.URL.Query().Get("cursor"); got != "opaque+/cursor=" {
			t.Errorf("cursor = %q, want opaque cursor unchanged", got)
		}
		if _, exists := r.URL.Query()["after"]; exists {
			t.Errorf("submitter query included operator after sequence: %v", r.URL.Query())
		}
		if _, exists := r.URL.Query()["snapshot"]; exists {
			t.Errorf("submitter query included operator snapshot sequence: %v", r.URL.Query())
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jobs":[{"id":"job-1","status":"queued","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}],"has_more":true,"next_cursor":"next opaque"}`))
	}))
	defer server.Close()

	page, err := New(Config{BaseURL: server.URL, Token: token}).ListForPrincipal(context.Background(), 3, "opaque+/cursor=")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 1 || page.Jobs[0].ID != "job-1" || page.Jobs[0].Status != "queued" || !page.HasMore || page.NextCursor != "next opaque" {
		t.Fatalf("submitter page = %+v", page)
	}
}

func TestListForPrincipalRejectsOperatorPaginationResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[],"snapshot_sequence":7,"has_more":false}`))
	}))
	defer server.Close()

	if _, err := New(Config{BaseURL: server.URL, Token: "token"}).ListForPrincipal(context.Background(), 0, ""); err == nil {
		t.Fatal("submitter client accepted operator pagination metadata or omitted next_cursor")
	}
}

func TestListForPrincipalFirstPageOmitsCursorAndUsesCollectionPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/jobs" || r.URL.RawQuery != "" {
			t.Errorf("first-page request = %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"jobs":[],"has_more":false,"next_cursor":""}`))
	}))
	defer server.Close()

	page, err := New(Config{BaseURL: server.URL, Token: "token"}).ListForPrincipal(context.Background(), 0, "")
	if err != nil || len(page.Jobs) != 0 || page.HasMore || page.NextCursor != "" {
		t.Fatalf("first submitter page = %+v, err=%v", page, err)
	}
}

func TestSubmitSendsBearerAndReturnsJobWithoutLeakingServerError(t *testing.T) {
	const token = "client-secret-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+token {
			t.Errorf("authorization = %q", got)
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/jobs" || r.Header.Get("Idempotency-Key") == "" {
			t.Errorf("request = %s %s headers=%v", r.Method, r.URL.Path, r.Header)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["task"] != "do the work" {
			t.Errorf("request body = %v", body)
		}
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":"unauthenticated","message":"` + token + ` /private/path"}`))
	}))
	defer server.Close()
	client := New(Config{BaseURL: server.URL, Token: token})
	_, err := client.Submit(context.Background(), "repo", "do the work", "stable-key")
	if err == nil || strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "/private/path") || !strings.Contains(err.Error(), "401") {
		t.Fatalf("sanitized error = %v", err)
	}
}
