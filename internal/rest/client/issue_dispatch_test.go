package restclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/leehosanganson/factory/internal/rest/jobs"
)

func TestSubmitIssueSendsTaskAndBoundedProvenance(t *testing.T) {
	const token = "issue-dispatch-token"
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+token {
			t.Errorf("authorization = %q", got)
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/jobs" || r.Header.Get("Idempotency-Key") == "" {
			t.Errorf("request = %s %s headers=%v", r.Method, r.URL.Path, r.Header)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		captured = body
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"job":{"id":"job-1","status":"queued"},"replayed":false,"links":{"self":"/v1/jobs/job-1","history":"/v1/jobs/job-1/history"}}`))
	}))
	defer server.Close()

	client := New(Config{BaseURL: server.URL, Token: token})
	task := "Implement the request from the issue body."
	provenance := restjobs.IssueProvenance{Tracker: "github", SourceRepository: "acme/widget", IssueNumber: 42, Version: strings.Repeat("a", 64), URL: "https://github.com/acme/widget/issues/42", Title: "Safe title"}
	submission, err := client.SubmitIssue(context.Background(), "destination", task, 42, provenance, "stable-issue-key")
	if err != nil {
		t.Fatalf("SubmitIssue: %v", err)
	}
	if submission.Job.ID != "job-1" || submission.Job.Status != restjobs.StatusQueued {
		t.Errorf("submission = %+v, want job-1/queued", submission.Job)
	}
	if captured["repository"] != "destination" {
		t.Errorf("request repository = %v", captured["repository"])
	}
	if captured["task"] != task {
		t.Errorf("request task = %v, want exact visible task", captured["task"])
	}
	gotProvenance, ok := captured["issue_provenance"].(map[string]any)
	if !ok || gotProvenance["tracker"] != "github" || gotProvenance["source_repository"] != "acme/widget" || gotProvenance["version"] != provenance.Version || gotProvenance["url"] != provenance.URL || gotProvenance["title"] != "Safe title" {
		t.Errorf("request provenance = %#v", captured["issue_provenance"])
	}
	if _, containsBody := gotProvenance["body"]; containsBody {
		t.Errorf("provenance duplicated issue body: %#v", gotProvenance)
	}
	if issue, ok := captured["issue"].(float64); !ok || int(issue) != 42 {
		t.Errorf("request issue = %v (type %T), want 42", captured["issue"], captured["issue"])
	}
	if issueNumber, ok := gotProvenance["issue_number"].(float64); !ok || int(issueNumber) != 42 {
		t.Errorf("provenance issue_number = %v, want 42", gotProvenance["issue_number"])
	}
}

func TestSubmitIssueRejectsNonPositiveIssueNumber(t *testing.T) {
	client := New(Config{BaseURL: "http://127.0.0.1:0", Token: "unused"})
	provenance := restjobs.IssueProvenance{Tracker: "github", SourceRepository: "acme/widget", IssueNumber: 1, Version: strings.Repeat("a", 64), URL: "https://github.com/acme/widget/issues/1"}
	for _, n := range []int{0, -1} {
		if _, err := client.SubmitIssue(context.Background(), "destination", "task", n, provenance, "key"); err == nil {
			t.Errorf("issue %d accepted, want rejection", n)
		}
	}
}

func TestSubmitIssueErrorIsSanitized(t *testing.T) {
	const token = "issue-dispatch-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":"unauthenticated","message":"` + token + ` /private/issue/path"}`))
	}))
	defer server.Close()

	client := New(Config{BaseURL: server.URL, Token: token})
	provenance := restjobs.IssueProvenance{Tracker: "github", SourceRepository: "acme/widget", IssueNumber: 42, Version: strings.Repeat("a", 64), URL: "https://github.com/acme/widget/issues/42"}
	_, err := client.SubmitIssue(context.Background(), "destination", "task", 42, provenance, "key")
	if err == nil || strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "/private/issue/path") || !strings.Contains(err.Error(), "401") {
		t.Fatalf("sanitized error = %v", err)
	}
}
