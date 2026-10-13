package restclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/leehosanganson/factory/internal/restjobs"
)

func TestSubmitIssueSendsIssueNumberAndRepository(t *testing.T) {
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
	submission, err := client.SubmitIssue(context.Background(), "acme/widget", 42, "stable-issue-key")
	if err != nil {
		t.Fatalf("SubmitIssue: %v", err)
	}
	if submission.Job.ID != "job-1" || submission.Job.Status != restjobs.StatusQueued {
		t.Errorf("submission = %+v, want job-1/queued", submission.Job)
	}
	if captured["repository"] != "acme/widget" {
		t.Errorf("request repository = %v", captured["repository"])
	}
	if issue, ok := captured["issue"].(float64); !ok || int(issue) != 42 {
		t.Errorf("request issue = %v (type %T), want 42", captured["issue"], captured["issue"])
	}
	if task, present := captured["task"]; !present || task != "" {
		t.Errorf("request must not carry a non-empty task field: %v", captured)
	}
}

func TestSubmitIssueRejectsNonPositiveIssueNumber(t *testing.T) {
	client := New(Config{BaseURL: "http://127.0.0.1:0", Token: "unused"})
	for _, n := range []int{0, -1} {
		if _, err := client.SubmitIssue(context.Background(), "acme/widget", n, "key"); err == nil {
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
	_, err := client.SubmitIssue(context.Background(), "acme/widget", 42, "key")
	if err == nil || strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "/private/issue/path") || !strings.Contains(err.Error(), "401") {
		t.Fatalf("sanitized error = %v", err)
	}
}
