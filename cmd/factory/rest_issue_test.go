package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRESTClientIssueCommandDispatchesIssue(t *testing.T) {
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer issue-cli-token" {
			t.Errorf("authorization = %q", got)
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/jobs" || r.Header.Get("Idempotency-Key") == "" {
			t.Errorf("request = %s %s key=%q", r.Method, r.URL.Path, r.Header.Get("Idempotency-Key"))
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		captured = body
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"job":{"id":"issue-job-1","status":"queued"},"replayed":false,"links":{"self":"/v1/jobs/issue-job-1","history":"/v1/jobs/issue-job-1/history"}}`))
	}))
	defer server.Close()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "rest-client.json")
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("issue-cli-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"base_url":"`+server.URL+`","token_file":"`+tokenPath+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	var out strings.Builder
	err := runRESTClient([]string{"issue", "--config", configPath, "--repository", "acme/widget", "--idempotency-key", "cli-issue-key", "42"}, &out)
	if err != nil {
		t.Fatalf("runRESTClient issue: %v", err)
	}
	if captured == nil {
		t.Fatal("server did not receive a submission")
	}
	if captured["repository"] != "acme/widget" {
		t.Errorf("request repository = %v", captured["repository"])
	}
	if issue, ok := captured["issue"].(float64); !ok || int(issue) != 42 {
		t.Errorf("request issue = %v (type %T), want 42", captured["issue"], captured["issue"])
	}
	if task, _ := captured["task"].(string); task != "" {
		t.Errorf("request must not carry a non-empty task field: %q", task)
	}
	if !strings.Contains(out.String(), "issue-job-1") || !strings.Contains(out.String(), "queued") {
		t.Errorf("output = %q, want job id and status", out.String())
	}
}

func TestRESTClientIssueCommandRejectsInvalidInput(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "rest-client.json")
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("unused"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"base_url":"http://127.0.0.1:0","token_file":"`+tokenPath+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	var out strings.Builder
	if err := runRESTClient([]string{"issue", "--config", configPath, "abc"}, &out); err == nil {
		t.Error("non-numeric issue accepted, want rejection")
	}
	out.Reset()
	if err := runRESTClient([]string{"issue", "--config", configPath, "--repository", "acme/widget", "0"}, &out); err == nil {
		t.Error("zero issue accepted, want rejection")
	}
	out.Reset()
	if err := runRESTClient([]string{"issue", "--config", configPath}, &out); err == nil {
		t.Error("missing repository/issue accepted, want usage error")
	}
}
