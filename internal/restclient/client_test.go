package restclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
