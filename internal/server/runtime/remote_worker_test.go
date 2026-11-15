package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
)

func TestSelectRemoteHarnessIsStableAndCapabilityAware(t *testing.T) {
	config := restserver.RemoteWorkerConfig{HarnessProfiles: map[string]restserver.RemoteHarnessConfig{
		"zeta":  {Executable: "z", Repositories: []string{"widget"}},
		"alpha": {Executable: "a", Repositories: []string{"widget"}},
		"other": {Executable: "o", Repositories: []string{"other"}},
	}}
	for _, jobID := range []string{"job-1", "job-2", "job-3"} {
		first, profile, err := SelectRemoteHarness(config, jobID, "widget")
		if err != nil {
			t.Fatal(err)
		}
		second, again, err := SelectRemoteHarness(config, jobID, "widget")
		if err != nil {
			t.Fatal(err)
		}
		if first != second || !reflect.DeepEqual(profile, again) || (first != "alpha" && first != "zeta") {
			t.Fatalf("selection %q = %q/%+v, then %q/%+v", jobID, first, profile, second, again)
		}
	}
	if _, _, err := SelectRemoteHarness(config, "job-1", "missing"); err == nil {
		t.Fatal("selected an incompatible harness")
	}
}

func TestRemoteWorkerClientAuthDTOAndIdempotentRetry(t *testing.T) {
	const secret = "0123456789abcdef0123456789abcdef"
	var claims []map[string]any
	claimReply := `{"job":{"id":"00000000-0000-4000-8000-000000000001","request":{"repository":"widget","task":"test"}},"attempt":{"job_id":"00000000-0000-4000-8000-000000000001","generation":2,"owner_token":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","worker_id":"worker-a","boot_id":"boot-a"}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+secret {
			t.Errorf("authorization=%q", got)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if _, exists := payload["harness"]; exists {
			t.Error("Controller request selected a harness")
		}
		if repositories, ok := payload["repositories"].([]any); !ok || !reflect.DeepEqual(repositories, []any{"widget"}) {
			t.Errorf("claim repository capabilities=%#v, want only configured alias widget", payload["repositories"])
		}
		claims = append(claims, payload)
		if len(claims) == 1 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Fatal("HTTP server does not support hijacking")
			}
			connection, _, err := hijacker.Hijack()
			if err != nil {
				t.Fatalf("hijack ambiguous claim response: %v", err)
			}
			_ = connection.Close()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(claimReply))
	}))
	defer server.Close()
	config := restserver.RemoteWorkerConfig{ControllerBaseURL: server.URL, SharedSecretFile: "/run/worker-secret", WorkerID: "worker-a", Repositories: map[string]string{"widget": "/srv/widget"}, HarnessProfiles: map[string]restserver.RemoteHarnessConfig{"profile": {Executable: "pi", Args: []string{"{system_prompt}", "{task}"}, Repositories: []string{"widget"}}}, Limits: restserver.RemoteWorkerLimits{JobTimeout: "1m", HarnessOutputBytes: 1024}}
	client, err := newRemoteWorkerClient(config, []byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	client.bootID = "boot-a"
	first, err := client.claim(context.Background(), "claim-stable")
	var retryable remoteWorkerRetryableError
	if !errors.As(err, &retryable) || first != nil {
		t.Fatalf("ambiguous first response claim=%+v err=%v, want retryable transport error", first, err)
	}
	second, err := client.claim(context.Background(), "claim-stable")
	if err != nil || second == nil {
		t.Fatalf("retry claim=%+v err=%v", second, err)
	}
	if len(claims) != 2 || claims[0]["claim_id"] != "claim-stable" || claims[1]["claim_id"] != "claim-stable" {
		t.Fatalf("claim IDs=%v", claims)
	}
	if !reflect.DeepEqual(claims[0]["repositories"], claims[1]["repositories"]) {
		t.Fatalf("claim retry changed repository capability set: %v vs %v", claims[0]["repositories"], claims[1]["repositories"])
	}
}

func TestRemoteWorkerDoesNotSendCredentialsToRedirects(t *testing.T) {
	const secret = "0123456789abcdef0123456789abcdef"
	var targetCalls int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls++
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("redirect target received authorization header %q", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+secret {
			t.Errorf("initial request authorization=%q", got)
		}
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	config := restserver.RemoteWorkerConfig{ControllerBaseURL: redirect.URL, SharedSecretFile: "/run/worker-secret", WorkerID: "worker-a", Repositories: map[string]string{"widget": "/srv/widget"}, HarnessProfiles: map[string]restserver.RemoteHarnessConfig{"profile": {Executable: "pi", Args: []string{"{system_prompt}", "{task}"}, Repositories: []string{"widget"}}}, Limits: restserver.RemoteWorkerLimits{JobTimeout: "1m", HarnessOutputBytes: 1024}}
	client, err := newRemoteWorkerClient(config, []byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	defer client.client.CloseIdleConnections()
	_, err = client.claim(context.Background(), "claim-redirect")
	if err == nil || targetCalls != 0 {
		t.Fatalf("redirect request error=%v target calls=%d, want redirect rejected without forwarding credentials", err, targetCalls)
	}
}

func TestRemoteWorkerHeartbeatErrorsAreNotCancellation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		body       string
		wantErr    bool
		wantCancel bool
	}{
		{"explicit cancellation", http.StatusOK, `{"cancellation_requested":true}`, false, true},
		{"transport server failure", http.StatusServiceUnavailable, `{"error":{"code":"service_unavailable"}}`, true, false},
		{"malformed response", http.StatusOK, `{}`, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := &remoteWorkerClient{baseURL: server.URL, secret: "secret", client: server.Client()}
			cancel, err := client.heartbeat(context.Background(), remoteWorkerAttempt{JobID: "job", Generation: 1, OwnerToken: "token", WorkerID: "w", BootID: "b"})
			if (err != nil) != tc.wantErr || cancel != tc.wantCancel {
				t.Fatalf("heartbeat=(%v,%v), want (%v,%v)", cancel, err, tc.wantCancel, tc.wantErr)
			}
		})
	}
}

func TestRemoteWorkerDoesNotRewriteSuccessAfterCancellationConflict(t *testing.T) {
	var finishes []map[string]any
	heartbeats := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/finish"):
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode finish: %v", err)
			}
			finishes = append(finishes, payload)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":{"code":"cancellation_pending","message":"pending"}}`))
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			heartbeats++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"cancellation_requested":true}`))
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client := &remoteWorkerClient{baseURL: server.URL, secret: "secret", client: server.Client()}
	attempt := remoteWorkerAttempt{JobID: "00000000-0000-4000-8000-000000000001", Generation: 1, OwnerToken: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", WorkerID: "worker-a", BootID: "boot-a"}
	outcome := remoteWorkerFinish{remoteWorkerAttemptRequest: remoteWorkerAttemptRequest{Generation: 1, OwnerToken: attempt.OwnerToken, WorkerID: attempt.WorkerID, BootID: attempt.BootID}, Status: restjobs.StatusSucceeded, Evidence: remoteWorkerEvidence{Checks: structCheck(), Limitations: []string{restjobs.LimitationAgentNotVerdict}}}
	err := retryRemoteFinishAfterStop(context.Background(), client, attempt, outcome, true)
	if err == nil || heartbeats != 1 || len(finishes) != 1 || finishes[0]["status"] != string(restjobs.StatusSucceeded) {
		t.Fatalf("finish error=%v heartbeats=%d finish requests=%#v; want inspectable conflict without rewriting success", err, heartbeats, finishes)
	}
}

func TestRemoteWorkerCanceledResultRetriesOnlyAfterExplicitCancellationObservation(t *testing.T) {
	var finishes []map[string]any
	heartbeats := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/finish"):
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode finish: %v", err)
			}
			finishes = append(finishes, payload)
			if len(finishes) == 1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":{"code":"cancellation_not_requested","message":"not requested"}}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"finished"}`))
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			heartbeats++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"cancellation_requested":true}`))
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client := &remoteWorkerClient{baseURL: server.URL, secret: "secret", client: server.Client()}
	attempt := remoteWorkerAttempt{JobID: "00000000-0000-4000-8000-000000000001", Generation: 1, OwnerToken: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", WorkerID: "worker-a", BootID: "boot-a"}
	outcome := remoteWorkerFinish{remoteWorkerAttemptRequest: remoteWorkerAttemptRequest{Generation: 1, OwnerToken: attempt.OwnerToken, WorkerID: attempt.WorkerID, BootID: attempt.BootID}, Status: restjobs.StatusCanceled, Evidence: remoteWorkerEvidence{Checks: []restjobs.VerificationCheck{{Name: "check-01", Outcome: restjobs.VerificationCanceled}}, Limitations: []string{restjobs.LimitationAgentNotVerdict}}}
	if err := retryRemoteFinishAfterStop(context.Background(), client, attempt, outcome, true); err != nil {
		t.Fatal(err)
	}
	if heartbeats != 1 || len(finishes) != 2 || finishes[0]["status"] != string(restjobs.StatusCanceled) || finishes[1]["status"] != string(restjobs.StatusCanceled) {
		t.Fatalf("heartbeats=%d finish requests=%#v, want observed cancellation followed by canceled result", heartbeats, finishes)
	}
}

func TestRemoteWorkerFinishRetrySendsIdenticalOutcome(t *testing.T) {
	const secret = "0123456789abcdef0123456789abcdef"
	var payloads []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+secret {
			t.Errorf("authorization=%q", got)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode finish: %v", err)
		}
		payloads = append(payloads, payload)
		if len(payloads) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"finished"}`))
	}))
	defer server.Close()
	client := &remoteWorkerClient{baseURL: server.URL, secret: secret, client: server.Client()}
	attempt := remoteWorkerAttempt{JobID: "00000000-0000-4000-8000-000000000001", Generation: 1, OwnerToken: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", WorkerID: "worker-a", BootID: "boot-a"}
	outcome := remoteWorkerFinish{remoteWorkerAttemptRequest: remoteWorkerAttemptRequest{Generation: 1, OwnerToken: attempt.OwnerToken, WorkerID: attempt.WorkerID, BootID: attempt.BootID}, Status: restjobs.StatusFailed, Evidence: remoteWorkerEvidence{Checks: structCheck(), Limitations: []string{"Agent-reported completion is not an independent correctness verdict."}}}
	if err := retryRemoteFinish(context.Background(), client, attempt, outcome); err != nil {
		t.Fatal(err)
	}
	if len(payloads) != 2 || !reflect.DeepEqual(payloads[0], payloads[1]) {
		t.Fatalf("finish retries differ: %#v", payloads)
	}
}

func structCheck() []restjobs.VerificationCheck {
	return []restjobs.VerificationCheck{{Name: "check-01", Outcome: restjobs.VerificationNotCompleted}}
}

func TestRemoteHeartbeatMonitorSeparatesCancellationAndTransportFailure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		body       string
		wantCancel bool
		wantErr    bool
	}{
		{"explicit cancel", http.StatusOK, `{"cancellation_requested":true}`, true, false},
		{"controller error", http.StatusServiceUnavailable, `{}`, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := &remoteWorkerClient{baseURL: server.URL, secret: "secret", client: server.Client()}
			ctx, cancel := context.WithCancel(context.Background())
			result := <-monitorRemoteAttempt(ctx, cancel, client, remoteWorkerAttempt{JobID: "job", Generation: 1, OwnerToken: "token", WorkerID: "w", BootID: "b"}, time.Hour)
			if result.cancellationRequested != tc.wantCancel || (result.err != nil) != tc.wantErr || ctx.Err() == nil {
				t.Fatalf("monitor result=%+v ctx err=%v", result, ctx.Err())
			}
		})
	}
}

func TestRemoteWorkerTransportErrorIsRetryableButRejectedRequestIsNot(t *testing.T) {
	rejected := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	defer rejected.Close()
	client := &remoteWorkerClient{baseURL: rejected.URL, secret: "secret", client: rejected.Client()}
	_, rejectedErr := client.claim(context.Background(), "claim")
	var rejectedRetryable remoteWorkerRetryableError
	if errors.As(rejectedErr, &rejectedRetryable) {
		t.Fatalf("auth rejection should not be retried: %v", rejectedErr)
	}
	client = &remoteWorkerClient{baseURL: "http://127.0.0.1:1", secret: "secret", client: &http.Client{}}
	_, err := client.claim(context.Background(), "claim")
	var retryable remoteWorkerRetryableError
	if !errors.As(err, &retryable) {
		t.Fatalf("transport error=%v, want retryable", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("error exposed secret: %v", err)
	}
}
