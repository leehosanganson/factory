package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
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

func TestRemoteWorkerCanceledResultDoesNotRetryWhenCancellationIsNotRequested(t *testing.T) {
	for _, tc := range []struct {
		name              string
		heartbeatStatus   int
		heartbeatBody     string
		wantErrorContains string
	}{
		{
			name:              "cancellation not requested",
			heartbeatStatus:   http.StatusOK,
			heartbeatBody:     `{"cancellation_requested":false}`,
			wantErrorContains: "finish was rejected",
		},
		{
			name:              "malformed heartbeat",
			heartbeatStatus:   http.StatusOK,
			heartbeatBody:     `{}`,
			wantErrorContains: "heartbeat failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
					_, _ = w.Write([]byte(`{"error":{"code":"cancellation_not_requested","message":"not requested"}}`))
				case strings.HasSuffix(r.URL.Path, "/heartbeat"):
					heartbeats++
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(tc.heartbeatStatus)
					_, _ = w.Write([]byte(tc.heartbeatBody))
				default:
					t.Errorf("unexpected request path %q", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			client := &remoteWorkerClient{baseURL: server.URL, secret: "secret", client: server.Client()}
			attempt := remoteWorkerAttempt{JobID: "00000000-0000-4000-8000-000000000001", Generation: 1, OwnerToken: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", WorkerID: "worker-a", BootID: "boot-a"}
			outcome := remoteWorkerFinish{remoteWorkerAttemptRequest: remoteWorkerAttemptRequest{Generation: 1, OwnerToken: attempt.OwnerToken, WorkerID: attempt.WorkerID, BootID: attempt.BootID}, Status: restjobs.StatusCanceled, Evidence: remoteWorkerEvidence{Checks: []restjobs.VerificationCheck{{Name: "check-01", Outcome: restjobs.VerificationCanceled}}, Limitations: []string{restjobs.LimitationAgentNotVerdict}}}

			err := retryRemoteFinishAfterStop(context.Background(), client, attempt, outcome, true)
			if err == nil || !strings.Contains(err.Error(), tc.wantErrorContains) || !strings.Contains(err.Error(), "inspectable") {
				t.Fatalf("finish error=%v, want inspectable error containing %q", err, tc.wantErrorContains)
			}
			if heartbeats != 1 || len(finishes) != 1 || finishes[0]["status"] != string(restjobs.StatusCanceled) {
				t.Fatalf("heartbeats=%d finish requests=%#v, want one heartbeat and one canceled finish attempt without acknowledgment or retry", heartbeats, finishes)
			}
		})
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

func TestRemoteWorkerCompletesControllerClaimWithLocalHarnessAndChecks(t *testing.T) {
	const (
		secret     = "fedcba9876543210fedcba9876543210"
		jobID      = "00000000-0000-4000-8000-000000000001"
		ownerToken = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		workerID   = "worker-a"
		generation = int64(3)
	)
	root := t.TempDir()
	initE2ERepository(t, root)
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)

	harnessCalls := filepath.Join(t.TempDir(), "harness-calls")
	harness := filepath.Join(t.TempDir(), "fake-harness.sh")
	harnessScript := "#!/bin/sh\nprintf '%s\\n' \"$PWD\" >> \"" + harnessCalls + "\"\nprintf 'PASS\\n'\n"
	if err := os.WriteFile(harness, []byte(harnessScript), 0o700); err != nil {
		t.Fatal(err)
	}
	checkMarker := filepath.Join(t.TempDir(), "verification-check-ran")
	check := filepath.Join(t.TempDir(), "verification-check.sh")
	checkScript := "#!/bin/sh\ntest -f README.md && pwd > \"" + checkMarker + "\"\n"
	if err := os.WriteFile(check, []byte(checkScript), 0o700); err != nil {
		t.Fatal(err)
	}
	secretFile := filepath.Join(t.TempDir(), "worker-secret")
	if err := os.WriteFile(secretFile, []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}

	finishReceived := make(chan map[string]any, 1)
	polledAfterFinish := make(chan struct{}, 1)
	var requestMu sync.Mutex
	var claimCount int
	var finishCount int
	var claimID string
	var bootID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+secret {
			t.Errorf("authorization=%q, want configured Worker secret", got)
		}
		switch {
		case r.URL.Path == "/v1/worker/claims":
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode claim: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			requestMu.Lock()
			claimCount++
			if got, ok := payload["claim_id"].(string); !ok || len(got) != 32 {
				t.Errorf("claim ID=%#v, want a generated 32-character ID", payload["claim_id"])
			} else if claimID == "" {
				claimID = got
			}
			if got := payload["worker_id"]; got != workerID {
				t.Errorf("claim worker_id=%#v, want %q", got, workerID)
			}
			if got, ok := payload["boot_id"].(string); !ok || got == "" {
				t.Errorf("claim boot_id=%#v, want generated identity", got)
			} else if bootID == "" {
				bootID = got
			} else if got != bootID {
				t.Errorf("claim boot_id changed: got %q, want %q", got, bootID)
			}
			if !reflect.DeepEqual(payload["repositories"], []any{"widget"}) {
				t.Errorf("claim repositories=%#v, want only the locally supported alias widget", payload["repositories"])
			}
			if _, exists := payload["harness"]; exists {
				t.Errorf("Controller claim request chose a harness: %#v", payload["harness"])
			}
			if len(payload) != 4 {
				t.Errorf("claim fields=%#v, want only claim identity and repository capabilities", payload)
			}
			firstClaim := claimCount == 1
			finished := finishCount > 0
			claimBootID := bootID
			requestMu.Unlock()
			if finished {
				select {
				case polledAfterFinish <- struct{}{}:
				default:
				}
			}
			if !firstClaim {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"job":{"id":"` + jobID + `","request":{"repository":"widget","task":"complete the assigned work"}},"attempt":{"job_id":"` + jobID + `","generation":3,"owner_token":"` + ownerToken + `","worker_id":"` + workerID + `","boot_id":"` + claimBootID + `"}}`))
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			requestMu.Lock()
			expectedBootID := bootID
			requestMu.Unlock()
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode heartbeat: %v", err)
			}
			if payload["generation"] != float64(generation) || payload["owner_token"] != ownerToken || payload["worker_id"] != workerID || payload["boot_id"] != expectedBootID {
				t.Errorf("heartbeat identity=%#v, want claim identity", payload)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"cancellation_requested":false}`))
		case strings.HasSuffix(r.URL.Path, "/finish"):
			requestMu.Lock()
			finishCount++
			requestMu.Unlock()
			if got, want := r.URL.Path, remoteAttemptPath(jobID, "finish"); got != want {
				t.Errorf("finish path=%q, want %q", got, want)
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read finish: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var payload map[string]any
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Errorf("decode finish: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			requestMu.Lock()
			expectedBootID := bootID
			requestMu.Unlock()
			want := map[string]any{
				"generation":  float64(generation),
				"owner_token": ownerToken,
				"worker_id":   workerID,
				"boot_id":     expectedBootID,
				"status":      string(restjobs.StatusSucceeded),
				"evidence": map[string]any{
					"checks":      []any{map[string]any{"name": "check-01", "outcome": string(restjobs.VerificationPassed)}},
					"limitations": []any{restjobs.LimitationAgentNotVerdict},
				},
			}
			if !reflect.DeepEqual(payload, want) {
				t.Errorf("finish payload=%#v, want exact successful outcome and attempt identity %#v", payload, want)
			}
			if strings.Contains(string(body), secret) {
				t.Error("finish request exposed the Worker secret")
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"finished"}`))
			finishReceived <- payload
		default:
			t.Errorf("unexpected Controller request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	config := restserver.RemoteWorkerConfig{
		ControllerBaseURL:  server.URL,
		SharedSecretFile:   secretFile,
		WorkerID:           workerID,
		Repositories:       map[string]string{"widget": root, "unadvertised": filepath.Join(t.TempDir(), "other-repo")},
		HarnessProfiles:    map[string]restserver.RemoteHarnessConfig{"local": {Executable: harness, Args: []string{"{system_prompt}", "{task}"}, Repositories: []string{"widget"}}},
		VerificationChecks: [][]string{{check}},
		Limits:             restserver.RemoteWorkerLimits{JobTimeout: "1m", HarnessOutputBytes: 4096},
	}
	go func() { done <- RunRemoteWorker(ctx, config) }()
	select {
	case <-finishReceived:
	case err := <-done:
		cancel()
		t.Fatalf("remote Worker exited before reporting successful finish: %v", err)
	case <-time.After(15 * time.Second):
		cancel()
		t.Fatal("remote Worker did not finish the assigned job")
	}
	select {
	case <-polledAfterFinish:
	case err := <-done:
		cancel()
		t.Fatalf("remote Worker exited after finish with an error: %v", err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("remote Worker did not request another claim after successful finish")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("remote Worker shutdown after cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("remote Worker did not return after cancellation")
	}

	calls, err := os.ReadFile(harnessCalls)
	if err != nil {
		t.Fatalf("read harness calls: %v", err)
	}
	workdirs := strings.Fields(string(calls))
	if len(workdirs) != 4 {
		t.Fatalf("harness calls=%q (%d), want one local harness invocation for each workflow stage", calls, len(workdirs))
	}
	if filepath.Clean(workdirs[0]) == filepath.Clean(root) {
		t.Fatalf("requirements harness ran in the source checkout %q", workdirs[0])
	}
	for index, workdir := range workdirs {
		if index > 1 && workdir != workdirs[1] {
			t.Fatalf("repository harness stages used different workdirs: %v", workdirs)
		}
	}
	repositoryWorkdir := workdirs[1]
	if filepath.Clean(repositoryWorkdir) == filepath.Clean(root) {
		t.Fatalf("harness ran in the source checkout %q, want a private repository copy", repositoryWorkdir)
	}
	if _, err := os.Stat(filepath.Join(repositoryWorkdir, "README.md")); err != nil {
		t.Fatalf("harness workdir %q does not contain the assigned repository files: %v", repositoryWorkdir, err)
	}
	checkWorkdir, err := os.ReadFile(checkMarker)
	if err != nil || filepath.Clean(strings.TrimSpace(string(checkWorkdir))) != filepath.Clean(repositoryWorkdir) {
		t.Fatalf("configured verification check ran in %q, want private repository %q (error %v)", checkWorkdir, repositoryWorkdir, err)
	}
	requestMu.Lock()
	defer requestMu.Unlock()
	if claimCount < 1 || claimID == "" || bootID == "" || finishCount != 1 {
		t.Fatalf("Controller claims=%d claim_id=%q boot_id=%q finish calls=%d, want identity-bearing claim and one finish", claimCount, claimID, bootID, finishCount)
	}
}

func TestRemoteWorkerFailsUnsupportedRepositoryWithoutRunningHarnessAndContinuesPolling(t *testing.T) {
	const (
		jobID      = "00000000-0000-4000-8000-000000000002"
		ownerToken = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
		workerID   = "worker-a"
		generation = int64(7)
	)
	harnessCalls := filepath.Join(t.TempDir(), "harness-calls")
	harness := filepath.Join(t.TempDir(), "fake-harness.sh")
	harnessScript := "#!/bin/sh\nprintf 'called\\n' >> \"" + harnessCalls + "\"\n"
	if err := os.WriteFile(harness, []byte(harnessScript), 0o700); err != nil {
		t.Fatal(err)
	}

	finishReceived := make(chan map[string]any, 1)
	polledAfterFinish := make(chan struct{}, 1)
	var claimCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/worker/claims":
			var payload struct {
				BootID string `json:"boot_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode claim: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			claimCount++
			if claimCount > 1 {
				select {
				case polledAfterFinish <- struct{}{}:
				default:
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
			claim := remoteWorkerClaim{
				Job:     remoteWorkerJob{ID: jobID},
				Attempt: remoteWorkerAttempt{JobID: jobID, Generation: generation, OwnerToken: ownerToken, WorkerID: workerID, BootID: payload.BootID},
			}
			claim.Job.Request.Repository = "unprofiled"
			claim.Job.Request.Task = "run the assigned task"
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(claim); err != nil {
				t.Errorf("encode claim: %v", err)
			}
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"cancellation_requested":false}`))
		case strings.HasSuffix(r.URL.Path, "/finish"):
			if got, want := r.URL.Path, remoteAttemptPath(jobID, "finish"); got != want {
				t.Errorf("finish path=%q, want assigned attempt path %q", got, want)
			}
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode finish: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			finishReceived <- payload
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"finished"}`))
		default:
			t.Errorf("unexpected Controller request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	config := restserver.RemoteWorkerConfig{
		WorkerID:     workerID,
		Repositories: map[string]string{"unprofiled": t.TempDir()},
		HarnessProfiles: map[string]restserver.RemoteHarnessConfig{
			"widget-only": {Executable: harness, Repositories: []string{"widget"}},
		},
	}
	client := &remoteWorkerClient{baseURL: server.URL, workerID: workerID, repositories: []string{"widget"}, secret: "test-secret", client: server.Client()}
	go func() { done <- runRemoteWorkerLoop(ctx, config, client) }()

	var finish map[string]any
	select {
	case finish = <-finishReceived:
	case err := <-done:
		cancel()
		t.Fatalf("remote Worker exited before reporting unsupported-repository failure: %v", err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("remote Worker did not report unsupported-repository failure")
	}
	select {
	case <-polledAfterFinish:
	case err := <-done:
		cancel()
		t.Fatalf("remote Worker exited after failure instead of polling again: %v", err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("remote Worker did not request another claim after reporting failure")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("remote Worker shutdown after cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("remote Worker did not return after cancellation")
	}

	want := map[string]any{
		"generation":  float64(generation),
		"owner_token": ownerToken,
		"worker_id":   workerID,
		"boot_id":     client.bootID,
		"status":      string(restjobs.StatusFailed),
		"evidence": map[string]any{
			"checks": []any{},
			"limitations": []any{
				restjobs.LimitationAgentNotVerdict,
				restjobs.LimitationNoChecksConfigured,
			},
		},
	}
	if !reflect.DeepEqual(finish, want) {
		t.Fatalf("finish payload=%#v, want failed outcome, no-check evidence, and assigned attempt identity %#v", finish, want)
	}
	if claimCount < 2 {
		t.Fatalf("Controller claims=%d, want another claim after finishing the unsupported assignment", claimCount)
	}
	if _, err := os.Stat(harnessCalls); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("harness call marker stat error=%v, want no harness execution", err)
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
