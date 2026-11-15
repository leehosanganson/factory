package restapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
)

const remoteTestSecret = "remote-worker-api-test-secret-at-least-32-bytes"
const remoteTestJobID = "00000000-0000-4000-8000-000000000001"
const remoteTestToken = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

type remoteWorkerStoreFake struct {
	claimCalls            int
	claimRepositories     []string
	claim                 restjobs.AttemptClaim
	claimErr              error
	cancellationRequested bool
	heartbeatErr          error
	finishStatus          restjobs.Status
	finishEvidence        restjobs.VerificationEvidence
	finishErr             error
	finishCalls           int
	storedFinish          bool
	storedStatus          restjobs.Status
	storedEvidence        restjobs.VerificationEvidence
}

func (f *remoteWorkerStoreFake) ClaimRemoteAttempt(_ context.Context, claimID, workerID, bootID string, repositories []string) (restjobs.AttemptClaim, error) {
	f.claimCalls++
	f.claimRepositories = append([]string(nil), repositories...)
	if claimID != "claim-1" || workerID != "worker-1" || bootID != "boot-1" || len(repositories) == 0 {
		return restjobs.AttemptClaim{}, restjobs.ErrInvalidInput
	}
	return f.claim, f.claimErr
}

func (f *remoteWorkerStoreFake) HeartbeatRemoteAttempt(attempt restjobs.AttemptHandle) (bool, error) {
	if attempt.JobID != remoteTestJobID {
		return false, restjobs.ErrInvalidInput
	}
	return f.cancellationRequested, f.heartbeatErr
}

func (f *remoteWorkerStoreFake) FinishRemoteAttempt(_ restjobs.AttemptHandle, status restjobs.Status, evidence restjobs.VerificationEvidence) error {
	f.finishCalls++
	f.finishStatus = status
	f.finishEvidence = evidence
	if f.finishErr != nil {
		return f.finishErr
	}
	if f.storedFinish {
		if f.storedStatus != status || !reflect.DeepEqual(f.storedEvidence, evidence) {
			return restjobs.ErrInvalidTransition
		}
		return nil
	}
	f.storedFinish = true
	f.storedStatus = status
	f.storedEvidence = evidence
	return nil
}

func newRemoteWorkerHandler(t *testing.T, store RemoteWorkerStore) http.Handler {
	t.Helper()
	manager := newTestManager(t, restjobs.Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 128})
	handler, err := New(manager, testAPIKeyObject(t), Config{
		MaxRequestBodyBytes: 2048,
		MaxTaskBytes:        128,
		RepositoryAliases:   map[string]struct{}{"widget": {}},
		Ready:               func() bool { return true },
		ReadyError:          func(context.Context) error { return nil },
		RemoteWorkerAPI:     &RemoteWorkerAPIConfig{Secret: []byte(remoteTestSecret), Store: store},
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func testAPIKeyObject(t *testing.T) (key restserver.APIKey) {
	t.Helper()
	loaded, err := restserver.LoadAPIKey(writeAPIKey(t, testAPIKey))
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

func remoteRequest(handler http.Handler, path, body, secret string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if secret != "" {
		req.Header.Set("Authorization", "Bearer "+secret)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}

func TestRemoteWorkerAPIRequiresSeparateSecretAndReturnsClaimTokenOnlyInClaim(t *testing.T) {
	store := &remoteWorkerStoreFake{claim: restjobs.AttemptClaim{
		Job:     restjobs.Snapshot{ID: remoteTestJobID, Request: restjobs.Request{Repository: "widget", Task: "inspect"}},
		Attempt: restjobs.AttemptHandle{JobID: remoteTestJobID, Generation: 3, OwnerToken: remoteTestToken, WorkerID: "worker-1", BootID: "boot-1"},
	}}
	handler := newRemoteWorkerHandler(t, store)
	body := `{"claim_id":"claim-1","worker_id":"worker-1","boot_id":"boot-1","repositories":["widget"]}`
	for _, secret := range []string{"", "wrong-secret"} {
		response := remoteRequest(handler, "/v1/worker/claims", body, secret)
		if response.Code != http.StatusUnauthorized {
			t.Errorf("secret %q status=%d, want unauthorized", secret, response.Code)
		}
	}
	response := remoteRequest(handler, "/v1/worker/claims", body, remoteTestSecret)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"owner_token":"`+remoteTestToken+`"`) {
		t.Fatalf("authenticated claim response=(%d,%s), expected owner token", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), `"owner_token"`) && !strings.Contains(response.Body.String(), `"attempt":{"job_id":`) {
		t.Fatalf("claim token was not limited to worker attempt DTO: %s", response.Body.String())
	}
	if strings.Contains(response.Body.String(), `"Attempt"`) || strings.Contains(response.Body.String(), `"status"`) {
		t.Fatalf("claim response leaked Go attempt or ordinary snapshot: %s", response.Body.String())
	}
	if !reflect.DeepEqual(store.claimRepositories, []string{"widget"}) {
		t.Fatalf("store capability aliases=%v, want sorted allowlist", store.claimRepositories)
	}
	ordinary := remoteRequest(handler, "/v1/worker/claims", body, testAPIKey)
	if ordinary.Code != http.StatusUnauthorized {
		t.Fatalf("human API credential authenticated worker route: %d", ordinary.Code)
	}
	ordinaryPayload := principalRequest(handler, http.MethodPost, "/v1/jobs", `{"repository":"widget","task":"human API remains available"}`, testAPIKey)
	if ordinaryPayload.Code != http.StatusAccepted {
		t.Fatalf("worker API config changed legacy human route: status=%d body=%s", ordinaryPayload.Code, ordinaryPayload.Body.String())
	}
	if strings.Contains(ordinaryPayload.Body.String(), remoteTestToken) || strings.Contains(ordinaryPayload.Body.String(), `"owner_token"`) {
		t.Fatalf("ordinary human API response exposed remote fencing token: %s", ordinaryPayload.Body.String())
	}
}

func TestRemoteWorkerAPIRejectsAliasesOutsideControllerAllowlist(t *testing.T) {
	store := &remoteWorkerStoreFake{}
	handler := newRemoteWorkerHandler(t, store)
	response := remoteRequest(handler, "/v1/worker/claims", `{"claim_id":"claim-1","worker_id":"worker-1","boot_id":"boot-1","repositories":["unknown"]}`, remoteTestSecret)
	if response.Code != http.StatusBadRequest || store.claimCalls != 0 {
		t.Fatalf("unknown alias response=(%d,%s), claim calls=%d; want bad request before storage", response.Code, response.Body.String(), store.claimCalls)
	}
}

func TestRemoteWorkerAPIRequiresAuthBeforeParsingOrRouting(t *testing.T) {
	handler := newRemoteWorkerHandler(t, &remoteWorkerStoreFake{})
	for _, tc := range []struct {
		path string
		body string
	}{
		{"/v1/worker/claims", "{"},
		{"/v1/worker/unknown", ""},
	} {
		response := remoteRequest(handler, tc.path, tc.body, "")
		if response.Code != http.StatusUnauthorized {
			t.Errorf("unauthenticated %s status=%d, want unauthorized", tc.path, response.Code)
		}
	}
}

func TestRemoteWorkerAPIRejectsMalformedBoundedJSON(t *testing.T) {
	handler := newRemoteWorkerHandler(t, &remoteWorkerStoreFake{})
	valid := `{"claim_id":"claim-1","worker_id":"worker-1","boot_id":"boot-1","repositories":["widget"]}`
	cases := []struct {
		name string
		body string
		want int
	}{
		{"malformed", `{"claim_id":`, http.StatusBadRequest},
		{"unknown", `{"claim_id":"claim-1","worker_id":"worker-1","boot_id":"boot-1","harness":"bad"}`, http.StatusBadRequest},
		{"trailing", valid + ` {}`, http.StatusBadRequest},
		{"duplicate key", `{"claim_id":"claim-1","worker_id":"worker-1","boot_id":"boot-1","worker_id":"worker-2"}`, http.StatusBadRequest},
		{"oversize", `{"claim_id":"` + strings.Repeat("x", 2200) + `","worker_id":"worker-1","boot_id":"boot-1","repositories":["widget"]}`, http.StatusRequestEntityTooLarge},
		{"missing repositories", `{"claim_id":"claim-1","worker_id":"worker-1","boot_id":"boot-1"}`, http.StatusBadRequest},
		{"empty repositories", `{"claim_id":"claim-1","worker_id":"worker-1","boot_id":"boot-1","repositories":[]}`, http.StatusBadRequest},
		{"invalid alias", `{"claim_id":"claim-1","worker_id":"worker-1","boot_id":"boot-1","repositories":["../widget"]}`, http.StatusBadRequest},
		{"duplicate alias", `{"claim_id":"claim-1","worker_id":"worker-1","boot_id":"boot-1","repositories":["widget","widget"]}`, http.StatusBadRequest},
		{"too many aliases", `{"claim_id":"claim-1","worker_id":"worker-1","boot_id":"boot-1","repositories":["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"]}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := remoteRequest(handler, "/v1/worker/claims", tc.body, remoteTestSecret)
			if response.Code != tc.want {
				t.Fatalf("status=%d body=%s, want %d", response.Code, response.Body.String(), tc.want)
			}
		})
	}
}

func TestRemoteWorkerHeartbeatSeparatesCancellationFromErrors(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cancel     bool
		err        error
		wantStatus int
		wantBody   string
	}{
		{"cancel requested", true, nil, http.StatusOK, `"cancellation_requested":true`},
		{"no cancellation", false, nil, http.StatusOK, `"cancellation_requested":false`},
		{"database error", false, errors.New("database unavailable"), http.StatusServiceUnavailable, "service_unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &remoteWorkerStoreFake{cancellationRequested: tc.cancel, heartbeatErr: tc.err}
			handler := newRemoteWorkerHandler(t, store)
			body := `{"generation":1,"owner_token":"` + remoteTestToken + `","worker_id":"worker-1","boot_id":"boot-1"}`
			response := remoteRequest(handler, "/v1/worker/attempts/"+remoteTestJobID+"/heartbeat", body, remoteTestSecret)
			if response.Code != tc.wantStatus || !strings.Contains(response.Body.String(), tc.wantBody) {
				t.Fatalf("response=(%d,%s), want status %d and %q", response.Code, response.Body.String(), tc.wantStatus, tc.wantBody)
			}
		})
	}
}

func TestRemoteWorkerFinishPassesExactTerminalEvidenceAndMapsConflicts(t *testing.T) {
	for _, tc := range []struct {
		name          string
		err           error
		wantStatus    int
		wantErrorCode string
	}{
		{"accepted", nil, http.StatusOK, ""},
		{"conflicting retry", restjobs.ErrInvalidTransition, http.StatusConflict, ""},
		{"cancellation pending", restjobs.ErrCancellationPending, http.StatusConflict, "cancellation_pending"},
		{"cancellation not requested", restjobs.ErrCancellationNotRequested, http.StatusConflict, "cancellation_not_requested"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence := `{"checks":[{"name":"check-01","outcome":"passed"}],"limitations":["Agent-reported completion is not an independent correctness verdict."]}`
			body := `{"generation":1,"owner_token":"` + remoteTestToken + `","worker_id":"worker-1","boot_id":"boot-1","status":"succeeded","evidence":` + evidence + `}`
			store := &remoteWorkerStoreFake{}
			if tc.name == "conflicting retry" {
				store.storedFinish = true
				store.storedStatus = restjobs.StatusSucceeded
				store.storedEvidence = restjobs.VerificationEvidence{Checks: []restjobs.VerificationCheck{{Name: "check-01", Outcome: restjobs.VerificationFailed}}, Limitations: []string{"Agent-reported completion is not an independent correctness verdict."}}
			} else if tc.err != nil {
				store.finishErr = tc.err
			}
			handler := newRemoteWorkerHandler(t, store)
			response := remoteRequest(handler, "/v1/worker/attempts/"+remoteTestJobID+"/finish", body, remoteTestSecret)
			if response.Code != tc.wantStatus {
				t.Fatalf("status=%d body=%s, want %d", response.Code, response.Body.String(), tc.wantStatus)
			}
			if tc.wantErrorCode != "" && !strings.Contains(response.Body.String(), `"code":"`+tc.wantErrorCode+`"`) {
				t.Fatalf("response=%s, want error code %q", response.Body.String(), tc.wantErrorCode)
			}
			if tc.name == "accepted" {
				retry := remoteRequest(handler, "/v1/worker/attempts/"+remoteTestJobID+"/finish", body, remoteTestSecret)
				if retry.Code != http.StatusOK {
					t.Fatalf("exact finish retry status=%d body=%s, want idempotent success", retry.Code, retry.Body.String())
				}
				conflictBody := strings.Replace(body, `"outcome":"passed"`, `"outcome":"failed"`, 1)
				conflict := remoteRequest(handler, "/v1/worker/attempts/"+remoteTestJobID+"/finish", conflictBody, remoteTestSecret)
				if conflict.Code != http.StatusConflict {
					t.Fatalf("conflicting finish status=%d body=%s, want conflict", conflict.Code, conflict.Body.String())
				}
			}
			wantCalls := 1
			if tc.name == "accepted" {
				wantCalls = 3
			}
			if store.finishCalls != wantCalls {
				t.Fatalf("finish calls=%d, want %d", store.finishCalls, wantCalls)
			}
			if tc.name == "accepted" && (!store.storedFinish || store.storedStatus != restjobs.StatusSucceeded || len(store.storedEvidence.Checks) != 1 || store.storedEvidence.Checks[0].Outcome != restjobs.VerificationPassed) {
				t.Fatalf("finish call status=%q evidence=%+v", store.storedStatus, store.storedEvidence)
			}
			if (tc.name == "cancellation pending" || tc.name == "cancellation not requested") && (store.storedFinish || store.finishStatus != restjobs.StatusSucceeded) {
				t.Fatalf("conflict stored/replaced worker outcome: stored=%t status=%q", store.storedFinish, store.finishStatus)
			}
		})
	}
}
