package restapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leehosanganson/factory/internal/rest/jobs"
	"github.com/leehosanganson/factory/internal/rest/server"
)

const testAPIKey = "handler-test-secret"

func newTestHandler(t *testing.T, manager restjobs.Manager, ready func() bool) *Handler {
	t.Helper()
	key, err := restserver.LoadAPIKey(writeAPIKey(t, testAPIKey))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(manager, key, Config{
		MaxRequestBodyBytes: 256,
		MaxTaskBytes:        128,
		RepositoryAliases:   map[string]struct{}{"widget": {}},
		Ready:               ready,
		ReadyError:          func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func newPrincipalTestHandler(t *testing.T, manager restjobs.Manager, principals ...restserver.PrincipalConfig) *Handler {
	t.Helper()
	config := restserver.Config{APIKeyFile: writeAPIKey(t, testAPIKey), Principals: principals}
	for _, item := range principals {
		token := item.ID + "-named-token"
		verifier := filepath.Join(t.TempDir(), "verifier.json")
		digest := sha256.Sum256([]byte(token))
		data, err := json.Marshal(struct {
			Version    int `json:"version"`
			Principals []struct {
				ID          string `json:"id"`
				TokenSHA256 string `json:"token_sha256"`
			} `json:"principals"`
		}{Version: 1, Principals: []struct {
			ID          string `json:"id"`
			TokenSHA256 string `json:"token_sha256"`
		}{{ID: item.ID, TokenSHA256: hex.EncodeToString(digest[:])}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(verifier, data, 0o600); err != nil {
			t.Fatal(err)
		}
		item.TokenFile = verifier
		for index := range config.Principals {
			if config.Principals[index].ID == item.ID {
				config.Principals[index].TokenFile = verifier
			}
		}
	}
	resolver, err := restserver.LoadPrincipalResolver(config)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(manager, resolver.APIKey(), Config{
		MaxRequestBodyBytes: 256,
		MaxTaskBytes:        128,
		RepositoryAliases:   map[string]struct{}{"widget": {}},
		Ready:               func() bool { return true },
		ReadyError:          func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func principalRequest(handler http.Handler, method, path, body, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	if method == http.MethodPost && (path == "/v1/jobs" || path == "/v1/threads") {
		r.Header.Set("Idempotency-Key", "handler-principal-test-key")
	}
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, r)
	return response
}

func namedPrincipalToken(id string) string { return id + "-named-token" }

func newTestManager(t *testing.T, config restjobs.Config) *restjobs.LocalManager {
	t.Helper()
	manager, err := restjobs.NewManager(config)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func managerConfig(queue, records, events int) restjobs.Config {
	return restjobs.Config{QueueCapacity: queue, MaxConcurrentJobs: 1, MaxRecords: records, MaxEventsPerJob: events}
}

func writeAPIKey(t *testing.T, value string) string {
	t.Helper()
	path := t.TempDir() + "/api-key"
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func request(handler http.Handler, method, path, body string, authenticated bool) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if authenticated {
		r.Header.Set("Authorization", "Bearer "+testAPIKey)
	}
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, r)
	return recorder
}

func TestCreateAndInspectThreadEnforcesAuthOwnershipAndIdempotency(t *testing.T) {
	manager := newTestManager(t, managerConfig(16, 32, 8))
	handler := newPrincipalTestHandler(t, manager,
		restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "bob", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true},
	)

	create := func(token, key, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/threads", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		return response
	}
	first := create(namedPrincipalToken("alice"), "thread-key", `{"repository":"widget","task":"inspect this work"}`)
	if first.Code != http.StatusAccepted {
		t.Fatalf("create Thread status=%d body=%s", first.Code, first.Body.String())
	}
	var admitted struct {
		Thread restjobs.Thread   `json:"thread"`
		Job    restjobs.Snapshot `json:"job"`
		Replay bool              `json:"replayed"`
		Links  threadLinksDTO    `json:"links"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &admitted); err != nil {
		t.Fatal(err)
	}
	if admitted.Thread.ID == "" || admitted.Job.ID == "" || admitted.Thread.Submitter != "alice" || admitted.Thread.JobIDs[0] != admitted.Job.ID || admitted.Replay || admitted.Links.Self != "/v1/threads/"+admitted.Thread.ID || admitted.Links.Job != "/v1/jobs/"+admitted.Job.ID {
		t.Fatalf("create Thread response=%+v", admitted)
	}
	replayed := create(namedPrincipalToken("alice"), "thread-key", `{"repository":"widget","task":"inspect this work"}`)
	if replayed.Code != http.StatusAccepted {
		t.Fatalf("Thread retry status=%d body=%s", replayed.Code, replayed.Body.String())
	}
	var replayResponse struct {
		Thread restjobs.Thread   `json:"thread"`
		Job    restjobs.Snapshot `json:"job"`
		Replay bool              `json:"replayed"`
	}
	if err := json.Unmarshal(replayed.Body.Bytes(), &replayResponse); err != nil {
		t.Fatal(err)
	}
	if !replayResponse.Replay || replayResponse.Thread.ID != admitted.Thread.ID || replayResponse.Job.ID != admitted.Job.ID {
		t.Fatalf("Thread retry response=%+v, want same Thread/Job pair marked replay", replayResponse)
	}
	if conflict := create(namedPrincipalToken("alice"), "thread-key", `{"repository":"widget","task":"different"}`); conflict.Code != http.StatusConflict {
		t.Fatalf("changed idempotency payload status=%d body=%s, want conflict", conflict.Code, conflict.Body.String())
	}
	if separate := create(namedPrincipalToken("alice"), "thread-key-2", `{"repository":"widget","task":"inspect this work"}`); separate.Code != http.StatusAccepted {
		t.Fatalf("distinct Thread key status=%d body=%s", separate.Code, separate.Body.String())
	}
	if foreign := create(namedPrincipalToken("bob"), "thread-key", `{"repository":"widget","task":"inspect this work"}`); foreign.Code != http.StatusAccepted {
		t.Fatalf("different principal Thread key status=%d body=%s", foreign.Code, foreign.Body.String())
	}

	own := principalRequest(handler, http.MethodGet, "/v1/threads/"+admitted.Thread.ID, "", namedPrincipalToken("alice"))
	if own.Code != http.StatusOK || !strings.Contains(own.Body.String(), admitted.Job.ID) {
		t.Fatalf("own Thread inspection status=%d body=%s", own.Code, own.Body.String())
	}
	for _, id := range []string{admitted.Thread.ID, "missing-thread"} {
		foreign := principalRequest(handler, http.MethodGet, "/v1/threads/"+id, "", namedPrincipalToken("bob"))
		if foreign.Code != http.StatusNotFound {
			t.Errorf("submitter inspection of %q status=%d body=%s, want indistinguishable 404", id, foreign.Code, foreign.Body.String())
		}
	}
	operator := principalRequest(handler, http.MethodGet, "/v1/threads/"+admitted.Thread.ID, "", namedPrincipalToken("admin"))
	legacy := request(handler, http.MethodGet, "/v1/threads/"+admitted.Thread.ID, "", true)
	if operator.Code != http.StatusOK || legacy.Code != http.StatusOK {
		t.Errorf("operator/legacy inspection status=(%d,%d), want 200", operator.Code, legacy.Code)
	}
	unauthenticated := request(handler, http.MethodGet, "/v1/threads/"+admitted.Thread.ID, "", false)
	if unauthenticated.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated inspection status=%d, want 401", unauthenticated.Code)
	}

	standalone := principalRequest(handler, http.MethodPost, "/v1/jobs", `{"repository":"widget","task":"ordinary scheduled work"}`, namedPrincipalToken("alice"))
	if standalone.Code != http.StatusAccepted {
		t.Fatalf("standalone Job admission status=%d body=%s", standalone.Code, standalone.Body.String())
	}
	var standaloneResponse struct {
		Job restjobs.Snapshot `json:"job"`
	}
	if err := json.Unmarshal(standalone.Body.Bytes(), &standaloneResponse); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.GetThread(standaloneResponse.Job.ID); !errors.Is(err, restjobs.ErrNotFound) {
		t.Errorf("ordinary /v1/jobs admission unexpectedly created Thread membership: %v", err)
	}
	followRequest := httptest.NewRequest(http.MethodPost, "/v1/threads/"+admitted.Thread.ID+"/jobs", strings.NewReader(`{"repository":"widget","task":"follow up"}`))
	followRequest.Header.Set("Authorization", "Bearer "+namedPrincipalToken("alice"))
	followRequest.Header.Set("Idempotency-Key", "member-one")
	followRequest.Header.Set("Content-Type", "application/json")
	follow := httptest.NewRecorder()
	handler.ServeHTTP(follow, followRequest)
	if follow.Code != http.StatusAccepted {
		t.Fatalf("add Thread Job status=%d body=%s", follow.Code, follow.Body.String())
	}
	var added struct {
		Thread restjobs.Thread   `json:"thread"`
		Job    restjobs.Snapshot `json:"job"`
		Replay bool              `json:"replayed"`
	}
	if err := json.Unmarshal(follow.Body.Bytes(), &added); err != nil {
		t.Fatal(err)
	}
	if added.Replay || len(added.Thread.JobIDs) != 2 || added.Thread.JobIDs[0] != admitted.Job.ID || added.Thread.JobIDs[1] != added.Job.ID {
		t.Fatalf("added Thread Job response=%+v, want initial and follow-up in order", added)
	}
	replayRequest := httptest.NewRequest(http.MethodPost, "/v1/threads/"+admitted.Thread.ID+"/jobs", strings.NewReader(`{"repository":"widget","task":"follow up"}`))
	replayRequest.Header.Set("Authorization", "Bearer "+namedPrincipalToken("alice"))
	replayRequest.Header.Set("Idempotency-Key", "member-one")
	replayRequest.Header.Set("Content-Type", "application/json")
	replay := httptest.NewRecorder()
	handler.ServeHTTP(replay, replayRequest)
	var replayedMember struct {
		Job      restjobs.Snapshot `json:"job"`
		Replayed bool              `json:"replayed"`
	}
	if replay.Code != http.StatusAccepted || json.Unmarshal(replay.Body.Bytes(), &replayedMember) != nil || !replayedMember.Replayed || replayedMember.Job.ID != added.Job.ID {
		t.Fatalf("Thread member replay status=%d body=%s, want original Job marked replayed", replay.Code, replay.Body.String())
	}
	foreignRequest := httptest.NewRequest(http.MethodPost, "/v1/threads/"+admitted.Thread.ID+"/jobs", strings.NewReader(`{"repository":"widget","task":"foreign"}`))
	foreignRequest.Header.Set("Authorization", "Bearer "+namedPrincipalToken("bob"))
	foreignRequest.Header.Set("Idempotency-Key", "foreign-member")
	foreignRequest.Header.Set("Content-Type", "application/json")
	foreignAdd := httptest.NewRecorder()
	handler.ServeHTTP(foreignAdd, foreignRequest)
	if foreignAdd.Code != http.StatusNotFound {
		t.Fatalf("foreign submitter member admission status=%d body=%s, want 404", foreignAdd.Code, foreignAdd.Body.String())
	}
}

func TestCompleteThreadRequiresOwnershipAndIsBodylessIdempotent(t *testing.T) {
	manager := newTestManager(t, managerConfig(16, 32, 8))
	handler := newPrincipalTestHandler(t, manager,
		restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "bob", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "operator", Role: "operator", Enabled: true},
	)
	created, _, err := manager.AdmitThreadAs("completion", "alice", restjobs.Request{Repository: "widget", Task: "still running"})
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/threads/" + created.Thread.ID + "/complete"
	doneThread, _, err := manager.AdmitThreadAs("already-done", "alice", restjobs.Request{Repository: "widget", Task: "finish before explicit action"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := manager.ClaimNext()
	if err != nil || claimed.ID != created.Jobs[0].ID {
		t.Fatalf("claim first Job=(%+v,%v)", claimed, err)
	}
	if err := manager.Finish(claimed.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	claimed, err = manager.ClaimNext()
	if err != nil || claimed.ID != doneThread.Jobs[0].ID {
		t.Fatalf("claim second Job=(%+v,%v)", claimed, err)
	}
	if err := manager.Finish(claimed.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	inspectDone := principalRequest(handler, http.MethodGet, "/v1/threads/"+doneThread.Thread.ID, "", namedPrincipalToken("alice"))
	var doneSnapshot restjobs.ThreadSnapshot
	if inspectDone.Code != http.StatusOK || json.Unmarshal(inspectDone.Body.Bytes(), &doneSnapshot) != nil || doneSnapshot.Thread.Status != restjobs.ThreadStatusDone || doneSnapshot.Thread.CompletedAt != nil {
		t.Fatalf("Done Thread inspection status=%d body=%s, want Done but open before explicit action", inspectDone.Code, inspectDone.Body.String())
	}
	if closed := principalRequest(handler, http.MethodPost, "/v1/threads/"+doneThread.Thread.ID+"/complete", "", namedPrincipalToken("alice")); closed.Code != http.StatusOK {
		t.Fatalf("explicit completion of Done Thread status=%d body=%s, want 200", closed.Code, closed.Body.String())
	}
	if got := request(handler, http.MethodPost, path, "", false); got.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated completion status=%d body=%s, want 401", got.Code, got.Body.String())
	}
	for _, token := range []string{namedPrincipalToken("bob"), namedPrincipalToken("operator")} {
		got := principalRequest(handler, http.MethodPost, path, "", token)
		if got.Code != http.StatusNotFound {
			t.Fatalf("non-owner completion status=%d body=%s, want non-disclosing 404", got.Code, got.Body.String())
		}
	}
	operatorOwned, _, err := manager.AdmitThreadAs("operator-owned", "operator", restjobs.Request{Repository: "widget", Task: "operator's own Thread"})
	if err != nil {
		t.Fatal(err)
	}
	if got := principalRequest(handler, http.MethodPost, "/v1/threads/"+operatorOwned.Thread.ID+"/complete", "", namedPrincipalToken("operator")); got.Code != http.StatusOK {
		t.Fatalf("operator completion of own Thread status=%d body=%s, want 200", got.Code, got.Body.String())
	}
	missing := principalRequest(handler, http.MethodPost, "/v1/threads/missing-thread/complete", "", namedPrincipalToken("alice"))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing Thread completion status=%d body=%s, want non-disclosing 404", missing.Code, missing.Body.String())
	}
	if body := principalRequest(handler, http.MethodPost, path, `{"completed":true}`, namedPrincipalToken("alice")); body.Code != http.StatusBadRequest {
		t.Fatalf("completion with body status=%d body=%s, want 400", body.Code, body.Body.String())
	}
	jobBefore, err := manager.Get(created.Jobs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	complete := principalRequest(handler, http.MethodPost, path, "", namedPrincipalToken("alice"))
	if complete.Code != http.StatusOK {
		t.Fatalf("owner completion status=%d body=%s, want 200", complete.Code, complete.Body.String())
	}
	var first restjobs.ThreadSnapshot
	if err := json.Unmarshal(complete.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.Thread.CompletedAt == nil || first.Thread.CompletedBy != "alice" || first.Jobs[0].ID != jobBefore.ID || first.Jobs[0].Status != jobBefore.Status || !first.Jobs[0].UpdatedAt.Equal(jobBefore.UpdatedAt) {
		t.Fatalf("completion response=%+v, want explicit completion without Job mutation", first)
	}
	retry := principalRequest(handler, http.MethodPost, path, "", namedPrincipalToken("alice"))
	var repeated restjobs.ThreadSnapshot
	if retry.Code != http.StatusOK || json.Unmarshal(retry.Body.Bytes(), &repeated) != nil || repeated.Thread.CompletedAt == nil || !repeated.Thread.CompletedAt.Equal(*first.Thread.CompletedAt) {
		t.Fatalf("repeated completion status=%d body=%s, want idempotent same completion", retry.Code, retry.Body.String())
	}
	if got := principalRequest(handler, http.MethodPost, path+"/jobs", `{"repository":"widget","task":"not a valid route"}`, namedPrincipalToken("alice")); got.Code != http.StatusNotFound {
		t.Fatalf("invalid nested route status=%d", got.Code)
	}
	add := httptest.NewRequest(http.MethodPost, "/v1/threads/"+created.Thread.ID+"/jobs", strings.NewReader(`{"repository":"widget","task":"after close"}`))
	add.Header.Set("Authorization", "Bearer "+namedPrincipalToken("alice"))
	add.Header.Set("Content-Type", "application/json")
	add.Header.Set("Idempotency-Key", "after-close")
	addResponse := httptest.NewRecorder()
	handler.ServeHTTP(addResponse, add)
	if addResponse.Code != http.StatusConflict || !strings.Contains(addResponse.Body.String(), "thread_closed") {
		t.Fatalf("append after completion status=%d body=%s, want sanitized 409", addResponse.Code, addResponse.Body.String())
	}
}

func TestThreadDoneNotificationsAreOwnerFilteredAndSafe(t *testing.T) {
	manager := newTestManager(t, managerConfig(16, 32, 8))
	handler := newPrincipalTestHandler(t, manager,
		restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "bob", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true},
	)
	created, _, err := manager.AdmitThreadAs("notify", "alice", restjobs.Request{Repository: "widget", Task: "private request secret text"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(created.Jobs[0].ID, restjobs.StatusCanceled); err != nil {
		t.Fatal(err)
	}
	path := "/v1/notifications"
	own := principalRequest(handler, http.MethodGet, path, "", namedPrincipalToken("alice"))
	if own.Code != http.StatusOK || strings.Contains(own.Body.String(), "private request secret text") || !strings.Contains(own.Body.String(), created.Thread.ID) {
		t.Fatalf("owner notification response=%d %s", own.Code, own.Body.String())
	}
	for _, token := range []string{namedPrincipalToken("bob"), namedPrincipalToken("admin"), testAPIKey} {
		response := principalRequest(handler, http.MethodGet, path, "", token)
		if response.Code != http.StatusOK || strings.Contains(response.Body.String(), created.Thread.ID) {
			t.Errorf("non-owner inbox response=%d %s, expected no foreign Thread", response.Code, response.Body.String())
		}
	}
	if response := request(handler, http.MethodGet, path, "", false); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated inbox status=%d body=%s", response.Code, response.Body.String())
	}
	for _, query := range []string{"?limit=0", "?limit=101", "?before=0", "?unexpected=1", "?limit=1&limit=2"} {
		if response := principalRequest(handler, http.MethodGet, path+query, "", namedPrincipalToken("alice")); response.Code != http.StatusBadRequest {
			t.Errorf("invalid query %q status=%d body=%s", query, response.Code, response.Body.String())
		}
	}
	if response := principalRequest(handler, http.MethodGet, path+"?limit=1", "", namedPrincipalToken("alice")); response.Code != http.StatusOK {
		t.Fatalf("bounded inbox status=%d body=%s", response.Code, response.Body.String())
	} else {
		var page restjobs.NotificationPage
		if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil || len(page.Notifications) != 1 || page.HasMore || page.NextCursor != 0 {
			t.Fatalf("bounded inbox page=(%+v,%v)", page, err)
		}
	}
	if response := principalRequest(handler, http.MethodGet, path, "", namedPrincipalToken("alice")); response.Code != http.StatusOK {
		t.Fatalf("repeated read status=%d body=%s", response.Code, response.Body.String())
	}
	if page, err := manager.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10); err != nil || len(page.Notifications) != 1 {
		t.Fatalf("repeated API read changed inbox=(%+v,%v)", page, err)
	}
}

func TestThreadRouteValidationAndUnsupportedStore(t *testing.T) {
	manager := newTestManager(t, managerConfig(4, 8, 8))
	handler := newTestHandler(t, manager, func() bool { return true })
	unsupported, err := New(struct{ restjobs.Manager }{Manager: manager}, handler.key, Config{MaxRequestBodyBytes: 256, MaxTaskBytes: 128, RepositoryAliases: map[string]struct{}{"widget": {}}, Ready: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	reqUnsupported := httptest.NewRequest(http.MethodPost, "/v1/threads", strings.NewReader(`{"repository":"widget","task":"x"}`))
	reqUnsupported.Header.Set("Authorization", "Bearer "+testAPIKey)
	reqUnsupported.Header.Set("Content-Type", "application/json")
	reqUnsupported.Header.Set("Idempotency-Key", "unsupported-store")
	unsupportedResponse := httptest.NewRecorder()
	unsupported.ServeHTTP(unsupportedResponse, reqUnsupported)
	if unsupportedResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("unsupported Store status=%d body=%s, want 503", unsupportedResponse.Code, unsupportedResponse.Body.String())
	}
	for _, tc := range []struct {
		name string
		body string
		want int
	}{
		{name: "issue field rejected", body: `{"repository":"widget","task":"x","issue":1}`, want: http.StatusBadRequest},
		{name: "unknown field rejected", body: `{"repository":"widget","task":"x","extra":true}`, want: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/threads", strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+testAPIKey)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", "validation-"+tc.name)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != tc.want {
				t.Fatalf("status=%d body=%s, want %d", response.Code, response.Body.String(), tc.want)
			}
		})
	}
	for _, tc := range []struct {
		method string
		path   string
		want   int
	}{{http.MethodGet, "/v1/threads", http.StatusMethodNotAllowed}, {http.MethodPost, "/v1/threads/id", http.StatusMethodNotAllowed}, {http.MethodGet, "/v1/threads/id/nested", http.StatusNotFound}} {
		response := request(handler, tc.method, tc.path, "", true)
		if response.Code != tc.want {
			t.Errorf("%s %s status=%d body=%s, want %d", tc.method, tc.path, response.Code, response.Body.String(), tc.want)
		}
	}
}

func TestDashboardPrincipalIsBoundToConfiguredOperatorAndSafeRoutes(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	handler := newTestHandler(t, manager, func() bool { return true })

	request := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(`{"repository":"widget","task":"trusted frontend"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "frontend-trusted-request")
	response := httptest.NewRecorder()
	handler.config.DashboardOperatorID = "admin"
	handler.ServeAsDashboardOperator(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("trusted principal admission status=%d body=%s", response.Code, response.Body.String())
	}

	for _, path := range []string{"/v1/operations", "/v1/jobs/one/reconcile", "/v1/jobs/one/events/extra"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		handler.ServeAsDashboardOperator(response, request)
		if response.Code != http.StatusNotFound {
			t.Errorf("dashboard exposed non-allowlisted path %s status=%d", path, response.Code)
		}
	}
	request = httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(`{"repository":"widget","task":"trusted frontend"}`))
	request.URL.RawPath = "/v1%2Fjobs"
	response = httptest.NewRecorder()
	handler.ServeAsDashboardOperator(response, request)
	if response.Code != http.StatusNotFound {
		t.Errorf("dashboard accepted escaped API path status=%d", response.Code)
	}
}

func TestSafeStageSummaryDoesNotExposePrivateEventMessages(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("safe-stage-summary", restjobs.Request{Repository: "widget", Task: "private task payload"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := manager.ClaimNextAttempt()
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RecordProgressForAttempt(claim.Attempt, restjobs.ProgressSummary{Stage: "implement", State: "started"}); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEventForAttempt(claim.Attempt, "private_event", "secret transcript /private/path"); err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	get := principalRequest(handler, http.MethodGet, "/v1/jobs/"+job.ID, "", testAPIKey)
	for _, want := range []string{`"stage":"implement"`, `"state":"started"`} {
		if !strings.Contains(get.Body.String(), want) {
			t.Fatalf("GET omitted stage summary %s: %s", want, get.Body.String())
		}
	}
	for _, secret := range []string{"secret transcript", "/private/path"} {
		if strings.Contains(get.Body.String(), secret) {
			t.Fatalf("GET leaked %q: %s", secret, get.Body.String())
		}
	}
	history := principalRequest(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/history", "", testAPIKey)
	if !strings.Contains(history.Body.String(), `"progress":{"stage":"implement","state":"started"}`) {
		t.Fatalf("history omitted dedicated progress summary: %s", history.Body.String())
	}
	for _, want := range []string{`"stage":"implement"`, `"state":"started"`} {
		if !strings.Contains(history.Body.String(), want) {
			t.Fatalf("history omitted safe summary %s: %s", want, history.Body.String())
		}
	}
	for _, secret := range []string{"secret transcript", "/private/path", "private_event"} {
		if strings.Contains(history.Body.String(), secret) {
			t.Fatalf("history leaked %q: %s", secret, history.Body.String())
		}
	}
}

func TestJobEventStreamReplaysSafeOrderedHistoryAndClosesAtTerminal(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 4))
	job, _, err := manager.Admit("event-stream", restjobs.Request{Repository: "widget", Task: "private task payload"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := manager.ClaimNextAttempt()
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEventForAttempt(claim.Attempt, "running", "/workspace/private-log credential-secret"); err != nil {
		t.Fatal(err)
	}
	if err := manager.RecordProgressForAttempt(claim.Attempt, restjobs.ProgressSummary{Stage: "implement", State: "started"}); err != nil {
		t.Fatal(err)
	}
	if err := manager.FinishAttempt(claim.Attempt, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}

	handler := newTestHandler(t, manager, func() bool { return true })
	response := request(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/events", "", true)
	body := response.Body.String()
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("event stream response status=%d content-type=%q body=%q", response.Code, response.Header().Get("Content-Type"), body)
	}
	for _, event := range []string{"id: 1", "event: queued", "id: 2", "event: running", "event: workflow_progress", `"stage":"implement"`, `"state":"started"`, "id: 3", "event: succeeded"} {
		if !strings.Contains(body, event) {
			t.Errorf("event stream omitted %q: %q", event, body)
		}
	}
	if strings.Contains(body, "private task payload") || strings.Contains(body, "private-log") || strings.Contains(body, "credential-secret") || strings.Contains(body, "provider") || strings.Contains(body, "path") {
		t.Fatalf("event stream exposed private job data: %q", body)
	}
	if strings.Index(body, "event: queued") > strings.Index(body, "event: running") || strings.Index(body, "event: running") > strings.Index(body, "event: succeeded") {
		t.Fatalf("event stream history is out of order: %q", body)
	}
}

func TestJobEventStreamReconnectReplaysOnlyEventsAfterCursor(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("stream-resume", restjobs.Request{Repository: "widget", Task: "private task"})
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	resume := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil)
	resume.Header.Set("Authorization", "Bearer "+testAPIKey)
	resume.Header.Set("Last-Event-ID", "1")
	ctx, cancel := context.WithCancel(resume.Context())
	resume = resume.WithContext(ctx)
	writer := newSynchronizedStreamRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(writer, resume)
		close(done)
	}()
	waitForTestCondition(t, func() bool { return len(handler.eventStreams) == 1 })
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(job.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	waitForTestCondition(t, func() bool { return strings.Contains(writer.String(), "event: succeeded") })
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream did not release after disconnect")
	}
	body := writer.String()
	if strings.Contains(body, "id: 1") || !strings.Contains(body, "id: 2") || !strings.Contains(body, "id: 3") {
		t.Fatalf("cursor replay included duplicate or omitted events: %q", body)
	}
}

func waitForTestCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not reached before timeout")
}

func TestJobEventStreamReplaysFromCursorForSQLiteStore(t *testing.T) {
	databaseDir := t.TempDir()
	if err := os.Chmod(databaseDir, 0o700); err != nil {
		t.Fatal(err)
	}
	database := databaseDir + "/jobs.db"
	manager, err := restjobs.OpenSQLiteStore(database, managerConfig(2, 2, 8))
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	job, _, err := manager.Admit("sqlite-event-stream", restjobs.Request{Repository: "widget", Task: "private SQLite task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(job.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	history, err := manager.History(job.ID)
	if err != nil || len(history.Events) != 3 {
		t.Fatalf("SQLite event history=%+v err=%v", history, err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	resume := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil)
	resume.Header.Set("Authorization", "Bearer "+testAPIKey)
	resume.Header.Set("Last-Event-ID", "2")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, resume)
	body := recorder.Body.String()
	if recorder.Code != http.StatusOK || strings.Contains(body, "id: 2") || !strings.Contains(body, "id: 3") || strings.Contains(body, "private SQLite task") {
		t.Fatalf("SQLite cursor replay status=%d body=%q", recorder.Code, body)
	}
}

func TestJobEventStreamReadinessAndMalformedCursor(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 4))
	job, _, err := manager.Admit("stream-ready", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return false })
	notReady := request(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/events", "", true)
	assertError(t, notReady, http.StatusServiceUnavailable, "not_ready")
	handler.config.Ready = func() bool { return true }
	for _, cursor := range []string{"not-a-number", "1,2"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil)
		req.Header.Set("Authorization", "Bearer "+testAPIKey)
		req.Header.Add("Last-Event-ID", cursor)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		assertError(t, response, http.StatusBadRequest, "invalid_request")
	}
	withQuery := request(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/events?cursor=1", "", true)
	assertError(t, withQuery, http.StatusBadRequest, "invalid_request")
}

func TestJobEventStreamTerminalSnapshotWithoutHistoryCloses(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 1))
	job, _, err := manager.Admit("stream-terminal-empty", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(job.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(job.ID, "audit", "event two"); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(job.ID, "audit", "event three"); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(job.ID, "audit", "event four"); err != nil {
		t.Fatal(err)
	}
	history, err := manager.History(job.ID)
	if err != nil || !history.Truncated || len(history.Events) != 1 || history.Events[0].Sequence != 6 {
		t.Fatalf("terminal history fixture=%+v err=%v", history, err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	response := request(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/events", "", true)
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, "event: terminal_snapshot") || !strings.Contains(body, `"status":"succeeded"`) || strings.Contains(body, "event two") || strings.Contains(body, "event three") || strings.Contains(body, "event four") || len(handler.eventStreams) != 0 {
		t.Fatalf("terminal tail replay status=%d active=%d body=%q", response.Code, len(handler.eventStreams), body)
	}
}

func TestJobEventStreamTerminalClosesWhenTerminalEventExpired(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 1))
	job, _, err := manager.Admit("stream-terminal-expired", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(job.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(job.ID, "audit", "private details"); err != nil {
		t.Fatal(err)
	}
	history, err := manager.History(job.ID)
	if err != nil || history.LatestSequence != 4 || history.Events[0].Type != "audit" {
		t.Fatalf("terminal event expiration fixture history=%+v err=%v", history, err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil)
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	req.Header.Set("Last-Event-ID", "4")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusOK || len(handler.eventStreams) != 0 {
		t.Fatalf("terminal stream status=%d active slots=%d body=%q", response.Code, len(handler.eventStreams), response.Body.String())
	}
}

func TestJobEventStreamCursorDoesNotRepeatAfterEventEviction(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 1))
	job, _, err := manager.Admit("stream-eviction", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(job.ID, "audit", "one"); err != nil {
		t.Fatal(err)
	}
	history, err := manager.History(job.ID)
	if err != nil || history.LatestSequence != 2 {
		t.Fatalf("pre-eviction cursor=%+v err=%v", history, err)
	}
	if err := manager.AddEvent(job.ID, "audit", "two"); err != nil {
		t.Fatal(err)
	}
	history, err = manager.History(job.ID)
	if err != nil || history.LatestSequence != 3 || history.Events[0].Sequence != 3 {
		t.Fatalf("post-eviction cursor=%+v err=%v", history, err)
	}
}

func TestJobEventStreamResetWhenHistoryExpiresDuringSubscription(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 1))
	job, _, err := manager.Admit("stream-expire-live", restjobs.Request{Repository: "widget", Task: "private task"})
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	writer := newSynchronizedStreamRecorder()
	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil).WithContext(requestCtx)
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(writer, req)
		close(done)
	}()
	waitForTestCondition(t, func() bool { return len(handler.eventStreams) == 1 })
	if err := manager.AddEvent(job.ID, "audit", "private log /workspace/token-secret"); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(job.ID, "audit", "another private event"); err != nil {
		t.Fatal(err)
	}
	waitForTestCondition(t, func() bool { return strings.Contains(writer.String(), `"reason":"cursor_expired"`) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reset stream did not terminate")
	}
	body := writer.String()
	if strings.Contains(body, "token-secret") || strings.Contains(body, "/workspace") || strings.Contains(body, "private task") || !strings.Contains(body, "GET status and history") {
		t.Fatalf("reset control leaked private data or omitted fallback: %q", body)
	}
}

func TestJobEventStreamAuthenticationCursorAndCapacity(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 3))
	job, _, err := manager.Admit("stream-auth", restjobs.Request{Repository: "widget", Task: "private"})
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })

	unauthenticated := request(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/events", "", false)
	assertError(t, unauthenticated, http.StatusUnauthorized, "unauthenticated")
	unknown := request(handler, http.MethodGet, "/v1/jobs/unknown/events", "", true)
	assertError(t, unknown, http.StatusNotFound, "not_found")
	wrongMethod := request(handler, http.MethodPost, "/v1/jobs/"+job.ID+"/events", "", true)
	assertError(t, wrongMethod, http.StatusMethodNotAllowed, "method_not_allowed")
	if wrongMethod.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("event stream Allow=%q, want GET", wrongMethod.Header().Get("Allow"))
	}

	oldest := newTestManager(t, managerConfig(2, 2, 1))
	truncatedJob, _, err := oldest.Admit("stream-stale", restjobs.Request{Repository: "widget", Task: "private"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldest.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := oldest.Finish(truncatedJob.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	truncatedHandler := newTestHandler(t, oldest, func() bool { return true })
	staleRequest := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+truncatedJob.ID+"/events", nil)
	staleRequest.Header.Set("Authorization", "Bearer "+testAPIKey)
	staleRequest.Header.Set("Last-Event-ID", "1")
	staleResponse := httptest.NewRecorder()
	truncatedHandler.ServeHTTP(staleResponse, staleRequest)
	assertError(t, staleResponse, http.StatusConflict, "cursor_expired")
	noCursor := request(truncatedHandler, http.MethodGet, "/v1/jobs/"+truncatedJob.ID+"/events", "", true)
	if noCursor.Code != http.StatusOK || !strings.Contains(noCursor.Body.String(), "event: succeeded") {
		t.Fatalf("terminal retained-tail stream status=%d body=%q", noCursor.Code, noCursor.Body.String())
	}
	badCursor := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil)
	badCursor.Header.Set("Authorization", "Bearer "+testAPIKey)
	badCursor.Header.Set("Last-Event-ID", "2")
	badCursorResponse := httptest.NewRecorder()
	handler.ServeHTTP(badCursorResponse, badCursor)
	assertError(t, badCursorResponse, http.StatusBadRequest, "invalid_request")

	entered := make(chan struct{}, eventStreamConcurrency+1)
	writer := &streamTestWriter{entered: entered, header: make(http.Header)}
	requestCtx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil).WithContext(requestCtx)
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(writer, req)
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("stream did not begin writing")
	}
	for index := 0; index < eventStreamConcurrency-1; index++ {
		handler.eventStreams <- struct{}{}
	}
	resp := request(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/events", "", true)
	assertError(t, resp, http.StatusServiceUnavailable, "stream_capacity")
	select {
	case <-done:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("slow-client write deadline did not release its handler")
	}
	cancel()
	if got := len(handler.eventStreams); got != eventStreamConcurrency-1 {
		t.Fatalf("event stream capacity retained %d slots after disconnect, want only %d simulated reservations", got, eventStreamConcurrency-1)
	}
	for range eventStreamConcurrency - 1 {
		<-handler.eventStreams
	}
}

type synchronizedStreamRecorder struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	header http.Header
	status int
}

func newSynchronizedStreamRecorder() *synchronizedStreamRecorder {
	return &synchronizedStreamRecorder{header: make(http.Header)}
}

func (r *synchronizedStreamRecorder) Header() http.Header { return r.header }
func (r *synchronizedStreamRecorder) WriteHeader(status int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status == 0 {
		r.status = status
	}
}
func (r *synchronizedStreamRecorder) Write(data []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.buffer.Write(data)
}
func (r *synchronizedStreamRecorder) Flush() {}
func (r *synchronizedStreamRecorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buffer.String()
}

type streamTestWriter struct {
	entered  chan struct{}
	header   http.Header
	mu       sync.Mutex
	deadline time.Time
}

func (w *streamTestWriter) Header() http.Header { return w.header }

func (w *streamTestWriter) WriteHeader(int) {}

func (w *streamTestWriter) Write(data []byte) (int, error) {
	select {
	case w.entered <- struct{}{}:
	default:
	}
	w.mu.Lock()
	deadline := w.deadline
	w.mu.Unlock()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	<-timer.C
	return 0, errors.New("simulated slow client write timeout")
}

func (w *streamTestWriter) SetWriteDeadline(time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.deadline = time.Now().Add(10 * time.Millisecond)
	return nil
}

func (w *streamTestWriter) Flush() {}

func TestNotReadyRejectsJobOperationsBeforeAuthentication(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 2))
	handler := newTestHandler(t, manager, func() bool { return false })
	for _, tc := range []struct {
		method string
		path   string
	}{
		{method: http.MethodPost, path: "/v1/jobs"},
		{method: http.MethodGet, path: "/v1/jobs/unknown"},
		{method: http.MethodGet, path: "/v1/jobs/unknown/events"},
		{method: http.MethodGet, path: "/v1/jobs"},
	} {
		response := request(handler, tc.method, tc.path, `{"repository":"widget","task":"task"}`, false)
		if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"not_ready"`) {
			t.Errorf("%s %s while not ready = %d %s", tc.method, tc.path, response.Code, response.Body.String())
		}
	}
}

func TestSubmitterAuthorizationOwnershipAndOperatorCompatibility(t *testing.T) {
	manager := newTestManager(t, managerConfig(8, 8, 8))
	issueNumber := 42
	first, _, err := manager.AdmitAs("shared-idempotency", "alice", restjobs.Request{Repository: "widget", Task: "alice private task", Issue: &issueNumber, IssueProvenance: &restjobs.IssueProvenance{
		Tracker: "github", SourceRepository: "acme/widget", IssueNumber: issueNumber, Version: strings.Repeat("a", 64),
		URL: "https://github.com/acme/widget/issues/42", Title: "Bounded issue title",
	}})
	if err != nil {
		t.Fatal(err)
	}
	bobIssueNumber := 43
	if _, _, err := manager.AdmitAs("shared-idempotency", "bob", restjobs.Request{Repository: "widget", Task: "bob private task", Issue: &bobIssueNumber, IssueProvenance: &restjobs.IssueProvenance{
		Tracker: "github", SourceRepository: "acme/widget", IssueNumber: bobIssueNumber, Version: strings.Repeat("b", 64),
		URL: "https://github.com/acme/widget/issues/43", Title: "Bob issue title",
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(first.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	handler := newPrincipalTestHandler(t, manager,
		restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "bob", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true},
	)
	aliceToken, bobToken := namedPrincipalToken("alice"), namedPrincipalToken("bob")
	if err := manager.AddEvent(first.ID, "running", "Job started"); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "/history", "/events", "/issue-provenance"} {
		own := principalRequest(handler, http.MethodGet, "/v1/jobs/"+first.ID+suffix, "", aliceToken)
		if own.Code != http.StatusOK {
			t.Errorf("owner GET %s status=%d body=%s", suffix, own.Code, own.Body.String())
		}
		if suffix == "/issue-provenance" && (strings.Contains(own.Body.String(), "alice private task") || !strings.Contains(own.Body.String(), "Bounded issue title")) {
			t.Errorf("owner provenance response leaked task or omitted metadata: %s", own.Body.String())
		}
		forbidden := principalRequest(handler, http.MethodGet, "/v1/jobs/"+first.ID+suffix, "", bobToken)
		unknown := principalRequest(handler, http.MethodGet, "/v1/jobs/not-a-job"+suffix, "", bobToken)
		if forbidden.Code != http.StatusNotFound || forbidden.Code != unknown.Code || forbidden.Body.String() != unknown.Body.String() {
			t.Errorf("%s ownership disclosed existence: forbidden=%d %s unknown=%d %s", suffix, forbidden.Code, forbidden.Body.String(), unknown.Code, unknown.Body.String())
		}
	}
	plainJob, _, err := manager.AdmitAs("alice-ordinary-job", "alice", restjobs.Request{Repository: "widget", Task: "ordinary private task"})
	if err != nil {
		t.Fatal(err)
	}
	plainOwner := principalRequest(handler, http.MethodGet, "/v1/jobs/"+plainJob.ID+"/issue-provenance", "", aliceToken)
	plainUnknown := principalRequest(handler, http.MethodGet, "/v1/jobs/not-a-job/issue-provenance", "", aliceToken)
	if plainOwner.Code != http.StatusNotFound || plainOwner.Body.String() != plainUnknown.Body.String() || strings.Contains(plainOwner.Body.String(), "ordinary private task") {
		t.Fatalf("owner request without provenance did not match generic not-found: owner=%d %s unknown=%d %s", plainOwner.Code, plainOwner.Body.String(), plainUnknown.Code, plainUnknown.Body.String())
	}
	plainOther := principalRequest(handler, http.MethodGet, "/v1/jobs/"+plainJob.ID+"/issue-provenance", "", bobToken)
	if plainOther.Code != http.StatusNotFound || plainOther.Body.String() != plainUnknown.Body.String() {
		t.Fatalf("nonowner request without provenance disclosed job existence: other=%d %s unknown=%d %s", plainOther.Code, plainOther.Body.String(), plainUnknown.Code, plainUnknown.Body.String())
	}
	ownHistory := principalRequest(handler, http.MethodGet, "/v1/jobs/"+first.ID+"/history", "", aliceToken)
	if !strings.Contains(ownHistory.Body.String(), `"type":"running"`) {
		t.Fatalf("owner history omitted expected retained event: %s", ownHistory.Body.String())
	}
	ownEvents := principalRequest(handler, http.MethodGet, "/v1/jobs/"+first.ID+"/events", "", aliceToken)
	if !strings.Contains(ownEvents.Body.String(), "event: queued") {
		t.Fatalf("owner event stream omitted job events: %s", ownEvents.Body.String())
	}
	for _, path := range []string{"/v1/operations", "/v1/jobs/known/cancel", "/v1/jobs/known/reconcile", "/v1/jobs/known/disposition/failed", "/v1/jobs/unknown/operator-path"} {
		response := principalRequest(handler, http.MethodGet, path, "", aliceToken)
		if response.Code != http.StatusNotFound || !strings.Contains(response.Body.String(), `"code":"not_found"`) || !strings.Contains(response.Body.String(), `"message":"Resource not found."`) {
			t.Errorf("submitter route %s = %d %s, want generic 404", path, response.Code, response.Body.String())
		}
	}
	for _, method := range []string{http.MethodPost, http.MethodDelete} {
		response := principalRequest(handler, method, "/v1/jobs/"+first.ID+"/cancel", "", aliceToken)
		if response.Code != http.StatusNotFound {
			t.Errorf("submitter %s cancel status=%d body=%s, want generic 404", method, response.Code, response.Body.String())
		}
	}
	// Identity comes only from the resolved credential. The same idempotency key
	// may be used by separate principals without colliding or transferring ownership.
	spoofedIdentity := principalRequest(handler, http.MethodPost, "/v1/jobs", `{"repository":"widget","task":"spoof","principal":"bob"}`, aliceToken)
	assertError(t, spoofedIdentity, http.StatusBadRequest, "unknown_field")
	for _, tc := range []struct{ id, token, task string }{{"alice", aliceToken, "new alice task"}, {"bob", bobToken, "new bob task"}} {
		response := principalRequest(handler, http.MethodPost, "/v1/jobs", `{"repository":"widget","task":"`+tc.task+`"}`, tc.token)
		var accepted struct {
			Job restjobs.Snapshot `json:"job"`
		}
		if response.Code != http.StatusAccepted || json.Unmarshal(response.Body.Bytes(), &accepted) != nil || accepted.Job.Submitter != tc.id {
			t.Fatalf("%s submit status=%d body=%s, submitter=%q", tc.id, response.Code, response.Body.String(), accepted.Job.Submitter)
		}
	}
	aliceJob, _, err := manager.AdmitAs("identity-check", "alice", restjobs.Request{Repository: "widget", Task: "identity"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.GetForPrincipal(aliceJob.ID, "bob"); !errors.Is(err, restjobs.ErrNotFound) {
		t.Fatalf("principal ownership not enforced by store: %v", err)
	}
	operator := principalRequest(handler, http.MethodGet, "/v1/jobs", "", namedPrincipalToken("admin"))
	legacy := request(handler, http.MethodGet, "/v1/jobs", "", true)
	operatorDetail := principalRequest(handler, http.MethodGet, "/v1/jobs/"+first.ID, "", namedPrincipalToken("admin"))
	if operatorDetail.Code != http.StatusOK || !strings.Contains(operatorDetail.Body.String(), first.ID) {
		t.Fatalf("operator cannot inspect another principal's job: %d %s", operatorDetail.Code, operatorDetail.Body.String())
	}
	for label, response := range map[string]*httptest.ResponseRecorder{"named operator": operator, "legacy key": legacy} {
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"snapshot_sequence"`) || strings.Contains(response.Body.String(), `"next_cursor"`) {
			t.Errorf("%s list changed legacy operator response: %d %s", label, response.Code, response.Body.String())
		}
	}
}

func TestSubmitterPaginationCursorIsOpaqueBoundAndExpires(t *testing.T) {
	manager := newTestManager(t, managerConfig(8, 8, 8))
	var bobFirstID string
	var aliceIDs []string
	for _, job := range []struct{ key, owner string }{{"a-1", "alice"}, {"b-1", "bob"}, {"a-2", "alice"}, {"a-3", "alice"}} {
		snapshot, _, err := manager.AdmitAs(job.key, job.owner, restjobs.Request{Repository: "widget", Task: "private"})
		if err != nil {
			t.Fatal(err)
		}
		if job.owner == "bob" {
			bobFirstID = snapshot.ID
		} else {
			aliceIDs = append(aliceIDs, snapshot.ID)
		}
	}
	handler := newPrincipalTestHandler(t, manager, restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true}, restserver.PrincipalConfig{ID: "bob", Role: "submitter", Enabled: true})
	bobJob, _, err := manager.AdmitAs("b-last", "bob", restjobs.Request{Repository: "widget", Task: "bob private"})
	if err != nil {
		t.Fatal(err)
	}
	first := principalRequest(handler, http.MethodGet, "/v1/jobs?limit=1", "", namedPrincipalToken("alice"))
	var page struct {
		Jobs       []restjobs.JobSummary `json:"jobs"`
		HasMore    bool                  `json:"has_more"`
		NextCursor string                `json:"next_cursor"`
	}
	if first.Code != http.StatusOK || json.Unmarshal(first.Body.Bytes(), &page) != nil || len(page.Jobs) != 1 || page.Jobs[0].ID != aliceIDs[0] || !page.HasMore || page.NextCursor == "" {
		t.Fatalf("submitter first page=%d %s", first.Code, first.Body.String())
	}
	for _, forbidden := range []string{"next_sequence", "snapshot_sequence", "after", "snapshot", "private"} {
		if strings.Contains(first.Body.String(), forbidden) {
			t.Fatalf("submitter list exposed %q: %s", forbidden, first.Body.String())
		}
	}
	if len(page.NextCursor) < 40 || strings.Contains(page.NextCursor, "=") {
		t.Fatalf("cursor is not an opaque high-entropy handle: %q", page.NextCursor)
	}
	if _, exists := handler.cursors[page.NextCursor]; !exists {
		t.Fatal("next_cursor not recorded server-side")
	}
	for _, query := range []string{"after=1&snapshot=4", "snapshot=4", "cursor=unknown"} {
		bad := principalRequest(handler, http.MethodGet, "/v1/jobs?"+query, "", namedPrincipalToken("alice"))
		assertError(t, bad, http.StatusBadRequest, "invalid_request")
	}
	wrongOwner := principalRequest(handler, http.MethodGet, "/v1/jobs?cursor="+page.NextCursor, "", namedPrincipalToken("bob"))
	assertError(t, wrongOwner, http.StatusBadRequest, "invalid_request")
	continued := principalRequest(handler, http.MethodGet, "/v1/jobs?limit=1&cursor="+page.NextCursor, "", namedPrincipalToken("alice"))
	var nextPage struct {
		Jobs []restjobs.JobSummary `json:"jobs"`
	}
	if continued.Code != http.StatusOK || json.Unmarshal(continued.Body.Bytes(), &nextPage) != nil || len(nextPage.Jobs) != 1 || nextPage.Jobs[0].ID != aliceIDs[1] || strings.Contains(continued.Body.String(), bobFirstID) || nextPage.Jobs[0].ID == bobJob.ID || strings.Contains(continued.Body.String(), "next_sequence") {
		t.Fatalf("cursor continuation status=%d body=%s", continued.Code, continued.Body.String())
	}
	newHandler := newPrincipalTestHandler(t, manager, restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true})
	invalidAfterRestart := principalRequest(newHandler, http.MethodGet, "/v1/jobs?cursor="+page.NextCursor, "", namedPrincipalToken("alice"))
	assertError(t, invalidAfterRestart, http.StatusBadRequest, "invalid_request")

	handler.now = func() time.Time { return time.Now().Add(submitterCursorTTL + time.Second) }
	expired := principalRequest(handler, http.MethodGet, "/v1/jobs?cursor="+page.NextCursor, "", namedPrincipalToken("alice"))
	assertError(t, expired, http.StatusBadRequest, "invalid_request")
	if _, exists := handler.cursors[page.NextCursor]; exists {
		t.Fatal("expired cursor was not purged")
	}
	if len(handler.cursorKeys) != 0 {
		t.Fatalf("expired cursor key was not purged: %+v", handler.cursorKeys)
	}
}

func TestSubmitterPaginationReplayReusesCursorAcrossPrincipals(t *testing.T) {
	manager := newTestManager(t, managerConfig(4, 4, 4))
	for _, job := range []struct{ key, principal string }{{"alice-1", "alice"}, {"bob-1", "bob"}, {"alice-2", "alice"}, {"bob-2", "bob"}} {
		if _, _, err := manager.AdmitAs(job.key, job.principal, restjobs.Request{Repository: "widget", Task: "task"}); err != nil {
			t.Fatal(err)
		}
	}
	handler := newPrincipalTestHandler(t, manager,
		restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "bob", Role: "submitter", Enabled: true},
	)
	getFirstPage := func(principal string) string {
		t.Helper()
		response := principalRequest(handler, http.MethodGet, "/v1/jobs?limit=1", "", namedPrincipalToken(principal))
		var page struct {
			HasMore    bool   `json:"has_more"`
			NextCursor string `json:"next_cursor"`
		}
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &page) != nil || !page.HasMore || page.NextCursor == "" {
			t.Fatalf("%s first page status=%d body=%s", principal, response.Code, response.Body.String())
		}
		return page.NextCursor
	}

	aliceCursor := getFirstPage("alice")
	if replayCursor := getFirstPage("alice"); replayCursor != aliceCursor {
		t.Fatalf("identical page replay issued a different cursor: first=%q replay=%q", aliceCursor, replayCursor)
	}
	if len(handler.cursors) != 1 || len(handler.cursorKeys) != 1 {
		t.Fatalf("replayed continuation consumed extra slots: cursors=%d keys=%d", len(handler.cursors), len(handler.cursorKeys))
	}
	if bobCursor := getFirstPage("bob"); bobCursor == aliceCursor {
		t.Fatal("different principal unexpectedly shared the same cursor handle")
	}
	if len(handler.cursors) != 2 || len(handler.cursorKeys) != 2 {
		t.Fatalf("second principal cursor state inconsistent: cursors=%d keys=%d", len(handler.cursors), len(handler.cursorKeys))
	}
}

func TestSubmitterCursorCapacityReturnsSanitizedUnavailable(t *testing.T) {
	manager := newTestManager(t, managerConfig(4, 4, 2))
	for range 2 {
		if _, _, err := manager.AdmitAs(fmt.Sprintf("key-%d", time.Now().UnixNano()), "alice", restjobs.Request{Repository: "widget", Task: "task"}); err != nil {
			t.Fatal(err)
		}
	}
	handler := newPrincipalTestHandler(t, manager, restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true})
	now := time.Now()
	for index := range submitterCursorCapacity {
		handler.cursors[fmt.Sprintf("occupied-%d", index)] = submitterCursor{principalID: "alice", expiresAt: now.Add(time.Hour)}
	}
	response := principalRequest(handler, http.MethodGet, "/v1/jobs?limit=1", "", namedPrincipalToken("alice"))
	assertError(t, response, http.StatusServiceUnavailable, "cursor_capacity")
	if strings.Contains(response.Body.String(), "alice") || strings.Contains(response.Body.String(), "occupied") {
		t.Fatalf("cursor capacity response leaked internal state: %s", response.Body.String())
	}
}

func TestListJobsRemainsAvailableAtAdmissionCapacity(t *testing.T) {
	manager := newTestManager(t, managerConfig(1, 1, 2))
	job, _, err := manager.Admit("list-at-capacity", restjobs.Request{Repository: "widget", Task: "private"})
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	response := request(handler, http.MethodGet, "/v1/jobs", "", true)
	var page restjobs.JobPage
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &page) != nil || len(page.Jobs) != 1 || page.Jobs[0].ID != job.ID {
		t.Fatalf("listing at full admission capacity status=%d body=%s", response.Code, response.Body.String())
	}
	if _, _, err := manager.Admit("over-capacity", restjobs.Request{Repository: "widget", Task: "another"}); !errors.Is(err, restjobs.ErrQueueFull) {
		t.Fatalf("admission at full capacity error=%v, want ErrQueueFull", err)
	}
}

func TestListJobsMapsStoreCapacityAndInternalErrorsWithoutLeakage(t *testing.T) {
	manager := newTestManager(t, managerConfig(1, 1, 2))
	readyHandler := newTestHandler(t, errorManager{Manager: manager, listErr: restjobs.ErrRegistryFull}, func() bool { return true })
	response := request(readyHandler, http.MethodGet, "/v1/jobs", "", true)
	assertError(t, response, http.StatusServiceUnavailable, "registry_full")
	failing := newTestHandler(t, errorManager{Manager: manager, listErr: errors.New("private /host/path provider secret")}, func() bool { return true })
	response = request(failing, http.MethodGet, "/v1/jobs", "", true)
	assertError(t, response, http.StatusInternalServerError, "internal_error")
	for _, forbidden := range []string{"private", "/host/path", "provider", "secret"} {
		if strings.Contains(response.Body.String(), forbidden) {
			t.Fatalf("listing error leaked %q: %s", forbidden, response.Body.String())
		}
	}
}

func TestReadinessBoundsConcurrentChecksAndRejectsExcessPromptly(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 2))
	key, err := restserver.LoadAPIKey(writeAPIKey(t, testAPIKey))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, readinessConcurrencyLimit)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	handler, err := New(manager, key, Config{
		MaxRequestBodyBytes: 256,
		MaxTaskBytes:        128,
		RepositoryAliases:   map[string]struct{}{"widget": {}},
		Ready:               func() bool { return true },
		ReadyError: func(context.Context) error {
			inFlight := active.Add(1)
			defer active.Add(-1)
			for prior := maximum.Load(); inFlight > prior && !maximum.CompareAndSwap(prior, inFlight); prior = maximum.Load() {
			}
			started <- struct{}{}
			<-release
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	admitted := make(chan *httptest.ResponseRecorder, readinessConcurrencyLimit)
	for range readinessConcurrencyLimit {
		go func() { admitted <- request(handler, http.MethodGet, "/readyz", "", false) }()
	}
	for range readinessConcurrencyLimit {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("readiness check did not start")
		}
	}

	for _, tc := range []struct {
		method string
		path   string
		body   string
		want   string
	}{
		{method: http.MethodGet, path: "/readyz", want: `{"status":"not_ready"}`},
		{method: http.MethodGet, path: "/v1/jobs/unknown", want: `{"error":{"code":"not_ready","message":"The server is not accepting requests."}}`},
	} {
		startedAt := time.Now()
		response := request(handler, tc.method, tc.path, tc.body, false)
		if response.Code != http.StatusServiceUnavailable || strings.TrimSpace(response.Body.String()) != tc.want {
			t.Errorf("excess %s %s response=%d %q", tc.method, tc.path, response.Code, response.Body.String())
		}
		if elapsed := time.Since(startedAt); elapsed > 100*time.Millisecond {
			t.Errorf("excess %s %s waited %s for readiness capacity", tc.method, tc.path, elapsed)
		}
	}
	if got := maximum.Load(); got != readinessConcurrencyLimit {
		t.Fatalf("maximum concurrent checks=%d, want bounded at %d", got, readinessConcurrencyLimit)
	}
	close(release)
	for range readinessConcurrencyLimit {
		response := <-admitted
		if response.Code != http.StatusOK {
			t.Errorf("admitted readiness status=%d, want 200", response.Code)
		}
	}
}

func TestReadinessCheckUsesRequestContextAndReleasesCapacity(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 2))
	handler := newTestHandler(t, manager, func() bool { return true })
	started := make(chan struct{})
	handler.config.ReadyError = func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	response := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, req)
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("readiness check did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled readiness handler did not return")
	}
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("canceled readiness status=%d, want 503", response.Code)
	}
	handler.config.ReadyError = func(context.Context) error { return nil }
	recovered := request(handler, http.MethodGet, "/readyz", "", false)
	if recovered.Code != http.StatusOK {
		t.Fatalf("readiness after cancellation status=%d, want 200", recovered.Code)
	}
}

func TestRoutesWithoutReadinessCallbackSkipReadinessChecks(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 2))
	handler := newTestHandler(t, manager, nil)
	var readinessChecks atomic.Int32
	handler.config.ReadyError = func(context.Context) error {
		readinessChecks.Add(1)
		return errors.New("unexpected readiness check")
	}
	response := request(handler, http.MethodGet, "/v1/jobs/unknown", "", false)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("route without readiness callback status=%d body=%s, want 401", response.Code, response.Body.String())
	}
	if got := readinessChecks.Load(); got != 0 {
		t.Fatalf("readiness checks without Ready callback=%d, want 0", got)
	}
}

func TestHealthAndReadinessAreMinimalAndUnauthenticated(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 2))
	ready := false
	handler := newTestHandler(t, manager, func() bool { return ready })
	for _, tc := range []struct {
		path string
		want int
		body string
	}{{"/healthz", 200, `{"status":"ok"}`}, {"/readyz", 503, `{"status":"not_ready"}`}} {
		t.Run(tc.path, func(t *testing.T) {
			response := request(handler, http.MethodGet, tc.path, "", false)
			if response.Code != tc.want || strings.TrimSpace(response.Body.String()) != tc.body {
				t.Fatalf("response = %d %s", response.Code, response.Body.String())
			}
		})
	}
	ready = true
	response := request(handler, http.MethodGet, "/readyz", "", false)
	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != `{"status":"ready"}` {
		t.Fatalf("ready response = %d %s", response.Code, response.Body.String())
	}
	if response := request(newTestHandler(t, manager, nil), http.MethodGet, "/readyz", "", false); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil readiness callback status = %d", response.Code)
	}
}

func TestInterruptedDispositionRouteIsAuthenticatedBodylessAndBounded(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("interrupted-disposition", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	called := atomic.Int32{}
	handler := newTestHandler(t, manager, func() bool { return true })
	handler.config.ResolveInterrupted = func(_ context.Context, id string, disposition restjobs.InterruptedDisposition) error {
		called.Add(1)
		if id != job.ID || disposition != restjobs.InterruptedDispositionFailed {
			return restjobs.ErrInvalidTransition
		}
		return nil
	}
	path := "/v1/jobs/" + job.ID + "/disposition/failed"
	if response := request(handler, http.MethodPost, path, "", false); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated disposition status=%d", response.Code)
	}
	if response := request(handler, http.MethodPost, path, `{"status":"succeeded"}`, true); response.Code != http.StatusBadRequest {
		t.Fatalf("body-bearing disposition status=%d body=%s", response.Code, response.Body.String())
	}
	for _, invalid := range []string{"succeeded", "running", "unknown"} {
		if response := request(handler, http.MethodPost, "/v1/jobs/"+job.ID+"/disposition/"+invalid, "", true); response.Code != http.StatusBadRequest {
			t.Errorf("invalid disposition %q status=%d body=%s", invalid, response.Code, response.Body.String())
		}
	}
	response := request(handler, http.MethodPost, path, "", true)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"queued"`) {
		t.Fatalf("valid disposition response=%d body=%s", response.Code, response.Body.String())
	}
	if called.Load() != 1 {
		t.Fatalf("disposition callback count=%d, want one callback for bodyless valid action", called.Load())
	}
	if response := request(handler, http.MethodGet, path, "", true); response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("disposition GET status=%d", response.Code)
	}
}

func TestQueuedJobCancellationIsAuthenticatedAndTerminalizesBeforeExecution(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("cancel-queued", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	path := "/v1/jobs/" + job.ID + "/cancel"
	if response := request(handler, http.MethodPost, path, "", false); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated cancellation status=%d", response.Code)
	}
	if response := request(handler, http.MethodPost, path, ` `, true); response.Code != http.StatusBadRequest {
		t.Fatalf("whitespace-body cancellation status=%d", response.Code)
	}
	response := request(handler, http.MethodPost, path, "", true)
	if response.Code != http.StatusOK {
		t.Fatalf("queued cancellation status=%d body=%s, want 200", response.Code, response.Body.String())
	}
	var canceled restjobs.Snapshot
	if err := json.Unmarshal(response.Body.Bytes(), &canceled); err != nil {
		t.Fatal(err)
	}
	if canceled.ID != job.ID || canceled.Status != restjobs.StatusCanceled {
		t.Fatalf("queued cancellation response=%+v, want same job canceled", canceled)
	}
	if _, err := manager.ClaimNext(); !errors.Is(err, restjobs.ErrNoQueuedJobs) {
		t.Fatalf("claim after queued cancellation=%v, want no queued jobs", err)
	}
	history, err := manager.History(job.ID)
	if err != nil || len(history.Events) != 2 || history.Events[1].Type != string(restjobs.StatusCanceled) {
		t.Fatalf("queued cancellation history=%+v err=%v", history, err)
	}
	if response := request(handler, http.MethodPost, path, "", true); response.Code != http.StatusConflict {
		t.Fatalf("repeat terminal cancellation status=%d body=%s, want 409", response.Code, response.Body.String())
	}
	if response := request(handler, http.MethodGet, path, "", true); response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("cancellation GET status=%d", response.Code)
	}
}

func TestCancellationRouteRepeatsPendingRequestWithoutDuplicateEvent(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("cancel-repeat", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	path := "/v1/jobs/" + job.ID + "/cancel"
	for range 2 {
		response := request(handler, http.MethodPost, path, "", true)
		if response.Code != http.StatusAccepted || !strings.Contains(response.Body.String(), `"cancellation_requested":true`) {
			t.Fatalf("pending cancellation response=%d body=%s, want 202 with pending flag", response.Code, response.Body.String())
		}
	}
	history, err := manager.History(job.ID)
	if err != nil || len(history.Events) != 3 || history.Events[2].Type != "cancel_requested" {
		t.Fatalf("repeated pending cancellation history=%+v err=%v", history, err)
	}
}

func TestCancellationRouteIsBodylessAndRejectsIneligibleJobsWithoutMutation(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("cancel-ineligible", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(job.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	path := "/v1/jobs/" + job.ID + "/cancel"
	if response := request(handler, http.MethodPost, path, `{"status":"canceled"}`, true); response.Code != http.StatusBadRequest {
		t.Fatalf("body-bearing cancellation status=%d body=%s", response.Code, response.Body.String())
	}
	response := request(handler, http.MethodPost, path, "", true)
	assertError(t, response, http.StatusConflict, "job_not_cancelable")
	got, err := manager.Get(job.ID)
	if err != nil || got.Status != restjobs.StatusSucceeded {
		t.Fatalf("terminal job mutated after rejected cancellation: %+v err=%v", got, err)
	}
}

func TestProviderReconciliationRouteRequiresAuthenticationAndEligibility(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("reconcile-route", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	called := atomic.Int32{}
	handler := newTestHandler(t, manager, func() bool { return true })
	handler.config.ReconcileProvider = func(context.Context, string) error {
		called.Add(1)
		return restjobs.ErrInvalidTransition
	}
	if response := request(handler, http.MethodPost, "/v1/jobs/"+job.ID+"/reconcile", "", false); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated reconciliation status=%d", response.Code)
	}
	response := request(handler, http.MethodPost, "/v1/jobs/"+job.ID+"/reconcile", "", true)
	assertError(t, response, http.StatusConflict, "reconciliation_not_allowed")
	if called.Load() != 1 || strings.Contains(response.Body.String(), "secret") {
		t.Fatalf("reconcile callback count=%d response=%s", called.Load(), response.Body.String())
	}
	if response := request(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/reconcile", "", true); response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("reconcile GET status=%d", response.Code)
	}
	if response := request(handler, http.MethodPost, "/v1/jobs/"+job.ID+"/reconcile", `{"number":1}`, true); response.Code != http.StatusBadRequest {
		t.Fatalf("body-bearing reconcile status=%d", response.Code)
	}
}

func TestListJobsIsAuthenticatedBoundedAndReadOnly(t *testing.T) {
	manager := newTestManager(t, managerConfig(3, 4, 8))
	first, _, err := manager.Admit("list-first", restjobs.Request{Repository: "widget", Task: "must never appear in list"})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := manager.Admit("list-second", restjobs.Request{Repository: "widget", Task: "another private task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(first.ID, restjobs.StatusFailed); err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	if response := request(handler, http.MethodGet, "/v1/jobs?limit=1", "", false); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated list status=%d body=%s", response.Code, response.Body.String())
	}
	response := request(handler, http.MethodGet, "/v1/jobs?limit=1", "", true)
	if response.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", response.Code, response.Body.String())
	}
	var page restjobs.JobPage
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 1 || page.Jobs[0].ID != first.ID || page.Jobs[0].Status != restjobs.StatusFailed || !page.HasMore || page.NextSequence == 0 || page.SnapshotSequence == 0 {
		t.Fatalf("first page=%+v want oldest retained record and cursor", page)
	}
	if strings.Contains(response.Body.String(), "must never appear") || strings.Contains(response.Body.String(), "another private task") || strings.Contains(response.Body.String(), "request") || strings.Contains(response.Body.String(), "provider") {
		t.Fatalf("list exposed payload or provider information: %s", response.Body.String())
	}
	third, _, err := manager.Admit("list-third", restjobs.Request{Repository: "widget", Task: "arrived between pages"})
	if err != nil {
		t.Fatal(err)
	}
	path := fmt.Sprintf("/v1/jobs?limit=1&after=%d&snapshot=%d", page.NextSequence, page.SnapshotSequence)
	response = request(handler, http.MethodGet, path, "", true)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), second.ID) || strings.Contains(response.Body.String(), third.ID) || strings.Contains(response.Body.String(), "arrived between pages") {
		t.Fatalf("continuation status=%d body=%s; must include second and exclude later admission", response.Code, response.Body.String())
	}
	before, err := manager.Get(second.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.History(second.ID); err != nil {
		t.Fatal(err)
	}
	beforeList, err := manager.ListJobs(context.Background(), 0, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	continuation := fmt.Sprintf("/v1/jobs?limit=1&after=%d&snapshot=%d", page.NextSequence, page.SnapshotSequence)
	if got := request(handler, http.MethodGet, continuation, "", true); got.Code != http.StatusOK {
		t.Fatalf("read-only continuation status=%d body=%s", got.Code, got.Body.String())
	}
	afterList, err := manager.ListJobs(context.Background(), 0, 0, 100)
	if err != nil || len(beforeList.Jobs) != len(afterList.Jobs) {
		t.Fatalf("listing changed retained record set: before=%+v after=%+v err=%v", beforeList, afterList, err)
	}
	after, err := manager.Get(second.ID)
	if err != nil || before.Status != after.Status || !before.UpdatedAt.Equal(after.UpdatedAt) {
		t.Fatalf("listing mutated job state: before=%+v after=%+v err=%v", before, after, err)
	}
	for _, invalid := range []string{"0", "-1", "101", "nope"} {
		if got := request(handler, http.MethodGet, "/v1/jobs?limit="+invalid, "", true); got.Code != http.StatusBadRequest {
			t.Errorf("invalid limit %q status=%d body=%s", invalid, got.Code, got.Body.String())
		}
	}
	for _, query := range []string{"after=1", "limit=1&after=2&snapshot=1", "limit=1&unknown=x", "limit=1&limit=2"} {
		if got := request(handler, http.MethodGet, "/v1/jobs?"+query, "", true); got.Code != http.StatusBadRequest {
			t.Errorf("invalid query %q status=%d body=%s", query, got.Code, got.Body.String())
		}
	}
	if got := request(handler, http.MethodDelete, "/v1/jobs", "", true); got.Code != http.StatusMethodNotAllowed || got.Header().Get("Allow") != "GET, POST" {
		t.Fatalf("collection DELETE status=%d allow=%q", got.Code, got.Header().Get("Allow"))
	}
}

func TestAuthenticationPrecedesJobPathResolution(t *testing.T) {
	handler := newTestHandler(t, newTestManager(t, managerConfig(2, 2, 2)), func() bool { return true })
	for _, path := range []string{"/v1/jobs", "/v1/jobs/unknown", "/v1/jobs/unknown/nope"} {
		response := request(handler, http.MethodGet, path, "", false)
		if response.Code != http.StatusUnauthorized {
			t.Errorf("GET %s unauthenticated = %d, want 401", path, response.Code)
		}
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	r.Header.Add("Authorization", "Bearer "+testAPIKey)
	r.Header.Add("Authorization", "Bearer second")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, r)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("duplicate Authorization headers status = %d", response.Code)
	}
}

func TestOperationsSummaryIsAuthenticatedBoundedAndSanitized(t *testing.T) {
	manager := newTestManager(t, managerConfig(1, 3, 4))
	running, _, err := manager.Admit("summary-1", restjobs.Request{Repository: "widget", Task: "top-secret task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	queued, _, err := manager.Admit("summary-2", restjobs.Request{Repository: "widget", Task: "another hidden task"})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(running.ID, "secret", "private event transcript"); err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	if response := request(handler, http.MethodGet, "/v1/operations", "", false); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated operations status=%d, body=%s", response.Code, response.Body.String())
	}
	response := request(handler, http.MethodGet, "/v1/operations", "", true)
	if response.Code != http.StatusOK {
		t.Fatalf("operations response = %d %s", response.Code, response.Body.String())
	}
	var summary restjobs.OperationalSummary
	if err := json.Unmarshal(response.Body.Bytes(), &summary); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"retained_records": true, "record_limit": true, "queue_capacity": true,
		"queued": true, "running": true, "succeeded": true, "failed": true,
		"canceled": true, "queue_saturated": true, "recovery_needed": true,
	}
	for field := range fields {
		if !allowed[field] {
			t.Errorf("operations response contains unexpected field %q", field)
		}
	}
	if len(fields) != len(allowed) {
		t.Errorf("operations response fields = %v, want exactly %d count-only fields", fields, len(allowed))
	}
	if summary.RetainedRecords != 2 || summary.Queued != 1 || summary.Running != 1 || summary.RecoveryNeeded != 0 || !summary.QueueSaturated {
		t.Fatalf("operations summary = %+v", summary)
	}
	for _, secret := range []string{running.ID, queued.ID, "top-secret task", "another hidden task", "private event transcript", "summary-1"} {
		if strings.Contains(response.Body.String(), secret) {
			t.Errorf("operations response leaked %q: %s", secret, response.Body.String())
		}
	}
}

func TestOperationsSummaryStoreFailureIsSanitized(t *testing.T) {
	manager := errorSummaryManager{Manager: newTestManager(t, managerConfig(1, 1, 1)), err: errors.New("/private/store.db credential=never-log")}
	handler := newTestHandler(t, manager, func() bool { return true })
	response := request(handler, http.MethodGet, "/v1/operations", "", true)
	assertError(t, response, http.StatusInternalServerError, "internal_error")
	if strings.Contains(response.Body.String(), "private/store.db") || strings.Contains(response.Body.String(), "never-log") {
		t.Fatalf("operations error leaked internal details: %s", response.Body.String())
	}
}

func TestAdmissionReplayConflictAndQueueFull(t *testing.T) {
	manager := newTestManager(t, managerConfig(1, 2, 4))
	handler := newTestHandler(t, manager, nil)
	first := postJob(handler, `{"repository":"widget","task":"Fix the queue.","issue":12}`, "retry-1")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first admission = %d %s", first.Code, first.Body.String())
	}
	var accepted struct {
		Job      restjobs.Snapshot `json:"job"`
		Replayed bool              `json:"replayed"`
		Links    linksDTO          `json:"links"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.Replayed || accepted.Job.Request.Issue == nil || *accepted.Job.Request.Issue != 12 || accepted.Links.Self != "/v1/jobs/"+accepted.Job.ID || accepted.Links.History != accepted.Links.Self+"/history" {
		t.Fatalf("unexpected admission DTO: %+v", accepted)
	}
	replay := postJob(handler, `{"repository":"widget","task":"Fix the queue.","issue":12}`, "retry-1")
	var replayDTO struct {
		Job      restjobs.Snapshot `json:"job"`
		Replayed bool              `json:"replayed"`
	}
	if replay.Code != http.StatusAccepted || json.Unmarshal(replay.Body.Bytes(), &replayDTO) != nil || !replayDTO.Replayed || replayDTO.Job.ID != accepted.Job.ID {
		t.Fatalf("replay = %d %s", replay.Code, replay.Body.String())
	}
	conflict := postJob(handler, `{"repository":"widget","task":"Different request"}`, "retry-1")
	assertError(t, conflict, http.StatusConflict, "idempotency_conflict")
	full := postJob(handler, `{"repository":"widget","task":"Another task"}`, "retry-2")
	assertError(t, full, http.StatusServiceUnavailable, "queue_full")
}

func TestIssueProvenanceAdmissionReplayAndSanitizedConflict(t *testing.T) {
	manager := newTestManager(t, managerConfig(4, 4, 8))
	handler := newTestHandler(t, manager, nil)
	handler.config.MaxRequestBodyBytes = 2048
	body := `{"repository":"widget","task":"visible issue task","issue":42,"issue_provenance":{"tracker":"github","source_repository":"acme/widget","issue_number":42,"version":"` + strings.Repeat("a", 64) + `","url":"https://github.com/acme/widget/issues/42","title":"Sanitized title"}}`
	first := postJob(handler, body, "issue-key-one")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first = %d %s", first.Code, first.Body.String())
	}
	var accepted struct {
		Job      restjobs.Snapshot `json:"job"`
		Replayed bool              `json:"replayed"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.Job.Request.Task != "visible issue task" || accepted.Job.Request.IssueProvenance == nil {
		t.Fatalf("accepted request=%+v", accepted.Job.Request)
	}
	detail := request(handler, http.MethodGet, "/v1/jobs/"+accepted.Job.ID, "", true)
	if detail.Code != http.StatusOK || !strings.Contains(detail.Body.String(), `"task":"visible issue task"`) || !strings.Contains(detail.Body.String(), `"issue_provenance"`) {
		t.Fatalf("authenticated job detail did not preserve visible task/provenance: %d %s", detail.Code, detail.Body.String())
	}
	provenance := request(handler, http.MethodGet, "/v1/jobs/"+accepted.Job.ID+"/issue-provenance", "", true)
	if provenance.Code != http.StatusOK {
		t.Fatalf("provenance inspection = %d %s", provenance.Code, provenance.Body.String())
	}
	escapedPath := request(handler, http.MethodGet, "/v1/jobs/"+accepted.Job.ID+"%2Fissue-provenance", "", true)
	assertError(t, escapedPath, http.StatusNotFound, "not_found")
	if strings.Contains(escapedPath.Body.String(), "visible issue task") {
		t.Fatalf("escaped path exposed job data: %s", escapedPath.Body.String())
	}
	var provenanceFields map[string]json.RawMessage
	if err := json.Unmarshal(provenance.Body.Bytes(), &provenanceFields); err != nil {
		t.Fatal(err)
	}
	if len(provenanceFields) != 2 || provenanceFields["job_id"] == nil || provenanceFields["issue_provenance"] == nil || strings.Contains(provenance.Body.String(), "visible issue task") {
		t.Fatalf("provenance projection was not narrowly bounded: %s", provenance.Body.String())
	}
	ordinary, _, err := manager.Admit("no-issue-provenance", restjobs.Request{Repository: "widget", Task: "ordinary task"})
	if err != nil {
		t.Fatal(err)
	}
	absent := request(handler, http.MethodGet, "/v1/jobs/"+ordinary.ID+"/issue-provenance", "", true)
	assertError(t, absent, http.StatusNotFound, "not_found")
	unknownProvenance := request(handler, http.MethodGet, "/v1/jobs/unknown/issue-provenance", "", true)
	if absent.Body.String() != unknownProvenance.Body.String() {
		t.Fatalf("absent provenance disclosed job existence: absent=%s unknown=%s", absent.Body.String(), unknownProvenance.Body.String())
	}
	second := postJob(handler, body, "issue-key-two")
	var replay struct {
		Job      restjobs.Snapshot `json:"job"`
		Replayed bool              `json:"replayed"`
	}
	if second.Code != http.StatusAccepted || json.Unmarshal(second.Body.Bytes(), &replay) != nil || !replay.Replayed || replay.Job.ID != accepted.Job.ID {
		t.Fatalf("different-key provenance replay=%d %s", second.Code, second.Body.String())
	}
	changed := strings.Replace(body, "visible issue task", "DIFFERENT_PRIVATE_TASK", 1)
	conflict := postJob(handler, changed, "issue-key-three")
	assertError(t, conflict, http.StatusConflict, "issue_provenance_conflict")
	if strings.Contains(conflict.Body.String(), "DIFFERENT_PRIVATE_TASK") || strings.Contains(conflict.Body.String(), "Sanitized title") {
		t.Fatalf("provenance conflict leaked request metadata: %s", conflict.Body.String())
	}
	omitted := postJob(handler, `{"repository":"widget","task":"visible issue task","issue":42}`, "issue-key-one")
	assertError(t, omitted, http.StatusConflict, "idempotency_conflict")
}

func TestMalformedIssueProvenanceIsRejected(t *testing.T) {
	handler := newTestHandler(t, newTestManager(t, managerConfig(4, 4, 8)), nil)
	handler.config.MaxRequestBodyBytes = 2048
	valid := `"issue_provenance":{"tracker":"github","source_repository":"acme/widget","issue_number":42,"version":"` + strings.Repeat("a", 64) + `","url":"https://github.com/acme/widget/issues/42","title":"Safe"}`
	for _, tc := range []struct{ name, provenance string }{
		{"unknown nested field", strings.TrimSuffix(valid, `}`) + `,"body":"duplicate body"}`},

		{"wrong tracker", strings.Replace(valid, `"github"`, `"gitlab"`, 1)},
		{"uppercase version", strings.Replace(valid, strings.Repeat("a", 64), strings.Repeat("A", 64), 1)},
		{"mismatched issue", strings.Replace(valid, `"issue_number":42`, `"issue_number":43`, 1)},
		{"invalid url", strings.Replace(valid, `https://github.com/acme/widget/issues/42`, `https://evil.example/issues/42`, 1)},
		{"non-object", `"issue_provenance":null`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"repository":"widget","task":"task",` + tc.provenance + `}`
			response := postJob(handler, body, "bad-"+strings.ReplaceAll(tc.name, " ", "-"))
			assertError(t, response, http.StatusBadRequest, "invalid_request")
		})
	}
}

func TestAdmissionRegistryFull(t *testing.T) {
	manager := newTestManager(t, managerConfig(1, 1, 4))
	first, _, err := manager.Admit("key-1", restjobs.Request{Repository: "widget", Task: "active"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, nil)
	response := postJob(handler, `{"repository":"widget","task":"cannot fit"}`, "key-2")
	assertError(t, response, http.StatusServiceUnavailable, "registry_full")
	if _, err := manager.Get(first.ID); err != nil {
		t.Fatalf("existing active job lost: %v", err)
	}
}

func TestRequestSchemaValidationAndBodyBounds(t *testing.T) {
	handler := newTestHandler(t, newTestManager(t, managerConfig(4, 4, 4)), nil)
	cases := []struct {
		name, body, contentType string
		status                  int
		code                    string
	}{
		{"issue URL", `{"repository":"widget","task":"valid","issue":"https://github.com/org/repo/issues/1"}`, "application/json", 400, "invalid_request"},
		{"nonpositive issue", `{"repository":"widget","task":"valid","issue":0}`, "application/json", 400, "invalid_request"},
		{"unknown alias", `{"repository":"secret-path","task":"valid"}`, "application/json", 400, "invalid_request"},
		{"blank task", `{"repository":"widget","task":"  "}`, "application/json", 400, "invalid_request"},
		{"unknown field", `{"repository":"widget","task":"valid","command":"oops"}`, "application/json", 400, "unknown_field"},
		{"duplicate field", `{"repository":"widget","repository":"widget","task":"valid"}`, "application/json", 400, "malformed_json"},
		{"nested duplicate", `{"repository":"widget","task":"valid","issue":1,"x":{"a":1,"a":2}}`, "application/json", 400, "malformed_json"},
		{"trailing value", `{"repository":"widget","task":"valid"} {}`, "application/json", 400, "malformed_json"},
		{"malformed", `{"repository":`, "application/json", 400, "malformed_json"},
		{"invalid utf8", string([]byte{'{', '"', 'r', 'e', 'p', 'o', 's', 'i', 't', 'o', 'r', 'y', '"', ':', '"', 0xff, '"', '}'}), "application/json", 400, "invalid_request"},
		{"unsupported media", `{"repository":"widget","task":"valid"}`, "text/plain", 415, "unsupported_media_type"},
		{"oversized", `{"repository":"widget","task":"` + strings.Repeat("x", 230) + `"}`, "application/json", 413, "body_too_large"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(tc.body))
			r.Header.Set("Authorization", "Bearer "+testAPIKey)
			r.Header.Set("Idempotency-Key", "key-"+tc.name)
			r.Header.Set("Content-Type", tc.contentType)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, r)
			assertError(t, response, tc.status, tc.code)
		})
	}
	missingHeader := postJobWithHeaders(handler, `{"repository":"widget","task":"valid"}`, "")
	assertError(t, missingHeader, 400, "invalid_request")
	duplicateHeaderRequest := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(`{"repository":"widget","task":"valid"}`))
	duplicateHeaderRequest.Header.Set("Authorization", "Bearer "+testAPIKey)
	duplicateHeaderRequest.Header.Add("Idempotency-Key", "one")
	duplicateHeaderRequest.Header.Add("Idempotency-Key", "two")
	duplicateHeaderRequest.Header.Set("Content-Type", "application/json")
	duplicateHeaderResponse := httptest.NewRecorder()
	handler.ServeHTTP(duplicateHeaderResponse, duplicateHeaderRequest)
	assertError(t, duplicateHeaderResponse, 400, "invalid_request")
}

func TestTaskLimitIsEnforcedBeforeAdmission(t *testing.T) {
	handler := newTestHandler(t, newTestManager(t, managerConfig(4, 4, 4)), nil)
	response := postJob(handler, `{"repository":"widget","task":"`+strings.Repeat("x", 129)+`"}`, "too-long-task")
	assertError(t, response, 400, "invalid_request")
}

func TestAPIAndManagerAcceptMaximumTaskBytes(t *testing.T) {
	const taskLimit = 256 << 10
	manager := newTestManager(t, restjobs.Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 2, MaxTaskBytes: taskLimit})
	key, err := restserver.LoadAPIKey(writeAPIKey(t, testAPIKey))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(manager, key, Config{MaxRequestBodyBytes: 512 << 10, MaxTaskBytes: taskLimit, RepositoryAliases: map[string]struct{}{"widget": {}}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(struct {
		Repository string `json:"repository"`
		Task       string `json:"task"`
	}{Repository: "widget", Task: strings.Repeat("x", taskLimit)})
	if err != nil {
		t.Fatal(err)
	}
	response := postJob(handler, string(body), "maximum-task")
	if response.Code != http.StatusAccepted {
		t.Fatalf("maximum task request status = %d body=%s", response.Code, response.Body.String())
	}
	overLimit, err := json.Marshal(struct {
		Repository string `json:"repository"`
		Task       string `json:"task"`
	}{Repository: "widget", Task: strings.Repeat("x", taskLimit+1)})
	if err != nil {
		t.Fatal(err)
	}
	assertError(t, postJob(handler, string(overLimit), "over-maximum-task"), http.StatusBadRequest, "invalid_request")
}

func TestStatusHistoryTruncationAndSafeErrors(t *testing.T) {
	manager := newTestManager(t, managerConfig(4, 4, 3))
	snapshot, _, err := manager.Admit("status-key", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(snapshot.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(snapshot.ID, "failed", "/host/path secret stack"); err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, nil)
	status := request(handler, http.MethodGet, "/v1/jobs/"+snapshot.ID, "", true)
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"task":"task"`) {
		t.Fatalf("status response = %d %s", status.Code, status.Body.String())
	}
	history := request(handler, http.MethodGet, "/v1/jobs/"+snapshot.ID+"/history", "", true)
	var historyDTO restjobs.History
	if history.Code != http.StatusOK || json.Unmarshal(history.Body.Bytes(), &historyDTO) != nil || !historyDTO.Truncated || len(historyDTO.Events) != 3 {
		t.Fatalf("history response = %d %s", history.Code, history.Body.String())
	}
	if historyDTO.Events[0].Type != "running" || historyDTO.Events[1].Type != "succeeded" || historyDTO.Events[2].Type != "failed" || historyDTO.Events[2].Message != "" || historyDTO.LatestSequence != 0 || historyDTO.Events[0].Sequence != 0 {
		t.Fatalf("retained history was not safely filtered: %+v", historyDTO.Events)
	}
	for _, forbidden := range []string{"/host/path", "secret", "stack"} {
		if strings.Contains(history.Body.String(), forbidden) {
			t.Fatalf("history exposed %q: %s", forbidden, history.Body.String())
		}
	}
	missing := request(handler, http.MethodGet, "/v1/jobs/missing", "", true)
	assertError(t, missing, 404, "not_found")

	failing := newTestHandler(t, errorManager{Manager: manager, getErr: errors.New("raw /host/path secret stack")}, nil)
	internal := request(failing, http.MethodGet, "/v1/jobs/"+snapshot.ID, "", true)
	assertError(t, internal, 500, "internal_error")
	for _, forbidden := range []string{"raw", "/host/path", "secret", "stack"} {
		if strings.Contains(internal.Body.String(), forbidden) {
			t.Fatalf("internal error leaked %q: %s", forbidden, internal.Body.String())
		}
	}
}

func TestClosedManagerReturnsStableUnavailableResponse(t *testing.T) {
	manager := newTestManager(t, managerConfig(1, 1, 1))
	manager.Close()
	handler := newTestHandler(t, manager, func() bool { return true })
	response := postJob(handler, `{"repository":"widget","task":"task"}`, "key")
	assertError(t, response, http.StatusServiceUnavailable, "server_shutting_down")
}

func TestMethodsPathsAndAllowHeaders(t *testing.T) {
	handler := newTestHandler(t, newTestManager(t, managerConfig(4, 4, 4)), nil)
	for _, tc := range []struct {
		method, path, allow string
		status              int
		auth                bool
	}{
		{http.MethodPost, "/healthz", "GET", 405, false},
		{http.MethodPost, "/readyz", "GET", 405, false},
		{http.MethodDelete, "/v1/jobs", "GET, POST", 405, true},
		{http.MethodPost, "/v1/operations", "GET", 405, true},
		{http.MethodPost, "/v1/jobs/id", "GET", 405, true},
		{http.MethodPost, "/v1/jobs/id/history", "GET", 405, true},
		{http.MethodPost, "/v1/jobs/id/events", "GET", 405, true},
		{http.MethodGet, "/v1/jobs/id/unknown", "", 404, true},
		{http.MethodGet, "/v1/jobsx", "", 404, false},
		{http.MethodGet, "/v1/operations/extra", "", 404, true},
		{http.MethodGet, "/v1/jobs/%69d", "", 404, true},
	} {
		t.Run(fmt.Sprintf("%s %s", tc.method, tc.path), func(t *testing.T) {
			response := request(handler, tc.method, tc.path, "", tc.auth)
			if response.Code != tc.status {
				t.Fatalf("status = %d, body %s", response.Code, response.Body.String())
			}
			if tc.allow != "" && response.Header().Get("Allow") != tc.allow {
				t.Fatalf("Allow = %q, want %q", response.Header().Get("Allow"), tc.allow)
			}
		})
	}
}

type errorManager struct {
	restjobs.Manager
	getErr  error
	listErr error
}

func (m errorManager) Get(id string) (restjobs.Snapshot, error) { return restjobs.Snapshot{}, m.getErr }
func (m errorManager) ListJobs(context.Context, uint64, uint64, int) (restjobs.JobPage, error) {
	if m.listErr != nil {
		return restjobs.JobPage{}, m.listErr
	}
	return m.Manager.ListJobs(context.Background(), 0, 0, 1)
}

type errorSummaryManager struct {
	restjobs.Manager
	err error
}

func (m errorSummaryManager) OperationalSummary(context.Context) (restjobs.OperationalSummary, error) {
	return restjobs.OperationalSummary{}, m.err
}

func postJob(handler http.Handler, body, key string) *httptest.ResponseRecorder {
	return postJobWithHeaders(handler, body, key)
}

func postJobWithHeaders(handler http.Handler, body, key string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+testAPIKey)
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	r.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, r)
	return response
}

func assertError(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	var envelope struct {
		Error errorDTO `json:"error"`
	}
	if response.Code != status || json.Unmarshal(response.Body.Bytes(), &envelope) != nil || envelope.Error.Code != code || envelope.Error.Message == "" {
		t.Fatalf("error response = %d %s, want %d %s", response.Code, response.Body.String(), status, code)
	}
}

func TestReadinessCanChangeWithAtomicCallback(t *testing.T) {
	var ready atomic.Bool
	handler := newTestHandler(t, newTestManager(t, managerConfig(1, 1, 1)), ready.Load)
	if got := request(handler, http.MethodGet, "/readyz", "", false).Code; got != 503 {
		t.Fatalf("initial readiness = %d", got)
	}
	ready.Store(true)
	if got := request(handler, http.MethodGet, "/readyz", "", false).Code; got != 200 {
		t.Fatalf("updated readiness = %d", got)
	}
}
