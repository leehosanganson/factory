package restapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
)

func TestScheduleRoutesRequireDurableStoreAndNamedSubmitter(t *testing.T) {
	memory := newTestManager(t, managerConfig(4, 4, 4))
	handler := newPrincipalTestHandler(t, memory,
		restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true},
	)
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodGet, "/v1/schedules", ""},
		{http.MethodPost, "/v1/schedules", `{"cron":"* * * * *","repository":"widget","task":"task"}`},
		{http.MethodGet, "/v1/schedules/schedule-id", ""},
		{http.MethodPatch, "/v1/schedules/schedule-id", `{"action":"pause"}`},
		{http.MethodDelete, "/v1/schedules/schedule-id", ""},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			r.Header.Set("Authorization", "Bearer "+namedPrincipalToken("alice"))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Idempotency-Key", "schedule-key")
			r.Header.Set("If-Match", `"1"`)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			assertError(t, w, http.StatusServiceUnavailable, "durable_store_required")
		})
	}
	for _, token := range []string{namedPrincipalToken("admin"), testAPIKey} {
		for _, tc := range []struct{ method, path, body string }{
			{http.MethodGet, "/v1/schedules", ""},
			{http.MethodPost, "/v1/schedules", `{"cron":"* * * * *","repository":"widget","task":"task"}`},
			{http.MethodGet, "/v1/schedules/schedule-id", ""},
			{http.MethodPatch, "/v1/schedules/schedule-id", `{"action":"pause"}`},
			{http.MethodDelete, "/v1/schedules/schedule-id", ""},
		} {
			got := principalRequest(handler, tc.method, tc.path, tc.body, token)
			want := http.StatusNotFound
			if tc.method == http.MethodGet && (tc.path == "/v1/schedules" || tc.path == "/v1/schedules/schedule-id") || tc.method == http.MethodPatch || tc.method == http.MethodDelete {
				want = http.StatusServiceUnavailable
			}
			if got.Code != want {
				t.Errorf("principal %q %s %s status=%d body=%s; want %d", token, tc.method, tc.path, got.Code, got.Body.String(), want)
			}
		}
	}
}

func TestScheduleAPIExposesOnlyBoundedOwnedDispatchSummary(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	config := restjobs.Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128}
	store, err := restjobs.OpenSQLiteStore(filepath.Join(dir, "schedule-response.db"), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	handler := newPrincipalTestHandler(t, store, restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true}, restserver.PrincipalConfig{ID: "bob", Role: "submitter", Enabled: true})
	create := httptest.NewRequest(http.MethodPost, "/v1/schedules", strings.NewReader(`{"cron":"* * * * *","repository":"widget","task":"bounded scheduled task"}`))
	create.Header.Set("Authorization", "Bearer "+namedPrincipalToken("alice"))
	create.Header.Set("Idempotency-Key", "summary-test")
	create.Header.Set("Content-Type", "application/json")
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, create)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	var createdBody struct {
		Schedule struct {
			ID string `json:"id"`
		} `json:"schedule"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &createdBody); err != nil {
		t.Fatal(err)
	}
	schedule, err := store.GetSchedule(createdBody.Schedule.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"fill-one", "fill-two", "fill-three"} {
		if _, _, err := store.Admit(key, restjobs.Request{Repository: "widget", Task: "fill queue"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DispatchScheduleOccurrences(context.Background(), schedule.NextOccurrence(), 1); err != nil {
		t.Fatal(err)
	}
	if err := store.DispatchScheduleOccurrences(context.Background(), schedule.NextOccurrence().Add(2*time.Minute), 1); err != nil {
		t.Fatal(err)
	}
	get := principalRequest(handler, http.MethodGet, "/v1/schedules/"+schedule.ID, "", namedPrincipalToken("alice"))
	if get.Code != http.StatusOK {
		t.Fatalf("get status=%d body=%s", get.Code, get.Body.String())
	}
	var response map[string]json.RawMessage
	if err := json.Unmarshal(get.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"pending_occurrence_at", "skipped_occurrences"} {
		if _, ok := response[field]; !ok {
			t.Errorf("schedule response lacks %q: %s", field, get.Body.String())
		}
	}
	for _, field := range []string{"next_occurrence_at", "pendingOccurrenceAt", "skippedRevision", "skippedCount"} {
		if _, ok := response[field]; ok {
			t.Errorf("schedule response leaked internal field %q: %s", field, get.Body.String())
		}
	}
	foreign := principalRequest(handler, http.MethodGet, "/v1/schedules/"+schedule.ID, "", namedPrincipalToken("bob"))
	if foreign.Code != http.StatusNotFound {
		t.Fatalf("foreign schedule status=%d body=%s", foreign.Code, foreign.Body.String())
	}
	list := principalRequest(handler, http.MethodGet, "/v1/schedules", "", namedPrincipalToken("alice"))
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), `"pending_occurrence_at"`) || !strings.Contains(list.Body.String(), `"skipped_occurrences"`) {
		t.Fatalf("owned schedule list status=%d body=%s", list.Code, list.Body.String())
	}
}

func TestScheduleHistoryPaginationIsOrderedBoundedAndMetadataOnly(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := restjobs.OpenSQLiteStore(filepath.Join(dir, "schedule-pages.db"), restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	handler := newPrincipalTestHandler(t, store, restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true}, restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true})
	createRequest := httptest.NewRequest(http.MethodPost, "/v1/schedules", strings.NewReader(`{"cron":"* * * * *","repository":"widget","task":"private task text"}`))
	createRequest.Header.Set("Authorization", "Bearer "+namedPrincipalToken("alice"))
	createRequest.Header.Set("Content-Type", "application/json")
	createRequest.Header.Set("Idempotency-Key", "schedule-page-create")
	create := httptest.NewRecorder()
	handler.ServeHTTP(create, createRequest)
	if create.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	var created struct {
		Schedule restjobs.Schedule `json:"schedule"`
	}
	if err := json.Unmarshal(create.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	id := created.Schedule.ID
	for revision := int64(1); revision <= 3; revision++ {
		mutation := restjobs.SchedulePause
		if revision == 2 {
			mutation = restjobs.ScheduleResume
		}
		if _, err := store.MutateScheduleAs(id, "admin", true, revision, mutation, nil); err != nil {
			t.Fatal(err)
		}
	}
	first := principalRequest(handler, http.MethodGet, "/v1/schedules/"+id+"/history?limit=2", "", namedPrincipalToken("admin"))
	if first.Code != http.StatusOK {
		t.Fatalf("first page status=%d body=%s", first.Code, first.Body.String())
	}
	var firstPage struct {
		Events       []restjobs.ScheduleAuditEvent `json:"events"`
		HasMore      bool                          `json:"has_more"`
		NextRevision int64                         `json:"next_revision"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstPage); err != nil {
		t.Fatal(err)
	}
	if len(firstPage.Events) != 2 || !firstPage.HasMore || firstPage.NextRevision != 2 {
		t.Fatalf("first page=%+v", firstPage)
	}
	if strings.Contains(first.Body.String(), "private task text") {
		t.Fatalf("history leaked schedule definition: %s", first.Body.String())
	}
	second := principalRequest(handler, http.MethodGet, "/v1/schedules/"+id+"/history?after_revision=2&limit=2", "", namedPrincipalToken("admin"))
	var secondPage struct {
		Events  []restjobs.ScheduleAuditEvent `json:"events"`
		HasMore bool                          `json:"has_more"`
	}
	if second.Code != http.StatusOK || json.Unmarshal(second.Body.Bytes(), &secondPage) != nil || len(secondPage.Events) != 2 || secondPage.Events[0].Revision != 3 || secondPage.Events[1].Revision != 4 || secondPage.HasMore {
		t.Fatalf("second page status=%d body=%s page=%+v", second.Code, second.Body.String(), secondPage)
	}
	for _, query := range []string{"?limit=0", "?limit=101", "?after_revision=-1", "?limit=1&limit=2", "?unknown=1"} {
		got := principalRequest(handler, http.MethodGet, "/v1/schedules/"+id+"/history"+query, "", namedPrincipalToken("admin"))
		if got.Code != http.StatusBadRequest {
			t.Errorf("query %s status=%d body=%s", query, got.Code, got.Body.String())
		}
	}
}

type scheduleHistoryErrorStore struct {
	restjobs.Manager
	restjobs.ScheduleStore
}

func (s scheduleHistoryErrorStore) ListScheduleHistory(string, string, bool, int64, int) (restjobs.ScheduleAuditPage, error) {
	return restjobs.ScheduleAuditPage{}, errors.New("database error with private details")
}

func TestScheduleHistoryOperationalStoreErrorReturnsInternalError(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := restjobs.OpenSQLiteStore(filepath.Join(dir, "schedule-history-error.db"), restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	created, _, err := store.CreateSchedule("alice", "history-error", restjobs.ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	handler := newPrincipalTestHandler(t, scheduleHistoryErrorStore{Manager: store, ScheduleStore: store}, restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true})
	got := principalRequest(handler, http.MethodGet, "/v1/schedules/"+created.ID+"/history", "", namedPrincipalToken("admin"))
	if got.Code != http.StatusInternalServerError {
		t.Fatalf("history status=%d body=%s; want internal error for store failure", got.Code, got.Body.String())
	}
	if !strings.Contains(got.Body.String(), `"code":"internal_error"`) || strings.Contains(got.Body.String(), "private details") {
		t.Fatalf("history response did not safely map store error: %s", got.Body.String())
	}
}

func TestScheduleOperatorAuditAndOwnerHistory(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := restjobs.OpenSQLiteStore(filepath.Join(dir, "schedule-audit.db"), restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	handler := newPrincipalTestHandler(t, store, restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true}, restserver.PrincipalConfig{ID: "bob", Role: "submitter", Enabled: true}, restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true})
	create := httptest.NewRequest(http.MethodPost, "/v1/schedules", strings.NewReader(`{"cron":"* * * * *","repository":"widget","task":"private task text"}`))
	create.Header.Set("Authorization", "Bearer "+namedPrincipalToken("alice"))
	create.Header.Set("Content-Type", "application/json")
	create.Header.Set("Idempotency-Key", "audit-create")
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, create)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	var body struct {
		Schedule restjobs.Schedule `json:"schedule"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	id := body.Schedule.ID
	list := principalRequest(handler, http.MethodGet, "/v1/schedules", "", namedPrincipalToken("admin"))
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), id) {
		t.Fatalf("operator list status=%d body=%s", list.Code, list.Body.String())
	}
	mutate := httptest.NewRequest(http.MethodPatch, "/v1/schedules/"+id, strings.NewReader(`{"action":"pause"}`))
	mutate.Header.Set("Authorization", "Bearer "+namedPrincipalToken("admin"))
	mutate.Header.Set("Content-Type", "application/json")
	mutate.Header.Set("If-Match", `"1"`)
	mutated := httptest.NewRecorder()
	handler.ServeHTTP(mutated, mutate)
	if mutated.Code != http.StatusOK || mutated.Header().Get("ETag") != `"2"` {
		t.Fatalf("operator mutate status=%d body=%s", mutated.Code, mutated.Body.String())
	}
	for _, principal := range []string{"admin", "alice"} {
		history := principalRequest(handler, http.MethodGet, "/v1/schedules/"+id+"/history", "", namedPrincipalToken(principal))
		if history.Code != http.StatusOK {
			t.Fatalf("%s history status=%d body=%s", principal, history.Code, history.Body.String())
		}
		for _, expected := range []string{`"creator":"alice"`, `"actor":"alice"`, `"action":"create"`, `"revision":1`, `"actor":"admin"`, `"action":"pause"`, `"revision":2`} {
			if !strings.Contains(history.Body.String(), expected) {
				t.Errorf("history lacks %s: %s", expected, history.Body.String())
			}
		}
		if strings.Contains(history.Body.String(), "private task text") {
			t.Errorf("history leaked schedule definition: %s", history.Body.String())
		}
	}
	if got := principalRequest(handler, http.MethodGet, "/v1/schedules/"+id+"/history", "", namedPrincipalToken("bob")); got.Code != http.StatusNotFound {
		t.Fatalf("foreign history status=%d body=%s", got.Code, got.Body.String())
	}
	if got := principalRequest(handler, http.MethodGet, "/v1/schedules/does-not-exist/history", "", namedPrincipalToken("admin")); got.Code != http.StatusNotFound {
		t.Fatalf("missing history status=%d body=%s", got.Code, got.Body.String())
	}
	before, err := store.ListScheduleHistory(id, "admin", true, 0, restjobs.MaxScheduleHistoryLimit)
	if err != nil || len(before.Events) != 2 {
		t.Fatalf("history before replay: page=%+v err=%v", before, err)
	}
	replayReq := httptest.NewRequest(http.MethodPost, "/v1/schedules", strings.NewReader(`{"cron":"* * * * *","repository":"widget","task":"private task text"}`))
	replayReq.Header.Set("Authorization", "Bearer "+namedPrincipalToken("alice"))
	replayReq.Header.Set("Content-Type", "application/json")
	replayReq.Header.Set("Idempotency-Key", "audit-create")
	replay := httptest.NewRecorder()
	handler.ServeHTTP(replay, replayReq)
	if replay.Code != http.StatusOK {
		t.Fatalf("idempotency replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	afterReplay, err := store.ListScheduleHistory(id, "admin", true, 0, restjobs.MaxScheduleHistoryLimit)
	if err != nil || len(afterReplay.Events) != len(before.Events) {
		t.Fatalf("replay changed audit: page=%+v err=%v", afterReplay, err)
	}
	if _, err := store.MutateScheduleAs(id, "admin", true, 1, restjobs.ScheduleResume, nil); err == nil {
		t.Fatal("stale operator mutation unexpectedly succeeded")
	}
	afterStale, err := store.ListScheduleHistory(id, "admin", true, 0, restjobs.MaxScheduleHistoryLimit)
	if err != nil || len(afterStale.Events) != len(before.Events) {
		t.Fatalf("stale mutation changed audit: page=%+v err=%v", afterStale, err)
	}
	if got := principalRequest(handler, http.MethodGet, "/v1/schedules/"+id, "", namedPrincipalToken("alice")); got.Code != http.StatusOK {
		t.Fatalf("submitter self-service broke: status=%d body=%s", got.Code, got.Body.String())
	}
}

func TestSQLiteScheduleAPIOwnershipIdempotencyAndRevision(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := restjobs.OpenSQLiteStore(filepath.Join(dir, "schedules.db"), restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	handler := newPrincipalTestHandler(t, store, restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true}, restserver.PrincipalConfig{ID: "bob", Role: "submitter", Enabled: true}, restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true})
	create := func(token, key, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/schedules", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Idempotency-Key", key)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	first := create(namedPrincipalToken("alice"), "schedule-one", `{"cron":"*/5 * * * *","repository":"widget","task":"run a bounded task"}`)
	if first.Code != http.StatusCreated || first.Header().Get("ETag") != "\"1\"" {
		t.Fatalf("create status=%d ETag=%q body=%s", first.Code, first.Header().Get("ETag"), first.Body.String())
	}
	var response struct {
		Schedule restjobs.Schedule `json:"schedule"`
		Replayed bool              `json:"replayed"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Schedule.Owner != "alice" || response.Schedule.Revision != 1 || response.Replayed {
		t.Fatalf("created schedule=%+v replay=%t", response.Schedule, response.Replayed)
	}
	id := response.Schedule.ID
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodGet, "/v1/schedules", ""},
		{http.MethodPost, "/v1/schedules", `{"cron":"* * * * *","repository":"widget","task":"task"}`},
		{http.MethodGet, "/v1/schedules/" + id, ""},
		{http.MethodPatch, "/v1/schedules/" + id, `{"action":"pause"}`},
		{http.MethodDelete, "/v1/schedules/" + id, ""},
	} {
		for _, token := range []string{namedPrincipalToken("admin"), testAPIKey} {
			if token == testAPIKey && (tc.method == http.MethodPatch || tc.method == http.MethodDelete) {
				continue
			}
			got := principalRequest(handler, tc.method, tc.path, tc.body, token)
			want := http.StatusNotFound
			if (token == namedPrincipalToken("admin") || token == testAPIKey) && (tc.method == http.MethodGet && (tc.path == "/v1/schedules" || tc.path == "/v1/schedules/"+id)) || token == namedPrincipalToken("admin") && (tc.method == http.MethodPatch || tc.method == http.MethodDelete) {
				if tc.method == http.MethodPatch || tc.method == http.MethodDelete {
					want = http.StatusPreconditionRequired
				} else {
					want = http.StatusOK
				}
			}
			if got.Code != want {
				t.Errorf("principal %q %s %s status=%d body=%s; want %d", token, tc.method, tc.path, got.Code, got.Body.String(), want)
			}
		}
	}
	get := principalRequest(handler, http.MethodGet, "/v1/schedules/"+id, "", namedPrincipalToken("alice"))
	if get.Code != http.StatusOK || get.Header().Get("ETag") != `"1"` {
		t.Fatalf("get status=%d ETag=%q body=%s", get.Code, get.Header().Get("ETag"), get.Body.String())
	}
	replay := create(namedPrincipalToken("alice"), "schedule-one", `{"cron":"*/5 * * * *","repository":"widget","task":"run a bounded task"}`)
	if replay.Code != http.StatusOK {
		t.Fatalf("replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	for _, tc := range []struct{ method, path string }{{http.MethodPost, "/v1/schedules"}} {
		if got := principalRequest(handler, tc.method, tc.path, `{"action":"pause"}`, namedPrincipalToken("admin")); got.Code != http.StatusNotFound {
			t.Errorf("operator self-service route %s %s status=%d", tc.method, tc.path, got.Code)
		}
	}
	if foreign := principalRequest(handler, http.MethodGet, "/v1/schedules/"+id, "", namedPrincipalToken("bob")); foreign.Code != http.StatusNotFound {
		t.Fatalf("foreign schedule status=%d body=%s", foreign.Code, foreign.Body.String())
	}
	if operator := principalRequest(handler, http.MethodGet, "/v1/schedules/"+id, "", namedPrincipalToken("admin")); operator.Code != http.StatusOK {
		t.Fatalf("operator schedule status=%d", operator.Code)
	}
	mutate := func(revision, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPatch, "/v1/schedules/"+id, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+namedPrincipalToken("alice"))
		r.Header.Set("If-Match", revision)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	start := make(chan struct{})
	responses := make(chan *httptest.ResponseRecorder, 2)
	var requests sync.WaitGroup
	for range 2 {
		requests.Add(1)
		go func() {
			defer requests.Done()
			<-start
			r := httptest.NewRequest(http.MethodPatch, "/v1/schedules/"+id, strings.NewReader(`{"action":"pause"}`))
			r.Header.Set("Authorization", "Bearer "+namedPrincipalToken("alice"))
			r.Header.Set("If-Match", `"1"`)
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			responses <- w
		}()
	}
	close(start)
	requests.Wait()
	close(responses)
	statusCounts := map[int]int{}
	for got := range responses {
		statusCounts[got.Code]++
		if got.Code == http.StatusOK && got.Header().Get("ETag") != `"2"` {
			t.Errorf("winning pause ETag=%q; want revision 2", got.Header().Get("ETag"))
		}
	}
	if statusCounts[http.StatusOK] != 1 || statusCounts[http.StatusConflict] != 1 {
		t.Fatalf("concurrent mutation statuses=%v; want one 200 and one 409", statusCounts)
	}
	if got := mutate("\"1\"", `{"action":"resume"}`); got.Code != http.StatusConflict || !strings.Contains(got.Body.String(), `"code":"revision_conflict"`) {
		t.Fatalf("stale revision status=%d body=%s", got.Code, got.Body.String())
	}
	missingMatch := httptest.NewRequest(http.MethodPatch, "/v1/schedules/"+id, strings.NewReader(`{"action":"resume"}`))
	missingMatch.Header.Set("Authorization", "Bearer "+namedPrincipalToken("alice"))
	missingMatch.Header.Set("Content-Type", "application/json")
	missing := httptest.NewRecorder()
	handler.ServeHTTP(missing, missingMatch)
	assertError(t, missing, http.StatusPreconditionRequired, "precondition_required")
	if got := mutate("\"2\"", `{"action":"resume"}`); got.Code != http.StatusOK || got.Header().Get("ETag") != "\"3\"" {
		t.Fatalf("resume status=%d ETag=%q body=%s", got.Code, got.Header().Get("ETag"), got.Body.String())
	}
	remove := httptest.NewRequest(http.MethodDelete, "/v1/schedules/"+id, nil)
	remove.Header.Set("Authorization", "Bearer "+namedPrincipalToken("alice"))
	remove.Header.Set("If-Match", `"3"`)
	removed := httptest.NewRecorder()
	handler.ServeHTTP(removed, remove)
	if removed.Code != http.StatusOK || removed.Header().Get("ETag") != `"4"` {
		t.Fatalf("remove status=%d ETag=%q body=%s", removed.Code, removed.Header().Get("ETag"), removed.Body.String())
	}
	getRemoved := principalRequest(handler, http.MethodGet, "/v1/schedules/"+id, "", namedPrincipalToken("alice"))
	var removedSchedule restjobs.Schedule
	if err := json.Unmarshal(getRemoved.Body.Bytes(), &removedSchedule); err != nil || getRemoved.Code != http.StatusOK || removedSchedule.Status != restjobs.ScheduleRemoved || removedSchedule.Revision != 4 {
		t.Fatalf("removed tombstone status=%d schedule=%+v err=%v body=%s", getRemoved.Code, removedSchedule, err, getRemoved.Body.String())
	}
	invalid := create(namedPrincipalToken("alice"), "invalid", "{\"cron\":\"@hourly\",\"repository\":\"widget\",\"task\":\"task\"}")
	assertError(t, invalid, http.StatusBadRequest, "invalid_cron")
	overlongCron := strings.Repeat("0,", 65) + "0 * * * *"
	if len(overlongCron) <= 128 {
		t.Fatalf("test cron length=%d; expected over 128", len(overlongCron))
	}
	overlongBody := `{"cron":"` + overlongCron + `","repository":"widget","task":"task"}`
	assertError(t, create(namedPrincipalToken("alice"), "overlong-cron", overlongBody), http.StatusBadRequest, "invalid_cron")
	for _, tc := range []struct {
		name, body, contentType, wantCode, wantMessage string
		wantStatus                                     int
	}{
		{"malformed JSON", `{"cron":`, "application/json", "malformed_json", "Request body is invalid.", http.StatusBadRequest},
		{"wrong content type", `{"cron":"* * * * *","repository":"widget","task":"task"}`, "text/plain", "unsupported_media_type", "Content-Type must be application/json.", http.StatusUnsupportedMediaType},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/v1/schedules", strings.NewReader(tc.body))
			r.Header.Set("Authorization", "Bearer "+namedPrincipalToken("alice"))
			r.Header.Set("Idempotency-Key", "invalid-"+tc.name)
			r.Header.Set("Content-Type", tc.contentType)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			assertError(t, w, tc.wantStatus, tc.wantCode)
			if !strings.Contains(w.Body.String(), tc.wantMessage) {
				t.Fatalf("body=%s; want message %q", w.Body.String(), tc.wantMessage)
			}
		})
	}
	unknown := create(namedPrincipalToken("alice"), "unknown-field", "{\"cron\":\"* * * * *\",\"repository\":\"widget\",\"task\":\"task\",\"timezone\":\"UTC\"}")
	assertError(t, unknown, http.StatusBadRequest, "unknown_field")
}
