package restapi

import (
	"context"
	"encoding/json"
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
			if got.Code != http.StatusNotFound {
				t.Errorf("principal %q %s %s status=%d body=%s; want 404", token, tc.method, tc.path, got.Code, got.Body.String())
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
			got := principalRequest(handler, tc.method, tc.path, tc.body, token)
			if got.Code != http.StatusNotFound {
				t.Errorf("principal %q %s %s status=%d body=%s; want 404", token, tc.method, tc.path, got.Code, got.Body.String())
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
	if foreign := principalRequest(handler, http.MethodGet, "/v1/schedules/"+id, "", namedPrincipalToken("bob")); foreign.Code != http.StatusNotFound {
		t.Fatalf("foreign schedule status=%d body=%s", foreign.Code, foreign.Body.String())
	}
	if operator := principalRequest(handler, http.MethodGet, "/v1/schedules/"+id, "", namedPrincipalToken("admin")); operator.Code != http.StatusNotFound {
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
