package restapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
)

func TestNamedOperatorKnownScheduleRoutesReturnMethodNotAllowed(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := restjobs.OpenSQLiteStore(filepath.Join(dir, "operator-methods.db"), restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	handler := newPrincipalTestHandler(t, store, restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true})

	for _, tc := range []struct {
		method string
		path   string
		status int
		allow  string
	}{
		{method: http.MethodPut, path: "/v1/schedules", status: http.StatusMethodNotAllowed, allow: "GET, POST"},
		{method: http.MethodPut, path: "/v1/schedules/schedule-id", status: http.StatusMethodNotAllowed, allow: "GET, PATCH, DELETE"},
		{method: http.MethodPut, path: "/v1/schedules/schedule-id/history", status: http.StatusMethodNotAllowed, allow: http.MethodGet},
		{method: http.MethodPut, path: "/v1/schedules/schedule-id/runs", status: http.StatusMethodNotAllowed, allow: http.MethodGet},
		{method: http.MethodPut, path: "/v1/schedules/schedule-id/unknown", status: http.StatusNotFound},
	} {
		response := principalRequest(handler, tc.method, tc.path, "", namedPrincipalToken("admin"))
		if response.Code != tc.status {
			t.Errorf("%s %s status=%d body=%s; want %d", tc.method, tc.path, response.Code, response.Body.String(), tc.status)
		}
		if got := response.Header().Get("Allow"); got != tc.allow {
			t.Errorf("%s %s Allow=%q; want %q", tc.method, tc.path, got, tc.allow)
		}
	}

	legacyCreate := principalRequest(handler, http.MethodPost, "/v1/schedules", `{"cron":"* * * * *","repository":"widget","task":"task"}`, testAPIKey)
	if legacyCreate.Code != http.StatusNotFound {
		t.Errorf("legacy shared schedule create status=%d body=%s; want 404", legacyCreate.Code, legacyCreate.Body.String())
	}
}

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
	for _, token := range []string{namedPrincipalToken("admin")} {
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
			if token == namedPrincipalToken("admin") && tc.method == http.MethodPost {
				want = http.StatusServiceUnavailable
			}
			if got.Code != want {
				t.Errorf("principal %q %s %s status=%d body=%s; want %d", token, tc.method, tc.path, got.Code, got.Body.String(), want)
			}
		}
	}
	legacyCreate := principalRequest(handler, http.MethodPost, "/v1/schedules", `{"cron":"* * * * *","repository":"widget","task":"task"}`, testAPIKey)
	if legacyCreate.Code != http.StatusNotFound {
		t.Errorf("legacy shared credential schedule create status=%d body=%s; want 404", legacyCreate.Code, legacyCreate.Body.String())
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

func TestDashboardScheduleCreateUsesSQLiteAndSafeProjection(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := restjobs.OpenSQLiteStore(filepath.Join(dir, "dashboard-create.db"), restjobs.Config{QueueCapacity: 8, MaxConcurrentJobs: 2, MaxRecords: 16, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	handler := newPrincipalTestHandler(t, store, restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true})
	handler.config.DashboardOperatorID = "admin"

	create := func(key string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/v1/schedules", strings.NewReader(`{"cron":"*/5 * * * *","repository":"widget","task":"private dashboard schedule task"}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", key)
		response := httptest.NewRecorder()
		handler.ServeAsDashboardOperator(response, request)
		return response
	}
	first := create("dashboard-schedule-create")
	if first.Code != http.StatusCreated {
		t.Fatalf("dashboard schedule create status=%d body=%s", first.Code, first.Body.String())
	}
	for _, forbidden := range []string{"private dashboard schedule task", `"task"`, `"repository"`, `"owner"`} {
		if strings.Contains(first.Body.String(), forbidden) {
			t.Errorf("dashboard create exposed %q: %s", forbidden, first.Body.String())
		}
	}
	var response struct {
		Schedule struct {
			ID string `json:"id"`
		} `json:"schedule"`
		Replayed bool `json:"replayed"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &response); err != nil || response.Schedule.ID == "" || response.Replayed {
		t.Fatalf("dashboard create response=%s err=%v", first.Body.String(), err)
	}
	stored, err := store.GetSchedule(response.Schedule.ID)
	if err != nil || stored.Owner != "admin" || stored.Task != "private dashboard schedule task" || stored.Repository != "widget" {
		t.Fatalf("stored dashboard schedule=%+v err=%v", stored, err)
	}
	history, err := store.ListScheduleHistory(response.Schedule.ID, "admin", true, 0, restjobs.DefaultScheduleHistoryLimit)
	if err != nil || len(history.Events) != 1 || history.Events[0].Creator != "admin" || history.Events[0].Actor != "admin" || history.Events[0].Action != restjobs.ScheduleCreated {
		t.Fatalf("dashboard schedule audit history=%+v err=%v", history, err)
	}
	replay := create("dashboard-schedule-create")
	if replay.Code != http.StatusOK || !strings.Contains(replay.Body.String(), `"replayed":true`) || !strings.Contains(replay.Body.String(), response.Schedule.ID) || strings.Contains(replay.Body.String(), "private dashboard schedule task") {
		t.Fatalf("dashboard schedule replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	for _, method := range []string{http.MethodPatch, http.MethodDelete} {
		request := httptest.NewRequest(method, "/v1/schedules/"+response.Schedule.ID, nil)
		blocked := httptest.NewRecorder()
		handler.ServeAsDashboardOperator(blocked, request)
		if blocked.Code != http.StatusNotFound {
			t.Errorf("dashboard %s schedule status=%d body=%s", method, blocked.Code, blocked.Body.String())
		}
	}
}

func TestDashboardScheduleProjectionOmitsTaskAndBoundsRuns(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := restjobs.OpenSQLiteStore(filepath.Join(dir, "dashboard-schedules.db"), restjobs.Config{QueueCapacity: 16, MaxConcurrentJobs: 2, MaxRecords: 20, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	handler := newPrincipalTestHandler(t, store, restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true})
	handler.config.DashboardOperatorID = "admin"
	create := principalRequest(handler, http.MethodPost, "/v1/schedules", `{"cron":"* * * * *","repository":"widget","task":"dashboard private task"}`, namedPrincipalToken("admin"))
	if create.Code != http.StatusCreated {
		t.Fatalf("named operator create status=%d body=%s", create.Code, create.Body.String())
	}
	// Create a second schedule through the dashboard-only handler and verify its
	// response remains limited to the existing redacted schedule projection.
	dashboardCreateRequest := httptest.NewRequest(http.MethodPost, "/v1/schedules", strings.NewReader(`{"cron":"* * * * *","repository":"widget","task":"dashboard task"}`))
	dashboardCreateRequest.Header.Set("Content-Type", "application/json")
	dashboardCreateRequest.Header.Set("Idempotency-Key", "dashboard-seed-create")
	dashboardCreate := httptest.NewRecorder()
	handler.ServeAsDashboardOperator(dashboardCreate, dashboardCreateRequest)
	if dashboardCreate.Code != http.StatusCreated || strings.Contains(dashboardCreate.Body.String(), "dashboard task") || strings.Contains(dashboardCreate.Body.String(), `"repository"`) || strings.Contains(dashboardCreate.Body.String(), `"owner"`) {
		t.Fatalf("dashboard schedule create status=%d body=%s", dashboardCreate.Code, dashboardCreate.Body.String())
	}
	var created struct {
		Schedule struct {
			ID string `json:"id"`
		} `json:"schedule"`
	}
	if err := json.Unmarshal(dashboardCreate.Body.Bytes(), &created); err != nil || created.Schedule.ID == "" {
		t.Fatalf("dashboard schedule create response=%s err=%v", dashboardCreate.Body.String(), err)
	}
	schedule, err := store.GetSchedule(created.Schedule.ID)
	if err != nil {
		t.Fatal(err)
	}
	first := schedule.NextOccurrence()
	if err := store.DispatchScheduleOccurrences(context.Background(), first, 1); err != nil {
		t.Fatal(err)
	}
	list := httptest.NewRecorder()
	handler.ServeAsDashboardOperator(list, httptest.NewRequest(http.MethodGet, "/v1/schedules", nil))
	if list.Code != http.StatusOK || strings.Contains(list.Body.String(), "dashboard private task") || strings.Contains(list.Body.String(), `"owner"`) || strings.Contains(list.Body.String(), `"repository"`) {
		t.Fatalf("dashboard schedule projection status=%d body=%s", list.Code, list.Body.String())
	}
	if !strings.Contains(list.Body.String(), `"cron":"* * * * *"`) || !strings.Contains(list.Body.String(), `"status":"active"`) {
		t.Fatalf("dashboard schedule projection lacks status/cron: %s", list.Body.String())
	}
	for i := range 15 {
		key := "dashboard-overdue-" + strconv.Itoa(i)
		if _, _, err := store.Admit(key, restjobs.Request{Repository: "widget", Task: "fill queue"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DispatchScheduleOccurrences(context.Background(), first.Add(3*time.Minute), 1); err != nil {
		t.Fatal(err)
	}
	list = httptest.NewRecorder()
	handler.ServeAsDashboardOperator(list, httptest.NewRequest(http.MethodGet, "/v1/schedules", nil))
	if !strings.Contains(list.Body.String(), `"skipped_occurrences"`) {
		t.Fatalf("dashboard schedule projection lacks skipped summary: %s", list.Body.String())
	}
	runs := httptest.NewRecorder()
	handler.ServeAsDashboardOperator(runs, httptest.NewRequest(http.MethodGet, "/v1/schedules/"+schedule.ID+"/runs?limit=5", nil))
	if runs.Code != http.StatusOK || strings.Contains(runs.Body.String(), "dashboard private task") || !strings.Contains(runs.Body.String(), `"job_url":"/v1/jobs/`) {
		t.Fatalf("dashboard run projection status=%d body=%s", runs.Code, runs.Body.String())
	}
	for _, query := range []string{"limit=6", "before_sequence=2", "limit=5&unexpected=1"} {
		tooMany := httptest.NewRecorder()
		handler.ServeAsDashboardOperator(tooMany, httptest.NewRequest(http.MethodGet, "/v1/schedules/"+schedule.ID+"/runs?"+query, nil))
		if tooMany.Code != http.StatusBadRequest {
			t.Errorf("dashboard accepted run query %q: status=%d body=%s", query, tooMany.Code, tooMany.Body.String())
		}
	}
}

func TestScheduleRunsExposeRetainedJobsWithOwnerFilteringAndPagination(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := restjobs.OpenSQLiteStore(filepath.Join(dir, "schedule-runs.db"), restjobs.Config{QueueCapacity: 8, MaxConcurrentJobs: 2, MaxRecords: 16, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	handler := newPrincipalTestHandler(t, store, restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true}, restserver.PrincipalConfig{ID: "bob", Role: "submitter", Enabled: true}, restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true})
	// Use the normal authenticated create route to set its required idempotency key.
	create := httptest.NewRequest(http.MethodPost, "/v1/schedules", strings.NewReader(`{"cron":"* * * * *","repository":"widget","task":"private scheduled task"}`))
	create.Header.Set("Authorization", "Bearer "+namedPrincipalToken("alice"))
	create.Header.Set("Idempotency-Key", "runs-create")
	create.Header.Set("Content-Type", "application/json")
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, create)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	var body struct {
		Schedule struct {
			ID string `json:"id"`
		} `json:"schedule"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	schedule, err := store.GetSchedule(body.Schedule.ID)
	if err != nil {
		t.Fatal(err)
	}
	first := schedule.NextOccurrence()
	if err := store.DispatchScheduleOccurrences(context.Background(), first, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.DispatchScheduleOccurrences(context.Background(), first.Add(time.Minute), 1); err != nil {
		t.Fatal(err)
	}
	// A definition edit changes only future occurrences, not Jobs already admitted.
	if _, err := store.MutateSchedule(schedule.ID, "alice", schedule.Revision, restjobs.ScheduleEdit, &restjobs.ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "replacement task"}); err != nil {
		t.Fatal(err)
	}

	if _, err := store.ListScheduleRuns(schedule.ID, "alice", false, 0, 1); err != nil {
		t.Fatalf("store runs: %v", err)
	}
	firstPage := principalRequest(handler, http.MethodGet, "/v1/schedules/"+schedule.ID+"/runs?limit=1", "", namedPrincipalToken("alice"))
	if firstPage.Code != http.StatusOK {
		t.Fatalf("runs status=%d body=%s", firstPage.Code, firstPage.Body.String())
	}
	var page struct {
		Runs []struct {
			Occurrence time.Time       `json:"occurrence_at"`
			JobID      string          `json:"job_id"`
			Status     restjobs.Status `json:"status"`
			Link       string          `json:"job_url"`
		} `json:"runs"`
		HasMore    bool   `json:"has_more"`
		NextBefore uint64 `json:"next_before_sequence"`
	}
	if err := json.Unmarshal(firstPage.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 1 || !page.HasMore || page.NextBefore == 0 || page.Runs[0].Status != restjobs.StatusQueued || page.Runs[0].Link != "/v1/jobs/"+page.Runs[0].JobID || !page.Runs[0].Occurrence.Equal(first.Add(time.Minute)) {
		t.Fatalf("first runs page=%+v body=%s", page, firstPage.Body.String())
	}
	if strings.Contains(firstPage.Body.String(), "private scheduled task") || strings.Contains(firstPage.Body.String(), "replacement task") {
		t.Fatalf("run listing exposed schedule task text: %s", firstPage.Body.String())
	}
	job, err := store.Get(page.Runs[0].JobID)
	if err != nil || job.Request.Task != "private scheduled task" || job.Status != restjobs.StatusQueued {
		t.Fatalf("admitted Job changed with schedule edit: job=%+v err=%v", job, err)
	}
	claimed, err := store.ClaimNextAttempt()
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Job.Status != restjobs.StatusRunning {
		t.Fatalf("claimed scheduled Job status=%s", claimed.Job.Status)
	}
	for _, mutation := range []restjobs.ScheduleMutation{restjobs.SchedulePause, restjobs.ScheduleResume, restjobs.ScheduleRemove} {
		current, err := store.GetSchedule(schedule.ID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.MutateSchedule(schedule.ID, "alice", current.Revision, mutation, nil); err != nil {
			t.Fatalf("%s schedule: %v", mutation, err)
		}
	}
	running, err := store.Get(claimed.Job.ID)
	if err != nil || running.Status != restjobs.StatusRunning || running.Request.Task != "private scheduled task" {
		t.Fatalf("schedule mutations altered admitted running Job: job=%+v err=%v", running, err)
	}
	if err := store.FinishAttempt(claimed.Attempt, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	lifecycle := principalRequest(handler, http.MethodGet, "/v1/schedules/"+schedule.ID+"/runs", "", namedPrincipalToken("alice"))
	if lifecycle.Code != http.StatusOK || !strings.Contains(lifecycle.Body.String(), `"status":"succeeded"`) {
		t.Fatalf("run listing did not show canonical Job lifecycle update: status=%d body=%s", lifecycle.Code, lifecycle.Body.String())
	}
	older := principalRequest(handler, http.MethodGet, "/v1/schedules/"+schedule.ID+"/runs?limit=1&before_sequence="+strconv.FormatUint(page.NextBefore, 10), "", namedPrincipalToken("alice"))
	if older.Code != http.StatusOK || !strings.Contains(older.Body.String(), `"has_more":false`) || !strings.Contains(older.Body.String(), `"occurrence_at":"`+first.Format(time.RFC3339)+`"`) {
		t.Fatalf("older runs page status=%d body=%s", older.Code, older.Body.String())
	}
	for _, token := range []string{namedPrincipalToken("bob"), namedPrincipalToken("admin"), testAPIKey} {
		foreign := principalRequest(handler, http.MethodGet, "/v1/schedules/"+schedule.ID+"/runs", "", token)
		want := http.StatusNotFound
		if token == namedPrincipalToken("admin") || token == testAPIKey {
			want = http.StatusOK
		}
		if foreign.Code != want {
			t.Errorf("runs token=%q status=%d body=%s want=%d", token, foreign.Code, foreign.Body.String(), want)
		}
	}
	invalid := principalRequest(handler, http.MethodGet, "/v1/schedules/"+schedule.ID+"/runs?limit=101", "", namedPrincipalToken("alice"))
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid limit status=%d body=%s", invalid.Code, invalid.Body.String())
	}
	invalidSequence := principalRequest(handler, http.MethodGet, "/v1/schedules/"+schedule.ID+"/runs?before_sequence=9223372036854775808", "", namedPrincipalToken("alice"))
	if invalidSequence.Code != http.StatusBadRequest {
		t.Fatalf("out-of-range before_sequence status=%d body=%s", invalidSequence.Code, invalidSequence.Body.String())
	}
	maxSequence := principalRequest(handler, http.MethodGet, "/v1/schedules/"+schedule.ID+"/runs?before_sequence=9223372036854775807", "", namedPrincipalToken("alice"))
	if maxSequence.Code != http.StatusOK {
		t.Fatalf("max signed before_sequence status=%d body=%s", maxSequence.Code, maxSequence.Body.String())
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

func TestNamedOperatorsCanCreateOwnedSchedulesWithScopedIdempotency(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := restjobs.OpenSQLiteStore(filepath.Join(dir, "operator-schedules.db"), restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	handler := newPrincipalTestHandler(t, store,
		restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true},
		restserver.PrincipalConfig{ID: "second-admin", Role: "operator", Enabled: true},
	)
	create := func(principal, task string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/schedules", strings.NewReader(`{"cron":"* * * * *","repository":"widget","task":"`+task+`"}`))
		r.Header.Set("Authorization", "Bearer "+namedPrincipalToken(principal))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "operator-schedule")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	first := create("admin", "operator task")
	if first.Code != http.StatusCreated {
		t.Fatalf("operator create status=%d body=%s", first.Code, first.Body.String())
	}
	var created struct {
		Schedule restjobs.Schedule `json:"schedule"`
		Replayed bool              `json:"replayed"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Schedule.Owner != "admin" || created.Replayed {
		t.Fatalf("created schedule=%+v replay=%t", created.Schedule, created.Replayed)
	}
	schedule, err := store.GetSchedule(created.Schedule.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DispatchScheduleOccurrences(context.Background(), schedule.NextOccurrence(), 1); err != nil {
		t.Fatal(err)
	}
	runs, err := store.ListScheduleRuns(created.Schedule.ID, "admin", true, 0, 50)
	if err != nil || len(runs.Runs) != 1 {
		t.Fatalf("operator schedule runs=%+v err=%v", runs, err)
	}
	job, err := store.Get(runs.Runs[0].JobID)
	if err != nil || job.Submitter != "admin" {
		t.Fatalf("scheduled job owner=%q err=%v; want admin", job.Submitter, err)
	}
	if replay := create("admin", "operator task"); replay.Code != http.StatusOK || !strings.Contains(replay.Body.String(), `"replayed":true`) {
		t.Fatalf("same-operator replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	if conflict := create("admin", "changed task"); conflict.Code != http.StatusConflict {
		t.Fatalf("same-operator payload conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	second := create("second-admin", "operator task")
	if second.Code != http.StatusCreated {
		t.Fatalf("second operator create status=%d body=%s", second.Code, second.Body.String())
	}
	var secondCreated struct {
		Schedule restjobs.Schedule `json:"schedule"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondCreated); err != nil {
		t.Fatal(err)
	}
	if secondCreated.Schedule.ID == created.Schedule.ID || secondCreated.Schedule.Owner != "second-admin" {
		t.Fatalf("idempotency key crossed operator namespaces: first=%+v second=%+v", created.Schedule, secondCreated.Schedule)
	}
	get := principalRequest(handler, http.MethodGet, "/v1/schedules/"+created.Schedule.ID, "", namedPrincipalToken("admin"))
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"owner":"admin"`) {
		t.Fatalf("operator get status=%d body=%s", get.Code, get.Body.String())
	}
	mutate := httptest.NewRequest(http.MethodPatch, "/v1/schedules/"+created.Schedule.ID, strings.NewReader(`{"action":"pause"}`))
	mutate.Header.Set("Authorization", "Bearer "+namedPrincipalToken("second-admin"))
	mutate.Header.Set("Content-Type", "application/json")
	mutate.Header.Set("If-Match", `"1"`)
	mutated := httptest.NewRecorder()
	handler.ServeHTTP(mutated, mutate)
	if mutated.Code != http.StatusOK {
		t.Fatalf("second operator mutation status=%d body=%s", mutated.Code, mutated.Body.String())
	}
	history := principalRequest(handler, http.MethodGet, "/v1/schedules/"+created.Schedule.ID+"/history", "", namedPrincipalToken("admin"))
	if history.Code != http.StatusOK || !strings.Contains(history.Body.String(), `"creator":"admin"`) || !strings.Contains(history.Body.String(), `"actor":"admin"`) || !strings.Contains(history.Body.String(), `"action":"create"`) || !strings.Contains(history.Body.String(), `"actor":"second-admin"`) || !strings.Contains(history.Body.String(), `"action":"pause"`) {
		t.Fatalf("operator audit history status=%d body=%s", history.Code, history.Body.String())
	}
	legacy := principalRequest(handler, http.MethodPost, "/v1/schedules", `{"cron":"* * * * *","repository":"widget","task":"legacy task"}`, testAPIKey)
	if legacy.Code != http.StatusNotFound {
		t.Fatalf("legacy shared key created schedule: status=%d body=%s", legacy.Code, legacy.Body.String())
	}
	dashboardRequest := httptest.NewRequest(http.MethodPost, "/v1/schedules", strings.NewReader(`{"cron":"* * * * *","repository":"widget","task":"dashboard task"}`))
	dashboardRequest.Header.Set("Content-Type", "application/json")
	dashboardRequest.Header.Set("Idempotency-Key", "dashboard-operator-create")
	dashboard := httptest.NewRecorder()
	handler.config.DashboardOperatorID = "admin"
	handler.ServeAsDashboardOperator(dashboard, dashboardRequest)
	if dashboard.Code != http.StatusCreated || strings.Contains(dashboard.Body.String(), "dashboard task") || strings.Contains(dashboard.Body.String(), `"repository"`) || strings.Contains(dashboard.Body.String(), `"owner"`) {
		t.Fatalf("dashboard operator schedule create status=%d body=%s", dashboard.Code, dashboard.Body.String())
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
		{http.MethodGet, "/v1/schedules/" + id, ""},
		{http.MethodPatch, "/v1/schedules/" + id, `{"action":"pause"}`},
		{http.MethodDelete, "/v1/schedules/" + id, ""},
	} {
		for _, token := range []string{namedPrincipalToken("admin"), testAPIKey} {
			if token == testAPIKey && (tc.method == http.MethodPatch || tc.method == http.MethodDelete || tc.method == http.MethodGet) {
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
		if got := principalRequest(handler, tc.method, tc.path, `{"cron":"* * * * *","repository":"widget","task":"operator task"}`, namedPrincipalToken("admin")); got.Code != http.StatusCreated {
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
