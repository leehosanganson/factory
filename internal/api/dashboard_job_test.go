package restapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
)

func TestDashboardJobRecoveryProjection(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "jobs.db")
	open := func() *restjobs.SQLiteStore {
		t.Helper()
		store, err := restjobs.OpenSQLiteStore(path, managerConfig(2, 2, 8))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.CloseStore() })
		return store
	}
	store := open()
	job, _, err := store.AdmitAs("recovery-job", "alice", restjobs.Request{Repository: "widget", Task: "private recovery task"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimNextAttempt()
	if err != nil {
		t.Fatal(err)
	}
	attempt := restjobs.ProviderAttempt{Generation: claim.Attempt.Generation, Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + job.ID, Commit: "0123456789abcdef"}
	if err := store.RecordProviderAttemptForAttempt(claim.Attempt, attempt); err != nil {
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	store = open()
	before, err := store.Get(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	history, err := store.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	beforeAttempt, err := store.ProviderAttempt(job.ID)
	if err != nil {
		t.Fatal(err)
	}

	handler := newPrincipalTestHandler(t, store,
		restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true},
		restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "bob", Role: "submitter", Enabled: true})
	handler.config.DashboardOperatorID = "admin"
	handler.config.ReconcileProvider = func(context.Context, string) error {
		t.Error("inspection called provider reconciliation")
		return nil
	}
	jobPath := "/v1/jobs/" + job.ID
	dashboard := func(method, path string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		handler.ServeAsDashboardOperator(response, httptest.NewRequest(method, path, nil))
		return response
	}
	for range 2 {
		response := dashboard(http.MethodGet, jobPath)
		body := response.Body.String()
		if response.Code != http.StatusOK || !strings.Contains(body, `"recovery_state":"needs_operator"`) || !strings.Contains(body, `"status":"running"`) || strings.Contains(body, `"request"`) || strings.Contains(body, "private recovery task") || strings.Contains(body, attempt.Commit) {
			t.Fatalf("dashboard status=%d body=%s", response.Code, body)
		}
	}
	if response := dashboard(http.MethodGet, "/v1/jobs/missing"); response.Code != http.StatusNotFound {
		t.Fatalf("missing dashboard job status=%d", response.Code)
	}
	if response := dashboard(http.MethodPost, jobPath); response.Code != http.StatusNotFound {
		t.Fatalf("dashboard accepted POST to job detail: %d", response.Code)
	}
	for _, tc := range []struct {
		token string
		code  int
	}{{namedPrincipalToken("admin"), http.StatusOK}, {namedPrincipalToken("alice"), http.StatusOK}, {namedPrincipalToken("bob"), http.StatusNotFound}, {"", http.StatusUnauthorized}} {
		response := principalRequest(handler, http.MethodGet, jobPath, "", tc.token)
		if response.Code != tc.code || strings.Contains(response.Body.String(), "recovery_state") {
			t.Fatalf("REST status=%d, want %d without projection, body=%s", response.Code, tc.code, response.Body.String())
		}
	}
	response := httptest.NewRecorder()
	handler.ServeAsDashboardSubmitter(response, httptest.NewRequest(http.MethodGet, jobPath, nil), restserver.Principal{ID: "alice", Role: "submitter"})
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "recovery_state") {
		t.Fatalf("submitter dashboard status=%d body=%s", response.Code, response.Body.String())
	}
	if after, err := store.Get(job.ID); err != nil || !reflect.DeepEqual(after, before) || !store.RecoveryNeeded(job.ID) {
		t.Fatalf("inspection changed Job or recovery marker: %+v, %v", after, err)
	}
	if after, err := store.History(job.ID); err != nil || !reflect.DeepEqual(after, history) {
		t.Fatalf("inspection changed history: %+v, %v", after, err)
	}
	if after, err := store.ProviderAttempt(job.ID); err != nil || after != beforeAttempt {
		t.Fatalf("inspection changed provider evidence: %+v, %v", after, err)
	}
	if _, err := store.ProviderObservation(job.ID); !errors.Is(err, restjobs.ErrInvalidTransition) {
		t.Fatalf("inspection recorded provider observation: %v", err)
	}
}

type finishingJobRecoveryReader struct {
	*restjobs.LocalManager
	attempt restjobs.AttemptHandle
}

func (s finishingJobRecoveryReader) Get(id string) (restjobs.Snapshot, error) {
	job, err := s.LocalManager.Get(id)
	if err != nil {
		return job, err
	}
	// Finish after capturing the running snapshot, before the recovery read.
	return job, s.FinishAttempt(s.attempt, restjobs.StatusSucceeded)
}

func TestDashboardJobRecoveryUnavailableOnStatusChange(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("finishing-job", restjobs.Request{Repository: "widget", Task: "test"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := manager.ClaimNextAttempt()
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, finishingJobRecoveryReader{manager, claim.Attempt}, func() bool { return true })
	handler.config.DashboardOperatorID = "admin"
	response := httptest.NewRecorder()
	handler.ServeAsDashboardOperator(response, httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID, nil))
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `"status":"running"`) || !strings.Contains(body, `"recovery_state":"unavailable"`) {
		t.Fatalf("dashboard status=%d body=%s", response.Code, body)
	}
}

type failingJobRecoveryReader struct{ restjobs.Manager }

func (failingJobRecoveryReader) ReadRecoveryNeeded(context.Context, string) (restjobs.Status, bool, error) {
	return "", false, errors.New("private database diagnostic")
}

func TestDashboardJobRecoveryUnavailable(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("unavailable-recovery", restjobs.Request{Repository: "widget", Task: "test"})
	if err != nil {
		t.Fatal(err)
	}
	for _, store := range []restjobs.Manager{failingJobRecoveryReader{manager}, struct{ restjobs.Manager }{manager}} {
		handler := newTestHandler(t, store, func() bool { return true })
		handler.config.DashboardOperatorID = "admin"
		response := httptest.NewRecorder()
		handler.ServeAsDashboardOperator(response, httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID, nil))
		body := response.Body.String()
		if response.Code != http.StatusOK || !strings.Contains(body, `"recovery_state":"unavailable"`) || strings.Contains(body, "private database diagnostic") {
			t.Fatalf("dashboard status=%d body=%s", response.Code, body)
		}
	}
}
