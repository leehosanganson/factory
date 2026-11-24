package restapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
)

func TestTaskBoardFollowUpRequiresOwner(t *testing.T) {
	manager := newTestManager(t, managerConfig(4, 8, 8))
	thread, _, err := manager.AdmitThreadAs("first", "alice", restjobs.Request{Repository: "widget", Task: "first task"})
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, nil)
	for _, tc := range []struct {
		id   string
		want int
	}{{"bob", http.StatusNotFound}, {"alice", http.StatusAccepted}} {
		request := httptest.NewRequest(http.MethodPost, "/v1/threads/"+thread.Thread.ID+"/jobs", strings.NewReader(`{"repository":"widget","task":"follow-up"}`))
		request.Header.Set("Idempotency-Key", "follow-up")
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeAsDashboardSubmitter(response, request, restserver.Principal{ID: tc.id, Role: "submitter"})
		if response.Code != tc.want {
			t.Errorf("%s append = %d, want %d", tc.id, response.Code, tc.want)
		}
	}
}

func TestTaskBoardReplyIsOwnerOnly(t *testing.T) {
	manager := newTestManager(t, managerConfig(4, 8, 8))
	thread, _, err := manager.AdmitThreadAs("reply", "alice", restjobs.Request{Repository: "widget", Task: "private prompt"})
	if err != nil {
		t.Fatal(err)
	}
	job, err := manager.ClaimNext()
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RecordVerificationEvidence(job.ID, restjobs.VerificationEvidence{Reply: "private reply"}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(job.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, nil)
	for _, tc := range []struct {
		id   string
		want int
	}{{"alice", 200}, {"bob", 404}} {
		res := httptest.NewRecorder()
		handler.ServeAsDashboardSubmitter(res, httptest.NewRequest(http.MethodGet, "/v1/threads/"+thread.Thread.ID, nil), restserver.Principal{ID: tc.id, Role: "submitter"})
		if res.Code != tc.want || strings.Contains(res.Body.String(), "private reply") != (tc.id == "alice") || strings.Contains(res.Body.String(), "private prompt") {
			t.Errorf("%s: %d %s", tc.id, res.Code, res.Body.String())
		}
	}
}
