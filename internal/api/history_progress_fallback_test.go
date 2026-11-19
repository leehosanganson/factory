package restapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
)

type historyProgressFallbackManager struct {
	restjobs.Manager
	jobID   string
	history restjobs.History
}

func (m historyProgressFallbackManager) History(id string) (restjobs.History, error) {
	if id == m.jobID {
		return m.history, nil
	}
	return m.Manager.History(id)
}

func TestGetHistoryFallsBackToLatestValidProgressEvent(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("history-progress-fallback", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}

	wrappedManager := historyProgressFallbackManager{
		Manager: manager,
		jobID:   job.ID,
		history: restjobs.History{
			JobID: job.ID,
			Events: []restjobs.Event{
				{At: time.Now(), Type: "stage_progress", Message: "stage=implement state=started"},
				{At: time.Now(), Type: "private_event", Message: "secret transcript"},
				{At: time.Now(), Type: "stage_progress", Message: "stage=implement state=unknown"},
			},
		},
	}
	handler := newTestHandler(t, wrappedManager, func() bool { return true })
	response := principalRequest(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/history", "", testAPIKey)
	if response.Code != http.StatusOK {
		t.Fatalf("GET history status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}

	var got restjobs.History
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode history response: %v", err)
	}
	wantProgress := &restjobs.ProgressSummary{Stage: "implement", State: "started"}
	if got.Progress == nil || *got.Progress != *wantProgress {
		t.Errorf("history progress = %+v, want latest valid event summary %+v", got.Progress, wantProgress)
	}
	if len(got.Events) != 2 {
		t.Fatalf("history events = %+v, want only the two safe stage_progress events", got.Events)
	}
	for _, event := range got.Events {
		if event.Type != "stage_progress" {
			t.Errorf("history event type = %q, want stage_progress", event.Type)
		}
		if event.Message != "" {
			t.Errorf("history event message = %q, want private progress payload omitted", event.Message)
		}
	}
	if strings.Contains(response.Body.String(), "secret transcript") || strings.Contains(response.Body.String(), "private_event") {
		t.Fatalf("history response exposed filtered event data: %s", response.Body.String())
	}
}
