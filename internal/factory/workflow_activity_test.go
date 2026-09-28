package factory

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadWorkflowActivityEventsReadsOnlyRecentTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workflow-events.jsonl")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6000; i++ {
		if _, err := fmt.Fprintf(file, `{"type":"stage.status","stage":"implement","message":"stage=implement old progress %04d"}`+"\n", i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := file.WriteString(`{"type":"stage.status","stage":"implement","message":"stage=implement current bounded progress"}` + "\n"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path); err != nil || info.Size() <= 64*1024 {
		t.Fatalf("fixture size = %v, err=%v; want event history larger than scan bound", info, err)
	}
	activity := latestWorkflowProgress(path, "implement")
	if !strings.Contains(activity, "current bounded progress") || strings.Contains(activity, "old progress 0000") {
		t.Fatalf("latest activity = %q; want only progress from recent history", activity)
	}
}

func TestWorkflowProgressSummaryUsesLatestStatusForActiveStage(t *testing.T) {
	events := []workflowActivityEvent{
		{Type: "stage.status", Stage: "review", Message: "stage=review old status"},
		{Type: "stage.started", Stage: "implement"},
		{Type: "stage.status", Stage: "implement", Message: "stage=implement 2026-01-02T03:04:05Z latest status"},
	}
	if got := workflowProgressSummary(events, "implement"); got != "implement · latest status" {
		t.Fatalf("workflow progress = %q; want latest active-stage status", got)
	}
}
