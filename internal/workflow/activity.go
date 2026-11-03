package workflow

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"strings"
	"time"
)

type ActivityEvent struct {
	Type    string
	Stage   string
	Message string
}

type workflowActivityEvent = ActivityEvent

func latestWorkflowProgress(path, activeStage string) string {
	if activity := workflowProgressSummary(readWorkflowActivityEvents(path), activeStage); activity != "" {
		return activity
	}
	return stageProgressFallback(activeStage)
}

func readWorkflowActivityEvents(path string) []workflowActivityEvent {
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() == 0 {
		return nil
	}
	const activityTailBytes = 64 * 1024
	offset := info.Size() - activityTailBytes
	if offset < 0 {
		offset = 0
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil
	}
	events := make([]workflowActivityEvent, 0)
	scanner := bufio.NewScanner(io.LimitReader(file, activityTailBytes))
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	if offset > 0 && !scanner.Scan() {
		return nil
	}
	for scanner.Scan() {
		var event WorkflowEventRecord
		if err := json.Unmarshal(scanner.Bytes(), &event); err == nil {
			events = append(events, workflowActivityEvent{Type: event.Type, Stage: event.Stage, Message: event.Message})
		}
	}
	return events
}

func WorkflowProgressSummary(events []ActivityEvent, activeStage string) string {
	return workflowProgressSummary(events, activeStage)
}

func workflowProgressSummary(events []workflowActivityEvent, activeStage string) string {
	if !validWorkflowStage(activeStage) {
		return "Workflow in progress"
	}
	activity := stageProgressFallback(activeStage)
	for _, event := range events {
		if event.Stage != activeStage {
			continue
		}
		switch event.Type {
		case "stage.started":
			activity = stageProgressFallback(activeStage)
		case "stage.status":
			if status := workflowStatusSummary(event.Message); status != "" {
				activity = activeStage + " · " + status
			}
		case "stage.completed":
			activity = activeStage + " completed"
		case "stage.failed":
			activity = activeStage + " failed"
		}
	}
	return activity
}

func stageProgressFallback(stage string) string {
	if validWorkflowStage(stage) {
		return stage + " in progress"
	}
	return "Workflow in progress"
}

func ValidWorkflowStage(stage string) bool { return validWorkflowStage(stage) }

func validWorkflowStage(stage string) bool {
	for _, candidate := range workflowStages {
		if stage == candidate {
			return true
		}
	}
	return false
}

func workflowStatusSummary(message string) string {
	fields := strings.Fields(message)
	if len(fields) > 0 && strings.HasPrefix(fields[0], "stage=") {
		fields = fields[1:]
	}
	if len(fields) > 0 {
		if _, err := time.Parse(time.RFC3339, fields[0]); err == nil {
			fields = fields[1:]
		}
	}
	return sanitizeSecondaryStatus(strings.Join(fields, " "))
}
