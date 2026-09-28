package factory

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"strings"
	"time"
)

type workflowActivityEvent struct {
	Type    string
	Stage   string
	Message string
}

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
	if _, err := file.Seek(offset, 0); err != nil {
		return nil
	}
	var events []workflowActivityEvent
	scanner := bufio.NewScanner(io.LimitReader(file, activityTailBytes))
	scanner.Buffer(make([]byte, 4096), maxSessionEventLineBytes)
	if offset > 0 && !scanner.Scan() {
		return nil
	}
	for scanner.Scan() {
		var event workflowEventRecord
		if err := json.Unmarshal(scanner.Bytes(), &event); err == nil {
			events = append(events, workflowActivityEvent{Type: event.Type, Stage: event.Stage, Message: event.Message})
		}
	}
	return events
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

func jobWorkflowProgress(events []SessionEvent) string {
	var stage string
	var stageEvents []workflowActivityEvent
	for _, event := range events {
		fields := strings.Fields(event.Message)
		eventStage := ""
		if len(fields) > 0 && strings.HasPrefix(fields[0], "stage=") {
			eventStage = strings.TrimPrefix(fields[0], "stage=")
		}
		if event.Type == "stage.started" && validWorkflowStage(eventStage) {
			stage = eventStage
			stageEvents = append(stageEvents, workflowActivityEvent{Type: event.Type, Stage: stage, Message: event.Message})
			continue
		}
		if eventStage == stage && validWorkflowStage(stage) {
			stageEvents = append(stageEvents, workflowActivityEvent{Type: event.Type, Stage: stage, Message: event.Message})
		}
	}
	if !validWorkflowStage(stage) {
		return ""
	}
	return workflowProgressSummary(stageEvents, stage)
}
