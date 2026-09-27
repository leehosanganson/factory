package factory

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
	"unicode/utf8"
)

type jobTraceSummary struct {
	Activity    string
	StatusCalls int
	ActivePi    int
}

func jobSessionFor(record JobRecord) string {
	if record.Type == monitorJobType {
		return monitorSessionID
	}
	return "workflow"
}

func summarizeJobTrace(store *JobStore, job JobRecord) jobTraceSummary {
	summary := jobTraceSummary{Activity: "No recorded activity"}
	events, err := store.SessionEvents(job.ID, jobSessionFor(job))
	if err != nil {
		return summary
	}
	active := map[string]int{}
	statusInvocations := map[string]struct{}{}
	for _, event := range events {
		switch event.Type {
		case "status.started", "status.completed":
			if event.Invocation != "" {
				statusInvocations[event.Invocation] = struct{}{}
			}
		case "agent.started":
			if event.Command == "pi" && event.PID > 0 {
				active[event.Invocation] = event.PID
			}
		case "agent.completed":
			delete(active, event.Invocation)
		}
		if isActivityEvent(event.Type) && !event.At.IsZero() {
			message := strings.Join(strings.Fields(terminalSafeText(event.Message+" "+event.Summary)), " ")
			if len(message) > 120 {
				message = truncateUTF8(message, 117) + "…"
			}
			summary.Activity = event.Type
			if message != "" {
				summary.Activity += ": " + message
			}
			summary.Activity += " (" + event.At.Local().Format("15:04:05") + ")"
		}
	}
	for _, pid := range active {
		if processAlive(pid) && processGroupIsOwn(pid) {
			summary.ActivePi++
		}
	}
	summary.StatusCalls = len(statusInvocations)
	if job.Type == implementationJobType {
		if activity := jobWorkflowProgress(events); activity != "" {
			summary.Activity = activity
		}
	}
	return summary
}

func truncateUTF8(text string, maxBytes int) string {
	if len(text) <= maxBytes {
		return text
	}
	for maxBytes > 0 && !utf8.RuneStart(text[maxBytes]) {
		maxBytes--
	}
	return text[:maxBytes]
}

func processGroupIsOwn(pid int) bool {
	pgid, err := syscall.Getpgid(pid)
	return err == nil && pgid == pid
}

func isActivityEvent(eventType string) bool {
	return strings.HasPrefix(eventType, "stage.") || strings.HasPrefix(eventType, "workflow.") || strings.HasPrefix(eventType, "monitor.") || strings.HasPrefix(eventType, "agent.") || strings.HasPrefix(eventType, "status.")
}

func jobProcessObserver(store *JobStore, jobID, sessionID string) func(string, int, bool) {
	return func(command string, pid int, started bool) {
		if filepath.Base(command) != "pi" {
			return
		}
		eventType := "agent.started"
		if !started {
			eventType = "agent.completed"
		}
		_ = store.AppendSessionEventDetails(jobID, sessionID, SessionEvent{Type: eventType, Command: filepath.Base(command), PID: pid, Invocation: fmt.Sprint(pid)})
	}
}

type statusJobObserver struct {
	JobSessionObserver
	store     *JobStore
	jobID     string
	sessionID string
}

func (o statusJobObserver) ObserveWorkflowEvent(event WorkflowEvent) error {
	if strings.HasPrefix(event.Type, "agent.") {
		return nil
	}
	if strings.HasPrefix(event.Type, "status.") {
		fields := strings.Fields(event.Message)
		if len(fields) > 0 && strings.HasPrefix(fields[0], "stage=") {
			fields = fields[1:]
		}
		record := SessionEvent{Type: event.Type, Message: event.Message, Outcome: event.Outcome}
		if event.Type == "status.started" {
			record.Invocation = strings.TrimPrefix(event.Message, "stage=")
			if len(fields) > 0 {
				record.Invocation = fields[len(fields)-1]
			}
		} else if len(fields) > 0 {
			record.Invocation = fields[0]
		}
		if event.Type == "status.completed" && len(fields) > 1 {
			record.Outcome = strings.TrimPrefix(fields[1], "outcome=")
			if record.Outcome == "success" {
				record.Summary = sanitizeSecondaryStatus(strings.TrimPrefix(strings.Join(fields[2:], " "), "summary="))
			}
		}
		return o.store.AppendSessionEventDetails(o.jobID, o.sessionID, record)
	}
	return o.JobSessionObserver.ObserveWorkflowEvent(event)
}

func classifyStatusOutcome(ctx context.Context, err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		return "canceled"
	}
	if err != nil {
		return "error"
	}
	return "success"
}
