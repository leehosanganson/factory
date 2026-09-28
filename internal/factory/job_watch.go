package factory

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"
)

const jobWatchPollInterval = time.Second

type watchedJob struct {
	ID                string
	Status            string
	Activity          string
	NextAction        string
	Phase             string
	LatestCheckAt     time.Time
	LatestCheckResult string
	RecentEvents      []monitorTraceEvent
}

func watchJobs(ctx context.Context, store *JobStore, ids []string, out io.Writer, terminal bool, interval time.Duration) error {
	ids = uniqueJobIDs(ids)
	if len(ids) == 0 {
		return fmt.Errorf("usage: factory job watch <id>...")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		jobs := make([]watchedJob, 0, len(ids))
		allTerminal := true
		for _, id := range ids {
			job, err := store.reconcileJob(id)
			if err != nil {
				return fmt.Errorf("watch job %s: %w", id, err)
			}
			watched := watchedJob{ID: id, Status: job.Status, Activity: summarizeJobTrace(store, job).Activity, NextAction: jobWatchNextAction(job)}
			if job.Monitor != nil {
				watched.Phase = monitorDisplayPhase(job.Monitor)
				watched.LatestCheckAt = job.Monitor.LatestCheckAt
				watched.LatestCheckResult = job.Monitor.LatestCheckResult
				watched.RecentEvents = append([]monitorTraceEvent(nil), job.Monitor.RecentEvents...)
			}
			jobs = append(jobs, watched)
			if !isTerminalStatus(job.Status) {
				allTerminal = false
			}
		}
		if err := writeJobWatchSnapshot(out, jobs, terminal); err != nil {
			return err
		}
		if allTerminal {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func jobWatchNextAction(job JobRecord) string {
	if job.Type == monitorJobType && job.Monitor != nil {
		switch monitorDisplayPhase(job.Monitor) {
		case "approval_pending":
			return fmt.Sprintf("Review with `factory monitor get %s --details`, then `factory monitor approve %s` or `factory monitor reject %s`.", job.ID, job.ID, job.ID)
		case "recoverable_failure":
			return fmt.Sprintf("After confirming the monitor worker stopped, run `factory monitor reset %s` to resume.", job.ID)
		}
	}
	switch job.Status {
	case "failed", "interrupted", "stopped", "cancelled":
		return fmt.Sprintf("Inspect with `factory job get %s --details` and `factory job logs %s`.", job.ID, job.ID)
	case "complete":
		if job.Type == implementationJobType && job.Worktree != "" {
			return fmt.Sprintf("Inspect the implementation output with `factory job get %s --details`, then review the recorded worktree diff.", job.ID)
		}
		if job.Type == tidyJobType {
			return fmt.Sprintf("Review the tidy results with `factory job get %s --details` and inspect the target diff.", job.ID)
		}
		return fmt.Sprintf("Inspect the completed result with `factory job get %s --details`.", job.ID)
	default:
		return "No action needed; let the job continue and check again later."
	}
}

func writeMonitorRecentEvents(out io.Writer, events []monitorTraceEvent, indent string) error {
	start := 0
	if len(events) > monitorRecentEventLimit {
		start = len(events) - monitorRecentEventLimit
	}
	if _, err := fmt.Fprintf(out, "%sRecent monitor events:\n", indent); err != nil {
		return err
	}
	if len(events) == 0 {
		_, err := fmt.Fprintf(out, "%s  (none)\n", indent)
		return err
	}
	for _, event := range events[start:] {
		at := "unknown time"
		if !event.At.IsZero() {
			at = event.At.Local().Format("15:04:05")
		}
		if _, err := fmt.Fprintf(out, "%s  %s %s: %s\n", indent, at, event.Phase, conciseMonitorEvent(event.Message)); err != nil {
			return err
		}
	}
	return nil
}

func uniqueJobIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	unique := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	return unique
}

func writeJobWatchSnapshot(out io.Writer, jobs []watchedJob, terminal bool) error {
	if terminal {
		if _, err := io.WriteString(out, "\033[H\033[2J"); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(out, "Selected jobs\n"); err != nil {
		return err
	}
	for _, job := range jobs {
		if _, err := fmt.Fprintf(out, "Job %s\n  Status: %s\n  Latest activity: %s\n  Next action: %s\n", job.ID, job.Status, strings.TrimSpace(job.Activity), job.NextAction); err != nil {
			return err
		}
		if job.Phase != "" {
			if _, err := fmt.Fprintf(out, "  Monitor phase: %s\n", job.Phase); err != nil {
				return err
			}
			checkAt := "never"
			if !job.LatestCheckAt.IsZero() {
				checkAt = job.LatestCheckAt.UTC().Format(time.RFC3339)
			}
			result := job.LatestCheckResult
			if result == "" {
				result = "unavailable"
			}
			if _, err := fmt.Fprintf(out, "  Latest successful PR/check query: %s (%s)\n", checkAt, result); err != nil {
				return err
			}
			if err := writeMonitorRecentEvents(out, job.RecentEvents, "  "); err != nil {
				return err
			}
		}
	}
	if !terminal {
		_, err := io.WriteString(out, "\n")
		return err
	}
	return nil
}
