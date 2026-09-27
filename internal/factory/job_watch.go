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
	ID       string
	Status   string
	Activity string
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
			jobs = append(jobs, watchedJob{ID: id, Status: job.Status, Activity: summarizeJobTrace(store, job).Activity})
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
		if _, err := fmt.Fprintf(out, "Job %s\n  Status: %s\n  Latest activity: %s\n", job.ID, job.Status, strings.TrimSpace(job.Activity)); err != nil {
			return err
		}
	}
	if !terminal {
		_, err := io.WriteString(out, "\n")
		return err
	}
	return nil
}
