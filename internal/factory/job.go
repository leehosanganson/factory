package factory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const implementationJobType = "implementation"
const monitorJobType = "monitor"
const monitorSessionID = "monitor"
const jobHeartbeatInterval = 5 * time.Second
const jobCancelPollInterval = 200 * time.Millisecond

// JobCommand runs the public detached-job management commands.
func JobCommand(args []string, cfg Config, target string, in io.Reader, out io.Writer) error {
	return JobCommandContext(context.Background(), args, cfg, target, in, out)
}

// JobCommandContext runs a job management command. Cancellation of attach only
// stops this observer; it never signals the detached worker.
func JobCommandContext(ctx context.Context, args []string, cfg Config, target string, in io.Reader, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: factory job start <type> <description> | list | show <id> | logs <id> [--session <id>] [--follow] | attach <id> | stop <id>")
	}
	root, err := JobStateRoot(cfg.StateDir)
	if err != nil {
		return err
	}
	store, err := NewJobStore(root)
	if err != nil {
		return err
	}
	switch args[0] {
	case "start":
		if len(args) < 3 {
			return fmt.Errorf("usage: factory job start <implementation|monitor> <description>")
		}
		description := strings.TrimSpace(strings.Join(args[2:], " "))
		if description == "" {
			return fmt.Errorf("job description must not be empty")
		}
		if args[1] == monitorJobType {
			if err := cfg.Validate(); err != nil {
				return err
			}
			legacyRoot, err := babysitRoot(cfg)
			if err != nil {
				return err
			}
			return startBabysit([]string{description}, cfg, target, legacyRoot, out)
		}
		if args[1] != implementationJobType {
			return fmt.Errorf("unsupported job type %q (supported: implementation, monitor)", args[1])
		}
		id, err := startImplementationJob(store, target, description)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "Started implementation job %s\n", id)
		return nil
	case "list":
		if len(args) != 1 {
			return fmt.Errorf("usage: factory job list")
		}
		jobs, err := store.reconcileJobs()
		if len(jobs) == 0 && err == nil {
			fmt.Fprintln(out, "No jobs.")
			return nil
		}
		for _, job := range jobs {
			fmt.Fprintf(out, "%s\t%s\t%s\t%s\n", job.ID, job.Type, job.Status, job.TaskDescription)
		}
		return err
	case "show":
		if len(args) != 2 {
			return fmt.Errorf("usage: factory job show <id>")
		}
		job, err := store.reconcileJob(args[1])
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "ID: %s\nType: %s\nStatus: %s\nTarget: %s\nDescription: %s\nCreated: %s\nUpdated: %s\n", job.ID, job.Type, job.Status, job.TargetPath, job.TaskDescription, job.CreatedAt.Format(time.RFC3339), job.UpdatedAt.Format(time.RFC3339))
		if !job.StartedAt.IsZero() {
			fmt.Fprintf(out, "Started: %s\n", job.StartedAt.Format(time.RFC3339))
		}
		if !job.EndedAt.IsZero() {
			fmt.Fprintf(out, "Ended: %s\n", job.EndedAt.Format(time.RFC3339))
		}
		for _, session := range job.Sessions {
			fmt.Fprintf(out, "Session: %s\t%s\n", session.ID, session.Status)
		}
		return nil
	case "attach":
		if len(args) != 2 {
			return fmt.Errorf("usage: factory job attach <id>")
		}
		return AttachJob(ctx, store, args[1], out)
	case "logs":
		follow := false
		filtered := make([]string, 0, len(args)-1)
		for _, arg := range args[1:] {
			if arg == "--follow" {
				if follow {
					return fmt.Errorf("--follow may only be specified once")
				}
				follow = true
				continue
			}
			filtered = append(filtered, arg)
		}
		if len(filtered) < 1 || len(filtered) > 3 || len(filtered) == 3 && filtered[1] != "--session" {
			return fmt.Errorf("usage: factory job logs <id> [--session <session-id>] [--follow]")
		}
		job, err := store.GetJob(filtered[0])
		if err != nil {
			return err
		}
		var path string
		if len(filtered) == 3 {
			path, err = store.SessionLogPath(filtered[0], filtered[2])
		} else if job.Type == monitorJobType {
			path, err = store.SessionLogPath(filtered[0], monitorSessionID)
			if err == nil {
				path, err = monitorLegacyLogPath(store, filtered[0], path)
			}
		} else {
			path, err = store.JobLogPath(filtered[0])
		}
		if err != nil {
			return err
		}
		if follow {
			var status string
			err := followLog(ctx, path, func() (bool, error) {
				current, err := store.reconcileJob(job.ID)
				if err != nil {
					return false, err
				}
				status = current.Status
				if current.Type == monitorJobType && status == "recoverable_failure" {
					stopped, err := monitorRecoverableFailureStopped(store, job.ID)
					return stopped, err
				}
				return isTerminalStatus(status), nil
			}, out)
			if err != nil {
				return err
			}
			if ctx.Err() == nil && status == "recoverable_failure" {
				return monitorStoppedError(job.ID)
			}
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		_, err = out.Write(data)
		return err
	case "stop":
		if len(args) != 2 {
			return fmt.Errorf("usage: factory job stop <id>")
		}
		job, err := store.reconcileJob(args[1])
		if err != nil {
			return err
		}
		if isTerminalStatus(job.Status) {
			return fmt.Errorf("job %s is already %s", job.ID, job.Status)
		}
		if err := store.RequestStop(job.ID); err != nil {
			return err
		}
		fmt.Fprintf(out, "Stop requested for job %s.\n", job.ID)
		return nil
	default:
		return fmt.Errorf("unknown job command %q", args[0])
	}
}

func StartImplementationJob(cfg Config, target, description string) (string, error) {
	root, err := JobStateRoot(cfg.StateDir)
	if err != nil {
		return "", err
	}
	store, err := NewJobStore(root)
	if err != nil {
		return "", err
	}
	return startImplementationJob(store, target, description)
}

func startImplementationJob(store *JobStore, target, description string) (string, error) {
	canonicalTarget, err := canonicalPath(target)
	if err != nil {
		return "", fmt.Errorf("resolve target directory: %w", err)
	}
	if info, err := os.Stat(canonicalTarget); err != nil || !info.IsDir() {
		return "", fmt.Errorf("target must be an existing directory")
	}
	if isWithin(canonicalTarget, store.Root()) {
		return "", fmt.Errorf("job state directory %s must be outside target directory %s", store.Root(), canonicalTarget)
	}
	unlock, err := store.LockTargetAdmission(canonicalTarget)
	if err != nil {
		return "", err
	}
	defer unlock()
	jobs, err := store.ListJobs()
	if err != nil {
		return "", err
	}
	for _, job := range jobs {
		if job.TargetPath != canonicalTarget || isTerminalStatus(job.Status) {
			continue
		}
		stale, err := store.reconcileOrphan(job)
		if err != nil {
			return "", err
		}
		if !stale {
			return "", fmt.Errorf("active job %s already targets %s", job.ID, canonicalTarget)
		}
	}
	id, err := newJobID()
	if err != nil {
		return "", err
	}
	job := JobRecord{ID: id, Type: implementationJobType, TaskDescription: description, TargetPath: canonicalTarget, Status: "queued"}
	if err := store.CreateJob(job); err != nil {
		return "", err
	}
	if _, err := store.CreateSession(id, "workflow", "queued"); err != nil {
		_, _ = store.UpdateJob(id, func(job *JobRecord) error { job.Status = "failed"; return nil })
		return "", err
	}
	if err := store.CreateJobLog(id); err != nil {
		_, _ = store.UpdateJob(id, func(job *JobRecord) error { job.Status = "failed"; return nil })
		return "", err
	}
	if _, err := launchJobWorker(id, store.Root()); err != nil {
		_, _ = store.UpdateJob(id, func(job *JobRecord) error { job.Status = "failed"; return nil })
		return "", err
	}
	return id, nil
}

// RunJobWorker reloads the persisted request and configuration, then owns the job lifecycle.
func RunJobWorker(id, root string) error {
	if !filepath.IsAbs(root) {
		return fmt.Errorf("job state root must be absolute")
	}
	canonicalRoot, err := canonicalPath(root)
	if err != nil || canonicalRoot != root {
		return fmt.Errorf("job state root must be canonical")
	}
	rootInfo, err := os.Stat(root)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("job state root must be a private directory")
	}
	store, err := NewJobStore(root)
	if err != nil {
		return err
	}
	job, err := store.GetJob(id)
	if err != nil {
		return err
	}
	if job.Type != implementationJobType {
		return fmt.Errorf("unsupported worker job type %q", job.Type)
	}
	unlock, err := store.LockTarget(job.TargetPath)
	if err != nil {
		return err
	}
	defer unlock()
	job, err = store.ClaimQueuedJob(id, job.TargetPath)
	if err != nil {
		return err
	}
	finalized := false
	defer func() {
		if !finalized {
			_ = finishJob(store, id, "failed")
		}
	}()
	finish := func(status string) error {
		err := finishJob(store, id, status)
		finalized = true
		return err
	}
	if err := store.WriteWorker(id, WorkerRecord{ID: id, PID: os.Getpid()}); err != nil {
		return err
	}
	defer store.ClearWorker(id)
	if store.StopRequested(id) {
		return finish("stopped")
	}
	if _, err := store.UpdateSession(id, "workflow", func(session *SessionRecord) error { session.Status = "running"; return nil }); err != nil {
		return err
	}
	cfg, err := LoadConfig("")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchDone := make(chan struct{})
	go func() {
		cancelTicker := time.NewTicker(jobCancelPollInterval)
		heartbeatTicker := time.NewTicker(jobHeartbeatInterval)
		defer cancelTicker.Stop()
		defer heartbeatTicker.Stop()
		defer close(watchDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-cancelTicker.C:
				if store.StopRequested(id) {
					cancel()
					return
				}
			case <-heartbeatTicker.C:
				_, _ = store.HeartbeatWorker(id)
			}
		}
	}()
	workflow := Workflow{
		Agent: Runner{Config: cfg}, Config: cfg, In: strings.NewReader(""), Out: os.Stdout,
		Workdir: job.TargetPath, Observer: JobSessionObserver{Store: store, JobID: id, SessionID: "workflow"},
	}
	runErr := workflow.RunContext(ctx, job.TaskDescription)
	cancel()
	<-watchDone
	status := "complete"
	if runErr != nil {
		status = "failed"
		if errors.Is(ctx.Err(), context.Canceled) && store.StopRequested(id) {
			status = "stopped"
		}
	} else if store.StopRequested(id) {
		status = "stopped"
	}
	finishErr := finish(status)
	return errors.Join(runErr, finishErr)
}

func finishJob(store *JobStore, id, status string) error {
	_, jobErr := store.UpdateJob(id, func(job *JobRecord) error { job.Status = status; return nil })
	_, sessionErr := store.UpdateSession(id, "workflow", func(session *SessionRecord) error { session.Status = status; return nil })
	return errors.Join(jobErr, sessionErr)
}

func newJobID() (string, error) {
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return time.Now().UTC().Format("20060102T150405") + "-" + hex.EncodeToString(random[:]), nil
}

func ensureCancelFile(path string) error {
	if err := ensureRegularIfExists(path); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}
