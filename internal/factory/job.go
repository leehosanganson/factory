package factory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const implementationJobType = "implementation"
const tidyJobType = "tidy"
const monitorJobType = "monitor"
const monitorSessionID = "monitor"
const jobHeartbeatInterval = 5 * time.Second
const jobCancelPollInterval = 200 * time.Millisecond

// JobCommand runs the public detached-job management commands.
func JobCommand(args []string, cfg Config, target string, in io.Reader, out io.Writer) error {
	return JobCommandContext(context.Background(), args, cfg, target, in, out)
}

// JobCommandContext runs a job management command. Cancellation of attach or
// watch only stops that observer; it never signals the detached worker.
func JobCommandContext(ctx context.Context, args []string, cfg Config, target string, in io.Reader, out io.Writer) error {
	return jobCommandContext(ctx, args, cfg, target, in, out, false)
}

// JobCommandContextWithTerminal runs a job command with terminal-aware output selection.
func JobCommandContextWithTerminal(ctx context.Context, args []string, cfg Config, target string, in io.Reader, out io.Writer, terminal bool) error {
	return jobCommandContext(ctx, args, cfg, target, in, out, terminal)
}

func jobCommandContext(ctx context.Context, args []string, cfg Config, target string, in io.Reader, out io.Writer, terminal bool) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: factory job start <type> <description> | list | get <id> [--details] | logs <id> [--session <id>] [--follow] | attach <id> | stop <id> | watch <id>...")
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
			return fmt.Errorf("usage: factory job start <implementation|tidy|monitor> <description>")
		}
		description := strings.TrimSpace(strings.Join(args[2:], " "))
		if description == "" {
			return fmt.Errorf("job description must not be empty")
		}
		if args[1] == monitorJobType {
			if err := cfg.Validate(); err != nil {
				return err
			}
			return startMonitor([]string{description}, cfg, target, root, out)
		}
		if args[1] != implementationJobType && args[1] != tidyJobType {
			return fmt.Errorf("unsupported job type %q (supported: implementation, tidy, monitor)", args[1])
		}
		id, err := startWorkflowJob(store, cfg, target, description, args[1])
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "Started %s job %s\n", args[1], id)
		return nil
	case "list":
		options, err := parseJobListOptions(args[1:])
		if err != nil {
			return err
		}
		jobs, scanned, hasMore, err := store.reconcileJobsWithLimit(options.scanLimit)
		if options.scanLimit > 0 {
			fmt.Fprintf(out, "History scan: selected %d valid job directories in filesystem iteration order.", scanned)
			if hasMore {
				fmt.Fprintln(out, " Additional records were not examined and may be omitted; use `factory job list` for the full history.")
			} else {
				fmt.Fprintln(out, " No additional job records were found.")
			}
		}
		if options.status != "" || options.jobType != "" {
			filtered := make([]JobRecord, 0, len(jobs))
			for _, job := range jobs {
				if options.status != "" && job.Status != options.status || options.jobType != "" && job.Type != options.jobType {
					continue
				}
				filtered = append(filtered, job)
			}
			jobs = filtered
		}
		if len(jobs) == 0 && (err == nil || options.status != "" || options.jobType != "") {
			fmt.Fprintln(out, "No jobs.")
			return err
		}
		if options.limit > 0 && len(jobs) > options.limit {
			jobs = jobs[:options.limit]
		}
		writeJobTable(out, store, jobs)
		return err
	case "get":
		id, details, err := parseDetailsID("factory job get <id> [--details]", args[1:])
		if err != nil {
			return err
		}
		job, err := store.reconcileJob(id)
		if err != nil {
			return err
		}
		if !details {
			writeJobTable(out, store, []JobRecord{job})
			return nil
		}
		writeJobSummary(out, store, job, true)
		return writeJobDetails(out, store, job)
	case "watch":
		return watchJobs(ctx, store, args[1:], out, terminal, jobWatchPollInterval)
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
					return monitorRecoverableFailureStopped(store, job.ID)
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

type jobListOptions struct {
	limit     int
	scanLimit int
	status    string
	jobType   string
}

const jobListUsage = "factory job list [--limit <n>] [--scan-limit <n>] [--status <status>] [--type <type>]"
const jobListStatuses = "queued, running, complete, closed, failed, stopped, cancelled, interrupted, recoverable_failure"
const jobListTypes = "implementation, tidy, monitor"

func parseJobListOptions(args []string) (jobListOptions, error) {
	var options jobListOptions
	seen := make(map[string]bool, 3)
	for i := 0; i < len(args); i++ {
		flag := args[i]
		if flag != "--limit" && flag != "--scan-limit" && flag != "--status" && flag != "--type" {
			return jobListOptions{}, fmt.Errorf("unknown job list argument %q (usage: %s)", flag, jobListUsage)
		}
		if seen[flag] {
			return jobListOptions{}, fmt.Errorf("%s may only be specified once", flag)
		}
		seen[flag] = true
		if i+1 >= len(args) || strings.HasPrefix(args[i+1], "--") {
			switch flag {
			case "--limit", "--scan-limit":
				return jobListOptions{}, fmt.Errorf("%s requires a positive integer", flag)
			case "--status":
				return jobListOptions{}, fmt.Errorf("--status requires one of: %s", jobListStatuses)
			default:
				return jobListOptions{}, fmt.Errorf("--type requires one of: %s", jobListTypes)
			}
		}
		value := args[i+1]
		i++
		switch flag {
		case "--limit", "--scan-limit":
			limit, err := strconv.Atoi(value)
			if err != nil || limit <= 0 {
				return jobListOptions{}, fmt.Errorf("%s must be a positive integer", flag)
			}
			if flag == "--limit" {
				options.limit = limit
			} else {
				options.scanLimit = limit
			}
		case "--status":
			if !containsJobListValue(jobListStatuses, value) {
				return jobListOptions{}, fmt.Errorf("invalid --status %q (valid statuses: %s)", value, jobListStatuses)
			}
			options.status = value
		case "--type":
			if !containsJobListValue(jobListTypes, value) {
				return jobListOptions{}, fmt.Errorf("invalid --type %q (valid types: %s)", value, jobListTypes)
			}
			options.jobType = value
		}
	}
	return options, nil
}

func containsJobListValue(values, value string) bool {
	for _, candidate := range strings.Split(values, ", ") {
		if value == candidate {
			return true
		}
	}
	return false
}

func parseDetailsID(usage string, args []string) (string, bool, error) {
	if len(args) < 1 || len(args) > 2 {
		return "", false, fmt.Errorf("usage: %s", usage)
	}
	if len(args) == 2 && args[1] != "--details" {
		return "", false, fmt.Errorf("usage: %s", usage)
	}
	return args[0], len(args) == 2, nil
}

func writeJobTable(out io.Writer, store *JobStore, jobs []JobRecord) {
	const typeWidth = 14
	const statusWidth = 18
	const descriptionWidth = 12
	const activityWidth = 12
	const publicationWidth = 11
	const targetPathWidth = 11
	fmt.Fprintf(out, "%-36s %-*s %-*s %-*s %-*s %-*s %-*s\n", "ID", typeWidth, "TYPE", statusWidth, "STATUS", descriptionWidth, "DESCRIPTION", activityWidth, "ACTIVITY", publicationWidth, "PUBLICATION", targetPathWidth, "TARGET")
	for _, job := range jobs {
		description := strings.Join(strings.Fields(terminalSafeText(job.TaskDescription)), " ")
		description = truncateJobListField(description, descriptionWidth)
		trace := summarizeJobTrace(store, job)
		publication := ""
		if job.Type == implementationJobType {
			publication = job.PublicationStatus
		}
		activity := strings.TrimSpace(strings.Join(strings.Fields(terminalSafeText(trace.Activity)), " "))
		activity = truncateJobListField(activity, activityWidth)
		targetPath := job.TargetPath
		if len([]rune(targetPath)) > targetPathWidth {
			targetPath = truncateJobListField(filepath.Base(targetPath), targetPathWidth)
		}
		jobType := truncateJobListField(job.Type, typeWidth)
		status := truncateJobListField(job.Status, statusWidth)
		publication = truncateJobListField(publication, publicationWidth)
		fmt.Fprintf(out, "%-36s %-*s %-*s %-*s %-*s %-*s %-*s\n", job.ID, typeWidth, jobType, statusWidth, status, descriptionWidth, description, activityWidth, activity, publicationWidth, publication, targetPathWidth, targetPath)
	}
}

func truncateJobListField(value string, width int) string {
	runes := []rune(value)
	if len(runes) > width {
		return string(runes[:width-1]) + "…"
	}
	return value
}

func writeJobSummary(out io.Writer, store *JobStore, job JobRecord, details bool) {
	fmt.Fprintf(out, "ID: %s\nType: %s\nStatus: %s\n", job.ID, job.Type, job.Status)
	if job.PublicationStatus != "" {
		fmt.Fprintf(out, "Publication: %s\n", job.PublicationStatus)
		if !details && job.PublicationSummary != "" {
			summary := strings.SplitN(job.PublicationSummary, "\n", 2)[0]
			fmt.Fprintf(out, "Publication summary: %s\n", summary)
		}
	}
	trace := summarizeJobTrace(store, job)
	fmt.Fprintf(out, "Latest activity: %s\nStatus calls: %d\nActive Factory-launched Pi subprocesses: %d (direct process observations only; descendants are not counted)\n", trace.Activity, trace.StatusCalls, trace.ActivePi)
	if !details {
		fmt.Fprintf(out, "Target: %s\n", job.TargetPath)
		return
	}
	fmt.Fprintf(out, "Target: %s\nCreated: %s\nUpdated: %s\n", job.TargetPath, job.CreatedAt.Format(time.RFC3339), job.UpdatedAt.Format(time.RFC3339))
	if job.Worktree != "" {
		fmt.Fprintf(out, "Worktree: %s\n", job.Worktree)
	}
	if job.WorkBranch != "" {
		fmt.Fprintf(out, "Work branch: %s\n", job.WorkBranch)
	}
	if !job.StartedAt.IsZero() {
		fmt.Fprintf(out, "Started: %s\n", job.StartedAt.Format(time.RFC3339))
	}
	if !job.EndedAt.IsZero() {
		fmt.Fprintf(out, "Ended: %s\n", job.EndedAt.Format(time.RFC3339))
	}
	fmt.Fprintf(out, "Description: %s\n", job.TaskDescription)
	if job.PublicationSummary != "" {
		fmt.Fprintf(out, "Publication summary: %s\n", job.PublicationSummary)
	}
	for _, session := range job.Sessions {
		fmt.Fprintf(out, "Session: %s (%s)\n", session.ID, session.Status)
	}
}

func writeJobDetails(out io.Writer, store *JobStore, job JobRecord) error {
	paths := []struct{ label, path string }{}
	jobLog, err := store.JobLogPath(job.ID)
	if err != nil {
		return err
	}
	paths = append(paths, struct{ label, path string }{"Job log", jobLog})
	for _, session := range job.Sessions {
		path, err := store.SessionLogPath(job.ID, session.ID)
		if err != nil {
			return err
		}
		paths = append(paths, struct{ label, path string }{"Session log " + session.ID, path})
	}
	for _, item := range paths {
		fmt.Fprintf(out, "%s path: %s\n", item.label, item.path)
		data, err := os.ReadFile(item.path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if len(data) > 0 {
			fmt.Fprintf(out, "%s:\n%s", item.label, data)
			if data[len(data)-1] != '\n' {
				fmt.Fprintln(out)
			}
		}
	}
	return nil
}

func StartImplementationJob(cfg Config, target, description string) (string, error) {
	return StartWorkflowJob(cfg, target, description, implementationJobType)
}

func StartTidyJob(cfg Config, target, description string) (string, error) {
	return StartWorkflowJob(cfg, target, description, tidyJobType)
}

func StartWorkflowJob(cfg Config, target, description, jobType string) (string, error) {
	if jobType != implementationJobType && jobType != tidyJobType {
		return "", fmt.Errorf("unsupported workflow job type %q", jobType)
	}
	if strings.TrimSpace(description) == "" {
		return "", fmt.Errorf("job description must not be empty")
	}
	root, err := JobStateRoot(cfg.StateDir)
	if err != nil {
		return "", err
	}
	store, err := NewJobStore(root)
	if err != nil {
		return "", err
	}
	return startWorkflowJob(store, cfg, target, description, jobType)
}

func lockDetachedJobCapacity(store *JobStore, cfg Config) (func(), error) {
	if cfg.DetachedJobMaxConcurrency == nil {
		return func() {}, nil
	}
	if *cfg.DetachedJobMaxConcurrency < 1 || *cfg.DetachedJobMaxConcurrency > maxDetachedJobConcurrency {
		return nil, fmt.Errorf("detached_job_max_concurrency must be between 1 and %d when set", maxDetachedJobConcurrency)
	}
	unlock, err := store.LockDetachedJobAdmission()
	if err != nil {
		return nil, err
	}
	if cfg.DetachedJobMaxConcurrency == nil {
		return unlock, nil
	}
	active, err := store.ActiveDetachedJobCount()
	if err != nil {
		unlock()
		return nil, err
	}
	if active >= *cfg.DetachedJobMaxConcurrency {
		unlock()
		return nil, fmt.Errorf("detached job concurrency limit reached (%d active); inspect with `factory job list` and retry after an active job finishes or is safely reconciled", active)
	}
	return unlock, nil
}

func startImplementationJob(store *JobStore, cfg Config, target, description string) (string, error) {
	return startWorkflowJob(store, cfg, target, description, implementationJobType)
}

func startTidyJob(store *JobStore, cfg Config, target, description string) (string, error) {
	return startWorkflowJob(store, cfg, target, description, tidyJobType)
}

func startWorkflowJob(store *JobStore, cfg Config, target, description, jobType string) (string, error) {
	if cfg.DetachedJobMaxConcurrency != nil && (*cfg.DetachedJobMaxConcurrency < 1 || *cfg.DetachedJobMaxConcurrency > maxDetachedJobConcurrency) {
		return "", fmt.Errorf("detached_job_max_concurrency must be between 1 and %d when set", maxDetachedJobConcurrency)
	}
	if jobType != implementationJobType && jobType != tidyJobType {
		return "", fmt.Errorf("unsupported workflow job type %q", jobType)
	}
	if strings.TrimSpace(description) == "" {
		return "", fmt.Errorf("job description must not be empty")
	}
	resolvedTarget, err := resolvedPath(target)
	if err != nil {
		return "", fmt.Errorf("resolve target directory: %w", err)
	}
	repository, err := runGit(context.Background(), resolvedTarget, "rev-parse", "--show-toplevel")
	if err == nil {
		repository, err = resolvedPath(repository)
		if err != nil {
			return "", err
		}
	}
	branch := ""
	branchRepository := repository
	if repository != "" {
		branch, _ = runGit(context.Background(), resolvedTarget, "branch", "--show-current")
		branchRepository, err = repositoryIdentity(repository)
		if err != nil {
			return "", err
		}
	}
	unlockRepository, err := store.LockRepositoryAdmission(repositoryOrTarget(branchRepository, resolvedTarget))
	if err != nil {
		return "", err
	}
	defer unlockRepository()
	unlockCapacity, err := lockDetachedJobCapacity(store, cfg)
	if err != nil {
		return "", err
	}
	defer unlockCapacity()
	var unlockBranch func()
	if jobType == implementationJobType && branch != "" {
		unlockBranch, err = store.LockBranch(branchRepository, branch)
		if err != nil {
			return "", fmt.Errorf("branch %q is already reserved by detached work: %w", branch, err)
		}
		defer unlockBranch()
	}
	if info, err := os.Stat(resolvedTarget); err != nil || !info.IsDir() {
		return "", fmt.Errorf("target must be an existing directory")
	}
	if isWithin(resolvedTarget, store.Root()) {
		return "", fmt.Errorf("job state directory %s must be outside target directory %s", store.Root(), resolvedTarget)
	}
	unlock, err := store.LockTargetAdmission(resolvedTarget)
	if err != nil {
		return "", err
	}
	defer unlock()
	jobs, err := store.ListJobs()
	if err != nil {
		return "", err
	}
	for _, job := range jobs {
		if job.TargetPath != resolvedTarget || isTerminalStatus(job.Status) {
			continue
		}
		stale, err := store.reconcileOrphan(job)
		if err != nil {
			return "", err
		}
		if !stale {
			return "", fmt.Errorf("active job %s already targets %s", job.ID, resolvedTarget)
		}
	}
	id, err := newJobID()
	if err != nil {
		return "", err
	}
	worktree := ""
	workBranch := ""
	targetHead := ""
	if jobType == implementationJobType {
		if err := requireImplementationCheckout(context.Background(), resolvedTarget); err != nil {
			return "", err
		}
		if cfg.AutoPublish {
			if err := requireCleanCheckout(context.Background(), resolvedTarget); err != nil {
				return "", err
			}
		}
	}
	if jobType == implementationJobType && repository != "" && branch != "" {
		targetHead, err = runGit(context.Background(), resolvedTarget, "rev-parse", "HEAD")
		if err != nil {
			return "", err
		}
		proposalLog := filepath.Join(store.Root(), id+"-slug.log")
		slug := implementationJobSlugWithFallback(context.Background(), Runner{Config: cfg}, description, store.Root(), proposalLog)
		_ = os.Remove(proposalLog)
		primary, err := primaryWorktree(repository)
		if err != nil {
			return "", err
		}
		worktreeParent, err := resolveWorktreeParent(cfg.WorktreeParent, primary)
		if err != nil {
			return "", err
		}
		worktree, workBranch, err = createImplementationWorktreeAtParent(repository, resolvedTarget, targetHead, slug, id, worktreeParent)
		if err != nil {
			return "", err
		}
	}
	job := JobRecord{ID: id, Type: jobType, TaskDescription: description, TargetPath: resolvedTarget, RepositoryPath: branchRepository, TargetBranch: branch, TargetHead: targetHead, Worktree: worktree, WorkBranch: workBranch, Status: "queued"}
	if err := store.CreateJob(job); err != nil {
		if worktree != "" {
			_, _ = runGit(context.Background(), repository, "worktree", "remove", "--force", worktree)
		}
		return "", err
	}
	if _, err := store.CreateSession(id, "workflow", "queued"); err != nil {
		_, _ = store.UpdateJob(id, func(job *JobRecord) error { job.Status = "failed"; return nil })
		if worktree != "" {
			_, _ = runGit(context.Background(), repository, "worktree", "remove", "--force", worktree)
		}
		return "", err
	}
	if err := store.CreateJobLog(id); err != nil {
		_, _ = store.UpdateJob(id, func(job *JobRecord) error { job.Status = "failed"; return nil })
		if worktree != "" {
			_, _ = runGit(context.Background(), repository, "worktree", "remove", "--force", worktree)
		}
		return "", err
	}
	if _, err := launchJobWorker(id, store.Root()); err != nil {
		_, _ = store.UpdateJob(id, func(job *JobRecord) error { job.Status = "failed"; return nil })
		if worktree != "" {
			_, _ = runGit(context.Background(), repository, "worktree", "remove", "--force", worktree)
		}
		return "", err
	}
	return id, nil
}

// RunJobWorker reloads the persisted request and configuration, then owns the job lifecycle.
func RunJobWorker(id, root string) error {
	if !filepath.IsAbs(root) {
		return fmt.Errorf("job state root must be absolute")
	}
	resolvedRoot, err := resolvedPath(root)
	if err != nil || resolvedRoot != root {
		return fmt.Errorf("job state root must use its resolved path")
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
	if job.Type != implementationJobType && job.Type != tidyJobType {
		return fmt.Errorf("unsupported worker job type %q", job.Type)
	}
	var unlockBranch, unlockWorkBranch func()
	if job.Type == implementationJobType && job.TargetBranch != "" {
		branchRepository := job.RepositoryPath
		if branchRepository == "" {
			branchRepository, err = repositoryIdentity(repositoryOrTarget(job.RepositoryPath, job.TargetPath))
			if err != nil {
				return err
			}
		}
		unlockBranch, err = store.LockBranch(branchRepository, job.TargetBranch)
		if err != nil {
			return fmt.Errorf("reserve implementation target branch %q: %w", job.TargetBranch, err)
		}
		defer unlockBranch()
	}
	if job.WorkBranch != "" {
		unlockWorkBranch, err = store.LockBranch(repositoryOrTarget(job.RepositoryPath, job.TargetPath), job.WorkBranch)
		if err != nil {
			return fmt.Errorf("reserve implementation branch %q: %w", job.WorkBranch, err)
		}
		defer unlockWorkBranch()
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
	observer := JobSessionObserver{Store: store, JobID: id, SessionID: "workflow"}
	var runErr error
	if job.Type == tidyJobType {
		runErr = (CleanWorkflow{
			Agent: Runner{Config: cfg, ProcessObserver: jobProcessObserver(store, id, "workflow")}, Config: cfg, In: strings.NewReader(""), Out: os.Stdout,
			Workdir: job.TargetPath, Observer: statusJobObserver{JobSessionObserver: observer, store: store, jobID: id, sessionID: "workflow"}, ProcessObserver: jobProcessObserver(store, id, "workflow"), NeverPublish: true,
		}).RunContext(ctx, job.TaskDescription)
	} else {
		workdir := job.TargetPath
		if job.Worktree != "" {
			workdir = job.Worktree
		}
		workflow := Workflow{
			Agent: Runner{Config: cfg, ProcessObserver: jobProcessObserver(store, id, "workflow")}, Config: cfg, In: strings.NewReader(""), Out: os.Stdout,
			Workdir: workdir, Observer: statusJobObserver{JobSessionObserver: observer, store: store, jobID: id, sessionID: "workflow"},
			ProcessObserver: jobProcessObserver(store, id, "workflow"),
		}
		runErr = workflow.RunContext(ctx, job.TaskDescription)
		if runErr == nil && cfg.AutoPublish && job.Worktree != "" {
			result, publishErr := publishImplementation(ctx, job.Worktree, job.TargetPath, job.TargetHead, job.TargetBranch, job.WorkBranch, job.TaskDescription, cfg.PipelineChecks)
			publicationStatus, publicationSummary := result.Status, result.Message
			if publishErr != nil {
				publicationStatus, publicationSummary = "unpublished", publishErr.Error()
				fmt.Fprintf(os.Stdout, "Workflow completed; PR publication did not complete.\n%s\n", publishErr)
			} else if result.Message != "" {
				fmt.Fprintln(os.Stdout, result.Message)
			}
			if publicationStatus != "" {
				_, updateErr := store.UpdateJob(id, func(record *JobRecord) error {
					record.PublicationStatus = publicationStatus
					record.PublicationSummary = publicationSummary
					return nil
				})
				if updateErr != nil {
					runErr = updateErr
				} else if publicationStatus == "published" {
					if err := cleanupPublishedImplementationWorktree(job.TargetPath, job.Worktree); err != nil {
						fmt.Fprintf(os.Stdout, "Published result is intact, but cleanup failed; worktree was retained at %s: %v\n", job.Worktree, err)
					}
				}
			}
		}
	}
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
	return newUUIDv4()
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
