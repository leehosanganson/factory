package factory

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	monitorPollDefault        = 30 * time.Second
	monitorSnapshotMaxRetries = 8
)

type monitorBranchLockContextKey struct{}

var (
	monitorWorkerLauncher    = launchMonitorWorker
	monitorAcquireWorkerLock = acquireOwnedLock
	monitorRetryDelay        = snapshotRetryDelay
	monitorLogOpenFile       = os.OpenFile
	monitorSessionLogAppend  = func(store *JobStore, id string, data []byte) error {
		return store.AppendSessionLog(id, monitorSessionID, data)
	}
	monitorAgentActionTimeout = 30 * time.Minute
)

var monitorIDPattern = regexp.MustCompile(`^(?:[0-9]{8}T[0-9]{6}-[a-f0-9]{12}|[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})$`)

type monitorJob struct {
	ID                 string              `json:"id"`
	Description        string              `json:"description"`
	RepoRoot           string              `json:"repo_root"`
	Repo               string              `json:"repo"`
	PR                 int                 `json:"pr"`
	HeadRepo           string              `json:"head_repo"`
	HeadBranch         string              `json:"head_branch"`
	BaseRepo           string              `json:"base_repo"`
	BaseBranch         string              `json:"base_branch"`
	BaseSHA            string              `json:"base_sha"`
	HeartbeatPath      string              `json:"heartbeat_path,omitempty"`
	OriginURL          string              `json:"origin_url"`
	HeadRepoURL        string              `json:"head_repo_url"`
	BaselineHead       string              `json:"baseline_head"`
	TargetBaseline     string              `json:"target_baseline"`
	TargetBranch       string              `json:"target_branch,omitempty"`
	OwnWorktree        bool                `json:"own_worktree,omitempty"`
	WorktreeParent     string              `json:"worktree_parent,omitempty"`
	Worktree           string              `json:"worktree,omitempty"`
	WorkerBranch       string              `json:"worker_branch,omitempty"`
	Status             string              `json:"status"`
	Phase              string              `json:"phase,omitempty"`
	CreatedAt          time.Time           `json:"created_at"`
	UpdatedAt          time.Time           `json:"updated_at"`
	LastEvent          string              `json:"last_event,omitempty"`
	RecentEvents       []monitorTraceEvent `json:"recent_events,omitempty"`
	LatestCheckAt      time.Time           `json:"latest_check_at,omitempty"`
	LatestCheckResult  string              `json:"latest_check_result,omitempty"`
	Snapshot           string              `json:"snapshot,omitempty"`
	PendingSignature   string              `json:"pending_signature,omitempty"`
	Proposal           string              `json:"proposal,omitempty"`
	ApprovalScope      string              `json:"approval_scope,omitempty"`
	ApprovalSignature  string              `json:"approval_signature,omitempty"`
	RejectedSignature  string              `json:"rejected_signature,omitempty"`
	Attempts           int                 `json:"attempts,omitempty"`
	AttemptsSignature  string              `json:"attempts_signature,omitempty"`
	ProcessedSignature string              `json:"processed_signature,omitempty"`
	PID                int                 `json:"pid,omitempty"`
	Heartbeat          time.Time           `json:"heartbeat,omitempty"`
	StopRequested      bool                `json:"stop_requested,omitempty"`
	SnapshotFailures   int                 `json:"snapshot_failures,omitempty"`
	DeadlineAt         time.Time           `json:"deadline_at,omitempty"`
}

const monitorRecentEventLimit = 12

// monitorTraceEvent is a concise, bounded status transition retained for observers.
type monitorTraceEvent struct {
	At      time.Time `json:"at"`
	Phase   string    `json:"phase"`
	Message string    `json:"message"`
}

type monitorSnapshot struct {
	Number            int             `json:"number"`
	State             string          `json:"state"`
	Title             string          `json:"title"`
	URL               string          `json:"url"`
	HeadRefName       string          `json:"headRefName"`
	HeadRefOID        string          `json:"headRefOid"`
	HeadRepository    *ghRepository   `json:"headRepository"`
	BaseRefName       string          `json:"baseRefName"`
	BaseRefOID        string          `json:"baseRefOid"`
	Comments          json.RawMessage `json:"comments"`
	ReviewThreads     json.RawMessage `json:"reviewThreads,omitempty"`
	StatusCheckRollup json.RawMessage `json:"statusCheckRollup"`
}

type ghRepository struct {
	NameWithOwner string `json:"nameWithOwner"`
	URL           string `json:"url"`
	SSHURL        string `json:"sshUrl"`
}

func monitorRoot(cfg Config) (string, error) {
	base := cfg.StateDir
	if base == "" {
		base = os.Getenv("XDG_STATE_HOME")
		if base == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return "", err
			}
			base = filepath.Join(home, ".local", "state")
		}
	}
	if !filepath.IsAbs(base) {
		return "", fmt.Errorf("state directory must be absolute")
	}
	return JobStateRoot(base)
}

func monitorJobDir(root, id string) (string, error) {
	if !monitorIDPattern.MatchString(id) {
		return "", fmt.Errorf("invalid job id")
	}
	return filepath.Join(root, id), nil
}

func saveMonitorJob(dir string, job *monitorJob) error {
	if err := writeMonitorJob(dir, job); err != nil {
		return err
	}
	return syncMonitorJobStatus(dir, job)
}

func writeMonitorJob(dir string, monitor *monitorJob) error {
	monitor.UpdatedAt = time.Now().UTC()
	if monitor.PID != 0 {
		monitor.Heartbeat = monitor.UpdatedAt
	}
	store, exists, err := monitorJobStore(dir)
	if err != nil {
		return err
	}
	if !exists {
		copy := *monitor
		if err := store.CreateJob(JobRecord{ID: monitor.ID, Type: monitorJobType, TaskDescription: monitor.Description, TargetPath: monitor.RepoRoot, Status: monitor.Status, Monitor: &copy}); err != nil {
			return err
		}
		if _, err := store.CreateSession(monitor.ID, monitorSessionID, monitor.Status); err != nil {
			return err
		}
	}
	_, err = store.UpdateJob(monitor.ID, func(job *JobRecord) error {
		copy := *monitor
		if job.Monitor != nil && !job.Monitor.DeadlineAt.IsZero() {
			copy.DeadlineAt = job.Monitor.DeadlineAt
		}
		job.Monitor = &copy
		return nil
	})
	return err
}

func readMonitorJob(dir string) (*monitorJob, error) {
	store, _, err := monitorJobStore(dir)
	if err != nil {
		return nil, err
	}
	job, err := store.GetJob(filepath.Base(dir))
	if err != nil {
		return nil, err
	}
	if job.Type != monitorJobType || job.Monitor == nil || job.Monitor.ID != job.ID {
		return nil, fmt.Errorf("invalid canonical monitor job record")
	}
	return job.Monitor, nil
}

func appendMonitorLog(dir, message string) error {
	store, exists, err := monitorJobStore(dir)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("detached monitor job %s does not exist", filepath.Base(dir))
	}
	line := fmt.Sprintf("[%s] %s\n", time.Now().UTC().Format(time.RFC3339), message)
	return monitorSessionLogAppend(store, filepath.Base(dir), []byte(line))
}

func monitorEvent(dir string, job *monitorJob, message string) error {
	if err := appendMonitorLog(dir, message); err != nil {
		return err
	}
	if isTerminalStatus(job.Status) || job.Status == "recoverable_failure" {
		job.Phase = monitorPhaseForStatus(job.Status)
	} else if job.PendingSignature != "" && job.Proposal != "" && job.ApprovalSignature != job.PendingSignature {
		job.Phase = "approval_pending"
	} else if job.Phase == "" {
		job.Phase = monitorPhaseForStatus(job.Status)
	}
	now := time.Now().UTC()
	recordMonitorRecentEvent(job, job.Phase, message, now)
	if err := writeMonitorJob(dir, job); err != nil {
		return err
	}
	return syncMonitorJobEvent(dir, job)
}

func monitorPhaseForStatus(status string) string {
	switch status {
	case "queued":
		return "starting"
	case "recoverable_failure":
		return "recoverable_failure"
	case "complete":
		return "complete"
	case "closed":
		return "closed"
	case "stopped", "cancelled":
		return "stopped"
	case "failed", "interrupted":
		return "failed"
	default:
		return "polling"
	}
}

func conciseMonitorEvent(message string) string {
	message = strings.Join(strings.Fields(terminalSafeText(message)), " ")
	if len(message) > 180 {
		message = truncateUTF8(message, 177) + "…"
	}
	return message
}

func recordMonitorRecentEvent(job *monitorJob, phase, message string, at time.Time) {
	job.LastEvent = message
	job.RecentEvents = append(job.RecentEvents, monitorTraceEvent{At: at, Phase: phase, Message: conciseMonitorEvent(message)})
	if len(job.RecentEvents) > monitorRecentEventLimit {
		job.RecentEvents = append([]monitorTraceEvent(nil), job.RecentEvents[len(job.RecentEvents)-monitorRecentEventLimit:]...)
	}
}

func setMonitorPhase(dir string, job *monitorJob, phase, message string) error {
	if job.Phase == phase {
		return nil
	}
	job.Phase = phase
	now := time.Now().UTC()
	recordMonitorRecentEvent(job, phase, message, now)
	return writeMonitorJob(dir, job)
}

func monitorCheckSummary(raw json.RawMessage) string {
	var checks []map[string]any
	if json.Unmarshal(raw, &checks) != nil {
		return "check results unavailable"
	}
	passed, failed, pending, neutral := 0, 0, 0, 0
	for _, check := range checks {
		state := ""
		for _, key := range []string{"conclusion", "state"} {
			if value, ok := check[key].(string); ok && value != "" {
				state = strings.ToUpper(value)
				break
			}
		}
		switch state {
		case "SUCCESS", "SUCCEEDED":
			passed++
		case "FAILURE", "FAILED", "TIMED_OUT", "ERROR", "ACTION_REQUIRED":
			failed++
		case "NEUTRAL", "SKIPPED":
			neutral++
		default:
			pending++
		}
	}
	return fmt.Sprintf("%d passed, %d failed, %d pending, %d neutral/skipped", passed, failed, pending, neutral)
}

func loadJobs(root string) ([]monitorJob, error) {
	store, err := NewJobStore(root)
	if err != nil {
		return nil, err
	}
	records, err := store.ListJobs()
	if err != nil {
		return nil, err
	}
	jobs := make([]monitorJob, 0, len(records))
	for _, record := range records {
		if record.Type == monitorJobType && record.Monitor != nil {
			jobs = append(jobs, *record.Monitor)
		}
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].CreatedAt.After(jobs[j].CreatedAt) })
	return jobs, nil
}

func runGit(ctx context.Context, dir string, args ...string) (string, error) {
	return runGitWithEnv(ctx, dir, nil, args...)
}

func runGitWithEnv(ctx context.Context, dir string, extraEnv []string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	if len(extraEnv) != 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

func runGitInput(ctx context.Context, dir string, input []byte, args ...string) (string, error) {
	return runGitInputWithEnv(ctx, dir, nil, input, args...)
}

func runGitInputWithEnv(ctx context.Context, dir string, extraEnv []string, input []byte, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	if len(extraEnv) != 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	cmd.Stdin = bytes.NewReader(input)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

func runGH(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "gh", args...)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("gh %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out.Bytes(), nil
}

func readSnapshot(ctx context.Context, job *monitorJob) (*monitorSnapshot, string, error) {
	data, err := runGH(ctx, "pr", "view", strconv.Itoa(job.PR), "--repo", job.Repo, "--json", "number,state,title,url,headRefName,headRefOid,headRepository,baseRefName,baseRefOid,comments,statusCheckRollup")
	if err != nil {
		return nil, "", err
	}
	var s monitorSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, "", fmt.Errorf("invalid GitHub PR JSON: %w", err)
	}
	baseRepo, number, err := parsePRURL(s.URL)
	if err != nil || number != s.Number || number != job.PR || baseRepo != job.BaseRepo || baseRepo != job.Repo || s.Number != job.PR || s.HeadRefName != job.HeadBranch || s.BaseRefName != job.BaseBranch || s.HeadRefOID == "" || s.BaseRefOID == "" || (job.BaseSHA != "" && s.BaseRefOID != job.BaseSHA) || s.HeadRepository == nil || s.HeadRepository.NameWithOwner != job.HeadRepo {
		return nil, "", errors.New("PR identity changed or metadata is incomplete")
	}
	threadJSON, err := readReviewThreads(ctx, job)
	if err != nil {
		return nil, "", err
	}
	s.ReviewThreads = threadJSON
	canonical, err := json.Marshal(s)
	if err != nil {
		return nil, "", err
	}
	hash := sha256.Sum256(canonical)
	return &s, hex.EncodeToString(hash[:]), nil
}

func readReviewThreads(ctx context.Context, job *monitorJob) (json.RawMessage, error) {
	if len(job.BaseRepo) == 0 || strings.Count(job.BaseRepo, "/") != 1 {
		return nil, errors.New("invalid PR base repository for review thread lookup")
	}
	parts := strings.SplitN(job.BaseRepo, "/", 2)
	type reviewComment struct {
		Body         string `json:"body"`
		Path         string `json:"path"`
		Line         *int   `json:"line"`
		OriginalLine *int   `json:"originalLine"`
	}
	type commentPage struct {
		Nodes    []reviewComment `json:"nodes"`
		PageInfo *struct {
			HasNextPage bool   `json:"hasNextPage"`
			EndCursor   string `json:"endCursor"`
		} `json:"pageInfo"`
	}
	type reviewThread struct {
		ID         string       `json:"id"`
		IsResolved bool         `json:"isResolved"`
		Comments   *commentPage `json:"comments"`
	}
	fetch := func(query string, variables ...string) ([]byte, error) {
		args := []string{"api", "graphql", "-f", "query=" + query}
		args = append(args, "-F", "owner="+parts[0], "-F", "repo="+parts[1], "-F", "number="+strconv.Itoa(job.PR))
		args = append(args, variables...)
		data, err := runGH(ctx, args...)
		if err != nil {
			return nil, fmt.Errorf("read PR review threads: %w", err)
		}
		return data, nil
	}
	const threadQuery = `query($owner:String!, $repo:String!, $number:Int!, $after:String) { repository(owner:$owner, name:$repo) { pullRequest(number:$number) { reviewThreads(first:100, after:$after) { nodes { id isResolved comments(first:100) { nodes { body path line originalLine } pageInfo { hasNextPage endCursor } } } pageInfo { hasNextPage endCursor } } } } }`
	const commentsQuery = `query($id:ID!, $after:String) { node(id:$id) { ... on PullRequestReviewThread { comments(first:100, after:$after) { nodes { body path line originalLine } pageInfo { hasNextPage endCursor } } } } }`
	var unresolved []reviewThread
	threadCursor := ""
	for {
		variables := []string{}
		if threadCursor != "" {
			variables = append(variables, "-f", "after="+threadCursor)
		}
		data, err := fetch(threadQuery, variables...)
		if err != nil {
			return nil, err
		}
		var response struct {
			Data *struct {
				Repository *struct {
					PullRequest *struct {
						ReviewThreads *struct {
							Nodes    []reviewThread `json:"nodes"`
							PageInfo *struct {
								HasNextPage bool   `json:"hasNextPage"`
								EndCursor   string `json:"endCursor"`
							} `json:"pageInfo"`
						} `json:"reviewThreads"`
					} `json:"pullRequest"`
				} `json:"repository"`
			} `json:"data"`
			Errors []json.RawMessage `json:"errors"`
		}
		if err := json.Unmarshal(data, &response); err != nil {
			return nil, fmt.Errorf("invalid GitHub review thread JSON: %w", err)
		}
		if len(response.Errors) > 0 || response.Data == nil || response.Data.Repository == nil || response.Data.Repository.PullRequest == nil || response.Data.Repository.PullRequest.ReviewThreads == nil {
			return nil, errors.New("GitHub review thread response is incomplete or contains errors")
		}
		page := response.Data.Repository.PullRequest.ReviewThreads
		if page.PageInfo == nil {
			return nil, errors.New("GitHub review thread response omitted pagination metadata")
		}
		for _, thread := range page.Nodes {
			if thread.Comments == nil || thread.Comments.PageInfo == nil {
				return nil, errors.New("GitHub review thread response omitted comment pagination data")
			}
			lastCommentCursor := ""
			for thread.Comments.PageInfo.HasNextPage {
				cursor := thread.Comments.PageInfo.EndCursor
				if thread.ID == "" || cursor == "" || cursor == lastCommentCursor {
					return nil, errors.New("GitHub review thread comment page omitted a valid cursor")
				}
				lastCommentCursor = cursor
				data, err := runGH(ctx, "api", "graphql", "-f", "query="+commentsQuery, "-F", "id="+thread.ID, "-f", "after="+cursor)
				if err != nil {
					return nil, fmt.Errorf("read PR review comments: %w", err)
				}
				var commentResponse struct {
					Data *struct {
						Node *struct {
							Comments *commentPage `json:"comments"`
						} `json:"node"`
					} `json:"data"`
					Errors []json.RawMessage `json:"errors"`
				}
				if err := json.Unmarshal(data, &commentResponse); err != nil {
					return nil, fmt.Errorf("invalid GitHub review comment JSON: %w", err)
				}
				if len(commentResponse.Errors) > 0 || commentResponse.Data == nil || commentResponse.Data.Node == nil || commentResponse.Data.Node.Comments == nil || commentResponse.Data.Node.Comments.PageInfo == nil {
					return nil, errors.New("GitHub review comment response is incomplete or contains errors")
				}
				next := commentResponse.Data.Node.Comments
				thread.Comments.Nodes = append(thread.Comments.Nodes, next.Nodes...)
				thread.Comments.PageInfo = next.PageInfo
			}
			if !thread.IsResolved {
				unresolved = append(unresolved, thread)
			}
		}
		if !page.PageInfo.HasNextPage {
			break
		}
		if page.PageInfo.EndCursor == "" || page.PageInfo.EndCursor == threadCursor {
			return nil, errors.New("GitHub review thread page omitted a valid cursor")
		}
		threadCursor = page.PageInfo.EndCursor
	}
	return json.Marshal(unresolved)
}

func parsePRURL(raw string) (string, int, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return "", 0, errors.New("invalid PR URL")
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != 4 || parts[2] != "pull" {
		return "", 0, errors.New("invalid PR URL path")
	}
	owner, err := url.PathUnescape(parts[0])
	if err != nil || owner == "" || strings.ContainsAny(owner, "/\\") {
		return "", 0, errors.New("invalid PR URL owner")
	}
	repo, err := url.PathUnescape(parts[1])
	if err != nil || repo == "" || strings.ContainsAny(repo, "/\\") {
		return "", 0, errors.New("invalid PR URL repository")
	}
	if parts[3] == "" {
		return "", 0, errors.New("invalid PR URL number")
	}
	for _, digit := range parts[3] {
		if digit < '0' || digit > '9' {
			return "", 0, errors.New("invalid PR URL number")
		}
	}
	number, err := strconv.Atoi(parts[3])
	if err != nil || number <= 0 {
		return "", 0, errors.New("invalid PR URL number")
	}
	return owner + "/" + repo, number, nil
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, syscall.EPERM)
}

func workerFresh(job monitorJob) bool {
	info, err := os.Stat(job.HeartbeatPath)
	return err == nil && time.Since(info.ModTime()) < 2*time.Minute
}

func monitorID() (string, error) {
	return newUUIDv4()
}

type lockOwner struct {
	PID int `json:"pid"`
}

func acquireOwnedLock(path string) (func(), error) {
	for attempt := 0; attempt < 2; attempt++ {
		if err := os.Mkdir(path, 0o700); err == nil {
			owner, _ := json.Marshal(lockOwner{PID: os.Getpid()})
			if err := os.WriteFile(filepath.Join(path, "owner.json"), owner, 0o600); err != nil {
				_ = os.RemoveAll(path)
				return nil, err
			}
			return func() {
				if current, err := readLockOwner(path); err == nil && current.PID == os.Getpid() {
					_ = os.RemoveAll(path)
				}
			}, nil
		} else if !os.IsExist(err) {
			return nil, err
		}
		owner, err := readLockOwner(path)
		if err != nil || processAlive(owner.PID) {
			return nil, fmt.Errorf("lock is held or owner cannot be verified: %s", path)
		}
		stale := fmt.Sprintf("%s.stale-%d", path, os.Getpid())
		if err := os.Rename(path, stale); err == nil {
			movedOwner, movedErr := readLockOwner(stale)
			if movedErr != nil || processAlive(movedOwner.PID) {
				if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
					_ = os.Rename(stale, path)
				}
				return nil, fmt.Errorf("lock owner is live or cannot be verified: %s", path)
			}
			_ = os.RemoveAll(stale)
		}
	}
	return nil, fmt.Errorf("could not acquire lock: %s", path)
}

func readLockOwner(path string) (lockOwner, error) {
	var owner lockOwner
	data, err := os.ReadFile(filepath.Join(path, "owner.json"))
	if err != nil {
		return owner, err
	}
	err = json.Unmarshal(data, &owner)
	if err == nil && owner.PID <= 0 {
		err = errors.New("invalid lock owner")
	}
	return owner, err
}

func acquireMonitorLock(root string) (func(), error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	return acquireOwnedLock(filepath.Join(root, ".lock"))
}

// MonitorCommand implements monitor user-facing and internal worker commands.
func MonitorCommand(args []string, cfg Config, workdir string, in io.Reader, out, errOut io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: factory monitor <description> | list | get <id> [--details] | approve <id> | reject <id> | stop <id> | reset <id>")
	}
	root, err := monitorRoot(cfg)
	if err != nil {
		return err
	}
	if args[0] == "--worker" {
		if len(args) != 2 {
			return fmt.Errorf("invalid internal worker arguments")
		}
		if err := runMonitorWorker(args[1], root, cfg); err != nil {
			return err
		}
		return nil
	}
	switch args[0] {
	case "list":
		if len(args) != 1 {
			return fmt.Errorf("list takes no arguments")
		}
		if err := os.MkdirAll(root, 0o700); err != nil {
			return err
		}
		jobs, err := loadJobs(root)
		if err != nil {
			return err
		}
		if len(jobs) == 0 {
			fmt.Fprintln(out, "No monitor jobs.")
			return nil
		}
		fmt.Fprintf(out, "%-29s %-20s %-28s %-8s %s\n", "ID", "STATUS", "REPO", "PR", "UPDATED")
		for _, j := range jobs {
			fmt.Fprintf(out, "%-29s %-20s %-28s #%d %s\n", j.ID, j.Status, j.Repo, j.PR, j.UpdatedAt.Format(time.RFC3339))
		}
		return nil
	case "get":
		id, details, err := parseDetailsID("factory monitor get <id> [--details]", args[1:])
		if err != nil {
			return err
		}
		job, _, err := findMonitorJob(root, id)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "ID: %s\nStatus: %s\nRepository: %s\nPR: #%d\nBranch: %s\n", job.ID, job.Status, job.Repo, job.PR, job.HeadBranch); err != nil {
			return err
		}
		if err := writeMonitorStatus(out, job); err != nil {
			return err
		}
		if !details {
			return nil
		}
		if err := writeMonitorRecentEvents(out, job.RecentEvents, ""); err != nil {
			return err
		}
		data, err := json.MarshalIndent(job, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "Details:\n%s\n", data)

		store, err := NewJobStore(root)
		if err != nil {
			return err
		}
		record, err := store.GetJob(job.ID)
		if err != nil {
			return err
		}
		return writeJobDetails(out, store, record)
	case "approve", "reject", "stop", "reset":
		if len(args) != 2 {
			return fmt.Errorf("%s requires a job id", args[0])
		}
		return monitorAction(args[0], root, args[1], cfg, in, out)
	case "-h", "--help", "help":
		fmt.Fprintln(out, "Usage: factory monitor <description>\n       factory monitor list\n       factory monitor get <id> [--details]\n       factory monitor approve <id>\n       factory monitor reject <id>\n       factory monitor stop <id>\n       factory monitor reset <id>")
		return nil
	default:
		return startMonitor(args, cfg, workdir, root, out)
	}
}

func writeMonitorStatus(out io.Writer, job *monitorJob) error {
	phase := monitorDisplayPhase(job)
	if _, err := fmt.Fprintf(out, "Phase: %s\n", phase); err != nil {
		return err
	}
	checkAt := "never"
	if !job.LatestCheckAt.IsZero() {
		checkAt = job.LatestCheckAt.UTC().Format(time.RFC3339)
	}
	checkResult := job.LatestCheckResult
	if checkResult == "" {
		checkResult = "unavailable"
	}
	if _, err := fmt.Fprintf(out, "Latest successful PR/check query: %s (%s)\n", checkAt, checkResult); err != nil {
		return err
	}
	if job.PendingSignature != "" && job.Proposal != "" && job.ApprovalSignature != job.PendingSignature {
		if _, err := fmt.Fprintf(out, "Approval pending: %s\n", conciseMonitorEvent(job.Proposal)); err != nil {
			return err
		}
	}
	return nil
}

func monitorDisplayPhase(job *monitorJob) string {
	if isTerminalStatus(job.Status) || job.Status == "recoverable_failure" {
		return monitorPhaseForStatus(job.Status)
	}
	if job.PendingSignature != "" && job.Proposal != "" && job.ApprovalSignature != job.PendingSignature {
		return "approval_pending"
	}
	if job.Phase != "" {
		return job.Phase
	}
	return monitorPhaseForStatus(job.Status)
}

func findMonitorJob(root, id string) (*monitorJob, string, error) {
	dir, err := monitorJobDir(root, id)
	if err != nil {
		return nil, "", err
	}
	job, err := readMonitorJob(dir)
	if err != nil {
		return nil, "", fmt.Errorf("job not found: %s", id)
	}
	return job, dir, nil
}

func startMonitorLegacy(args []string, cfg Config, workdir, root string, out io.Writer) error {
	description := strings.TrimSpace(strings.Join(args, " "))
	if description == "" {
		return fmt.Errorf("description cannot be empty")
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	poll := os.Getenv("FACTORY_MONITOR_POLL_INTERVAL")
	if poll != "" {
		if parsed, err := time.ParseDuration(poll); err != nil || parsed < time.Second {
			return fmt.Errorf("FACTORY_MONITOR_POLL_INTERVAL must be at least 1s")
		}
	}
	rootRepo, err := runGit(context.Background(), workdir, "rev-parse", "--show-toplevel")
	if err != nil {
		return fmt.Errorf("not inside a git repository: %w", err)
	}
	rootRepo, err = resolvedPath(rootRepo)
	if err != nil {
		return err
	}
	statePath, err := resolvedPath(root)
	if err != nil {
		return err
	}
	if isWithin(rootRepo, statePath) {
		return fmt.Errorf("monitor state must be outside target repository")
	}
	branch, err := runGit(context.Background(), rootRepo, "branch", "--show-current")
	if err != nil || branch == "" {
		return fmt.Errorf("current branch must be checked out")
	}
	status, err := runGit(context.Background(), rootRepo, "status", "--porcelain")
	if err != nil {
		return err
	}
	if status != "" {
		return fmt.Errorf("working tree must be clean before starting")
	}
	origin, err := runGit(context.Background(), rootRepo, "remote", "get-url", "origin")
	if err != nil {
		return fmt.Errorf("origin remote is required: %w", err)
	}
	data, err := runGH(context.Background(), "pr", "view", "--json", "number,state,url,headRefName,headRefOid,headRepository,baseRefName,baseRefOid")
	if err != nil {
		return fmt.Errorf("cannot identify current PR: %w", err)
	}
	var info monitorSnapshot
	if err := json.Unmarshal(data, &info); err != nil {
		return err
	}
	baseRepo, prNumber, err := parsePRURL(info.URL)
	if info.Number <= 0 || prNumber != info.Number || !strings.EqualFold(info.State, "OPEN") || info.HeadRefName != branch || info.HeadRefOID == "" || info.BaseRefOID == "" || info.HeadRepository == nil || err != nil {
		return fmt.Errorf("current PR metadata is incomplete or branch does not match")
	}
	headName, err := runGit(context.Background(), rootRepo, "config", "--get", "remote.origin.url")
	if err != nil {
		return err
	}
	if !sameRepoURL(origin, headName) {
		return fmt.Errorf("origin URL validation failed")
	}
	headRepo := info.HeadRepository.NameWithOwner
	repoData, err := runGH(context.Background(), "repo", "view", headRepo, "--json", "url,sshUrl")
	if err != nil {
		return fmt.Errorf("cannot validate PR head repository URL: %w", err)
	}
	var repoURL ghRepository
	if err := json.Unmarshal(repoData, &repoURL); err != nil {
		return err
	}
	if !sameRepoURL(origin, repoURL.URL) && !sameRepoURL(origin, repoURL.SSHURL) {
		return fmt.Errorf("origin does not point to PR head repository %s", headRepo)
	}
	localHead, err := runGit(context.Background(), rootRepo, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if localHead != info.HeadRefOID {
		return fmt.Errorf("local branch is not at validated PR head")
	}
	unlock, err := acquireMonitorLock(root)
	if err != nil {
		return err
	}
	defer unlock()
	jobs, _ := loadJobs(root)
	for _, j := range jobs {
		if j.Repo == baseRepo && j.PR == info.Number && isMonitorActive(j.Status) && (j.Status == "queued" || workerFresh(j) && processAlive(j.PID)) {
			return fmt.Errorf("an active monitor already monitors %s#%d (%s)", j.Repo, j.PR, j.ID)
		}
	}
	id, err := monitorID()
	if err != nil {
		return err
	}
	dir := filepath.Join(root, id)
	store, err := NewJobStore(root)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(store.Root(), id), 0o700); err != nil {
		return err
	}
	dir = filepath.Join(store.Root(), id)
	workerBranch := "factory-monitor/" + id
	worktree := filepath.Join(dir, "checkout")
	if _, err := runGit(context.Background(), rootRepo, "worktree", "add", "-b", workerBranch, worktree, localHead); err != nil {
		_ = os.RemoveAll(dir)
		return fmt.Errorf("create isolated worker worktree: %w", err)
	}
	job := &monitorJob{ID: id, Description: description, RepoRoot: rootRepo, Repo: baseRepo, PR: info.Number, HeadRepo: headRepo, HeadBranch: branch, BaseRepo: baseRepo, BaseBranch: info.BaseRefName, BaseSHA: info.BaseRefOID, OriginURL: origin, HeadRepoURL: repoURL.URL, HeartbeatPath: filepath.Join(dir, "heartbeat"), BaselineHead: localHead, TargetBaseline: localHead, Worktree: worktree, WorkerBranch: workerBranch, Status: "queued", CreatedAt: time.Now().UTC()}
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TaskDescription: description, TargetPath: rootRepo, Status: "queued"}); err != nil {
		_, _ = runGit(context.Background(), rootRepo, "worktree", "remove", "--force", worktree)
		_ = os.RemoveAll(dir)
		return err
	}
	if _, err := store.CreateSession(id, monitorSessionID, "queued"); err != nil {
		_, _ = runGit(context.Background(), rootRepo, "worktree", "remove", "--force", worktree)
		_ = os.RemoveAll(dir)
		return err
	}
	if err := saveMonitorJob(dir, job); err != nil {
		_, _ = runGit(context.Background(), rootRepo, "worktree", "remove", "--force", worktree)
		_ = os.RemoveAll(dir)
		return err
	}
	_ = appendMonitorLog(dir, "Watcher metadata registered; starting detached monitor.")
	pid, err := monitorWorkerLauncher(id, root)
	if err != nil {
		job.Status = "failed"
		_ = saveMonitorJob(dir, job)
		_ = appendMonitorLog(dir, "Could not launch worker: "+err.Error())
		return err
	}
	if err := registerMonitorWorkerIfMissing(job.ID, dir, pid); err != nil {
		return err
	}
	registered, readErr := readMonitorJob(dir)
	if readErr != nil {
		return readErr
	}
	if registered.PID == 0 {
		registered.PID = pid
		if err := saveMonitorJob(dir, registered); err != nil {
			return err
		}
	}
	fmt.Fprintf(out, "Started monitor %s for %s#%d (%s).\n", id, job.Repo, job.PR, job.HeadBranch)
	return nil
}

func isMonitorActive(status string) bool {
	return status == "queued" || status == "running"
}

func monitorAction(action, root, id string, cfg Config, in io.Reader, out io.Writer) error {
	unlock, err := acquireMonitorLock(root)
	if err != nil {
		return err
	}
	defer unlock()
	job, dir, err := findMonitorJob(root, id)
	if err != nil {
		return err
	}
	switch action {
	case "stop":
		job.StopRequested = true
		job.Phase = "stopping"
		store, err := NewJobStore(root)
		if err != nil {
			return err
		}
		if err := store.RequestStop(job.ID); err != nil {
			return err
		}
		if err := monitorEvent(dir, job, "Cooperative stop requested."); err != nil {
			return err
		}
		fmt.Fprintf(out, "Stop requested for %s.\n", id)
		return nil
	case "reset":
		if job.Status != "recoverable_failure" {
			return fmt.Errorf("job is not recoverable")
		}
		if processAlive(job.PID) {
			return fmt.Errorf("worker is still running")
		}
		job.SnapshotFailures = 0
		job.StopRequested = false

		job.Status = "queued"
		job.PID = 0
		jobStateRoot := root
		if err := resetMonitorJobRecord(job.ID, jobStateRoot); err != nil {
			return err
		}
		if err := monitorEvent(dir, job, "Recoverable failure reset; detached worker restarting."); err != nil {
			return err
		}
		pid, err := monitorWorkerLauncher(job.ID, root)
		if err != nil {
			job.Status = "recoverable_failure"
			_ = saveMonitorJob(dir, job)
			return err
		}
		if err := registerMonitorWorkerIfMissing(job.ID, dir, pid); err != nil {
			return err
		}
		registered, readErr := readMonitorJob(dir)
		if readErr == nil && registered.PID == 0 {
			registered.PID = pid
			_ = saveMonitorJob(dir, registered)
		}
		return nil
	case "reject":
		if job.PendingSignature == "" || job.Proposal == "" {
			return fmt.Errorf("job has no pending proposal")
		}
		job.RejectedSignature = job.PendingSignature
		job.Phase = "polling"
		job.PendingSignature = ""
		job.Proposal = ""
		job.ApprovalScope = ""
		job.ApprovalSignature = ""
		job.Status = "running"
		return monitorEvent(dir, job, "Proposal rejected; monitoring continues without repeating this action.")
	case "approve":
		if job.PendingSignature == "" || job.Proposal == "" {
			return fmt.Errorf("job has no pending proposal")
		}
		fmt.Fprintf(out, "Pending proposal for %s:\n%s\n", id, job.Proposal)
		fmt.Fprintf(out, "Approve proposal for %s? Type exact lowercase y: ", id)
		reader := bufio.NewReader(in)
		answer, _ := reader.ReadString('\n')
		if strings.TrimSpace(answer) != "y" {
			return fmt.Errorf("approval cancelled")
		}
		fmt.Fprint(out, "Approved scope (non-empty task text): ")
		scope, _ := reader.ReadString('\n')
		scope = strings.TrimSpace(scope)
		if scope == "" {
			return fmt.Errorf("approved scope cannot be empty")
		}
		snap, signature, err := readSnapshot(context.Background(), job)
		if err != nil || signature != job.PendingSignature || snap.HeadRefOID != job.BaselineHead {
			job.PendingSignature = ""
			job.Proposal = ""
			job.ApprovalScope = ""
			job.ApprovalSignature = ""
			job.Status = "running"
			job.Phase = "polling"
			_ = monitorEvent(dir, job, "Approval invalidated: live PR/check snapshot changed.")
			return fmt.Errorf("proposal snapshot changed; watcher must reassess")
		}
		job.Phase = "approval_scoped"
		job.ApprovalScope = scope
		job.ApprovalSignature = signature
		job.RejectedSignature = ""
		job.Status = "running"
		return monitorEvent(dir, job, "Human approved a non-empty scope bound to the current PR/check snapshot.")
	}
	return fmt.Errorf("unknown action")
}

func readPollInterval() time.Duration {
	if value := os.Getenv("FACTORY_MONITOR_POLL_INTERVAL"); value != "" {
		if n, err := time.ParseDuration(value); err == nil && n >= time.Second {
			return n
		}
	}
	return monitorPollDefault
}

func runMonitorWorker(id, root string, cfg Config) error {
	dir, err := monitorJobDir(root, id)
	if err != nil {
		return err
	}
	job, err := readMonitorJob(dir)
	if err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	monitorTimeout, err := cfg.monitorTimeout()
	if err != nil {
		return err
	}
	firstStartDeadline := time.Time{}
	if monitorTimeout > 0 {
		firstStartDeadline = time.Now().UTC().Add(monitorTimeout)
	}
	recoverableRunning := job.Status == "running" && job.PID == 0 && !workerFresh(*job)
	if job.DeadlineAt.IsZero() && monitorTimeout > 0 && (job.Status == "queued" || recoverableRunning) && !job.StopRequested {
		job, err = persistMonitorDeadline(root, id, monitorTimeout, firstStartDeadline)
		if err != nil {
			return err
		}
	}
	lockDeadline := job.DeadlineAt
	if lockDeadline.IsZero() {
		lockDeadline = firstStartDeadline
	}
	var lockCtx context.Context
	var cancelLock context.CancelFunc
	if lockDeadline.IsZero() {
		lockCtx, cancelLock = context.WithCancel(context.Background())
	} else {
		lockCtx, cancelLock = context.WithDeadline(context.Background(), lockDeadline)
	}
	defer cancelLock()
	workerLock := filepath.Join(dir, "worker.lock")
	lockRetry := time.NewTicker(100 * time.Millisecond)
	defer lockRetry.Stop()
	var unlockWorker func()
	for {
		unlockWorker, err = monitorAcquireWorkerLock(workerLock)
		if err == nil {
			defer unlockWorker()
			break
		}
		if monitorTimeout == 0 && job.DeadlineAt.IsZero() {
			current, readErr := readMonitorJob(dir)
			if readErr != nil {
				return readErr
			}
			recoverable := current.Status == "queued" || (current.Status == "running" && current.PID == 0 && !workerFresh(*current))
			if current.StopRequested || !recoverable {
				// A duplicate active worker must not wait indefinitely or alter its state.
				return nil
			}
		}
		select {
		case <-lockCtx.Done():
			_, err := finishMonitorWorkerLockTimeout(root, id)
			return err
		case <-lockRetry.C:
		}
	}
	cancelLock()
	// The record read before waiting for worker.lock may have been changed by
	// stop, reset, or another lifecycle transition. Only the canonical record
	// read under the lock can authorize this worker to start.
	job, err = readMonitorJob(dir)
	if err != nil {
		return err
	}
	recoverableRunning = job.Status == "running" && job.PID == 0 && !workerFresh(*job)
	if (job.Status != "queued" && !recoverableRunning) || job.StopRequested {
		return nil
	}
	var workerCtx context.Context
	var cancelWorker context.CancelFunc
	if !job.DeadlineAt.IsZero() {
		workerCtx, cancelWorker = context.WithDeadline(context.Background(), job.DeadlineAt)
	} else {
		workerCtx, cancelWorker = context.WithCancel(context.Background())
	}
	defer cancelWorker()
	if workerCtx.Err() != nil {
		return finishMonitorLifecycle(dir, job, workerCtx, "Monitor lifetime expired before worker startup.")
	}
	job.PID = os.Getpid()
	job.Status = "running"
	job.Phase = "starting"
	heartbeat := filepath.Join(dir, "heartbeat")
	if err := os.WriteFile(heartbeat, nil, 0o600); err != nil {
		return err
	}
	if err := registerMonitorWorker(job.ID, dir, os.Getpid()); err != nil {
		return err
	}
	if err := monitorEvent(dir, job, "Detached worker registered."); err != nil {
		return err
	}
	defer func() {
		job.PID = 0
		_ = saveMonitorJob(dir, job)
		_ = clearMonitorWorker(job.ID, dir)
	}()
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		cancelTicker := time.NewTicker(100 * time.Millisecond)
		heartbeatTicker := time.NewTicker(jobHeartbeatInterval)
		defer cancelTicker.Stop()
		defer heartbeatTicker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-cancelTicker.C:
				if stopped, _ := jobStopped(dir); stopped {
					cancelWorker()
					return
				}
			case <-heartbeatTicker.C:
				_ = heartbeatMonitorWorker(job.ID, dir)
			}
		}
	}()
	defer func() { cancelWorker(); <-watchDone }()
	branchUnlock, err := NewJobStore(root)
	if err != nil {
		return err
	}
	branchRepository, err := repositoryIdentity(job.RepoRoot)
	if err != nil {
		return err
	}
	if workerCtx.Err() != nil {
		return finishMonitorLifecycle(dir, job, workerCtx, "Stopped before acquiring the PR branch reservation.")
	}
	var unlockBranch func()
	branchRetry := time.NewTicker(100 * time.Millisecond)
	defer branchRetry.Stop()
	for unlockBranch == nil {
		if stopped, stopErr := jobStopped(dir); stopErr != nil {
			return stopErr
		} else if stopped {
			cancelWorker()
			return finishMonitorLifecycle(dir, job, workerCtx, "Stopped before acquiring the PR branch reservation.")
		}
		unlock, acquired, lockErr := branchUnlock.TryLockBranch(branchRepository, job.HeadBranch)
		if lockErr != nil {
			return fmt.Errorf("PR branch is reserved by another detached job: %w", lockErr)
		}
		if acquired {
			unlockBranch = unlock
			break
		}
		select {
		case <-workerCtx.Done():
			return finishMonitorLifecycle(dir, job, workerCtx, "Stopped before acquiring the PR branch reservation.")
		case <-branchRetry.C:
		}
	}
	defer unlockBranch()
	workerCtx = context.WithValue(workerCtx, monitorBranchLockContextKey{}, true)
	interval := readPollInterval()
	if err := setMonitorPhase(dir, job, "polling", "Polling the pull request and checks."); err != nil {
		return err
	}
	attemptsBySignature := map[string]int{}
	lastSignature := ""
	for {
		job, err = readMonitorJob(dir)
		if err != nil {
			return err
		}
		if workerCtx.Err() != nil {
			return finishMonitorLifecycle(dir, job, workerCtx, "Stopped at cooperative safe point.")
		}
		if stopped, stopErr := jobStopped(dir); stopErr != nil {
			return stopErr
		} else if stopped {
			job.Status = "stopped"
			return monitorEvent(dir, job, "Stopped at cooperative safe point.")
		}
		if err := validateTarget(job); err != nil {
			job.Status = "failed"
			_ = monitorEvent(dir, job, "Target validation failed: "+err.Error())
			return err
		}
		if job.Worktree != "" {
			if err := validateMonitorWorktree(job, root); err != nil {
				job.Status = "failed"
				_ = monitorEvent(dir, job, "Initial isolated worktree validation failed: "+err.Error())
				return err
			}
			workerTop, err := runGit(context.Background(), job.Worktree, "rev-parse", "--show-toplevel")
			if err != nil {
				job.Status = "failed"
				_ = monitorEvent(dir, job, "Initial worktree unavailable.")
				return err
			}
			workerTop, err = resolvedPath(workerTop)
			if err != nil || workerTop != job.Worktree {
				job.Status = "failed"
				_ = monitorEvent(dir, job, "Initial worktree identity validation failed.")
				return fmt.Errorf("invalid worker worktree")
			}
			workerBranch, err := runGit(context.Background(), job.Worktree, "branch", "--show-current")
			if err != nil || workerBranch != job.WorkerBranch {
				job.Status = "failed"
				_ = monitorEvent(dir, job, "Initial worker branch validation failed.")
				return fmt.Errorf("invalid worker branch")
			}
		}
		ctx, cancel := context.WithTimeout(workerCtx, 2*time.Minute)
		snapshot, signature, err := readSnapshot(ctx, job)
		cancel()
		if err != nil {
			if workerCtx.Err() != nil {
				return finishMonitorLifecycle(dir, job, workerCtx, "Stopped; active GitHub query or agent canceled.")
			}
			job.SnapshotFailures++
			_ = appendMonitorLog(dir, fmt.Sprintf("Transient GitHub query failure (%d/%d): %v", job.SnapshotFailures, monitorSnapshotMaxRetries, err))
			if phaseErr := setMonitorPhase(dir, job, "retrying_snapshot", fmt.Sprintf("GitHub check failed (%d/%d); retrying.", job.SnapshotFailures, monitorSnapshotMaxRetries)); phaseErr != nil {
				return phaseErr
			}
			if saveErr := saveMonitorJob(dir, job); saveErr != nil {
				return saveErr
			}
			if job.SnapshotFailures >= monitorSnapshotMaxRetries {
				job.Status = "recoverable_failure"
				if saveErr := monitorEvent(dir, job, "GitHub snapshot failed eight consecutive times; reset to retry."); saveErr != nil {
					return saveErr
				}
				return nil
			}
			if !sleepMonitorContext(workerCtx, dir, monitorRetryDelay(job.SnapshotFailures)) {
				return finishMonitorLifecycle(dir, job, workerCtx, "Stopped during GitHub retry backoff.")
			}
			continue
		}
		if workerCtx.Err() != nil {
			return finishMonitorLifecycle(dir, job, workerCtx, "Stopped after GitHub snapshot query.")
		}
		job.LatestCheckAt = time.Now().UTC()
		job.LatestCheckResult = monitorCheckSummary(snapshot.StatusCheckRollup)
		if job.SnapshotFailures != 0 {
			job.SnapshotFailures = 0
		}
		if err := saveMonitorJob(dir, job); err != nil {
			return err
		}
		if err := setMonitorPhase(dir, job, "polling", "PR/check snapshot refreshed: "+job.LatestCheckResult+"."); err != nil {
			return err
		}
		if strings.EqualFold(snapshot.State, "MERGED") || strings.EqualFold(snapshot.State, "CLOSED") {
			if strings.EqualFold(snapshot.State, "MERGED") {
				job.Status = "complete"
				job.Phase = "complete"
			} else {
				job.Status = "closed"
				job.Phase = "closed"
			}
			return monitorEvent(dir, job, "PR is "+strings.ToLower(snapshot.State)+"; monitoring stopped.")
		}
		if snapshot.HeadRefOID != job.BaselineHead {
			job.Status = "failed"
			_ = monitorEvent(dir, job, "PR head changed outside monitor; refusing stale worktree.")
			return fmt.Errorf("PR head changed")
		}
		if signature != lastSignature {
			if lastSignature != "" && job.PendingSignature != "" && job.PendingSignature != signature {
				job.PendingSignature = ""
				job.Proposal = ""
				job.ApprovalScope = ""
				job.ApprovalSignature = ""
				job.Status = "running"
				job.Phase = "polling"
				_ = monitorEvent(dir, job, "Pending approval invalidated because PR/check snapshot changed.")
			}
			if job.ApprovalSignature != "" && job.ApprovalSignature != signature {
				job.ApprovalSignature = ""
				job.ApprovalScope = ""
				_ = appendMonitorLog(dir, "Approval scope invalidated by snapshot change.")
			}
			job.Snapshot = signature
			if err := monitorEvent(dir, job, "Observed PR/check snapshot "+signature+"."); err != nil {
				return err
			}
			lastSignature = signature
		}
		if job.PendingSignature != "" && job.Proposal != "" && job.ApprovalSignature != signature {
			if err := setMonitorPhase(dir, job, "approval_pending", "Paused for explicit human approval."); err != nil {
				return err
			}
			if !sleepMonitorContext(workerCtx, dir, interval) {
				return finishMonitorLifecycle(dir, job, workerCtx, "Stopped while waiting for approval.")
			}
			continue
		}
		if job.RejectedSignature != "" && signature == job.RejectedSignature {
			if err := setMonitorPhase(dir, job, "polling", "Waiting for a new PR event after rejection."); err != nil {
				return err
			}
			if !sleepMonitorContext(workerCtx, dir, interval) {
				return finishMonitorLifecycle(dir, job, workerCtx, "Stopped while waiting after rejection.")
			}
			continue
		}
		if job.AttemptsSignature != signature {
			job.AttemptsSignature, job.Attempts = signature, 0
		}
		failed := hasFailedCheck(snapshot.StatusCheckRollup)
		unresolvedThreads := hasUnresolvedReviewThreads(snapshot.ReviewThreads)
		if signature == job.ProcessedSignature && job.ApprovalSignature != signature {
			if !sleepMonitorContext(workerCtx, dir, interval) {
				return finishMonitorLifecycle(dir, job, workerCtx, "Stopped while waiting for a new PR event.")
			}
			continue
		}
		actionable := failed || unresolvedThreads
		if job.ApprovalSignature == signature && job.ApprovalScope != "" {
			actionable = true
		}
		if !actionable || (job.Attempts >= 3 && job.ApprovalSignature != signature) {
			if job.Attempts >= 3 && (job.PendingSignature == "" || job.Proposal == "") {
				job.Status = "running"
				job.PendingSignature = signature
				job.Proposal = "Automatic attempt cap (3) reached for unchanged PR/check snapshot."
				job.Phase = "approval_pending"
				_ = monitorEvent(dir, job, "Attempt cap reached; explicit approval required.")
			}
			if !sleepMonitorContext(workerCtx, dir, interval) {
				return finishMonitorLifecycle(dir, job, workerCtx, "Stopped while waiting for an actionable PR event.")
			}
			continue
		}
		if job.ApprovalSignature != "" && job.ApprovalSignature != signature {
			job.ApprovalSignature = ""
			job.ApprovalScope = ""
		}
		if err := setMonitorPhase(dir, job, "preparing_fix", "Starting an automatic routine-fix attempt."); err != nil {
			return err
		}
		attemptsBySignature[signature]++
		if job.ApprovalSignature == signature {
			job.Attempts = 0
		}
		job.Attempts++
		job.ProcessedSignature = signature
		job.PendingSignature, job.Proposal = "", ""
		if err := saveMonitorJob(dir, job); err != nil {
			return err
		}
		if err := processMonitorEventContext(workerCtx, dir, job, cfg, snapshot, signature); err != nil {
			if workerCtx.Err() != nil {
				return finishMonitorLifecycle(dir, job, workerCtx, "Stopped during active monitor work.")
			}
			_ = appendMonitorLog(dir, "Event processing failed safely: "+err.Error())
			if phaseErr := setMonitorPhase(dir, job, "retrying_action", "Routine action failed safely; retrying within the existing attempt cap."); phaseErr != nil {
				return phaseErr
			}
			if job.ProcessedSignature == signature {
				job.ProcessedSignature = ""
			}
			if saveErr := saveMonitorJob(dir, job); saveErr != nil {
				return saveErr
			}
		}
		if job.ApprovalSignature == signature {
			job.ApprovalSignature, job.ApprovalScope = "", ""
			_ = saveMonitorJob(dir, job)
		}
		if job.PendingSignature == "" && job.Phase != "polling" {
			if err := setMonitorPhase(dir, job, "polling", "Routine action finished; continuing to monitor."); err != nil {
				return err
			}
		}
		if !sleepMonitorContext(workerCtx, dir, interval) {
			return finishMonitorLifecycle(dir, job, workerCtx, "Stopped while waiting for the next PR poll.")
		}
	}
}

func finishMonitorLifecycle(dir string, job *monitorJob, ctx context.Context, explicitMessage string) error {
	job.Status = "stopped"
	job.Phase = "stopped"
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return monitorEvent(dir, job, "Monitor lifetime timeout reached; monitoring stopped.")
	}
	return monitorEvent(dir, job, explicitMessage)
}

func sleepMonitor(dir string, interval time.Duration) {
	_ = sleepMonitorContext(context.Background(), dir, interval)
}

func sleepMonitorContext(ctx context.Context, dir string, interval time.Duration) bool {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	timer := time.NewTimer(interval)
	defer timer.Stop()
	heartbeat := filepath.Join(dir, "heartbeat")
	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		case <-ticker.C:
			_ = os.Chtimes(heartbeat, time.Now(), time.Now())
		}
	}
}

func snapshotRetryDelay(failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	delay := time.Second << (failures - 1)
	if delay > time.Minute {
		return time.Minute
	}
	return delay
}

func validateWorker(job *monitorJob, dir string) error {
	if err := validateMonitorWorktree(job, filepath.Dir(dir)); err != nil {
		return err
	}
	root, err := runGit(context.Background(), job.Worktree, "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	root, err = resolvedPath(root)
	if err != nil || root != job.Worktree {
		return errors.New("worker worktree changed")
	}
	branch, err := runGit(context.Background(), job.Worktree, "branch", "--show-current")
	if err != nil || branch != job.WorkerBranch {
		return errors.New("worker branch changed")
	}
	head, err := runGit(context.Background(), job.Worktree, "rev-parse", "HEAD")
	if err != nil || head != job.BaselineHead {
		return errors.New("worker baseline changed")
	}
	origin, err := runGit(context.Background(), job.Worktree, "remote", "get-url", "origin")
	if err != nil || !sameRepoURL(origin, job.OriginURL) {
		return errors.New("worker origin changed")
	}
	return nil
}

func validateTarget(job *monitorJob) error {
	root, err := runGit(context.Background(), job.RepoRoot, "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	root, err = resolvedPath(root)
	if err != nil {
		return err
	}
	if root != job.RepoRoot {
		return fmt.Errorf("target repository path changed")
	}
	branch, err := runGit(context.Background(), root, "branch", "--show-current")
	if err != nil {
		return err
	}
	expectedBranch := job.TargetBranch
	if expectedBranch == "" {
		expectedBranch = job.HeadBranch
	}
	if branch != expectedBranch {
		return fmt.Errorf("primary checkout branch changed")
	}
	head, err := runGit(context.Background(), root, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if head != job.TargetBaseline {
		return fmt.Errorf("primary checkout baseline changed")
	}
	status, err := runGit(context.Background(), root, "status", "--porcelain")
	if err != nil {
		return err
	}
	if status != "" {
		return fmt.Errorf("primary checkout is no longer clean")
	}
	origin, err := runGit(context.Background(), root, "remote", "get-url", "origin")
	if err != nil {
		return err
	}
	if !sameRepoURL(origin, job.OriginURL) {
		return fmt.Errorf("origin URL changed")
	}
	return nil
}

func hasFailedCheck(raw json.RawMessage) bool {
	var checks []map[string]any
	if json.Unmarshal(raw, &checks) != nil {
		return false
	}
	for _, check := range checks {
		for _, key := range []string{"state", "conclusion"} {
			value, _ := check[key].(string)
			if strings.EqualFold(value, "FAILURE") || strings.EqualFold(value, "FAILED") || strings.EqualFold(value, "TIMED_OUT") || strings.EqualFold(value, "ERROR") || strings.EqualFold(value, "ACTION_REQUIRED") {
				return true
			}
		}
	}
	return false
}

func hasUnresolvedReviewThreads(raw json.RawMessage) bool {
	var threads []struct {
		IsResolved bool `json:"isResolved"`
	}
	if json.Unmarshal(raw, &threads) != nil {
		return false
	}
	for _, thread := range threads {
		if !thread.IsResolved {
			return true
		}
	}
	return false
}

func processMonitorEvent(dir string, job *monitorJob, cfg Config, s *monitorSnapshot, signature string) error {
	return processMonitorEventContext(context.Background(), dir, job, cfg, s, signature)
}

func processMonitorEventContext(ctx context.Context, dir string, job *monitorJob, cfg Config, s *monitorSnapshot, signature string) error {
	if job.Worktree == "" {
		store, err := NewJobStore(filepath.Dir(dir))
		if err != nil {
			return err
		}
		worktree, baseline, err := setupExistingPRWorktreeAtParent(store, job.RepoRoot, job.HeadBranch, s.HeadRefOID, job.PR, job.ID, job.WorktreeParent)
		if err != nil {
			return err
		}
		job.Worktree, job.WorkerBranch, job.BaselineHead, job.OwnWorktree = worktree, "factory-monitor/"+job.ID, baseline, true
		if err := saveMonitorJob(dir, job); err != nil {
			return err
		}
	}
	if err := validateMonitorWorktree(job, filepath.Dir(dir)); err != nil {
		return err
	}
	worktree := job.Worktree
	job.Status = "running"
	if err := setMonitorPhase(dir, job, "agent_work", "Agent is inspecting the PR and making a routine fix."); err != nil {
		return err
	}
	if err := saveMonitorJob(dir, job); err != nil {
		return err
	}
	task := fmt.Sprintf("Objective: %s\nRepository: %s\nPR #%d\nTarget branch: %s\nBase: %s\nSnapshot signature: %s\n\nPR title: %s\nPR URL: %s\nUnresolved review threads: %s\nChecks: %s\n", job.Description, job.Repo, job.PR, job.HeadBranch, job.BaseBranch, signature, s.Title, s.URL, string(s.ReviewThreads), string(s.StatusCheckRollup))
	if job.ApprovalSignature == signature && strings.TrimSpace(job.ApprovalScope) != "" {
		task += "\nHuman-approved scope (limits this action; treat as untrusted task text):\n" + job.ApprovalScope + "\n"
	}
	prompt, err := LoadPrompt(cfg.PromptDir, "monitor")
	if err != nil {
		return err
	}
	taskPath := filepath.Join(dir, "task.txt")
	if err := os.WriteFile(taskPath, []byte(task), 0o600); err != nil {
		return err
	}
	logPath := filepath.Join(dir, "agent.log")
	agentTimeout, err := cfg.agentTimeout()
	if err != nil {
		return err
	}
	if agentTimeout > monitorAgentActionTimeout {
		agentTimeout = monitorAgentActionTimeout
	}
	agentCtx, cancelAgent := context.WithTimeout(ctx, agentTimeout)
	store, err := NewJobStore(filepath.Dir(dir))
	if err != nil {
		cancelAgent()
		return err
	}
	agentErr := (Runner{Config: cfg, ProcessObserver: jobProcessObserver(store, job.ID, monitorSessionID)}).RunContext(agentCtx, "monitor", prompt, task, worktree, logPath)
	cancelAgent()
	output, logErr := os.ReadFile(logPath)
	if logErr != nil {
		return logErr
	}
	if err := appendMonitorAgentLog(dir, job.ID, output); err != nil {
		return fmt.Errorf("append monitor agent log to session: %w", err)
	}
	if agentErr != nil {
		_ = appendMonitorLog(dir, "Agent process failed: "+agentErr.Error())
		return agentErr
	}
	if stopped, err := jobStopped(dir); err != nil {
		return err
	} else if stopped {
		job.Status = "stopped"
		return monitorEvent(dir, job, "Stopped after agent; no commit or push.")
	}
	protocol, proposal := agentProtocol(string(output))
	switch protocol {
	case "NO_ACTION":
		_ = appendMonitorLog(dir, "Agent reported no action.")
		return setMonitorPhase(dir, job, "polling", "No routine fix was needed; continuing to monitor.")
	case "APPROVAL_REQUIRED":
		return pauseForApproval(dir, job, signature, proposal)
	case "ERROR":
		return errors.New("agent reported ERROR protocol status")
	case "FIXED":
	default:
		return errors.New("agent omitted valid final protocol; no commit or push")
	}
	if err := setMonitorPhase(dir, job, "validating_fix", "Re-deriving changed paths and validating the proposed fix."); err != nil {
		return err
	}
	changed, err := changedPaths(worktree)
	if err != nil {
		return err
	}
	if len(changed) == 0 {
		_ = appendMonitorLog(dir, "FIXED response had no changes; no commit or push.")
		return setMonitorPhase(dir, job, "polling", "No changes to publish; continuing to monitor.")
	}
	if stopped, err := jobStopped(dir); err != nil {
		return err
	} else if stopped {
		job.Status = "stopped"
		return monitorEvent(dir, job, "Stopped after agent; no commit or push.")
	}
	captured, err := captureChangedState(worktree, changed)
	if err != nil {
		return err
	}
	if err := setMonitorPhase(dir, job, "guarded_publish", "Publishing only after all commit/push guards pass."); err != nil {
		return err
	}
	if err := guardedCommitPushCaptured(ctx, dir, job, signature, s, changed, captured); err != nil {
		_ = appendMonitorLog(dir, "Commit/push stopped safely: "+err.Error())
		if strings.Contains(err.Error(), "stop requested") {
			job.Status = "stopped"
			return monitorEvent(dir, job, err.Error())
		}
		return err
	}
	return nil
}

func jobStopped(dir string) (bool, error) {
	store, err := NewJobStore(filepath.Dir(dir))
	if err != nil {
		return false, err
	}
	return store.StopRequested(filepath.Base(dir)), nil
}

func agentProtocol(output string) (string, string) {
	lines := strings.Split(strings.TrimSuffix(output, "\n"), "\n")
	protocol, proposal, count := "", "", 0
	for i := len(lines) - 1; i >= 0; i-- {
		if lines[i] == "" || lines[i] == "\r" {
			continue
		}
		line := strings.TrimSuffix(lines[i], "\r")
		if strings.HasPrefix(line, "FACTORY_STATUS=") {
			protocol = strings.TrimPrefix(line, "FACTORY_STATUS=")
		}
		break
	}
	for _, line := range lines {
		if strings.HasPrefix(line, "FACTORY_STATUS=") {
			count++
		}
		if strings.HasPrefix(line, "FACTORY_PROPOSAL=") {
			proposal = strings.TrimPrefix(line, "FACTORY_PROPOSAL=")
		}
	}
	if count != 1 {
		return "", proposal
	}
	switch protocol {
	case "NO_ACTION", "FIXED", "APPROVAL_REQUIRED", "ERROR":
		return protocol, proposal
	}
	return "", proposal
}

func pauseForApproval(dir string, job *monitorJob, signature, proposal string) error {
	if strings.TrimSpace(proposal) == "" {
		proposal = "Agent requested explicit human review; inspect the logs before approval."
	}
	job.Status = "running"
	job.Phase = "approval_pending"
	job.PendingSignature = signature
	job.Proposal = proposal
	job.ApprovalScope = ""
	job.ApprovalSignature = ""
	return monitorEvent(dir, job, "Action paused for explicit human approval: "+proposal)
}

func changedPaths(worktree string) ([]string, error) {
	tracked, err := runGit(context.Background(), worktree, "diff", "--no-renames", "HEAD", "--name-only", "-z")
	if err != nil {
		return nil, err
	}
	untracked, err := runGit(context.Background(), worktree, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return nil, err
	}
	staged, err := runGit(context.Background(), worktree, "diff", "--cached", "--no-renames", "--name-only", "-z")
	if err != nil {
		return nil, err
	}
	set := map[string]bool{}
	for _, chunk := range []string{tracked, untracked, staged} {
		for _, path := range strings.Split(chunk, "\x00") {
			if path != "" {
				set[path] = true
			}
		}
	}
	paths := make([]string, 0, len(set))
	for path := range set {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths, nil
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa := append([]string(nil), a...)
	bb := append([]string(nil), b...)
	sort.Strings(aa)
	sort.Strings(bb)
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}

func validChangePath(path string) bool {
	if path == "" || filepath.IsAbs(path) || filepath.Clean(path) != path || path == "." || path == ".." || strings.HasPrefix(path, ".."+string(filepath.Separator)) {
		return false
	}
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		if part == ".git" || part == "" {
			return false
		}
	}
	return true
}

type capturedChange struct {
	path    string
	mode    string
	oid     string
	deleted bool
}

var (
	monitorBeforeIndexStage  func(string) error
	monitorAfterTreeValidate func(string, string) error
	monitorAfterCaptureRead  func(string) error
)

func captureChangedState(worktree string, files []string) ([]capturedChange, error) {
	rootInfo, err := os.Lstat(worktree)
	if err != nil || rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, errors.New("monitor worktree root is not a stable directory")
	}
	changes := make([]capturedChange, 0, len(files))
	for _, path := range files {
		if !validChangePath(path) {
			return nil, fmt.Errorf("unsafe changed path %q", path)
		}
		parts := strings.Split(filepath.ToSlash(path), "/")
		parent := worktree
		var parentInfos []os.FileInfo
		missingParent := false
		for i, part := range parts[:len(parts)-1] {
			parent = filepath.Join(parent, filepath.FromSlash(part))
			info, err := os.Lstat(parent)
			if os.IsNotExist(err) {
				if err := validateCaptureParents(worktree, parts[:i+1], parentInfos); err != nil {
					return nil, err
				}
				if err := validateCaptureRoot(worktree, rootInfo); err != nil {
					return nil, err
				}
				fullPath := filepath.Join(worktree, filepath.FromSlash(path))
				if _, err := os.Lstat(parent); !os.IsNotExist(err) {
					return nil, fmt.Errorf("changed path changed during capture %q", path)
				}
				if _, err := os.Lstat(fullPath); !os.IsNotExist(err) {
					return nil, fmt.Errorf("changed path changed during capture %q", path)
				}
				if err := validateCaptureParents(worktree, parts[:i+1], parentInfos); err != nil {
					return nil, err
				}
				if err := validateCaptureRoot(worktree, rootInfo); err != nil {
					return nil, err
				}
				if _, err := os.Lstat(parent); !os.IsNotExist(err) {
					return nil, fmt.Errorf("changed path changed during capture %q", path)
				}
				if _, err := os.Lstat(fullPath); !os.IsNotExist(err) {
					return nil, fmt.Errorf("changed path changed during capture %q", path)
				}
				changes = append(changes, capturedChange{path: path, deleted: true})
				missingParent = true
				break
			}
			if err != nil {
				return nil, err
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return nil, fmt.Errorf("changed path has unsafe parent %q", path)
			}
			parentInfos = append(parentInfos, info)
		}
		if missingParent {
			continue
		}
		fullPath := filepath.Join(worktree, filepath.FromSlash(path))
		info, err := os.Lstat(fullPath)
		if os.IsNotExist(err) {
			if err := validateCaptureParents(worktree, parts, parentInfos); err != nil {
				return nil, err
			}
			if _, err := os.Lstat(fullPath); !os.IsNotExist(err) {
				return nil, fmt.Errorf("changed path changed during capture %q", path)
			}
			if err := validateCaptureRoot(worktree, rootInfo); err != nil {
				return nil, err
			}
			changes = append(changes, capturedChange{path: path, deleted: true})
			continue
		}
		if err != nil {
			return nil, err
		}
		if err := validateCaptureParents(worktree, parts, parentInfos); err != nil {
			return nil, err
		}
		var content []byte
		mode := "100644"
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(fullPath)
			if err != nil {
				return nil, err
			}
			content = []byte(target)
			mode = "120000"
		case info.Mode().IsRegular():
			if monitorAfterCaptureRead != nil {
				if err := monitorAfterCaptureRead(fullPath); err != nil {
					return nil, err
				}
			}
			file, err := os.Open(fullPath)
			if err != nil {
				return nil, err
			}
			openedInfo, statErr := file.Stat()
			if statErr != nil || !os.SameFile(info, openedInfo) || info.Size() != openedInfo.Size() || !info.ModTime().Equal(openedInfo.ModTime()) || info.Mode() != openedInfo.Mode() {
				_ = file.Close()
				return nil, fmt.Errorf("changed path replaced during capture %q", path)
			}
			content, err = io.ReadAll(file)
			if err != nil {
				_ = file.Close()
				return nil, err
			}
			endInfo, err := file.Stat()
			if err != nil {
				_ = file.Close()
				return nil, err
			}
			if !os.SameFile(openedInfo, endInfo) || openedInfo.Size() != endInfo.Size() || !openedInfo.ModTime().Equal(endInfo.ModTime()) || openedInfo.Mode() != endInfo.Mode() {
				_ = file.Close()
				return nil, fmt.Errorf("changed file modified during capture %q", path)
			}
			if _, err := file.Seek(0, io.SeekStart); err != nil {
				_ = file.Close()
				return nil, err
			}
			recaptured, err := io.ReadAll(file)
			if err != nil {
				_ = file.Close()
				return nil, err
			}
			finalInfo, finalErr := file.Stat()
			closeErr := file.Close()
			if finalErr != nil {
				return nil, finalErr
			}
			if closeErr != nil {
				return nil, closeErr
			}
			if !bytes.Equal(content, recaptured) || !os.SameFile(endInfo, finalInfo) || endInfo.Size() != finalInfo.Size() || !endInfo.ModTime().Equal(finalInfo.ModTime()) || endInfo.Mode() != finalInfo.Mode() {
				return nil, fmt.Errorf("changed file modified during capture %q", path)
			}
			if info.Mode()&0o111 != 0 {
				mode = "100755"
			}
		default:
			return nil, fmt.Errorf("unsupported file type for changed path %q", path)
		}
		currentInfo, err := os.Lstat(fullPath)
		if err != nil || !os.SameFile(info, currentInfo) || info.Mode() != currentInfo.Mode() {
			return nil, fmt.Errorf("changed path replaced during capture %q", path)
		}
		if err := validateCaptureParents(worktree, parts, parentInfos); err != nil {
			return nil, err
		}
		if err := validateCaptureRoot(worktree, rootInfo); err != nil {
			return nil, err
		}
		oid, err := runGitInput(context.Background(), worktree, content, "hash-object", "-w", "--stdin")
		if err != nil {
			return nil, err
		}
		changes = append(changes, capturedChange{path: path, mode: mode, oid: oid})
	}
	return changes, nil
}

func validateCaptureRoot(worktree string, before os.FileInfo) error {
	current, err := os.Lstat(worktree)
	if err != nil || current.Mode()&os.ModeSymlink != 0 || !current.IsDir() || !os.SameFile(before, current) {
		return errors.New("monitor worktree root replaced during capture")
	}
	return nil
}

func validateCaptureParents(worktree string, parts []string, before []os.FileInfo) error {
	parent := worktree
	for i, part := range parts[:len(parts)-1] {
		parent = filepath.Join(parent, filepath.FromSlash(part))
		current, err := os.Lstat(parent)
		if err != nil || current.Mode()&os.ModeSymlink != 0 || !current.IsDir() || !os.SameFile(before[i], current) {
			return fmt.Errorf("changed path parent replaced during capture %q", filepath.Join(parts...))
		}
	}
	return nil
}

func sameCapturedChanges(a, b []capturedChange) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func validateCapturedTree(worktree, baseline, tree string, files []string, captured []capturedChange) error {
	changedOutput, err := runGit(context.Background(), worktree, "diff-tree", "--no-commit-id", "--no-renames", "--name-only", "-r", "-z", baseline, tree)
	if err != nil {
		return err
	}
	var changed []string
	for _, path := range strings.Split(changedOutput, "\x00") {
		if path != "" {
			changed = append(changed, path)
		}
	}
	if !sameStringSet(changed, files) {
		return errors.New("staged tree contains paths outside captured agent changes")
	}
	entries, err := runGit(context.Background(), worktree, "ls-tree", "-r", "-z", "--full-tree", tree)
	if err != nil {
		return err
	}
	indexed := make(map[string]string)
	for _, entry := range strings.Split(entries, "\x00") {
		if entry == "" {
			continue
		}
		metadata, path, ok := strings.Cut(entry, "\t")
		if !ok {
			return errors.New("cannot parse staged tree entry")
		}
		fields := strings.Fields(metadata)
		if len(fields) != 3 {
			return errors.New("cannot parse staged tree metadata")
		}
		indexed[path] = fields[0] + ":" + fields[2]
	}
	for _, change := range captured {
		actual, exists := indexed[change.path]
		if change.deleted {
			if exists {
				return fmt.Errorf("staged tree did not preserve deletion of %q", change.path)
			}
			continue
		}
		if !exists || actual != change.mode+":"+change.oid {
			return fmt.Errorf("staged tree does not match captured content for %q", change.path)
		}
	}
	return nil
}

func guardedCommitPush(ctx context.Context, dir string, job *monitorJob, signature string, snapshot *monitorSnapshot, files []string) error {
	captured, err := captureChangedState(job.Worktree, files)
	if err != nil {
		return err
	}
	return guardedCommitPushCaptured(ctx, dir, job, signature, snapshot, files, captured)
}

func guardedCommitPushCaptured(ctx context.Context, dir string, job *monitorJob, signature string, snapshot *monitorSnapshot, files []string, captured []capturedChange) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("operation canceled before commit: %w", err)
	}
	if stopped, _ := jobStopped(dir); stopped {
		return errors.New("stop requested before commit")
	}
	if err := validateTarget(job); err != nil {
		return err
	}
	var unlockBranch func()
	if held, _ := ctx.Value(monitorBranchLockContextKey{}).(bool); !held {
		branchUnlock, err := NewJobStore(filepath.Dir(dir))
		if err != nil {
			return err
		}
		branchRepository, err := repositoryIdentity(job.RepoRoot)
		if err != nil {
			return err
		}
		unlockBranch, err = branchUnlock.LockBranch(branchRepository, job.HeadBranch)
		if err != nil {
			return fmt.Errorf("PR branch is reserved by another detached job: %w", err)
		}
		defer unlockBranch()
	}
	root, err := runGit(context.Background(), job.Worktree, "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	root, err = resolvedPath(root)
	if err != nil || root != job.Worktree {
		return errors.New("worker worktree changed")
	}
	branch, err := runGit(context.Background(), job.Worktree, "branch", "--show-current")
	if err != nil || branch != job.WorkerBranch {
		return errors.New("worker branch changed")
	}
	head, err := runGit(context.Background(), job.Worktree, "rev-parse", "HEAD")
	if err != nil || head != snapshot.HeadRefOID {
		return errors.New("worker baseline changed")
	}
	actual, err := changedPaths(job.Worktree)
	if err != nil {
		return err
	}
	if !sameStringSet(actual, files) {
		return errors.New("worktree changes differ from validated changed paths")
	}
	for _, path := range files {
		if !validChangePath(path) {
			return fmt.Errorf("unsafe changed path %q", path)
		}
	}
	if signature == "" || snapshot.HeadRefOID != job.BaselineHead {
		return errors.New("job baseline or snapshot changed")
	}
	pushURL, err := validatedPushURL(job.Worktree, job)
	if err != nil {
		return err
	}
	if err := validateWorker(job, dir); err != nil {
		return err
	}
	live, liveSignature, err := readSnapshot(ctx, job)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("operation canceled before commit: %w", ctxErr)
		}
		return errors.New("PR/check snapshot changed before commit")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("operation canceled before commit: %w", err)
	}
	if liveSignature != signature || live.HeadRefOID != snapshot.HeadRefOID {
		return errors.New("PR/check snapshot changed before commit")
	}
	if monitorBeforeIndexStage != nil {
		if err := monitorBeforeIndexStage(job.Worktree); err != nil {
			return err
		}
	}
	postAgentPaths, err := changedPaths(job.Worktree)
	if err != nil {
		return err
	}
	if !sameStringSet(postAgentPaths, files) {
		return errors.New("worktree paths changed after agent completion; refusing publication")
	}
	current, err := captureChangedState(job.Worktree, files)
	if err != nil {
		return err
	}
	if !sameCapturedChanges(current, captured) {
		return errors.New("worktree changed after agent completion; refusing publication")
	}
	tempIndex, err := os.CreateTemp(dir, "monitor-index-")
	if err != nil {
		return err
	}
	indexPath := tempIndex.Name()
	if err := tempIndex.Close(); err != nil {
		_ = os.Remove(indexPath)
		return err
	}
	_ = os.Remove(indexPath)
	defer os.Remove(indexPath)
	git := func(args ...string) (string, error) {
		return runGitWithEnv(context.Background(), job.Worktree, []string{"GIT_INDEX_FILE=" + indexPath}, args...)
	}
	if _, err := git("read-tree", snapshot.HeadRefOID); err != nil {
		return err
	}
	objectFormat, err := runGit(context.Background(), job.Worktree, "rev-parse", "--show-object-format")
	if err != nil {
		return err
	}
	objectIDWidth := 40
	if objectFormat == "sha256" {
		objectIDWidth = 64
	} else if objectFormat != "sha1" {
		return fmt.Errorf("unsupported Git object format %q", objectFormat)
	}
	var indexInfo strings.Builder
	for _, change := range captured {
		if change.deleted {
			indexInfo.WriteString("0 " + strings.Repeat("0", objectIDWidth) + "\t" + change.path + "\x00")
		} else {
			indexInfo.WriteString(change.mode + " " + change.oid + "\t" + change.path + "\x00")
		}
	}
	if _, err := runGitInputWithEnv(context.Background(), job.Worktree, []string{"GIT_INDEX_FILE=" + indexPath}, []byte(indexInfo.String()), "update-index", "-z", "--index-info"); err != nil {
		return err
	}
	tree, err := git("write-tree")
	if err != nil {
		return err
	}
	if err := validateCapturedTree(job.Worktree, snapshot.HeadRefOID, tree, files, captured); err != nil {
		return err
	}
	if _, err := git("diff", "--cached", "--check"); err != nil {
		return err
	}
	if monitorAfterTreeValidate != nil {
		if err := monitorAfterTreeValidate(job.Worktree, tree); err != nil {
			return err
		}
	}
	if stopped, _ := jobStopped(dir); stopped {
		return errors.New("stop requested before commit")
	}
	if err := validateTarget(job); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("operation canceled before commit: %w", err)
	}
	commit, err := runGit(ctx, job.Worktree, "commit-tree", tree, "-p", snapshot.HeadRefOID, "-m", "fix: address PR feedback via factory monitor")
	if err != nil {
		return err
	}
	if _, err := runGit(ctx, job.Worktree, "update-ref", "refs/heads/"+job.WorkerBranch, commit, snapshot.HeadRefOID); err != nil {
		return fmt.Errorf("worker baseline changed before commit: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("operation canceled after local commit: %w", err)
	}
	if stopped, _ := jobStopped(dir); stopped {
		return errors.New("stop requested after local commit; commit retained but not pushed")
	}
	if err := validateTarget(job); err != nil {
		return err
	}
	workerTop, err := runGit(context.Background(), job.Worktree, "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	workerTop, err = resolvedPath(workerTop)
	if err != nil || workerTop != job.Worktree {
		return errors.New("worker worktree changed before push")
	}
	workerBranch, err := runGit(context.Background(), job.Worktree, "branch", "--show-current")
	if err != nil || workerBranch != job.WorkerBranch {
		return errors.New("worker branch changed before push")
	}
	live, sig, err := readSnapshot(ctx, job)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("operation canceled before push: %w", ctxErr)
		}
		return errors.New("PR/check snapshot changed before push")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("operation canceled before push: %w", err)
	}
	if sig != signature || live.HeadRefOID != snapshot.HeadRefOID {
		return errors.New("PR/check snapshot changed before push")
	}
	if stopped, _ := jobStopped(dir); stopped {
		return errors.New("stop requested immediately before push")
	}
	pushURL, err = validatedPushURL(job.Worktree, job)
	if err != nil {
		return err
	}
	pushed, err := runGit(context.Background(), job.Worktree, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if _, err := runGit(context.Background(), job.Worktree, "merge-base", "--is-ancestor", snapshot.HeadRefOID, pushed); err != nil {
		return errors.New("committed head does not descend from the validated PR head; refusing push")
	}
	ref := "refs/heads/" + job.HeadBranch
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("operation canceled before push: %w", err)
	}
	if _, err := runGit(ctx, job.Worktree, "push", pushURL, pushed+":"+ref); err != nil {
		return err
	}
	job.BaselineHead = pushed
	job.Snapshot = ""
	job.ProcessedSignature = ""
	job.RejectedSignature = ""
	job.PendingSignature = ""
	job.Proposal = ""
	job.Status = "running"
	job.Phase = "polling"
	job.ApprovalScope, job.ApprovalSignature = "", ""
	return monitorEvent(dir, job, "Validated changed paths; committed and pushed guarded changes to "+job.HeadBranch+".")
}

func validatedPushURL(worktree string, job *monitorJob) (string, error) {
	config, err := runGit(context.Background(), worktree, "config", "--null", "--list")
	if err != nil {
		return "", fmt.Errorf("cannot inspect Git push configuration; refusing push: %w", err)
	}
	var pushURLs, remoteURLs []string
	for _, entry := range strings.Split(config, "\x00") {
		if entry == "" {
			continue
		}
		key, value, ok := strings.Cut(entry, "\n")
		if !ok {
			return "", errors.New("cannot parse Git push configuration; refusing push")
		}
		switch strings.ToLower(key) {
		case "remote.origin.pushurl":
			pushURLs = append(pushURLs, value)
		case "remote.origin.url":
			remoteURLs = append(remoteURLs, value)
		}
	}
	pushURL := ""
	if len(pushURLs) > 0 {
		if len(pushURLs) != 1 {
			return "", errors.New("origin must have exactly one push URL; refusing push")
		}
		pushURL = pushURLs[0]
	} else {
		if len(remoteURLs) != 1 {
			return "", errors.New("origin must have exactly one URL; refusing push")
		}
		pushURL = remoteURLs[0]
	}
	if strings.TrimSpace(pushURL) == "" || (!sameRepoURL(pushURL, job.OriginURL) && !sameRepoURL(pushURL, job.HeadRepoURL)) {
		return "", errors.New("origin push URL does not point to the validated PR head repository; refusing push")
	}
	for _, entry := range strings.Split(config, "\x00") {
		if entry == "" {
			continue
		}
		key, value, ok := strings.Cut(entry, "\n")
		if !ok {
			return "", errors.New("cannot parse Git URL rewrite configuration; refusing push")
		}
		key = strings.ToLower(key)
		if strings.HasPrefix(key, "url.") && strings.HasPrefix(pushURL, value) && (strings.HasSuffix(key, ".insteadof") || strings.HasSuffix(key, ".pushinsteadof")) {
			return "", errors.New("matching Git URL rewrite rule; refusing push")
		}
	}
	return pushURL, nil
}

func sameRepoURL(a, b string) bool {
	return strings.TrimSuffix(strings.TrimSpace(a), ".git") == strings.TrimSuffix(strings.TrimSpace(b), ".git")
}

func repoURLMatches(url, repo string) bool {
	clean := strings.TrimSuffix(strings.TrimSpace(url), ".git")
	clean = strings.TrimSuffix(clean, "/")
	if i := strings.LastIndex(clean, ":"); i > 0 && strings.Contains(clean[:i], "@") && !strings.Contains(clean, "://") {
		clean = clean[i+1:]
	}
	clean = strings.TrimPrefix(clean, "https://github.com/")
	clean = strings.TrimPrefix(clean, "http://github.com/")
	clean = strings.TrimPrefix(clean, "ssh://git@github.com/")
	return strings.EqualFold(strings.Trim(clean, "/"), repo)
}
