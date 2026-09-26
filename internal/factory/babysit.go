package factory

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	babysitPollDefault        = 30 * time.Second
	babysitSnapshotMaxRetries = 8
)

var (
	babysitWorkerLauncher     = launchBabysitWorker
	babysitRetryDelay         = snapshotRetryDelay
	babysitLogOpenFile        = os.OpenFile
	babysitAgentActionTimeout = 30 * time.Minute
)

var babysitIDPattern = regexp.MustCompile(`^[0-9]{8}T[0-9]{6}-[a-f0-9]{12}$`)

type babysitJob struct {
	ID                 string    `json:"id"`
	Description        string    `json:"description"`
	RepoRoot           string    `json:"repo_root"`
	Repo               string    `json:"repo"`
	PR                 int       `json:"pr"`
	HeadRepo           string    `json:"head_repo"`
	HeadBranch         string    `json:"head_branch"`
	BaseRepo           string    `json:"base_repo"`
	BaseBranch         string    `json:"base_branch"`
	BaseSHA            string    `json:"base_sha"`
	HeartbeatPath      string    `json:"heartbeat_path,omitempty"`
	OriginURL          string    `json:"origin_url"`
	HeadRepoURL        string    `json:"head_repo_url"`
	BaselineHead       string    `json:"baseline_head"`
	TargetBaseline     string    `json:"target_baseline"`
	Worktree           string    `json:"worktree,omitempty"`
	WorkerBranch       string    `json:"worker_branch,omitempty"`
	Status             string    `json:"status"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
	LastEvent          string    `json:"last_event,omitempty"`
	Snapshot           string    `json:"snapshot,omitempty"`
	PendingSignature   string    `json:"pending_signature,omitempty"`
	Proposal           string    `json:"proposal,omitempty"`
	ApprovalScope      string    `json:"approval_scope,omitempty"`
	ApprovalSignature  string    `json:"approval_signature,omitempty"`
	RejectedSignature  string    `json:"rejected_signature,omitempty"`
	Attempts           int       `json:"attempts,omitempty"`
	AttemptsSignature  string    `json:"attempts_signature,omitempty"`
	ProcessedSignature string    `json:"processed_signature,omitempty"`
	PID                int       `json:"pid,omitempty"`
	Heartbeat          time.Time `json:"heartbeat,omitempty"`
	StopRequested      bool      `json:"stop_requested,omitempty"`
	SnapshotFailures   int       `json:"snapshot_failures,omitempty"`
}

type babysitSnapshot struct {
	Number            int             `json:"number"`
	State             string          `json:"state"`
	Title             string          `json:"title"`
	URL               string          `json:"url"`
	HeadRefName       string          `json:"headRefName"`
	HeadRefOID        string          `json:"headRefOid"`
	HeadRepository    *ghRepository   `json:"headRepository"`
	BaseRefName       string          `json:"baseRefName"`
	BaseRefOID        string          `json:"baseRefOid"`
	BaseRepository    *ghRepository   `json:"baseRepository"`
	Comments          json.RawMessage `json:"comments"`
	StatusCheckRollup json.RawMessage `json:"statusCheckRollup"`
}

type ghRepository struct {
	NameWithOwner string `json:"nameWithOwner"`
	URL           string `json:"url"`
	SSHURL        string `json:"sshUrl"`
}

func babysitRoot(cfg Config) (string, error) {
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
	return filepath.Join(base, "factory", "jobs"), nil
}

func babysitJobDir(root, id string) (string, error) {
	if !babysitIDPattern.MatchString(id) {
		return "", fmt.Errorf("invalid job id")
	}
	return filepath.Join(root, id), nil
}

func saveBabysitJob(dir string, job *babysitJob) error {
	if err := writeBabysitJob(dir, job); err != nil {
		return err
	}
	return syncMonitorJobStatus(dir, job)
}

func writeBabysitJob(dir string, job *babysitJob) error {
	job.UpdatedAt = time.Now().UTC()
	if job.PID != 0 {
		job.Heartbeat = time.Now().UTC()
	}
	data, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".job-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	ok := false
	defer func() {
		tmp.Close()
		if !ok {
			os.Remove(name)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, filepath.Join(dir, "job.json")); err != nil {
		return err
	}
	ok = true
	return nil
}

func readBabysitJob(dir string) (*babysitJob, error) {
	data, err := os.ReadFile(filepath.Join(dir, "job.json"))
	if err != nil {
		return nil, err
	}
	var job babysitJob
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, err
	}
	if !babysitIDPattern.MatchString(job.ID) || filepath.Base(dir) != job.ID {
		return nil, fmt.Errorf("invalid job metadata")
	}
	return &job, nil
}

func appendBabysitLog(dir, message string) error {
	path := filepath.Join(dir, "actions.log")
	if err := ensureRegularIfExists(path); err != nil {
		return err
	}
	f, err := babysitLogOpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := fmt.Fprintf(f, "[%s] %s\n", time.Now().UTC().Format(time.RFC3339), message)
	closeErr := f.Close()
	return errors.Join(writeErr, closeErr)
}

func babysitEvent(dir string, job *babysitJob, message string) error {
	if err := appendBabysitLog(dir, message); err != nil {
		return err
	}
	job.LastEvent = message
	if err := writeBabysitJob(dir, job); err != nil {
		return err
	}
	return syncMonitorJobEvent(dir, job)
}

func loadJobs(root string) ([]babysitJob, error) {
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var jobs []babysitJob
	for _, entry := range entries {
		if !entry.IsDir() || !babysitIDPattern.MatchString(entry.Name()) {
			continue
		}
		job, err := readBabysitJob(filepath.Join(root, entry.Name()))
		if err != nil {
			continue
		}
		jobs = append(jobs, *job)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].CreatedAt.After(jobs[j].CreatedAt) })
	return jobs, nil
}

func runGit(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
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

func readSnapshot(ctx context.Context, job *babysitJob) (*babysitSnapshot, string, error) {
	data, err := runGH(ctx, "pr", "view", strconv.Itoa(job.PR), "--repo", job.Repo, "--json", "number,state,title,url,headRefName,headRefOid,headRepository,baseRefName,baseRefOid,baseRepository,comments,statusCheckRollup")
	if err != nil {
		return nil, "", err
	}
	var s babysitSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, "", fmt.Errorf("invalid GitHub PR JSON: %w", err)
	}
	if s.Number != job.PR || s.HeadRefName != job.HeadBranch || s.BaseRefName != job.BaseBranch || s.HeadRefOID == "" || s.BaseRefOID == "" || (job.BaseSHA != "" && s.BaseRefOID != job.BaseSHA) || s.HeadRepository == nil || s.HeadRepository.NameWithOwner != job.HeadRepo || s.BaseRepository == nil || s.BaseRepository.NameWithOwner != job.BaseRepo {
		return nil, "", errors.New("PR identity changed or metadata is incomplete")
	}
	canonical, err := json.Marshal(s)
	if err != nil {
		return nil, "", err
	}
	hash := sha256.Sum256(canonical)
	return &s, hex.EncodeToString(hash[:]), nil
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

func workerFresh(job babysitJob) bool {
	info, err := os.Stat(job.HeartbeatPath)
	return err == nil && time.Since(info.ModTime()) < 2*time.Minute
}

func babysitID() (string, error) {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return time.Now().UTC().Format("20060102T150405") + "-" + hex.EncodeToString(b[:]), nil
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

func acquireBabysitLock(root string) (func(), error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	return acquireOwnedLock(filepath.Join(root, ".lock"))
}

// BabysitCommand implements monitor user-facing and internal worker commands.
func BabysitCommand(args []string, cfg Config, workdir string, in io.Reader, out, errOut io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: factory monitor <description> | list | describe <id> | approve <id> | reject <id> | stop <id> | reset <id>")
	}
	root, err := babysitRoot(cfg)
	if err != nil {
		return err
	}
	if args[0] == "--worker" {
		if len(args) != 2 {
			return fmt.Errorf("invalid internal worker arguments")
		}
		if err := runBabysitWorker(args[1], root, cfg); err != nil {
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
		fmt.Fprintf(out, "%-29s %-20s %-28s %-8s %s\n", "ID", "STATUS", "REPO", "PR", "UPDATED")
		for _, j := range jobs {
			fmt.Fprintf(out, "%-29s %-20s %-28s #%d %s\n", j.ID, j.Status, j.Repo, j.PR, j.UpdatedAt.Format(time.RFC3339))
		}
		return nil
	case "describe":
		if len(args) != 2 {
			return fmt.Errorf("describe requires a job id")
		}
		job, dir, err := findBabysitJob(root, args[1])
		if err != nil {
			return err
		}
		data, _ := json.MarshalIndent(job, "", "  ")
		fmt.Fprintln(out, string(data))
		if log, err := os.ReadFile(filepath.Join(dir, "actions.log")); err == nil {
			fmt.Fprintln(out, "\nActions:\n"+string(log))
		}
		for _, name := range []string{"proposal.txt", "agent.log"} {
			if data, err := os.ReadFile(filepath.Join(dir, name)); err == nil {
				fmt.Fprintf(out, "\n%s:\n%s\n", name, data)
			}
		}
		return nil
	case "approve", "reject", "stop", "reset":
		if len(args) != 2 {
			return fmt.Errorf("%s requires a job id", args[0])
		}
		return babysitAction(args[0], root, args[1], cfg, in, out)
	case "-h", "--help", "help":
		fmt.Fprintln(out, "Usage: factory monitor <description>\n       factory monitor list\n       factory monitor describe <id>\n       factory monitor approve <id>\n       factory monitor reject <id>\n       factory monitor stop <id>\n       factory monitor reset <id>\nLegacy alias: factory babysit")
		return nil
	default:
		return startBabysit(args, cfg, workdir, root, out)
	}
}

func findBabysitJob(root, id string) (*babysitJob, string, error) {
	dir, err := babysitJobDir(root, id)
	if err != nil {
		return nil, "", err
	}
	job, err := readBabysitJob(dir)
	if err != nil {
		return nil, "", fmt.Errorf("job not found: %s", id)
	}
	return job, dir, nil
}

func startBabysit(args []string, cfg Config, workdir, root string, out io.Writer) error {
	description := strings.TrimSpace(strings.Join(args, " "))
	if description == "" {
		return fmt.Errorf("description cannot be empty")
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	poll := os.Getenv("FACTORY_BABYSIT_POLL_INTERVAL")
	if poll != "" {
		if parsed, err := time.ParseDuration(poll); err != nil || parsed < time.Second {
			return fmt.Errorf("FACTORY_BABYSIT_POLL_INTERVAL must be at least 1s")
		}
	}
	rootRepo, err := runGit(context.Background(), workdir, "rev-parse", "--show-toplevel")
	if err != nil {
		return fmt.Errorf("not inside a git repository: %w", err)
	}
	rootRepo, err = canonicalPath(rootRepo)
	if err != nil {
		return err
	}
	statePath, err := canonicalPath(root)
	if err != nil {
		return err
	}
	if isWithin(rootRepo, statePath) {
		return fmt.Errorf("babysit state must be outside target repository")
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
	data, err := runGH(context.Background(), "pr", "view", "--json", "number,state,headRefName,headRefOid,headRepository,baseRefName,baseRefOid,baseRepository")
	if err != nil {
		return fmt.Errorf("cannot identify current PR: %w", err)
	}
	var info babysitSnapshot
	if err := json.Unmarshal(data, &info); err != nil {
		return err
	}
	if info.Number <= 0 || !strings.EqualFold(info.State, "OPEN") || info.HeadRefName != branch || info.HeadRefOID == "" || info.BaseRefOID == "" || info.HeadRepository == nil || info.BaseRepository == nil {
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
	unlock, err := acquireBabysitLock(root)
	if err != nil {
		return err
	}
	defer unlock()
	jobs, _ := loadJobs(root)
	for _, j := range jobs {
		if j.Repo == info.BaseRepository.NameWithOwner && j.PR == info.Number && isBabysitActive(j.Status) && (j.Status == "starting" || workerFresh(j) && processAlive(j.PID)) {
			return fmt.Errorf("an active babysitter already monitors %s#%d (%s)", j.Repo, j.PR, j.ID)
		}
	}
	id, err := babysitID()
	if err != nil {
		return err
	}
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	workerBranch := "factory-babysit/" + id
	worktree := filepath.Join(dir, "checkout")
	if _, err := runGit(context.Background(), rootRepo, "worktree", "add", "-b", workerBranch, worktree, localHead); err != nil {
		_ = os.RemoveAll(dir)
		return fmt.Errorf("create isolated worker worktree: %w", err)
	}
	job := &babysitJob{ID: id, Description: description, RepoRoot: rootRepo, Repo: info.BaseRepository.NameWithOwner, PR: info.Number, HeadRepo: headRepo, HeadBranch: branch, BaseRepo: info.BaseRepository.NameWithOwner, BaseBranch: info.BaseRefName, BaseSHA: info.BaseRefOID, OriginURL: origin, HeadRepoURL: repoURL.URL, HeartbeatPath: filepath.Join(dir, "heartbeat"), BaselineHead: localHead, TargetBaseline: localHead, Worktree: worktree, WorkerBranch: workerBranch, Status: "starting", CreatedAt: time.Now().UTC()}
	if err := saveBabysitJob(dir, job); err != nil {
		_, _ = runGit(context.Background(), rootRepo, "worktree", "remove", "--force", worktree)
		_ = os.RemoveAll(dir)
		return err
	}
	if err := createMonitorJobRecord(cfg, job); err != nil {
		_, _ = runGit(context.Background(), rootRepo, "worktree", "remove", "--force", worktree)
		_ = os.RemoveAll(dir)
		return err
	}
	_ = appendBabysitLog(dir, "Watcher metadata registered; starting detached monitor.")
	pid, err := babysitWorkerLauncher(id, root)
	if err != nil {
		job.Status = "failed"
		_ = saveBabysitJob(dir, job)
		_ = appendBabysitLog(dir, "Could not launch worker: "+err.Error())
		return err
	}
	if err := registerMonitorWorkerIfMissing(job.ID, dir, pid); err != nil {
		return err
	}
	registered, readErr := readBabysitJob(dir)
	if readErr != nil {
		return readErr
	}
	if registered.PID == 0 {
		registered.PID = pid
		if err := saveBabysitJob(dir, registered); err != nil {
			return err
		}
	}
	fmt.Fprintf(out, "Started babysitter %s for %s#%d (%s).\n", id, job.Repo, job.PR, job.HeadBranch)
	return nil
}

func isBabysitActive(status string) bool {
	return status == "starting" || status == "running" || status == "awaiting_approval"
}

func babysitAction(action, root, id string, cfg Config, in io.Reader, out io.Writer) error {
	unlock, err := acquireBabysitLock(root)
	if err != nil {
		return err
	}
	defer unlock()
	job, dir, err := findBabysitJob(root, id)
	if err != nil {
		return err
	}
	switch action {
	case "stop":
		job.StopRequested = true
		if store, exists, err := existingMonitorJobStore(dir, job.ID); err != nil {
			return err
		} else if exists {
			if err := store.RequestStop(job.ID); err != nil {
				return err
			}
		}
		if err := os.WriteFile(filepath.Join(dir, "stop.requested"), []byte("stop\\n"), 0o600); err != nil {
			return err
		}
		if err := babysitEvent(dir, job, "Cooperative stop requested."); err != nil {
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
		if job.Worktree != "" {
			_, _ = runGit(context.Background(), job.RepoRoot, "worktree", "remove", "--force", job.Worktree)
			job.Worktree, job.WorkerBranch = "", ""
		}
		job.Status = "starting"
		job.PID = 0
		_ = os.Remove(filepath.Join(dir, "stop.requested"))
		if err := resetMonitorJobRecord(job.ID, filepath.Join(root, "v2")); err != nil {
			return err
		}
		if err := babysitEvent(dir, job, "Recoverable failure reset; detached worker restarting."); err != nil {
			return err
		}
		pid, err := babysitWorkerLauncher(job.ID, root)
		if err != nil {
			job.Status = "recoverable_failure"
			_ = saveBabysitJob(dir, job)
			return err
		}
		if err := registerMonitorWorkerIfMissing(job.ID, dir, pid); err != nil {
			return err
		}
		registered, readErr := readBabysitJob(dir)
		if readErr == nil && registered.PID == 0 {
			registered.PID = pid
			_ = saveBabysitJob(dir, registered)
		}
		return nil
	case "reject":
		if job.Status != "awaiting_approval" {
			return fmt.Errorf("job has no pending proposal")
		}
		job.RejectedSignature = job.PendingSignature
		job.PendingSignature = ""
		job.Proposal = ""
		job.ApprovalScope = ""
		job.ApprovalSignature = ""
		job.Status = "running"
		return babysitEvent(dir, job, "Proposal rejected; monitoring continues without repeating this action.")
	case "approve":
		if job.Status != "awaiting_approval" || job.PendingSignature == "" || job.Proposal == "" {
			return fmt.Errorf("job has no pending proposal")
		}
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
			_ = babysitEvent(dir, job, "Approval invalidated: live PR/check snapshot changed.")
			return fmt.Errorf("proposal snapshot changed; watcher must reassess")
		}
		job.ApprovalScope = scope
		job.ApprovalSignature = signature
		job.RejectedSignature = ""
		job.Status = "running"
		return babysitEvent(dir, job, "Human approved a non-empty scope bound to the current PR/check snapshot.")
	}
	return fmt.Errorf("unknown action")
}

func readPollInterval() time.Duration {
	if value := os.Getenv("FACTORY_BABYSIT_POLL_INTERVAL"); value != "" {
		if n, err := time.ParseDuration(value); err == nil && n >= time.Second {
			return n
		}
	}
	return babysitPollDefault
}

func runBabysitWorker(id, root string, cfg Config) error {
	dir, err := babysitJobDir(root, id)
	if err != nil {
		return err
	}
	job, err := readBabysitJob(dir)
	if err != nil {
		return err
	}
	workerLock := filepath.Join(dir, "worker.lock")
	unlockWorker, err := acquireOwnedLock(workerLock)
	if err != nil {
		return fmt.Errorf("worker already running for job %s: %w", id, err)
	}
	defer unlockWorker()
	if err := cfg.Validate(); err != nil {
		job.Status = "failed"
		_ = saveBabysitJob(dir, job)
		return err
	}
	job.PID = os.Getpid()
	job.Status = "running"
	heartbeat := filepath.Join(dir, "heartbeat")
	if err := os.WriteFile(heartbeat, nil, 0o600); err != nil {
		return err
	}
	if err := registerMonitorWorker(job.ID, dir, os.Getpid()); err != nil {
		return err
	}
	if err := babysitEvent(dir, job, "Detached worker registered."); err != nil {
		return err
	}
	defer func() {
		if job.Worktree != "" {
			_, _ = runGit(context.Background(), job.RepoRoot, "worktree", "remove", "--force", job.Worktree)
		}
	}()
	defer func() {
		job.PID = 0
		_ = saveBabysitJob(dir, job)
		_ = clearMonitorWorker(job.ID, dir)
	}()
	interval := readPollInterval()
	workerCtx, cancelWorker := context.WithCancel(context.Background())
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
	attemptsBySignature := map[string]int{}
	lastSignature := ""
	for {
		job, err = readBabysitJob(dir)
		if err != nil {
			return err
		}
		if stopped, stopErr := jobStopped(dir); stopErr != nil {
			return stopErr
		} else if stopped {
			job.Status = "stopped"
			return babysitEvent(dir, job, "Stopped at cooperative safe point.")
		}
		if err := validateTarget(job); err != nil {
			job.Status = "failed"
			_ = babysitEvent(dir, job, "Target validation failed: "+err.Error())
			return err
		}
		if job.Worktree != "" {
			workerTop, err := runGit(context.Background(), job.Worktree, "rev-parse", "--show-toplevel")
			if err != nil {
				job.Status = "failed"
				_ = babysitEvent(dir, job, "Initial worktree unavailable.")
				return err
			}
			workerTop, err = canonicalPath(workerTop)
			if err != nil || workerTop != job.Worktree {
				job.Status = "failed"
				_ = babysitEvent(dir, job, "Initial worktree identity validation failed.")
				return fmt.Errorf("invalid worker worktree")
			}
			workerBranch, err := runGit(context.Background(), job.Worktree, "branch", "--show-current")
			if err != nil || workerBranch != job.WorkerBranch {
				job.Status = "failed"
				_ = babysitEvent(dir, job, "Initial worker branch validation failed.")
				return fmt.Errorf("invalid worker branch")
			}
		}
		ctx, cancel := context.WithTimeout(workerCtx, 2*time.Minute)
		snapshot, signature, err := readSnapshot(ctx, job)
		cancel()
		if err != nil {
			if workerCtx.Err() != nil {
				job.Status = "stopped"
				return babysitEvent(dir, job, "Stopped; active GitHub query or agent canceled.")
			}
			job.SnapshotFailures++
			_ = appendBabysitLog(dir, fmt.Sprintf("Transient GitHub query failure (%d/%d): %v", job.SnapshotFailures, babysitSnapshotMaxRetries, err))
			if saveErr := saveBabysitJob(dir, job); saveErr != nil {
				return saveErr
			}
			if job.SnapshotFailures >= babysitSnapshotMaxRetries {
				job.Status = "recoverable_failure"
				if saveErr := babysitEvent(dir, job, "GitHub snapshot failed eight consecutive times; reset to retry."); saveErr != nil {
					return saveErr
				}
				return nil
			}
			if !sleepBabysitContext(workerCtx, dir, babysitRetryDelay(job.SnapshotFailures)) {
				job.Status = "stopped"
				return babysitEvent(dir, job, "Stopped during GitHub retry backoff.")
			}
			continue
		}
		if job.SnapshotFailures != 0 {
			job.SnapshotFailures = 0
			if err := saveBabysitJob(dir, job); err != nil {
				return err
			}
		}
		if strings.EqualFold(snapshot.State, "MERGED") || strings.EqualFold(snapshot.State, "CLOSED") {
			if strings.EqualFold(snapshot.State, "MERGED") {
				job.Status = "completed"
			} else {
				job.Status = "closed"
			}
			return babysitEvent(dir, job, "PR is "+strings.ToLower(snapshot.State)+"; monitoring stopped.")
		}
		if snapshot.HeadRefOID != job.BaselineHead {
			job.Status = "failed"
			_ = babysitEvent(dir, job, "PR head changed outside babysitter; refusing stale worktree.")
			return fmt.Errorf("PR head changed")
		}
		initialSnapshot := job.Snapshot == ""
		changedSnapshot := !initialSnapshot && signature != job.Snapshot
		if signature != lastSignature {
			if lastSignature != "" && job.PendingSignature != "" && job.PendingSignature != signature {
				job.PendingSignature = ""
				job.Proposal = ""
				job.ApprovalScope = ""
				job.ApprovalSignature = ""
				job.Status = "running"
				_ = babysitEvent(dir, job, "Pending approval invalidated because PR/check snapshot changed.")
			}
			if job.ApprovalSignature != "" && job.ApprovalSignature != signature {
				job.ApprovalSignature = ""
				job.ApprovalScope = ""
				_ = appendBabysitLog(dir, "Approval scope invalidated by snapshot change.")
			}
			job.Snapshot = signature
			if err := babysitEvent(dir, job, "Observed PR/check snapshot "+signature+"."); err != nil {
				return err
			}
			lastSignature = signature
		}
		if job.Status == "awaiting_approval" {
			if !sleepBabysitContext(workerCtx, dir, interval) {
				job.Status = "stopped"
				return babysitEvent(dir, job, "Stopped while waiting for approval.")
			}
			continue
		}
		if job.RejectedSignature != "" && signature == job.RejectedSignature {
			if !sleepBabysitContext(workerCtx, dir, interval) {
				job.Status = "stopped"
				return babysitEvent(dir, job, "Stopped while waiting after rejection.")
			}
			continue
		}
		if job.AttemptsSignature != signature {
			job.AttemptsSignature, job.Attempts = signature, 0
		}
		failed := hasFailedCheck(snapshot.StatusCheckRollup)
		if signature == job.ProcessedSignature && job.ApprovalSignature != signature && !failed {
			if !sleepBabysitContext(workerCtx, dir, interval) {
				job.Status = "stopped"
				return babysitEvent(dir, job, "Stopped while waiting for a new PR event.")
			}
			continue
		}
		actionable := failed || changedSnapshot || (initialSnapshot && hasNewCommentEvent(snapshot.Comments))
		if job.ApprovalSignature == signature && job.ApprovalScope != "" {
			actionable = true
		}
		if !actionable || (job.Attempts >= 3 && job.ApprovalSignature != signature) {
			if job.Attempts >= 3 && job.Status != "awaiting_approval" {
				job.Status = "awaiting_approval"
				job.PendingSignature = signature
				job.Proposal = "Automatic attempt cap (3) reached for unchanged PR/check snapshot."
				_ = babysitEvent(dir, job, "Attempt cap reached; explicit approval required.")
			}
			if !sleepBabysitContext(workerCtx, dir, interval) {
				job.Status = "stopped"
				return babysitEvent(dir, job, "Stopped while waiting for an actionable PR event.")
			}
			continue
		}
		if job.ApprovalSignature != "" && job.ApprovalSignature != signature {
			job.ApprovalSignature = ""
			job.ApprovalScope = ""
		}
		attemptsBySignature[signature]++
		if job.ApprovalSignature == signature {
			job.Attempts = 0
		}
		job.Attempts++
		job.ProcessedSignature = signature
		job.PendingSignature, job.Proposal = "", ""
		if err := saveBabysitJob(dir, job); err != nil {
			return err
		}
		if err := processBabysitEventContext(workerCtx, dir, job, cfg, snapshot, signature); err != nil {
			_ = appendBabysitLog(dir, "Event processing failed safely: "+err.Error())
		}
		if job.ApprovalSignature == signature {
			job.ApprovalSignature, job.ApprovalScope = "", ""
			_ = saveBabysitJob(dir, job)
		}
		if !sleepBabysitContext(workerCtx, dir, interval) {
			job.Status = "stopped"
			return babysitEvent(dir, job, "Stopped while waiting for the next PR poll.")
		}
	}
}

func sleepBabysit(dir string, interval time.Duration) {
	_ = sleepBabysitContext(context.Background(), dir, interval)
}

func sleepBabysitContext(ctx context.Context, dir string, interval time.Duration) bool {
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

func validateWorker(job *babysitJob) error {
	if job.Worktree == "" || job.WorkerBranch == "" {
		return errors.New("worker worktree is not registered")
	}
	root, err := runGit(context.Background(), job.Worktree, "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	root, err = canonicalPath(root)
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

func validateTarget(job *babysitJob) error {
	root, err := runGit(context.Background(), job.RepoRoot, "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	root, err = canonicalPath(root)
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
	if branch != job.HeadBranch {
		return fmt.Errorf("target branch changed")
	}
	head, err := runGit(context.Background(), root, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if head != job.TargetBaseline {
		return fmt.Errorf("target baseline changed")
	}
	status, err := runGit(context.Background(), root, "status", "--porcelain")
	if err != nil {
		return err
	}
	if status != "" {
		return fmt.Errorf("target checkout is no longer clean")
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
			if strings.EqualFold(value, "FAILURE") || strings.EqualFold(value, "FAILED") || strings.EqualFold(value, "TIMED_OUT") || strings.EqualFold(value, "ERROR") {
				return true
			}
		}
	}
	return false
}

func hasNewCommentEvent(raw json.RawMessage) bool {
	return len(raw) > 2 && string(raw) != "null" && string(raw) != "[]"
}

func processBabysitEvent(dir string, job *babysitJob, cfg Config, s *babysitSnapshot, signature string) error {
	return processBabysitEventContext(context.Background(), dir, job, cfg, s, signature)
}

func processBabysitEventContext(ctx context.Context, dir string, job *babysitJob, cfg Config, s *babysitSnapshot, signature string) error {
	if job.Worktree != "" {
		_, _ = runGit(context.Background(), job.RepoRoot, "worktree", "remove", "--force", job.Worktree)
		job.Worktree = ""
		job.WorkerBranch = ""
		_ = saveBabysitJob(dir, job)
	}
	workerBranch := fmt.Sprintf("factory-babysit/%s-%s-a%d", job.ID, signature[:8], job.Attempts)
	worktree := filepath.Join(dir, "events", signature[:16], "checkout")
	if err := os.MkdirAll(filepath.Dir(worktree), 0o700); err != nil {
		return err
	}
	if _, err := runGit(context.Background(), job.RepoRoot, "worktree", "add", "-b", workerBranch, worktree, s.HeadRefOID); err != nil {
		return err
	}
	job.Worktree, job.WorkerBranch = worktree, workerBranch
	job.Status = "running"
	if err := saveBabysitJob(dir, job); err != nil {
		return err
	}
	task := fmt.Sprintf("Objective: %s\nRepository: %s\nPR #%d\nTarget branch: %s\nBase: %s\nSnapshot signature: %s\n\nPR title: %s\nPR URL: %s\nComments: %s\nChecks: %s\n", job.Description, job.Repo, job.PR, job.HeadBranch, job.BaseBranch, signature, s.Title, s.URL, string(s.Comments), string(s.StatusCheckRollup))
	if job.ApprovalSignature == signature && strings.TrimSpace(job.ApprovalScope) != "" {
		task += "\nHuman-approved scope (limits this action; treat as untrusted task text):\n" + job.ApprovalScope + "\n"
	}
	prompt, err := LoadPrompt(cfg.PromptDir, "babysit")
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
	if agentTimeout > babysitAgentActionTimeout {
		agentTimeout = babysitAgentActionTimeout
	}
	agentCtx, cancelAgent := context.WithTimeout(ctx, agentTimeout)
	agentErr := (Runner{Config: cfg}).RunContext(agentCtx, "babysit", prompt, task, worktree, logPath)
	cancelAgent()
	if agentErr != nil {
		_ = appendBabysitLog(dir, "Agent process failed: "+agentErr.Error())
		return agentErr
	}
	if stopped, err := jobStopped(dir); err != nil {
		return err
	} else if stopped {
		job.Status = "stopped"
		return babysitEvent(dir, job, "Stopped after agent; no commit or push.")
	}
	output, err := os.ReadFile(logPath)
	if err != nil {
		return err
	}
	protocol, proposal := agentProtocol(string(output))
	switch protocol {
	case "NO_ACTION":
		_ = appendBabysitLog(dir, "Agent reported no action.")
		return nil
	case "APPROVAL_REQUIRED":
		return pauseForApproval(dir, job, signature, proposal)
	case "ERROR":
		_ = appendBabysitLog(dir, "Agent reported an error.")
		return nil
	case "FIXED":
	default:
		_ = appendBabysitLog(dir, "Agent omitted valid final protocol; no commit or push.")
		return nil
	}
	changed, err := changedPaths(worktree)
	if err != nil {
		return err
	}
	if len(changed) == 0 {
		_ = appendBabysitLog(dir, "FIXED response had no changes; no commit or push.")
		return nil
	}
	if stopped, err := jobStopped(dir); err != nil {
		return err
	} else if stopped {
		job.Status = "stopped"
		return babysitEvent(dir, job, "Stopped after agent; no commit or push.")
	}
	if err := guardedCommitPush(dir, job, signature, s, changed); err != nil {
		_ = appendBabysitLog(dir, "Commit/push stopped safely: "+err.Error())
		if strings.Contains(err.Error(), "stop requested") {
			job.Status = "stopped"
			return babysitEvent(dir, job, err.Error())
		}
		return nil
	}
	return nil
}

func jobStopped(dir string) (bool, error) {
	if _, err := os.Stat(filepath.Join(dir, "stop.requested")); err == nil {
		return true, nil
	} else if !os.IsNotExist(err) {
		return false, err
	}
	id := filepath.Base(dir)
	if babysitIDPattern.MatchString(id) {
		store, exists, err := existingMonitorJobStore(dir, id)
		if err != nil {
			return false, err
		}
		if exists && store.StopRequested(id) {
			return true, nil
		}
	}
	job, err := readBabysitJob(dir)
	if err != nil {
		return false, err
	}
	return job.StopRequested, nil
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

func pauseForApproval(dir string, job *babysitJob, signature, proposal string) error {
	if strings.TrimSpace(proposal) == "" {
		proposal = "Agent requested explicit human review; inspect the logs before approval."
	}
	job.Status = "awaiting_approval"
	job.PendingSignature = signature
	job.Proposal = proposal
	job.ApprovalScope = ""
	job.ApprovalSignature = ""
	_ = os.WriteFile(filepath.Join(dir, "proposal.txt"), []byte(proposal+"\n"), 0o600)
	return babysitEvent(dir, job, "Action paused for explicit human approval: "+proposal)
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

func guardedCommitPush(dir string, job *babysitJob, signature string, snapshot *babysitSnapshot, files []string) error {
	if stopped, _ := jobStopped(dir); stopped {
		return errors.New("stop requested before commit")
	}
	if err := validateTarget(job); err != nil {
		return err
	}
	root, err := runGit(context.Background(), job.Worktree, "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	root, err = canonicalPath(root)
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
	if err := validateWorker(job); err != nil {
		return err
	}
	live, liveSignature, err := readSnapshot(context.Background(), job)
	if err != nil || liveSignature != signature || live.HeadRefOID != snapshot.HeadRefOID {
		return errors.New("PR/check snapshot changed before commit")
	}
	args := []string{"add", "--"}
	for _, path := range files {
		args = append(args, ":(literal)"+path)
	}
	if _, err := runGit(context.Background(), job.Worktree, args...); err != nil {
		return err
	}
	if _, err := runGit(context.Background(), job.Worktree, "diff", "--cached", "--check"); err != nil {
		return err
	}
	if stopped, _ := jobStopped(dir); stopped {
		return errors.New("stop requested before commit")
	}
	if err := validateTarget(job); err != nil {
		return err
	}
	if _, err := runGit(context.Background(), job.Worktree, "commit", "-m", "fix: address PR feedback via factory babysit"); err != nil {
		return err
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
	workerTop, err = canonicalPath(workerTop)
	if err != nil || workerTop != job.Worktree {
		return errors.New("worker worktree changed before push")
	}
	workerBranch, err := runGit(context.Background(), job.Worktree, "branch", "--show-current")
	if err != nil || workerBranch != job.WorkerBranch {
		return errors.New("worker branch changed before push")
	}
	origin, err := runGit(context.Background(), job.Worktree, "remote", "get-url", "origin")
	if err != nil || !sameRepoURL(origin, job.OriginURL) {
		return errors.New("worker origin changed before push")
	}
	live, sig, err := readSnapshot(context.Background(), job)
	if err != nil || sig != signature || live.HeadRefOID != snapshot.HeadRefOID {
		return errors.New("PR/check snapshot changed before push")
	}
	if stopped, _ := jobStopped(dir); stopped {
		return errors.New("stop requested immediately before push")
	}
	if _, err := runGit(context.Background(), job.Worktree, "push", "origin", "HEAD:refs/heads/"+job.HeadBranch); err != nil {
		return err
	}
	pushed, err := runGit(context.Background(), job.Worktree, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	job.BaselineHead = pushed
	job.Snapshot = ""
	job.ProcessedSignature = ""
	job.RejectedSignature = ""
	job.PendingSignature = ""
	job.Proposal = ""
	job.Status = "running"
	job.ApprovalScope, job.ApprovalSignature = "", ""
	return babysitEvent(dir, job, "Validated changed paths; committed and pushed guarded changes to "+job.HeadBranch+".")
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
