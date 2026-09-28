package factory

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	jobRecordVersion         = 1
	maxSessionEventLineBytes = 1024 * 1024
	orphanJobGracePeriod     = 30 * time.Second
)

var storedIDPattern = regexp.MustCompile(`^(?:[A-Za-z0-9][A-Za-z0-9_-]{0,127}|[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})$`)
var errFileLockBusy = errors.New("file lock is busy")

var jobLockMu sync.Mutex
var jobLockByPath = make(map[string]*sync.Mutex)

// JobRecord is the versioned, shared lifecycle record for a unit of work.
type JobRecord struct {
	Version         int               `json:"version"`
	ID              string            `json:"id"`
	Type            string            `json:"type"`
	TaskDescription string            `json:"task_description,omitempty"`
	TargetPath      string            `json:"target_path,omitempty"`
	RepositoryPath  string            `json:"repository_path,omitempty"`
	TargetBranch    string            `json:"target_branch,omitempty"`
	TargetHead      string            `json:"target_head,omitempty"`
	Worktree        string            `json:"worktree,omitempty"`
	WorkBranch      string            `json:"work_branch,omitempty"`
	Status          string            `json:"status"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
	StartedAt       time.Time         `json:"started_at,omitempty"`
	EndedAt         time.Time         `json:"ended_at,omitempty"`
	Sessions        []SessionMetadata `json:"sessions,omitempty"`
	Monitor         *monitorJob       `json:"monitor,omitempty"`
}

// SessionMetadata preserves session order and lifecycle metadata in the job record.
type SessionMetadata struct {
	ID        string            `json:"id"`
	Type      string            `json:"type,omitempty"`
	Status    string            `json:"status"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	StartedAt time.Time         `json:"started_at,omitempty"`
	EndedAt   time.Time         `json:"ended_at,omitempty"`
}

// SessionRecord identifies a log-bearing execution session associated with a job.
type SessionRecord struct {
	Version   int               `json:"version"`
	ID        string            `json:"id"`
	JobID     string            `json:"job_id"`
	Type      string            `json:"type,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Status    string            `json:"status"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
	StartedAt time.Time         `json:"started_at,omitempty"`
	EndedAt   time.Time         `json:"ended_at,omitempty"`
}

// JobOwner records the current lifecycle owner and its most recent heartbeat.
type JobOwner struct {
	PID         int       `json:"pid"`
	StartedAt   time.Time `json:"started_at"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
}

// WorkerRecord records the worker process associated with a job.
type WorkerRecord struct {
	ID          string    `json:"id,omitempty"`
	PID         int       `json:"pid"`
	StartedAt   time.Time `json:"started_at"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
}

// SessionEvent is appended to a session's durable event history.
type SessionEvent struct {
	At         time.Time `json:"at"`
	Type       string    `json:"type"`
	Message    string    `json:"message"`
	Command    string    `json:"command,omitempty"`
	PID        int       `json:"pid,omitempty"`
	Invocation string    `json:"invocation,omitempty"`
	Outcome    string    `json:"outcome,omitempty"`
	Summary    string    `json:"summary,omitempty"`
}

// JobStore provides concurrency-safe persistence beneath a jobs state root.
type JobStore struct {
	root               string
	writeSessionRecord func(string, SessionRecord) error
	appendSessionLog   func(string, string, []byte) error
}

// JobSessionObserver records workflow lifecycle events in a job session.
type JobSessionObserver struct {
	Store     *JobStore
	JobID     string
	SessionID string
}

// ObserveWorkflowEvent persists a workflow event in the associated session.
func (o JobSessionObserver) ObserveWorkflowEvent(event WorkflowEvent) error {
	if o.Store == nil {
		return fmt.Errorf("workflow observer requires a job store")
	}
	message := event.Message
	if event.Stage != "" {
		message = fmt.Sprintf("stage=%s %s", event.Stage, message)
	}
	record := SessionEvent{Type: event.Type, Message: message, Outcome: event.Outcome}
	if strings.HasPrefix(event.Type, "status.") {
		fields := strings.Fields(event.Message)
		if len(fields) > 0 && strings.HasPrefix(fields[0], "stage=") {
			fields = fields[1:]
		}
		if event.Type == "status.started" {
			record.Invocation = event.Message
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
	}
	if err := o.Store.AppendSessionEventDetails(o.JobID, o.SessionID, record); err != nil {
		return err
	}
	return o.Store.AppendSessionLog(o.JobID, o.SessionID, []byte(fmt.Sprintf("%s %s %s\n", time.Now().UTC().Format(time.RFC3339), event.Type, message)))
}

// NewJobStore creates or opens a job store. The root must be absolute.
func NewJobStore(root string) (*JobStore, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("job state root must be absolute")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create job state root: %w", err)
	}
	root, err := resolvedPath(root)
	if err != nil {
		return nil, fmt.Errorf("resolve job state root: %w", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, fmt.Errorf("secure job state root: %w", err)
	}
	return &JobStore{root: root}, nil
}

// JobStateRoot returns the detached-jobs directory associated with factory state.
func JobStateRoot(override string) (string, error) {
	base := override
	if base == "" {
		base = os.Getenv("XDG_STATE_HOME")
		if base == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return "", fmt.Errorf("find home directory: %w", err)
			}
			base = filepath.Join(home, ".local", "state")
		}
	}
	if !filepath.IsAbs(base) {
		return "", fmt.Errorf("state directory must be absolute")
	}
	return filepath.Join(base, "factory", "detached-jobs"), nil
}

// Root returns the resolved filesystem path backing this store.
func (s *JobStore) Root() string { return s.root }

// LockTarget serializes jobs targeting the same resolved path across processes.
func (s *JobStore) LockTarget(target string) (func(), error) {
	return s.lockTargetIn(".targets", target)
}

// TryLockTarget acquires the target lock without waiting if another worker owns it.
func (s *JobStore) TryLockTarget(target string) (func(), bool, error) {
	return s.tryLockTargetIn(".targets", target)
}

// LockTargetAdmission serializes job creation for a target without blocking its worker lock.
func (s *JobStore) LockTargetAdmission(target string) (func(), error) {
	return s.lockTargetIn(".admissions", target)
}

// LockBranch reserves one branch in a resolved repository across detached jobs.
func (s *JobStore) LockBranch(repository, branch string) (func(), error) {
	return s.lockBranch(repository, branch, false)
}

func (s *JobStore) LockRepositoryAdmission(repository string) (func(), error) {
	resolved, err := resolvedPath(repository)
	if err != nil {
		return nil, fmt.Errorf("resolve admission repository: %w", err)
	}
	locks := filepath.Join(s.root, ".repository-admissions")
	if err := os.MkdirAll(locks, 0o700); err != nil {
		return nil, err
	}
	if err := ensureRealDirectory(s.root, locks); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(resolved))
	return s.lockNamed(filepath.Join(locks, hex.EncodeToString(sum[:])+".lock"), false)
}

func (s *JobStore) TryLockBranch(repository, branch string) (func(), bool, error) {
	unlock, err := s.lockBranch(repository, branch, true)
	if errors.Is(err, errFileLockBusy) {
		return nil, false, nil
	}
	return unlock, err == nil, err
}

func (s *JobStore) lockBranch(repository, branch string, try bool) (func(), error) {
	resolved, err := resolvedPath(repository)
	if err != nil {
		return nil, fmt.Errorf("resolve branch repository: %w", err)
	}
	if strings.TrimSpace(branch) == "" || strings.ContainsRune(branch, 0) {
		return nil, fmt.Errorf("branch name must not be empty")
	}
	locks := filepath.Join(s.root, ".branches")
	if err := os.MkdirAll(locks, 0o700); err != nil {
		return nil, err
	}
	if err := ensureRealDirectory(s.root, locks); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(resolved + "\x00" + branch))
	path := filepath.Join(locks, hex.EncodeToString(sum[:])+".lock")
	return s.lockNamed(path, try)
}

func (s *JobStore) lockNamed(path string, try bool) (func(), error) {
	jobLockMu.Lock()
	localLock := jobLockByPath[path]
	if localLock == nil {
		localLock = &sync.Mutex{}
		jobLockByPath[path] = localLock
	}
	jobLockMu.Unlock()
	if try {
		if !localLock.TryLock() {
			return nil, errFileLockBusy
		}
	} else {
		localLock.Lock()
	}
	timeout := 30 * time.Second
	if try {
		timeout = 0
	}
	file, err := acquireFileLock(path, timeout)
	if err != nil {
		localLock.Unlock()
		return nil, err
	}
	return func() {
		_ = releaseFileLock(file)
		localLock.Unlock()
	}, nil
}

func (s *JobStore) lockTargetIn(lockDir, target string) (func(), error) {
	path, localLock, err := s.targetLockPath(lockDir, target)
	if err != nil {
		return nil, err
	}
	localLock.Lock()
	file, err := acquireFileLock(path, 30*time.Second)
	if err != nil {
		localLock.Unlock()
		return nil, err
	}
	return func() {
		_ = releaseFileLock(file)
		localLock.Unlock()
	}, nil
}

func (s *JobStore) tryLockTargetIn(lockDir, target string) (func(), bool, error) {
	path, localLock, err := s.targetLockPath(lockDir, target)
	if err != nil {
		return nil, false, err
	}
	if !localLock.TryLock() {
		return nil, false, nil
	}
	file, err := acquireFileLock(path, 0)
	if err != nil {
		localLock.Unlock()
		if errors.Is(err, errFileLockBusy) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return func() {
		_ = releaseFileLock(file)
		localLock.Unlock()
	}, true, nil
}

func (s *JobStore) targetLockPath(lockDir, target string) (string, *sync.Mutex, error) {
	resolved, err := resolvedPath(target)
	if err != nil {
		return "", nil, fmt.Errorf("resolve job target: %w", err)
	}
	locks := filepath.Join(s.root, lockDir)
	if err := os.MkdirAll(locks, 0o700); err != nil {
		return "", nil, err
	}
	if err := ensureRealDirectory(s.root, locks); err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256([]byte(resolved))
	path := filepath.Join(locks, hex.EncodeToString(sum[:])+".lock")
	jobLockMu.Lock()
	localLock := jobLockByPath[path]
	if localLock == nil {
		localLock = &sync.Mutex{}
		jobLockByPath[path] = localLock
	}
	jobLockMu.Unlock()
	return path, localLock, nil
}

// reconcileOrphan marks an aged, unowned target job interrupted only while holding
// its target lock, proving that no live worker currently owns the target.
func (s *JobStore) reconcileOrphan(job JobRecord) (bool, error) {
	if job.Type == monitorJobType {
		return s.reconcileMonitorOrphan(job)
	}
	now := time.Now()
	stale := false
	switch job.Status {
	case "queued":
		if now.Sub(job.CreatedAt) < orphanJobGracePeriod {
			return false, nil
		}
		if _, err := s.ReadWorker(job.ID); err == nil {
			return false, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		stale = true
	case "running":
		lastActivity := job.UpdatedAt
		worker, err := s.ReadWorker(job.ID)
		if err == nil && worker.HeartbeatAt.After(lastActivity) {
			lastActivity = worker.HeartbeatAt
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		if now.Sub(lastActivity) < orphanJobGracePeriod {
			return false, nil
		}
		stale = true
	default:
		return false, nil
	}
	if !stale {
		return false, nil
	}
	unlock, acquired, err := s.TryLockTarget(job.TargetPath)
	if err != nil || !acquired {
		return false, err
	}
	defer unlock()
	current, err := s.GetJob(job.ID)
	if err != nil {
		return false, err
	}
	if current.Status != job.Status || current.UpdatedAt != job.UpdatedAt {
		return false, nil
	}
	if _, err := s.UpdateJob(job.ID, func(record *JobRecord) error {
		record.Status = "interrupted"
		return nil
	}); err != nil {
		return false, err
	}
	if _, err := s.UpdateSession(job.ID, "workflow", func(session *SessionRecord) error {
		session.Status = "interrupted"
		return nil
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := s.ClearWorker(job.ID); err != nil {
		return false, err
	}
	return true, nil
}

// reconcileMonitorOrphan uses the per-job lock shared by monitor heartbeat writes.
// Monitor workers do not hold target locks, so target locking cannot prove they are idle.
func (s *JobStore) reconcileMonitorOrphan(job JobRecord) (bool, error) {
	unlock, err := s.LockJob(job.ID)
	if err != nil {
		return false, err
	}
	defer unlock()

	current, err := s.GetJob(job.ID)
	if err != nil {
		return false, err
	}
	if current.Type != monitorJobType || current.Status != job.Status || !current.UpdatedAt.Equal(job.UpdatedAt) {
		return false, nil
	}
	if current.Status != "queued" && current.Status != "running" {
		return false, nil
	}

	now := time.Now()
	lastActivity := current.UpdatedAt
	if current.Status == "queued" {
		lastActivity = current.CreatedAt
	}
	worker, err := s.ReadWorker(current.ID)
	if err == nil {
		if worker.HeartbeatAt.After(lastActivity) {
			lastActivity = worker.HeartbeatAt
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if now.Sub(lastActivity) < orphanJobGracePeriod {
		return false, nil
	}

	session, err := s.GetSession(current.ID, monitorSessionID)
	if err != nil {
		return false, fmt.Errorf("read monitor session: %w", err)
	}
	setLifecycleTimes(current.Status, "interrupted", &current.StartedAt, &current.EndedAt)
	current.Status = "interrupted"
	current.UpdatedAt = now.UTC()
	for i := range current.Sessions {
		if current.Sessions[i].ID == monitorSessionID {
			setLifecycleTimes(session.Status, "interrupted", &session.StartedAt, &session.EndedAt)
			session.Status = "interrupted"
			session.UpdatedAt = current.UpdatedAt
			current.Sessions[i] = sessionMetadata(session)
			break
		}
	}
	jobDir, err := s.jobDir(current.ID, false)
	if err != nil {
		return false, err
	}
	if err := writeJSONAtomic(jobDir, "job.json", current); err != nil {
		return false, err
	}
	sessionDir, err := s.sessionDir(current.ID, monitorSessionID, false)
	if err != nil {
		return false, err
	}
	if err := writeJSONAtomic(sessionDir, "session.json", session); err != nil {
		return false, err
	}
	if current.Monitor == nil {
		return false, fmt.Errorf("monitor job is missing its canonical monitor state")
	}
	current.Monitor.Status = "interrupted"
	current.Monitor.LastEvent = "Monitor worker was stale; reconciliation marked the job interrupted."
	current.Monitor.UpdatedAt = current.UpdatedAt
	if err := writeJSONAtomic(jobDir, "job.json", current); err != nil {
		return false, err
	}
	if err := s.clearWorkerLocked(current.ID); err != nil {
		return false, err
	}
	return true, nil
}

// CreateJobLog creates the private detached-worker transcript.
func (s *JobStore) CreateJobLog(id string) error {
	dir, err := s.jobDir(id, false)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, "worker.log")
	if err := ensureRegularIfExists(path); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	return file.Close()
}

// JobLogPath returns the validated path to the detached-worker transcript.
func (s *JobStore) JobLogPath(id string) (string, error) {
	dir, err := s.jobDir(id, false)
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "worker.log")
	if err := ensureRegularIfExists(path); err != nil {
		return "", err
	}
	return path, nil
}

// RequestStop durably requests cooperative worker cancellation.
func (s *JobStore) RequestStop(id string) error {
	dir, err := s.jobDir(id, false)
	if err != nil {
		return err
	}
	return ensureCancelFile(filepath.Join(dir, "cancel.request"))
}

// ClearStopRequest removes a durable cooperative stop request for a resumable job.
func (s *JobStore) ClearStopRequest(id string) error {
	dir, err := s.jobDir(id, false)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, "cancel.request")
	if err := ensureRegularIfExists(path); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(dir)
}

// StopRequested reports whether a durable cooperative stop was requested.
func (s *JobStore) StopRequested(id string) bool {
	dir, err := s.jobDir(id, false)
	if err != nil {
		return false
	}
	info, err := os.Lstat(filepath.Join(dir, "cancel.request"))
	return err == nil && info.Mode().IsRegular()
}

// LockJob serializes all mutations for a job across processes. The returned
// function releases the lock and is safe to defer.
func (s *JobStore) LockJob(id string) (func(), error) {
	if err := validateStoredID(id); err != nil {
		return nil, err
	}
	locks := filepath.Join(s.root, ".locks")
	if err := os.MkdirAll(locks, 0o700); err != nil {
		return nil, err
	}
	if err := ensureRealDirectory(s.root, locks); err != nil {
		return nil, err
	}
	path := filepath.Join(locks, id+".lock")
	jobLockMu.Lock()
	localLock := jobLockByPath[path]
	if localLock == nil {
		localLock = &sync.Mutex{}
		jobLockByPath[path] = localLock
	}
	jobLockMu.Unlock()
	localLock.Lock()
	file, err := acquireFileLock(path, 30*time.Second)
	if err != nil {
		localLock.Unlock()
		return nil, err
	}
	return func() {
		_ = releaseFileLock(file)
		localLock.Unlock()
	}, nil
}

// CreateJob persists a new job. Existing IDs are never overwritten.
func (s *JobStore) CreateJob(job JobRecord) error {
	if err := validateStoredID(job.ID); err != nil {
		return err
	}
	unlock, err := s.LockJob(job.ID)
	if err != nil {
		return err
	}
	defer unlock()
	dir, err := s.jobDir(job.ID, true)
	if err != nil {
		return err
	}
	if err := ensureRealDirectory(s.root, dir); err != nil {
		return err
	}
	if _, err := os.Lstat(filepath.Join(dir, "job.json")); err == nil {
		return fmt.Errorf("job already exists: %s", job.ID)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	now := time.Now().UTC()
	job.Version = jobRecordVersion
	if job.Type == monitorJobType && job.Monitor == nil {
		job.Monitor = &monitorJob{ID: job.ID, Description: job.TaskDescription, RepoRoot: job.TargetPath, Status: job.Status, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt}
	}
	if job.TargetPath != "" {
		if !filepath.IsAbs(job.TargetPath) {
			return fmt.Errorf("job target path must be absolute")
		}
		job.TargetPath, err = resolvedPath(job.TargetPath)
		if err != nil {
			return fmt.Errorf("resolve job target path: %w", err)
		}
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	if job.Monitor != nil {
		job.Monitor.ID = job.ID
		job.Monitor.Description = job.TaskDescription
		job.Monitor.RepoRoot = job.TargetPath
		job.Monitor.Status = job.Status
		job.Monitor.CreatedAt = job.CreatedAt
		job.Monitor.UpdatedAt = now
	}
	job.UpdatedAt = now
	if job.Status == "running" && job.StartedAt.IsZero() {
		job.StartedAt = now
	}
	if isTerminalStatus(job.Status) && job.EndedAt.IsZero() {
		job.EndedAt = now
	}
	return writeJSONAtomic(dir, "job.json", job)
}

// GetJob reads a job record and rejects unsupported record versions.
func (s *JobStore) GetJob(id string) (JobRecord, error) {
	var job JobRecord
	dir, err := s.jobDir(id, false)
	if err != nil {
		return job, err
	}
	if err := readJSONRegular(filepath.Join(dir, "job.json"), &job); err != nil {
		return job, err
	}
	if job.Version != jobRecordVersion || job.ID != id {
		return JobRecord{}, fmt.Errorf("invalid or unsupported job record")
	}
	return job, nil
}

// ClaimQueuedJob atomically transitions a queued job to running. Other states
// cannot be claimed, preventing duplicate or terminal jobs from being replayed.
func (s *JobStore) ClaimQueuedJob(id, expectedTarget string) (JobRecord, error) {
	unlock, err := s.LockJob(id)
	if err != nil {
		return JobRecord{}, err
	}
	defer unlock()
	job, err := s.GetJob(id)
	if err != nil {
		return JobRecord{}, err
	}
	if job.TargetPath != expectedTarget {
		return JobRecord{}, fmt.Errorf("job %s target changed before claim", id)
	}
	if job.Status != "queued" {
		return JobRecord{}, fmt.Errorf("job %s cannot be claimed from status %s", id, job.Status)
	}
	setLifecycleTimes(job.Status, "running", &job.StartedAt, &job.EndedAt)
	job.Status = "running"
	job.UpdatedAt = time.Now().UTC()
	dir, err := s.jobDir(id, false)
	if err != nil {
		return JobRecord{}, err
	}
	if err := writeJSONAtomic(dir, "job.json", job); err != nil {
		return JobRecord{}, err
	}
	return job, nil
}

// UpdateJob applies a serialized mutation and atomically replaces the record.
func (s *JobStore) UpdateJob(id string, update func(*JobRecord) error) (JobRecord, error) {
	unlock, err := s.LockJob(id)
	if err != nil {
		return JobRecord{}, err
	}
	defer unlock()
	job, err := s.GetJob(id)
	if err != nil {
		return JobRecord{}, err
	}
	createdAt := job.CreatedAt
	oldStatus := job.Status
	if err := update(&job); err != nil {
		return JobRecord{}, err
	}
	if job.ID != id || job.CreatedAt != createdAt {
		return JobRecord{}, fmt.Errorf("job ID and creation time are immutable")
	}
	job.Version = jobRecordVersion
	if job.Type == monitorJobType && job.Monitor == nil {
		job.Monitor = &monitorJob{ID: job.ID, Description: job.TaskDescription, RepoRoot: job.TargetPath, Status: job.Status}
	}
	if job.TargetPath != "" {
		if !filepath.IsAbs(job.TargetPath) {
			return JobRecord{}, fmt.Errorf("job target path must be absolute")
		}
		job.TargetPath, err = resolvedPath(job.TargetPath)
		if err != nil {
			return JobRecord{}, fmt.Errorf("resolve job target path: %w", err)
		}
	}
	if job.RepositoryPath != "" {
		job.RepositoryPath, err = resolvedPath(job.RepositoryPath)
		if err != nil {
			return JobRecord{}, fmt.Errorf("resolve job repository path: %w", err)
		}
	}
	if oldStatus != job.Status {
		setLifecycleTimes(oldStatus, job.Status, &job.StartedAt, &job.EndedAt)
	}
	job.UpdatedAt = time.Now().UTC()
	dir, err := s.jobDir(id, false)
	if err != nil {
		return JobRecord{}, err
	}
	if err := writeJSONAtomic(dir, "job.json", job); err != nil {
		return JobRecord{}, err
	}
	return job, nil
}

// reconcileJob applies orphan recovery to one job and returns its current record.
func (s *JobStore) reconcileJob(id string) (JobRecord, error) {
	job, err := s.GetJob(id)
	if err != nil {
		return JobRecord{}, err
	}
	if _, err := s.reconcileOrphan(job); err != nil {
		return JobRecord{}, err
	}
	if err := s.reconcileTerminalSession(id); err != nil {
		return JobRecord{}, err
	}
	return s.GetJob(id)
}

// reconcileTerminalSession repairs the workflow session after the job's terminal
// record has been persisted. Each record is replaced atomically, so a later call
// can finish the repair if either write is interrupted.
func (s *JobStore) reconcileTerminalSession(id string) error {
	unlock, err := s.LockJob(id)
	if err != nil {
		return err
	}
	defer unlock()

	job, err := s.GetJob(id)
	if err != nil {
		return err
	}
	if (job.Type != implementationJobType && job.Type != tidyJobType) || !isTerminalStatus(job.Status) {
		return nil
	}
	session, err := s.GetSession(id, "workflow")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	if session.Status != job.Status || session.EndedAt.IsZero() {
		setLifecycleTimes(session.Status, job.Status, &session.StartedAt, &session.EndedAt)
		session.Status = job.Status
		session.UpdatedAt = now
		dir, err := s.sessionDir(id, "workflow", false)
		if err != nil {
			return err
		}
		if s.writeSessionRecord != nil {
			if err := s.writeSessionRecord(dir, session); err != nil {
				return err
			}
		} else if err := writeJSONAtomic(dir, "session.json", session); err != nil {
			return err
		}
	}

	metadata := sessionMetadata(session)
	found := false
	for i := range job.Sessions {
		if job.Sessions[i].ID != "workflow" {
			continue
		}
		found = true
		if reflect.DeepEqual(job.Sessions[i], metadata) {
			return nil
		}
		job.Sessions[i] = metadata
		break
	}
	if !found {
		job.Sessions = append(job.Sessions, metadata)
	}
	job.UpdatedAt = now
	jobDir, err := s.jobDir(id, false)
	if err != nil {
		return err
	}
	return writeJSONAtomic(jobDir, "job.json", job)
}

// reconcileJobs scans every persisted job and applies orphan recovery before returning them.
func (s *JobStore) reconcileJobs() ([]JobRecord, error) {
	jobs, err := s.ListJobs()
	if err != nil {
		return nil, err
	}
	var reconcileErrs []error
	for i := range jobs {
		job, reconcileErr := s.reconcileJob(jobs[i].ID)
		if reconcileErr != nil {
			reconcileErrs = append(reconcileErrs, fmt.Errorf("reconcile job %s: %w", jobs[i].ID, reconcileErr))
			continue
		}
		jobs[i] = job
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].CreatedAt.After(jobs[j].CreatedAt) })
	return jobs, errors.Join(reconcileErrs...)
}

// ListJobs returns valid persisted records ordered newest first.
func (s *JobStore) ListJobs() ([]JobRecord, error) {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, err
	}
	jobs := make([]JobRecord, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || validateStoredID(entry.Name()) != nil {
			continue
		}
		job, err := s.GetJob(entry.Name())
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read job %s: %w", entry.Name(), err)
		}
		jobs = append(jobs, job)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].CreatedAt.After(jobs[j].CreatedAt) })
	return jobs, nil
}

// CreateSession creates a session record and its private log file.
func (s *JobStore) CreateSession(jobID, sessionID, status string) (SessionRecord, error) {
	if err := validateStoredID(sessionID); err != nil {
		return SessionRecord{}, err
	}
	unlock, err := s.LockJob(jobID)
	if err != nil {
		return SessionRecord{}, err
	}
	defer unlock()
	if _, err := s.GetJob(jobID); err != nil {
		return SessionRecord{}, err
	}
	parent, err := s.sessionsDir(jobID, true)
	if err != nil {
		return SessionRecord{}, err
	}
	finalDir := filepath.Join(parent, sessionID)
	if _, err := os.Lstat(finalDir); err == nil {
		return SessionRecord{}, fmt.Errorf("session already exists: %s", sessionID)
	} else if !errors.Is(err, os.ErrNotExist) {
		return SessionRecord{}, err
	}
	dir, err := os.MkdirTemp(parent, ".session-*")
	if err != nil {
		return SessionRecord{}, err
	}
	committed := false
	published := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(dir)
			if published {
				_ = os.RemoveAll(finalDir)
			}
		}
	}()
	now := time.Now().UTC()
	session := SessionRecord{Version: jobRecordVersion, ID: sessionID, JobID: jobID, Status: status, CreatedAt: now, UpdatedAt: now}
	if status == "running" {
		session.StartedAt = now
	}
	if isTerminalStatus(status) {
		session.EndedAt = now
	}
	if err := writeJSONAtomic(dir, "session.json", session); err != nil {
		return SessionRecord{}, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "session.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return SessionRecord{}, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return SessionRecord{}, err
	}
	if err := f.Close(); err != nil {
		return SessionRecord{}, err
	}
	if err := os.Rename(dir, finalDir); err != nil {
		return SessionRecord{}, err
	}
	published = true
	dir = finalDir
	job, err := s.GetJob(jobID)
	if err != nil {
		return SessionRecord{}, err
	}
	job.Sessions = append(job.Sessions, sessionMetadata(session))
	job.UpdatedAt = now
	jobDir, err := s.jobDir(jobID, false)
	if err != nil {
		return SessionRecord{}, err
	}
	if err := writeJSONAtomic(jobDir, "job.json", job); err != nil {
		return SessionRecord{}, err
	}
	committed = true
	return session, nil
}

// GetSession reads a session record.
func (s *JobStore) GetSession(jobID, sessionID string) (SessionRecord, error) {
	var session SessionRecord
	dir, err := s.sessionDir(jobID, sessionID, false)
	if err != nil {
		return session, err
	}
	if err := readJSONRegular(filepath.Join(dir, "session.json"), &session); err != nil {
		return session, err
	}
	if session.Version != jobRecordVersion || session.ID != sessionID || session.JobID != jobID {
		return SessionRecord{}, fmt.Errorf("invalid or unsupported session record")
	}
	return session, nil
}

// UpdateSession applies a serialized mutation and atomically replaces the record.
func (s *JobStore) UpdateSession(jobID, sessionID string, update func(*SessionRecord) error) (SessionRecord, error) {
	unlock, err := s.LockJob(jobID)
	if err != nil {
		return SessionRecord{}, err
	}
	defer unlock()
	session, err := s.GetSession(jobID, sessionID)
	if err != nil {
		return SessionRecord{}, err
	}
	createdAt := session.CreatedAt
	oldStatus := session.Status
	if err := update(&session); err != nil {
		return SessionRecord{}, err
	}
	if session.ID != sessionID || session.JobID != jobID || session.CreatedAt != createdAt {
		return SessionRecord{}, fmt.Errorf("session identity and creation time are immutable")
	}
	session.Version = jobRecordVersion
	if oldStatus != session.Status {
		setLifecycleTimes(oldStatus, session.Status, &session.StartedAt, &session.EndedAt)
	}
	session.UpdatedAt = time.Now().UTC()
	dir, err := s.sessionDir(jobID, sessionID, false)
	if err != nil {
		return SessionRecord{}, err
	}
	if s.writeSessionRecord != nil {
		if err := s.writeSessionRecord(dir, session); err != nil {
			return SessionRecord{}, err
		}
	} else if err := writeJSONAtomic(dir, "session.json", session); err != nil {
		return SessionRecord{}, err
	}
	job, err := s.GetJob(jobID)
	if err != nil {
		return SessionRecord{}, err
	}
	for i := range job.Sessions {
		if job.Sessions[i].ID == sessionID {
			job.Sessions[i] = sessionMetadata(session)
			break
		}
	}
	job.UpdatedAt = session.UpdatedAt
	jobDir, err := s.jobDir(jobID, false)
	if err != nil {
		return SessionRecord{}, err
	}
	if err := writeJSONAtomic(jobDir, "job.json", job); err != nil {
		return SessionRecord{}, err
	}
	return session, nil
}

// ListSessions returns a job's valid sessions ordered newest first.
func (s *JobStore) ListSessions(jobID string) ([]SessionRecord, error) {
	parent, err := s.sessionsDir(jobID, false)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		return nil, err
	}
	sessions := make([]SessionRecord, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || validateStoredID(entry.Name()) != nil {
			continue
		}
		session, err := s.GetSession(jobID, entry.Name())
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read session %s: %w", entry.Name(), err)
		}
		sessions = append(sessions, session)
	}
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].CreatedAt.Before(sessions[j].CreatedAt) })
	return sessions, nil
}

// SessionLogPath returns the validated path to a session's private log.
func (s *JobStore) SessionLogPath(jobID, sessionID string) (string, error) {
	dir, err := s.sessionDir(jobID, sessionID, false)
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "session.log")
	if err := ensureRegularIfExists(path); err != nil {
		return "", err
	}
	return path, nil
}

// AppendSessionEvent persists an event and updates the session timestamp under
// the per-job lock, so independent jobs never contend with one another.
func (s *JobStore) AppendSessionEvent(jobID, sessionID, eventType, message string) error {
	return s.AppendSessionEventDetails(jobID, sessionID, SessionEvent{Type: eventType, Message: message})
}

func (s *JobStore) AppendSessionEventDetails(jobID, sessionID string, event SessionEvent) error {
	unlock, err := s.LockJob(jobID)
	if err != nil {
		return err
	}
	defer unlock()
	session, err := s.GetSession(jobID, sessionID)
	if err != nil {
		return err
	}
	dir, err := s.sessionDir(jobID, sessionID, false)
	if err != nil {
		return err
	}
	event.At = time.Now().UTC()
	line, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if len(line) > maxSessionEventLineBytes {
		return fmt.Errorf("session event exceeds maximum line size of %d bytes", maxSessionEventLineBytes)
	}
	path := filepath.Join(dir, "events.jsonl")
	if err := ensureRegularIfExists(path); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	session.UpdatedAt = event.At
	if err := writeJSONAtomic(dir, "session.json", session); err != nil {
		return err
	}
	return s.updateSessionMetadataLocked(jobID, session)
}

// AppendSessionLog appends private, human-readable output to a session log.
func (s *JobStore) AppendSessionLog(jobID, sessionID string, data []byte) error {
	if s.appendSessionLog != nil {
		return s.appendSessionLog(jobID, sessionID, data)
	}
	unlock, err := s.LockJob(jobID)
	if err != nil {
		return err
	}
	defer unlock()
	dir, err := s.sessionDir(jobID, sessionID, false)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, "session.log")
	if err := ensureRegularIfExists(path); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Chmod(0o600); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	return f.Sync()
}

// SessionEvents returns the durable event history in append order.
func (s *JobStore) SessionEvents(jobID, sessionID string) ([]SessionEvent, error) {
	unlock, err := s.LockJob(jobID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	dir, err := s.sessionDir(jobID, sessionID, false)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "events.jsonl")
	if err := ensureRegularIfExists(path); err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var events []SessionEvent
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), maxSessionEventLineBytes)
	for scanner.Scan() {
		var event SessionEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("decode session event: %w", err)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

// WriteOwner atomically registers the current lifecycle owner.
func (s *JobStore) WriteOwner(id string, owner JobOwner) error {
	if owner.PID <= 0 {
		return fmt.Errorf("owner PID must be positive")
	}
	unlock, err := s.LockJob(id)
	if err != nil {
		return err
	}
	defer unlock()
	dir, err := s.jobDir(id, false)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if owner.StartedAt.IsZero() {
		owner.StartedAt = now
	}
	owner.HeartbeatAt = now
	return writeJSONAtomic(dir, "owner.json", owner)
}

// HeartbeatOwner refreshes an existing owner record without changing its PID.
func (s *JobStore) HeartbeatOwner(id string) (JobOwner, error) {
	unlock, err := s.LockJob(id)
	if err != nil {
		return JobOwner{}, err
	}
	defer unlock()
	dir, err := s.jobDir(id, false)
	if err != nil {
		return JobOwner{}, err
	}
	var owner JobOwner
	if err := readJSONRegular(filepath.Join(dir, "owner.json"), &owner); err != nil {
		return owner, err
	}
	if owner.PID <= 0 {
		return JobOwner{}, fmt.Errorf("invalid owner record")
	}
	owner.HeartbeatAt = time.Now().UTC()
	return owner, writeJSONAtomic(dir, "owner.json", owner)
}

// ReadOwner reads the current lifecycle owner record.
func (s *JobStore) ReadOwner(id string) (JobOwner, error) {
	var owner JobOwner
	dir, err := s.jobDir(id, false)
	if err != nil {
		return owner, err
	}
	if err := readJSONRegular(filepath.Join(dir, "owner.json"), &owner); err != nil {
		return owner, err
	}
	if owner.PID <= 0 {
		return JobOwner{}, fmt.Errorf("invalid owner record")
	}
	return owner, nil
}

// WriteWorker atomically registers or refreshes the associated worker record.
func (s *JobStore) WriteWorker(id string, worker WorkerRecord) error {
	if worker.PID <= 0 {
		return fmt.Errorf("worker PID must be positive")
	}
	unlock, err := s.LockJob(id)
	if err != nil {
		return err
	}
	defer unlock()
	dir, err := s.jobDir(id, false)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if worker.StartedAt.IsZero() {
		worker.StartedAt = now
	}
	worker.HeartbeatAt = now
	return writeJSONAtomic(dir, "worker.json", worker)
}

// ReadWorker reads the associated worker record.
func (s *JobStore) ReadWorker(id string) (WorkerRecord, error) {
	var worker WorkerRecord
	dir, err := s.jobDir(id, false)
	if err != nil {
		return worker, err
	}
	if err := readJSONRegular(filepath.Join(dir, "worker.json"), &worker); err != nil {
		return worker, err
	}
	if worker.PID <= 0 {
		return WorkerRecord{}, fmt.Errorf("invalid worker record")
	}
	return worker, nil
}

// HeartbeatWorker refreshes an existing worker record without changing its PID.
func (s *JobStore) HeartbeatWorker(id string) (WorkerRecord, error) {
	unlock, err := s.LockJob(id)
	if err != nil {
		return WorkerRecord{}, err
	}
	defer unlock()
	dir, err := s.jobDir(id, false)
	if err != nil {
		return WorkerRecord{}, err
	}
	var worker WorkerRecord
	if err := readJSONRegular(filepath.Join(dir, "worker.json"), &worker); err != nil {
		return worker, err
	}
	if worker.PID <= 0 {
		return WorkerRecord{}, fmt.Errorf("invalid worker record")
	}
	worker.HeartbeatAt = time.Now().UTC()
	return worker, writeJSONAtomic(dir, "worker.json", worker)
}

// ClearWorker removes a worker record if one exists.
func (s *JobStore) ClearWorker(id string) error {
	unlock, err := s.LockJob(id)
	if err != nil {
		return err
	}
	defer unlock()
	return s.clearWorkerLocked(id)
}

func (s *JobStore) clearWorkerLocked(id string) error {
	dir, err := s.jobDir(id, false)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, "worker.json")
	if err := ensureRegularIfExists(path); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(dir)
}

func (s *JobStore) jobDir(id string, create bool) (string, error) {
	if err := validateStoredID(id); err != nil {
		return "", err
	}
	dir := filepath.Join(s.root, id)
	if create {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return "", err
		}
	}
	if err := ensureRealDirectory(s.root, dir); err != nil {
		return "", err
	}
	return dir, nil
}

func (s *JobStore) sessionsDir(jobID string, create bool) (string, error) {
	jobDir, err := s.jobDir(jobID, false)
	if err != nil {
		return "", err
	}
	path := filepath.Join(jobDir, "sessions")
	if create {
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return "", err
		}
	}
	if err := ensureRealDirectory(jobDir, path); err != nil {
		return "", err
	}
	return path, nil
}

func (s *JobStore) sessionDir(jobID, sessionID string, create bool) (string, error) {
	if err := validateStoredID(sessionID); err != nil {
		return "", err
	}
	parent, err := s.sessionsDir(jobID, create)
	if err != nil {
		return "", err
	}
	dir := filepath.Join(parent, sessionID)
	if create {
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return "", err
		}
	}
	if err := ensureRealDirectory(parent, dir); err != nil {
		return "", err
	}
	return dir, nil
}

func setLifecycleTimes(oldStatus, newStatus string, startedAt, endedAt *time.Time) {
	now := time.Now().UTC()
	if oldStatus != "running" && newStatus == "running" && startedAt.IsZero() {
		*startedAt = now
	}
	if isTerminalStatus(newStatus) {
		if endedAt.IsZero() {
			*endedAt = now
		}
	} else {
		*endedAt = time.Time{}
	}
}

func isTerminalStatus(status string) bool {
	switch status {
	case "complete", "closed", "failed", "stopped", "cancelled", "interrupted":
		return true
	default:
		return false
	}
}

func (s *JobStore) updateSessionMetadataLocked(jobID string, session SessionRecord) error {
	job, err := s.GetJob(jobID)
	if err != nil {
		return err
	}
	for i := range job.Sessions {
		if job.Sessions[i].ID == session.ID {
			job.Sessions[i] = sessionMetadata(session)
			break
		}
	}
	job.UpdatedAt = session.UpdatedAt
	dir, err := s.jobDir(jobID, false)
	if err != nil {
		return err
	}
	return writeJSONAtomic(dir, "job.json", job)
}

func sessionMetadata(session SessionRecord) SessionMetadata {
	metadata := make(map[string]string, len(session.Metadata))
	for key, value := range session.Metadata {
		metadata[key] = value
	}
	return SessionMetadata{
		ID: session.ID, Type: session.Type, Status: session.Status,
		Metadata: metadata, CreatedAt: session.CreatedAt,
		StartedAt: session.StartedAt, EndedAt: session.EndedAt,
	}
}

func validateStoredID(id string) error {
	if !storedIDPattern.MatchString(id) || id == "." || id == ".." {
		return fmt.Errorf("invalid job or session id %q", id)
	}
	return nil
}

func ensureRealDirectory(parent, path string) error {
	parentPath, err := resolvedPath(parent)
	if err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("state path is not a real directory: %s", path)
	}
	resolved, err := resolvedPath(path)
	if err != nil {
		return err
	}
	if !isWithin(parentPath, resolved) || filepath.Dir(resolved) != parentPath {
		return fmt.Errorf("state path escapes its parent: %s", path)
	}
	return nil
}

func ensureRegularIfExists(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("state file is not regular: %s", path)
	}
	return nil
}

func readJSONRegular(path string, target any) error {
	if err := ensureRegularIfExists(path); err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return fmt.Errorf("unexpected trailing data in %s", filepath.Base(path))
	}
	return nil
}

func writeJSONAtomic(dir, name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".record-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
