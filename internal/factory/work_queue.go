package factory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

const workQueueRecordVersion = 1

var (
	ErrWorkRequestConflict = errors.New("work request deduplication key conflicts with existing payload")
	ErrNoWorkAvailable     = errors.New("no work request available")
	ErrStaleWorkClaim      = errors.New("work claim is stale or not owned by this queue")
	workDedupKeyPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$`)
	workProviderPattern    = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
	workRecordNamePattern  = regexp.MustCompile(`^[0-9a-f]{64}\.json$`)
)

// WorkRequest is the provider-neutral identity of one bounded unit of work.
// Credentials and provider response payloads do not belong in this record.
type WorkRequest struct {
	TrackerProvider  string `json:"tracker_provider"`
	IssueID          string `json:"issue_id"`
	CodeHostProvider string `json:"code_host_provider"`
	Repository       string `json:"repository"`
	DeduplicationKey string `json:"deduplication_key"`
}

// WorkItem is the durable queue view of a request.
type WorkItem struct {
	Request         WorkRequest `json:"request"`
	State           string      `json:"state"`
	ClaimGeneration uint64      `json:"claim_generation"`
	CreatedAt       time.Time   `json:"created_at"`
}

// WorkClaim is a fencing token returned to the current owner of a request.
type WorkClaim struct {
	Request    WorkRequest
	Generation uint64
}

// WorkQueue is the injectable contract used by future admission and worker code.
type WorkQueue interface {
	Enqueue(context.Context, WorkRequest) error
	Get(context.Context, string) (WorkItem, error)
	List(context.Context) ([]WorkItem, error)
	Claim(context.Context) (WorkClaim, error)
	Acknowledge(context.Context, WorkClaim) error
	Release(context.Context, WorkClaim) error
}

type workQueueRecord struct {
	Version         int         `json:"version"`
	Request         WorkRequest `json:"request"`
	State           string      `json:"state"`
	ClaimGeneration uint64      `json:"claim_generation"`
	CreatedAt       time.Time   `json:"created_at"`
}

// LocalWorkQueue is a durable, single-host queue backed by atomic JSON records
// and OS file locks. Close releases claims held by this process; a subsequent
// claimant obtains a higher generation, fencing any prior claim token.
type LocalWorkQueue struct {
	root   string
	mu     sync.Mutex
	closed bool
	claims map[string]func()
}

var _ WorkQueue = (*LocalWorkQueue)(nil)

// NewLocalWorkQueue creates or opens a single-host work queue at an absolute path.
func NewLocalWorkQueue(root string) (*LocalWorkQueue, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("work queue root must be absolute")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create work queue root: %w", err)
	}
	root, err := resolvedPath(root)
	if err != nil {
		return nil, fmt.Errorf("resolve work queue root: %w", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, fmt.Errorf("secure work queue root: %w", err)
	}
	for _, name := range []string{"records", "claims", "locks"} {
		path := filepath.Join(root, name)
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("create work queue %s directory: %w", name, err)
		}
		if err := ensureRealDirectory(root, path); err != nil {
			return nil, err
		}
	}
	return &LocalWorkQueue{root: root, claims: make(map[string]func())}, nil
}

// Close releases this process's outstanding claims. Persisted claimed items are
// recoverable by the next claimant, which must use a new fencing generation.
func (q *LocalWorkQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	for key, unlock := range q.claims {
		unlock()
		delete(q.claims, key)
	}
	return nil
}

func (q *LocalWorkQueue) Enqueue(ctx context.Context, request WorkRequest) error {
	if err := validateWorkRequest(request); err != nil {
		return err
	}
	if err := checkWorkContext(ctx); err != nil {
		return err
	}
	unlock, err := q.lockQueue()
	if err != nil {
		return err
	}
	defer unlock()
	if err := q.ensureQueueDir("records"); err != nil {
		return err
	}
	path := q.recordPath(request.DeduplicationKey)
	record, err := readWorkQueueRecord(path, request.DeduplicationKey)
	if err == nil {
		if record.Request != request {
			return ErrWorkRequestConflict
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	record = workQueueRecord{Version: workQueueRecordVersion, Request: request, State: "queued", CreatedAt: time.Now().UTC()}
	return writeWorkQueueRecord(path, record)
}

func (q *LocalWorkQueue) Get(ctx context.Context, deduplicationKey string) (WorkItem, error) {
	if err := validateWorkDeduplicationKey(deduplicationKey); err != nil {
		return WorkItem{}, err
	}
	if err := checkWorkContext(ctx); err != nil {
		return WorkItem{}, err
	}
	unlock, err := q.lockQueue()
	if err != nil {
		return WorkItem{}, err
	}
	defer unlock()
	if err := q.ensureQueueDir("records"); err != nil {
		return WorkItem{}, err
	}
	record, err := readWorkQueueRecord(q.recordPath(deduplicationKey), deduplicationKey)
	if err != nil {
		return WorkItem{}, err
	}
	return workQueueItem(record), nil
}

func (q *LocalWorkQueue) List(ctx context.Context) ([]WorkItem, error) {
	if err := checkWorkContext(ctx); err != nil {
		return nil, err
	}
	unlock, err := q.lockQueue()
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := q.ensureQueueDir("records"); err != nil {
		return nil, err
	}
	dir := filepath.Join(q.root, "records")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	items := make([]WorkItem, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".record-") && strings.HasSuffix(name, ".tmp") {
			if err := ensureRegularIfExists(filepath.Join(dir, name)); err != nil {
				return nil, err
			}
			continue
		}
		if !workRecordNamePattern.MatchString(name) {
			return nil, fmt.Errorf("unexpected work queue record %q", name)
		}
		key := ""
		itemPath := filepath.Join(dir, name)
		record, err := readWorkQueueRecordByName(itemPath, name)
		if err != nil {
			return nil, err
		}
		key = record.Request.DeduplicationKey
		if q.recordName(key) != name {
			return nil, fmt.Errorf("work queue record key does not match its filename: %s", name)
		}
		items = append(items, workQueueItem(record))
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].Request.DeduplicationKey < items[j].Request.DeduplicationKey
		}
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})
	return items, nil
}

func (q *LocalWorkQueue) Claim(ctx context.Context) (WorkClaim, error) {
	if err := checkWorkContext(ctx); err != nil {
		return WorkClaim{}, err
	}
	if err := q.ensureOpen(); err != nil {
		return WorkClaim{}, err
	}
	unlock, err := q.lockQueue()
	if err != nil {
		return WorkClaim{}, err
	}
	defer unlock()
	if err := q.ensureQueueDir("records"); err != nil {
		return WorkClaim{}, err
	}
	entries, err := os.ReadDir(filepath.Join(q.root, "records"))
	if err != nil {
		return WorkClaim{}, err
	}
	var records []workQueueRecord
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".record-") && strings.HasSuffix(name, ".tmp") {
			if err := ensureRegularIfExists(filepath.Join(q.root, "records", name)); err != nil {
				return WorkClaim{}, err
			}
			continue
		}
		if !workRecordNamePattern.MatchString(name) {
			return WorkClaim{}, fmt.Errorf("unexpected work queue record %q", name)
		}
		record, err := readWorkQueueRecordByName(filepath.Join(q.root, "records", name), name)
		if err != nil {
			return WorkClaim{}, err
		}
		if q.recordName(record.Request.DeduplicationKey) != name {
			return WorkClaim{}, fmt.Errorf("work queue record key does not match its filename: %s", name)
		}
		if record.State == "queued" || record.State == "claimed" {
			records = append(records, record)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].Request.DeduplicationKey < records[j].Request.DeduplicationKey
		}
		return records[i].CreatedAt.Before(records[j].CreatedAt)
	})
	for _, record := range records {
		if err := checkWorkContext(ctx); err != nil {
			return WorkClaim{}, err
		}
		claimUnlock, acquired, err := q.tryClaimLock(record.Request.DeduplicationKey)
		if err != nil {
			return WorkClaim{}, err
		}
		if !acquired {
			continue
		}
		record.ClaimGeneration++
		if record.ClaimGeneration == 0 {
			claimUnlock()
			return WorkClaim{}, fmt.Errorf("work claim generation exhausted")
		}
		record.State = "claimed"
		if err := writeWorkQueueRecord(q.recordPath(record.Request.DeduplicationKey), record); err != nil {
			claimUnlock()
			return WorkClaim{}, err
		}
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			claimUnlock()
			return WorkClaim{}, fmt.Errorf("work queue is closed")
		}
		q.claims[workClaimMapKey(record.Request.DeduplicationKey, record.ClaimGeneration)] = claimUnlock
		q.mu.Unlock()
		return WorkClaim{Request: record.Request, Generation: record.ClaimGeneration}, nil
	}
	return WorkClaim{}, ErrNoWorkAvailable
}

func (q *LocalWorkQueue) Acknowledge(ctx context.Context, claim WorkClaim) error {
	return q.finishClaim(ctx, claim, "acknowledged")
}

func (q *LocalWorkQueue) Release(ctx context.Context, claim WorkClaim) error {
	return q.finishClaim(ctx, claim, "queued")
}

func (q *LocalWorkQueue) finishClaim(ctx context.Context, claim WorkClaim, state string) error {
	if err := checkWorkContext(ctx); err != nil {
		return err
	}
	if err := validateWorkRequest(claim.Request); err != nil || claim.Generation == 0 {
		return ErrStaleWorkClaim
	}
	unlock, err := q.lockQueue()
	if err != nil {
		return err
	}
	defer unlock()
	q.mu.Lock()
	defer q.mu.Unlock()
	claimKey := workClaimMapKey(claim.Request.DeduplicationKey, claim.Generation)
	claimUnlock, ownsClaim := q.claims[claimKey]
	if !ownsClaim {
		return ErrStaleWorkClaim
	}
	record, err := readWorkQueueRecord(q.recordPath(claim.Request.DeduplicationKey), claim.Request.DeduplicationKey)
	if err != nil {
		return err
	}
	if record.State != "claimed" || record.ClaimGeneration != claim.Generation || record.Request != claim.Request {
		return ErrStaleWorkClaim
	}
	record.State = state
	if err := writeWorkQueueRecord(q.recordPath(claim.Request.DeduplicationKey), record); err != nil {
		return err
	}
	delete(q.claims, claimKey)
	claimUnlock()
	return nil
}

func (q *LocalWorkQueue) lockQueue() (func(), error) {
	if err := q.ensureOpen(); err != nil {
		return nil, err
	}
	if err := q.ensureQueueDir("locks"); err != nil {
		return nil, err
	}
	path := filepath.Join(q.root, "locks", "queue.lock")
	if err := ensureRegularIfExists(path); err != nil {
		return nil, err
	}
	return q.lockNamed(path, false)
}

func (q *LocalWorkQueue) tryClaimLock(key string) (func(), bool, error) {
	if err := q.ensureQueueDir("claims"); err != nil {
		return nil, false, err
	}
	path := filepath.Join(q.root, "claims", q.recordName(key)+".lock")
	if err := ensureRegularIfExists(path); err != nil {
		return nil, false, err
	}
	unlock, err := q.lockNamed(path, true)
	if errors.Is(err, errFileLockBusy) {
		return nil, false, nil
	}
	return unlock, err == nil, err
}

func (q *LocalWorkQueue) lockNamed(path string, try bool) (func(), error) {
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

func (q *LocalWorkQueue) ensureOpen() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return fmt.Errorf("work queue is closed")
	}
	return nil
}

func (q *LocalWorkQueue) ensureQueueDir(name string) error {
	if name != "records" && name != "claims" && name != "locks" {
		return fmt.Errorf("invalid work queue directory %q", name)
	}
	return ensureRealDirectory(q.root, filepath.Join(q.root, name))
}

func (q *LocalWorkQueue) recordPath(key string) string {
	return filepath.Join(q.root, "records", q.recordName(key))
}

func (q *LocalWorkQueue) recordName(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:]) + ".json"
}

func validateWorkRequest(request WorkRequest) error {
	if !workProviderPattern.MatchString(request.TrackerProvider) {
		return fmt.Errorf("invalid work request tracker provider")
	}
	if err := validateWorkLabel("issue ID", request.IssueID, 256); err != nil {
		return err
	}
	if !workProviderPattern.MatchString(request.CodeHostProvider) {
		return fmt.Errorf("invalid work request code host provider")
	}
	if err := validateWorkLabel("repository", request.Repository, 512); err != nil {
		return err
	}
	return validateWorkDeduplicationKey(request.DeduplicationKey)
}

func validateWorkLabel(label, value string, max int) error {
	if strings.TrimSpace(value) == "" || len(value) > max {
		return fmt.Errorf("invalid work request %s", label)
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return fmt.Errorf("invalid work request %s", label)
		}
	}
	return nil
}

func validateWorkDeduplicationKey(key string) error {
	if !workDedupKeyPattern.MatchString(key) {
		return fmt.Errorf("invalid work request deduplication key %q", key)
	}
	return nil
}

func checkWorkContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("work queue context must not be nil")
	}
	return ctx.Err()
}

func readWorkQueueRecord(path, expectedKey string) (workQueueRecord, error) {
	var record workQueueRecord
	if err := ensureRegularIfExists(path); err != nil {
		return record, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return record, err
	}
	if err := decodeWorkQueueRecord(data, &record); err != nil {
		return record, fmt.Errorf("invalid work queue record %s: %w", filepath.Base(path), err)
	}
	if err := validateWorkQueueRecord(record); err != nil {
		return record, fmt.Errorf("invalid work queue record %s: %w", filepath.Base(path), err)
	}
	if expectedKey != "" && record.Request.DeduplicationKey != expectedKey {
		return record, fmt.Errorf("work queue record key does not match its filename")
	}
	return record, nil
}

func readWorkQueueRecordByName(path, name string) (workQueueRecord, error) {
	if !workRecordNamePattern.MatchString(name) {
		return workQueueRecord{}, fmt.Errorf("invalid work queue record filename %q", name)
	}
	return readWorkQueueRecord(path, "")
}

func decodeWorkQueueRecord(data []byte, record *workQueueRecord) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(record); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return fmt.Errorf("unexpected trailing record data")
	}
	return nil
}

func validateWorkQueueRecord(record workQueueRecord) error {
	if record.Version != workQueueRecordVersion {
		return fmt.Errorf("unsupported record version %d", record.Version)
	}
	if err := validateWorkRequest(record.Request); err != nil {
		return err
	}
	if record.CreatedAt.IsZero() {
		return fmt.Errorf("record has no creation time")
	}
	switch record.State {
	case "queued":
	case "claimed":
		if record.ClaimGeneration == 0 {
			return fmt.Errorf("claimed record has no claim generation")
		}
	case "acknowledged":
		if record.ClaimGeneration == 0 {
			return fmt.Errorf("acknowledged record has no claim generation")
		}
	default:
		return fmt.Errorf("invalid work queue state %q", record.State)
	}
	return nil
}

func writeWorkQueueRecord(path string, record workQueueRecord) error {
	if err := validateWorkQueueRecord(record); err != nil {
		return err
	}
	if err := ensureRegularIfExists(path); err != nil {
		return err
	}
	return writeJSONAtomic(filepath.Dir(path), filepath.Base(path), record)
}

func workClaimMapKey(key string, generation uint64) string {
	return key + "\x00" + fmt.Sprint(generation)
}

func workQueueItem(record workQueueRecord) WorkItem {
	return WorkItem{Request: record.Request, State: record.State, ClaimGeneration: record.ClaimGeneration, CreatedAt: record.CreatedAt}
}
