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
	"sort"
	"strings"
	"sync"
	"time"
)

const issueObservationRecordVersion = 1

// IssueObservation is one immutable, durable snapshot associated with a work request.
type IssueObservation struct {
	DeduplicationKey string        `json:"deduplication_key"`
	Snapshot         IssueSnapshot `json:"snapshot"`
	ObservedAt       time.Time     `json:"observed_at"`
}

// IssueObservationStore records immutable issue observations and reconciles their lifecycle.
type IssueObservationStore interface {
	Record(context.Context, string, IssueSnapshot) (bool, error)
	List(context.Context, string) ([]IssueObservation, error)
	Reconcile(context.Context, string) (IssueLifecycleState, error)
}

type issueObservationRecord struct {
	RecordVersion int              `json:"record_version"`
	Observation   IssueObservation `json:"observation"`
}

const issueLifecycleRecordVersion = 1

// IssueLifecycleState is reconciled from the request's immutable observation history.
type IssueLifecycleState struct {
	DeduplicationKey string    `json:"deduplication_key"`
	Status           string    `json:"status"`
	BaselineVersion  string    `json:"baseline_version,omitempty"`
	LatestVersion    string    `json:"latest_version"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type issueLifecycleRecord struct {
	RecordVersion int                 `json:"record_version"`
	State         IssueLifecycleState `json:"state"`
}

// LocalIssueObservationStore stores immutable snapshots in private atomic JSON files.
type LocalIssueObservationStore struct {
	root string
	mu   sync.Mutex
}

var _ IssueObservationStore = (*LocalIssueObservationStore)(nil)

// NewLocalIssueObservationStore creates or opens a local observation store.
func NewLocalIssueObservationStore(root string) (*LocalIssueObservationStore, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("issue observation store root must be absolute")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create issue observation store: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("issue observation store path is not a real directory: %s", root)
	}
	root, err = resolvedPath(root)
	if err != nil {
		return nil, fmt.Errorf("resolve issue observation store: %w", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, fmt.Errorf("secure issue observation store: %w", err)
	}
	if err := ensureRealDirectory(filepath.Dir(root), root); err != nil {
		return nil, err
	}
	return &LocalIssueObservationStore{root: root}, nil
}

func (s *LocalIssueObservationStore) Record(ctx context.Context, deduplicationKey string, snapshot IssueSnapshot) (bool, error) {
	if err := validateWorkDeduplicationKey(deduplicationKey); err != nil {
		return false, err
	}
	if err := checkWorkContext(ctx); err != nil {
		return false, err
	}
	if strings.TrimSpace(snapshot.Version) == "" {
		return false, fmt.Errorf("issue snapshot version must not be empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := checkWorkContext(ctx); err != nil {
		return false, err
	}
	if err := ensureRealDirectory(filepath.Dir(s.root), s.root); err != nil {
		return false, err
	}
	if err := ensureRegularIfExists(filepath.Join(s.root, "observations.lock")); err != nil {
		return false, err
	}
	lock, err := acquireFileLock(filepath.Join(s.root, "observations.lock"), 30*time.Second)
	if err != nil {
		return false, err
	}
	defer releaseFileLock(lock)

	observation := IssueObservation{DeduplicationKey: deduplicationKey, Snapshot: snapshot, ObservedAt: time.Now().UTC()}
	path := s.recordPath(deduplicationKey, snapshot.Version)
	if err := ensureRegularIfExists(path); err != nil {
		return false, err
	}
	if _, err := os.Lstat(path); err == nil {
		existing, err := readIssueObservationRecord(path)
		if err != nil {
			return false, err
		}
		if existing.Observation.DeduplicationKey != deduplicationKey || existing.Observation.Snapshot.Version != snapshot.Version {
			return false, fmt.Errorf("issue observation record key does not match its filename")
		}
		if !sameIssueSnapshot(existing.Observation.Snapshot, snapshot) {
			return false, fmt.Errorf("issue snapshot version conflicts with recorded observation")
		}
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	record := issueObservationRecord{RecordVersion: issueObservationRecordVersion, Observation: observation}
	if err := writeJSONAtomic(s.root, filepath.Base(path), record); err != nil {
		return false, fmt.Errorf("record issue observation: %w", err)
	}
	return true, nil
}

func (s *LocalIssueObservationStore) List(ctx context.Context, deduplicationKey string) ([]IssueObservation, error) {
	if err := validateWorkDeduplicationKey(deduplicationKey); err != nil {
		return nil, err
	}
	if err := checkWorkContext(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ensureRealDirectory(filepath.Dir(s.root), s.root); err != nil {
		return nil, err
	}
	return s.listLocked(ctx, deduplicationKey)
}

func (s *LocalIssueObservationStore) listLocked(ctx context.Context, deduplicationKey string) ([]IssueObservation, error) {
	if err := checkWorkContext(ctx); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, err
	}
	observations := make([]IssueObservation, 0)
	for _, entry := range entries {
		if entry.Name() == "observations.lock" {
			if err := ensureRegularIfExists(filepath.Join(s.root, entry.Name())); err != nil {
				return nil, err
			}
			continue
		}
		if strings.HasPrefix(entry.Name(), "lifecycle-") && strings.HasSuffix(entry.Name(), ".json") {
			if err := ensureRegularIfExists(filepath.Join(s.root, entry.Name())); err != nil {
				return nil, err
			}
			continue
		}
		if strings.HasPrefix(entry.Name(), ".record-") && strings.HasSuffix(entry.Name(), ".tmp") {
			if err := ensureRegularIfExists(filepath.Join(s.root, entry.Name())); err != nil {
				return nil, err
			}
			continue
		}
		if !workRecordNamePattern.MatchString(entry.Name()) {
			return nil, fmt.Errorf("unexpected issue observation record %q", entry.Name())
		}
		path := filepath.Join(s.root, entry.Name())
		if err := ensureRegularIfExists(path); err != nil {
			return nil, err
		}
		record, err := readIssueObservationRecord(path)
		if err != nil {
			return nil, err
		}
		if s.recordName(record.Observation.DeduplicationKey, record.Observation.Snapshot.Version) != entry.Name() {
			return nil, fmt.Errorf("issue observation record key does not match its filename: %s", entry.Name())
		}
		if record.Observation.DeduplicationKey == deduplicationKey {
			observations = append(observations, record.Observation)
		}
	}
	sort.Slice(observations, func(i, j int) bool {
		if observations[i].Snapshot.UpdatedAt.Equal(observations[j].Snapshot.UpdatedAt) {
			return observations[i].Snapshot.Version < observations[j].Snapshot.Version
		}
		return observations[i].Snapshot.UpdatedAt.Before(observations[j].Snapshot.UpdatedAt)
	})
	return observations, nil
}

// Reconcile derives lifecycle state from durable observations and repairs a missing or stale state record.
// Observation and lifecycle persistence share a lock so concurrent watches cannot regress state.
func (s *LocalIssueObservationStore) Reconcile(ctx context.Context, deduplicationKey string) (IssueLifecycleState, error) {
	if err := validateWorkDeduplicationKey(deduplicationKey); err != nil {
		return IssueLifecycleState{}, err
	}
	if err := checkWorkContext(ctx); err != nil {
		return IssueLifecycleState{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ensureRealDirectory(filepath.Dir(s.root), s.root); err != nil {
		return IssueLifecycleState{}, err
	}
	lockPath := filepath.Join(s.root, "observations.lock")
	if err := ensureRegularIfExists(lockPath); err != nil {
		return IssueLifecycleState{}, err
	}
	lock, err := acquireFileLock(lockPath, 30*time.Second)
	if err != nil {
		return IssueLifecycleState{}, err
	}
	defer releaseFileLock(lock)

	path := s.lifecyclePath(deduplicationKey)
	if err := ensureRegularIfExists(path); err != nil {
		return IssueLifecycleState{}, err
	}
	var existing *IssueLifecycleState
	if _, err := os.Lstat(path); err == nil {
		record, err := readIssueLifecycleRecord(path)
		if err != nil {
			return IssueLifecycleState{}, err
		}
		if record.State.DeduplicationKey != deduplicationKey {
			return IssueLifecycleState{}, fmt.Errorf("issue lifecycle state key does not match its filename")
		}
		existing = &record.State
	} else if !errors.Is(err, os.ErrNotExist) {
		return IssueLifecycleState{}, err
	}
	observations, err := s.listLocked(ctx, deduplicationKey)
	if err != nil {
		return IssueLifecycleState{}, err
	}
	if len(observations) == 0 {
		return IssueLifecycleState{}, fmt.Errorf("cannot reconcile issue lifecycle without observations")
	}
	sort.Slice(observations, func(i, j int) bool {
		if observations[i].ObservedAt.Equal(observations[j].ObservedAt) {
			return observations[i].Snapshot.Version < observations[j].Snapshot.Version
		}
		return observations[i].ObservedAt.Before(observations[j].ObservedAt)
	})
	state := IssueLifecycleState{DeduplicationKey: deduplicationKey, Status: "baseline", LatestVersion: observations[0].Snapshot.Version}
	openVersions := make(map[string]bool)
	closed := false
	for _, observation := range observations {
		state.LatestVersion = observation.Snapshot.Version
		if observation.Snapshot.State != "open" && observation.Snapshot.State != "closed" {
			return IssueLifecycleState{}, fmt.Errorf("cannot reconcile unsupported issue snapshot state %q", observation.Snapshot.State)
		}
		if observation.Snapshot.State == "closed" {
			closed = true
		}
		if observation.Snapshot.State == "open" && !openVersions[observation.Snapshot.Version] {
			openVersions[observation.Snapshot.Version] = true
			if state.BaselineVersion == "" {
				state.BaselineVersion = observation.Snapshot.Version
			} else {
				state.Status = "waiting_for_human"
			}
		}
	}
	if closed {
		state.Status = "stopped"
	}
	if state.Status == "baseline" && state.BaselineVersion == "" {
		state.Status = "stopped"
	}
	if existing != nil && existing.Status == state.Status && existing.BaselineVersion == state.BaselineVersion && existing.LatestVersion == state.LatestVersion {
		return *existing, nil
	}
	state.UpdatedAt = time.Now().UTC()
	if err := writeJSONAtomic(s.root, filepath.Base(path), issueLifecycleRecord{RecordVersion: issueLifecycleRecordVersion, State: state}); err != nil {
		return IssueLifecycleState{}, fmt.Errorf("persist issue lifecycle state: %w", err)
	}
	return state, nil
}

func (s *LocalIssueObservationStore) lifecyclePath(deduplicationKey string) string {
	sum := sha256.Sum256([]byte(deduplicationKey))
	return filepath.Join(s.root, "lifecycle-"+hex.EncodeToString(sum[:])+".json")
}

func readIssueLifecycleRecord(path string) (issueLifecycleRecord, error) {
	var record issueLifecycleRecord
	if err := ensureRegularIfExists(path); err != nil {
		return record, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return record, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return record, fmt.Errorf("invalid issue lifecycle state %s: %w", filepath.Base(path), err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return record, fmt.Errorf("invalid issue lifecycle state %s: unexpected trailing data", filepath.Base(path))
	}
	state := record.State
	if record.RecordVersion != issueLifecycleRecordVersion || validateWorkDeduplicationKey(state.DeduplicationKey) != nil || state.UpdatedAt.IsZero() || state.LatestVersion == "" || (state.Status != "baseline" && state.Status != "waiting_for_human" && state.Status != "stopped") {
		return record, fmt.Errorf("invalid issue lifecycle state %s", filepath.Base(path))
	}
	return record, nil
}

func sameIssueSnapshot(a, b IssueSnapshot) bool {
	return a.Repository == b.Repository && a.Number == b.Number && a.Title == b.Title && a.Body == b.Body &&
		a.State == b.State && a.URL == b.URL && a.UpdatedAt.Equal(b.UpdatedAt) && a.Version == b.Version
}

func (s *LocalIssueObservationStore) recordPath(deduplicationKey, version string) string {
	return filepath.Join(s.root, s.recordName(deduplicationKey, version))
}

func (s *LocalIssueObservationStore) recordName(deduplicationKey, version string) string {
	sum := sha256.Sum256([]byte(deduplicationKey + "\x00" + version))
	return hex.EncodeToString(sum[:]) + ".json"
}

func readIssueObservationRecord(path string) (issueObservationRecord, error) {
	var record issueObservationRecord
	if err := ensureRegularIfExists(path); err != nil {
		return record, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return record, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return record, fmt.Errorf("invalid issue observation record %s: %w", filepath.Base(path), err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return record, fmt.Errorf("invalid issue observation record %s: unexpected trailing data", filepath.Base(path))
	}
	if record.RecordVersion != issueObservationRecordVersion || validateWorkDeduplicationKey(record.Observation.DeduplicationKey) != nil || strings.TrimSpace(record.Observation.Snapshot.Version) == "" || record.Observation.ObservedAt.IsZero() {
		return record, fmt.Errorf("invalid issue observation record %s", filepath.Base(path))
	}
	return record, nil
}
