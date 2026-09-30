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
	"unicode/utf8"
)

const (
	issueObservationRecordVersion = 1
	humanDirectionRecordVersion   = 1
	MaxHumanDirectionBytes        = 16 * 1024
)

// IssueObservation is one immutable, durable snapshot associated with a work request.
type IssueObservation struct {
	DeduplicationKey string        `json:"deduplication_key"`
	Snapshot         IssueSnapshot `json:"snapshot"`
	ObservedAt       time.Time     `json:"observed_at"`
}

// HumanDirection is an immutable instruction pinned to one issue snapshot version.
type HumanDirection struct {
	DeduplicationKey string    `json:"deduplication_key"`
	IssueVersion     string    `json:"issue_version"`
	Instruction      string    `json:"instruction"`
	RecordedAt       time.Time `json:"recorded_at"`
}

// IssueObservationStore persists issue observations, reconciled lifecycle, and version-pinned human directions.
type IssueObservationStore interface {
	Record(context.Context, string, IssueSnapshot) (bool, error)
	List(context.Context, string) ([]IssueObservation, error)
	Reconcile(context.Context, string) (IssueLifecycleState, error)
	RecordDirection(context.Context, string, string, string) (HumanDirection, bool, error)
	ListDirections(context.Context, string) ([]HumanDirection, error)
}

type humanDirectionRecord struct {
	RecordVersion int            `json:"record_version"`
	Direction     HumanDirection `json:"direction"`
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
	observations, err := s.listLocked(ctx, deduplicationKey)
	if err != nil {
		return false, err
	}
	observedAt := time.Now().UTC()
	var latestObservedAt time.Time
	for _, existing := range observations {
		if existing.ObservedAt.After(latestObservedAt) {
			latestObservedAt = existing.ObservedAt
		}
	}
	if !observedAt.After(latestObservedAt) {
		observedAt = latestObservedAt.Add(time.Nanosecond)
	}
	observation := IssueObservation{DeduplicationKey: deduplicationKey, Snapshot: snapshot, ObservedAt: observedAt}
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

func (s *LocalIssueObservationStore) RecordDirection(ctx context.Context, deduplicationKey, issueVersion, instruction string) (HumanDirection, bool, error) {
	if err := validateWorkDeduplicationKey(deduplicationKey); err != nil {
		return HumanDirection{}, false, err
	}
	if strings.TrimSpace(issueVersion) == "" {
		return HumanDirection{}, false, fmt.Errorf("issue version must not be empty")
	}
	if strings.TrimSpace(instruction) == "" {
		return HumanDirection{}, false, fmt.Errorf("instruction must not be empty")
	}
	if !utf8.ValidString(instruction) {
		return HumanDirection{}, false, fmt.Errorf("instruction must be valid UTF-8")
	}
	if len([]byte(instruction)) > MaxHumanDirectionBytes {
		return HumanDirection{}, false, fmt.Errorf("instruction exceeds %d bytes", MaxHumanDirectionBytes)
	}
	if err := checkWorkContext(ctx); err != nil {
		return HumanDirection{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ensureRealDirectory(filepath.Dir(s.root), s.root); err != nil {
		return HumanDirection{}, false, err
	}
	lockPath := filepath.Join(s.root, "observations.lock")
	if err := ensureRegularIfExists(lockPath); err != nil {
		return HumanDirection{}, false, err
	}
	lock, err := acquireFileLock(lockPath, 30*time.Second)
	if err != nil {
		return HumanDirection{}, false, err
	}
	defer releaseFileLock(lock)
	if err := checkWorkContext(ctx); err != nil {
		return HumanDirection{}, false, err
	}

	path := s.directionPath(deduplicationKey, issueVersion)
	if err := ensureRegularIfExists(path); err != nil {
		return HumanDirection{}, false, err
	}
	if _, err := os.Lstat(path); err == nil {
		record, err := readHumanDirectionRecord(path)
		if err != nil {
			return HumanDirection{}, false, err
		}
		if record.Direction.DeduplicationKey != deduplicationKey || record.Direction.IssueVersion != issueVersion {
			return HumanDirection{}, false, fmt.Errorf("human direction record key does not match its filename")
		}
		if record.Direction.Instruction != instruction {
			return HumanDirection{}, false, fmt.Errorf("a different human direction is already recorded for issue version %q", issueVersion)
		}
		return record.Direction, false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return HumanDirection{}, false, err
	}

	observations, err := s.listLocked(ctx, deduplicationKey)
	if err != nil {
		return HumanDirection{}, false, err
	}
	if len(observations) == 0 {
		return HumanDirection{}, false, fmt.Errorf("cannot record direction without issue observations")
	}
	sort.Slice(observations, func(i, j int) bool {
		if observations[i].ObservedAt.Equal(observations[j].ObservedAt) {
			return observations[i].Snapshot.Version < observations[j].Snapshot.Version
		}
		return observations[i].ObservedAt.Before(observations[j].ObservedAt)
	})
	latest := observations[len(observations)-1]
	if latest.Snapshot.State != "open" || latest.Snapshot.Version != issueVersion {
		return HumanDirection{}, false, fmt.Errorf("issue version is not the latest open observation")
	}
	lifecyclePath := s.lifecyclePath(deduplicationKey)
	if err := ensureRegularIfExists(lifecyclePath); err != nil {
		return HumanDirection{}, false, err
	}
	lifecycle, err := readIssueLifecycleRecord(lifecyclePath)
	if err != nil {
		return HumanDirection{}, false, fmt.Errorf("read reconciled issue lifecycle: %w", err)
	}
	if lifecycle.State.DeduplicationKey != deduplicationKey || lifecycle.State.Status != "waiting_for_human" || lifecycle.State.LatestVersion != issueVersion {
		return HumanDirection{}, false, fmt.Errorf("issue is not waiting for human direction at version %q", issueVersion)
	}

	directions, err := s.listDirectionsLocked(ctx, deduplicationKey)
	if err != nil {
		return HumanDirection{}, false, err
	}
	recordedAt := time.Now().UTC()
	for _, existing := range directions {
		if !recordedAt.After(existing.RecordedAt) {
			recordedAt = existing.RecordedAt.Add(time.Nanosecond)
		}
	}
	direction := HumanDirection{DeduplicationKey: deduplicationKey, IssueVersion: issueVersion, Instruction: instruction, RecordedAt: recordedAt}
	if err := writeJSONAtomic(s.root, filepath.Base(path), humanDirectionRecord{RecordVersion: humanDirectionRecordVersion, Direction: direction}); err != nil {
		return HumanDirection{}, false, fmt.Errorf("record human direction: %w", err)
	}
	return direction, true, nil
}

func (s *LocalIssueObservationStore) ListDirections(ctx context.Context, deduplicationKey string) ([]HumanDirection, error) {
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
	if err := ensureRegularIfExists(filepath.Join(s.root, "observations.lock")); err != nil {
		return nil, err
	}
	lock, err := acquireFileLock(filepath.Join(s.root, "observations.lock"), 30*time.Second)
	if err != nil {
		return nil, err
	}
	defer releaseFileLock(lock)
	return s.listDirectionsLocked(ctx, deduplicationKey)
}

func (s *LocalIssueObservationStore) listDirectionsLocked(ctx context.Context, deduplicationKey string) ([]HumanDirection, error) {
	if err := checkWorkContext(ctx); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, err
	}
	directions := make([]HumanDirection, 0)
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "direction-") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(s.root, entry.Name())
		if err := ensureRegularIfExists(path); err != nil {
			return nil, err
		}
		record, err := readHumanDirectionRecord(path)
		if err != nil {
			return nil, err
		}
		direction := record.Direction
		if s.directionName(direction.DeduplicationKey, direction.IssueVersion) != entry.Name() {
			return nil, fmt.Errorf("human direction record key does not match its filename: %s", entry.Name())
		}
		if direction.DeduplicationKey == deduplicationKey {
			directions = append(directions, direction)
		}
	}
	sort.Slice(directions, func(i, j int) bool {
		if directions[i].RecordedAt.Equal(directions[j].RecordedAt) {
			return directions[i].IssueVersion < directions[j].IssueVersion
		}
		return directions[i].RecordedAt.Before(directions[j].RecordedAt)
	})
	return directions, nil
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
		if strings.HasPrefix(entry.Name(), "direction-") && strings.HasSuffix(entry.Name(), ".json") {
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

func (s *LocalIssueObservationStore) directionPath(deduplicationKey, issueVersion string) string {
	return filepath.Join(s.root, s.directionName(deduplicationKey, issueVersion))
}

func (s *LocalIssueObservationStore) directionName(deduplicationKey, issueVersion string) string {
	sum := sha256.Sum256([]byte(deduplicationKey + "\x00" + issueVersion))
	return "direction-" + hex.EncodeToString(sum[:]) + ".json"
}

func readHumanDirectionRecord(path string) (humanDirectionRecord, error) {
	var record humanDirectionRecord
	if err := ensureRegularIfExists(path); err != nil {
		return record, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return record, err
	}
	if !utf8.Valid(data) {
		return record, fmt.Errorf("invalid human direction record %s: record is not valid UTF-8", filepath.Base(path))
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return record, fmt.Errorf("invalid human direction record %s: %w", filepath.Base(path), err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return record, fmt.Errorf("invalid human direction record %s: unexpected trailing data", filepath.Base(path))
	}
	direction := record.Direction
	if record.RecordVersion != humanDirectionRecordVersion || validateWorkDeduplicationKey(direction.DeduplicationKey) != nil || strings.TrimSpace(direction.IssueVersion) == "" || strings.TrimSpace(direction.Instruction) == "" || !utf8.ValidString(direction.Instruction) || len([]byte(direction.Instruction)) > MaxHumanDirectionBytes || direction.RecordedAt.IsZero() {
		return record, fmt.Errorf("invalid human direction record %s", filepath.Base(path))
	}
	return record, nil
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
