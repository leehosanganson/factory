// Package restjobs contains the process-local REST job admission and inspection
// model. It does not execute jobs or persist records.
package restjobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

var (
	ErrInvalidInput        = errors.New("invalid job input")
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with an existing request")
	ErrQueueFull           = errors.New("job queue is full")
	ErrRegistryFull        = errors.New("job registry is full")
	ErrNotFound            = errors.New("job not found")
	ErrInvalidTransition   = errors.New("invalid job status transition")
	ErrNoQueuedJobs        = errors.New("no queued jobs")
	ErrNoWorkerSlots       = errors.New("no worker slots available")
	ErrManagerClosed       = errors.New("job manager is closed")
)

const (
	maxRecordsLimit = 100_000
	maxQueueLimit   = 100_000
	maxEventsLimit  = 10_000
	maxKeyBytes     = 256
	maxAliasBytes   = 256
	maxTaskBytes    = 65_536
	maxEventBytes   = 4_096
)

// Status is a stable lifecycle value suitable for JSON serialization.
type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusCanceled  Status = "canceled"
)

// Terminal reports whether a status represents completed work.
func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusCanceled
}

// Request is the only job payload accepted by the manager. It deliberately
// contains no command, executable, or execution-mode field.
type Request struct {
	Repository string `json:"repository"`
	Task       string `json:"task"`
	Issue      *int   `json:"issue,omitempty"`
}

// Config contains operator-selected process-local capacity and payload bounds.
// A zero MaxTaskBytes uses the default of 65,536 bytes.
type Config struct {
	QueueCapacity     int
	MaxConcurrentJobs int
	MaxRecords        int
	MaxEventsPerJob   int
	MaxTaskBytes      int
}

// Snapshot is a copy of the public job state and is safe to serialize after the
// manager lock has been released.
type Snapshot struct {
	ID        string    `json:"id"`
	Request   Request   `json:"request"`
	Status    Status    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Event is one bounded history entry.
type Event struct {
	At      time.Time `json:"at"`
	Type    string    `json:"type"`
	Message string    `json:"message,omitempty"`
}

// History is a bounded copy of one job's retained event stream.
type History struct {
	JobID     string  `json:"job_id"`
	Events    []Event `json:"events"`
	Truncated bool    `json:"truncated"`
}

// Manager is the executor-independent seam for admission and job inspection.
// ClaimNext and Finish provide explicit synchronous lifecycle control; no
// goroutines or workers are started by this package. WaitClaim blocks until a
// job and worker slot are available, context cancellation, or Close. Close
// stops admission, cancels queued jobs, and leaves running jobs for workers to
// finish.
type Manager interface {
	Admit(idempotencyKey string, request Request) (Snapshot, bool, error)
	ClaimNext() (Snapshot, error)
	WaitClaim(ctx context.Context) (Snapshot, error)
	Close()
	Get(id string) (Snapshot, error)
	History(id string) (History, error)
	AddEvent(id, eventType, message string) error
	Finish(id string, terminalStatus Status) error
}

type job struct {
	snapshot  Snapshot
	sequence  uint64
	events    []Event
	truncated bool
}

// LocalManager is an in-memory bounded registry and pending queue.
type LocalManager struct {
	mu           sync.Mutex
	config       Config
	jobs         map[string]*job
	idempotency  map[string]string
	queue        []string
	running      int
	nextSequence uint64
	notify       chan struct{}
	closed       bool
}

var _ Manager = (*LocalManager)(nil)

// NewManager creates a process-local manager with validated positive capacity
// bounds. Queue capacity may not exceed the record cap.
func NewManager(config Config) (*LocalManager, error) {
	if config.MaxTaskBytes == 0 {
		config.MaxTaskBytes = maxTaskBytes
	}
	if config.QueueCapacity < 1 || config.QueueCapacity > maxQueueLimit ||
		config.MaxConcurrentJobs < 1 || config.MaxConcurrentJobs > maxRecordsLimit ||
		config.MaxRecords < 1 || config.MaxRecords > maxRecordsLimit ||
		config.MaxEventsPerJob < 1 || config.MaxEventsPerJob > maxEventsLimit ||
		config.MaxTaskBytes < 1 || config.MaxTaskBytes > maxTaskBytes ||
		config.QueueCapacity > config.MaxRecords || config.MaxConcurrentJobs > config.MaxRecords {
		return nil, fmt.Errorf("%w: capacity bounds are outside supported limits", ErrInvalidInput)
	}
	return &LocalManager{
		config:      config,
		jobs:        make(map[string]*job),
		idempotency: make(map[string]string),
		notify:      make(chan struct{}),
	}, nil
}

// Admit validates and normalizes the key and payload before atomically checking
// replay/capacity rules and enqueuing a new record. replay is true when an
// identical retained request already exists for the key.
func (m *LocalManager) Admit(idempotencyKey string, request Request) (snapshot Snapshot, replay bool, err error) {
	key, normalized, err := normalizeAdmission(idempotencyKey, request, m.config.MaxTaskBytes)
	if err != nil {
		return Snapshot{}, false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Snapshot{}, false, ErrManagerClosed
	}
	if id, ok := m.idempotency[key]; ok {
		existing := m.jobs[id]
		if existing == nil {
			delete(m.idempotency, key)
		} else if !sameRequest(existing.snapshot.Request, normalized) {
			return Snapshot{}, false, ErrIdempotencyConflict
		} else {
			return cloneSnapshot(existing.snapshot), true, nil
		}
	}
	if len(m.queue) >= m.config.QueueCapacity {
		return Snapshot{}, false, ErrQueueFull
	}
	needsEviction := len(m.jobs) >= m.config.MaxRecords
	if needsEviction && !m.hasTerminalLocked() {
		return Snapshot{}, false, ErrRegistryFull
	}
	id, err := newJobID()
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("create job ID: %w", err)
	}
	if needsEviction {
		m.evictOldestTerminalLocked()
	}
	now := time.Now().UTC()
	m.nextSequence++
	created := Snapshot{ID: id, Request: normalized, Status: StatusQueued, CreatedAt: now, UpdatedAt: now}
	entry := &job{snapshot: created, sequence: m.nextSequence}
	m.appendEventLocked(entry, "queued", "Job admitted")
	m.jobs[id] = entry
	m.idempotency[key] = id
	m.queue = append(m.queue, id)
	m.signalLocked()
	return cloneSnapshot(created), false, nil
}

// ClaimNext removes the oldest pending job from queue capacity and moves it to
// running. It returns ErrNoQueuedJobs if the queue is empty.
func (m *LocalManager) ClaimNext() (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Snapshot{}, ErrManagerClosed
	}
	return m.claimLocked()
}

// WaitClaim blocks without polling until a queued job and worker slot are
// available. A closed manager returns ErrManagerClosed; cancellation returns
// ctx.Err().
func (m *LocalManager) WaitClaim(ctx context.Context) (Snapshot, error) {
	if ctx == nil {
		return Snapshot{}, ErrInvalidInput
	}
	for {
		if err := ctx.Err(); err != nil {
			return Snapshot{}, err
		}
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return Snapshot{}, ErrManagerClosed
		}
		if err := ctx.Err(); err != nil {
			m.mu.Unlock()
			return Snapshot{}, err
		}
		if len(m.queue) > 0 && m.running < m.config.MaxConcurrentJobs {
			snapshot, err := m.claimLocked()
			m.mu.Unlock()
			return snapshot, err
		}
		changed := m.notify
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return Snapshot{}, ctx.Err()
		case <-changed:
		}
	}
}

// Close is idempotent. It atomically stops admission and claims, marks all
// queued jobs canceled with history events, and leaves running jobs unchanged
// so their workers can finish them.
func (m *LocalManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	now := time.Now().UTC()
	for _, id := range m.queue {
		entry := m.jobs[id]
		if entry == nil || entry.snapshot.Status != StatusQueued {
			continue
		}
		entry.snapshot.Status = StatusCanceled
		entry.snapshot.UpdatedAt = now
		m.appendEventLocked(entry, string(StatusCanceled), "Manager closed before job started")
	}
	m.queue = nil
	m.signalLocked()
}

func (m *LocalManager) claimLocked() (Snapshot, error) {
	if len(m.queue) == 0 {
		return Snapshot{}, ErrNoQueuedJobs
	}
	if m.running >= m.config.MaxConcurrentJobs {
		return Snapshot{}, ErrNoWorkerSlots
	}
	id := m.queue[0]
	m.queue[0] = ""
	m.queue = m.queue[1:]
	entry := m.jobs[id]
	if entry == nil || entry.snapshot.Status != StatusQueued {
		return Snapshot{}, fmt.Errorf("queued job invariant violated")
	}
	entry.snapshot.Status = StatusRunning
	entry.snapshot.UpdatedAt = time.Now().UTC()
	m.running++
	m.appendEventLocked(entry, "running", "Job started")
	return cloneSnapshot(entry.snapshot), nil
}

// Get returns a defensive copy of a retained job snapshot.
func (m *LocalManager) Get(id string) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.jobs[id]
	if entry == nil {
		return Snapshot{}, ErrNotFound
	}
	return cloneSnapshot(entry.snapshot), nil
}

// History returns a defensive copy of the retained bounded event history.
func (m *LocalManager) History(id string) (History, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.jobs[id]
	if entry == nil {
		return History{}, ErrNotFound
	}
	events := append([]Event(nil), entry.events...)
	return History{JobID: id, Events: events, Truncated: entry.truncated}, nil
}

// AddEvent appends a bounded event to any retained job. Event strings must be
// valid UTF-8 and within fixed byte limits.
func (m *LocalManager) AddEvent(id, eventType, message string) error {
	eventType = strings.TrimSpace(eventType)
	message = strings.TrimSpace(message)
	if !validText(eventType, 1, 64, false) || !validText(message, 0, maxEventBytes, true) {
		return ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.jobs[id]
	if entry == nil {
		return ErrNotFound
	}
	m.appendEventLocked(entry, eventType, message)
	return nil
}

// Finish marks a running job terminal. Only the explicit terminal lifecycle
// statuses are accepted; queued jobs must first be claimed.
func (m *LocalManager) Finish(id string, terminalStatus Status) error {
	if !terminalStatus.Terminal() {
		return ErrInvalidTransition
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.jobs[id]
	if entry == nil {
		return ErrNotFound
	}
	if entry.snapshot.Status != StatusRunning {
		return ErrInvalidTransition
	}
	entry.snapshot.Status = terminalStatus
	entry.snapshot.UpdatedAt = time.Now().UTC()
	m.running--
	m.appendEventLocked(entry, string(terminalStatus), "Job finished")
	m.signalLocked()
	return nil
}

func (m *LocalManager) signalLocked() {
	close(m.notify)
	m.notify = make(chan struct{})
}

func (m *LocalManager) appendEventLocked(entry *job, eventType, message string) {
	event := Event{At: time.Now().UTC(), Type: eventType, Message: message}
	if len(entry.events) == m.config.MaxEventsPerJob {
		copy(entry.events, entry.events[1:])
		entry.events[len(entry.events)-1] = event
		entry.truncated = true
		return
	}
	entry.events = append(entry.events, event)
}

func (m *LocalManager) hasTerminalLocked() bool {
	for _, entry := range m.jobs {
		if entry.snapshot.Status.Terminal() {
			return true
		}
	}
	return false
}

func (m *LocalManager) evictOldestTerminalLocked() bool {
	var oldestID string
	var oldest *job
	for id, entry := range m.jobs {
		if !entry.snapshot.Status.Terminal() {
			continue
		}
		if oldest == nil || entry.sequence < oldest.sequence {
			oldestID, oldest = id, entry
		}
	}
	if oldest == nil {
		return false
	}
	delete(m.jobs, oldestID)
	for key, id := range m.idempotency {
		if id == oldestID {
			delete(m.idempotency, key)
		}
	}
	return true
}

func normalizeAdmission(key string, request Request, maxTaskBytes int) (string, Request, error) {
	key = strings.TrimSpace(key)
	request.Repository = strings.TrimSpace(request.Repository)
	request.Task = strings.TrimSpace(request.Task)
	if !validText(key, 1, maxKeyBytes, false) ||
		!validText(request.Repository, 1, maxAliasBytes, false) ||
		!validText(request.Task, 1, maxTaskBytes, true) {
		return "", Request{}, ErrInvalidInput
	}
	if request.Issue != nil {
		if *request.Issue <= 0 {
			return "", Request{}, ErrInvalidInput
		}
		issue := *request.Issue
		request.Issue = &issue
	}
	return key, request, nil
}

func validText(value string, min, max int, allowWhitespaceControls bool) bool {
	if len(value) < min || len(value) > max || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) && !(allowWhitespaceControls && (r == '\n' || r == '\r' || r == '\t')) {
			return false
		}
	}
	return true
}

func sameRequest(a, b Request) bool {
	if a.Repository != b.Repository || a.Task != b.Task {
		return false
	}
	if a.Issue == nil || b.Issue == nil {
		return a.Issue == nil && b.Issue == nil
	}
	return *a.Issue == *b.Issue
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	if snapshot.Request.Issue != nil {
		issue := *snapshot.Request.Issue
		snapshot.Request.Issue = &issue
	}
	return snapshot
}

func newJobID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", encoded[:8], encoded[8:12], encoded[12:16], encoded[16:20], encoded[20:]), nil
}
