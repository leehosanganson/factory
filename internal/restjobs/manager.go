// Package restjobs contains the process-local REST job admission and inspection
// model. It does not execute jobs or persist records.
package restjobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
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

func validateProviderOutcome(outcome ProviderOutcome) error {
	parsed, err := url.Parse(outcome.URL)
	if outcome.Provider != "github" || !validRepositoryName(outcome.Repository) || outcome.Number <= 0 || outcome.Branch == "" || outcome.Commit == "" || outcome.State != "open" || err != nil || parsed.Scheme != "https" || parsed.Host != "github.com" || parsed.User != nil || parsed.RawPath != "" || parsed.Path != "/"+outcome.Repository+"/pull/"+fmt.Sprint(outcome.Number) || parsed.RawQuery != "" || parsed.Fragment != "" {
		return ErrInvalidInput
	}
	return nil
}

func validRepositoryName(value string) bool {
	parts := strings.Split(value, "/")
	return len(parts) == 2 && parts[0] != "" && parts[1] != "" && !strings.Contains(value, "..")
}

const (
	maxRecordsLimit    = 1000
	maxConcurrentLimit = 100_000
	maxQueueLimit      = 100_000
	maxEventsLimit     = 200
	maxKeyBytes        = 256
	maxAliasBytes      = 256
	maxTaskBytes       = 256 << 10
	maxEventBytes      = 4_096

	defaultRegistryBytes = 256 << 20
	recordOverheadBytes  = 256
	eventOverheadBytes   = 64
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
// Zero MaxRecords, MaxEventsPerJob, MaxTaskBytes, and RegistryBytes use bounded defaults.
type Config struct {
	QueueCapacity     int
	MaxConcurrentJobs int
	MaxRecords        int
	MaxEventsPerJob   int
	MaxTaskBytes      int
	RegistryBytes     int64
}

// Snapshot is a copy of the public job state and is safe to serialize after the
// manager lock has been released.
type ProviderOutcome struct {
	Provider   string `json:"provider"`
	Repository string `json:"repository"`
	Number     int    `json:"number"`
	URL        string `json:"url"`
	Branch     string `json:"branch"`
	Commit     string `json:"commit"`
	State      string `json:"state"`
}

type Snapshot struct {
	ID        string           `json:"id"`
	Request   Request          `json:"request"`
	Status    Status           `json:"status"`
	CreatedAt time.Time        `json:"created_at"`
	UpdatedAt time.Time        `json:"updated_at"`
	Provider  *ProviderOutcome `json:"provider,omitempty"`
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

// OperationalSummary reports bounded aggregate job state without job payloads.
type OperationalSummary struct {
	RetainedRecords int  `json:"retained_records"`
	RecordLimit     int  `json:"record_limit"`
	QueueCapacity   int  `json:"queue_capacity"`
	Queued          int  `json:"queued"`
	Running         int  `json:"running"`
	Succeeded       int  `json:"succeeded"`
	Failed          int  `json:"failed"`
	Canceled        int  `json:"canceled"`
	QueueSaturated  bool `json:"queue_saturated"`
	RecoveryNeeded  int  `json:"recovery_needed"`
}

// RecoveryReport classifies retained work after opening a store. Only queued
// work is safe to resume automatically; running work may have performed
// external side effects and requires reconciliation.
type RecoveryReport struct {
	Resume        []Snapshot
	NeedsOperator []Snapshot
	Terminal      []Snapshot
}

// Store is the persistence contract shared by the local in-memory store and
// future durable stores. Implementations must make Admit atomic with respect
// to idempotency, preserve lifecycle/history consistency, and honor cancellation
// for context-bearing operations.
//
// ClaimNext and Finish provide explicit synchronous lifecycle control; no
// goroutines or workers are started by this package. WaitClaim blocks until a
// job and worker slot are available, context cancellation, or Close. Close
// stops admission, cancels queued jobs, and leaves running jobs for workers to
// finish.
type Store interface {
	Admit(idempotencyKey string, request Request) (Snapshot, bool, error)
	ClaimNext() (Snapshot, error)
	WaitClaim(ctx context.Context) (Snapshot, error)
	Close()
	Get(id string) (Snapshot, error)
	History(id string) (History, error)
	AddEvent(id, eventType, message string) error
	RecordProviderOutcome(id string, outcome ProviderOutcome) error
	Finish(id string, terminalStatus Status) error
	Recover(ctx context.Context) (RecoveryReport, error)
	OperationalSummary(ctx context.Context) (OperationalSummary, error)
}

// Manager is retained as the executor-facing name for the Store contract.
type Manager interface {
	Store
}

// RecoveryDisposition describes what is safe to do with a persisted job after
// process restart. Running work must never be replayed automatically because
// it may have performed external side effects before interruption.
type RecoveryDisposition string

const (
	RecoveryResume         RecoveryDisposition = "resume"
	RecoveryNeedsOperator  RecoveryDisposition = "needs_operator"
	RecoveryRetainTerminal RecoveryDisposition = "retain_terminal"
)

// RecoveryDispositionFor classifies persisted statuses conservatively. Queued
// work has not been claimed and may be resumed; terminal work is retained;
// running or unknown states require operator reconciliation.
func RecoveryDispositionFor(status Status) RecoveryDisposition {
	switch status {
	case StatusQueued:
		return RecoveryResume
	case StatusSucceeded, StatusFailed, StatusCanceled:
		return RecoveryRetainTerminal
	default:
		return RecoveryNeedsOperator
	}
}

type job struct {
	snapshot       Snapshot
	sequence       uint64
	events         []Event
	truncated      bool
	accountedBytes uint64
}

// LocalManager is an in-memory bounded registry and pending queue.
type LocalManager struct {
	mu            sync.Mutex
	config        Config
	jobs          map[string]*job
	idempotency   map[string]string
	queue         []string
	running       int
	nextSequence  uint64
	registryBytes uint64
	notify        chan struct{}
	closed        bool
}

var _ Manager = (*LocalManager)(nil)

// NewManager creates a process-local manager with validated positive capacity
// bounds. Queue capacity may not exceed the record cap.
func NewManager(config Config) (*LocalManager, error) {
	if config.MaxRecords == 0 {
		config.MaxRecords = maxRecordsLimit
	}
	if config.MaxEventsPerJob == 0 {
		config.MaxEventsPerJob = maxEventsLimit
	}
	if config.MaxTaskBytes == 0 {
		config.MaxTaskBytes = maxTaskBytes
	}
	if config.RegistryBytes == 0 {
		config.RegistryBytes = defaultRegistryBytes
	}
	if config.QueueCapacity < 1 || config.QueueCapacity > maxQueueLimit ||
		config.MaxConcurrentJobs < 1 || config.MaxConcurrentJobs > maxConcurrentLimit ||
		config.MaxRecords < 1 || config.MaxRecords > maxRecordsLimit ||
		config.MaxEventsPerJob < 1 || config.MaxEventsPerJob > maxEventsLimit ||
		config.MaxTaskBytes < 1 || config.MaxTaskBytes > maxTaskBytes ||
		config.RegistryBytes < 1 || config.RegistryBytes > defaultRegistryBytes ||
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
	id, err := newJobID()
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("create job ID: %w", err)
	}
	now := time.Now().UTC()
	m.nextSequence++
	created := Snapshot{ID: id, Request: normalized, Status: StatusQueued, CreatedAt: now, UpdatedAt: now}
	entry := &job{snapshot: created, sequence: m.nextSequence}
	initialEvent := Event{Type: "queued", Message: "Job admitted"}
	recordCharge := recordBytes(normalized, key)
	initialCharge := eventBytes(initialEvent)
	if !m.makeRoomLocked(recordCharge + initialCharge) {
		return Snapshot{}, false, ErrRegistryFull
	}
	entry.accountedBytes = recordCharge
	m.registryBytes += recordCharge
	m.jobs[id] = entry
	if !m.appendEventLocked(entry, initialEvent.Type, initialEvent.Message) {
		delete(m.jobs, id)
		m.registryBytes -= recordCharge
		return Snapshot{}, false, ErrRegistryFull
	}
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
		if !m.appendEventLocked(entry, string(StatusCanceled), "Manager closed before job started") {
			entry.truncated = true
		}
		entry.snapshot.Status = StatusCanceled
		entry.snapshot.UpdatedAt = now
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
	entry := m.jobs[id]
	if entry == nil || entry.snapshot.Status != StatusQueued {
		return Snapshot{}, fmt.Errorf("queued job invariant violated")
	}
	if !m.appendEventLocked(entry, "running", "Job started") {
		entry.truncated = true
	}
	m.queue[0] = ""
	m.queue = m.queue[1:]
	entry.snapshot.Status = StatusRunning
	entry.snapshot.UpdatedAt = time.Now().UTC()
	m.running++
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

// Recover classifies retained records without mutating them. The local backend
// starts empty after process launch; durable implementations use the same
// contract to identify queued work and interrupted work requiring reconciliation.
func (m *LocalManager) OperationalSummary(ctx context.Context) (OperationalSummary, error) {
	if ctx == nil {
		return OperationalSummary{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return OperationalSummary{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	summary := OperationalSummary{RetainedRecords: len(m.jobs), RecordLimit: m.config.MaxRecords, QueueCapacity: m.config.QueueCapacity, QueueSaturated: len(m.queue) >= m.config.QueueCapacity}
	for _, entry := range m.jobs {
		if err := ctx.Err(); err != nil {
			return OperationalSummary{}, err
		}
		switch RecoveryDispositionFor(entry.snapshot.Status) {
		case RecoveryResume:
			summary.Queued++
		case RecoveryNeedsOperator:
			summary.Running++
		case RecoveryRetainTerminal:
			switch entry.snapshot.Status {
			case StatusSucceeded:
				summary.Succeeded++
			case StatusFailed:
				summary.Failed++
			case StatusCanceled:
				summary.Canceled++
			}
		}
	}
	return summary, nil
}

func (m *LocalManager) Recover(ctx context.Context) (RecoveryReport, error) {
	if ctx == nil {
		return RecoveryReport{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return RecoveryReport{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var report RecoveryReport
	for _, entry := range m.jobs {
		if err := ctx.Err(); err != nil {
			return RecoveryReport{}, err
		}
		snapshot := cloneSnapshot(entry.snapshot)
		switch RecoveryDispositionFor(entry.snapshot.Status) {
		case RecoveryResume:
			report.Resume = append(report.Resume, snapshot)
		case RecoveryNeedsOperator:
			report.NeedsOperator = append(report.NeedsOperator, snapshot)
		case RecoveryRetainTerminal:
			report.Terminal = append(report.Terminal, snapshot)
		}
	}
	return report, nil
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
	if !m.appendEventLocked(entry, eventType, message) {
		return ErrRegistryFull
	}
	return nil
}

func (m *LocalManager) RecordProviderOutcome(id string, outcome ProviderOutcome) error {
	if err := validateProviderOutcome(outcome); err != nil {
		return ErrInvalidInput
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
	copy := outcome
	entry.snapshot.Provider = &copy
	entry.snapshot.UpdatedAt = time.Now().UTC()
	return nil
}

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
	if !m.appendEventLocked(entry, string(terminalStatus), "Job finished") {
		entry.truncated = true
	}
	entry.snapshot.Status = terminalStatus
	entry.snapshot.UpdatedAt = time.Now().UTC()
	m.running--
	m.signalLocked()
	return nil
}

func (m *LocalManager) signalLocked() {
	close(m.notify)
	m.notify = make(chan struct{})
}

// appendEventLocked evicts oldest terminal jobs, then oldest events, to reserve
// the logical bytes needed for an event. Callers keep lifecycle state transitions
// independent of event availability and mark omitted history as truncated.
func (m *LocalManager) appendEventLocked(entry *job, eventType, message string) bool {
	event := Event{At: time.Now().UTC(), Type: eventType, Message: message}
	charge := eventBytes(event)
	limit := uint64(m.config.RegistryBytes)
	if m.registryBytes > limit {
		return false
	}

	replacedCharge := uint64(0)
	if len(entry.events) == m.config.MaxEventsPerJob {
		replacedCharge = eventBytes(entry.events[0])
	}
	needed := uint64(0)
	if charge > replacedCharge {
		needed = charge - replacedCharge
	}
	available := limit - m.registryBytes
	if needed > available {
		var reclaimable uint64
		for _, existing := range m.jobs {
			if existing == entry && replacedCharge > 0 {
				continue
			}
			if existing != entry && existing.snapshot.Status.Terminal() {
				reclaimable += existing.accountedBytes
				continue
			}
			for _, oldEvent := range existing.events {
				reclaimable += eventBytes(oldEvent)
			}
		}
		if needed-available > reclaimable {
			return false
		}
	}
	for needed > limit-m.registryBytes {
		var excludedFirst *job
		if replacedCharge > 0 {
			excludedFirst = entry
		}
		oldestTerminal := m.oldestTerminalLocked(entry)
		oldestEvent := m.oldestEventLocked(excludedFirst, replacedCharge > 0)
		if oldestTerminal != nil {
			m.removeJobLocked(oldestTerminal)
			continue
		}
		if oldestEvent == nil {
			return false
		}
		m.removeOldestEventLocked(oldestEvent)
	}
	if replacedCharge > 0 {
		m.removeOldestEventLocked(entry)
	}
	entry.events = append(entry.events, event)
	m.registryBytes += charge
	entry.accountedBytes += charge
	return true
}

func (m *LocalManager) oldestEventLocked(excludedFirst *job, skipFirst bool) *job {
	var oldest *job
	var oldestEvent time.Time
	for _, entry := range m.jobs {
		first := 0
		if entry == excludedFirst && skipFirst {
			first = 1
		}
		if first >= len(entry.events) {
			continue
		}
		eventAt := entry.events[first].At
		if oldest == nil || eventAt.Before(oldestEvent) || (eventAt.Equal(oldestEvent) && entry.sequence < oldest.sequence) {
			oldest = entry
			oldestEvent = eventAt
		}
	}
	return oldest
}

func (m *LocalManager) oldestTerminalLocked(excluded *job) *job {
	var oldest *job
	for _, entry := range m.jobs {
		if entry == excluded || !entry.snapshot.Status.Terminal() {
			continue
		}
		if oldest == nil || entry.sequence < oldest.sequence {
			oldest = entry
		}
	}
	return oldest
}

func (m *LocalManager) removeJobLocked(entry *job) {
	m.registryBytes -= entry.accountedBytes
	delete(m.jobs, entry.snapshot.ID)
	for key, id := range m.idempotency {
		if id == entry.snapshot.ID {
			delete(m.idempotency, key)
		}
	}
}

func (m *LocalManager) makeRoomLocked(needed uint64) bool {
	limit := uint64(m.config.RegistryBytes)
	if m.registryBytes > limit || needed > limit {
		return false
	}
	for len(m.jobs) >= m.config.MaxRecords || needed > limit-m.registryBytes {
		if !m.evictOldestTerminalExceptLocked(nil) {
			return false
		}
	}
	return true
}

func (m *LocalManager) removeOldestEventLocked(entry *job) {
	m.registryBytes -= eventBytes(entry.events[0])
	entry.accountedBytes -= eventBytes(entry.events[0])
	copy(entry.events, entry.events[1:])
	entry.events[len(entry.events)-1] = Event{}
	entry.events = entry.events[:len(entry.events)-1]
	entry.truncated = true
}

func recordBytes(request Request, key string) uint64 {
	return uint64(recordOverheadBytes) + uint64(len(request.Task)) + uint64(len(request.Repository)) + uint64(len(key))
}

func eventBytes(event Event) uint64 {
	return uint64(eventOverheadBytes) + uint64(len(event.Type)) + uint64(len(event.Message))
}

func (m *LocalManager) evictOldestTerminalExceptLocked(protected *job) bool {
	var oldestID string
	var oldest *job
	for id, entry := range m.jobs {
		if !entry.snapshot.Status.Terminal() || entry == protected {
			continue
		}
		if oldest == nil || entry.sequence < oldest.sequence {
			oldestID, oldest = id, entry
		}
	}
	if oldest == nil {
		return false
	}
	m.registryBytes -= oldest.accountedBytes
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
