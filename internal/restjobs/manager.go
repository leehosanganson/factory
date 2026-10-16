// Package restjobs contains the process-local REST job admission and inspection
// model. It does not execute jobs or persist records.
package restjobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

var (
	ErrInvalidInput        = errors.New("invalid job input")
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with an existing request")
	ErrProvenanceConflict  = errors.New("issue provenance conflicts with an existing request")
	ErrQueueFull           = errors.New("job queue is full")
	ErrRegistryFull        = errors.New("job registry is full")
	ErrNotFound            = errors.New("job not found")
	ErrInvalidTransition   = errors.New("invalid job status transition")
	ErrNoQueuedJobs        = errors.New("no queued jobs")
	ErrNoWorkerSlots       = errors.New("no worker slots available")
	ErrManagerClosed       = errors.New("job manager is closed")
	ErrStaleAttempt        = errors.New("stale worker attempt")
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

const attemptLeaseDuration = time.Minute

const (
	maxRecordsLimit            = 1000
	maxConcurrentLimit         = 100_000
	maxQueueLimit              = 100_000
	maxEventsLimit             = 200
	maxKeyBytes                = 256
	maxAliasBytes              = 256
	maxTaskBytes               = 256 << 10
	maxIssueTitleBytes         = 4 << 10
	maxEventBytes              = 4_096
	maxVerificationChecks      = 16
	maxVerificationLimitations = 4
	maxVerificationTextBytes   = 128

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
	Repository      string           `json:"repository"`
	Task            string           `json:"task"`
	Issue           *int             `json:"issue,omitempty"`
	IssueProvenance *IssueProvenance `json:"issue_provenance,omitempty"`
}

// IssueProvenance identifies one explicitly dispatched GitHub issue snapshot.
// It contains bounded identity metadata only; the task remains the request body.
type IssueProvenance struct {
	Tracker          string `json:"tracker"`
	SourceRepository string `json:"source_repository"`
	IssueNumber      int    `json:"issue_number"`
	Version          string `json:"version"`
	URL              string `json:"url"`
	Title            string `json:"title"`
}

var (
	githubIssueOwnerPattern = regexp.MustCompile(`^[A-Za-z0-9-]+$`)
	githubIssueRepoPattern  = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
)

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

// ProviderAttempt is durable identity captured immediately before the provider write.
type ProviderAttempt struct {
	Provider   string `json:"provider"`
	Repository string `json:"repository"`
	Branch     string `json:"branch"`
	Commit     string `json:"commit"`
	Uncertain  bool   `json:"uncertain"`
}

// ProviderOutcome is a validated provider-side PR result.
type ProviderOutcome struct {
	Provider   string `json:"provider"`
	Repository string `json:"repository"`
	Number     int    `json:"number"`
	URL        string `json:"url"`
	Branch     string `json:"branch"`
	Commit     string `json:"commit"`
	State      string `json:"state"`
}

// VerificationCheck contains only a generated safe label and outcome.
type VerificationCheck struct {
	Name    string `json:"name"`
	Outcome string `json:"outcome"`
}

const (
	VerificationNotCompleted = "not_completed"
	VerificationPassed       = "passed"
	VerificationFailed       = "failed"
	VerificationCanceled     = "canceled"

	LimitationNoChecksConfigured = "No operator-configured verification checks were run."
	LimitationAgentNotVerdict    = "Agent-reported completion is not an independent correctness verdict."
)

// VerificationEvidence is bounded job outcome metadata safe for API exposure.
type VerificationEvidence struct {
	Checks      []VerificationCheck `json:"checks"`
	Limitations []string            `json:"limitations"`
}

func validateVerificationEvidence(evidence VerificationEvidence) error {
	if len(evidence.Checks) > maxVerificationChecks || len(evidence.Limitations) > maxVerificationLimitations {
		return ErrInvalidInput
	}
	for index, check := range evidence.Checks {
		if check.Name != fmt.Sprintf("check-%02d", index+1) || (check.Outcome != VerificationNotCompleted && check.Outcome != VerificationPassed && check.Outcome != VerificationFailed && check.Outcome != VerificationCanceled) {
			return ErrInvalidInput
		}
	}
	for _, limitation := range evidence.Limitations {
		if len(limitation) > maxVerificationTextBytes || (limitation != LimitationNoChecksConfigured && limitation != LimitationAgentNotVerdict) {
			return ErrInvalidInput
		}
	}
	return nil
}

// AttemptHandle is the immutable fencing identity issued for one worker claim.
// Its owner token is opaque and must not be exposed to job clients.
type ProgressEventStore interface {
	RecordProgressForAttempt(AttemptHandle, ProgressSummary) error
}

type AttemptIdentityStore interface{ SetAttemptIdentity(AttemptHandle) error }

type AttemptHandle struct {
	JobID      string `json:"-"`
	Generation int64  `json:"-"`
	OwnerToken string `json:"-"`
	WorkerID   string `json:"-"`
	BootID     string `json:"-"`
}

// WorkerSummary is safe attribution metadata for the currently or most recently
// persisted attempt. It contains no lease token or host-specific path.
type WorkerSummary struct {
	WorkerID string `json:"worker_id"`
	BootID   string `json:"boot_id"`
}

// ProgressSummary is a bounded allowlisted workflow stage and state.
type ProgressSummary struct {
	Stage string `json:"stage"`
	State string `json:"state"`
}

// AttemptClaim pairs a claimed job snapshot with its fencing identity.
type AttemptClaim struct {
	Job     Snapshot
	Attempt AttemptHandle
}

// Snapshot is a copy of public job state. Verification contains only safe
// check labels/outcomes and fixed limitation messages, never command output.
type Snapshot struct {
	ID                    string                `json:"id"`
	Submitter             string                `json:"submitted_by"`
	Request               Request               `json:"request"`
	Status                Status                `json:"status"`
	CancellationRequested bool                  `json:"cancellation_requested,omitempty"`
	CreatedAt             time.Time             `json:"created_at"`
	UpdatedAt             time.Time             `json:"updated_at"`
	Provider              *ProviderOutcome      `json:"provider,omitempty"`
	Verification          *VerificationEvidence `json:"verification,omitempty"`
	Worker                *WorkerSummary        `json:"worker,omitempty"`
	Progress              *ProgressSummary      `json:"progress,omitempty"`
}

// Event is one bounded history entry.
type Event struct {
	At       time.Time `json:"at"`
	Type     string    `json:"type"`
	Message  string    `json:"message,omitempty"`
	Stage    string    `json:"stage,omitempty"`
	State    string    `json:"state,omitempty"`
	Sequence uint64    `json:"-"`
}

// History is a bounded copy of one job's retained event stream.
type History struct {
	JobID          string           `json:"job_id"`
	Events         []Event          `json:"events"`
	Truncated      bool             `json:"truncated"`
	Worker         *WorkerSummary   `json:"worker,omitempty"`
	Progress       *ProgressSummary `json:"progress,omitempty"`
	LatestSequence uint64           `json:"-"`
}

// JobSummary is the safe, compact representation returned by collection listing.
type JobSummary struct {
	ID        string    `json:"id"`
	Status    Status    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// JobPage is an admission-ordered page bounded by SnapshotSequence. NextSequence
// is a continuation cursor for a subsequent request.
type JobPage struct {
	Jobs             []JobSummary `json:"jobs"`
	SnapshotSequence uint64       `json:"snapshot_sequence"`
	NextSequence     uint64       `json:"next_sequence,omitempty"`
	HasMore          bool         `json:"has_more"`
}

const (
	DefaultJobListLimit = 50
	MaxJobListLimit     = 100
)

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
// ClaimNext and WaitClaim are unfenced single-process/admin compatibility APIs;
// they must never be used by distributed workers. Attempt-aware workers use
// ClaimNextAttempt and WaitClaimAttempt, then only the corresponding fenced
// mutation methods. Finish provides explicit synchronous compatibility control;
// no goroutines or workers are started by this package. Close stops admission,
// cancels queued jobs, and leaves running jobs for workers to finish.
type Store interface {
	Admit(idempotencyKey string, request Request) (Snapshot, bool, error)
	AdmitAs(idempotencyKey, principalID string, request Request) (Snapshot, bool, error)
	ResolveInterrupted(id string, disposition InterruptedDisposition) error
	ClaimNext() (Snapshot, error)
	WaitClaim(ctx context.Context) (Snapshot, error)
	ClaimNextAttempt() (AttemptClaim, error)
	WaitClaimAttempt(ctx context.Context) (AttemptClaim, error)
	HeartbeatAttempt(AttemptHandle) error
	AddEventForAttempt(AttemptHandle, string, string) error
	RecordProgressForAttempt(AttemptHandle, ProgressSummary) error
	RecordProviderAttemptForAttempt(AttemptHandle, ProviderAttempt) error
	MarkProviderAttemptUncertainForAttempt(AttemptHandle) error
	RecordProviderOutcomeForAttempt(AttemptHandle, ProviderOutcome) error
	RecordVerificationEvidenceForAttempt(AttemptHandle, VerificationEvidence) error
	FinishAttempt(AttemptHandle, Status) error
	Close()
	Get(id string) (Snapshot, error)
	GetForPrincipal(id, principalID string) (Snapshot, error)
	ListJobs(ctx context.Context, afterSequence, snapshotSequence uint64, limit int) (JobPage, error)
	ListForPrincipal(ctx context.Context, principalID string, afterSequence, snapshotSequence uint64, limit int) (JobPage, error)
	History(id string) (History, error)
	EventStreamState(ctx context.Context, id string) (Snapshot, History, error)
	AddEvent(id, eventType, message string) error
	RecordProviderOutcome(id string, outcome ProviderOutcome) error
	RecordVerificationEvidence(id string, evidence VerificationEvidence) error
	Finish(id string, terminalStatus Status) error
	Cancel(id string) (Snapshot, error)
	Recover(ctx context.Context) (RecoveryReport, error)
	OperationalSummary(ctx context.Context) (OperationalSummary, error)
}

// Manager is retained as the executor-facing name for the Store contract.
type Manager interface {
	Store
}

// InterruptedDisposition is the bounded non-success outcome an operator may
// record for SQLite work found running at startup, only when no provider side
// effect was recorded.
type InterruptedDisposition string

const (
	InterruptedDispositionFailed   InterruptedDisposition = "failed"
	InterruptedDispositionCanceled InterruptedDisposition = "canceled"
)

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

type issueProvenanceKey struct {
	tracker          string
	sourceRepository string
	issueNumber      int
	version          string
}

type idempotencyIdentity struct {
	principalID string
	key         string
}

type job struct {
	snapshot        Snapshot
	providerAttempt *ProviderAttempt
	generation      int64
	ownerToken      string
	leaseUntil      time.Time
	sequence        uint64
	nextEvent       uint64
	events          []Event
	truncated       bool
	accountedBytes  uint64
}

const legacySharedPrincipalID = "legacy-shared"

// LocalManager is an in-memory bounded registry and pending queue.
type LocalManager struct {
	mu            sync.Mutex
	config        Config
	jobs          map[string]*job
	idempotency   map[idempotencyIdentity]string
	provenance    map[issueProvenanceKey]string
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
		idempotency: make(map[idempotencyIdentity]string),
		provenance:  make(map[issueProvenanceKey]string),
		notify:      make(chan struct{}),
	}, nil
}

// Admit validates and normalizes the key and payload before atomically checking
// replay/capacity rules and enqueuing a new record. replay is true when an
// identical retained request already exists for the key.
func (m *LocalManager) Admit(idempotencyKey string, request Request) (Snapshot, bool, error) {
	return m.AdmitAs(idempotencyKey, legacySharedPrincipalID, request)
}

// AdmitAs atomically admits a job attributed to principalID. Idempotency keys
// are scoped to that principal, while issue provenance remains globally unique.
func (m *LocalManager) AdmitAs(idempotencyKey, principalID string, request Request) (snapshot Snapshot, replay bool, err error) {
	key, normalized, err := normalizeAdmission(idempotencyKey, request, m.config.MaxTaskBytes)
	if err != nil {
		return Snapshot{}, false, err
	}
	principalID, err = normalizePrincipalID(principalID)
	if err != nil {
		return Snapshot{}, false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Snapshot{}, false, ErrManagerClosed
	}
	var provenanceEntry *job
	if normalized.IssueProvenance != nil {
		provenanceKey := keyForIssueProvenance(*normalized.IssueProvenance)
		if id, ok := m.provenance[provenanceKey]; ok {
			provenanceEntry = m.jobs[id]
			if provenanceEntry == nil {
				delete(m.provenance, provenanceKey)
			} else if provenanceEntry.snapshot.Submitter != principalID {
				return Snapshot{}, false, ErrProvenanceConflict
			}
		}
	}
	identity := idempotencyIdentity{principalID: principalID, key: key}
	if id, ok := m.idempotency[identity]; ok {
		existing := m.jobs[id]
		if existing == nil {
			delete(m.idempotency, identity)
		} else if !sameRequest(existing.snapshot.Request, normalized) {
			return Snapshot{}, false, ErrIdempotencyConflict
		} else {
			return cloneSnapshot(existing.snapshot), true, nil
		}
	}
	if provenanceEntry != nil {
		if !sameRequest(provenanceEntry.snapshot.Request, normalized) {
			return Snapshot{}, false, ErrProvenanceConflict
		}
		keyBytes := uint64(recordOverheadBytes + len(key))
		if !m.makeByteRoomLocked(keyBytes, provenanceEntry) {
			return Snapshot{}, false, ErrRegistryFull
		}
		provenanceEntry.accountedBytes += keyBytes
		m.registryBytes += keyBytes
		m.idempotency[identity] = provenanceEntry.snapshot.ID
		return cloneSnapshot(provenanceEntry.snapshot), true, nil
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
	created := Snapshot{ID: id, Submitter: principalID, Request: normalized, Status: StatusQueued, CreatedAt: now, UpdatedAt: now}
	entry := &job{snapshot: created, sequence: m.nextSequence}
	initialEvent := Event{Type: "queued", Message: "Job admitted"}
	recordCharge := recordBytes(normalized, key, principalID)
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
	m.idempotency[identity] = id
	if normalized.IssueProvenance != nil {
		m.provenance[keyForIssueProvenance(*normalized.IssueProvenance)] = id
	}
	m.queue = append(m.queue, id)
	m.signalLocked()
	return cloneSnapshot(created), false, nil
}

// ClaimNext is an unfenced single-process/admin compatibility claim. It does
// not create AttemptHandle ownership state and must never be used by distributed
// workers. Use ClaimNextAttempt for worker execution.
func (m *LocalManager) ClaimNext() (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Snapshot{}, ErrManagerClosed
	}
	return m.claimLegacyLocked()
}

func (m *LocalManager) ClaimNextAttempt() (AttemptClaim, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return AttemptClaim{}, ErrManagerClosed
	}
	return m.claimLocked()
}

// WaitClaim blocks without polling until a queued job and worker slot are
// available. This is an unfenced single-process/admin compatibility API; it
// must never be used by distributed workers. Use WaitClaimAttempt instead.
func (m *LocalManager) WaitClaim(ctx context.Context) (Snapshot, error) {
	claim, err := m.waitClaim(ctx, false)
	return claim.Job, err
}

func (m *LocalManager) WaitClaimAttempt(ctx context.Context) (AttemptClaim, error) {
	claim, err := m.waitClaim(ctx, true)
	return claim, err
}

func (m *LocalManager) waitClaim(ctx context.Context, attemptAware bool) (AttemptClaim, error) {
	if ctx == nil {
		return AttemptClaim{}, ErrInvalidInput
	}
	for {
		if err := ctx.Err(); err != nil {
			return AttemptClaim{}, err
		}
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return AttemptClaim{}, ErrManagerClosed
		}
		if err := ctx.Err(); err != nil {
			m.mu.Unlock()
			return AttemptClaim{}, err
		}
		if len(m.queue) > 0 && m.running < m.config.MaxConcurrentJobs {
			var claim AttemptClaim
			var err error
			if attemptAware {
				claim, err = m.claimLocked()
			} else {
				var snapshot Snapshot
				snapshot, err = m.claimLegacyLocked()
				claim.Job = snapshot
			}
			m.mu.Unlock()
			return claim, err
		}
		changed := m.notify
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return AttemptClaim{}, ctx.Err()
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

func (m *LocalManager) claimLegacyLocked() (Snapshot, error) {
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

func (m *LocalManager) claimLocked() (AttemptClaim, error) {
	if len(m.queue) == 0 {
		return AttemptClaim{}, ErrNoQueuedJobs
	}
	if m.running >= m.config.MaxConcurrentJobs {
		return AttemptClaim{}, ErrNoWorkerSlots
	}
	id := m.queue[0]
	entry := m.jobs[id]
	if entry == nil || entry.snapshot.Status != StatusQueued {
		return AttemptClaim{}, fmt.Errorf("queued job invariant violated")
	}
	token, err := newOwnerToken()
	if err != nil {
		return AttemptClaim{}, err
	}
	if entry.generation == int64(^uint64(0)>>1) {
		return AttemptClaim{}, ErrInvalidTransition
	}
	entry.generation++
	entry.ownerToken = token
	entry.leaseUntil = time.Now().UTC().Add(attemptLeaseDuration)
	if !m.appendEventLocked(entry, "running", "Job started") {
		entry.truncated = true
	}
	m.queue[0] = ""
	m.queue = m.queue[1:]
	entry.snapshot.Status = StatusRunning
	entry.snapshot.UpdatedAt = time.Now().UTC()
	m.running++
	return AttemptClaim{Job: cloneSnapshot(entry.snapshot), Attempt: AttemptHandle{JobID: id, Generation: entry.generation, OwnerToken: entry.ownerToken}}, nil
}

func newOwnerToken() (string, error) {
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", errors.New("generate worker owner token")
	}
	return hex.EncodeToString(token[:]), nil
}

func validAttemptHandle(attempt AttemptHandle) bool {
	if !validJobID(attempt.JobID) || attempt.Generation <= 0 || len(attempt.OwnerToken) != 64 || (attempt.WorkerID == "") != (attempt.BootID == "") {
		return false
	}
	for _, char := range attempt.OwnerToken {
		if !(char >= '0' && char <= '9') && !(char >= 'a' && char <= 'f') {
			return false
		}
	}
	if attempt.WorkerID != "" && (!validWorkerIdentity(attempt.WorkerID) || !validWorkerIdentity(attempt.BootID)) {
		return false
	}
	return true
}

func WorkerIdentityValid(value string) bool { return validWorkerIdentity(value) }

func defaultWorkerLabel(value string) string {
	if value == "" {
		return "worker-local"
	}
	return value
}

func validWorkerIdentity(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z') && !(char >= 'A' && char <= 'Z') && !(char >= '0' && char <= '9') && char != '.' && char != '_' && char != ':' && char != '-' {
			return false
		}
	}
	return true
}

func validProgress(progress ProgressSummary) bool {
	stage := progress.Stage == "requirements" || progress.Stage == "implement" || progress.Stage == "review" || progress.Stage == "document"
	state := progress.State == "started" || progress.State == "completed" || progress.State == "failed"
	return stage && state
}

func ParseProgressEvent(message string) (*ProgressSummary, bool) {
	fields := strings.Split(message, " ")
	if len(fields) != 2 || !strings.HasPrefix(fields[0], "stage=") || !strings.HasPrefix(fields[1], "state=") {
		return nil, false
	}
	stage, state := strings.TrimPrefix(fields[0], "stage="), strings.TrimPrefix(fields[1], "state=")
	validStage := stage == "requirements" || stage == "implement" || stage == "review" || stage == "document"
	validState := state == "started" || state == "completed" || state == "failed"
	if !validStage || !validState {
		return nil, false
	}
	return &ProgressSummary{Stage: stage, State: state}, true
}

func (m *LocalManager) ownsAttemptLocked(attempt AttemptHandle) (*job, error) {
	if !validAttemptHandle(attempt) {
		return nil, ErrInvalidInput
	}
	entry := m.jobs[attempt.JobID]
	if entry == nil {
		return nil, ErrNotFound
	}
	if entry.snapshot.Status != StatusRunning || entry.generation != attempt.Generation || entry.ownerToken != attempt.OwnerToken || !time.Now().UTC().Before(entry.leaseUntil) {
		return nil, ErrStaleAttempt
	}
	return entry, nil
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

// GetForPrincipal returns only a job attributed to principalID. Missing and
// differently attributed jobs have the same store-level result.
func (m *LocalManager) GetForPrincipal(id, principalID string) (Snapshot, error) {
	principalID, err := normalizePrincipalID(principalID)
	if err != nil {
		return Snapshot{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.jobs[id]
	if entry == nil || entry.snapshot.Submitter != principalID {
		return Snapshot{}, ErrNotFound
	}
	return cloneSnapshot(entry.snapshot), nil
}

// ListJobs returns summaries in immutable admission order. The first page captures
// a ceiling so subsequent admissions cannot shift a client's traversal.
func (m *LocalManager) ListJobs(ctx context.Context, afterSequence, snapshotSequence uint64, limit int) (JobPage, error) {
	return m.listJobs(ctx, "", afterSequence, snapshotSequence, limit)
}

// ListForPrincipal filters by submitter before pagination is applied.
func (m *LocalManager) ListForPrincipal(ctx context.Context, principalID string, afterSequence, snapshotSequence uint64, limit int) (JobPage, error) {
	principalID, err := normalizePrincipalID(principalID)
	if err != nil {
		return JobPage{}, err
	}
	return m.listJobs(ctx, principalID, afterSequence, snapshotSequence, limit)
}

func (m *LocalManager) listJobs(ctx context.Context, principalID string, afterSequence, snapshotSequence uint64, limit int) (JobPage, error) {
	if err := validateJobPageRequest(ctx, afterSequence, snapshotSequence, limit); err != nil {
		return JobPage{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return JobPage{}, err
	}
	if snapshotSequence == 0 {
		snapshotSequence = m.nextSequence
	}
	ordered := make([]*job, 0, limit+1)
	for _, entry := range m.jobs {
		if (principalID != "" && entry.snapshot.Submitter != principalID) || entry.sequence <= afterSequence || entry.sequence > snapshotSequence {
			continue
		}
		index := sort.Search(len(ordered), func(i int) bool { return ordered[i].sequence >= entry.sequence })
		if index >= limit+1 {
			continue
		}
		ordered = append(ordered, nil)
		copy(ordered[index+1:], ordered[index:])
		ordered[index] = entry
		if len(ordered) > limit+1 {
			ordered = ordered[:limit+1]
		}
	}
	page := JobPage{Jobs: make([]JobSummary, 0, limit), SnapshotSequence: snapshotSequence, HasMore: len(ordered) > limit}
	if page.HasMore {
		ordered = ordered[:limit]
	}
	for _, entry := range ordered {
		page.Jobs = append(page.Jobs, summarize(entry.snapshot))
		page.NextSequence = entry.sequence
	}
	if !page.HasMore {
		page.NextSequence = 0
	}
	return page, nil
}

func validateJobPageRequest(ctx context.Context, afterSequence, snapshotSequence uint64, limit int) error {
	maxSequence := uint64(^uint64(0) >> 1)
	if ctx == nil || limit < 1 || limit > MaxJobListLimit || afterSequence > maxSequence || snapshotSequence > maxSequence || (snapshotSequence == 0 && afterSequence != 0) || (snapshotSequence != 0 && afterSequence > snapshotSequence) {
		return ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func summarize(snapshot Snapshot) JobSummary {
	return JobSummary{ID: snapshot.ID, Status: snapshot.Status, CreatedAt: snapshot.CreatedAt, UpdatedAt: snapshot.UpdatedAt}
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
	return History{JobID: id, Events: events, Truncated: entry.truncated, Worker: cloneWorker(entry.snapshot.Worker), Progress: cloneProgress(entry.snapshot.Progress), LatestSequence: entry.nextEvent}, nil
}

func (m *LocalManager) EventStreamState(ctx context.Context, id string) (Snapshot, History, error) {
	if ctx == nil {
		return Snapshot{}, History{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, History{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Snapshot{}, History{}, err
	}
	entry := m.jobs[id]
	if entry == nil {
		return Snapshot{}, History{}, ErrNotFound
	}
	events := append([]Event(nil), entry.events...)
	history := History{JobID: id, Events: events, Truncated: entry.truncated, Worker: cloneWorker(entry.snapshot.Worker), Progress: cloneProgress(entry.snapshot.Progress), LatestSequence: entry.nextEvent}
	return cloneSnapshot(entry.snapshot), history, nil
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
	if entry.generation > 0 || entry.ownerToken != "" {
		return ErrStaleAttempt
	}
	if !m.appendEventLocked(entry, eventType, message) {
		return ErrRegistryFull
	}
	return nil
}

func (m *LocalManager) HeartbeatAttempt(attempt AttemptHandle) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.ownsAttemptLocked(attempt)
	if err != nil {
		return err
	}
	entry.leaseUntil = time.Now().UTC().Add(attemptLeaseDuration)
	return nil
}

func (m *LocalManager) SetAttemptIdentity(attempt AttemptHandle) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.ownsAttemptLocked(attempt)
	if err != nil {
		return err
	}
	entry.snapshot.Worker = &WorkerSummary{WorkerID: defaultWorkerLabel(attempt.WorkerID), BootID: defaultWorkerLabel(attempt.BootID)}
	return nil
}

func (m *LocalManager) RecordProgressForAttempt(attempt AttemptHandle, progress ProgressSummary) error {
	if !validProgress(progress) {
		return ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.ownsAttemptLocked(attempt)
	if err != nil {
		return err
	}
	entry.snapshot.Progress = cloneProgress(&progress)
	entry.snapshot.Worker = &WorkerSummary{WorkerID: defaultWorkerLabel(attempt.WorkerID), BootID: defaultWorkerLabel(attempt.BootID)}
	entry.snapshot.UpdatedAt = time.Now().UTC()
	return nil
}

func (m *LocalManager) AddEventForAttempt(attempt AttemptHandle, eventType, message string) error {
	eventType, message = strings.TrimSpace(eventType), strings.TrimSpace(message)
	if !validAttemptHandle(attempt) || !validText(eventType, 1, 64, false) || !validText(message, 0, maxEventBytes, true) {
		return ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.ownsAttemptLocked(attempt)
	if err != nil {
		return err
	}
	if !m.appendEventLocked(entry, eventType, message) {
		return ErrRegistryFull
	}
	if eventType == "stage_progress" {
		if progress, ok := ParseProgressEvent(message); ok {
			entry.snapshot.Progress = progress
		}
	}
	entry.snapshot.Worker = &WorkerSummary{WorkerID: defaultWorkerLabel(attempt.WorkerID), BootID: defaultWorkerLabel(attempt.BootID)}
	entry.leaseUntil = time.Now().UTC().Add(attemptLeaseDuration)
	return nil
}

func (m *LocalManager) RecordProviderAttemptForAttempt(attemptHandle AttemptHandle, attempt ProviderAttempt) error {
	if !validAttemptHandle(attemptHandle) || validateProviderAttempt(attempt) != nil || attempt.Branch != "factory/job/"+attemptHandle.JobID || !validCommitID(attempt.Commit) {
		return ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.ownsAttemptLocked(attemptHandle)
	if err != nil {
		return err
	}
	if entry.providerAttempt != nil {
		if *entry.providerAttempt != attempt {
			return ErrInvalidTransition
		}
		return nil
	}
	copy := attempt
	entry.providerAttempt = &copy
	return nil
}

func (m *LocalManager) MarkProviderAttemptUncertainForAttempt(attemptHandle AttemptHandle) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.ownsAttemptLocked(attemptHandle)
	if err != nil {
		return err
	}
	if entry.providerAttempt == nil {
		return ErrInvalidTransition
	}
	entry.providerAttempt.Uncertain = true
	return nil
}

func (m *LocalManager) RecordProviderOutcomeForAttempt(attempt AttemptHandle, outcome ProviderOutcome) error {
	if !validAttemptHandle(attempt) || validateProviderOutcome(outcome) != nil {
		return ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.ownsAttemptLocked(attempt)
	if err != nil {
		return err
	}
	copy := outcome
	entry.snapshot.Provider = &copy
	entry.snapshot.UpdatedAt = time.Now().UTC()
	entry.leaseUntil = time.Now().UTC().Add(attemptLeaseDuration)
	return nil
}

func (m *LocalManager) RecordVerificationEvidenceForAttempt(attempt AttemptHandle, evidence VerificationEvidence) error {
	if !validAttemptHandle(attempt) || validateVerificationEvidence(evidence) != nil {
		return ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.ownsAttemptLocked(attempt)
	if err != nil {
		return err
	}
	previousCharge := verificationEvidenceBytes(entry.snapshot.Verification)
	nextCharge := verificationEvidenceBytes(&evidence)
	if nextCharge > previousCharge {
		delta := nextCharge - previousCharge
		if !m.makeByteRoomLocked(delta, entry) {
			return ErrRegistryFull
		}
		entry.accountedBytes += delta
		m.registryBytes += delta
	} else {
		delta := previousCharge - nextCharge
		entry.accountedBytes -= delta
		m.registryBytes -= delta
	}
	copy := cloneVerificationEvidence(evidence)
	entry.snapshot.Verification = &copy
	entry.snapshot.UpdatedAt = time.Now().UTC()
	entry.leaseUntil = time.Now().UTC().Add(attemptLeaseDuration)
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
	if entry.generation > 0 || entry.ownerToken != "" {
		return ErrStaleAttempt
	}
	if entry.snapshot.Status != StatusRunning {
		return ErrInvalidTransition
	}
	copy := outcome
	entry.snapshot.Provider = &copy
	entry.snapshot.UpdatedAt = time.Now().UTC()
	return nil
}

func (m *LocalManager) RecordVerificationEvidence(id string, evidence VerificationEvidence) error {
	if validateVerificationEvidence(evidence) != nil {
		return ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.jobs[id]
	if entry == nil {
		return ErrNotFound
	}
	if entry.generation > 0 || entry.ownerToken != "" {
		return ErrStaleAttempt
	}
	if entry.snapshot.Status != StatusRunning {
		return ErrInvalidTransition
	}
	previousCharge := verificationEvidenceBytes(entry.snapshot.Verification)
	nextCharge := verificationEvidenceBytes(&evidence)
	if nextCharge > previousCharge {
		delta := nextCharge - previousCharge
		if !m.makeByteRoomLocked(delta, entry) {
			return ErrRegistryFull
		}
		entry.accountedBytes += delta
		m.registryBytes += delta
	} else {
		delta := previousCharge - nextCharge
		entry.accountedBytes -= delta
		m.registryBytes -= delta
	}
	copy := cloneVerificationEvidence(evidence)
	entry.snapshot.Verification = &copy
	entry.snapshot.UpdatedAt = time.Now().UTC()
	return nil
}

// ResolveInterrupted is unsupported by the volatile store: it has no durable
// startup recovery classification to authorize an operator disposition.
func (m *LocalManager) ResolveInterrupted(id string, disposition InterruptedDisposition) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.jobs[id] == nil {
		return ErrNotFound
	}
	return ErrInvalidTransition
}

func (m *LocalManager) Cancel(id string) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.jobs[id]
	if entry == nil {
		return Snapshot{}, ErrNotFound
	}
	switch entry.snapshot.Status {
	case StatusQueued:
		if !m.appendEventLocked(entry, string(StatusCanceled), "Job canceled before start") {
			entry.truncated = true
		}
		entry.snapshot.Status = StatusCanceled
		entry.snapshot.UpdatedAt = time.Now().UTC()
		for index, queuedID := range m.queue {
			if queuedID == id {
				m.queue = append(m.queue[:index], m.queue[index+1:]...)
				break
			}
		}
		m.signalLocked()
		return cloneSnapshot(entry.snapshot), nil
	case StatusRunning:
		if entry.snapshot.CancellationRequested {
			return cloneSnapshot(entry.snapshot), nil
		}
		entry.snapshot.CancellationRequested = true
		entry.snapshot.UpdatedAt = time.Now().UTC()
		if !m.appendEventLocked(entry, "cancel_requested", "Cancellation requested") {
			entry.truncated = true
		}
		m.signalLocked()
		return cloneSnapshot(entry.snapshot), nil
	default:
		return Snapshot{}, ErrInvalidTransition
	}
}

func (m *LocalManager) FinishAttempt(attempt AttemptHandle, terminalStatus Status) error {
	if !validAttemptHandle(attempt) || !terminalStatus.Terminal() {
		return ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.ownsAttemptLocked(attempt)
	if err != nil {
		return err
	}
	if !m.appendEventLocked(entry, string(terminalStatus), "Job finished") {
		entry.truncated = true
	}
	entry.snapshot.Status = terminalStatus
	entry.snapshot.CancellationRequested = false
	entry.snapshot.UpdatedAt = time.Now().UTC()
	m.running--
	m.signalLocked()
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
	if entry.generation > 0 || entry.ownerToken != "" {
		return ErrStaleAttempt
	}
	if entry.snapshot.Status != StatusRunning {
		return ErrInvalidTransition
	}
	if !m.appendEventLocked(entry, string(terminalStatus), "Job finished") {
		entry.truncated = true
	}
	entry.snapshot.Status = terminalStatus
	entry.snapshot.CancellationRequested = false
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
	event := Event{At: time.Now().UTC(), Type: eventType, Message: message, Sequence: entry.nextEvent + 1}
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
	entry.nextEvent = event.Sequence
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
	if entry.snapshot.Request.IssueProvenance != nil {
		delete(m.provenance, keyForIssueProvenance(*entry.snapshot.Request.IssueProvenance))
	}
	for identity, id := range m.idempotency {
		if id == entry.snapshot.ID {
			delete(m.idempotency, identity)
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

func (m *LocalManager) makeByteRoomLocked(needed uint64, protected *job) bool {
	limit := uint64(m.config.RegistryBytes)
	if m.registryBytes > limit || needed > limit {
		return false
	}
	available := limit - m.registryBytes
	if needed <= available {
		return true
	}

	deficit := needed - available
	var reclaimable uint64
	for _, entry := range m.jobs {
		if entry == protected || !entry.snapshot.Status.Terminal() {
			continue
		}
		if entry.accountedBytes >= deficit-reclaimable {
			reclaimable = deficit
			break
		}
		reclaimable += entry.accountedBytes
	}
	if reclaimable < deficit {
		return false
	}

	for needed > limit-m.registryBytes {
		if !m.evictOldestTerminalExceptLocked(protected) {
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

func recordBytes(request Request, key, principalID string) uint64 {
	size := uint64(recordOverheadBytes) + uint64(len(request.Task)) + uint64(len(request.Repository)) + uint64(len(key)) + uint64(len(principalID))
	if request.IssueProvenance != nil {
		provenance := request.IssueProvenance
		size += uint64(len(provenance.Tracker) + len(provenance.SourceRepository) + len(provenance.Version) + len(provenance.URL) + len(provenance.Title) + 32)
	}
	return size
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
	if oldest.snapshot.Request.IssueProvenance != nil {
		delete(m.provenance, keyForIssueProvenance(*oldest.snapshot.Request.IssueProvenance))
	}
	for identity, id := range m.idempotency {
		if id == oldestID {
			delete(m.idempotency, identity)
		}
	}
	return true
}

func normalizePrincipalID(principalID string) (string, error) {
	principalID = strings.TrimSpace(principalID)
	if !validText(principalID, 1, maxAliasBytes, false) {
		return "", ErrInvalidInput
	}
	return principalID, nil
}

func normalizeAdmission(key string, request Request, maxTaskBytes int) (string, Request, error) {
	key = strings.TrimSpace(key)
	request.Repository = strings.TrimSpace(request.Repository)
	if request.IssueProvenance == nil {
		request.Task = strings.TrimSpace(request.Task)
	}
	if !validText(key, 1, maxKeyBytes, false) ||
		!validText(request.Repository, 1, maxAliasBytes, false) ||
		strings.TrimSpace(request.Task) == "" || !validText(request.Task, 1, maxTaskBytes, true) {
		return "", Request{}, ErrInvalidInput
	}
	if request.Issue != nil {
		if *request.Issue <= 0 {
			return "", Request{}, ErrInvalidInput
		}
		issue := *request.Issue
		request.Issue = &issue
	}
	if request.IssueProvenance != nil {
		provenance := *request.IssueProvenance
		if !validIssueProvenance(provenance) || (request.Issue != nil && *request.Issue != provenance.IssueNumber) {
			return "", Request{}, ErrInvalidInput
		}
		request.IssueProvenance = &provenance
	}
	return key, request, nil
}

func validIssueProvenanceFields(provenance IssueProvenance) bool {
	parts := strings.Split(provenance.SourceRepository, "/")
	if provenance.Tracker != "github" || len(parts) != 2 || !githubIssueOwnerPattern.MatchString(parts[0]) || !githubIssueRepoPattern.MatchString(parts[1]) || parts[1] == "." || parts[1] == ".." || provenance.IssueNumber <= 0 || len(provenance.Version) != 64 || !validText(provenance.Title, 0, maxIssueTitleBytes, false) {
		return false
	}
	for _, digit := range provenance.Version {
		if !(digit >= '0' && digit <= '9') && !(digit >= 'a' && digit <= 'f') {
			return false
		}
	}
	for _, r := range provenance.Title {
		if isBidiFormattingControl(r) {
			return false
		}
	}
	return true
}

func validIssueProvenanceURL(provenance IssueProvenance) bool {
	wantURL := fmt.Sprintf("https://github.com/%s/issues/%d", provenance.SourceRepository, provenance.IssueNumber)
	parsed, err := url.Parse(provenance.URL)
	return err == nil && provenance.URL == wantURL && parsed.Scheme == "https" && parsed.Host == "github.com" && parsed.User == nil && parsed.RawPath == "" && parsed.Path == fmt.Sprintf("/%s/issues/%d", provenance.SourceRepository, provenance.IssueNumber) && parsed.RawQuery == "" && parsed.Fragment == ""
}

func validIssueProvenance(provenance IssueProvenance) bool {
	return validIssueProvenanceFields(provenance) && validIssueProvenanceURL(provenance)
}

func isBidiFormattingControl(r rune) bool {
	return r == 0x061c || r == 0x200e || r == 0x200f ||
		(r >= 0x202a && r <= 0x202e) || (r >= 0x2066 && r <= 0x206f)
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
	if a.Repository != b.Repository || a.Task != b.Task || !sameIssueProvenance(a.IssueProvenance, b.IssueProvenance) {
		return false
	}
	if a.Issue == nil || b.Issue == nil {
		return a.Issue == nil && b.Issue == nil
	}
	return *a.Issue == *b.Issue
}

func keyForIssueProvenance(provenance IssueProvenance) issueProvenanceKey {
	return issueProvenanceKey{tracker: provenance.Tracker, sourceRepository: provenance.SourceRepository, issueNumber: provenance.IssueNumber, version: provenance.Version}
}

func sameIssueProvenance(a, b *IssueProvenance) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func cloneWorker(worker *WorkerSummary) *WorkerSummary {
	if worker == nil {
		return nil
	}
	copy := *worker
	return &copy
}

func cloneProgress(progress *ProgressSummary) *ProgressSummary {
	if progress == nil {
		return nil
	}
	copy := *progress
	return &copy
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	if snapshot.Status.Terminal() {
		snapshot.CancellationRequested = false
	}
	if snapshot.Request.Issue != nil {
		issue := *snapshot.Request.Issue
		snapshot.Request.Issue = &issue
	}
	if snapshot.Request.IssueProvenance != nil {
		provenance := *snapshot.Request.IssueProvenance
		snapshot.Request.IssueProvenance = &provenance
	}
	if snapshot.Verification != nil {
		copy := cloneVerificationEvidence(*snapshot.Verification)
		snapshot.Verification = &copy
	}
	snapshot.Worker = cloneWorker(snapshot.Worker)
	snapshot.Progress = cloneProgress(snapshot.Progress)
	return snapshot
}

func verificationEvidenceBytes(evidence *VerificationEvidence) uint64 {
	if evidence == nil {
		return 0
	}
	encoded, _ := json.Marshal(evidence)
	return uint64(len(encoded))
}

func cloneVerificationEvidence(evidence VerificationEvidence) VerificationEvidence {
	evidence.Checks = append([]VerificationCheck(nil), evidence.Checks...)
	evidence.Limitations = append([]string(nil), evidence.Limitations...)
	return evidence
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
