package restjobs

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// ThreadStatus is the aggregate lifecycle status of a Thread's Jobs.
type ThreadStatus string

const (
	ThreadStatusNeedAttention ThreadStatus = "need_attention"
	ThreadStatusInProgress    ThreadStatus = "in_progress"
	ThreadStatusDone          ThreadStatus = "done"
)

// Thread is an ordered user-facing work context. Its status and updated time
// are projections of its retained Jobs; completion is explicit lifecycle state.
type Thread struct {
	ID          string       `json:"id"`
	Submitter   string       `json:"submitted_by"`
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`
	JobIDs      []string     `json:"job_ids"`
	Status      ThreadStatus `json:"status"`
	CompletedAt *time.Time   `json:"completed_at,omitempty"`
	CompletedBy string       `json:"completed_by,omitempty"`
}

// ThreadSnapshot is a defensive, ordered view of a Thread and its Jobs.
type ThreadSnapshot struct {
	Thread Thread     `json:"thread"`
	Jobs   []Snapshot `json:"jobs"`
}

// ThreadSummary is the safe, compact representation returned by Thread listing.
type ThreadSummary struct {
	ID          string       `json:"id"`
	Status      ThreadStatus `json:"status"`
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`
	CompletedAt *time.Time   `json:"completed_at,omitempty"`
}

// ThreadPage is a bounded page ordered by immutable initial Job admission.
type ThreadPage struct {
	Threads          []ThreadSummary `json:"threads"`
	SnapshotSequence uint64          `json:"snapshot_sequence"`
	NextSequence     uint64          `json:"next_sequence,omitempty"`
	HasMore          bool            `json:"has_more"`
}

const (
	DefaultThreadListLimit = 50
	MaxThreadListLimit     = 100
)

// ThreadStore is the Store contract for ordered Thread membership and explicit
// owner-controlled completion.
type ThreadStore interface {
	Store
	AdmitThreadAs(idempotencyKey, principalID string, request Request) (ThreadSnapshot, bool, error)
	AdmitThreadJobAs(threadID, idempotencyKey, principalID string, request Request) (ThreadSnapshot, Snapshot, bool, error)
	GetThread(id string) (ThreadSnapshot, error)
	GetThreadForPrincipal(id, principalID string) (ThreadSnapshot, error)
	ListThreads(ctx context.Context, beforeSequence, snapshotSequence uint64, limit int) (ThreadPage, error)
	ListThreadsForPrincipal(ctx context.Context, principalID string, beforeSequence, snapshotSequence uint64, limit int) (ThreadPage, error)
	CompleteThreadAs(id, principalID string) (ThreadSnapshot, error)
}

type threadRecord struct {
	id           string
	submitter    string
	createdAt    time.Time
	initialJobID string
	jobIDs       []string
	completedAt  *time.Time
	completedBy  string
	doneCycle    int64
}

type threadJobIdentity struct {
	principalID string
	threadID    string
	key         string
}

const threadRecordOverheadBytes = 128

var _ ThreadStore = (*LocalManager)(nil)

// ListThreads returns bounded Thread summaries newest-first by immutable initial
// Job admission sequence. snapshotSequence fixes the upper admission boundary.
func (m *LocalManager) ListThreads(ctx context.Context, beforeSequence, snapshotSequence uint64, limit int) (ThreadPage, error) {
	return m.listThreads(ctx, "", beforeSequence, snapshotSequence, limit)
}

// ListThreadsForPrincipal filters Threads to their submitter before paging.
func (m *LocalManager) ListThreadsForPrincipal(ctx context.Context, principalID string, beforeSequence, snapshotSequence uint64, limit int) (ThreadPage, error) {
	principalID, err := normalizePrincipalID(principalID)
	if err != nil {
		return ThreadPage{}, err
	}
	return m.listThreads(ctx, principalID, beforeSequence, snapshotSequence, limit)
}

func (m *LocalManager) listThreads(ctx context.Context, principalID string, beforeSequence, snapshotSequence uint64, limit int) (ThreadPage, error) {
	if err := validateThreadPageRequest(ctx, beforeSequence, snapshotSequence, limit); err != nil {
		return ThreadPage{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ThreadPage{}, err
	}
	if snapshotSequence == 0 {
		snapshotSequence = m.nextSequence
	}
	page := ThreadPage{Threads: make([]ThreadSummary, 0, limit), SnapshotSequence: snapshotSequence}
	entries := make([]struct {
		sequence uint64
		thread   *threadRecord
	}, 0, limit+1)
	for _, thread := range m.threads {
		if principalID != "" && thread.submitter != principalID {
			continue
		}
		initial := m.jobs[thread.initialJobID]
		if initial == nil || initial.sequence > snapshotSequence || beforeSequence != 0 && initial.sequence >= beforeSequence {
			continue
		}
		entry := struct {
			sequence uint64
			thread   *threadRecord
		}{initial.sequence, thread}
		index := sort.Search(len(entries), func(i int) bool { return entries[i].sequence <= initial.sequence })
		if index >= limit+1 {
			continue
		}
		entries = append(entries, struct {
			sequence uint64
			thread   *threadRecord
		}{})
		copy(entries[index+1:], entries[index:])
		entries[index] = entry
		if len(entries) > limit+1 {
			entries = entries[:limit+1]
		}
	}
	page.HasMore = len(entries) > limit
	if page.HasMore {
		entries = entries[:limit]
	}
	for _, entry := range entries {
		snapshot := m.threadSnapshotLocked(entry.thread).Thread
		summary := ThreadSummary{ID: snapshot.ID, Status: snapshot.Status, CreatedAt: snapshot.CreatedAt, UpdatedAt: snapshot.UpdatedAt}
		if snapshot.CompletedAt != nil {
			completed := *snapshot.CompletedAt
			summary.CompletedAt = &completed
		}
		page.Threads = append(page.Threads, summary)
		page.NextSequence = entry.sequence
	}
	if !page.HasMore {
		page.NextSequence = 0
	}
	return page, nil
}

func validateThreadPageRequest(ctx context.Context, beforeSequence, snapshotSequence uint64, limit int) error {
	maxSequence := uint64(^uint64(0) >> 1)
	if ctx == nil || limit < 1 || limit > MaxThreadListLimit || beforeSequence > maxSequence || snapshotSequence > maxSequence || (snapshotSequence == 0 && beforeSequence != 0) || (snapshotSequence != 0 && beforeSequence > snapshotSequence) {
		return ErrInvalidInput
	}
	return ctx.Err()
}

// CompleteThreadAs explicitly closes an owned Thread without changing its Jobs
// or derived aggregate status. A repeated call is an idempotent read.
func (m *LocalManager) CompleteThreadAs(id, principalID string) (ThreadSnapshot, error) {
	principalID, err := normalizePrincipalID(principalID)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ThreadSnapshot{}, ErrManagerClosed
	}
	thread := m.threads[id]
	if thread == nil || thread.submitter != principalID {
		return ThreadSnapshot{}, ErrNotFound
	}
	if thread.completedAt == nil {
		now := time.Now().UTC()
		thread.completedAt = &now
		thread.completedBy = principalID
	}
	return m.threadSnapshotLocked(thread), nil
}

// AdmitThreadAs atomically creates a Thread and its first Job in the existing
// process-local job queue. Thread idempotency keys have their own namespace and
// are scoped to the submitting principal.
func (m *LocalManager) AdmitThreadAs(idempotencyKey, principalID string, request Request) (ThreadSnapshot, bool, error) {
	key, normalized, err := normalizeAdmission(idempotencyKey, request, m.config.MaxTaskBytes)
	if err != nil {
		return ThreadSnapshot{}, false, err
	}
	if err := validatePublicAdmissionKey(key); err != nil {
		return ThreadSnapshot{}, false, err
	}
	if normalized.Issue != nil || normalized.IssueProvenance != nil {
		return ThreadSnapshot{}, false, ErrInvalidInput
	}
	principalID, err = normalizePrincipalID(principalID)
	if err != nil {
		return ThreadSnapshot{}, false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ThreadSnapshot{}, false, ErrManagerClosed
	}
	identity := idempotencyIdentity{principalID: principalID, key: key}
	if threadID, ok := m.threadIdempotency[identity]; ok {
		thread := m.threads[threadID]
		if thread == nil {
			delete(m.threadIdempotency, identity)
		} else {
			if thread.initialJobID != "" {
				if entry := m.jobs[thread.initialJobID]; entry == nil {
					delete(m.threadIdempotency, identity)
				} else {
					if !sameRequest(entry.snapshot.Request, normalized) {
						return ThreadSnapshot{}, false, ErrIdempotencyConflict
					}
					return m.threadSnapshotLocked(thread), true, nil
				}
			}
		}
	}
	if len(m.queue) >= m.config.QueueCapacity {
		return ThreadSnapshot{}, false, ErrQueueFull
	}

	threadID, err := newJobID()
	if err != nil {
		return ThreadSnapshot{}, false, fmt.Errorf("create thread ID: %w", err)
	}
	jobID, err := newJobID()
	if err != nil {
		return ThreadSnapshot{}, false, fmt.Errorf("create job ID: %w", err)
	}
	now := time.Now().UTC()
	created := Snapshot{ID: jobID, Submitter: principalID, Request: normalized, Status: StatusQueued, CreatedAt: now, UpdatedAt: now}
	entry := &job{snapshot: created, sequence: m.nextSequence + 1, threadID: threadID}
	thread := &threadRecord{id: threadID, submitter: principalID, createdAt: now, initialJobID: jobID, jobIDs: []string{jobID}}
	initialEvent := Event{Type: "queued", Message: "Job admitted"}
	recordCharge := recordBytes(normalized, key, principalID) + threadRecordOverheadBytes
	initialCharge := eventBytes(initialEvent)
	if !m.makeThreadRoomLocked(recordCharge+initialCharge, nil) {
		return ThreadSnapshot{}, false, ErrRegistryFull
	}

	entry.accountedBytes = recordCharge
	m.registryBytes += recordCharge
	m.nextSequence++
	m.jobs[jobID] = entry
	m.threads[threadID] = thread
	m.threadIdempotency[identity] = threadID
	if !m.appendEventLocked(entry, initialEvent.Type, initialEvent.Message) {
		delete(m.jobs, jobID)
		delete(m.threads, threadID)
		delete(m.threadIdempotency, identity)
		m.registryBytes -= entry.accountedBytes
		m.nextSequence--
		return ThreadSnapshot{}, false, ErrRegistryFull
	}
	m.queue = append(m.queue, jobID)
	m.signalLocked()
	return m.threadSnapshotLocked(thread), false, nil
}

// GetThread returns a defensive snapshot of a retained Thread.
func (m *LocalManager) GetThread(id string) (ThreadSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	thread := m.threads[id]
	if thread == nil {
		return ThreadSnapshot{}, ErrNotFound
	}
	return m.threadSnapshotLocked(thread), nil
}

// GetThreadForPrincipal hides missing and differently attributed Threads behind
// the same not-found result.
func (m *LocalManager) GetThreadForPrincipal(id, principalID string) (ThreadSnapshot, error) {
	principalID, err := normalizePrincipalID(principalID)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	thread := m.threads[id]
	if thread == nil || thread.submitter != principalID {
		return ThreadSnapshot{}, ErrNotFound
	}
	return m.threadSnapshotLocked(thread), nil
}

func (m *LocalManager) makeThreadRoomLocked(needed uint64, protected *job) bool {
	limit := uint64(m.config.RegistryBytes)
	if m.registryBytes > limit || needed > limit {
		return false
	}

	terminalCount := 0
	var terminalBytes uint64
	for _, entry := range m.jobs {
		if entry.snapshot.Status.Terminal() && entry != protected {
			terminalCount++
			terminalBytes += entry.accountedBytes
		}
	}
	if len(m.jobs) >= m.config.MaxRecords && terminalCount < len(m.jobs)-m.config.MaxRecords+1 {
		return false
	}
	available := limit - m.registryBytes
	if needed > available && terminalBytes < needed-available {
		return false
	}

	for len(m.jobs) >= m.config.MaxRecords || needed > limit-m.registryBytes {
		if !m.evictOldestTerminalExceptLocked(protected) {
			return false
		}
	}
	return true
}

func (m *LocalManager) threadSnapshotLocked(thread *threadRecord) ThreadSnapshot {
	result := ThreadSnapshot{
		Thread: Thread{
			ID: thread.id, Submitter: thread.submitter, CreatedAt: thread.createdAt,
			UpdatedAt: thread.createdAt, JobIDs: append([]string(nil), thread.jobIDs...), CompletedBy: thread.completedBy,
		},
		Jobs: make([]Snapshot, 0, len(thread.jobIDs)),
	}
	if thread.completedAt != nil {
		completedAt := *thread.completedAt
		result.Thread.CompletedAt = &completedAt
	}
	for _, jobID := range thread.jobIDs {
		entry := m.jobs[jobID]
		if entry == nil {
			continue
		}
		result.Jobs = append(result.Jobs, cloneSnapshot(entry.snapshot))
		if entry.snapshot.UpdatedAt.After(result.Thread.UpdatedAt) {
			result.Thread.UpdatedAt = entry.snapshot.UpdatedAt
		}
	}
	result.Thread.Status = DeriveThreadStatus(result.Jobs, nil)
	return result
}

// DeriveThreadStatus projects the aggregate status from authoritative Job
// snapshots. recoveryNeeded may classify durable running Jobs that require
// operator recovery; nil is appropriate only for stores with no such state.
func DeriveThreadStatus(jobs []Snapshot, recoveryNeeded func(string) bool) ThreadStatus {
	needsAttention := false
	inProgress := false
	for _, job := range jobs {
		if recoveryNeeded != nil && recoveryNeeded(job.ID) {
			needsAttention = true
			continue
		}
		switch job.Status {
		case StatusQueued, StatusRunning:
			inProgress = true
		case StatusSucceeded, StatusFailed, StatusCanceled:
		default:
			needsAttention = true
		}
	}
	switch {
	case needsAttention || len(jobs) == 0:
		return ThreadStatusNeedAttention
	case inProgress:
		return ThreadStatusInProgress
	default:
		return ThreadStatusDone
	}
}
