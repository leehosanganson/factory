package restjobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// AdmitThreadJobAs atomically adds a Job to an owned Thread. Its idempotency
// namespace is scoped by principal and Thread, separate from standalone Jobs
// and Thread creation.
func (m *LocalManager) AdmitThreadJobAs(threadID, idempotencyKey, principalID string, request Request) (ThreadSnapshot, Snapshot, bool, error) {
	key, normalized, err := normalizeAdmission(idempotencyKey, request, m.config.MaxTaskBytes)
	if err != nil || normalized.Issue != nil || normalized.IssueProvenance != nil {
		return ThreadSnapshot{}, Snapshot{}, false, ErrInvalidInput
	}
	principalID, err = normalizePrincipalID(principalID)
	if err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ThreadSnapshot{}, Snapshot{}, false, ErrManagerClosed
	}
	thread := m.threads[threadID]
	if thread == nil || thread.submitter != principalID {
		return ThreadSnapshot{}, Snapshot{}, false, ErrNotFound
	}
	identity := threadJobIdentity{principalID: principalID, threadID: threadID, key: key}
	if jobID := m.threadJobKeys[identity]; jobID != "" {
		entry := m.jobs[jobID]
		if entry == nil {
			return ThreadSnapshot{}, Snapshot{}, false, fmt.Errorf("Thread member idempotency invariant violated")
		}
		if !sameRequest(entry.snapshot.Request, normalized) {
			return ThreadSnapshot{}, Snapshot{}, false, ErrIdempotencyConflict
		}
		return m.threadSnapshotLocked(thread), cloneSnapshot(entry.snapshot), true, nil
	}
	if thread.completedAt != nil {
		return ThreadSnapshot{}, Snapshot{}, false, ErrThreadClosed
	}
	if len(m.queue) >= m.config.QueueCapacity {
		return ThreadSnapshot{}, Snapshot{}, false, ErrQueueFull
	}
	jobID, err := newJobID()
	if err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, fmt.Errorf("create job ID: %w", err)
	}
	now := time.Now().UTC()
	created := Snapshot{ID: jobID, Submitter: principalID, Request: normalized, Status: StatusQueued, CreatedAt: now, UpdatedAt: now}
	entry := &job{snapshot: created, sequence: m.nextSequence + 1, threadID: threadID}
	initialEvent := Event{Type: "queued", Message: "Job admitted to Thread"}
	charge := recordBytes(normalized, key, principalID) + threadRecordOverheadBytes + eventBytes(initialEvent)
	if !m.makeThreadRoomLocked(charge, m.jobs[thread.initialJobID]) {
		return ThreadSnapshot{}, Snapshot{}, false, ErrRegistryFull
	}
	entry.accountedBytes = charge
	m.registryBytes += charge
	m.nextSequence++
	m.jobs[jobID] = entry
	thread.jobIDs = append(thread.jobIDs, jobID)
	m.threadJobKeys[identity] = jobID
	if !m.appendEventLocked(entry, initialEvent.Type, initialEvent.Message) {
		delete(m.jobs, jobID)
		thread.jobIDs = thread.jobIDs[:len(thread.jobIDs)-1]
		delete(m.threadJobKeys, identity)
		m.registryBytes -= entry.accountedBytes
		m.nextSequence--
		return ThreadSnapshot{}, Snapshot{}, false, ErrRegistryFull
	}
	m.queue = append(m.queue, jobID)
	m.signalLocked()
	return m.threadSnapshotLocked(thread), cloneSnapshot(created), false, nil
}

func addThreadJobSQLite(ctx context.Context, db *sql.DB, config Config, threadID, key, principalID string, request Request) (ThreadSnapshot, Snapshot, bool, error) {
	key, request, err := normalizeThreadJobAdmission(config, threadID, key, principalID, request)
	if err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, err
	}
	ctx, cancel := context.WithTimeout(ctx, sqliteBusyTimeout)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("SQLite job store unavailable")
	}
	defer tx.Rollback()
	// Acquire the SQLite writer lock before inspecting membership or completion,
	// serializing this admission with explicit completion across connections.
	if result, err := tx.ExecContext(ctx, `UPDATE factory_threads SET completed_at=completed_at WHERE id=?`, threadID); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("lock SQLite Thread admission")
	} else if rows, _ := result.RowsAffected(); rows == 0 {
		return ThreadSnapshot{}, Snapshot{}, false, ErrNotFound
	}
	var owner string
	if err := tx.QueryRowContext(ctx, `SELECT submitter FROM factory_threads WHERE id=?`, threadID).Scan(&owner); errors.Is(err, sql.ErrNoRows) || err == nil && owner != principalID {
		return ThreadSnapshot{}, Snapshot{}, false, ErrNotFound
	} else if err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("read SQLite Thread owner")
	}
	var jobID, raw string
	err = tx.QueryRowContext(ctx, `SELECT job_id,request_json FROM factory_thread_job_idempotency WHERE principal_id=? AND thread_id=? AND key=?`, principalID, threadID, key).Scan(&jobID, &raw)
	if err == nil {
		var existing Request
		if json.Unmarshal([]byte(raw), &existing) != nil {
			return ThreadSnapshot{}, Snapshot{}, false, errors.New("SQLite Thread member record is invalid")
		}
		if !sameRequest(existing, request) {
			return ThreadSnapshot{}, Snapshot{}, false, ErrIdempotencyConflict
		}
		snapshot, _, err := readThreadSnapshot(ctx, tx, threadID, principalID, false)
		if err != nil {
			return ThreadSnapshot{}, Snapshot{}, false, err
		}
		added, err := readJobSnapshotSQLite(ctx, tx, jobID)
		if err != nil {
			return ThreadSnapshot{}, Snapshot{}, false, fmt.Errorf("read replayed SQLite Thread Job %s: %w", jobID, err)
		}
		if err := tx.Commit(); err != nil {
			return ThreadSnapshot{}, Snapshot{}, false, errors.New("finish SQLite Thread member replay")
		}
		return snapshot, added, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("read SQLite Thread member idempotency record")
	}
	var retained, active, position int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM factory_jobs`).Scan(&retained); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("check SQLite retained-record capacity")
	}
	var completedAt sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT completed_at FROM factory_threads WHERE id=?`, threadID).Scan(&completedAt); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("read SQLite Thread completion")
	}
	if completedAt.Valid {
		return ThreadSnapshot{}, Snapshot{}, false, ErrThreadClosed
	}
	if retained >= config.MaxRecords {
		return ThreadSnapshot{}, Snapshot{}, false, ErrRegistryFull
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM factory_jobs WHERE status IN ('queued','running')`).Scan(&active); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("check SQLite job capacity")
	}
	if active >= config.QueueCapacity+config.MaxConcurrentJobs {
		return ThreadSnapshot{}, Snapshot{}, false, ErrQueueFull
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(position),0)+1 FROM factory_thread_jobs WHERE thread_id=?`, threadID).Scan(&position); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("allocate SQLite Thread member position")
	}
	var sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(queue_sequence),0)+1 FROM factory_jobs`).Scan(&sequence); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("allocate SQLite job queue position")
	}
	if position == 1 {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("SQLite Thread has no initial membership")
	}
	jobID, err = newJobID()
	if err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("create SQLite Thread member Job ID")
	}
	now := time.Now().UTC()
	stamp := now.Format(time.RFC3339Nano)
	encoded, err := json.Marshal(request)
	if err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, ErrInvalidInput
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_jobs(id,request_json,status,created_at,updated_at,queue_sequence,history_truncated,submitter) VALUES(?,?,?,?,?,?,0,?)`, jobID, string(encoded), StatusQueued, stamp, stamp, sequence, principalID); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("persist SQLite Thread member Job")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_thread_jobs(thread_id,job_id,position,created_at) VALUES(?,?,?,?)`, threadID, jobID, position, stamp); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("persist SQLite Thread membership")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_thread_job_idempotency(principal_id,thread_id,key,job_id,request_json) VALUES(?,?,?,?,?)`, principalID, threadID, key, jobID, string(encoded)); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("persist SQLite Thread member idempotency key")
	}
	if err := insertEvent(ctx, tx, jobID, "queued", "Job admitted to Thread", now, config.MaxEventsPerJob); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("persist SQLite Thread member history")
	}
	snapshot, _, err := readThreadSnapshot(ctx, tx, threadID, principalID, false)
	if err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, err
	}
	added, err := readJobSnapshotSQLite(ctx, tx, jobID)
	if err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("commit SQLite Thread member admission")
	}
	return snapshot, added, false, nil
}

func addThreadJobPostgres(ctx context.Context, db *sql.DB, config Config, threadID, key, principalID string, request Request) (ThreadSnapshot, Snapshot, bool, error) {
	key, request, err := normalizeThreadJobAdmission(config, threadID, key, principalID, request)
	if err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("PostgreSQL job store unavailable")
	}
	defer tx.Rollback()
	var owner string
	var completedAt sql.NullTime
	if err := tx.QueryRowContext(ctx, `SELECT submitter,completed_at FROM factory_threads WHERE id=$1 FOR UPDATE`, threadID).Scan(&owner, &completedAt); errors.Is(err, sql.ErrNoRows) || err == nil && owner != principalID {
		return ThreadSnapshot{}, Snapshot{}, false, ErrNotFound
	} else if err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("lock PostgreSQL Thread member admission")
	}
	var jobID, raw string
	err = tx.QueryRowContext(ctx, `SELECT job_id,request_json FROM factory_thread_job_idempotency WHERE principal_id=$1 AND thread_id=$2 AND key=$3`, principalID, threadID, key).Scan(&jobID, &raw)
	if err == nil {
		var existing Request
		if json.Unmarshal([]byte(raw), &existing) != nil {
			return ThreadSnapshot{}, Snapshot{}, false, errors.New("PostgreSQL Thread member record is invalid")
		}
		if !sameRequest(existing, request) {
			return ThreadSnapshot{}, Snapshot{}, false, ErrIdempotencyConflict
		}
		snapshot, err := (&PostgresStore{db: db, config: config, recoveryNeeded: make(map[string]struct{})}).threadSnapshotTx(ctx, tx, threadID, principalID)
		if err != nil {
			return ThreadSnapshot{}, Snapshot{}, false, err
		}
		var added Snapshot
		for _, member := range snapshot.Jobs {
			if member.ID == jobID {
				added = member
				break
			}
		}
		if added.ID == "" {
			return ThreadSnapshot{}, Snapshot{}, false, ErrNotFound
		}
		if err := tx.Commit(); err != nil {
			return ThreadSnapshot{}, Snapshot{}, false, errors.New("finish PostgreSQL Thread member replay")
		}
		return snapshot, added, true, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return ThreadSnapshot{}, Snapshot{}, false, fmt.Errorf("read PostgreSQL Thread member idempotency record: %w", err)
	}
	var retained, active, position int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(*) FILTER (WHERE status IN ('queued','running')) FROM factory_jobs`).Scan(&retained, &active); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("check PostgreSQL job capacity")
	}
	if completedAt.Valid {
		return ThreadSnapshot{}, Snapshot{}, false, ErrThreadClosed
	}
	if retained >= config.MaxRecords {
		return ThreadSnapshot{}, Snapshot{}, false, ErrRegistryFull
	}
	if active >= config.QueueCapacity+config.MaxConcurrentJobs {
		return ThreadSnapshot{}, Snapshot{}, false, ErrQueueFull
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(position),0)+1 FROM factory_thread_jobs WHERE thread_id=$1`, threadID).Scan(&position); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("allocate PostgreSQL Thread member position")
	}
	if position <= 1 {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("PostgreSQL Thread has no initial membership")
	}
	jobID, err = newJobID()
	if err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("create PostgreSQL Thread member Job ID")
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, ErrInvalidInput
	}
	var queueSequence int64
	if err := tx.QueryRowContext(ctx, `SELECT nextval('factory_rest_job_sequence')`).Scan(&queueSequence); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("allocate PostgreSQL queue position")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_jobs(id,request_json,status,created_at,updated_at,queue_sequence,history_truncated,submitter) VALUES($1,$2,'queued',clock_timestamp(),clock_timestamp(),$3,false,$4)`, jobID, string(encoded), queueSequence, principalID); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("persist PostgreSQL Thread member Job")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_thread_jobs(thread_id,job_id,position) VALUES($1,$2,$3)`, threadID, jobID, position); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("persist PostgreSQL Thread membership")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_thread_job_idempotency(principal_id,thread_id,key,job_id,request_json) VALUES($1,$2,$3,$4,$5)`, principalID, threadID, key, jobID, string(encoded)); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("persist PostgreSQL Thread member idempotency key")
	}
	if err := insertPostgresEvent(ctx, tx, jobID, "queued", "Job admitted to Thread", config.MaxEventsPerJob); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("persist PostgreSQL Thread member history")
	}
	if err := tx.Commit(); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, errors.New("commit PostgreSQL Thread member admission")
	}
	// Read the member snapshots after commit using the store's stable read path.
	store := &PostgresStore{db: db, config: config, recoveryNeeded: make(map[string]struct{})}
	snapshot, err := store.GetThreadForPrincipal(threadID, principalID)
	if err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, err
	}
	var added Snapshot
	for _, member := range snapshot.Jobs {
		if member.ID == jobID {
			added = member
			break
		}
	}
	if added.ID == "" {
		return ThreadSnapshot{}, Snapshot{}, false, ErrNotFound
	}
	return snapshot, added, false, nil
}

func normalizeThreadJobAdmission(config Config, threadID, key, principalID string, request Request) (string, Request, error) {
	key, normalized, err := normalizeAdmission(key, request, config.MaxTaskBytes)
	if err != nil || normalized.Issue != nil || normalized.IssueProvenance != nil || threadID == "" {
		return "", Request{}, ErrInvalidInput
	}
	principalID, err = normalizePrincipalID(principalID)
	if err != nil {
		return "", Request{}, err
	}
	return key, normalized, nil
}

func readThreadSnapshot(ctx context.Context, tx *sql.Tx, threadID, principalID string, postgres bool) (ThreadSnapshot, Snapshot, error) {
	placeholder := "?"
	if postgres {
		placeholder = "$1"
	}
	query := `SELECT job_id FROM factory_thread_jobs WHERE thread_id=` + placeholder + ` ORDER BY position`
	rows, err := tx.QueryContext(ctx, query, threadID)
	if err != nil {
		return ThreadSnapshot{}, Snapshot{}, errors.New("read Thread membership: " + err.Error())
	}
	var jobIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return ThreadSnapshot{}, Snapshot{}, errors.New("scan Thread membership")
		}
		jobIDs = append(jobIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ThreadSnapshot{}, Snapshot{}, errors.New("read Thread membership")
	}
	if err := rows.Close(); err != nil {
		return ThreadSnapshot{}, Snapshot{}, errors.New("close Thread membership")
	}
	jobs := make([]Snapshot, 0, len(jobIDs))
	for _, id := range jobIDs {
		var job Snapshot
		if postgres {
			job, err = readJobSnapshotPostgres(ctx, tx, id)
		} else {
			job, err = readJobSnapshotSQLite(ctx, tx, id)
		}
		if err != nil {
			return ThreadSnapshot{}, Snapshot{}, fmt.Errorf("read Thread member %s: %w", id, err)
		}
		jobs = append(jobs, job)
	}
	threadQuery := `SELECT id,submitter,created_at,completed_at,completed_by FROM factory_threads WHERE id=?`
	if postgres {
		threadQuery = `SELECT id,submitter,created_at,completed_at,completed_by FROM factory_threads WHERE id=$1`
	}
	var thread Thread
	if postgres {
		if err := tx.QueryRowContext(ctx, threadQuery, threadID).Scan(&thread.ID, &thread.Submitter, &thread.CreatedAt, &thread.CompletedAt, &thread.CompletedBy); errors.Is(err, sql.ErrNoRows) {
			return ThreadSnapshot{}, Snapshot{}, ErrNotFound
		} else if err != nil {
			return ThreadSnapshot{}, Snapshot{}, errors.New("read Thread record")
		}
	} else {
		var createdAt string
		var completedAt, completedBy sql.NullString
		if err := tx.QueryRowContext(ctx, threadQuery, threadID).Scan(&thread.ID, &thread.Submitter, &createdAt, &completedAt, &completedBy); errors.Is(err, sql.ErrNoRows) {
			return ThreadSnapshot{}, Snapshot{}, ErrNotFound
		} else if err != nil {
			return ThreadSnapshot{}, Snapshot{}, errors.New("read SQLite Thread record")
		}
		thread.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			return ThreadSnapshot{}, Snapshot{}, errors.New("decode SQLite Thread timestamp")
		}
		if completedBy.Valid {
			thread.CompletedBy = completedBy.String
		}
		if completedAt.Valid {
			completed, parseErr := time.Parse(time.RFC3339Nano, completedAt.String)
			if parseErr != nil {
				return ThreadSnapshot{}, Snapshot{}, errors.New("decode SQLite Thread completion timestamp")
			}
			thread.CompletedAt = &completed
		}
	}
	thread.JobIDs = make([]string, len(jobs))
	thread.UpdatedAt = thread.CreatedAt
	for i, job := range jobs {
		thread.JobIDs[i] = job.ID
		if job.UpdatedAt.After(thread.UpdatedAt) {
			thread.UpdatedAt = job.UpdatedAt
		}
	}
	thread.Status = DeriveThreadStatus(jobs, nil)
	if len(jobs) == 0 {
		return ThreadSnapshot{}, Snapshot{}, errors.New("Thread has no members")
	}
	if principalID != "" && thread.Submitter != principalID {
		return ThreadSnapshot{}, Snapshot{}, ErrNotFound
	}
	var last Snapshot
	if len(jobs) > 0 {
		last = jobs[len(jobs)-1]
	}
	return ThreadSnapshot{Thread: thread, Jobs: jobs}, last, nil
}

func readJobSnapshotSQLite(ctx context.Context, tx *sql.Tx, id string) (Snapshot, error) {
	snapshot, err := scanSnapshot(tx.QueryRowContext(ctx, `SELECT j.id,j.request_json,j.status,j.created_at,j.updated_at,j.history_truncated,j.cancellation_requested,j.submitter,(SELECT provider_json FROM factory_job_provider_outcomes WHERE job_id=j.id),(SELECT evidence_json FROM factory_job_verification_evidence WHERE job_id=j.id) FROM factory_jobs j WHERE j.id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, ErrNotFound
	}
	if err != nil {
		return Snapshot{}, errors.New("read SQLite Thread member Job")
	}
	return snapshot, nil
}

func readJobSnapshotPostgres(ctx context.Context, tx *sql.Tx, id string) (Snapshot, error) {
	return scanPostgresSnapshot(tx.QueryRowContext(ctx, pgSnapshotQuery()+`WHERE j.id=$1`, id))
}
