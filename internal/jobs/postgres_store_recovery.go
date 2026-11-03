package restjobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// RecoveryNeeded reports whether this store instance opened with an interrupted
// running job (or subsequently resolved it). A live, unexpired worker lease is
// not classified as interrupted.
func (s *PostgresStore) RecoveryNeeded(id string) bool {
	return s.recoveryNeededWithQuery(context.Background(), s.db, id)
}

type postgresRecoveryQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *PostgresStore) recoveryNeededWithQuery(ctx context.Context, queryer postgresRecoveryQueryer, id string) bool {
	s.recoveryMu.RLock()
	_, known := s.recoveryNeeded[id]
	s.recoveryMu.RUnlock()
	if s.checkOpen() != nil {
		return known
	}
	var needed bool
	query := `SELECT EXISTS(SELECT 1 FROM factory_jobs j LEFT JOIN factory_rest_worker_ownership o ON o.job_id=j.id WHERE j.id=$1 AND j.status='running' AND (o.job_id IS NULL OR o.lease_until<=clock_timestamp()))`
	if known {
		// Once classified, remain conservative while the job is still running,
		// but observe terminal transitions made by peer worker/store instances.
		query = `SELECT EXISTS(SELECT 1 FROM factory_jobs WHERE id=$1 AND status='running')`
	}
	if err := queryer.QueryRowContext(ctx, query, id).Scan(&needed); err != nil {
		return known
	}
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	if needed {
		s.recoveryNeeded[id] = struct{}{}
	} else {
		delete(s.recoveryNeeded, id)
	}
	return needed
}

// AdmitThreadAs atomically persists a Thread and its initial queued Job. Thread
// idempotency keys use a separate principal-scoped namespace from job keys.
func (s *PostgresStore) AdmitThreadAs(idempotencyKey, principalID string, request Request) (ThreadSnapshot, bool, error) {
	s.admitMu.Lock()
	defer s.admitMu.Unlock()
	key, normalized, err := normalizeAdmission(idempotencyKey, request, s.config.MaxTaskBytes)
	if err != nil {
		return ThreadSnapshot{}, false, err
	}
	if normalized.Issue != nil || normalized.IssueProvenance != nil {
		return ThreadSnapshot{}, false, ErrInvalidInput
	}
	principalID, err = normalizePrincipalID(principalID)
	if err != nil {
		return ThreadSnapshot{}, false, err
	}
	if err := s.checkAccepting(); err != nil {
		return ThreadSnapshot{}, false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ThreadSnapshot{}, false, errors.New("PostgreSQL job store unavailable")
	}
	defer tx.Rollback()
	// Serialize capacity checks and idempotency decisions across shared stores.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, postgresStoreMigrationLock+1); err != nil {
		return ThreadSnapshot{}, false, errors.New("lock PostgreSQL admission")
	}
	var threadID, raw string
	err = tx.QueryRowContext(ctx, `SELECT thread_id,request_json FROM factory_thread_idempotency WHERE principal_id=$1 AND key=$2`, principalID, key).Scan(&threadID, &raw)
	if err == nil {
		var existing Request
		if json.Unmarshal([]byte(raw), &existing) != nil {
			return ThreadSnapshot{}, false, errors.New("PostgreSQL Thread record is invalid")
		}
		if !sameRequest(existing, normalized) {
			return ThreadSnapshot{}, false, ErrIdempotencyConflict
		}
		snapshot, err := s.threadSnapshotTx(ctx, tx, threadID, "")
		if err != nil {
			return ThreadSnapshot{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return ThreadSnapshot{}, false, errors.New("finish PostgreSQL Thread idempotency lookup")
		}
		return snapshot, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ThreadSnapshot{}, false, errors.New("read PostgreSQL Thread idempotency record")
	}

	var retained, active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(*) FILTER (WHERE status IN ('queued','running')) FROM factory_jobs`).Scan(&retained, &active); err != nil {
		return ThreadSnapshot{}, false, errors.New("check PostgreSQL job capacity")
	}
	if retained >= s.config.MaxRecords {
		return ThreadSnapshot{}, false, ErrRegistryFull
	}
	if active >= s.config.QueueCapacity+s.config.MaxConcurrentJobs {
		return ThreadSnapshot{}, false, ErrQueueFull
	}

	threadID, err = newJobID()
	if err != nil {
		return ThreadSnapshot{}, false, errors.New("create PostgreSQL Thread identifier")
	}
	jobID, err := newJobID()
	if err != nil {
		return ThreadSnapshot{}, false, errors.New("create PostgreSQL job identifier")
	}
	var sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT nextval('factory_rest_job_sequence')`).Scan(&sequence); err != nil {
		return ThreadSnapshot{}, false, errors.New("allocate PostgreSQL queue position")
	}
	requestJSON, err := json.Marshal(normalized)
	if err != nil {
		return ThreadSnapshot{}, false, ErrInvalidInput
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_jobs(id,request_json,status,created_at,updated_at,queue_sequence,submitter) VALUES($1,$2,'queued',clock_timestamp(),clock_timestamp(),$3,$4)`, jobID, string(requestJSON), sequence, principalID); err != nil {
		return ThreadSnapshot{}, false, errors.New("persist PostgreSQL Thread job")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_threads(id,submitter,created_at,initial_job_id) VALUES($1,$2,clock_timestamp(),$3)`, threadID, principalID, jobID); err != nil {
		return ThreadSnapshot{}, false, errors.New("persist PostgreSQL Thread")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_thread_jobs(thread_id,job_id,position) VALUES($1,$2,1)`, threadID, jobID); err != nil {
		return ThreadSnapshot{}, false, errors.New("persist initial PostgreSQL Thread membership")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_thread_idempotency(principal_id,key,thread_id,request_json) VALUES($1,$2,$3,$4)`, principalID, key, threadID, string(requestJSON)); err != nil {
		return ThreadSnapshot{}, false, errors.New("persist PostgreSQL Thread idempotency key")
	}
	if err := insertPostgresEvent(ctx, tx, jobID, "queued", "Job admitted", s.config.MaxEventsPerJob); err != nil {
		return ThreadSnapshot{}, false, errors.New("persist PostgreSQL Thread admission history")
	}
	if err := tx.Commit(); err != nil {
		return ThreadSnapshot{}, false, errors.New("commit PostgreSQL Thread admission")
	}
	snapshot, err := s.getThread(threadID, "")
	if err != nil {
		return ThreadSnapshot{}, false, err
	}
	return snapshot, false, nil
}

// GetThread returns a defensive snapshot of a retained PostgreSQL Thread.
func (s *PostgresStore) GetThread(id string) (ThreadSnapshot, error) {
	return s.getThread(id, "")
}

// GetThreadForPrincipal hides missing and differently attributed Threads behind
// the same not-found result.
func (s *PostgresStore) GetThreadForPrincipal(id, principalID string) (ThreadSnapshot, error) {
	principalID, err := normalizePrincipalID(principalID)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	return s.getThread(id, principalID)
}

func (s *PostgresStore) getThread(id, principalID string) (ThreadSnapshot, error) {
	if err := s.checkOpen(); err != nil {
		return ThreadSnapshot{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ThreadSnapshot{}, errors.New("read PostgreSQL Thread")
	}
	defer tx.Rollback()
	snapshot, err := s.threadSnapshotTx(ctx, tx, id, principalID)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return ThreadSnapshot{}, errors.New("finish PostgreSQL Thread read")
	}
	return snapshot, nil
}

// Legacy single-member Thread reader retained temporarily for historical migration diagnostics.
func (s *PostgresStore) threadSnapshotTxLegacy(ctx context.Context, tx *sql.Tx, id, principalID string) (ThreadSnapshot, error) {
	query := `SELECT id,submitter,created_at,initial_job_id FROM factory_threads WHERE id=$1`
	args := []any{id}
	if principalID != "" {
		query += ` AND submitter=$2`
		args = append(args, principalID)
	}
	var thread Thread
	var jobID string
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&thread.ID, &thread.Submitter, &thread.CreatedAt, &jobID); errors.Is(err, sql.ErrNoRows) {
		return ThreadSnapshot{}, ErrNotFound
	} else if err != nil {
		return ThreadSnapshot{}, errors.New("read PostgreSQL Thread record")
	}
	job, err := scanPostgresSnapshot(tx.QueryRowContext(ctx, pgSnapshotQuery()+`WHERE j.id=$1`, jobID))
	if errors.Is(err, sql.ErrNoRows) {
		return ThreadSnapshot{}, errors.New("PostgreSQL Thread job is missing")
	} else if err != nil {
		return ThreadSnapshot{}, errors.New("read PostgreSQL Thread job")
	}
	thread.JobIDs = []string{jobID}
	thread.UpdatedAt = thread.CreatedAt
	if job.UpdatedAt.After(thread.UpdatedAt) {
		thread.UpdatedAt = job.UpdatedAt
	}
	thread.Status = DeriveThreadStatus([]Snapshot{job}, func(id string) bool {
		return s.recoveryNeededWithQuery(ctx, tx, id)
	})
	return ThreadSnapshot{Thread: thread, Jobs: []Snapshot{job}}, nil
}

// RecordProviderAttempt stores the provider-write identity for an unfenced
// compatibility worker. Attempt-aware workers should use the Store method.
func (s *PostgresStore) RecordProviderAttempt(id string, attempt ProviderAttempt) error {
	if validateProviderAttempt(attempt) != nil || attempt.Branch != "factory/job/"+id || !validCommitID(attempt.Commit) {
		return ErrInvalidInput
	}
	return s.recordProviderAttempt(id, nil, attempt)
}

// MarkProviderAttemptUncertain marks the provider-write outcome uncertain for
// an unfenced compatibility worker.
func (s *PostgresStore) MarkProviderAttemptUncertain(id string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.markProviderUncertain(id, nil)
}

// ProviderAttempt returns the durable provider attempt identity.
func (s *PostgresStore) ProviderAttempt(id string) (ProviderAttempt, error) {
	if err := s.checkOpen(); err != nil {
		return ProviderAttempt{}, err
	}
	var raw string
	if err := s.db.QueryRow(`SELECT attempt_json FROM factory_job_provider_attempts WHERE job_id=$1`, id).Scan(&raw); errors.Is(err, sql.ErrNoRows) {
		var exists bool
		if err := s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM factory_jobs WHERE id=$1)`, id).Scan(&exists); err != nil {
			return ProviderAttempt{}, errors.New("read PostgreSQL provider attempt")
		}
		if !exists {
			return ProviderAttempt{}, ErrNotFound
		}
		return ProviderAttempt{}, ErrInvalidTransition
	} else if err != nil {
		return ProviderAttempt{}, errors.New("read PostgreSQL provider attempt")
	}
	var attempt ProviderAttempt
	if json.Unmarshal([]byte(raw), &attempt) != nil || validateProviderAttempt(attempt) != nil || attempt.Branch != "factory/job/"+id || !validCommitID(attempt.Commit) {
		return ProviderAttempt{}, errors.New("PostgreSQL provider attempt is invalid")
	}
	return attempt, nil
}

// ReconcileProviderOutcome atomically records an operator-confirmed read-only
// provider result. The caller performs live-provider confirmation; this method
// enforces identity and recovery eligibility before changing the shared status.
func (s *PostgresStore) ReconcileProviderOutcome(id string, outcome ProviderOutcome) error {
	if validateProviderOutcome(outcome) != nil {
		return ErrInvalidInput
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	encoded, _ := json.Marshal(outcome)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("PostgreSQL job store unavailable")
	}
	defer tx.Rollback()
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM factory_jobs WHERE id=$1 FOR UPDATE`, id).Scan(&status); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return errors.New("read PostgreSQL job state")
	}
	threadID, wasDone, err := postgresThreadDoneTransition(ctx, tx, id)
	if err != nil {
		return err
	}
	if status == string(StatusSucceeded) {
		var prior string
		if err := tx.QueryRowContext(ctx, `SELECT provider_json FROM factory_job_provider_outcomes WHERE job_id=$1`, id).Scan(&prior); err != nil || prior != string(encoded) {
			return ErrInvalidTransition
		}
		return tx.Commit()
	}
	_, interrupted := s.recoveryNeeded[id]
	if status != string(StatusFailed) && (status != string(StatusRunning) || !interrupted) {
		return ErrInvalidTransition
	}
	var attemptJSON string
	attemptErr := tx.QueryRowContext(ctx, `SELECT attempt_json FROM factory_job_provider_attempts WHERE job_id=$1`, id).Scan(&attemptJSON)
	var attempt ProviderAttempt
	if attemptErr == nil {
		if json.Unmarshal([]byte(attemptJSON), &attempt) != nil || validateProviderAttempt(attempt) != nil || attempt.Branch != "factory/job/"+id || !validCommitID(attempt.Commit) {
			return ErrInvalidInput
		}
	} else if !errors.Is(attemptErr, sql.ErrNoRows) {
		return errors.New("read PostgreSQL provider attempt")
	} else {
		var prior string
		if err := tx.QueryRowContext(ctx, `SELECT provider_json FROM factory_job_provider_outcomes WHERE job_id=$1`, id).Scan(&prior); err != nil {
			return ErrInvalidTransition
		}
		var persisted ProviderOutcome
		if json.Unmarshal([]byte(prior), &persisted) != nil || validateProviderOutcome(persisted) != nil {
			return errors.New("PostgreSQL provider outcome is invalid")
		}
		if persisted != outcome {
			return ErrInvalidInput
		}
		attempt = ProviderAttempt{Provider: persisted.Provider, Repository: persisted.Repository, Branch: persisted.Branch, Commit: persisted.Commit}
	}
	if outcome.Provider != attempt.Provider || outcome.Repository != attempt.Repository || outcome.Branch != attempt.Branch || outcome.Commit != attempt.Commit {
		return ErrInvalidInput
	}
	var previous string
	previousErr := tx.QueryRowContext(ctx, `SELECT provider_json FROM factory_job_provider_outcomes WHERE job_id=$1`, id).Scan(&previous)
	if previousErr == nil && previous != string(encoded) {
		return ErrInvalidTransition
	}
	if previousErr != nil && !errors.Is(previousErr, sql.ErrNoRows) {
		return errors.New("read PostgreSQL provider outcome")
	}
	if previousErr != nil {
		if _, err := tx.ExecContext(ctx, `INSERT INTO factory_job_provider_outcomes(job_id,provider_json,updated_at) VALUES($1,$2,clock_timestamp())`, id, string(encoded)); err != nil {
			return errors.New("record PostgreSQL provider outcome")
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE factory_jobs SET status='succeeded',cancellation_requested=false,updated_at=clock_timestamp() WHERE id=$1 AND status=$2`, id, status); err != nil {
		return errors.New("finish reconciled PostgreSQL job")
	}
	if err := insertPostgresEvent(ctx, tx, id, string(StatusSucceeded), "Job finished", s.config.MaxEventsPerJob); err != nil {
		return errors.New("record PostgreSQL reconciliation completion")
	}
	if err := insertPostgresEvent(ctx, tx, id, "provider_reconciled", "Operator confirmed provider outcome", s.config.MaxEventsPerJob); err != nil {
		return errors.New("record PostgreSQL reconciliation history")
	}
	if err := recordPostgresDoneTransition(ctx, tx, threadID, wasDone); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return errors.New("commit PostgreSQL provider reconciliation")
	}
	delete(s.recoveryNeeded, id)
	return nil
}
