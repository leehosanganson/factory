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
	s.recoveryMu.RLock()
	_, known := s.recoveryNeeded[id]
	s.recoveryMu.RUnlock()
	if known || s.checkOpen() != nil {
		return known
	}
	var needed bool
	if err := s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM factory_jobs j LEFT JOIN factory_rest_worker_ownership o ON o.job_id=j.id WHERE j.id=$1 AND j.status='running' AND (o.job_id IS NULL OR o.lease_until<=clock_timestamp()))`, id).Scan(&needed); err != nil || !needed {
		return false
	}
	s.recoveryMu.Lock()
	s.recoveryNeeded[id] = struct{}{}
	s.recoveryMu.Unlock()
	return true
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
	if err := tx.Commit(); err != nil {
		return errors.New("commit PostgreSQL provider reconciliation")
	}
	delete(s.recoveryNeeded, id)
	return nil
}
