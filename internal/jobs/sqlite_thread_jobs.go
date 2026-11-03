package restjobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

func (s *SQLiteStore) AdmitThreadJobAs(threadID, key, principalID string, request Request) (ThreadSnapshot, Snapshot, bool, error) {
	if err := s.checkAccepting(); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, err
	}
	return addThreadJobSQLite(context.Background(), s.db, s.config, threadID, key, principalID, request)
}

func (s *SQLiteStore) threadSnapshotTx(ctx context.Context, tx *sql.Tx, id, principalID string) (ThreadSnapshot, error) {
	query := `SELECT id,submitter,created_at,completed_at,completed_by FROM factory_threads WHERE id=?`
	args := []any{id}
	if principalID != "" {
		query += ` AND submitter=?`
		args = append(args, principalID)
	}
	var thread Thread
	var createdAt string
	var completedAt, completedBy sql.NullString
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&thread.ID, &thread.Submitter, &createdAt, &completedAt, &completedBy); errors.Is(err, sql.ErrNoRows) {
		return ThreadSnapshot{}, ErrNotFound
	} else if err != nil {
		return ThreadSnapshot{}, errors.New("read SQLite Thread record")
	}
	var err error
	thread.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return ThreadSnapshot{}, errors.New("decode SQLite Thread timestamp")
	}
	if completedBy.Valid {
		thread.CompletedBy = completedBy.String
	}
	if completedAt.Valid {
		completed, parseErr := time.Parse(time.RFC3339Nano, completedAt.String)
		if parseErr != nil {
			return ThreadSnapshot{}, errors.New("decode SQLite Thread completion timestamp")
		}
		thread.CompletedAt = &completed
	}
	rows, err := tx.QueryContext(ctx, `SELECT job_id FROM factory_thread_jobs WHERE thread_id=? ORDER BY position`, id)
	if err != nil {
		return ThreadSnapshot{}, errors.New("read SQLite Thread membership")
	}
	var jobIDs []string
	for rows.Next() {
		var jobID string
		if err := rows.Scan(&jobID); err != nil {
			rows.Close()
			return ThreadSnapshot{}, errors.New("scan SQLite Thread membership")
		}
		jobIDs = append(jobIDs, jobID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ThreadSnapshot{}, errors.New("read SQLite Thread membership")
	}
	if err := rows.Close(); err != nil {
		return ThreadSnapshot{}, errors.New("close SQLite Thread membership")
	}
	jobs := make([]Snapshot, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		job, err := scanSnapshot(tx.QueryRowContext(ctx, `SELECT j.id,j.request_json,j.status,j.created_at,j.updated_at,j.history_truncated,j.cancellation_requested,j.submitter,(SELECT provider_json FROM factory_job_provider_outcomes WHERE job_id=j.id),(SELECT evidence_json FROM factory_job_verification_evidence WHERE job_id=j.id) FROM factory_jobs j WHERE j.id=?`, jobID))
		if err != nil {
			return ThreadSnapshot{}, fmt.Errorf("read SQLite Thread Job %s: %w", jobID, err)
		}
		var workerID, bootID sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT worker_id,boot_id FROM factory_worker_ownership WHERE job_id=?`, jobID).Scan(&workerID, &bootID); err == nil && workerID.Valid && bootID.Valid {
			job.Worker = &WorkerSummary{WorkerID: workerID.String, BootID: bootID.String}
		} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return ThreadSnapshot{}, errors.New("read SQLite Thread worker identity")
		}
		var stage, state sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT stage,state FROM factory_job_progress WHERE job_id=?`, jobID).Scan(&stage, &state); err == nil && stage.Valid && state.Valid {
			job.Progress = &ProgressSummary{Stage: stage.String, State: state.String}
		} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return ThreadSnapshot{}, errors.New("read SQLite Thread progress")
		}
		jobs = append(jobs, job)
	}
	thread.JobIDs = make([]string, len(jobs))
	thread.UpdatedAt = thread.CreatedAt
	for i, job := range jobs {
		thread.JobIDs[i] = job.ID
		if job.UpdatedAt.After(thread.UpdatedAt) {
			thread.UpdatedAt = job.UpdatedAt
		}
	}
	thread.Status = DeriveThreadStatus(jobs, s.RecoveryNeeded)
	return ThreadSnapshot{Thread: thread, Jobs: jobs}, nil
}
