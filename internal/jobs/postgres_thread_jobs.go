package restjobs

import (
	"context"
	"database/sql"
	"errors"
)

func (s *PostgresStore) AdmitThreadJobAs(threadID, key, principalID string, request Request) (ThreadSnapshot, Snapshot, bool, error) {
	if err := s.checkAccepting(); err != nil {
		return ThreadSnapshot{}, Snapshot{}, false, err
	}
	return addThreadJobPostgres(context.Background(), s.db, s.config, threadID, key, principalID, request)
}

func (s *PostgresStore) threadSnapshotTx(ctx context.Context, tx *sql.Tx, id, principalID string) (ThreadSnapshot, error) {
	query := `SELECT id,submitter,created_at,completed_at,completed_by FROM factory_threads WHERE id=$1`
	args := []any{id}
	if principalID != "" {
		query += ` AND submitter=$2`
		args = append(args, principalID)
	}
	var thread Thread
	var completedAt sql.NullTime
	var completedBy sql.NullString
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&thread.ID, &thread.Submitter, &thread.CreatedAt, &completedAt, &completedBy); errors.Is(err, sql.ErrNoRows) {
		return ThreadSnapshot{}, ErrNotFound
	} else if err != nil {
		return ThreadSnapshot{}, errors.New("read PostgreSQL Thread record")
	}
	if completedAt.Valid {
		thread.CompletedAt = &completedAt.Time
	}
	if completedBy.Valid {
		thread.CompletedBy = completedBy.String
	}
	rows, err := tx.QueryContext(ctx, `SELECT j.id,j.request_json,j.status,j.created_at,j.updated_at,j.history_truncated,j.cancellation_requested,j.submitter,
		(SELECT provider_json FROM factory_job_provider_outcomes WHERE job_id=j.id),
		(SELECT evidence_json FROM factory_job_verification_evidence WHERE job_id=j.id),
		(SELECT json_build_object('worker_id', worker_id, 'boot_id', boot_id)::text FROM factory_rest_worker_ownership WHERE job_id=j.id),
		(SELECT json_build_object('stage',stage,'state',state)::text FROM factory_job_progress WHERE job_id=j.id)
		FROM factory_jobs j JOIN factory_thread_jobs m ON m.job_id=j.id WHERE m.thread_id=$1 ORDER BY m.position`, id)
	if err != nil {
		return ThreadSnapshot{}, errors.New("read PostgreSQL Thread members")
	}
	var jobs []Snapshot
	for rows.Next() {
		job, err := scanPostgresSnapshot(rows)
		if err != nil {
			rows.Close()
			return ThreadSnapshot{}, errors.New("scan PostgreSQL Thread member Job")
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ThreadSnapshot{}, errors.New("read PostgreSQL Thread members")
	}
	if err := rows.Close(); err != nil {
		return ThreadSnapshot{}, errors.New("close PostgreSQL Thread members")
	}
	thread.JobIDs = make([]string, len(jobs))
	thread.UpdatedAt = thread.CreatedAt
	for i, job := range jobs {
		thread.JobIDs[i] = job.ID
		if job.UpdatedAt.After(thread.UpdatedAt) {
			thread.UpdatedAt = job.UpdatedAt
		}
	}
	thread.Status = DeriveThreadStatus(jobs, func(jobID string) bool { return s.recoveryNeededWithQuery(ctx, tx, jobID) })
	return ThreadSnapshot{Thread: thread, Jobs: jobs}, nil
}
