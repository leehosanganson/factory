package restjobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

func (s *SQLiteStore) ListThreads(ctx context.Context, beforeSequence, snapshotSequence uint64, limit int) (ThreadPage, error) {
	return s.listThreads(ctx, "", beforeSequence, snapshotSequence, limit)
}

func (s *SQLiteStore) ListThreadsForPrincipal(ctx context.Context, principalID string, beforeSequence, snapshotSequence uint64, limit int) (ThreadPage, error) {
	principalID, err := normalizePrincipalID(principalID)
	if err != nil {
		return ThreadPage{}, err
	}
	return s.listThreads(ctx, principalID, beforeSequence, snapshotSequence, limit)
}

func (s *SQLiteStore) listThreads(ctx context.Context, principalID string, beforeSequence, snapshotSequence uint64, limit int) (ThreadPage, error) {
	if err := validateThreadPageRequest(ctx, beforeSequence, snapshotSequence, limit); err != nil {
		return ThreadPage{}, err
	}
	if err := s.checkOpen(); err != nil {
		return ThreadPage{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ThreadPage{}, errors.New("begin SQLite Thread listing")
	}
	defer tx.Rollback()
	if snapshotSequence == 0 {
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(queue_sequence),0) FROM factory_jobs`).Scan(&snapshotSequence); err != nil {
			return ThreadPage{}, errors.New("read SQLite Thread listing snapshot")
		}
	}
	query := `SELECT t.id,j.queue_sequence FROM factory_threads t JOIN factory_jobs j ON j.id=t.initial_job_id WHERE j.queue_sequence<=?`
	args := []any{snapshotSequence}
	if beforeSequence != 0 {
		query += ` AND j.queue_sequence<?`
		args = append(args, beforeSequence)
	}
	if principalID != "" {
		query += ` AND t.submitter=?`
		args = append(args, principalID)
	}
	query += ` ORDER BY j.queue_sequence DESC LIMIT ?`
	args = append(args, limit+1)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return ThreadPage{}, errors.New("read SQLite Thread listing")
	}
	type item struct {
		id       string
		sequence uint64
	}
	items := make([]item, 0, limit+1)
	for rows.Next() {
		var value item
		if err := rows.Scan(&value.id, &value.sequence); err != nil {
			rows.Close()
			return ThreadPage{}, errors.New("decode SQLite Thread listing")
		}
		items = append(items, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ThreadPage{}, errors.New("read SQLite Thread listing")
	}
	if err := rows.Close(); err != nil {
		return ThreadPage{}, errors.New("close SQLite Thread listing")
	}
	page := ThreadPage{Threads: make([]ThreadSummary, 0, limit), SnapshotSequence: snapshotSequence, HasMore: len(items) > limit}
	if page.HasMore {
		items = items[:limit]
	}
	for _, item := range items {
		snapshot, err := s.threadSnapshotTx(ctx, tx, item.id, "")
		if err != nil {
			return ThreadPage{}, err
		}
		summary := ThreadSummary{ID: snapshot.Thread.ID, Status: snapshot.Thread.Status, CreatedAt: snapshot.Thread.CreatedAt, UpdatedAt: snapshot.Thread.UpdatedAt}
		if snapshot.Thread.CompletedAt != nil {
			completed := *snapshot.Thread.CompletedAt
			summary.CompletedAt = &completed
		}
		page.Threads = append(page.Threads, summary)
		page.NextSequence = item.sequence
	}
	if !page.HasMore {
		page.NextSequence = 0
	}
	if err := tx.Commit(); err != nil {
		return ThreadPage{}, errors.New("finish SQLite Thread listing")
	}
	return page, nil
}

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
