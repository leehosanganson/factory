package restjobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

func (s *PostgresStore) ListThreads(ctx context.Context, beforeSequence, snapshotSequence uint64, limit int) (ThreadPage, error) {
	return s.listThreads(ctx, "", beforeSequence, snapshotSequence, limit)
}

func (s *PostgresStore) ListThreadsForPrincipal(ctx context.Context, principalID string, beforeSequence, snapshotSequence uint64, limit int) (ThreadPage, error) {
	principalID, err := normalizePrincipalID(principalID)
	if err != nil {
		return ThreadPage{}, err
	}
	return s.listThreads(ctx, principalID, beforeSequence, snapshotSequence, limit)
}

func (s *PostgresStore) listThreads(ctx context.Context, principalID string, beforeSequence, snapshotSequence uint64, limit int) (ThreadPage, error) {
	if err := validateThreadPageRequest(ctx, beforeSequence, snapshotSequence, limit); err != nil {
		return ThreadPage{}, err
	}
	if err := s.checkOpen(); err != nil {
		return ThreadPage{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return ThreadPage{}, errors.New("begin PostgreSQL Thread listing")
	}
	defer tx.Rollback()
	if snapshotSequence == 0 {
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(queue_sequence),0) FROM factory_jobs`).Scan(&snapshotSequence); err != nil {
			return ThreadPage{}, errors.New("read PostgreSQL Thread listing snapshot")
		}
	}
	query := `SELECT t.id,j.queue_sequence FROM factory_threads t JOIN factory_jobs j ON j.id=t.initial_job_id WHERE j.queue_sequence<=$1`
	args := []any{snapshotSequence}
	if beforeSequence != 0 {
		args = append(args, beforeSequence)
		query += ` AND j.queue_sequence<$` + fmt.Sprint(len(args))
	}
	if principalID != "" {
		args = append(args, principalID)
		query += ` AND t.submitter=$` + fmt.Sprint(len(args))
	}
	args = append(args, limit+1)
	query += ` ORDER BY j.queue_sequence DESC LIMIT $` + fmt.Sprint(len(args))
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return ThreadPage{}, errors.New("read PostgreSQL Thread listing")
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
			return ThreadPage{}, errors.New("decode PostgreSQL Thread listing")
		}
		items = append(items, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ThreadPage{}, errors.New("read PostgreSQL Thread listing")
	}
	if err := rows.Close(); err != nil {
		return ThreadPage{}, errors.New("close PostgreSQL Thread listing")
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
		return ThreadPage{}, errors.New("finish PostgreSQL Thread listing")
	}
	return page, nil
}

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
