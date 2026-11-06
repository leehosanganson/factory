package restjobs

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// GitHubIssueWebhookStore binds a stable GitHub delivery identity to the
// canonical Thread+Job admission transaction. PostgreSQL is required so
// webhook admission shares the intake-owner registry with the issue poller.
type GitHubIssueWebhookStore interface {
	ThreadStore
	AdmitGitHubIssueWebhook(ctx context.Context, deliveryID, bodySHA256, principalID string, request Request, owner IntakeOwner) (ThreadSnapshot, bool, error)
	supportsGitHubIssueWebhook()
}

func validateGitHubWebhookAdmission(ctx context.Context, deliveryID, bodySHA256, principalID string, request Request, maxTaskBytes int) (string, string, Request, error) {
	if ctx == nil || deliveryID == "" || len(deliveryID) > 256 || strings.TrimSpace(deliveryID) != deliveryID || strings.ContainsAny(deliveryID, "\x00\r\n") || len(bodySHA256) != sha256.Size*2 {
		return "", "", Request{}, ErrInvalidInput
	}
	for _, c := range bodySHA256 {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return "", "", Request{}, ErrInvalidInput
		}
	}
	principalID, err := normalizePrincipalID(principalID)
	if err != nil {
		return "", "", Request{}, err
	}
	deliveryKey := sha256.Sum256([]byte(deliveryID))
	key, normalized, err := normalizeAdmission("github-issues-labeled-v1-"+hex.EncodeToString(deliveryKey[:]), request, maxTaskBytes)
	if err != nil || normalized.IssueProvenance != nil {
		return "", "", Request{}, ErrInvalidInput
	}
	return key, principalID, normalized, nil
}

func (*SQLiteStore) AdmitGitHubIssueWebhook(context.Context, string, string, string, Request, IntakeOwner) (ThreadSnapshot, bool, error) {
	return ThreadSnapshot{}, false, ErrGitHubWebhookUnavailable
}

func (*PostgresStore) supportsGitHubIssueWebhook() {}

func (s *PostgresStore) AdmitGitHubIssueWebhook(ctx context.Context, deliveryID, bodySHA256, principalID string, request Request, owner IntakeOwner) (ThreadSnapshot, bool, error) {
	key, principalID, request, err := validateGitHubWebhookAdmission(ctx, deliveryID, bodySHA256, principalID, request, s.config.MaxTaskBytes)
	if err != nil || owner.SourceRepository == "" || validateIntakeOwner(owner.SourceRepository, owner.TriggerLabel, IntakeOwnerWebhook, owner.Generation) != nil {
		return ThreadSnapshot{}, false, ErrInvalidInput
	}
	if err := s.checkAccepting(); err != nil {
		return ThreadSnapshot{}, false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ThreadSnapshot{}, false, errors.New("PostgreSQL job store unavailable")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, postgresStoreMigrationLock+1); err != nil {
		return ThreadSnapshot{}, false, errors.New("lock PostgreSQL webhook admission")
	}
	if err := s.checkIntakeOwnerTx(ctx, tx, owner.SourceRepository, owner.TriggerLabel, IntakeOwnerWebhook, owner.Generation); err != nil {
		return ThreadSnapshot{}, false, err
	}
	var priorDigest, priorThread string
	err = tx.QueryRowContext(ctx, `SELECT body_sha256,thread_id FROM factory_github_issue_webhook_deliveries WHERE delivery_id=$1`, deliveryID).Scan(&priorDigest, &priorThread)
	if err == nil {
		if priorDigest != bodySHA256 {
			return ThreadSnapshot{}, false, ErrIdempotencyConflict
		}
		var owner string
		if err := tx.QueryRowContext(ctx, `SELECT submitter FROM factory_threads WHERE id=$1`, priorThread).Scan(&owner); err != nil || owner != principalID {
			return ThreadSnapshot{}, false, ErrIdempotencyConflict
		}
		snapshot, err := s.threadSnapshotTx(ctx, tx, priorThread, principalID)
		if err != nil || len(snapshot.Jobs) != 1 || snapshot.Jobs[0].Submitter != principalID {
			return ThreadSnapshot{}, false, errors.New("read PostgreSQL GitHub webhook Thread")
		}
		if err := tx.Commit(); err != nil {
			return ThreadSnapshot{}, false, errors.New("finish PostgreSQL GitHub webhook replay")
		}
		return snapshot, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ThreadSnapshot{}, false, errors.New("read PostgreSQL GitHub webhook delivery")
	}
	var collision int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM factory_thread_idempotency WHERE principal_id=$1 AND key=$2`, principalID, key).Scan(&collision); err != nil || collision != 0 {
		return ThreadSnapshot{}, false, ErrIdempotencyConflict
	}
	var retained, active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(*) FILTER (WHERE status IN ('queued','running')) FROM factory_jobs`).Scan(&retained, &active); err != nil {
		return ThreadSnapshot{}, false, errors.New("check PostgreSQL webhook admission capacity")
	}
	if retained >= s.config.MaxRecords {
		return ThreadSnapshot{}, false, ErrRegistryFull
	}
	if active >= s.config.QueueCapacity+s.config.MaxConcurrentJobs {
		return ThreadSnapshot{}, false, ErrQueueFull
	}
	threadID, err := newJobID()
	if err != nil {
		return ThreadSnapshot{}, false, errors.New("create PostgreSQL webhook Thread identifier")
	}
	jobID, err := newJobID()
	if err != nil {
		return ThreadSnapshot{}, false, errors.New("create PostgreSQL webhook Job identifier")
	}
	var sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT nextval('factory_rest_job_sequence')`).Scan(&sequence); err != nil {
		return ThreadSnapshot{}, false, errors.New("allocate PostgreSQL webhook queue position")
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return ThreadSnapshot{}, false, ErrInvalidInput
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_jobs(id,request_json,status,created_at,updated_at,queue_sequence,history_truncated,submitter) VALUES($1,$2,'queued',clock_timestamp(),clock_timestamp(),$3,false,$4)`, jobID, string(encoded), sequence, principalID); err != nil {
		return ThreadSnapshot{}, false, errors.New("persist PostgreSQL webhook Job")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_threads(id,submitter,created_at,initial_job_id) VALUES($1,$2,clock_timestamp(),$3)`, threadID, principalID, jobID); err != nil {
		return ThreadSnapshot{}, false, errors.New("persist PostgreSQL webhook Thread")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_thread_idempotency(principal_id,key,thread_id,request_json) VALUES($1,$2,$3,$4)`, principalID, key, threadID, string(encoded)); err != nil {
		return ThreadSnapshot{}, false, errors.New("persist PostgreSQL webhook Thread idempotency")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_thread_jobs(thread_id,job_id,position) VALUES($1,$2,1)`, threadID, jobID); err != nil {
		return ThreadSnapshot{}, false, errors.New("persist PostgreSQL webhook Thread membership")
	}
	if err := insertPostgresEvent(ctx, tx, jobID, "queued", "Job admitted", s.config.MaxEventsPerJob); err != nil {
		return ThreadSnapshot{}, false, errors.New("persist PostgreSQL webhook admission history")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_github_issue_webhook_deliveries(delivery_id,body_sha256,thread_id) VALUES($1,$2,$3)`, deliveryID, bodySHA256, threadID); err != nil {
		return ThreadSnapshot{}, false, errors.New("persist PostgreSQL GitHub webhook delivery")
	}
	snapshot, err := s.threadSnapshotTx(ctx, tx, threadID, principalID)
	if err != nil {
		return ThreadSnapshot{}, false, errors.New("read PostgreSQL webhook Thread admission")
	}
	if err := tx.Commit(); err != nil {
		return ThreadSnapshot{}, false, errors.New("commit PostgreSQL GitHub webhook admission")
	}
	return snapshot, false, nil
}

var _ GitHubIssueWebhookStore = (*PostgresStore)(nil)
