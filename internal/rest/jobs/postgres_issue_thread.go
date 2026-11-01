package restjobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// AdmitIssuePollerThread atomically admits one source/version as a canonical
// Thread and its Job, or attaches the exact retained poller Job to its Thread.
func (s *PostgresStore) AdmitIssuePollerThread(ctx context.Context, key, principalID string, request Request, triggerLabel string) (ThreadSnapshot, bool, error) {
	s.admitMu.Lock()
	defer s.admitMu.Unlock()
	if ctx == nil || triggerLabel == "" || request.IssueProvenance == nil || request.IssueProvenance.Tracker != "github" {
		return ThreadSnapshot{}, false, ErrInvalidInput
	}
	key, request, err := normalizeAdmission(key, request, s.config.MaxTaskBytes)
	if err != nil {
		return ThreadSnapshot{}, false, err
	}
	principalID, err = normalizePrincipalID(principalID)
	if err != nil || principalID != "github-issue-poller" {
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
		return ThreadSnapshot{}, false, errors.New("lock PostgreSQL issue Thread admission")
	}

	p := request.IssueProvenance
	var jobID, raw, submitter string
	err = tx.QueryRowContext(ctx, `SELECT j.id,j.request_json,j.submitter
		FROM factory_job_issue_provenance p JOIN factory_jobs j ON j.id=p.job_id
		WHERE p.tracker=$1 AND p.source_repository=$2 AND p.issue_number=$3 AND p.version=$4`,
		p.Tracker, p.SourceRepository, p.IssueNumber, p.Version).Scan(&jobID, &raw, &submitter)
	existingJob := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return ThreadSnapshot{}, false, errors.New("read PostgreSQL issue provenance")
	}
	var jobKeyJob, jobKeyRequest string
	jobKeyErr := tx.QueryRowContext(ctx, `SELECT job_id,request_json FROM factory_job_idempotency WHERE principal_id=$1 AND key=$2`, principalID, key).Scan(&jobKeyJob, &jobKeyRequest)
	if jobKeyErr == nil {
		var keyRequest Request
		if !existingJob || jobKeyJob != jobID || json.Unmarshal([]byte(jobKeyRequest), &keyRequest) != nil || !sameRequest(keyRequest, request) {
			return ThreadSnapshot{}, false, ErrIdempotencyConflict
		}
	}
	if jobKeyErr != nil && !errors.Is(jobKeyErr, sql.ErrNoRows) {
		return ThreadSnapshot{}, false, errors.New("read PostgreSQL issue idempotency record")
	}
	if existingJob {
		var prior Request
		if json.Unmarshal([]byte(raw), &prior) != nil {
			return ThreadSnapshot{}, false, errors.New("PostgreSQL issue Job record is invalid")
		}
		if submitter != principalID || !sameRequest(prior, request) {
			return ThreadSnapshot{}, false, ErrProvenanceConflict
		}
	}

	var mappedThread, mappedRequest string
	mapErr := tx.QueryRowContext(ctx, `SELECT thread_id,request_json FROM factory_thread_idempotency WHERE principal_id=$1 AND key=$2`, principalID, key).Scan(&mappedThread, &mappedRequest)
	if mapErr == nil {
		var mapped Request
		if json.Unmarshal([]byte(mappedRequest), &mapped) != nil || !sameRequest(mapped, request) || !existingJob || errors.Is(jobKeyErr, sql.ErrNoRows) || jobKeyJob != jobID {
			return ThreadSnapshot{}, false, ErrIdempotencyConflict
		}
		var proven bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
			SELECT 1 FROM factory_threads t
			JOIN factory_thread_jobs m ON m.thread_id=t.id AND m.job_id=$1
			WHERE t.id=$2 AND t.submitter=$3 AND t.initial_job_id=$1
			AND (SELECT COUNT(*) FROM factory_thread_jobs members WHERE members.thread_id=t.id)=1
			)`, jobID, mappedThread, principalID).Scan(&proven); err != nil {
			return ThreadSnapshot{}, false, errors.New("verify PostgreSQL issue Thread membership")
		}
		if !proven {
			return ThreadSnapshot{}, false, ErrProvenanceConflict
		}
		if err := recordIssuePollerAdmission(ctx, tx, request, jobID, triggerLabel); err != nil {
			return ThreadSnapshot{}, false, errors.New("record PostgreSQL issue-poller admission")
		}
		snapshot, err := s.threadSnapshotTx(ctx, tx, mappedThread, principalID)
		if err != nil {
			return ThreadSnapshot{}, false, errors.New("read PostgreSQL issue Thread")
		}
		if err := tx.Commit(); err != nil {
			return ThreadSnapshot{}, false, errors.New("finish PostgreSQL issue Thread replay")
		}
		return snapshot, true, nil
	}
	if !errors.Is(mapErr, sql.ErrNoRows) {
		return ThreadSnapshot{}, false, errors.New("read PostgreSQL issue Thread idempotency record")
	}

	var threadID string
	hasMembership := false
	if existingJob {
		var memberships int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM factory_thread_jobs WHERE job_id=$1`, jobID).Scan(&memberships); err != nil {
			return ThreadSnapshot{}, false, errors.New("read PostgreSQL issue Job membership")
		}
		if memberships > 0 {
			var memberCount int
			err = tx.QueryRowContext(ctx, `SELECT t.id,(SELECT COUNT(*) FROM factory_thread_jobs members WHERE members.thread_id=t.id)
				FROM factory_threads t JOIN factory_thread_jobs m ON m.thread_id=t.id
				WHERE m.job_id=$1 AND t.initial_job_id=$1 AND t.submitter=$2`, jobID, principalID).Scan(&threadID, &memberCount)
			if errors.Is(err, sql.ErrNoRows) || err == nil && memberCount != 1 {
				return ThreadSnapshot{}, false, ErrProvenanceConflict
			}
			if err != nil {
				return ThreadSnapshot{}, false, errors.New("verify PostgreSQL issue Thread ownership")
			}
			hasMembership = true
		}
	}

	if existingJob && hasMembership {
		if errors.Is(jobKeyErr, sql.ErrNoRows) {
			if _, err := tx.ExecContext(ctx, `INSERT INTO factory_job_idempotency(principal_id,key,job_id,request_json) VALUES($1,$2,$3,$4)`, principalID, key, jobID, raw); err != nil {
				return ThreadSnapshot{}, false, errors.New("persist PostgreSQL issue Job idempotency key")
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO factory_thread_idempotency(principal_id,key,thread_id,request_json) VALUES($1,$2,$3,$4)`, principalID, key, threadID, raw); err != nil {
			return ThreadSnapshot{}, false, errors.New("persist PostgreSQL issue Thread idempotency key")
		}
		if err := recordIssuePollerAdmission(ctx, tx, request, jobID, triggerLabel); err != nil {
			return ThreadSnapshot{}, false, errors.New("record PostgreSQL issue-poller admission")
		}
		snapshot, err := s.threadSnapshotTx(ctx, tx, threadID, principalID)
		if err != nil {
			return ThreadSnapshot{}, false, errors.New("read PostgreSQL issue Thread")
		}
		if len(snapshot.Jobs) != 1 || snapshot.Jobs[0].ID != jobID || len(snapshot.Thread.JobIDs) != 1 || snapshot.Thread.JobIDs[0] != jobID {
			return ThreadSnapshot{}, false, ErrProvenanceConflict
		}
		if err := tx.Commit(); err != nil {
			return ThreadSnapshot{}, false, errors.New("finish PostgreSQL issue Thread replay")
		}
		return snapshot, true, nil
	}

	if !existingJob {
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
		jobID, err = newJobID()
		if err != nil {
			return ThreadSnapshot{}, false, errors.New("create PostgreSQL issue Job identifier")
		}
		var sequence int64
		if err := tx.QueryRowContext(ctx, `SELECT nextval('factory_rest_job_sequence')`).Scan(&sequence); err != nil {
			return ThreadSnapshot{}, false, errors.New("allocate PostgreSQL queue position")
		}
		encoded, err := json.Marshal(request)
		if err != nil {
			return ThreadSnapshot{}, false, ErrInvalidInput
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO factory_jobs(id,request_json,status,created_at,updated_at,queue_sequence,submitter) VALUES($1,$2,'queued',clock_timestamp(),clock_timestamp(),$3,$4)`, jobID, string(encoded), sequence, principalID); err != nil {
			return ThreadSnapshot{}, false, errors.New("persist PostgreSQL issue Job")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO factory_job_idempotency(principal_id,key,job_id,request_json) VALUES($1,$2,$3,$4)`, principalID, key, jobID, string(encoded)); err != nil {
			return ThreadSnapshot{}, false, errors.New("persist PostgreSQL issue idempotency key")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO factory_job_issue_provenance(tracker,source_repository,issue_number,version,job_id) VALUES($1,$2,$3,$4,$5)`, p.Tracker, p.SourceRepository, p.IssueNumber, p.Version, jobID); err != nil {
			return ThreadSnapshot{}, false, ErrProvenanceConflict
		}
		if err := insertPostgresEvent(ctx, tx, jobID, "queued", "Job admitted", s.config.MaxEventsPerJob); err != nil {
			return ThreadSnapshot{}, false, errors.New("persist PostgreSQL issue admission history")
		}
	}

	if existingJob && errors.Is(jobKeyErr, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO factory_job_idempotency(principal_id,key,job_id,request_json) VALUES($1,$2,$3,$4)`, principalID, key, jobID, raw); err != nil {
			return ThreadSnapshot{}, false, errors.New("persist PostgreSQL issue Job idempotency key")
		}
	}
	threadID, err = newJobID()
	if err != nil {
		return ThreadSnapshot{}, false, errors.New("create PostgreSQL issue Thread identifier")
	}
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return ThreadSnapshot{}, false, ErrInvalidInput
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_threads(id,submitter,created_at,initial_job_id) VALUES($1,$2,clock_timestamp(),$3)`, threadID, principalID, jobID); err != nil {
		return ThreadSnapshot{}, false, errors.New("persist PostgreSQL issue Thread")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_thread_jobs(thread_id,job_id,position) VALUES($1,$2,1)`, threadID, jobID); err != nil {
		return ThreadSnapshot{}, false, errors.New("persist PostgreSQL issue Thread membership")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_thread_idempotency(principal_id,key,thread_id,request_json) VALUES($1,$2,$3,$4)`, principalID, key, threadID, string(requestJSON)); err != nil {
		return ThreadSnapshot{}, false, errors.New("persist PostgreSQL issue Thread idempotency key")
	}
	if err := recordIssuePollerAdmission(ctx, tx, request, jobID, triggerLabel); err != nil {
		return ThreadSnapshot{}, false, errors.New("record PostgreSQL issue-poller admission")
	}
	snapshot, err := s.threadSnapshotTx(ctx, tx, threadID, principalID)
	if err != nil {
		return ThreadSnapshot{}, false, errors.New("read PostgreSQL issue Thread")
	}
	if err := tx.Commit(); err != nil {
		return ThreadSnapshot{}, false, errors.New("commit PostgreSQL issue Thread admission")
	}
	return snapshot, existingJob, nil
}
