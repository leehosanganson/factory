package restjobs

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

const issuePollerAdvisoryLock int64 = 0x46414354504F4C4C
const maxIssuePollerOutcomeAttempts = 5
const maxIssuePollerPending = 500
const maxIssuePollerOutcomeHistory = 50_000

// IssuePollerGuard holds a PostgreSQL session-level singleton lock.
type IssuePollerGuard struct {
	conn *sql.Conn
}

// AcquireIssuePollerGuard enforces one poller process database-wide.
func (s *PostgresStore) AcquireIssuePollerGuard(ctx context.Context) (*IssuePollerGuard, error) {
	if ctx == nil || s == nil || s.db == nil {
		return nil, ErrInvalidInput
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, errors.New("acquire PostgreSQL issue-poller connection")
	}
	var acquired bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, issuePollerAdvisoryLock).Scan(&acquired); err != nil || !acquired {
		_ = conn.Close()
		return nil, errors.New("another issue poller owns the PostgreSQL database")
	}
	return &IssuePollerGuard{conn: conn}, nil
}

// Close releases the singleton lock even when shutdown context has expired.
func (g *IssuePollerGuard) Close() error {
	if g == nil || g.conn == nil {
		return nil
	}
	defer func() {
		_ = g.conn.Close()
		g.conn = nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var released bool
	if err := g.conn.QueryRowContext(ctx, `SELECT pg_advisory_unlock($1)`, issuePollerAdvisoryLock).Scan(&released); err != nil || !released {
		return errors.New("release PostgreSQL issue-poller lock")
	}
	return nil
}

type IssuePollerOutcome struct {
	SourceRepository string
	IssueNumber      int
	Version          string
	JobID            string
	Label            string
	Outcome          string
	ReasonClass      string
	Attempts         int
}

type IssuePollerOperatorAction struct {
	SourceRepository string `json:"source_repository"`
	IssueNumber      int    `json:"issue_number"`
	Version          string `json:"version"`
	Outcome          string `json:"outcome"`
	ReasonClass      string `json:"reason_class"`
	Attempts         int    `json:"attempts"`
}

// RecordIssuePollerQuarantine durably records a safe identity/version/reason outcome.
func (s *PostgresStore) RecordIssuePollerQuarantine(ctx context.Context, source string, number int, version, reasonClass string) error {
	if ctx == nil || !validIssuePollerSource(source) || number < 1 || len(version) != 64 || (reasonClass != "task_too_large" && reasonClass != "title_too_large" && reasonClass != "provenance_conflict") {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("begin PostgreSQL issue-poller quarantine")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, postgresStoreMigrationLock+1); err != nil {
		return errors.New("lock PostgreSQL issue-poller ledger")
	}
	var existingOutcome string
	existingErr := tx.QueryRowContext(ctx, `SELECT outcome FROM factory_issue_poller_outcomes WHERE tracker='github' AND source_repository=$1 AND issue_number=$2 AND version=$3 FOR UPDATE`, source, number, version).Scan(&existingOutcome)
	if existingErr != nil && !errors.Is(existingErr, sql.ErrNoRows) {
		return errors.New("read PostgreSQL issue-poller quarantine")
	}
	if existingErr == nil && existingOutcome != "quarantined" {
		return errors.New("issue-poller identity already has a different durable outcome")
	}
	if errors.Is(existingErr, sql.ErrNoRows) {
		if err := makeIssuePollerRoom(ctx, tx); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO factory_issue_poller_outcomes
		(tracker,source_repository,issue_number,version,job_id,label,outcome,reason_class)
		VALUES('github',$1,$2,$3,NULL,'','quarantined',$4)
		ON CONFLICT(tracker,source_repository,issue_number,version) DO UPDATE SET reason_class=EXCLUDED.reason_class,updated_at=clock_timestamp()
		WHERE factory_issue_poller_outcomes.outcome='quarantined'`, source, number, version, reasonClass)
	if err != nil {
		return errors.New("persist PostgreSQL issue-poller quarantine")
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return errors.New("issue-poller identity already has a different durable outcome")
	}
	if err := tx.Commit(); err != nil {
		return errors.New("commit PostgreSQL issue-poller quarantine")
	}
	return nil
}

func issuePollerRetentionDeleteCount(total, completed, limit int) (int, error) {
	if total < limit {
		return 0, nil
	}
	need := total - limit + 1
	if completed < need {
		return 0, errors.New("PostgreSQL issue-poller ledger is full of unresolved records")
	}
	return need, nil
}

func makeIssuePollerRoom(ctx context.Context, tx *sql.Tx) error {
	var total, completed int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(*) FILTER (WHERE outcome IN ('label_removed','operator_action_resolved')) FROM factory_issue_poller_outcomes`).Scan(&total, &completed); err != nil {
		return errors.New("count PostgreSQL issue-poller ledger")
	}
	deleteCount, err := issuePollerRetentionDeleteCount(total, completed, maxIssuePollerOutcomeHistory)
	if err != nil || deleteCount == 0 {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM factory_issue_poller_outcomes WHERE (tracker,source_repository,issue_number,version) IN (
		SELECT tracker,source_repository,issue_number,version FROM factory_issue_poller_outcomes
		WHERE outcome IN ('label_removed','operator_action_resolved') ORDER BY updated_at LIMIT $1)`, deleteCount); err != nil {
		return errors.New("trim completed PostgreSQL issue-poller history")
	}
	return nil
}

// ListIssuePollerOperatorActions returns a bounded page of unresolved outcomes.
func (s *PostgresStore) ListIssuePollerOperatorActions(ctx context.Context, limit int) ([]IssuePollerOperatorAction, error) {
	if ctx == nil || limit < 1 || limit > 100 {
		return nil, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT source_repository,issue_number,version,outcome,reason_class,attempts
		FROM factory_issue_poller_outcomes WHERE outcome IN ('quarantined','operator_action_required')
		ORDER BY updated_at LIMIT $1`, limit)
	if err != nil {
		return nil, errors.New("read PostgreSQL issue-poller operator actions")
	}
	defer rows.Close()
	var result []IssuePollerOperatorAction
	for rows.Next() {
		var item IssuePollerOperatorAction
		if err := rows.Scan(&item.SourceRepository, &item.IssueNumber, &item.Version, &item.Outcome, &item.ReasonClass, &item.Attempts); err != nil {
			return nil, errors.New("read PostgreSQL issue-poller operator actions")
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("read PostgreSQL issue-poller operator actions")
	}
	return result, nil
}

// ResolveIssuePollerOperatorAction records explicit operator handling without provider writes.
func (s *PostgresStore) ResolveIssuePollerOperatorAction(ctx context.Context, source string, number int, version string) error {
	if ctx == nil || !validIssuePollerSource(source) || number < 1 || len(version) != 64 {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := s.db.ExecContext(ctx, `UPDATE factory_issue_poller_outcomes SET outcome='operator_action_resolved',updated_at=clock_timestamp()
		WHERE tracker='github' AND source_repository=$1 AND issue_number=$2 AND version=$3 AND outcome IN ('quarantined','operator_action_required')`, source, number, version)
	if err != nil {
		return errors.New("resolve PostgreSQL issue-poller operator action")
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return errors.New("issue-poller operator action is not unresolved")
	}
	return nil
}

func (s *PostgresStore) IssuePollerCursor(ctx context.Context, source string) (int, error) {
	if ctx == nil || !validIssuePollerSource(source) {
		return 0, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := s.db.ExecContext(ctx, `INSERT INTO factory_issue_poller_scan_state(source_repository,page) VALUES($1,1) ON CONFLICT(source_repository) DO NOTHING`, source); err != nil {
		return 0, errors.New("initialize PostgreSQL issue-poller scan cursor")
	}
	var page int
	if err := s.db.QueryRowContext(ctx, `SELECT page FROM factory_issue_poller_scan_state WHERE source_repository=$1`, source).Scan(&page); err != nil || page < 1 {
		return 0, errors.New("read PostgreSQL issue-poller scan cursor")
	}
	return page, nil
}

// AdvanceIssuePollerCursor uses compare-and-swap so a cursor only advances after
// every candidate on the page was either durably admitted or confirmed ineligible.
func (s *PostgresStore) AdvanceIssuePollerCursor(ctx context.Context, source string, current, next int) error {
	if ctx == nil || !validIssuePollerSource(source) || current < 1 || next < 1 {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := s.db.ExecContext(ctx, `UPDATE factory_issue_poller_scan_state SET page=$3,updated_at=clock_timestamp() WHERE source_repository=$1 AND page=$2`, source, current, next)
	if err != nil {
		return errors.New("persist PostgreSQL issue-poller scan cursor")
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return errors.New("PostgreSQL issue-poller scan cursor changed concurrently")
	}
	return nil
}

func validIssuePollerSource(source string) bool {
	parts := strings.Split(source, "/")
	return len(parts) == 2 && parts[0] != "" && parts[1] != "" && !strings.ContainsAny(parts[0]+parts[1], "\\\x00")
}

func recordIssuePollerAdmission(ctx context.Context, tx *sql.Tx, request Request, jobID, label string) error {
	p := request.IssueProvenance
	if p == nil || p.Tracker != "github" {
		return ErrInvalidInput
	}
	var existingOutcome, existingJobID, existingLabel string
	err := tx.QueryRowContext(ctx, `SELECT outcome,job_id,label FROM factory_issue_poller_outcomes WHERE tracker='github' AND source_repository=$1 AND issue_number=$2 AND version=$3`, p.SourceRepository, p.IssueNumber, p.Version).Scan(&existingOutcome, &existingJobID, &existingLabel)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	alreadyExists := err == nil
	if alreadyExists && (existingJobID != jobID || existingLabel != label) {
		return errors.New("issue-poller provenance points to conflicting durable state")
	}
	if !alreadyExists || existingOutcome == "label_removed" {
		var pending int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM factory_issue_poller_outcomes WHERE outcome IN ('pending_label_removal','label_removal_failed')`).Scan(&pending); err != nil || pending >= maxIssuePollerPending {
			return errors.New("issue-poller pending label capacity is full")
		}
		if err := makeIssuePollerRoom(ctx, tx); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO factory_issue_poller_outcomes
		(tracker,source_repository,issue_number,version,job_id,label,outcome)
		VALUES('github',$1,$2,$3,$4,$5,'pending_label_removal')
		ON CONFLICT(tracker,source_repository,issue_number,version) DO UPDATE SET
		label=EXCLUDED.label,
		outcome=CASE WHEN factory_issue_poller_outcomes.outcome='label_removed' THEN 'pending_label_removal' ELSE factory_issue_poller_outcomes.outcome END,
		updated_at=clock_timestamp()`, p.SourceRepository, p.IssueNumber, p.Version, jobID, label)
	return err
}

// RecordIssuePollerLabel binds an admitted provenance identity to its exact trigger label.
func (s *PostgresStore) RecordIssuePollerLabel(ctx context.Context, source string, number int, version, label string) error {
	if ctx == nil || source == "" || number < 1 || len(version) != 64 || label == "" {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := s.db.ExecContext(ctx, `UPDATE factory_issue_poller_outcomes SET label=$4,updated_at=clock_timestamp()
		WHERE tracker='github' AND source_repository=$1 AND issue_number=$2 AND version=$3 AND outcome='pending_label_removal' AND (label='' OR label=$4)`, source, number, version, label)
	if err != nil {
		return errors.New("persist PostgreSQL issue-poller label")
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return errors.New("issue-poller audit record is unavailable or conflicts")
	}
	return nil
}

// ListIssuePollerPending returns a bounded set of label operations still needing confirmation.
func (s *PostgresStore) IssuePollerOutcome(ctx context.Context, source string, number int, version string) (string, error) {
	if ctx == nil || !validIssuePollerSource(source) || number < 1 || len(version) != 64 {
		return "", ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var outcome string
	err := s.db.QueryRowContext(ctx, `SELECT outcome FROM factory_issue_poller_outcomes WHERE tracker='github' AND source_repository=$1 AND issue_number=$2 AND version=$3`, source, number, version).Scan(&outcome)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", errors.New("read PostgreSQL issue-poller outcome")
	}
	return outcome, nil
}

func (s *PostgresStore) ListIssuePollerPending(ctx context.Context) ([]IssuePollerOutcome, error) {
	if ctx == nil {
		return nil, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT source_repository,issue_number,version,job_id,label,outcome,attempts
		FROM factory_issue_poller_outcomes WHERE outcome IN ('pending_label_removal','label_removal_failed')
		ORDER BY updated_at LIMIT $1`, maxIssuePollerPending)
	if err != nil {
		return nil, errors.New("read PostgreSQL issue-poller outcomes")
	}
	defer rows.Close()
	var outcomes []IssuePollerOutcome
	for rows.Next() {
		var item IssuePollerOutcome
		if err := rows.Scan(&item.SourceRepository, &item.IssueNumber, &item.Version, &item.JobID, &item.Label, &item.Outcome, &item.Attempts); err != nil {
			return nil, errors.New("read PostgreSQL issue-poller outcomes")
		}
		outcomes = append(outcomes, item)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("read PostgreSQL issue-poller outcomes")
	}
	return outcomes, nil
}

// FinishIssuePollerLabel records an auditable bounded outcome for label handling.
func issuePollerLabelOutcome(attempts int, outcome string) (string, string, error) {
	if attempts < 0 || (outcome != "label_removed" && outcome != "label_removal_failed") {
		return "", "", ErrInvalidInput
	}
	if outcome == "label_removal_failed" && attempts+1 >= maxIssuePollerOutcomeAttempts {
		return "operator_action_required", "label_removal_failed", nil
	}
	return outcome, "", nil
}

func (s *PostgresStore) FinishIssuePollerLabel(ctx context.Context, source string, number int, version, outcome string) error {
	if ctx == nil || !validIssuePollerSource(source) || number < 1 || len(version) != 64 || (outcome != "label_removed" && outcome != "label_removal_failed") {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("begin PostgreSQL issue-poller outcome")
	}
	defer tx.Rollback()
	var attempts int
	var current string
	if err := tx.QueryRowContext(ctx, `SELECT attempts,outcome FROM factory_issue_poller_outcomes WHERE tracker='github' AND source_repository=$1 AND issue_number=$2 AND version=$3 FOR UPDATE`, source, number, version).Scan(&attempts, &current); err != nil {
		return errors.New("read PostgreSQL issue-poller outcome")
	}
	if current != "pending_label_removal" && current != "label_removal_failed" {
		return errors.New("issue-poller outcome is not pending")
	}
	next, reason, err := issuePollerLabelOutcome(attempts, outcome)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE factory_issue_poller_outcomes SET outcome=$4,reason_class=CASE WHEN $5='' THEN reason_class ELSE $5 END,
		attempts=LEAST(attempts+1,$6),updated_at=clock_timestamp()
		WHERE tracker='github' AND source_repository=$1 AND issue_number=$2 AND version=$3`, source, number, version, next, reason, maxIssuePollerOutcomeAttempts); err != nil {
		return errors.New("persist PostgreSQL issue-poller outcome")
	}
	if err := tx.Commit(); err != nil {
		return errors.New("commit PostgreSQL issue-poller outcome")
	}
	return nil
}
