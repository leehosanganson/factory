package restjobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

var _ ScheduleStore = (*PostgresStore)(nil)

func (s *PostgresStore) CreateSchedule(owner, key string, input ScheduleInput) (Schedule, bool, error) {
	owner, err := normalizePrincipalID(owner)
	if err != nil || owner == legacySharedPrincipalID {
		return Schedule{}, false, ErrInvalidInput
	}
	key = strings.TrimSpace(key)
	if key == "" || len(key) > maxKeyBytes {
		return Schedule{}, false, ErrInvalidInput
	}
	input, err = normalizeScheduleInput(input, s.config.MaxTaskBytes)
	if err != nil {
		return Schedule{}, false, err
	}
	request, _ := json.Marshal(input)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.dbClosed {
		return Schedule{}, false, ErrManagerClosed
	}
	tx, err := s.db.Begin()
	if err != nil {
		return Schedule{}, false, errors.New("PostgreSQL schedule store unavailable")
	}
	defer tx.Rollback()
	var lockResult any
	if err = tx.QueryRow(`SELECT pg_advisory_xact_lock(hashtextextended(length($1)::text || ':' || $1 || $2, 0))`, owner, key).Scan(&lockResult); err != nil {
		return Schedule{}, false, errors.New("lock PostgreSQL schedule idempotency")
	}
	var id, prior string
	err = tx.QueryRow(`SELECT schedule_id,request_json FROM factory_schedule_idempotency WHERE owner=$1 AND idempotency_key=$2`, owner, key).Scan(&id, &prior)
	if err == nil {
		if prior != string(request) {
			return Schedule{}, false, ErrScheduleIDConflict
		}
		schedule, readErr := getPostgresSchedule(tx, id)
		return schedule, readErr == nil, readErr
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Schedule{}, false, errors.New("read PostgreSQL schedule idempotency")
	}
	id, err = newScheduleID()
	if err != nil {
		return Schedule{}, false, err
	}
	now := time.Now().UTC()
	nextOccurrence, err := NextScheduleOccurrence(input.Cron, now)
	if err != nil {
		return Schedule{}, false, ErrInvalidInput
	}
	schedule := Schedule{ID: id, Owner: owner, Cron: input.Cron, Repository: input.Repository, Task: input.Task, Status: ScheduleActive, Revision: 1, CreatedAt: now, UpdatedAt: now, nextOccurrenceAt: nextOccurrence, skippedRevision: 1}
	if _, err = tx.Exec(`INSERT INTO factory_schedules(id,owner,cron,repository,task,status,revision,created_at,updated_at,next_occurrence_at,skipped_revision) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$7)`, id, owner, input.Cron, input.Repository, input.Task, schedule.Status, schedule.Revision, now, now, nextOccurrence); err != nil {
		return Schedule{}, false, errors.New("create PostgreSQL schedule")
	}
	if _, err = tx.Exec(`INSERT INTO factory_schedule_idempotency(owner,idempotency_key,schedule_id,request_json) VALUES($1,$2,$3,$4)`, owner, key, id, string(request)); err != nil {
		return Schedule{}, false, errors.New("record PostgreSQL schedule idempotency")
	}
	if _, err = tx.Exec(`INSERT INTO factory_schedule_audit(schedule_id,revision,creator,actor,action,occurred_at) VALUES($1,$2,$3,$3,$4,$5)`, id, schedule.Revision, owner, ScheduleCreated, now); err != nil {
		return Schedule{}, false, errors.New("record PostgreSQL schedule creation audit")
	}
	if err = tx.Commit(); err != nil {
		return Schedule{}, false, errors.New("commit PostgreSQL schedule")
	}
	return schedule, false, nil
}
func (s *PostgresStore) GetSchedule(id string) (Schedule, error) {
	if err := s.checkOpen(); err != nil {
		return Schedule{}, err
	}
	return getPostgresSchedule(s.db, id)
}
func (s *PostgresStore) ListSchedules(owner string) ([]Schedule, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	owner, err := normalizePrincipalID(owner)
	if err != nil {
		return nil, ErrInvalidInput
	}
	rows, err := s.db.Query(`SELECT id,owner,cron,repository,task,status,revision,created_at,updated_at,next_occurrence_at,pending_occurrence_at,skipped_revision,skipped_first_at,skipped_last_at,skipped_count FROM factory_schedules WHERE owner=$1 ORDER BY created_at,id`, owner)
	if err != nil {
		return nil, errors.New("list PostgreSQL schedules")
	}
	defer rows.Close()
	var result []Schedule
	for rows.Next() {
		schedule, scanErr := scanPostgresSchedule(rows)
		if scanErr != nil {
			return nil, errors.New("read PostgreSQL schedules")
		}
		result = append(result, schedule)
	}
	if err = rows.Err(); err != nil {
		return nil, errors.New("read PostgreSQL schedules")
	}
	if result == nil {
		result = []Schedule{}
	}
	return result, nil
}
func (s *PostgresStore) ListAllSchedules() ([]Schedule, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`SELECT id,owner,cron,repository,task,status,revision,created_at,updated_at,next_occurrence_at,pending_occurrence_at,skipped_revision,skipped_first_at,skipped_last_at,skipped_count FROM factory_schedules ORDER BY created_at,id`)
	if err != nil {
		return nil, errors.New("list all PostgreSQL schedules")
	}
	defer rows.Close()
	var result []Schedule
	for rows.Next() {
		schedule, scanErr := scanPostgresSchedule(rows)
		if scanErr != nil {
			return nil, errors.New("read all PostgreSQL schedules")
		}
		result = append(result, schedule)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("read all PostgreSQL schedules")
	}
	if result == nil {
		result = []Schedule{}
	}
	return result, nil
}

func (s *PostgresStore) ListScheduleHistory(id, actor string, operator bool, afterRevision int64, limit int) (ScheduleAuditPage, error) {
	if err := s.checkOpen(); err != nil {
		return ScheduleAuditPage{}, err
	}
	if afterRevision < 0 || limit < 1 || limit > MaxScheduleHistoryLimit {
		return ScheduleAuditPage{}, ErrInvalidInput
	}
	var owner string
	if err := s.db.QueryRow(`SELECT owner FROM factory_schedules WHERE id=$1`, id).Scan(&owner); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ScheduleAuditPage{}, ErrNotFound
		}
		return ScheduleAuditPage{}, errors.New("read PostgreSQL schedule owner")
	}
	if !operator && owner != actor {
		return ScheduleAuditPage{}, ErrNotFound
	}
	rows, err := s.db.Query(`SELECT schedule_id,creator,actor,action,revision,occurred_at FROM factory_schedule_audit WHERE schedule_id=$1 AND revision>$2 ORDER BY revision LIMIT $3`, id, afterRevision, limit+1)
	if err != nil {
		return ScheduleAuditPage{}, errors.New("read PostgreSQL schedule history")
	}
	defer rows.Close()
	result := make([]ScheduleAuditEvent, 0, limit+1)
	for rows.Next() {
		var event ScheduleAuditEvent
		var action string
		if err := rows.Scan(&event.ScheduleID, &event.Creator, &event.Actor, &action, &event.Revision, &event.At); err != nil {
			return ScheduleAuditPage{}, errors.New("read PostgreSQL schedule history")
		}
		event.Action = ScheduleAction(action)
		event.At = event.At.UTC()
		result = append(result, event)
	}
	if err := rows.Err(); err != nil {
		return ScheduleAuditPage{}, errors.New("read PostgreSQL schedule history")
	}
	page := ScheduleAuditPage{Events: result}
	if len(result) > limit {
		page.HasMore = true
		page.Events = result[:limit]
		page.NextRevision = page.Events[len(page.Events)-1].Revision
	}
	return page, nil
}

func (s *PostgresStore) ListScheduleRuns(id, actor string, operator bool, beforeSequence uint64, limit int) (ScheduleRunPage, error) {
	if err := s.checkOpen(); err != nil {
		return ScheduleRunPage{}, err
	}
	if limit < 1 || limit > MaxScheduleHistoryLimit {
		return ScheduleRunPage{}, ErrInvalidInput
	}
	var owner string
	if err := s.db.QueryRow(`SELECT owner FROM factory_schedules WHERE id=$1`, id).Scan(&owner); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ScheduleRunPage{}, ErrNotFound
		}
		return ScheduleRunPage{}, errors.New("read PostgreSQL schedule owner")
	}
	if !operator && owner != actor {
		return ScheduleRunPage{}, ErrNotFound
	}
	query := `SELECT r.occurrence_at,j.id,j.status,j.created_at,j.updated_at,j.queue_sequence FROM factory_schedule_runs r JOIN factory_jobs j ON j.id=r.job_id WHERE r.schedule_id=$1 AND j.submitter=$2`
	args := []any{id, owner}
	if beforeSequence > 0 {
		query += ` AND j.queue_sequence<$3`
		args = append(args, beforeSequence)
	}
	query += ` ORDER BY j.queue_sequence DESC LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit+1)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return ScheduleRunPage{}, errors.New("read PostgreSQL schedule runs")
	}
	defer rows.Close()
	result := make([]ScheduleRun, 0, limit+1)
	for rows.Next() {
		var occurrence, created, updated time.Time
		var jobID string
		var status Status
		var sequence uint64
		if err := rows.Scan(&occurrence, &jobID, &status, &created, &updated, &sequence); err != nil {
			return ScheduleRunPage{}, errors.New("read PostgreSQL schedule runs")
		}
		run, err := scanScheduleRun(occurrence.UTC().Format(time.RFC3339Nano), jobID, status, created.UTC().Format(time.RFC3339Nano), updated.UTC().Format(time.RFC3339Nano), sequence)
		if err != nil {
			return ScheduleRunPage{}, errors.New("decode PostgreSQL schedule occurrence")
		}
		result = append(result, run)
	}
	if err := rows.Err(); err != nil {
		return ScheduleRunPage{}, errors.New("read PostgreSQL schedule runs")
	}
	page := ScheduleRunPage{Runs: result}
	if len(result) > limit {
		page.HasMore = true
		page.Runs = result[:limit]
		page.NextBefore = page.Runs[len(page.Runs)-1].Sequence
	}
	return page, nil
}

func (s *PostgresStore) MutateSchedule(id, owner string, revision int64, mutation ScheduleMutation, input *ScheduleInput) (Schedule, error) {
	return s.MutateScheduleAs(id, owner, false, revision, mutation, input)
}

func (s *PostgresStore) MutateScheduleAs(id, actor string, operator bool, revision int64, mutation ScheduleMutation, input *ScheduleInput) (Schedule, error) {
	owner, err := normalizePrincipalID(actor)
	if err != nil {
		return Schedule{}, ErrNotFound
	}
	if revision < 1 {
		return Schedule{}, ErrInvalidInput
	}
	var normalized ScheduleInput
	if mutation == ScheduleEdit {
		if input == nil {
			return Schedule{}, ErrInvalidInput
		}
		normalized, err = normalizeScheduleInput(*input, s.config.MaxTaskBytes)
		if err != nil {
			return Schedule{}, err
		}
	} else if input != nil {
		return Schedule{}, ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.dbClosed {
		return Schedule{}, ErrManagerClosed
	}
	tx, err := s.db.Begin()
	if err != nil {
		return Schedule{}, errors.New("PostgreSQL schedule store unavailable")
	}
	defer tx.Rollback()
	schedule, err := scanPostgresSchedule(tx.QueryRow(`SELECT id,owner,cron,repository,task,status,revision,created_at,updated_at,next_occurrence_at,pending_occurrence_at,skipped_revision,skipped_first_at,skipped_last_at,skipped_count FROM factory_schedules WHERE id=$1 FOR UPDATE`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Schedule{}, ErrNotFound
	}
	if err != nil {
		return Schedule{}, errors.New("lock PostgreSQL schedule mutation")
	}
	if !operator && schedule.Owner != owner {
		return Schedule{}, ErrNotFound
	}
	if schedule.Revision != revision {
		return Schedule{}, ErrScheduleConflict
	}
	if schedule.Status == ScheduleRemoved {
		if mutation == ScheduleResume {
			return Schedule{}, ErrInvalidTransition
		}
		return Schedule{}, ErrScheduleTransition
	}
	switch mutation {
	case SchedulePause:
		if schedule.Status != ScheduleActive {
			return Schedule{}, ErrScheduleTransition
		}
		schedule.Status = SchedulePaused
	case ScheduleResume:
		if schedule.Status != SchedulePaused {
			return Schedule{}, ErrScheduleTransition
		}
		schedule.Status = ScheduleActive
	case ScheduleEdit:
		schedule.Cron, schedule.Repository, schedule.Task = normalized.Cron, normalized.Repository, normalized.Task
	case ScheduleRemove:
		schedule.Status = ScheduleRemoved
	default:
		return Schedule{}, ErrInvalidInput
	}
	schedule.Revision++
	schedule.UpdatedAt = time.Now().UTC()
	if mutation == ScheduleEdit || mutation == ScheduleResume || mutation == SchedulePause || mutation == ScheduleRemove {
		schedule.nextOccurrenceAt, err = NextScheduleOccurrence(schedule.Cron, schedule.UpdatedAt)
		if err != nil {
			return Schedule{}, ErrInvalidInput
		}
		schedule.Skipped = nil
		schedule.pendingOccurrenceAt = nil
		schedule.skippedRevision, schedule.skippedFirstAt, schedule.skippedLastAt, schedule.skippedCount = schedule.Revision, nil, nil, 0
	}
	var result sql.Result
	if mutation == ScheduleEdit {
		result, err = tx.Exec(`UPDATE factory_schedules SET cron=$1,repository=$2,task=$3,status=$4,revision=$5,updated_at=$6,next_occurrence_at=$7,pending_occurrence_at=NULL,skipped_revision=$5,skipped_first_at=NULL,skipped_last_at=NULL,skipped_count=0 WHERE id=$8 AND owner=$9 AND revision=$10`, schedule.Cron, schedule.Repository, schedule.Task, schedule.Status, schedule.Revision, schedule.UpdatedAt, schedule.nextOccurrenceAt, id, schedule.Owner, revision)
	} else {
		result, err = tx.Exec(`UPDATE factory_schedules SET status=$1,revision=$2,updated_at=$3,next_occurrence_at=$4,pending_occurrence_at=NULL,skipped_revision=$2,skipped_first_at=NULL,skipped_last_at=NULL,skipped_count=0 WHERE id=$5 AND owner=$6 AND revision=$7`, schedule.Status, schedule.Revision, schedule.UpdatedAt, schedule.nextOccurrenceAt, id, schedule.Owner, revision)
	}
	if err != nil {
		return Schedule{}, errors.New("update PostgreSQL schedule")
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return Schedule{}, ErrScheduleConflict
	}
	if _, err = tx.Exec(`INSERT INTO factory_schedule_audit(schedule_id,revision,creator,actor,action,occurred_at) VALUES($1,$2,$3,$4,$5,$6)`, id, schedule.Revision, schedule.Owner, owner, scheduleAuditAction(mutation), schedule.UpdatedAt); err != nil {
		return Schedule{}, errors.New("record PostgreSQL schedule mutation audit")
	}
	if err = tx.Commit(); err != nil {
		return Schedule{}, errors.New("commit PostgreSQL schedule")
	}
	return schedule, nil
}

func (s *PostgresStore) backfillScheduleCursors(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dbClosed {
		return ErrManagerClosed
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,cron,updated_at FROM factory_schedules WHERE next_occurrence_at IS NULL`)
	if err != nil {
		return errors.New("read PostgreSQL schedules missing occurrence cursors")
	}
	type cursor struct {
		id, cron string
		updated  time.Time
	}
	var missing []cursor
	for rows.Next() {
		var item cursor
		if err := rows.Scan(&item.id, &item.cron, &item.updated); err != nil {
			rows.Close()
			return errors.New("read PostgreSQL schedule cursor")
		}
		missing = append(missing, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return errors.New("read PostgreSQL schedule cursor")
	}
	if err := rows.Close(); err != nil {
		return errors.New("close PostgreSQL schedule cursors")
	}
	for _, item := range missing {
		next, err := NextScheduleOccurrence(item.cron, item.updated)
		if err != nil {
			return errors.New("calculate PostgreSQL schedule cursor")
		}
		if _, err := s.db.ExecContext(ctx, `UPDATE factory_schedules SET next_occurrence_at=$1,skipped_revision=revision WHERE id=$2 AND next_occurrence_at IS NULL`, next, item.id); err != nil {
			return errors.New("backfill PostgreSQL schedule cursor")
		}
	}
	return nil
}

type postgresScheduleQuery interface{ QueryRow(string, ...any) *sql.Row }

func getPostgresSchedule(query postgresScheduleQuery, id string) (Schedule, error) {
	schedule, err := scanPostgresSchedule(query.QueryRow(`SELECT id,owner,cron,repository,task,status,revision,created_at,updated_at,next_occurrence_at,pending_occurrence_at,skipped_revision,skipped_first_at,skipped_last_at,skipped_count FROM factory_schedules WHERE id=$1`, id))
	if err != nil {
		return Schedule{}, ErrNotFound
	}
	return schedule, nil
}
func scanPostgresSchedule(row scheduleRow) (Schedule, error) {
	var schedule Schedule
	var status string
	var pending, skippedFirst, skippedLast sql.NullTime
	if err := row.Scan(&schedule.ID, &schedule.Owner, &schedule.Cron, &schedule.Repository, &schedule.Task, &status, &schedule.Revision, &schedule.CreatedAt, &schedule.UpdatedAt, &schedule.nextOccurrenceAt, &pending, &schedule.skippedRevision, &skippedFirst, &skippedLast, &schedule.skippedCount); err != nil {
		return Schedule{}, err
	}
	schedule.Status = ScheduleStatus(status)
	schedule.CreatedAt = schedule.CreatedAt.UTC()
	schedule.UpdatedAt = schedule.UpdatedAt.UTC()
	schedule.nextOccurrenceAt = schedule.nextOccurrenceAt.UTC()
	if pending.Valid {
		value := pending.Time.UTC()
		schedule.pendingOccurrenceAt = &value
	}
	if skippedFirst.Valid {
		value := skippedFirst.Time.UTC()
		schedule.skippedFirstAt = &value
	}
	if skippedLast.Valid {
		value := skippedLast.Time.UTC()
		schedule.skippedLastAt = &value
	}
	if schedule.skippedCount > 0 && schedule.skippedFirstAt != nil && schedule.skippedLastAt != nil {
		schedule.Skipped = &SkippedScheduleOccurrences{Revision: schedule.skippedRevision, FirstAt: *schedule.skippedFirstAt, LastAt: *schedule.skippedLastAt, Count: schedule.skippedCount}
	}
	return schedule, nil
}
