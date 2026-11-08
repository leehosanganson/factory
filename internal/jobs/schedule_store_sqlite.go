package restjobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

var _ ScheduleStore = (*SQLiteStore)(nil)

func (s *SQLiteStore) CreateSchedule(owner, key string, input ScheduleInput) (Schedule, bool, error) {
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
		return Schedule{}, false, errors.New("SQLite schedule store unavailable")
	}
	defer tx.Rollback()
	var id, prior string
	err = tx.QueryRow(`SELECT schedule_id,request_json FROM factory_schedule_idempotency WHERE owner=? AND idempotency_key=?`, owner, key).Scan(&id, &prior)
	if err == nil {
		if prior != string(request) {
			return Schedule{}, false, ErrScheduleIDConflict
		}
		schedule, readErr := getSQLiteSchedule(tx, id)
		return schedule, readErr == nil, readErr
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Schedule{}, false, errors.New("read SQLite schedule idempotency")
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
	if _, err = tx.Exec(`INSERT INTO factory_schedules(id,owner,cron,repository,task,status,revision,created_at,updated_at,next_occurrence_at,skipped_revision) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, id, owner, input.Cron, input.Repository, input.Task, schedule.Status, schedule.Revision, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), nextOccurrence.Format(time.RFC3339Nano), 1); err != nil {
		return Schedule{}, false, errors.New("create SQLite schedule")
	}
	if _, err = tx.Exec(`INSERT INTO factory_schedule_idempotency(owner,idempotency_key,schedule_id,request_json) VALUES(?,?,?,?)`, owner, key, id, string(request)); err != nil {
		return Schedule{}, false, errors.New("record SQLite schedule idempotency")
	}
	if _, err = tx.Exec(`INSERT INTO factory_schedule_audit(schedule_id,revision,creator,actor,action,occurred_at) VALUES(?,?,?,?,?,?)`, id, schedule.Revision, owner, owner, ScheduleCreated, now.Format(time.RFC3339Nano)); err != nil {
		return Schedule{}, false, errors.New("record SQLite schedule creation audit")
	}
	if err = tx.Commit(); err != nil {
		return Schedule{}, false, errors.New("commit SQLite schedule")
	}
	return schedule, false, nil
}
func (s *SQLiteStore) GetSchedule(id string) (Schedule, error) {
	if err := s.checkOpen(); err != nil {
		return Schedule{}, err
	}
	return getSQLiteSchedule(s.db, id)
}
func (s *SQLiteStore) ListSchedules(owner string) ([]Schedule, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	owner, err := normalizePrincipalID(owner)
	if err != nil {
		return nil, ErrInvalidInput
	}
	rows, err := s.db.Query(`SELECT id,owner,cron,repository,task,status,revision,created_at,updated_at,next_occurrence_at,pending_occurrence_at,skipped_revision,skipped_first_at,skipped_last_at,skipped_count FROM factory_schedules WHERE owner=? ORDER BY created_at,id`, owner)
	if err != nil {
		return nil, errors.New("list SQLite schedules")
	}
	defer rows.Close()
	var result []Schedule
	for rows.Next() {
		schedule, scanErr := scanSQLiteSchedule(rows)
		if scanErr != nil {
			return nil, errors.New("read SQLite schedules")
		}
		result = append(result, schedule)
	}
	if err = rows.Err(); err != nil {
		return nil, errors.New("read SQLite schedules")
	}
	if result == nil {
		result = []Schedule{}
	}
	return result, nil
}
func (s *SQLiteStore) ListRecentSchedules(limit int) ([]Schedule, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 100 {
		return nil, ErrInvalidInput
	}
	rows, err := s.db.Query(`SELECT id,owner,cron,repository,task,status,revision,created_at,updated_at,next_occurrence_at,pending_occurrence_at,skipped_revision,skipped_first_at,skipped_last_at,skipped_count FROM (SELECT * FROM factory_schedules ORDER BY created_at DESC,id DESC LIMIT ?) ORDER BY created_at,id`, limit)
	if err != nil {
		return nil, errors.New("list recent SQLite schedules")
	}
	defer rows.Close()
	var result []Schedule
	for rows.Next() {
		schedule, scanErr := scanSQLiteSchedule(rows)
		if scanErr != nil {
			return nil, errors.New("read recent SQLite schedules")
		}
		result = append(result, schedule)
	}
	if err = rows.Err(); err != nil {
		return nil, errors.New("read recent SQLite schedules")
	}
	if result == nil {
		result = []Schedule{}
	}
	return result, nil
}

func (s *SQLiteStore) ListAllSchedules() ([]Schedule, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`SELECT id,owner,cron,repository,task,status,revision,created_at,updated_at,next_occurrence_at,pending_occurrence_at,skipped_revision,skipped_first_at,skipped_last_at,skipped_count FROM factory_schedules ORDER BY created_at,id`)
	if err != nil {
		return nil, errors.New("list all SQLite schedules")
	}
	defer rows.Close()
	var result []Schedule
	for rows.Next() {
		schedule, scanErr := scanSQLiteSchedule(rows)
		if scanErr != nil {
			return nil, errors.New("read all SQLite schedules")
		}
		result = append(result, schedule)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("read all SQLite schedules")
	}
	if result == nil {
		result = []Schedule{}
	}
	return result, nil
}

func (s *SQLiteStore) ListScheduleHistory(id, actor string, operator bool, afterRevision int64, limit int) (ScheduleAuditPage, error) {
	if err := s.checkOpen(); err != nil {
		return ScheduleAuditPage{}, err
	}
	if afterRevision < 0 || limit < 1 || limit > MaxScheduleHistoryLimit {
		return ScheduleAuditPage{}, ErrInvalidInput
	}
	var owner string
	if err := s.db.QueryRow(`SELECT owner FROM factory_schedules WHERE id=?`, id).Scan(&owner); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ScheduleAuditPage{}, ErrNotFound
		}
		return ScheduleAuditPage{}, errors.New("read SQLite schedule owner")
	}
	if !operator && owner != actor {
		return ScheduleAuditPage{}, ErrNotFound
	}
	rows, err := s.db.Query(`SELECT schedule_id,creator,actor,action,revision,occurred_at FROM factory_schedule_audit WHERE schedule_id=? AND revision>? ORDER BY revision LIMIT ?`, id, afterRevision, limit+1)
	if err != nil {
		return ScheduleAuditPage{}, errors.New("read SQLite schedule history")
	}
	defer rows.Close()
	result := make([]ScheduleAuditEvent, 0, limit+1)
	for rows.Next() {
		var event ScheduleAuditEvent
		var action, occurred string
		if err := rows.Scan(&event.ScheduleID, &event.Creator, &event.Actor, &action, &event.Revision, &occurred); err != nil {
			return ScheduleAuditPage{}, errors.New("read SQLite schedule history")
		}
		event.Action = ScheduleAction(action)
		event.At, err = time.Parse(time.RFC3339Nano, occurred)
		if err != nil {
			return ScheduleAuditPage{}, errors.New("parse SQLite schedule history")
		}
		result = append(result, event)
	}
	if err := rows.Err(); err != nil {
		return ScheduleAuditPage{}, errors.New("read SQLite schedule history")
	}
	page := ScheduleAuditPage{Events: result}
	if len(result) > limit {
		page.HasMore = true
		page.Events = result[:limit]
		page.NextRevision = page.Events[len(page.Events)-1].Revision
	}
	return page, nil
}

func (s *SQLiteStore) ListScheduleRuns(id, actor string, operator bool, beforeSequence uint64, limit int) (ScheduleRunPage, error) {
	if err := s.checkOpen(); err != nil {
		return ScheduleRunPage{}, err
	}
	if limit < 1 || limit > MaxScheduleHistoryLimit {
		return ScheduleRunPage{}, ErrInvalidInput
	}
	var owner string
	if err := s.db.QueryRow(`SELECT owner FROM factory_schedules WHERE id=?`, id).Scan(&owner); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ScheduleRunPage{}, ErrNotFound
		}
		return ScheduleRunPage{}, errors.New("read SQLite schedule owner")
	}
	if !operator && owner != actor {
		return ScheduleRunPage{}, ErrNotFound
	}
	query := `SELECT r.occurrence_at,j.id,j.status,j.created_at,j.updated_at,j.queue_sequence FROM factory_schedule_runs r JOIN factory_jobs j ON j.id=r.job_id WHERE r.schedule_id=? AND j.submitter=?`
	args := []any{id, owner}
	if beforeSequence > 0 {
		query += ` AND j.queue_sequence<?`
		args = append(args, beforeSequence)
	}
	query += ` ORDER BY j.queue_sequence DESC LIMIT ?`
	args = append(args, limit+1)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return ScheduleRunPage{}, errors.New("read SQLite schedule runs")
	}
	defer rows.Close()
	result := make([]ScheduleRun, 0, limit+1)
	for rows.Next() {
		var occurrence, jobID, status, created, updated string
		var sequence uint64
		if err := rows.Scan(&occurrence, &jobID, &status, &created, &updated, &sequence); err != nil {
			return ScheduleRunPage{}, errors.New("read SQLite schedule runs")
		}
		run, err := scanScheduleRun(occurrence, jobID, Status(status), created, updated, sequence)
		if err != nil {
			return ScheduleRunPage{}, errors.New("decode SQLite schedule occurrence")
		}
		result = append(result, run)
	}
	if err := rows.Err(); err != nil {
		return ScheduleRunPage{}, errors.New("read SQLite schedule runs")
	}
	page := ScheduleRunPage{Runs: result}
	if len(result) > limit {
		page.HasMore = true
		page.Runs = result[:limit]
		page.NextBefore = page.Runs[len(page.Runs)-1].Sequence
	}
	return page, nil
}

func (s *SQLiteStore) MutateSchedule(id, owner string, revision int64, mutation ScheduleMutation, input *ScheduleInput) (Schedule, error) {
	return s.MutateScheduleAs(id, owner, false, revision, mutation, input)
}

func (s *SQLiteStore) MutateScheduleAs(id, actor string, operator bool, revision int64, mutation ScheduleMutation, input *ScheduleInput) (Schedule, error) {
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
		return Schedule{}, errors.New("SQLite schedule store unavailable")
	}
	defer tx.Rollback()
	// Acquire SQLite's single writer before reading the current user revision.
	if result, err := tx.Exec(`UPDATE factory_schedules SET revision=revision WHERE id=?`, id); err != nil {
		return Schedule{}, errors.New("lock SQLite schedule mutation")
	} else if changed, _ := result.RowsAffected(); changed == 0 {
		return Schedule{}, ErrNotFound
	}
	schedule, err := getSQLiteSchedule(tx, id)
	if err != nil {
		return Schedule{}, err
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
		result, err = tx.Exec(`UPDATE factory_schedules SET cron=?,repository=?,task=?,status=?,revision=?,updated_at=?,next_occurrence_at=?,pending_occurrence_at=NULL,skipped_revision=?,skipped_first_at=NULL,skipped_last_at=NULL,skipped_count=0 WHERE id=? AND owner=? AND revision=?`, schedule.Cron, schedule.Repository, schedule.Task, schedule.Status, schedule.Revision, schedule.UpdatedAt.Format(time.RFC3339Nano), schedule.nextOccurrenceAt.Format(time.RFC3339Nano), schedule.Revision, id, schedule.Owner, revision)
	} else {
		result, err = tx.Exec(`UPDATE factory_schedules SET status=?,revision=?,updated_at=?,next_occurrence_at=?,pending_occurrence_at=NULL,skipped_revision=?,skipped_first_at=NULL,skipped_last_at=NULL,skipped_count=0 WHERE id=? AND owner=? AND revision=?`, schedule.Status, schedule.Revision, schedule.UpdatedAt.Format(time.RFC3339Nano), schedule.nextOccurrenceAt.Format(time.RFC3339Nano), schedule.Revision, id, schedule.Owner, revision)
	}
	if err != nil {
		return Schedule{}, errors.New("update SQLite schedule")
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return Schedule{}, ErrScheduleConflict
	}
	if _, err = tx.Exec(`INSERT INTO factory_schedule_audit(schedule_id,revision,creator,actor,action,occurred_at) VALUES(?,?,?,?,?,?)`, id, schedule.Revision, schedule.Owner, owner, scheduleAuditAction(mutation), schedule.UpdatedAt.Format(time.RFC3339Nano)); err != nil {
		return Schedule{}, errors.New("record SQLite schedule mutation audit")
	}
	if err = tx.Commit(); err != nil {
		return Schedule{}, errors.New("commit SQLite schedule")
	}
	return schedule, nil
}

func (s *SQLiteStore) backfillScheduleCursors(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dbClosed {
		return ErrManagerClosed
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,cron,updated_at FROM factory_schedules WHERE next_occurrence_at IS NULL`)
	if err != nil {
		return errors.New("read SQLite schedules missing occurrence cursors")
	}
	type cursor struct{ id, cron, updated string }
	var missing []cursor
	for rows.Next() {
		var item cursor
		if err := rows.Scan(&item.id, &item.cron, &item.updated); err != nil {
			rows.Close()
			return errors.New("read SQLite schedule cursor")
		}
		missing = append(missing, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return errors.New("read SQLite schedule cursor")
	}
	if err := rows.Close(); err != nil {
		return errors.New("close SQLite schedule cursors")
	}
	for _, item := range missing {
		updated, err := time.Parse(time.RFC3339Nano, item.updated)
		if err != nil {
			return errors.New("parse SQLite schedule update time")
		}
		next, err := NextScheduleOccurrence(item.cron, updated)
		if err != nil {
			return errors.New("calculate SQLite schedule cursor")
		}
		if _, err := s.db.ExecContext(ctx, `UPDATE factory_schedules SET next_occurrence_at=?,skipped_revision=revision WHERE id=? AND next_occurrence_at IS NULL`, next.Format(time.RFC3339Nano), item.id); err != nil {
			return errors.New("backfill SQLite schedule cursor")
		}
	}
	return nil
}

type scheduleRow interface{ Scan(...any) error }

func getSQLiteSchedule(query interface{ QueryRow(string, ...any) *sql.Row }, id string) (Schedule, error) {
	schedule, err := scanSQLiteSchedule(query.QueryRow(`SELECT id,owner,cron,repository,task,status,revision,created_at,updated_at,next_occurrence_at,pending_occurrence_at,skipped_revision,skipped_first_at,skipped_last_at,skipped_count FROM factory_schedules WHERE id=?`, id))
	if err != nil {
		return Schedule{}, ErrNotFound
	}
	return schedule, nil
}
func scanSQLiteSchedule(row scheduleRow) (Schedule, error) {
	var schedule Schedule
	var status, created, updated, next string
	var pending, skippedFirst, skippedLast sql.NullString
	if err := row.Scan(&schedule.ID, &schedule.Owner, &schedule.Cron, &schedule.Repository, &schedule.Task, &status, &schedule.Revision, &created, &updated, &next, &pending, &schedule.skippedRevision, &skippedFirst, &skippedLast, &schedule.skippedCount); err != nil {
		return Schedule{}, err
	}
	schedule.Status = ScheduleStatus(status)
	var err error
	schedule.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return Schedule{}, err
	}
	schedule.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
	if err != nil {
		return Schedule{}, err
	}
	schedule.nextOccurrenceAt, err = time.Parse(time.RFC3339Nano, next)
	if err != nil {
		return Schedule{}, err
	}
	if pending.Valid {
		value, parseErr := time.Parse(time.RFC3339Nano, pending.String)
		if parseErr != nil {
			return Schedule{}, parseErr
		}
		schedule.pendingOccurrenceAt = &value
	}
	if skippedFirst.Valid {
		value, parseErr := time.Parse(time.RFC3339Nano, skippedFirst.String)
		if parseErr != nil {
			return Schedule{}, parseErr
		}
		schedule.skippedFirstAt = &value
	}
	if skippedLast.Valid {
		value, parseErr := time.Parse(time.RFC3339Nano, skippedLast.String)
		if parseErr != nil {
			return Schedule{}, parseErr
		}
		schedule.skippedLastAt = &value
	}
	if schedule.skippedCount > 0 && schedule.skippedFirstAt != nil && schedule.skippedLastAt != nil {
		schedule.Skipped = &SkippedScheduleOccurrences{Revision: schedule.skippedRevision, FirstAt: *schedule.skippedFirstAt, LastAt: *schedule.skippedLastAt, Count: schedule.skippedCount}
	}
	return schedule, nil
}
func normalizeScheduleInput(input ScheduleInput, maxTask int) (ScheduleInput, error) {
	input.Cron = strings.TrimSpace(input.Cron)
	if ValidateScheduleCron(input.Cron) != nil {
		return ScheduleInput{}, ErrInvalidInput
	}
	key, request, err := normalizeAdmission("schedule-validation", Request{Repository: input.Repository, Task: input.Task}, maxTask)
	if err != nil || key == "" {
		return ScheduleInput{}, ErrInvalidInput
	}
	input.Repository, input.Task = request.Repository, request.Task
	return input, nil
}
