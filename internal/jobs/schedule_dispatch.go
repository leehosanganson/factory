package restjobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const maxSchedulesPerDispatch = 32

func (s *SQLiteStore) DispatchDueScheduleOccurrences(ctx context.Context, now time.Time, limit int) error {
	if ctx == nil || limit < 1 {
		return ErrInvalidInput
	}
	if err := s.checkAccepting(); err != nil {
		return err
	}
	if limit > maxSchedulesPerDispatch {
		limit = maxSchedulesPerDispatch
	}
	processed := make(map[string]struct{}, limit)
	for range limit {
		id, before, err := s.nextDueScheduleOccurrence(ctx, now, processed)
		if err != nil {
			return err
		}
		if !before {
			return nil
		}
		if err := s.dispatchScheduleOccurrence(ctx, now, id, 1); err != nil {
			return err
		}
		processed[id] = struct{}{}
	}
	return nil
}

func (s *PostgresStore) DispatchDueScheduleOccurrences(ctx context.Context, now time.Time, limit int) error {
	if ctx == nil || limit < 1 {
		return ErrInvalidInput
	}
	if err := s.checkAccepting(); err != nil {
		return err
	}
	if limit > maxSchedulesPerDispatch {
		limit = maxSchedulesPerDispatch
	}
	processed := make(map[string]struct{}, limit)
	for range limit {
		id, before, err := s.nextDueScheduleOccurrence(ctx, now, processed)
		if err != nil {
			return err
		}
		if !before {
			return nil
		}
		if err := s.dispatchScheduleOccurrence(ctx, now, id, 1); err != nil {
			return err
		}
		processed[id] = struct{}{}
	}
	return nil
}

func (s *SQLiteStore) nextDueScheduleOccurrence(ctx context.Context, now time.Time, processed map[string]struct{}) (string, bool, error) {
	if err := s.checkAccepting(); err != nil {
		return "", false, err
	}
	query := `SELECT id FROM factory_schedules WHERE status='active' AND (pending_occurrence_at IS NOT NULL OR next_occurrence_at<=?)`
	args := []any{now.UTC().Format(time.RFC3339Nano)}
	if len(processed) > 0 {
		query += ` AND id NOT IN (` + strings.TrimRight(strings.Repeat("?,", len(processed)), ",") + `)`
		for id := range processed {
			args = append(args, id)
		}
	}
	query += ` ORDER BY COALESCE(pending_occurrence_at,next_occurrence_at),id LIMIT 1`
	var id string
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&id); errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	} else if err != nil {
		return "", false, errors.New("check SQLite due schedules")
	}
	return id, true, nil
}

func (s *PostgresStore) nextDueScheduleOccurrence(ctx context.Context, now time.Time, processed map[string]struct{}) (string, bool, error) {
	if err := s.checkAccepting(); err != nil {
		return "", false, err
	}
	ids := make([]string, 0, len(processed))
	for id := range processed {
		ids = append(ids, id)
	}
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT id FROM factory_schedules WHERE status='active' AND (pending_occurrence_at IS NOT NULL OR next_occurrence_at<=$1) AND NOT (id = ANY($2::text[])) ORDER BY COALESCE(pending_occurrence_at,next_occurrence_at),id LIMIT 1`, now.UTC(), ids).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, errors.New("check PostgreSQL due schedules")
	}
	return id, true, nil
}

// DispatchScheduleOccurrences commits each due occurrence together with its
// canonical Job, or durably retains the oldest occurrence when capacity is full.
func (s *SQLiteStore) DispatchScheduleOccurrences(ctx context.Context, now time.Time, limit int) error {
	if ctx == nil || limit < 1 || limit > maxSchedulesPerDispatch {
		return ErrInvalidInput
	}
	processed := make(map[string]struct{}, limit)
	for range limit {
		id, before, err := s.nextDueScheduleOccurrence(ctx, now, processed)
		if err != nil || !before {
			return err
		}
		if err := s.dispatchScheduleOccurrence(ctx, now, id, 1); err != nil {
			return err
		}
		processed[id] = struct{}{}
	}
	return nil
}

func (s *SQLiteStore) dispatchScheduleOccurrence(ctx context.Context, now time.Time, scheduleID string, limit int) error {
	if ctx == nil || limit != 1 {
		return ErrInvalidInput
	}
	if err := s.checkAccepting(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("begin SQLite schedule dispatch")
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id,owner,cron,repository,task,status,revision,created_at,updated_at,next_occurrence_at,pending_occurrence_at,skipped_revision,skipped_first_at,skipped_last_at,skipped_count FROM factory_schedules WHERE status='active' AND (pending_occurrence_at IS NOT NULL OR next_occurrence_at<=?) AND (?='' OR id=?) ORDER BY COALESCE(pending_occurrence_at,next_occurrence_at),id LIMIT 1`, now.UTC().Format(time.RFC3339Nano), scheduleID, scheduleID)
	if err != nil {
		return errors.New("read due SQLite schedules")
	}
	var schedules []Schedule
	for rows.Next() {
		schedule, e := scanSQLiteSchedule(rows)
		if e != nil {
			rows.Close()
			return errors.New("read due SQLite schedule")
		}
		schedules = append(schedules, schedule)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return errors.New("read due SQLite schedules")
	}
	if err = rows.Close(); err != nil {
		return errors.New("close due SQLite schedules")
	}
	for _, schedule := range schedules {
		if err := dispatchSQLiteSchedule(ctx, tx, s.config, schedule, now.UTC()); err != nil {
			return err
		}
	}
	if len(schedules) == 0 {
		return nil
	}
	if err := tx.Commit(); err != nil {
		return errors.New("commit SQLite schedule dispatch")
	}
	return nil
}

func (s *PostgresStore) DispatchScheduleOccurrences(ctx context.Context, now time.Time, limit int) error {
	if ctx == nil || limit < 1 || limit > maxSchedulesPerDispatch {
		return ErrInvalidInput
	}
	processed := make(map[string]struct{}, limit)
	for range limit {
		id, before, err := s.nextDueScheduleOccurrence(ctx, now, processed)
		if err != nil || !before {
			return err
		}
		if err := s.dispatchScheduleOccurrence(ctx, now, id, 1); err != nil {
			return err
		}
		processed[id] = struct{}{}
	}
	return nil
}

func (s *PostgresStore) dispatchScheduleOccurrence(ctx context.Context, now time.Time, scheduleID string, limit int) error {
	if ctx == nil || limit != 1 {
		return ErrInvalidInput
	}
	if err := s.checkAccepting(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("begin PostgreSQL schedule dispatch")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, postgresStoreMigrationLock+1); err != nil {
		return errors.New("lock PostgreSQL schedule admission")
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,owner,cron,repository,task,status,revision,created_at,updated_at,next_occurrence_at,pending_occurrence_at,skipped_revision,skipped_first_at,skipped_last_at,skipped_count FROM factory_schedules WHERE status='active' AND (pending_occurrence_at IS NOT NULL OR next_occurrence_at<=$1) AND ($2='' OR id=$2) ORDER BY COALESCE(pending_occurrence_at,next_occurrence_at),id LIMIT 1 FOR UPDATE SKIP LOCKED`, now.UTC(), scheduleID)
	if err != nil {
		return errors.New("read due PostgreSQL schedules")
	}
	var schedules []Schedule
	for rows.Next() {
		schedule, e := scanPostgresSchedule(rows)
		if e != nil {
			rows.Close()
			return errors.New("read due PostgreSQL schedule")
		}
		schedules = append(schedules, schedule)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return errors.New("read due PostgreSQL schedules")
	}
	if err = rows.Close(); err != nil {
		return errors.New("close due PostgreSQL schedules")
	}
	for _, schedule := range schedules {
		if err := dispatchPostgresSchedule(ctx, tx, s.config, schedule, now.UTC()); err != nil {
			return err
		}
	}
	if len(schedules) == 0 {
		return nil
	}
	if err := tx.Commit(); err != nil {
		return errors.New("commit PostgreSQL schedule dispatch")
	}
	return nil
}

func dispatchSQLiteSchedule(ctx context.Context, tx *sql.Tx, config Config, schedule Schedule, now time.Time) error {
	occurrence := schedule.nextOccurrenceAt
	if schedule.pendingOccurrenceAt != nil {
		occurrence = *schedule.pendingOccurrenceAt
	}
	if schedule.pendingOccurrenceAt == nil && occurrence.Before(now.Truncate(time.Minute)) {
		var err error
		schedule, err = skipMissedSQLiteOccurrences(ctx, tx, schedule, now)
		if err != nil {
			return err
		}
		occurrence = schedule.nextOccurrenceAt
		if occurrence.After(now) {
			return nil
		}
	}
	_, _, count, err := countScheduleOccurrences(schedule.Cron, occurrence.Add(-time.Minute), now)
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	skipAfter := occurrence
	if schedule.pendingOccurrenceAt != nil && schedule.skippedRevision == schedule.Revision && schedule.skippedLastAt != nil {
		skipAfter = *schedule.skippedLastAt
	}
	_, lastDue, laterCount, err := countScheduleOccurrences(schedule.Cron, skipAfter, now)
	if err != nil {
		return err
	}
	admitErr := admitSQLiteScheduleJob(ctx, tx, config, schedule, occurrence)
	if admitErr != nil && !errors.Is(admitErr, ErrQueueFull) && !errors.Is(admitErr, ErrRegistryFull) {
		return admitErr
	}
	pending := ""
	firstSkipped, lastSkipped := schedule.skippedFirstAt, schedule.skippedLastAt
	skippedCount, skippedRevision := schedule.skippedCount, schedule.skippedRevision
	if skippedRevision != schedule.Revision {
		firstSkipped, lastSkipped, skippedCount, skippedRevision = nil, nil, 0, schedule.Revision
	}
	if admitErr != nil {
		pending = occurrence.Format(time.RFC3339Nano)
	}
	if laterCount > 0 {
		first, _, _, err := countScheduleOccurrences(schedule.Cron, skipAfter, lastDue)
		if err != nil {
			return err
		}
		if firstSkipped == nil {
			firstSkipped = &first
		}
		lastSkipped, skippedCount = &lastDue, skippedCount+laterCount
	}
	next, err := NextScheduleOccurrence(schedule.Cron, now)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE factory_schedules SET next_occurrence_at=?,pending_occurrence_at=NULLIF(?,''),skipped_revision=?,skipped_first_at=?,skipped_last_at=?,skipped_count=? WHERE id=? AND revision=? AND status='active'`, next.Format(time.RFC3339Nano), pending, skippedRevision, nullableSQLiteTime(firstSkipped), nullableSQLiteTime(lastSkipped), skippedCount, schedule.ID, schedule.Revision)
	if err != nil {
		return errors.New("update SQLite schedule dispatch state")
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return ErrScheduleConflict
	}
	return nil
}

func skipMissedSQLiteOccurrences(ctx context.Context, tx *sql.Tx, schedule Schedule, now time.Time) (Schedule, error) {
	missedThrough := now.Truncate(time.Minute).Add(-time.Minute)
	first, last, count, err := countScheduleOccurrences(schedule.Cron, schedule.nextOccurrenceAt.Add(-time.Minute), missedThrough)
	if err != nil {
		return Schedule{}, err
	}
	next, err := NextScheduleOccurrence(schedule.Cron, missedThrough)
	if err != nil {
		return Schedule{}, err
	}
	firstSkipped, lastSkipped, total, revision := schedule.skippedFirstAt, schedule.skippedLastAt, schedule.skippedCount, schedule.skippedRevision
	if revision != schedule.Revision {
		firstSkipped, lastSkipped, total, revision = nil, nil, 0, schedule.Revision
	}
	if count > 0 {
		if firstSkipped == nil {
			firstSkipped = &first
		}
		lastSkipped, total = &last, total+count
	}
	result, err := tx.ExecContext(ctx, `UPDATE factory_schedules SET next_occurrence_at=?,skipped_revision=?,skipped_first_at=?,skipped_last_at=?,skipped_count=? WHERE id=? AND revision=? AND status='active'`, next.Format(time.RFC3339Nano), revision, nullableSQLiteTime(firstSkipped), nullableSQLiteTime(lastSkipped), total, schedule.ID, schedule.Revision)
	if err != nil {
		return Schedule{}, errors.New("record missed SQLite schedule occurrences")
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return Schedule{}, ErrScheduleConflict
	}
	schedule.nextOccurrenceAt, schedule.skippedRevision, schedule.skippedFirstAt, schedule.skippedLastAt, schedule.skippedCount = next, revision, firstSkipped, lastSkipped, total
	return schedule, nil
}

func dispatchPostgresSchedule(ctx context.Context, tx *sql.Tx, config Config, schedule Schedule, now time.Time) error {
	occurrence := schedule.nextOccurrenceAt
	if schedule.pendingOccurrenceAt != nil {
		occurrence = *schedule.pendingOccurrenceAt
	}
	if schedule.pendingOccurrenceAt == nil && occurrence.Before(now.Truncate(time.Minute)) {
		var err error
		schedule, err = skipMissedPostgresOccurrences(ctx, tx, schedule, now)
		if err != nil {
			return err
		}
		occurrence = schedule.nextOccurrenceAt
		if occurrence.After(now) {
			return nil
		}
	}
	_, _, count, err := countScheduleOccurrences(schedule.Cron, occurrence.Add(-time.Minute), now)
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	skipAfter := occurrence
	if schedule.pendingOccurrenceAt != nil && schedule.skippedRevision == schedule.Revision && schedule.skippedLastAt != nil {
		skipAfter = *schedule.skippedLastAt
	}
	_, lastDue, laterCount, err := countScheduleOccurrences(schedule.Cron, skipAfter, now)
	if err != nil {
		return err
	}
	admitErr := admitPostgresScheduleJob(ctx, tx, config, schedule, occurrence)
	if admitErr != nil && !errors.Is(admitErr, ErrQueueFull) && !errors.Is(admitErr, ErrRegistryFull) {
		return admitErr
	}
	var pending any
	firstSkipped, lastSkipped := schedule.skippedFirstAt, schedule.skippedLastAt
	skippedCount, skippedRevision := schedule.skippedCount, schedule.skippedRevision
	if skippedRevision != schedule.Revision {
		firstSkipped, lastSkipped, skippedCount, skippedRevision = nil, nil, 0, schedule.Revision
	}
	if admitErr != nil {
		pending = occurrence
	}
	if laterCount > 0 {
		first, _, _, err := countScheduleOccurrences(schedule.Cron, skipAfter, lastDue)
		if err != nil {
			return err
		}
		if firstSkipped == nil {
			firstSkipped = &first
		}
		lastSkipped, skippedCount = &lastDue, skippedCount+laterCount
	}
	next, err := NextScheduleOccurrence(schedule.Cron, now)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE factory_schedules SET next_occurrence_at=$1,pending_occurrence_at=$2,skipped_revision=$3,skipped_first_at=$4,skipped_last_at=$5,skipped_count=$6 WHERE id=$7 AND revision=$8 AND status='active'`, next, pending, skippedRevision, firstSkipped, lastSkipped, skippedCount, schedule.ID, schedule.Revision)
	if err != nil {
		return errors.New("update PostgreSQL schedule dispatch state")
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return ErrScheduleConflict
	}
	return nil
}

func skipMissedPostgresOccurrences(ctx context.Context, tx *sql.Tx, schedule Schedule, now time.Time) (Schedule, error) {
	missedThrough := now.Truncate(time.Minute).Add(-time.Minute)
	first, last, count, err := countScheduleOccurrences(schedule.Cron, schedule.nextOccurrenceAt.Add(-time.Minute), missedThrough)
	if err != nil {
		return Schedule{}, err
	}
	next, err := NextScheduleOccurrence(schedule.Cron, missedThrough)
	if err != nil {
		return Schedule{}, err
	}
	firstSkipped, lastSkipped, total, revision := schedule.skippedFirstAt, schedule.skippedLastAt, schedule.skippedCount, schedule.skippedRevision
	if revision != schedule.Revision {
		firstSkipped, lastSkipped, total, revision = nil, nil, 0, schedule.Revision
	}
	if count > 0 {
		if firstSkipped == nil {
			firstSkipped = &first
		}
		lastSkipped, total = &last, total+count
	}
	result, err := tx.ExecContext(ctx, `UPDATE factory_schedules SET next_occurrence_at=$1,skipped_revision=$2,skipped_first_at=$3,skipped_last_at=$4,skipped_count=$5 WHERE id=$6 AND revision=$7 AND status='active'`, next, revision, firstSkipped, lastSkipped, total, schedule.ID, schedule.Revision)
	if err != nil {
		return Schedule{}, errors.New("record missed PostgreSQL schedule occurrences")
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return Schedule{}, ErrScheduleConflict
	}
	schedule.nextOccurrenceAt, schedule.skippedRevision, schedule.skippedFirstAt, schedule.skippedLastAt, schedule.skippedCount = next, revision, firstSkipped, lastSkipped, total
	return schedule, nil
}

func admitSQLiteScheduleJob(ctx context.Context, tx *sql.Tx, config Config, schedule Schedule, occurrence time.Time) error {
	key := scheduleOccurrenceKey(schedule.ID, schedule.Revision, occurrence)
	request, err := normalizeScheduleRequest(schedule, config.MaxTaskBytes)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return ErrInvalidInput
	}
	var retained, active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(*) FILTER (WHERE status IN ('queued','running')) FROM factory_jobs`).Scan(&retained, &active); err != nil {
		return errors.New("check SQLite schedule Job capacity")
	}
	if retained >= config.MaxRecords {
		return ErrRegistryFull
	}
	if active >= config.QueueCapacity+config.MaxConcurrentJobs {
		return ErrQueueFull
	}
	var sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(queue_sequence),0)+1 FROM factory_jobs`).Scan(&sequence); err != nil {
		return errors.New("allocate SQLite schedule Job position")
	}
	id, err := newJobID()
	if err != nil {
		return errors.New("create SQLite schedule Job identifier")
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_jobs(id,request_json,status,created_at,updated_at,queue_sequence,history_truncated,submitter) VALUES(?,?,?,?,?,?,0,?)`, id, string(encoded), StatusQueued, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), sequence, schedule.Owner); err != nil {
		return errors.New("persist SQLite scheduled Job")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_job_idempotency(principal_id,key,job_id,request_json) VALUES(?,?,?,?)`, schedule.Owner, key, id, string(encoded)); err != nil {
		return errors.New("persist SQLite scheduled Job identity")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_schedule_runs(schedule_id,occurrence_at,schedule_revision,job_id) VALUES(?,?,?,?)`, schedule.ID, occurrence.UTC().Format(time.RFC3339Nano), schedule.Revision, id); err != nil {
		return errors.New("persist SQLite schedule run provenance")
	}
	if err := insertEvent(ctx, tx, id, "queued", "Job admitted", now, config.MaxEventsPerJob); err != nil {
		return errors.New("persist SQLite scheduled Job history")
	}
	return nil
}

func admitPostgresScheduleJob(ctx context.Context, tx *sql.Tx, config Config, schedule Schedule, occurrence time.Time) error {
	key := scheduleOccurrenceKey(schedule.ID, schedule.Revision, occurrence)
	request, err := normalizeScheduleRequest(schedule, config.MaxTaskBytes)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return ErrInvalidInput
	}
	var retained, active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(*) FILTER (WHERE status IN ('queued','running')) FROM factory_jobs`).Scan(&retained, &active); err != nil {
		return errors.New("check PostgreSQL schedule Job capacity")
	}
	if retained >= config.MaxRecords {
		return ErrRegistryFull
	}
	if active >= config.QueueCapacity+config.MaxConcurrentJobs {
		return ErrQueueFull
	}
	var sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT nextval('factory_rest_job_sequence')`).Scan(&sequence); err != nil {
		return errors.New("allocate PostgreSQL schedule Job position")
	}
	id, err := newJobID()
	if err != nil {
		return errors.New("create PostgreSQL schedule Job identifier")
	}
	var created, updated time.Time
	if err := tx.QueryRowContext(ctx, `INSERT INTO factory_jobs(id,request_json,status,created_at,updated_at,queue_sequence,submitter) VALUES($1,$2,'queued',clock_timestamp(),clock_timestamp(),$3,$4) RETURNING created_at,updated_at`, id, string(encoded), sequence, schedule.Owner).Scan(&created, &updated); err != nil {
		return errors.New("persist PostgreSQL scheduled Job")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_job_idempotency(principal_id,key,job_id,request_json) VALUES($1,$2,$3,$4)`, schedule.Owner, key, id, string(encoded)); err != nil {
		return errors.New("persist PostgreSQL scheduled Job identity")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_schedule_runs(schedule_id,occurrence_at,schedule_revision,job_id) VALUES($1,$2,$3,$4)`, schedule.ID, occurrence.UTC(), schedule.Revision, id); err != nil {
		return errors.New("persist PostgreSQL schedule run provenance")
	}
	if err := insertPostgresEvent(ctx, tx, id, "queued", "Job admitted", config.MaxEventsPerJob); err != nil {
		return errors.New("persist PostgreSQL scheduled Job history")
	}
	return nil
}

func normalizeScheduleRequest(schedule Schedule, maxTaskBytes int) (Request, error) {
	_, request, err := normalizeAdmission("schedule:"+schedule.ID, Request{Repository: schedule.Repository, Task: schedule.Task}, maxTaskBytes)
	return request, err
}

func scheduleOccurrenceKey(id string, revision int64, occurrence time.Time) string {
	return fmt.Sprintf("schedule:%s:%d:%s", id, revision, occurrence.UTC().Format(time.RFC3339))
}
func nullableSQLiteTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func scheduleCronDayMatches(cron scheduleCron, day time.Time) bool {
	if !cron.month.values[int(day.Month())] {
		return false
	}
	dom, dow := cron.dom.values[day.Day()], cron.dow.values[int(day.Weekday())]
	if !cron.dom.wildcard && !cron.dow.wildcard {
		return dom || dow
	}
	if cron.dom.wildcard {
		return dow
	}
	return dom
}

func sortedCronValues(field cronField) []int {
	values := make([]int, 0, len(field.values))
	for value := range field.values {
		values = append(values, value)
	}
	sort.Ints(values)
	return values
}

func countScheduleOccurrences(expression string, after, through time.Time) (time.Time, time.Time, int64, error) {
	cron, err := parseScheduleCron(expression)
	if err != nil {
		return time.Time{}, time.Time{}, 0, ErrInvalidInput
	}
	after, through = after.UTC(), through.UTC()
	if !through.After(after) {
		return time.Time{}, time.Time{}, 0, nil
	}
	start, end := after.Truncate(time.Minute).Add(time.Minute), through.Truncate(time.Minute)
	if end.Before(start) {
		return time.Time{}, time.Time{}, 0, nil
	}
	var first, last time.Time
	var count int64
	minuteValues := sortedCronValues(cron.minute)
	hourValues := sortedCronValues(cron.hour)
	perDay := int64(len(minuteValues) * len(hourValues))
	firstMinute := hourValues[0]*60 + minuteValues[0]
	lastMinute := hourValues[len(hourValues)-1]*60 + minuteValues[len(minuteValues)-1]
	for day := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC); !day.After(end); day = day.AddDate(0, 0, 1) {
		if !scheduleCronDayMatches(cron, day) {
			continue
		}
		dayStart := day
		dayEnd := day.Add(24*time.Hour - time.Minute)
		if dayEnd.Before(start) || dayStart.After(end) {
			continue
		}
		dayFirst, dayLast := dayStart.Add(time.Duration(firstMinute)*time.Minute), dayStart.Add(time.Duration(lastMinute)*time.Minute)
		if dayFirst.Before(start) || dayLast.After(end) {
			for _, hour := range hourValues {
				for _, minute := range minuteValues {
					candidate := day.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute)
					if candidate.Before(start) || candidate.After(end) {
						continue
					}
					if count == 0 {
						first = candidate
					}
					last, count = candidate, count+1
				}
			}
			continue
		}
		if count == 0 {
			first = dayFirst
		}
		last, count = dayLast, count+perDay
	}
	return first, last, count, nil
}
