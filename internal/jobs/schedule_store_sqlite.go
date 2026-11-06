package restjobs

import (
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
	schedule := Schedule{ID: id, Owner: owner, Cron: input.Cron, Repository: input.Repository, Task: input.Task, Status: ScheduleActive, Revision: 1, CreatedAt: now, UpdatedAt: now}
	if _, err = tx.Exec(`INSERT INTO factory_schedules(id,owner,cron,repository,task,status,revision,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)`, id, owner, input.Cron, input.Repository, input.Task, schedule.Status, schedule.Revision, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
		return Schedule{}, false, errors.New("create SQLite schedule")
	}
	if _, err = tx.Exec(`INSERT INTO factory_schedule_idempotency(owner,idempotency_key,schedule_id,request_json) VALUES(?,?,?,?)`, owner, key, id, string(request)); err != nil {
		return Schedule{}, false, errors.New("record SQLite schedule idempotency")
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
	rows, err := s.db.Query(`SELECT id,owner,cron,repository,task,status,revision,created_at,updated_at FROM factory_schedules WHERE owner=? ORDER BY created_at,id`, owner)
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
func (s *SQLiteStore) MutateSchedule(id, owner string, revision int64, mutation ScheduleMutation, input *ScheduleInput) (Schedule, error) {
	owner, err := normalizePrincipalID(owner)
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
	schedule, err := getSQLiteSchedule(tx, id)
	if err != nil {
		return Schedule{}, err
	}
	if schedule.Owner != owner {
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
	var result sql.Result
	if mutation == ScheduleEdit {
		result, err = tx.Exec(`UPDATE factory_schedules SET cron=?,repository=?,task=?,status=?,revision=?,updated_at=? WHERE id=? AND owner=? AND revision=?`, schedule.Cron, schedule.Repository, schedule.Task, schedule.Status, schedule.Revision, schedule.UpdatedAt.Format(time.RFC3339Nano), id, owner, revision)
	} else {
		result, err = tx.Exec(`UPDATE factory_schedules SET status=?,revision=?,updated_at=? WHERE id=? AND owner=? AND revision=?`, schedule.Status, schedule.Revision, schedule.UpdatedAt.Format(time.RFC3339Nano), id, owner, revision)
	}
	if err != nil {
		return Schedule{}, errors.New("update SQLite schedule")
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return Schedule{}, ErrScheduleConflict
	}
	if err = tx.Commit(); err != nil {
		return Schedule{}, errors.New("commit SQLite schedule")
	}
	return schedule, nil
}

type scheduleRow interface{ Scan(...any) error }

func getSQLiteSchedule(query interface{ QueryRow(string, ...any) *sql.Row }, id string) (Schedule, error) {
	schedule, err := scanSQLiteSchedule(query.QueryRow(`SELECT id,owner,cron,repository,task,status,revision,created_at,updated_at FROM factory_schedules WHERE id=?`, id))
	if err != nil {
		return Schedule{}, ErrNotFound
	}
	return schedule, nil
}
func scanSQLiteSchedule(row scheduleRow) (Schedule, error) {
	var schedule Schedule
	var status, created, updated string
	if err := row.Scan(&schedule.ID, &schedule.Owner, &schedule.Cron, &schedule.Repository, &schedule.Task, &status, &schedule.Revision, &created, &updated); err != nil {
		return Schedule{}, err
	}
	schedule.Status = ScheduleStatus(status)
	var err error
	schedule.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return Schedule{}, err
	}
	schedule.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
	return schedule, err
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
