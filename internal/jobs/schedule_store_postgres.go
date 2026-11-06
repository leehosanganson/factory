package restjobs

import (
	"database/sql"
	"encoding/json"
	"errors"
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
	schedule := Schedule{ID: id, Owner: owner, Cron: input.Cron, Repository: input.Repository, Task: input.Task, Status: ScheduleActive, Revision: 1, CreatedAt: now, UpdatedAt: now}
	if _, err = tx.Exec(`INSERT INTO factory_schedules(id,owner,cron,repository,task,status,revision,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, id, owner, input.Cron, input.Repository, input.Task, schedule.Status, schedule.Revision, now, now); err != nil {
		return Schedule{}, false, errors.New("create PostgreSQL schedule")
	}
	if _, err = tx.Exec(`INSERT INTO factory_schedule_idempotency(owner,idempotency_key,schedule_id,request_json) VALUES($1,$2,$3,$4)`, owner, key, id, string(request)); err != nil {
		return Schedule{}, false, errors.New("record PostgreSQL schedule idempotency")
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
	rows, err := s.db.Query(`SELECT id,owner,cron,repository,task,status,revision,created_at,updated_at FROM factory_schedules WHERE owner=$1 ORDER BY created_at,id`, owner)
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
func (s *PostgresStore) MutateSchedule(id, owner string, revision int64, mutation ScheduleMutation, input *ScheduleInput) (Schedule, error) {
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
		return Schedule{}, errors.New("PostgreSQL schedule store unavailable")
	}
	defer tx.Rollback()
	schedule, err := getPostgresSchedule(tx, id)
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
		result, err = tx.Exec(`UPDATE factory_schedules SET cron=$1,repository=$2,task=$3,status=$4,revision=$5,updated_at=$6 WHERE id=$7 AND owner=$8 AND revision=$9`, schedule.Cron, schedule.Repository, schedule.Task, schedule.Status, schedule.Revision, schedule.UpdatedAt, id, owner, revision)
	} else {
		result, err = tx.Exec(`UPDATE factory_schedules SET status=$1,revision=$2,updated_at=$3 WHERE id=$4 AND owner=$5 AND revision=$6`, schedule.Status, schedule.Revision, schedule.UpdatedAt, id, owner, revision)
	}
	if err != nil {
		return Schedule{}, errors.New("update PostgreSQL schedule")
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return Schedule{}, ErrScheduleConflict
	}
	if err = tx.Commit(); err != nil {
		return Schedule{}, errors.New("commit PostgreSQL schedule")
	}
	return schedule, nil
}

type postgresScheduleQuery interface{ QueryRow(string, ...any) *sql.Row }

func getPostgresSchedule(query postgresScheduleQuery, id string) (Schedule, error) {
	schedule, err := scanPostgresSchedule(query.QueryRow(`SELECT id,owner,cron,repository,task,status,revision,created_at,updated_at FROM factory_schedules WHERE id=$1`, id))
	if err != nil {
		return Schedule{}, ErrNotFound
	}
	return schedule, nil
}
func scanPostgresSchedule(row scheduleRow) (Schedule, error) {
	var schedule Schedule
	var status string
	if err := row.Scan(&schedule.ID, &schedule.Owner, &schedule.Cron, &schedule.Repository, &schedule.Task, &status, &schedule.Revision, &schedule.CreatedAt, &schedule.UpdatedAt); err != nil {
		return Schedule{}, err
	}
	schedule.Status = ScheduleStatus(status)
	schedule.CreatedAt = schedule.CreatedAt.UTC()
	schedule.UpdatedAt = schedule.UpdatedAt.UTC()
	return schedule, nil
}
