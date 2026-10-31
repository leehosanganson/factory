package restjobs

import (
	"context"
	"database/sql"
	"errors"
)

var _ NotificationStore = (*PostgresStore)(nil)

func (s *PostgresStore) ListNotificationsForPrincipal(ctx context.Context, principalID string, before int64, limit int) (NotificationPage, error) {
	if ctx == nil {
		return NotificationPage{}, ErrInvalidInput
	}
	if err := s.checkOpen(); err != nil {
		return NotificationPage{}, err
	}
	principalID, err := normalizePrincipalID(principalID)
	if err != nil {
		return NotificationPage{}, ErrInvalidInput
	}
	if err := validateNotificationPage(ctx, before, limit); err != nil {
		return NotificationPage{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,thread_id,status,created_at FROM factory_thread_notifications WHERE owner=$1 AND ($2=0 OR id<$2) ORDER BY id DESC LIMIT $3`, principalID, before, limit+1)
	if err != nil {
		return NotificationPage{}, errors.New("read PostgreSQL notification inbox")
	}
	defer rows.Close()
	page := NotificationPage{Notifications: make([]ThreadNotification, 0, limit)}
	for rows.Next() {
		var item ThreadNotification
		if err := rows.Scan(&item.ID, &item.ThreadID, &item.Status, &item.CreatedAt); err != nil {
			return NotificationPage{}, errors.New("read PostgreSQL notification inbox")
		}
		if len(page.Notifications) == limit {
			page.HasMore = true
			break
		}
		page.Notifications = append(page.Notifications, item)
	}
	if err := rows.Err(); err != nil {
		return NotificationPage{}, errors.New("read PostgreSQL notification inbox")
	}
	if page.HasMore && len(page.Notifications) > 0 {
		page.NextCursor = page.Notifications[len(page.Notifications)-1].ID
	}
	return page, nil
}

func insertPostgresThreadNotification(ctx context.Context, tx *sql.Tx, threadID string) error {
	var owner string
	var closed sql.NullTime
	var cycle int64
	if err := tx.QueryRowContext(ctx, `SELECT submitter,completed_at,done_cycle FROM factory_threads WHERE id=$1 FOR UPDATE`, threadID).Scan(&owner, &closed, &cycle); errors.Is(err, sql.ErrNoRows) {
		return nil
	} else if err != nil {
		return errors.New("read PostgreSQL Thread notification owner")
	}
	if closed.Valid {
		return nil
	}
	cycle++
	if _, err := tx.ExecContext(ctx, `UPDATE factory_threads SET done_cycle=$2 WHERE id=$1`, threadID, cycle); err != nil {
		return errors.New("advance PostgreSQL Thread Done cycle")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_thread_notifications(thread_id,done_cycle,owner,status) VALUES($1,$2,$3,'done')`, threadID, cycle, owner); err != nil {
		return errors.New("persist PostgreSQL Thread notification")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM factory_thread_notifications WHERE id NOT IN (SELECT id FROM factory_thread_notifications ORDER BY id DESC LIMIT $1)`, maxRecordsLimit); err != nil {
		return errors.New("bound PostgreSQL Thread notification retention")
	}
	return nil
}

func postgresThreadDoneTransition(ctx context.Context, tx *sql.Tx, jobID string) (string, bool, error) {
	var threadID string
	err := tx.QueryRowContext(ctx, `SELECT thread_id FROM factory_thread_jobs WHERE job_id=$1`, jobID).Scan(&threadID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, errors.New("read PostgreSQL Thread membership for completion")
	}
	var locked string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM factory_threads WHERE id=$1 FOR UPDATE`, threadID).Scan(&locked); err != nil {
		return "", false, errors.New("lock PostgreSQL Thread completion")
	}
	var before string
	err = tx.QueryRowContext(ctx, `SELECT CASE
		WHEN EXISTS(SELECT 1 FROM factory_thread_jobs m JOIN factory_jobs j ON j.id=m.job_id WHERE m.thread_id=$1 AND j.status IN ('queued','running')) THEN 'in_progress'
		WHEN EXISTS(SELECT 1 FROM factory_thread_jobs m JOIN factory_jobs j ON j.id=m.job_id WHERE m.thread_id=$1 AND j.status NOT IN ('succeeded','failed','canceled')) THEN 'need_attention'
		ELSE 'done' END FROM factory_threads WHERE id=$1`, threadID).Scan(&before)
	if err != nil {
		return "", false, errors.New("read prior PostgreSQL Thread status")
	}
	return threadID, before == string(ThreadStatusDone), nil
}

func recordPostgresDoneTransition(ctx context.Context, tx *sql.Tx, threadID string, wasDone bool) error {
	if threadID == "" || wasDone {
		return nil
	}
	var after bool
	err := tx.QueryRowContext(ctx, `SELECT NOT EXISTS(SELECT 1 FROM factory_thread_jobs m JOIN factory_jobs j ON j.id=m.job_id WHERE m.thread_id=$1 AND (j.status IN ('queued','running') OR j.status NOT IN ('succeeded','failed','canceled')))`, threadID).Scan(&after)
	if err != nil {
		return errors.New("read updated PostgreSQL Thread status")
	}
	if after {
		return insertPostgresThreadNotification(ctx, tx, threadID)
	}
	return nil
}
