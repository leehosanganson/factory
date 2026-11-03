package restjobs

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var _ NotificationStore = (*LocalManager)(nil)
var _ NotificationStore = (*SQLiteStore)(nil)

func (s *SQLiteStore) ListNotificationsForPrincipal(ctx context.Context, principalID string, before int64, limit int) (NotificationPage, error) {
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
	rows, err := s.db.QueryContext(ctx, `SELECT id,thread_id,status,created_at FROM factory_thread_notifications WHERE owner=? AND (?=0 OR id<?) ORDER BY id DESC LIMIT ?`, principalID, before, before, limit+1)
	if err != nil {
		return NotificationPage{}, errors.New("read SQLite notification inbox")
	}
	defer rows.Close()
	page := NotificationPage{Notifications: make([]ThreadNotification, 0, limit)}
	for rows.Next() {
		var item ThreadNotification
		var created string
		if err := rows.Scan(&item.ID, &item.ThreadID, &item.Status, &created); err != nil {
			return NotificationPage{}, errors.New("read SQLite notification inbox")
		}
		item.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return NotificationPage{}, errors.New("decode SQLite notification timestamp")
		}
		if len(page.Notifications) == limit {
			page.HasMore = true
			break
		}
		page.Notifications = append(page.Notifications, item)
	}
	if err := rows.Err(); err != nil {
		return NotificationPage{}, errors.New("read SQLite notification inbox")
	}
	if page.HasMore && len(page.Notifications) > 0 {
		page.NextCursor = page.Notifications[len(page.Notifications)-1].ID
	}
	return page, nil
}

func insertSQLiteThreadNotification(ctx context.Context, tx *sql.Tx, threadID string, now time.Time) error {
	var owner string
	var closed sql.NullString
	var cycle int64
	if err := tx.QueryRowContext(ctx, `SELECT submitter,completed_at,done_cycle FROM factory_threads WHERE id=?`, threadID).Scan(&owner, &closed, &cycle); errors.Is(err, sql.ErrNoRows) {
		return nil
	} else if err != nil {
		return errors.New("read SQLite Thread notification owner")
	}
	if closed.Valid {
		return nil
	}
	cycle++
	if _, err := tx.ExecContext(ctx, `UPDATE factory_threads SET done_cycle=? WHERE id=?`, cycle, threadID); err != nil {
		return errors.New("advance SQLite Thread Done cycle")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_thread_notifications(thread_id,done_cycle,owner,status,created_at) VALUES(?,?,?,'done',?)`, threadID, cycle, owner, now.UTC().Format(time.RFC3339Nano)); err != nil {
		return errors.New("persist SQLite Thread notification")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM factory_thread_notifications WHERE id NOT IN (SELECT id FROM factory_thread_notifications ORDER BY id DESC LIMIT ?)`, maxRecordsLimit); err != nil {
		return errors.New("bound SQLite Thread notification retention")
	}
	return nil
}

func sqliteThreadDoneTransition(ctx context.Context, tx *sql.Tx, jobID string) (string, bool, error) {
	var threadID string
	err := tx.QueryRowContext(ctx, `SELECT thread_id FROM factory_thread_jobs WHERE job_id=?`, jobID).Scan(&threadID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, errors.New("read SQLite Thread membership for completion")
	}
	var before string
	err = tx.QueryRowContext(ctx, `SELECT CASE
		WHEN EXISTS(SELECT 1 FROM factory_thread_jobs m JOIN factory_jobs j ON j.id=m.job_id WHERE m.thread_id=? AND j.status IN ('queued','running')) THEN 'in_progress'
		WHEN EXISTS(SELECT 1 FROM factory_thread_jobs m JOIN factory_jobs j ON j.id=m.job_id WHERE m.thread_id=? AND j.status NOT IN ('succeeded','failed','canceled')) THEN 'need_attention'
		ELSE 'done' END FROM factory_threads WHERE id=?`, threadID, threadID, threadID).Scan(&before)
	if err != nil {
		return "", false, errors.New("read prior SQLite Thread status")
	}
	return threadID, before == string(ThreadStatusDone), nil
}

func recordSQLiteDoneTransition(ctx context.Context, tx *sql.Tx, threadID string, wasDone bool, now time.Time) error {
	if threadID == "" || wasDone {
		return nil
	}
	var after bool
	err := tx.QueryRowContext(ctx, `SELECT NOT EXISTS(SELECT 1 FROM factory_thread_jobs m JOIN factory_jobs j ON j.id=m.job_id WHERE m.thread_id=? AND (j.status IN ('queued','running') OR j.status NOT IN ('succeeded','failed','canceled')))`, threadID).Scan(&after)
	if err != nil {
		return errors.New("read updated SQLite Thread status")
	}
	if after {
		return insertSQLiteThreadNotification(ctx, tx, threadID, now)
	}
	return nil
}
