package restjobs

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

func (s *SQLiteStore) CompleteThreadAs(id, principalID string) (ThreadSnapshot, error) {
	if err := s.checkAccepting(); err != nil {
		return ThreadSnapshot{}, err
	}
	principalID, err := normalizePrincipalID(principalID)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), sqliteBusyTimeout)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ThreadSnapshot{}, errors.New("SQLite job store unavailable")
	}
	defer tx.Rollback()
	if result, err := tx.ExecContext(ctx, `UPDATE factory_threads SET completed_at=completed_at WHERE id=?`, id); err != nil {
		return ThreadSnapshot{}, errors.New("lock SQLite Thread completion")
	} else if count, _ := result.RowsAffected(); count == 0 {
		return ThreadSnapshot{}, ErrNotFound
	}
	var owner string
	var completedAt sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT submitter,completed_at FROM factory_threads WHERE id=?`, id).Scan(&owner, &completedAt); errors.Is(err, sql.ErrNoRows) || err == nil && owner != principalID {
		return ThreadSnapshot{}, ErrNotFound
	} else if err != nil {
		return ThreadSnapshot{}, errors.New("read SQLite Thread completion")
	}
	if !completedAt.Valid {
		stamp := time.Now().UTC().Format(time.RFC3339Nano)
		if _, err := tx.ExecContext(ctx, `UPDATE factory_threads SET completed_at=?,completed_by=? WHERE id=?`, stamp, principalID, id); err != nil {
			return ThreadSnapshot{}, errors.New("persist SQLite Thread completion")
		}
	}
	snapshot, err := s.threadSnapshotTx(ctx, tx, id, principalID)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return ThreadSnapshot{}, errors.New("commit SQLite Thread completion")
	}
	return snapshot, nil
}

func (s *PostgresStore) CompleteThreadAs(id, principalID string) (ThreadSnapshot, error) {
	if err := s.checkAccepting(); err != nil {
		return ThreadSnapshot{}, err
	}
	principalID, err := normalizePrincipalID(principalID)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ThreadSnapshot{}, errors.New("PostgreSQL job store unavailable")
	}
	defer tx.Rollback()
	var owner string
	var completedAt sql.NullTime
	if err := tx.QueryRowContext(ctx, `SELECT submitter,completed_at FROM factory_threads WHERE id=$1 FOR UPDATE`, id).Scan(&owner, &completedAt); errors.Is(err, sql.ErrNoRows) || err == nil && owner != principalID {
		return ThreadSnapshot{}, ErrNotFound
	} else if err != nil {
		return ThreadSnapshot{}, errors.New("lock PostgreSQL Thread completion")
	}
	if !completedAt.Valid {
		if _, err := tx.ExecContext(ctx, `UPDATE factory_threads SET completed_at=clock_timestamp(),completed_by=$1 WHERE id=$2`, principalID, id); err != nil {
			return ThreadSnapshot{}, errors.New("persist PostgreSQL Thread completion")
		}
	}
	snapshot, err := s.threadSnapshotTx(ctx, tx, id, principalID)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return ThreadSnapshot{}, errors.New("commit PostgreSQL Thread completion")
	}
	return snapshot, nil
}
