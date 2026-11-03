package restjobs

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"time"

	sqlite "modernc.org/sqlite"
)

var ErrWorkerSessionConflict = errors.New("worker session conflicts with an active session")
var ErrWorkerSessionBusy = errors.New("worker session registration is busy")

type WorkerSession struct {
	ID          string     `json:"id"`
	PrincipalID string     `json:"principal_id"`
	BootID      string     `json:"boot_id"`
	Status      string     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	RetiredAt   *time.Time `json:"retired_at,omitempty"`
}

type WorkerSessionStore interface {
	RegisterWorkerSession(context.Context, string, string) (WorkerSession, bool, error)
	RetireWorkerSession(context.Context, string, string, string) (WorkerSession, error)
}

func validWorkerSessionIdentity(principalID, bootID string) bool {
	return validPostgresOwnerID(principalID) && validPostgresOwnerID(bootID)
}

func registerSQLiteWorkerSession(ctx context.Context, db *sql.DB, principalID, bootID string) (WorkerSession, bool, error) {
	if ctx == nil || !validWorkerSessionIdentity(principalID, bootID) {
		return WorkerSession{}, false, ErrInvalidInput
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return WorkerSession{}, false, errors.New("begin SQLite worker session registration")
	}
	defer tx.Rollback()
	var retired bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM factory_worker_sessions WHERE principal_id=? AND boot_id=? AND status='retired')`, principalID, bootID).Scan(&retired); err != nil {
		return WorkerSession{}, false, errors.New("read SQLite retired worker session")
	}
	if retired {
		return WorkerSession{}, false, ErrWorkerSessionConflict
	}
	var existing WorkerSession
	var created string
	err = tx.QueryRowContext(ctx, `SELECT id,principal_id,boot_id,status,created_at FROM factory_worker_sessions WHERE principal_id=? AND status='active'`, principalID).Scan(&existing.ID, &existing.PrincipalID, &existing.BootID, &existing.Status, &created)
	if err == nil {
		existing.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return WorkerSession{}, false, errors.New("decode SQLite worker session timestamp")
		}
		if existing.BootID != bootID {
			return WorkerSession{}, false, ErrWorkerSessionConflict
		}
		return existing, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		var sqliteErr *sqlite.Error
		if errors.As(err, &sqliteErr) && (sqliteErr.Code()&0xff == 5 || sqliteErr.Code()&0xff == 6) {
			return WorkerSession{}, false, ErrWorkerSessionBusy
		}
		return WorkerSession{}, false, errors.New("read SQLite worker session")
	}
	session, err := newWorkerSession(principalID, bootID)
	if err != nil {
		return WorkerSession{}, false, err
	}
	now := session.CreatedAt.Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_worker_sessions(id,principal_id,boot_id,status,created_at) VALUES(?,?,?,'active',?)`, session.ID, principalID, bootID, now); err != nil {
		var sqliteErr *sqlite.Error
		if errors.As(err, &sqliteErr) && (sqliteErr.Code()&0xff == 5 || sqliteErr.Code()&0xff == 6) {
			return WorkerSession{}, false, ErrWorkerSessionBusy
		}
		return WorkerSession{}, false, errors.New("persist SQLite worker session")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_worker_session_audit(session_id,principal_id,boot_id,event,created_at) VALUES(?,?,?,'registered',?)`, session.ID, principalID, bootID, now); err != nil {
		return WorkerSession{}, false, errors.New("audit SQLite worker session registration")
	}
	if err := tx.Commit(); err != nil {
		var sqliteErr *sqlite.Error
		if errors.As(err, &sqliteErr) && (sqliteErr.Code()&0xff == 5 || sqliteErr.Code()&0xff == 6) {
			return WorkerSession{}, false, ErrWorkerSessionBusy
		}
		return WorkerSession{}, false, errors.New("commit SQLite worker session registration")
	}
	return session, false, nil
}

func retireSQLiteWorkerSession(ctx context.Context, db *sql.DB, principalID, bootID, sessionID string) (WorkerSession, error) {
	if ctx == nil || !validWorkerSessionIdentity(principalID, bootID) || !validPostgresOwnerID(sessionID) {
		return WorkerSession{}, ErrInvalidInput
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return WorkerSession{}, errors.New("begin SQLite worker session retirement")
	}
	defer tx.Rollback()
	var session WorkerSession
	var created string
	if err := tx.QueryRowContext(ctx, `SELECT id,principal_id,boot_id,status,created_at FROM factory_worker_sessions WHERE id=? AND principal_id=? AND boot_id=? AND status='active'`, sessionID, principalID, bootID).Scan(&session.ID, &session.PrincipalID, &session.BootID, &session.Status, &created); errors.Is(err, sql.ErrNoRows) {
		return WorkerSession{}, ErrNotFound
	} else if err != nil {
		return WorkerSession{}, errors.New("read SQLite worker session for retirement")
	}
	session.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return WorkerSession{}, errors.New("decode SQLite worker session timestamp")
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE factory_worker_sessions SET status='retired',retired_at=? WHERE id=? AND status='active'`, now.Format(time.RFC3339Nano), sessionID); err != nil {
		return WorkerSession{}, errors.New("retire SQLite worker session")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_worker_session_audit(session_id,principal_id,boot_id,event,created_at) VALUES(?,?,?,'retired',?)`, session.ID, principalID, session.BootID, now.Format(time.RFC3339Nano)); err != nil {
		return WorkerSession{}, errors.New("audit SQLite worker session retirement")
	}
	if err := tx.Commit(); err != nil {
		return WorkerSession{}, errors.New("commit SQLite worker session retirement")
	}
	session.Status, session.RetiredAt = "retired", &now
	return session, nil
}

func registerPostgresWorkerSession(ctx context.Context, db *sql.DB, principalID, bootID string) (WorkerSession, bool, error) {
	if ctx == nil || !validWorkerSessionIdentity(principalID, bootID) {
		return WorkerSession{}, false, ErrInvalidInput
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return WorkerSession{}, false, errors.New("begin PostgreSQL worker session registration")
	}
	defer tx.Rollback()
	hash := sha256.Sum256([]byte(principalID))
	lockKey := int64(0)
	for _, b := range hash[:8] {
		lockKey = lockKey<<8 | int64(b)
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, lockKey); err != nil {
		return WorkerSession{}, false, errors.New("serialize PostgreSQL worker session registration")
	}
	var retired bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM factory_worker_sessions WHERE principal_id=$1 AND boot_id=$2 AND status='retired')`, principalID, bootID).Scan(&retired); err != nil {
		return WorkerSession{}, false, errors.New("read PostgreSQL retired worker session")
	}
	if retired {
		return WorkerSession{}, false, ErrWorkerSessionConflict
	}
	var existing WorkerSession
	if err := tx.QueryRowContext(ctx, `SELECT id,principal_id,boot_id,status,created_at FROM factory_worker_sessions WHERE principal_id=$1 AND status='active' FOR UPDATE`, principalID).Scan(&existing.ID, &existing.PrincipalID, &existing.BootID, &existing.Status, &existing.CreatedAt); err == nil {
		if existing.BootID != bootID {
			return WorkerSession{}, false, ErrWorkerSessionConflict
		}
		return existing, true, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return WorkerSession{}, false, errors.New("read PostgreSQL worker session")
	}
	session, err := newWorkerSession(principalID, bootID)
	if err != nil {
		return WorkerSession{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_worker_sessions(id,principal_id,boot_id,status,created_at) VALUES($1,$2,$3,'active',$4)`, session.ID, principalID, bootID, session.CreatedAt); err != nil {
		return WorkerSession{}, false, errors.New("persist PostgreSQL worker session")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_worker_session_audit(session_id,principal_id,boot_id,event) VALUES($1,$2,$3,'registered')`, session.ID, principalID, bootID); err != nil {
		return WorkerSession{}, false, errors.New("audit PostgreSQL worker session registration")
	}
	if err := tx.Commit(); err != nil {
		return WorkerSession{}, false, errors.New("commit PostgreSQL worker session registration")
	}
	return session, false, nil
}

func retirePostgresWorkerSession(ctx context.Context, db *sql.DB, principalID, bootID, sessionID string) (WorkerSession, error) {
	if ctx == nil || !validWorkerSessionIdentity(principalID, bootID) || !validPostgresOwnerID(sessionID) {
		return WorkerSession{}, ErrInvalidInput
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return WorkerSession{}, errors.New("begin PostgreSQL worker session retirement")
	}
	defer tx.Rollback()
	var session WorkerSession
	if err := tx.QueryRowContext(ctx, `SELECT id,principal_id,boot_id,status,created_at FROM factory_worker_sessions WHERE id=$1 AND principal_id=$2 AND boot_id=$3 AND status='active' FOR UPDATE`, sessionID, principalID, bootID).Scan(&session.ID, &session.PrincipalID, &session.BootID, &session.Status, &session.CreatedAt); errors.Is(err, sql.ErrNoRows) {
		return WorkerSession{}, ErrNotFound
	} else if err != nil {
		return WorkerSession{}, errors.New("read PostgreSQL worker session for retirement")
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE factory_worker_sessions SET status='retired',retired_at=$1 WHERE id=$2 AND status='active'`, now, sessionID); err != nil {
		return WorkerSession{}, errors.New("retire PostgreSQL worker session")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_worker_session_audit(session_id,principal_id,boot_id,event) VALUES($1,$2,$3,'retired')`, session.ID, principalID, session.BootID); err != nil {
		return WorkerSession{}, errors.New("audit PostgreSQL worker session retirement")
	}
	if err := tx.Commit(); err != nil {
		return WorkerSession{}, errors.New("commit PostgreSQL worker session retirement")
	}
	session.Status, session.RetiredAt = "retired", &now
	return session, nil
}

func newWorkerSession(principalID, bootID string) (WorkerSession, error) {
	id, err := newJobID()
	if err != nil {
		return WorkerSession{}, fmt.Errorf("create worker session ID: %w", err)
	}
	return WorkerSession{ID: id, PrincipalID: principalID, BootID: bootID, Status: "active", CreatedAt: time.Now().UTC()}, nil
}

func (s *SQLiteStore) RegisterWorkerSession(ctx context.Context, principalID, bootID string) (WorkerSession, bool, error) {
	return registerSQLiteWorkerSession(ctx, s.db, principalID, bootID)
}
func (s *SQLiteStore) RetireWorkerSession(ctx context.Context, principalID, bootID, sessionID string) (WorkerSession, error) {
	return retireSQLiteWorkerSession(ctx, s.db, principalID, bootID, sessionID)
}
func (s *PostgresStore) RegisterWorkerSession(ctx context.Context, principalID, bootID string) (WorkerSession, bool, error) {
	return registerPostgresWorkerSession(ctx, s.db, principalID, bootID)
}
func (s *PostgresStore) RetireWorkerSession(ctx context.Context, principalID, bootID, sessionID string) (WorkerSession, error) {
	return retirePostgresWorkerSession(ctx, s.db, principalID, bootID, sessionID)
}
