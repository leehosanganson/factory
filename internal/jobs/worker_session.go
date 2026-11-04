package restjobs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	sqlite "modernc.org/sqlite"
)

var ErrWorkerSessionConflict = errors.New("worker session conflicts with an active session")
var ErrWorkerSessionBusy = errors.New("worker session registration is busy")

const WorkerSessionCapabilityTTL = 15 * time.Minute
const workerSessionCapabilityBytes = 32

type WorkerSession struct {
	ID                  string     `json:"id"`
	PrincipalID         string     `json:"principal_id"`
	BootID              string     `json:"boot_id"`
	Status              string     `json:"status"`
	CreatedAt           time.Time  `json:"created_at"`
	RetiredAt           *time.Time `json:"retired_at,omitempty"`
	Capability          string     `json:"capability,omitempty"`
	CapabilityExpiresAt *time.Time `json:"capability_expires_at,omitempty"`
}

type WorkerSessionStore interface {
	RegisterWorkerSession(context.Context, string, string) (WorkerSession, bool, error)
	AuthenticateWorkerCapability(context.Context, string, string) (WorkerSession, error)
	RetireWorkerCapability(context.Context, string, string) (WorkerSession, error)
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
		capability, expiresAt, err := rotateWorkerSessionCapability(ctx, tx, "?", existing.ID, existing.PrincipalID, existing.BootID, "capability_rotated")
		if err != nil {
			return WorkerSession{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return WorkerSession{}, false, errors.New("commit SQLite worker session capability rotation")
		}
		existing.Capability, existing.CapabilityExpiresAt = capability, &expiresAt
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
	capability, expiresAt, err := rotateWorkerSessionCapability(ctx, tx, "?", session.ID, principalID, bootID, "capability_issued")
	if err != nil {
		return WorkerSession{}, false, err
	}
	session.Capability, session.CapabilityExpiresAt = capability, &expiresAt
	if err := tx.Commit(); err != nil {
		var sqliteErr *sqlite.Error
		if errors.As(err, &sqliteErr) && (sqliteErr.Code()&0xff == 5 || sqliteErr.Code()&0xff == 6) {
			return WorkerSession{}, false, ErrWorkerSessionBusy
		}
		return WorkerSession{}, false, errors.New("commit SQLite worker session registration")
	}
	return session, false, nil
}

func retireSQLiteWorkerCapability(ctx context.Context, db *sql.DB, principalID, capability string) (WorkerSession, error) {
	if ctx == nil || !validPostgresOwnerID(principalID) || !validWorkerSessionCapability(capability) {
		return WorkerSession{}, ErrNotFound
	}
	hash := sha256.Sum256([]byte(capability))
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return WorkerSession{}, errors.New("begin SQLite worker session retirement")
	}
	defer tx.Rollback()
	var session WorkerSession
	var created string
	if err := tx.QueryRowContext(ctx, `SELECT id,principal_id,boot_id,status,created_at FROM factory_worker_sessions WHERE principal_id=? AND capability_hash=? AND status='active' AND capability_expires_at>?`, principalID, hex.EncodeToString(hash[:]), expiryComparisonValue("?")).Scan(&session.ID, &session.PrincipalID, &session.BootID, &session.Status, &created); errors.Is(err, sql.ErrNoRows) {
		return WorkerSession{}, ErrNotFound
	} else if err != nil {
		return WorkerSession{}, errors.New("read SQLite worker session for capability retirement")
	}
	session.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return WorkerSession{}, errors.New("decode SQLite worker session timestamp")
	}
	now, err := retireWorkerSessionTx(ctx, tx, "?", session, hash, true)
	if err != nil {
		return WorkerSession{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkerSession{}, errors.New("commit SQLite worker session capability retirement")
	}
	session.Status, session.RetiredAt = "retired", &now
	return session, nil
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
	if _, err := retireWorkerSessionTx(ctx, tx, "?", session, [32]byte{}, false); err != nil {
		return WorkerSession{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkerSession{}, errors.New("commit SQLite worker session retirement")
	}
	session.Status, session.RetiredAt = "retired", &now
	return session, nil
}

func retirePostgresWorkerCapability(ctx context.Context, db *sql.DB, principalID, capability string) (WorkerSession, error) {
	if ctx == nil || !validPostgresOwnerID(principalID) || !validWorkerSessionCapability(capability) {
		return WorkerSession{}, ErrNotFound
	}
	hash := sha256.Sum256([]byte(capability))
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return WorkerSession{}, errors.New("begin PostgreSQL worker session retirement")
	}
	defer tx.Rollback()
	var session WorkerSession
	if err := tx.QueryRowContext(ctx, `SELECT id,principal_id,boot_id,status,created_at FROM factory_worker_sessions WHERE principal_id=$1 AND capability_hash=$2 AND status='active' AND capability_expires_at>clock_timestamp() FOR UPDATE`, principalID, hex.EncodeToString(hash[:])).Scan(&session.ID, &session.PrincipalID, &session.BootID, &session.Status, &session.CreatedAt); errors.Is(err, sql.ErrNoRows) {
		return WorkerSession{}, ErrNotFound
	} else if err != nil {
		return WorkerSession{}, errors.New("read PostgreSQL worker session for capability retirement")
	}
	now, err := retireWorkerSessionTx(ctx, tx, "$", session, hash, true)
	if err != nil {
		return WorkerSession{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkerSession{}, errors.New("commit PostgreSQL worker session capability retirement")
	}
	session.Status, session.RetiredAt = "retired", &now
	return session, nil
}

func retireWorkerSessionTx(ctx context.Context, tx workerSessionCapabilityTx, placeholder string, session WorkerSession, capabilityHash [32]byte, requireCapability bool) (time.Time, error) {
	now := time.Now().UTC()
	query := `UPDATE factory_worker_sessions SET status='retired',retired_at=?,capability_hash=NULL,capability_expires_at=NULL WHERE id=? AND status='active'`
	args := []any{now.Format(time.RFC3339Nano), session.ID}
	if requireCapability {
		query += ` AND capability_hash=?`
		args = append(args, hex.EncodeToString(capabilityHash[:]))
	}
	if placeholder != "?" {
		query = `UPDATE factory_worker_sessions SET status='retired',retired_at=$1,capability_hash=NULL,capability_expires_at=NULL WHERE id=$2 AND status='active'`
		args = []any{now, session.ID}
		if requireCapability {
			query += ` AND capability_hash=$3`
			args = append(args, hex.EncodeToString(capabilityHash[:]))
		}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return time.Time{}, errors.New("retire worker session")
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return time.Time{}, ErrNotFound
	}
	var auditErr error
	if placeholder == "?" {
		_, auditErr = tx.ExecContext(ctx, `INSERT INTO factory_worker_session_audit(session_id,principal_id,boot_id,event,created_at) VALUES(?,?,?,'retired',?)`, session.ID, session.PrincipalID, session.BootID, now.Format(time.RFC3339Nano))
	} else {
		_, auditErr = tx.ExecContext(ctx, `INSERT INTO factory_worker_session_audit(session_id,principal_id,boot_id,event) VALUES($1,$2,$3,'retired')`, session.ID, session.PrincipalID, session.BootID)
	}
	if auditErr != nil {
		return time.Time{}, errors.New("audit worker session retirement")
	}
	return now, nil
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
		capability, expiresAt, err := rotateWorkerSessionCapability(ctx, tx, "$", existing.ID, existing.PrincipalID, existing.BootID, "capability_rotated")
		if err != nil {
			return WorkerSession{}, false, err
		}
		existing.Capability, existing.CapabilityExpiresAt = capability, &expiresAt
		if err := tx.Commit(); err != nil {
			return WorkerSession{}, false, errors.New("commit PostgreSQL worker session capability rotation")
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
	capability, expiresAt, err := rotateWorkerSessionCapability(ctx, tx, "$", session.ID, principalID, bootID, "capability_issued")
	if err != nil {
		return WorkerSession{}, false, err
	}
	session.Capability, session.CapabilityExpiresAt = capability, &expiresAt
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
	if _, err := tx.ExecContext(ctx, `UPDATE factory_worker_sessions SET status='retired',retired_at=$1,capability_hash=NULL,capability_expires_at=NULL WHERE id=$2 AND status='active'`, now, sessionID); err != nil {
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

type workerSessionCapabilityTx interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func rotateWorkerSessionCapability(ctx context.Context, tx workerSessionCapabilityTx, placeholder, sessionID, principalID, bootID, event string) (string, time.Time, error) {
	raw := make([]byte, workerSessionCapabilityBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", time.Time{}, errors.New("generate worker session capability")
	}
	capability := base64.RawURLEncoding.EncodeToString(raw)
	hash := sha256.Sum256([]byte(capability))
	expiresAt := time.Now().UTC().Add(WorkerSessionCapabilityTTL)
	var query string
	if placeholder == "?" {
		query = `UPDATE factory_worker_sessions SET capability_hash=?,capability_expires_at=? WHERE id=? AND status='active'`
	} else {
		query = `UPDATE factory_worker_sessions SET capability_hash=$1,capability_expires_at=$2 WHERE id=$3 AND status='active'`
	}
	expiryValue := any(expiresAt)
	if placeholder == "?" {
		expiryValue = expiresAt.Format("2006-01-02T15:04:05.000000000Z07:00")
	}
	result, err := tx.ExecContext(ctx, query, hex.EncodeToString(hash[:]), expiryValue, sessionID)
	if err != nil {
		return "", time.Time{}, errors.New("persist worker session capability")
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return "", time.Time{}, errors.New("worker session capability target is unavailable")
	}
	auditQuery := `INSERT INTO factory_worker_session_audit(session_id,principal_id,boot_id,event,capability_hash,capability_expires_at,created_at) VALUES(?,?,?,?,?,?,?)`
	createdAt := any(time.Now().UTC().Format(time.RFC3339Nano))
	if placeholder != "?" {
		auditQuery = `INSERT INTO factory_worker_session_audit(session_id,principal_id,boot_id,event,capability_hash,capability_expires_at) VALUES($1,$2,$3,$4,$5,$6)`
		createdAt = nil
	}
	var auditErr error
	if placeholder == "?" {
		_, auditErr = tx.ExecContext(ctx, auditQuery, sessionID, principalID, bootID, event, hex.EncodeToString(hash[:]), expiresAt.Format(time.RFC3339Nano), createdAt)
	} else {
		_, auditErr = tx.ExecContext(ctx, auditQuery, sessionID, principalID, bootID, event, hex.EncodeToString(hash[:]), expiresAt)
	}
	if auditErr != nil {
		return "", time.Time{}, fmt.Errorf("audit worker session capability: %w", auditErr)
	}
	return capability, expiresAt, nil
}

func authenticateSQLiteWorkerCapability(ctx context.Context, db *sql.DB, principalID, capability string) (WorkerSession, error) {
	return authenticateWorkerCapability(ctx, db, principalID, capability, "?")
}

func authenticatePostgresWorkerCapability(ctx context.Context, db *sql.DB, principalID, capability string) (WorkerSession, error) {
	return authenticateWorkerCapability(ctx, db, principalID, capability, "$1")
}

func authenticateWorkerCapability(ctx context.Context, db *sql.DB, principalID, capability, placeholder string) (WorkerSession, error) {
	if ctx == nil || !validPostgresOwnerID(principalID) || !validWorkerSessionCapability(capability) {
		return WorkerSession{}, ErrNotFound
	}
	hash := sha256.Sum256([]byte(capability))
	query := `SELECT id,principal_id,boot_id,status,created_at,capability_expires_at FROM factory_worker_sessions WHERE principal_id=? AND capability_hash=? AND status='active' AND capability_expires_at > ?`
	if placeholder != "?" {
		query = `SELECT id,principal_id,boot_id,status,created_at,capability_expires_at FROM factory_worker_sessions WHERE principal_id=$1 AND capability_hash=$2 AND status='active' AND capability_expires_at > $3`
	}
	var session WorkerSession
	var createdAtRaw any
	var expiresAtRaw any
	if err := db.QueryRowContext(ctx, query, principalID, hex.EncodeToString(hash[:]), expiryComparisonValue(placeholder)).Scan(&session.ID, &session.PrincipalID, &session.BootID, &session.Status, &createdAtRaw, &expiresAtRaw); errors.Is(err, sql.ErrNoRows) {
		return WorkerSession{}, ErrNotFound
	} else if err != nil {
		return WorkerSession{}, fmt.Errorf("authenticate worker session capability: %w", err)
	}
	var expiresAt time.Time
	if placeholder == "?" {
		createdAtText, ok := createdAtRaw.(string)
		if !ok {
			if bytes, bytesOK := createdAtRaw.([]byte); bytesOK {
				createdAtText, ok = string(bytes), true
			}
		}
		if !ok {
			return WorkerSession{}, errors.New("decode SQLite worker session timestamp")
		}
		parsedCreatedAt, err := time.Parse(time.RFC3339Nano, createdAtText)
		if err != nil {
			return WorkerSession{}, errors.New("decode SQLite worker session timestamp")
		}
		session.CreatedAt = parsedCreatedAt
		expiresAtText, ok := expiresAtRaw.(string)
		if !ok {
			if bytes, bytesOK := expiresAtRaw.([]byte); bytesOK {
				expiresAtText, ok = string(bytes), true
			}
		}
		if !ok {
			return WorkerSession{}, errors.New("decode SQLite worker session capability expiry")
		}
		expiresAt, err = time.Parse(time.RFC3339Nano, expiresAtText)
		if err != nil {
			return WorkerSession{}, errors.New("decode SQLite worker session capability expiry")
		}
	} else {
		var ok bool
		session.CreatedAt, ok = createdAtRaw.(time.Time)
		if !ok {
			return WorkerSession{}, errors.New("decode PostgreSQL worker session timestamp")
		}
		expiresAt, ok = expiresAtRaw.(time.Time)
		if !ok {
			return WorkerSession{}, errors.New("decode PostgreSQL worker session capability expiry")
		}
	}
	session.CapabilityExpiresAt = &expiresAt
	return session, nil
}

func validWorkerSessionCapability(capability string) bool {
	if len(capability) != base64.RawURLEncoding.EncodedLen(workerSessionCapabilityBytes) {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(capability)
	return err == nil && len(decoded) == workerSessionCapabilityBytes
}

func expiryComparisonValue(placeholder string) any {
	now := time.Now().UTC()
	if placeholder == "?" {
		return now.Format("2006-01-02T15:04:05.000000000Z07:00")
	}
	return now
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
func (s *SQLiteStore) AuthenticateWorkerCapability(ctx context.Context, principalID, capability string) (WorkerSession, error) {
	return authenticateSQLiteWorkerCapability(ctx, s.db, principalID, capability)
}
func (s *SQLiteStore) RetireWorkerCapability(ctx context.Context, principalID, capability string) (WorkerSession, error) {
	return retireSQLiteWorkerCapability(ctx, s.db, principalID, capability)
}
func (s *SQLiteStore) RetireWorkerSession(ctx context.Context, principalID, bootID, sessionID string) (WorkerSession, error) {
	return retireSQLiteWorkerSession(ctx, s.db, principalID, bootID, sessionID)
}
func (s *PostgresStore) RegisterWorkerSession(ctx context.Context, principalID, bootID string) (WorkerSession, bool, error) {
	return registerPostgresWorkerSession(ctx, s.db, principalID, bootID)
}
func (s *PostgresStore) AuthenticateWorkerCapability(ctx context.Context, principalID, capability string) (WorkerSession, error) {
	return authenticatePostgresWorkerCapability(ctx, s.db, principalID, capability)
}
func (s *PostgresStore) RetireWorkerCapability(ctx context.Context, principalID, capability string) (WorkerSession, error) {
	return retirePostgresWorkerCapability(ctx, s.db, principalID, capability)
}
func (s *PostgresStore) RetireWorkerSession(ctx context.Context, principalID, bootID, sessionID string) (WorkerSession, error) {
	return retirePostgresWorkerSession(ctx, s.db, principalID, bootID, sessionID)
}
