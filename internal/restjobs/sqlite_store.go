package restjobs

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const sqliteBusyTimeout = 5 * time.Second

// SQLiteStore persists REST jobs across process restarts. Interrupted running
// jobs remain running and require operator reconciliation; they are never
// blindly re-queued because external effects may already have happened.
type SQLiteStore struct {
	db        *sql.DB
	config    Config
	mu        sync.Mutex
	closed    bool
	dbClosed  bool
	closeCh   chan struct{}
	closeDone chan struct{}
	closeOnce sync.Once
}

var _ Store = (*SQLiteStore)(nil)

// OpenSQLiteStore opens a file-backed SQLite database and applies embedded,
// forward-only migrations. The database parent directory must already exist.
func OpenSQLiteStore(path string, config Config) (*SQLiteStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("SQLite database path is empty")
	}
	manager, err := NewManager(config)
	if err != nil {
		return nil, err
	}
	dsn, err := sqliteDSN(path)
	if err != nil {
		return nil, err
	}
	if path != ":memory:" {
		if err := ensurePrivateSQLiteFile(path); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, errors.New("open SQLite job store")
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &SQLiteStore{db: db, config: manager.config, closeCh: make(chan struct{}), closeDone: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, errors.New("SQLite job store is unavailable")
	}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := ensureProviderColumn(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func ensureProviderColumn(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS factory_job_provider_outcomes (
		job_id TEXT PRIMARY KEY NOT NULL REFERENCES factory_jobs(id),
		provider_json TEXT NOT NULL,
		updated_at TEXT NOT NULL
	)`)
	if err != nil {
		return errors.New("initialize SQLite provider outcome schema")
	}
	return nil
}

func sqliteDSN(path string) (string, error) {
	if path == ":memory:" {
		return "file:factory-memory?mode=memory&cache=shared&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)", nil
	}
	if !filepath.IsAbs(path) {
		return "", errors.New("SQLite database path must be absolute")
	}
	info, err := os.Stat(filepath.Dir(path))
	if err != nil || !info.IsDir() {
		return "", errors.New("SQLite database parent directory is unavailable")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", errors.New("SQLite database parent directory must be private")
	}
	if existing, err := os.Lstat(path); err == nil {
		if !existing.Mode().IsRegular() || existing.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("SQLite database path must be a regular file")
		}
		if existing.Mode().Perm()&0o077 != 0 {
			return "", errors.New("SQLite database file must be private")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", errors.New("SQLite database path is unavailable")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", errors.New("resolve SQLite database path")
	}
	return "file:" + url.PathEscape(abs) + "?mode=rwc&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)", nil
}

func ensurePrivateSQLiteFile(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err == nil {
		if closeErr := file.Close(); closeErr != nil {
			return errors.New("close initialized SQLite database file")
		}
		return nil
	}
	if !errors.Is(err, os.ErrExist) {
		return errors.New("create private SQLite database file")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return errors.New("SQLite database file must be a private regular file")
	}
	return nil
}

func (s *SQLiteStore) migrate(ctx context.Context) error {
	files, err := MigrationNames()
	if err != nil || len(files) == 0 {
		return errors.New("SQLite job store migrations are unavailable")
	}
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS factory_schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)`); err != nil {
		return errors.New("initialize SQLite migration ledger")
	}
	var current int
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM factory_schema_migrations`).Scan(&current); err != nil {
		return errors.New("read SQLite migration version")
	}
	if current > len(files) {
		return errors.New("SQLite database schema version is newer than this binary")
	}
	for i, name := range files {
		version := i + 1
		if version <= current {
			var applied string
			if err := s.db.QueryRowContext(ctx, `SELECT name FROM factory_schema_migrations WHERE version=?`, version).Scan(&applied); err != nil || applied != filepath.Base(name) {
				return errors.New("SQLite migration ledger does not match embedded migrations")
			}
			continue
		}
		payload, err := migrationFiles.ReadFile(name)
		if err != nil {
			return errors.New("read SQLite migration")
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return errors.New("begin SQLite migration")
		}
		if _, err := tx.ExecContext(ctx, string(payload)); err != nil {
			_ = tx.Rollback()
			return errors.New("apply SQLite job store migration")
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO factory_schema_migrations(version,name,applied_at) VALUES(?,?,?)`, version, filepath.Base(name), time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			_ = tx.Rollback()
			return errors.New("record SQLite migration")
		}
		if err := tx.Commit(); err != nil {
			return errors.New("commit SQLite migration")
		}
	}
	return nil
}

func (s *SQLiteStore) Admit(key string, request Request) (Snapshot, bool, error) {
	key, normalized, err := normalizeAdmission(key, request, s.config.MaxTaskBytes)
	if err != nil {
		return Snapshot{}, false, err
	}
	if err := s.checkAccepting(); err != nil {
		return Snapshot{}, false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), sqliteBusyTimeout)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, false, errors.New("SQLite job store unavailable")
	}
	defer tx.Rollback()
	var id, raw string
	err = tx.QueryRowContext(ctx, `SELECT j.id,j.request_json FROM factory_job_idempotency i JOIN factory_jobs j ON j.id=i.job_id WHERE i.key=?`, key).Scan(&id, &raw)
	if err == nil {
		var existing Request
		if json.Unmarshal([]byte(raw), &existing) != nil {
			return Snapshot{}, false, errors.New("SQLite job record is invalid")
		}
		if !sameRequest(existing, normalized) {
			return Snapshot{}, false, ErrIdempotencyConflict
		}
		snapshot, err := scanSnapshot(tx.QueryRowContext(ctx, `SELECT j.id,j.request_json,j.status,j.created_at,j.updated_at,j.history_truncated,(SELECT provider_json FROM factory_job_provider_outcomes WHERE job_id=j.id) FROM factory_jobs j WHERE j.id=?`, id))
		if err != nil {
			return Snapshot{}, false, errors.New("read SQLite job record")
		}
		if err := tx.Commit(); err != nil {
			return Snapshot{}, false, errors.New("finish SQLite idempotency lookup")
		}
		return snapshot, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, false, errors.New("read SQLite idempotency record")
	}
	var retained int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM factory_jobs`).Scan(&retained); err != nil {
		return Snapshot{}, false, errors.New("check SQLite retained-record capacity")
	}
	if retained >= s.config.MaxRecords {
		return Snapshot{}, false, ErrRegistryFull
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM factory_jobs WHERE status IN ('queued','running')`).Scan(&count); err != nil {
		return Snapshot{}, false, errors.New("check SQLite job capacity")
	}
	if count >= s.config.QueueCapacity+s.config.MaxConcurrentJobs {
		return Snapshot{}, false, ErrQueueFull
	}
	id, err = newJobID()
	if err != nil {
		return Snapshot{}, false, errors.New("create SQLite job identifier")
	}
	var sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(queue_sequence),0)+1 FROM factory_jobs`).Scan(&sequence); err != nil {
		return Snapshot{}, false, errors.New("allocate SQLite queue position")
	}
	now := time.Now().UTC()
	requestJSON, err := json.Marshal(normalized)
	if err != nil {
		return Snapshot{}, false, ErrInvalidInput
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_jobs(id,request_json,status,created_at,updated_at,queue_sequence,history_truncated) VALUES(?,?,?,?,?,?,0)`, id, string(requestJSON), StatusQueued, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), sequence); err != nil {
		return Snapshot{}, false, errors.New("persist SQLite job")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_job_idempotency(key,job_id,request_json) VALUES(?,?,?)`, key, id, string(requestJSON)); err != nil {
		return Snapshot{}, false, errors.New("persist SQLite idempotency key")
	}
	if err := insertEvent(ctx, tx, id, "queued", "Job admitted", now, s.config.MaxEventsPerJob); err != nil {
		return Snapshot{}, false, errors.New("persist SQLite admission history")
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, false, errors.New("commit SQLite job admission")
	}
	return Snapshot{ID: id, Request: normalized, Status: StatusQueued, CreatedAt: now, UpdatedAt: now}, false, nil
}

func (s *SQLiteStore) ClaimNext() (Snapshot, error) {
	if err := s.checkAccepting(); err != nil {
		return Snapshot{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), sqliteBusyTimeout)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, errors.New("SQLite job store unavailable")
	}
	defer tx.Rollback()
	var running int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM factory_jobs WHERE status='running'`).Scan(&running); err != nil {
		return Snapshot{}, errors.New("read SQLite worker capacity")
	}
	if running >= s.config.MaxConcurrentJobs {
		return Snapshot{}, ErrNoWorkerSlots
	}
	var id string
	err = tx.QueryRowContext(ctx, `SELECT id FROM factory_jobs WHERE status='queued' ORDER BY queue_sequence LIMIT 1`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, ErrNoQueuedJobs
	}
	if err != nil {
		return Snapshot{}, errors.New("read SQLite queue")
	}
	now := time.Now().UTC()
	result, err := tx.ExecContext(ctx, `UPDATE factory_jobs SET status='running',updated_at=? WHERE id=? AND status='queued'`, now.Format(time.RFC3339Nano), id)
	if err != nil {
		return Snapshot{}, errors.New("claim SQLite job")
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return Snapshot{}, ErrNoWorkerSlots
	}
	if err := insertEvent(ctx, tx, id, "running", "Job started", now, s.config.MaxEventsPerJob); err != nil {
		return Snapshot{}, errors.New("record SQLite job start")
	}
	snapshot, err := scanSnapshot(tx.QueryRowContext(ctx, `SELECT j.id,j.request_json,j.status,j.created_at,j.updated_at,j.history_truncated,(SELECT provider_json FROM factory_job_provider_outcomes WHERE job_id=j.id) FROM factory_jobs j WHERE j.id=?`, id))
	if err != nil {
		return Snapshot{}, errors.New("read claimed SQLite job")
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, errors.New("commit SQLite job claim")
	}
	return snapshot, nil
}

func (s *SQLiteStore) WaitClaim(ctx context.Context) (Snapshot, error) {
	if ctx == nil {
		return Snapshot{}, ErrInvalidInput
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return Snapshot{}, err
		}
		snapshot, err := s.ClaimNext()
		if err == nil {
			return snapshot, nil
		}
		if !errors.Is(err, ErrNoQueuedJobs) && !errors.Is(err, ErrNoWorkerSlots) {
			return Snapshot{}, err
		}
		select {
		case <-ctx.Done():
			return Snapshot{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *SQLiteStore) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		close(s.closeCh)
		s.mu.Unlock()
		defer close(s.closeDone)
		go func() {
			select {
			case <-s.closeDone:
			case <-time.After(sqliteBusyTimeout):
				_ = s.db.Close()
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), sqliteBusyTimeout)
		defer cancel()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return
		}
		rows, err := tx.QueryContext(ctx, `SELECT id FROM factory_jobs WHERE status='queued' ORDER BY queue_sequence`)
		if err != nil {
			_ = tx.Rollback()
			return
		}
		var queued []string
		for rows.Next() {
			var id string
			if rows.Scan(&id) != nil {
				_ = rows.Close()
				_ = tx.Rollback()
				return
			}
			queued = append(queued, id)
		}
		if rows.Err() != nil {
			_ = rows.Close()
			_ = tx.Rollback()
			return
		}
		_ = rows.Close()
		for _, id := range queued {
			now := time.Now().UTC()
			if _, err := tx.ExecContext(ctx, `UPDATE factory_jobs SET status='canceled', updated_at=? WHERE id=? AND status='queued'`, now.Format(time.RFC3339Nano), id); err != nil {
				_ = tx.Rollback()
				return
			}
			if err := insertEvent(ctx, tx, id, string(StatusCanceled), "Manager closed before job started", now, s.config.MaxEventsPerJob); err != nil {
				_ = tx.Rollback()
				return
			}
		}
		_ = tx.Commit()
	})
	<-s.closeDone
}

func (s *SQLiteStore) CloseStore() error {
	s.Close()
	s.mu.Lock()
	if s.dbClosed {
		s.mu.Unlock()
		return nil
	}
	s.dbClosed = true
	s.mu.Unlock()
	return s.db.Close()
}

func (s *SQLiteStore) RecordProviderOutcome(id string, outcome ProviderOutcome) error {
	if err := validateProviderOutcome(outcome); err != nil {
		return ErrInvalidInput
	}
	if err := s.checkAccepting(); err != nil {
		return err
	}
	encoded, err := json.Marshal(outcome)
	if err != nil {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(context.Background(), sqliteBusyTimeout)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("SQLite job store unavailable")
	}
	defer tx.Rollback()
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM factory_jobs WHERE id=?`, id).Scan(&status); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return errors.New("read SQLite job state")
	}
	if status != string(StatusRunning) {
		return ErrInvalidTransition
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_job_provider_outcomes(job_id,provider_json,updated_at) VALUES(?,?,?) ON CONFLICT(job_id) DO UPDATE SET provider_json=excluded.provider_json,updated_at=excluded.updated_at`, id, string(encoded), now); err != nil {
		return errors.New("record SQLite provider outcome")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE factory_jobs SET updated_at=? WHERE id=? AND status='running'`, now, id); err != nil {
		return errors.New("update SQLite job timestamp")
	}
	return tx.Commit()
}

func (s *SQLiteStore) Get(id string) (Snapshot, error) {
	if err := s.checkOpen(); err != nil {
		return Snapshot{}, err
	}
	snapshot, err := scanSnapshot(s.db.QueryRow(`SELECT j.id,j.request_json,j.status,j.created_at,j.updated_at,j.history_truncated,(SELECT provider_json FROM factory_job_provider_outcomes WHERE job_id=j.id) FROM factory_jobs j WHERE j.id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, ErrNotFound
	}
	if err != nil {
		return Snapshot{}, errors.New("read SQLite job")
	}
	return snapshot, nil
}

func (s *SQLiteStore) History(id string) (History, error) {
	if _, err := s.Get(id); err != nil {
		return History{}, err
	}
	rows, err := s.db.Query(`SELECT at,type,message FROM factory_job_events WHERE job_id=? ORDER BY sequence`, id)
	if err != nil {
		return History{}, errors.New("read SQLite job history")
	}
	defer rows.Close()
	history := History{JobID: id}
	for rows.Next() {
		var at, typ, message string
		if err := rows.Scan(&at, &typ, &message); err != nil {
			return History{}, errors.New("decode SQLite job history")
		}
		stamp, err := time.Parse(time.RFC3339Nano, at)
		if err != nil {
			return History{}, errors.New("decode SQLite history timestamp")
		}
		history.Events = append(history.Events, Event{At: stamp, Type: typ, Message: message})
	}
	if err := rows.Err(); err != nil {
		return History{}, errors.New("read SQLite job history")
	}
	var truncated int
	if err := s.db.QueryRow(`SELECT history_truncated FROM factory_jobs WHERE id=?`, id).Scan(&truncated); err != nil {
		return History{}, errors.New("read SQLite history marker")
	}
	history.Truncated = truncated != 0
	return history, nil
}

func (s *SQLiteStore) AddEvent(id, eventType, message string) error {
	eventType, message = strings.TrimSpace(eventType), strings.TrimSpace(message)
	if !validText(eventType, 1, 64, false) || !validText(message, 0, maxEventBytes, true) {
		return ErrInvalidInput
	}
	if err := s.checkAccepting(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), sqliteBusyTimeout)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("SQLite job store unavailable")
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE factory_jobs SET updated_at=? WHERE id=?`, time.Now().UTC().Format(time.RFC3339Nano), id)
	if err != nil {
		return errors.New("update SQLite job")
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		return ErrNotFound
	}
	if err := insertEvent(ctx, tx, id, eventType, message, time.Now().UTC(), s.config.MaxEventsPerJob); err != nil {
		return errors.New("record SQLite job event")
	}
	if err := tx.Commit(); err != nil {
		return errors.New("commit SQLite job event")
	}
	return nil
}

func (s *SQLiteStore) Finish(id string, status Status) error {
	if !status.Terminal() {
		return ErrInvalidTransition
	}
	if err := s.checkAccepting(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), sqliteBusyTimeout)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("SQLite job store unavailable")
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	result, err := tx.ExecContext(ctx, `UPDATE factory_jobs SET status=?,updated_at=? WHERE id=? AND status='running'`, status, now.Format(time.RFC3339Nano), id)
	if err != nil {
		return errors.New("finish SQLite job")
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM factory_jobs WHERE id=?`, id).Scan(&exists); err != nil || exists == 0 {
			return ErrNotFound
		}
		return ErrInvalidTransition
	}
	if err := insertEvent(ctx, tx, id, string(status), "Job finished", now, s.config.MaxEventsPerJob); err != nil {
		return errors.New("record SQLite job completion")
	}
	if err := tx.Commit(); err != nil {
		return errors.New("commit SQLite job completion")
	}
	return nil
}

func (s *SQLiteStore) Recover(ctx context.Context) (RecoveryReport, error) {
	if ctx == nil {
		return RecoveryReport{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return RecoveryReport{}, err
	}
	if err := s.checkOpen(); err != nil {
		return RecoveryReport{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT j.id,j.request_json,j.status,j.created_at,j.updated_at,j.history_truncated,(SELECT provider_json FROM factory_job_provider_outcomes WHERE job_id=j.id) FROM factory_jobs j ORDER BY j.queue_sequence`)
	if err != nil {
		return RecoveryReport{}, errors.New("recover SQLite jobs")
	}
	defer rows.Close()
	var report RecoveryReport
	for rows.Next() {
		snapshot, err := scanSnapshot(rows)
		if err != nil {
			return RecoveryReport{}, errors.New("decode SQLite recovery record")
		}
		switch RecoveryDispositionFor(snapshot.Status) {
		case RecoveryResume:
			report.Resume = append(report.Resume, snapshot)
		case RecoveryNeedsOperator:
			report.NeedsOperator = append(report.NeedsOperator, snapshot)
		case RecoveryRetainTerminal:
			report.Terminal = append(report.Terminal, snapshot)
		}
	}
	if err := rows.Err(); err != nil {
		return RecoveryReport{}, errors.New("recover SQLite jobs")
	}
	return report, nil
}

func (s *SQLiteStore) Ping(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidInput
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := s.db.PingContext(ctx); err != nil {
		return errors.New("SQLite job store is unavailable")
	}
	return nil
}

func (s *SQLiteStore) checkOpen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dbClosed {
		return ErrManagerClosed
	}
	return nil
}

func (s *SQLiteStore) checkAccepting() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.dbClosed {
		return ErrManagerClosed
	}
	return nil
}

func insertEvent(ctx context.Context, tx *sql.Tx, id, eventType, message string, at time.Time, limit int) error {
	var sequence int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM factory_job_events WHERE job_id=?`, id).Scan(&sequence); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_job_events(job_id,sequence,at,type,message) VALUES(?,?,?,?,?)`, id, sequence, at.UTC().Format(time.RFC3339Nano), eventType, message); err != nil {
		return err
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM factory_job_events WHERE job_id=?`, id).Scan(&count); err != nil {
		return err
	}
	if count > limit {
		if _, err := tx.ExecContext(ctx, `DELETE FROM factory_job_events WHERE job_id=? AND sequence IN (SELECT sequence FROM factory_job_events WHERE job_id=? ORDER BY sequence LIMIT ?)`, id, id, count-limit); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE factory_jobs SET history_truncated=1 WHERE id=?`, id); err != nil {
			return err
		}
	}
	return nil
}

func scanSnapshot(row interface{ Scan(...any) error }) (Snapshot, error) {
	var snapshot Snapshot
	var raw, status, created, updated string
	var truncated int
	var providerJSON sql.NullString
	if err := row.Scan(&snapshot.ID, &raw, &status, &created, &updated, &truncated, &providerJSON); err != nil {
		return Snapshot{}, err
	}
	if err := json.Unmarshal([]byte(raw), &snapshot.Request); err != nil {
		return Snapshot{}, err
	}
	snapshot.Status = Status(status)
	var err error
	if snapshot.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return Snapshot{}, err
	}
	if snapshot.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated); err != nil {
		return Snapshot{}, err
	}
	if snapshot.Request.Issue != nil {
		issue := *snapshot.Request.Issue
		snapshot.Request.Issue = &issue
	}
	if providerJSON.Valid {
		var outcome ProviderOutcome
		if err := json.Unmarshal([]byte(providerJSON.String), &outcome); err != nil || validateProviderOutcome(outcome) != nil {
			return Snapshot{}, errors.New("invalid persisted provider outcome")
		}
		snapshot.Provider = &outcome
	}
	_ = truncated
	return snapshot, nil
}

func MigrationNames() ([]string, error) {
	files, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return nil, errors.New("read SQLite migration list")
	}
	sort.Strings(files)
	return files, nil
}

func MigrationChecksum() (string, error) {
	files, err := MigrationNames()
	if err != nil {
		return "", err
	}
	h := sha256.New()
	for _, file := range files {
		payload, err := migrationFiles.ReadFile(file)
		if err != nil {
			return "", errors.New("read SQLite migration payload")
		}
		_, _ = h.Write([]byte(filepath.Base(file)))
		_, _ = h.Write(payload)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
