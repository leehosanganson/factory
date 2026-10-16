package restjobs

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

//go:embed postgres_migrations/*.sql
var postgresMigrationFiles embed.FS

const postgresStoreMigrationLock int64 = 0x464143544F5253 // "FACTORS"

// PostgresStore is the canonical durable REST job store. It shares one job
// status authority for admission, lifecycle, history, and worker fencing.
type PostgresStore struct {
	db                  *sql.DB
	config              Config
	workerID            string
	bootID              string
	mu                  sync.Mutex
	admitMu             sync.Mutex
	recoveryMu          sync.RWMutex
	recoveryNeeded      map[string]struct{}
	closed              bool
	dbClosed            bool
	cancelQueuedOnClose bool
	closeOnce           sync.Once
}

var _ Store = (*PostgresStore)(nil)

// OpenPostgresStore opens a PostgreSQL connection pool and applies migrations.
func OpenPostgresStore(dsn string, config Config) (*PostgresStore, error) {
	return openPostgresStore(dsn, config, "", "", true, true)
}

// OpenPostgresServerStore preserves queued work on shutdown and recognizes
// only expired leases as interrupted; active peer workers remain eligible.
func OpenPostgresServerStore(dsn string, config Config) (*PostgresStore, error) {
	return openPostgresStore(dsn, config, "", "", true, false)
}

// OpenPostgresWorkerStore opens the shared store with observable claim identity.
func OpenPostgresWorkerStore(dsn string, config Config, workerID, bootID string) (*PostgresStore, error) {
	return openPostgresStore(dsn, config, workerID, bootID, true, false)
}

func openPostgresStore(dsn string, config Config, workerID, bootID string, recover, cancelQueued bool) (*PostgresStore, error) {
	if (workerID == "") != (bootID == "") || (workerID != "" && (!validPostgresOwnerID(workerID) || !validPostgresOwnerID(bootID))) {
		return nil, ErrInvalidInput
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid PostgreSQL job store connection")
	}
	store, err := newPostgresStore(stdlib.OpenDB(*parsed), config, recover)
	if err != nil {
		return nil, err
	}
	// Shared server and worker processes leave queued records in place on close.
	store.cancelQueuedOnClose = cancelQueued
	store.workerID, store.bootID = workerID, bootID
	return store, nil
}

// NewPostgresStore initializes an existing PostgreSQL pool. The store owns and
// closes db after Close; callers must not use it independently afterward.
func NewPostgresStore(db *sql.DB, config Config) (*PostgresStore, error) {
	return newPostgresStore(db, config, true)
}

func newPostgresStore(db *sql.DB, config Config, recover bool) (*PostgresStore, error) {
	if db == nil {
		return nil, ErrInvalidInput
	}
	manager, err := NewManager(config)
	if err != nil {
		return nil, err
	}
	s := &PostgresStore{db: db, config: manager.config, recoveryNeeded: make(map[string]struct{}), cancelQueuedOnClose: true}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, errors.New("PostgreSQL job store is unavailable")
	}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if !recover {
		return s, nil
	}
	rows, err := db.QueryContext(ctx, `SELECT j.id FROM factory_jobs j LEFT JOIN factory_rest_worker_ownership o ON o.job_id=j.id WHERE j.status='running' AND (o.job_id IS NULL OR o.lease_until<=clock_timestamp())`)
	if err != nil {
		_ = db.Close()
		return nil, errors.New("read PostgreSQL recovery-needed jobs")
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = db.Close()
			return nil, errors.New("read PostgreSQL recovery-needed jobs")
		}
		s.recoveryNeeded[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		_ = db.Close()
		return nil, errors.New("read PostgreSQL recovery-needed jobs")
	}
	return s, nil
}

func (s *PostgresStore) migrate(ctx context.Context) error {
	files, err := postgresMigrationNames()
	if err != nil || len(files) == 0 {
		return errors.New("PostgreSQL job store migrations are unavailable")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("begin PostgreSQL job store migration")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, postgresStoreMigrationLock); err != nil {
		return errors.New("lock PostgreSQL job store migration")
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS factory_rest_schema_migrations (version integer PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		return errors.New("initialize PostgreSQL migration ledger")
	}
	var current int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM factory_rest_schema_migrations`).Scan(&current); err != nil {
		return errors.New("read PostgreSQL migration version")
	}
	if current > len(files) {
		return errors.New("PostgreSQL database schema is newer than this binary")
	}
	for index, name := range files {
		version := index + 1
		if version <= current {
			var applied string
			if err := tx.QueryRowContext(ctx, `SELECT name FROM factory_rest_schema_migrations WHERE version=$1`, version).Scan(&applied); err != nil || applied != name {
				return errors.New("PostgreSQL migration ledger does not match embedded migrations")
			}
			continue
		}
		payload, err := postgresMigrationFiles.ReadFile("postgres_migrations/" + name)
		if err != nil {
			return errors.New("read PostgreSQL migration")
		}
		if _, err := tx.ExecContext(ctx, string(payload)); err != nil {
			return errors.New("apply PostgreSQL job store migration")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO factory_rest_schema_migrations(version,name) VALUES($1,$2)`, version, name); err != nil {
			return errors.New("record PostgreSQL migration")
		}
	}
	if err := tx.Commit(); err != nil {
		return errors.New("commit PostgreSQL job store migration")
	}
	return nil
}

func postgresMigrationNames() ([]string, error) {
	entries, err := fs.Glob(postgresMigrationFiles, "postgres_migrations/*.sql")
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		names = append(names, strings.TrimPrefix(entry, "postgres_migrations/"))
	}
	sort.Strings(names)
	return names, nil
}

func (s *PostgresStore) SetWorkerIdentity(workerID, bootID string) error {
	if !validPostgresOwnerID(workerID) || !validPostgresOwnerID(bootID) {
		return ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.dbClosed {
		return ErrManagerClosed
	}
	s.workerID, s.bootID = workerID, bootID
	return nil
}

func (s *PostgresStore) checkOpen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dbClosed {
		return ErrManagerClosed
	}
	return nil
}

func (s *PostgresStore) checkAccepting() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.dbClosed {
		return ErrManagerClosed
	}
	return nil
}

func pgSnapshotQuery() string {
	return `SELECT j.id,j.request_json,j.status,j.created_at,j.updated_at,j.history_truncated,j.cancellation_requested,j.submitter,
	(SELECT provider_json FROM factory_job_provider_outcomes WHERE job_id=j.id),
	(SELECT evidence_json FROM factory_job_verification_evidence WHERE job_id=j.id),
	(SELECT json_build_object('worker_id', worker_id, 'boot_id', boot_id)::text FROM factory_rest_worker_ownership WHERE job_id=j.id),
	(SELECT json_build_object('stage',stage,'state',state)::text FROM factory_job_progress WHERE job_id=j.id)
	FROM factory_jobs j `
}

func scanPostgresSnapshot(row interface{ Scan(...any) error }) (Snapshot, error) {
	var snap Snapshot
	var raw, status string
	var truncated, canceled bool
	var providerJSON, evidenceJSON, workerJSON, progressMessage sql.NullString
	if err := row.Scan(&snap.ID, &raw, &status, &snap.CreatedAt, &snap.UpdatedAt, &truncated, &canceled, &snap.Submitter, &providerJSON, &evidenceJSON, &workerJSON, &progressMessage); err != nil {
		return Snapshot{}, err
	}
	if err := json.Unmarshal([]byte(raw), &snap.Request); err != nil {
		return Snapshot{}, err
	}
	snap.Status = Status(status)
	snap.CancellationRequested = canceled
	if providerJSON.Valid {
		var outcome ProviderOutcome
		if json.Unmarshal([]byte(providerJSON.String), &outcome) != nil || validateProviderOutcome(outcome) != nil {
			return Snapshot{}, errors.New("invalid persisted provider outcome")
		}
		snap.Provider = &outcome
	}
	if evidenceJSON.Valid {
		var evidence VerificationEvidence
		if json.Unmarshal([]byte(evidenceJSON.String), &evidence) != nil || validateVerificationEvidence(evidence) != nil {
			return Snapshot{}, errors.New("invalid persisted verification evidence")
		}
		snap.Verification = &evidence
	}
	if workerJSON.Valid {
		var worker WorkerSummary
		if json.Unmarshal([]byte(workerJSON.String), &worker) != nil || !validPostgresOwnerID(worker.WorkerID) || !validPostgresOwnerID(worker.BootID) {
			return Snapshot{}, errors.New("invalid persisted worker identity")
		}
		snap.Worker = &worker
	}
	if progressMessage.Valid {
		var progress ProgressSummary
		if json.Unmarshal([]byte(progressMessage.String), &progress) != nil || !validProgress(progress) {
			return Snapshot{}, errors.New("invalid persisted progress")
		}
		snap.Progress = &progress
	}
	return cloneSnapshot(snap), nil
}

func (s *PostgresStore) Admit(key string, request Request) (Snapshot, bool, error) {
	return s.AdmitAs(key, legacySharedPrincipalID, request)
}

func (s *PostgresStore) AdmitAs(key, principalID string, request Request) (Snapshot, bool, error) {
	return s.admitAsContext(context.Background(), key, principalID, request)
}

func (s *PostgresStore) admitAsContext(parent context.Context, key, principalID string, request Request) (Snapshot, bool, error) {
	s.admitMu.Lock()
	defer s.admitMu.Unlock()
	key, request, err := normalizeAdmission(key, request, s.config.MaxTaskBytes)
	if err != nil {
		return Snapshot{}, false, err
	}
	principalID, err = normalizePrincipalID(principalID)
	if err != nil {
		return Snapshot{}, false, err
	}
	if err := s.checkAccepting(); err != nil {
		return Snapshot{}, false, err
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, false, errors.New("PostgreSQL job store unavailable")
	}
	defer tx.Rollback()
	// Serialize admission checks across independent control-plane connections.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, postgresStoreMigrationLock+1); err != nil {
		return Snapshot{}, false, errors.New("lock PostgreSQL admission")
	}
	var existingID, raw, existingPrincipal string
	if request.IssueProvenance != nil {
		p := request.IssueProvenance
		err := tx.QueryRowContext(ctx, `SELECT j.id,j.request_json,j.submitter FROM factory_job_issue_provenance p JOIN factory_jobs j ON j.id=p.job_id WHERE p.tracker=$1 AND p.source_repository=$2 AND p.issue_number=$3 AND p.version=$4`, p.Tracker, p.SourceRepository, p.IssueNumber, p.Version).Scan(&existingID, &raw, &existingPrincipal)
		if err == nil && existingPrincipal != principalID {
			return Snapshot{}, false, ErrProvenanceConflict
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return Snapshot{}, false, errors.New("read PostgreSQL issue provenance")
		}
	}
	err = tx.QueryRowContext(ctx, `SELECT j.id,j.request_json FROM factory_job_idempotency i JOIN factory_jobs j ON j.id=i.job_id WHERE i.principal_id=$1 AND i.key=$2`, principalID, key).Scan(&existingID, &raw)
	if err == nil {
		var prior Request
		if json.Unmarshal([]byte(raw), &prior) != nil {
			return Snapshot{}, false, errors.New("PostgreSQL job record is invalid")
		}
		if !sameRequest(prior, request) {
			return Snapshot{}, false, ErrIdempotencyConflict
		}
		snap, err := scanPostgresSnapshot(tx.QueryRowContext(ctx, pgSnapshotQuery()+`WHERE j.id=$1`, existingID))
		if err != nil {
			return Snapshot{}, false, errors.New("read PostgreSQL job record")
		}
		if err = tx.Commit(); err != nil {
			return Snapshot{}, false, errors.New("finish PostgreSQL idempotency lookup")
		}
		return snap, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, false, errors.New("read PostgreSQL idempotency record")
	}
	if request.IssueProvenance != nil {
		p := request.IssueProvenance
		err = tx.QueryRowContext(ctx, `SELECT j.id,j.request_json FROM factory_job_issue_provenance p JOIN factory_jobs j ON j.id=p.job_id WHERE p.tracker=$1 AND p.source_repository=$2 AND p.issue_number=$3 AND p.version=$4`, p.Tracker, p.SourceRepository, p.IssueNumber, p.Version).Scan(&existingID, &raw)
		if err == nil {
			var prior Request
			if json.Unmarshal([]byte(raw), &prior) != nil {
				return Snapshot{}, false, errors.New("PostgreSQL job record is invalid")
			}
			if !sameRequest(prior, request) {
				return Snapshot{}, false, ErrProvenanceConflict
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO factory_job_idempotency(principal_id,key,job_id,request_json) VALUES($1,$2,$3,$4)`, principalID, key, existingID, raw); err != nil {
				return Snapshot{}, false, errors.New("persist PostgreSQL idempotency key")
			}
			snap, e := scanPostgresSnapshot(tx.QueryRowContext(ctx, pgSnapshotQuery()+`WHERE j.id=$1`, existingID))
			if e != nil {
				return Snapshot{}, false, errors.New("read PostgreSQL job record")
			}
			if e = tx.Commit(); e != nil {
				return Snapshot{}, false, errors.New("finish PostgreSQL provenance lookup")
			}
			return snap, true, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return Snapshot{}, false, errors.New("read PostgreSQL issue provenance")
		}
	}
	var retained, active int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(*) FILTER (WHERE status IN ('queued','running')) FROM factory_jobs`).Scan(&retained, &active); err != nil {
		return Snapshot{}, false, errors.New("check PostgreSQL job capacity")
	}
	if retained >= s.config.MaxRecords {
		return Snapshot{}, false, ErrRegistryFull
	}
	if active >= s.config.QueueCapacity+s.config.MaxConcurrentJobs {
		return Snapshot{}, false, ErrQueueFull
	}
	id, err := newJobID()
	if err != nil {
		return Snapshot{}, false, errors.New("create PostgreSQL job identifier")
	}
	var sequence int64
	if err = tx.QueryRowContext(ctx, `SELECT nextval('factory_rest_job_sequence')`).Scan(&sequence); err != nil {
		return Snapshot{}, false, errors.New("allocate PostgreSQL queue position")
	}
	var requestJSON []byte
	requestJSON, err = json.Marshal(request)
	if err != nil {
		return Snapshot{}, false, ErrInvalidInput
	}
	var snap Snapshot
	err = tx.QueryRowContext(ctx, `INSERT INTO factory_jobs(id,request_json,status,created_at,updated_at,queue_sequence,submitter) VALUES($1,$2,'queued',clock_timestamp(),clock_timestamp(),$3,$4) RETURNING created_at,updated_at`, id, string(requestJSON), sequence, principalID).Scan(&snap.CreatedAt, &snap.UpdatedAt)
	if err != nil {
		return Snapshot{}, false, errors.New("persist PostgreSQL job")
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO factory_job_idempotency(principal_id,key,job_id,request_json) VALUES($1,$2,$3,$4)`, principalID, key, id, string(requestJSON)); err != nil {
		return Snapshot{}, false, errors.New("persist PostgreSQL idempotency key")
	}
	if request.IssueProvenance != nil {
		p := request.IssueProvenance
		if _, err = tx.ExecContext(ctx, `INSERT INTO factory_job_issue_provenance(tracker,source_repository,issue_number,version,job_id) VALUES($1,$2,$3,$4,$5)`, p.Tracker, p.SourceRepository, p.IssueNumber, p.Version, id); err != nil {
			return Snapshot{}, false, ErrProvenanceConflict
		}
	}
	if err = insertPostgresEvent(ctx, tx, id, "queued", "Job admitted", s.config.MaxEventsPerJob); err != nil {
		return Snapshot{}, false, errors.New("persist PostgreSQL admission history")
	}
	if err = tx.Commit(); err != nil {
		return Snapshot{}, false, errors.New("commit PostgreSQL job admission")
	}
	snap.ID = id
	snap.Submitter = principalID
	snap.Request = request
	snap.Status = StatusQueued
	return snap, false, nil
}

func (s *PostgresStore) claimNext(attemptAware bool) (AttemptClaim, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.claimNextContext(ctx, attemptAware)
}

func (s *PostgresStore) claimNextContext(ctx context.Context, attemptAware bool) (AttemptClaim, error) {
	if ctx == nil {
		return AttemptClaim{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return AttemptClaim{}, err
	}
	if err := s.checkAccepting(); err != nil {
		return AttemptClaim{}, err
	}
	var token string
	var err error
	if attemptAware {
		token, err = newOwnerToken()
		if err != nil {
			return AttemptClaim{}, err
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AttemptClaim{}, postgresClaimContextError(ctx, "PostgreSQL job store unavailable")
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, postgresStoreMigrationLock+2); err != nil {
		return AttemptClaim{}, postgresClaimContextError(ctx, "lock PostgreSQL worker capacity")
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM factory_jobs WHERE status='running'`)
	if err != nil {
		return AttemptClaim{}, postgresClaimContextError(ctx, "read PostgreSQL worker capacity")
	}
	s.recoveryMu.RLock()
	running := 0
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			s.recoveryMu.RUnlock()
			_ = rows.Close()
			return AttemptClaim{}, postgresClaimContextError(ctx, "read PostgreSQL worker capacity")
		}
		if _, recoveryNeeded := s.recoveryNeeded[id]; !recoveryNeeded {
			running++
		}
	}
	rowsErr := rows.Err()
	s.recoveryMu.RUnlock()
	_ = rows.Close()
	if rowsErr != nil {
		return AttemptClaim{}, postgresClaimContextError(ctx, "read PostgreSQL worker capacity")
	}
	if running >= s.config.MaxConcurrentJobs {
		return AttemptClaim{}, ErrNoWorkerSlots
	}
	var id string
	err = tx.QueryRowContext(ctx, `SELECT id FROM factory_jobs WHERE status='queued' ORDER BY queue_sequence LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return AttemptClaim{}, ErrNoQueuedJobs
	}
	if err != nil {
		return AttemptClaim{}, postgresClaimContextError(ctx, "read PostgreSQL queue")
	}
	var generation int64
	if attemptAware {
		if err = tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT generation FROM factory_rest_worker_ownership WHERE job_id=$1),0)+1`, id).Scan(&generation); err != nil {
			return AttemptClaim{}, postgresClaimContextError(ctx, "read PostgreSQL worker generation")
		}
		if generation <= 0 {
			return AttemptClaim{}, ErrInvalidTransition
		}
		workerID, bootID := pgIdentityOrDefault(s.workerID, "worker-local"), pgIdentityOrDefault(s.bootID, "boot-local")
		if _, err = tx.ExecContext(ctx, `INSERT INTO factory_rest_worker_ownership(job_id,generation,owner_token,worker_id,boot_id,lease_until) VALUES($1,$2,$3,$4,$5,clock_timestamp()+interval '1 minute') ON CONFLICT(job_id) DO UPDATE SET generation=EXCLUDED.generation,owner_token=EXCLUDED.owner_token,worker_id=EXCLUDED.worker_id,boot_id=EXCLUDED.boot_id,lease_until=EXCLUDED.lease_until`, id, generation, token, workerID, bootID); err != nil {
			return AttemptClaim{}, postgresClaimContextError(ctx, "record PostgreSQL worker ownership")
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO factory_rest_attempt_history(job_id,generation,worker_id,boot_id) VALUES($1,$2,$3,$4)`, id, generation, workerID, bootID); err != nil {
			return AttemptClaim{}, postgresClaimContextError(ctx, "record PostgreSQL attempt history")
		}
	}
	res, err := tx.ExecContext(ctx, `UPDATE factory_jobs SET status='running',updated_at=clock_timestamp() WHERE id=$1 AND status='queued'`, id)
	if err != nil {
		return AttemptClaim{}, postgresClaimContextError(ctx, "claim PostgreSQL job")
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return AttemptClaim{}, ErrNoWorkerSlots
	}
	if err = insertPostgresEvent(ctx, tx, id, "running", "Job started", s.config.MaxEventsPerJob); err != nil {
		return AttemptClaim{}, postgresClaimContextError(ctx, "record PostgreSQL job start")
	}
	snap, err := scanPostgresSnapshot(tx.QueryRowContext(ctx, pgSnapshotQuery()+`WHERE j.id=$1`, id))
	if err != nil {
		return AttemptClaim{}, postgresClaimContextError(ctx, "read claimed PostgreSQL job")
	}
	if err = ctx.Err(); err != nil {
		return AttemptClaim{}, err
	}
	if err = tx.Commit(); err != nil {
		return AttemptClaim{}, postgresClaimContextError(ctx, "commit PostgreSQL job claim")
	}
	claim := AttemptClaim{Job: snap}
	if attemptAware {
		claim.Attempt = AttemptHandle{JobID: id, Generation: generation, OwnerToken: token, WorkerID: pgIdentityOrDefault(s.workerID, "worker-local"), BootID: pgIdentityOrDefault(s.bootID, "boot-local")}
	}
	return claim, nil
}

func pgIdentityOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func postgresClaimContextError(ctx context.Context, message string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New(message)
}

func (s *PostgresStore) ClaimNext() (Snapshot, error) {
	claim, err := s.claimNext(false)
	return claim.Job, err
}
func (s *PostgresStore) ClaimNextAttempt() (AttemptClaim, error) { return s.claimNext(true) }
func (s *PostgresStore) WaitClaim(ctx context.Context) (Snapshot, error) {
	c, e := s.waitClaim(ctx, false)
	return c.Job, e
}
func (s *PostgresStore) WaitClaimAttempt(ctx context.Context) (AttemptClaim, error) {
	return s.waitClaim(ctx, true)
}
func (s *PostgresStore) waitClaim(ctx context.Context, attempt bool) (AttemptClaim, error) {
	if ctx == nil {
		return AttemptClaim{}, ErrInvalidInput
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return AttemptClaim{}, err
		}
		c, err := s.claimNextContext(ctx, attempt)
		if err == nil {
			return c, nil
		}
		if !errors.Is(err, ErrNoQueuedJobs) && !errors.Is(err, ErrNoWorkerSlots) {
			return AttemptClaim{}, err
		}
		select {
		case <-ctx.Done():
			return AttemptClaim{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *PostgresStore) Close() {
	s.closeOnce.Do(func() {
		s.admitMu.Lock()
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.admitMu.Unlock()
		if !s.cancelQueuedOnClose {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return
		}
		defer tx.Rollback()
		rows, err := tx.QueryContext(ctx, `SELECT id FROM factory_jobs WHERE status='queued' ORDER BY queue_sequence FOR UPDATE`)
		if err != nil {
			return
		}
		var ids []string
		for rows.Next() {
			var id string
			if rows.Scan(&id) != nil {
				_ = rows.Close()
				return
			}
			ids = append(ids, id)
		}
		if rows.Err() != nil {
			_ = rows.Close()
			return
		}
		_ = rows.Close()
		for _, id := range ids {
			if _, err := tx.ExecContext(ctx, `UPDATE factory_jobs SET status='canceled',updated_at=clock_timestamp() WHERE id=$1 AND status='queued'`, id); err != nil {
				return
			}
			if err := insertPostgresEvent(ctx, tx, id, "canceled", "Manager closed before job started", s.config.MaxEventsPerJob); err != nil {
				return
			}
		}
		_ = tx.Commit()
	})
}
func (s *PostgresStore) CloseStore() error {
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

func (s *PostgresStore) Get(id string) (Snapshot, error) {
	if err := s.checkOpen(); err != nil {
		return Snapshot{}, err
	}
	snap, err := scanPostgresSnapshot(s.db.QueryRow(pgSnapshotQuery()+`WHERE j.id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, ErrNotFound
	}
	if err != nil {
		return Snapshot{}, errors.New("read PostgreSQL job")
	}
	return snap, nil
}
func (s *PostgresStore) GetForPrincipal(id, principal string) (Snapshot, error) {
	principal, err := normalizePrincipalID(principal)
	if err != nil {
		return Snapshot{}, err
	}
	if err = s.checkOpen(); err != nil {
		return Snapshot{}, err
	}
	snap, err := scanPostgresSnapshot(s.db.QueryRow(pgSnapshotQuery()+`WHERE j.id=$1 AND j.submitter=$2`, id, principal))
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, ErrNotFound
	}
	if err != nil {
		return Snapshot{}, errors.New("read PostgreSQL job")
	}
	return snap, nil
}

func (s *PostgresStore) ListJobs(ctx context.Context, after, snapshot uint64, limit int) (JobPage, error) {
	return s.listJobs(ctx, "", after, snapshot, limit)
}
func (s *PostgresStore) ListForPrincipal(ctx context.Context, principal string, after, snapshot uint64, limit int) (JobPage, error) {
	principal, err := normalizePrincipalID(principal)
	if err != nil {
		return JobPage{}, err
	}
	return s.listJobs(ctx, principal, after, snapshot, limit)
}
func (s *PostgresStore) listJobs(ctx context.Context, principal string, after, snapshot uint64, limit int) (JobPage, error) {
	if err := validateJobPageRequest(ctx, after, snapshot, limit); err != nil {
		return JobPage{}, err
	}
	if err := s.checkOpen(); err != nil {
		return JobPage{}, err
	}
	if snapshot == 0 {
		if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(queue_sequence),0) FROM factory_jobs`).Scan(&snapshot); err != nil {
			return JobPage{}, errors.New("read PostgreSQL listing snapshot")
		}
	}
	q := `SELECT id,status,created_at,updated_at,queue_sequence FROM factory_jobs WHERE queue_sequence>$1 AND queue_sequence<=$2`
	args := []any{after, snapshot}
	if principal != "" {
		q += ` AND submitter=$3`
		args = append(args, principal)
	}
	q += fmt.Sprintf(` ORDER BY queue_sequence LIMIT $%d`, len(args)+1)
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return JobPage{}, errors.New("read PostgreSQL job listing")
	}
	defer rows.Close()
	page := JobPage{Jobs: make([]JobSummary, 0, limit), SnapshotSequence: snapshot}
	for rows.Next() {
		var summary JobSummary
		var status string
		var sequence uint64
		if err := rows.Scan(&summary.ID, &status, &summary.CreatedAt, &summary.UpdatedAt, &sequence); err != nil {
			return JobPage{}, errors.New("decode PostgreSQL job listing")
		}
		if len(page.Jobs) == limit {
			page.HasMore = true
			break
		}
		summary.Status = Status(status)
		page.Jobs = append(page.Jobs, summary)
		page.NextSequence = sequence
	}
	if err := rows.Err(); err != nil {
		return JobPage{}, errors.New("read PostgreSQL job listing")
	}
	if !page.HasMore {
		page.NextSequence = 0
	}
	return page, nil
}

func (s *PostgresStore) History(id string) (History, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, h, e := s.EventStreamState(ctx, id)
	return h, e
}
func (s *PostgresStore) EventStreamState(ctx context.Context, id string) (Snapshot, History, error) {
	if ctx == nil {
		return Snapshot{}, History{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, History{}, err
	}
	if err := s.checkOpen(); err != nil {
		return Snapshot{}, History{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Snapshot{}, History{}, errors.New("read PostgreSQL job history")
	}
	defer tx.Rollback()
	snap, err := scanPostgresSnapshot(tx.QueryRowContext(ctx, pgSnapshotQuery()+`WHERE j.id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, History{}, ErrNotFound
	}
	if err != nil {
		return Snapshot{}, History{}, errors.New("read PostgreSQL job record")
	}
	h := History{JobID: id}
	if err = tx.QueryRowContext(ctx, `SELECT event_sequence,history_truncated FROM factory_jobs WHERE id=$1`, id).Scan(&h.LatestSequence, &h.Truncated); err != nil {
		return Snapshot{}, History{}, errors.New("read PostgreSQL history metadata")
	}
	rows, err := tx.QueryContext(ctx, `SELECT sequence,at,type,message FROM factory_job_events WHERE job_id=$1 ORDER BY sequence`, id)
	if err != nil {
		return Snapshot{}, History{}, errors.New("read PostgreSQL job history")
	}
	for rows.Next() {
		var e Event
		if err = rows.Scan(&e.Sequence, &e.At, &e.Type, &e.Message); err != nil {
			_ = rows.Close()
			return Snapshot{}, History{}, errors.New("decode PostgreSQL history")
		}
		h.Events = append(h.Events, e)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return Snapshot{}, History{}, errors.New("read PostgreSQL job history")
	}
	_ = rows.Close()
	h.Worker = cloneWorker(snap.Worker)
	h.Progress = cloneProgress(snap.Progress)
	if err = ctx.Err(); err != nil {
		return Snapshot{}, History{}, err
	}
	if err = tx.Commit(); err != nil {
		return Snapshot{}, History{}, errors.New("finish PostgreSQL history read")
	}
	return snap, h, nil
}

func (s *PostgresStore) AddEvent(id, typ, msg string) error {
	typ, msg = strings.TrimSpace(typ), strings.TrimSpace(msg)
	if !validText(typ, 1, 64, false) || !validText(msg, 0, maxEventBytes, true) {
		return ErrInvalidInput
	}
	if err := s.checkAccepting(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("PostgreSQL job store unavailable")
	}
	defer tx.Rollback()
	if err = s.lockPostgresJob(ctx, tx, id); err != nil {
		return err
	}
	var owned bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM factory_rest_worker_ownership WHERE job_id=$1)`, id).Scan(&owned); err != nil {
		return errors.New("read PostgreSQL worker ownership")
	}
	if owned {
		return ErrStaleAttempt
	}
	res, err := tx.ExecContext(ctx, `UPDATE factory_jobs SET updated_at=clock_timestamp() WHERE id=$1`, id)
	if err != nil {
		return errors.New("update PostgreSQL job")
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrNotFound
	}
	if err = insertPostgresEvent(ctx, tx, id, typ, msg, s.config.MaxEventsPerJob); err != nil {
		return errors.New("record PostgreSQL job event")
	}
	return tx.Commit()
}
func (s *PostgresStore) AddEventForAttempt(a AttemptHandle, typ, msg string) error {
	typ, msg = strings.TrimSpace(typ), strings.TrimSpace(msg)
	if !validAttemptHandle(a) || !validText(typ, 1, 64, false) || !validText(msg, 0, maxEventBytes, true) {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.beginAttempt(ctx, a)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE factory_jobs SET updated_at=clock_timestamp() WHERE id=$1`, a.JobID); err != nil {
		return errors.New("update PostgreSQL job event")
	}
	if err = insertPostgresEvent(ctx, tx, a.JobID, typ, msg, s.config.MaxEventsPerJob); err != nil {
		return errors.New("record PostgreSQL job event")
	}
	return tx.Commit()
}

func (s *PostgresStore) SetAttemptIdentity(a AttemptHandle) error {
	if !validAttemptHandle(a) {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.beginAttempt(ctx, a)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `UPDATE factory_rest_worker_ownership SET worker_id=$4,boot_id=$5 WHERE job_id=$1 AND generation=$2 AND owner_token=$3`, a.JobID, a.Generation, a.OwnerToken, pgIdentityOrDefault(a.WorkerID, "worker-local"), pgIdentityOrDefault(a.BootID, "boot-local"))
	if err != nil {
		return errors.New("record PostgreSQL worker identity")
	}
	return tx.Commit()
}

func (s *PostgresStore) RecordProgressForAttempt(a AttemptHandle, progress ProgressSummary) error {
	if !validProgress(progress) {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.beginAttempt(ctx, a)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_job_progress(job_id,stage,state) VALUES($1,$2,$3) ON CONFLICT(job_id) DO UPDATE SET stage=EXCLUDED.stage,state=EXCLUDED.state,updated_at=clock_timestamp()`, a.JobID, progress.Stage, progress.State); err != nil {
		return errors.New("record PostgreSQL stage progress")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE factory_jobs SET updated_at=clock_timestamp() WHERE id=$1`, a.JobID); err != nil {
		return errors.New("update PostgreSQL progress timestamp")
	}
	return tx.Commit()
}

func (s *PostgresStore) HeartbeatAttempt(a AttemptHandle) error {
	if !validAttemptHandle(a) {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.beginAttempt(ctx, a)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE factory_rest_worker_ownership SET lease_until=clock_timestamp()+interval '1 minute' WHERE job_id=$1 AND generation=$2 AND owner_token=$3 AND worker_id=$4 AND boot_id=$5 AND lease_until>clock_timestamp()`, a.JobID, a.Generation, a.OwnerToken, pgIdentityOrDefault(a.WorkerID, "worker-local"), pgIdentityOrDefault(a.BootID, "boot-local"))
	if err != nil {
		return errors.New("renew PostgreSQL worker lease")
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrStaleAttempt
	}
	return tx.Commit()
}
func (s *PostgresStore) beginAttempt(ctx context.Context, a AttemptHandle) (*sql.Tx, error) {
	if !validAttemptHandle(a) {
		return nil, ErrInvalidInput
	}
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, errors.New("PostgreSQL job store unavailable")
	}
	var valid bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM factory_jobs j JOIN factory_rest_worker_ownership o ON o.job_id=j.id WHERE j.id=$1 AND j.status='running' AND o.generation=$2 AND o.owner_token=$3 AND o.worker_id=$4 AND o.boot_id=$5 AND o.lease_until>clock_timestamp())`, a.JobID, a.Generation, a.OwnerToken, pgIdentityOrDefault(a.WorkerID, "worker-local"), pgIdentityOrDefault(a.BootID, "boot-local")).Scan(&valid)
	if err != nil {
		_ = tx.Rollback()
		return nil, errors.New("read PostgreSQL worker ownership")
	}
	if !valid {
		var running, expired bool
		checkErr := tx.QueryRowContext(ctx, `SELECT j.status='running', (o.job_id IS NULL OR o.lease_until<=clock_timestamp()) FROM factory_jobs j LEFT JOIN factory_rest_worker_ownership o ON o.job_id=j.id WHERE j.id=$1`, a.JobID).Scan(&running, &expired)
		_ = tx.Rollback()
		if checkErr == nil && running && expired {
			s.recoveryMu.Lock()
			s.recoveryNeeded[a.JobID] = struct{}{}
			s.recoveryMu.Unlock()
		}
		return nil, ErrStaleAttempt
	}
	var lockedID string
	if err = tx.QueryRowContext(ctx, `SELECT j.id FROM factory_jobs j JOIN factory_rest_worker_ownership o ON o.job_id=j.id WHERE j.id=$1 AND j.status='running' AND o.generation=$2 AND o.owner_token=$3 AND o.worker_id=$4 AND o.boot_id=$5 AND o.lease_until>clock_timestamp() FOR UPDATE OF j,o`, a.JobID, a.Generation, a.OwnerToken, pgIdentityOrDefault(a.WorkerID, "worker-local"), pgIdentityOrDefault(a.BootID, "boot-local")).Scan(&lockedID); errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return nil, ErrStaleAttempt
	} else if err != nil {
		_ = tx.Rollback()
		return nil, errors.New("lock PostgreSQL worker ownership")
	}
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM factory_jobs j JOIN factory_rest_worker_ownership o ON o.job_id=j.id WHERE j.id=$1 AND j.status='running' AND o.generation=$2 AND o.owner_token=$3 AND o.worker_id=$4 AND o.boot_id=$5 AND o.lease_until>clock_timestamp())`, a.JobID, a.Generation, a.OwnerToken, pgIdentityOrDefault(a.WorkerID, "worker-local"), pgIdentityOrDefault(a.BootID, "boot-local")).Scan(&valid); err != nil {
		_ = tx.Rollback()
		return nil, errors.New("revalidate PostgreSQL worker lease")
	}
	if !valid {
		_ = tx.Rollback()
		return nil, ErrStaleAttempt
	}
	return tx, nil
}
func (s *PostgresStore) lockPostgresJob(ctx context.Context, tx *sql.Tx, id string) error {
	var status string
	err := tx.QueryRowContext(ctx, `SELECT status FROM factory_jobs WHERE id=$1 FOR UPDATE`, id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return errors.New("read PostgreSQL job state")
	}
	return nil
}

func (s *PostgresStore) RecordProviderAttemptForAttempt(a AttemptHandle, p ProviderAttempt) error {
	if !validAttemptHandle(a) || validateProviderAttempt(p) != nil || p.Branch != "factory/job/"+a.JobID || !validCommitID(p.Commit) {
		return ErrInvalidInput
	}
	return s.recordProviderAttempt(a.JobID, &a, p)
}
func (s *PostgresStore) recordProviderAttempt(id string, a *AttemptHandle, p ProviderAttempt) error {
	encoded, _ := json.Marshal(p)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var tx *sql.Tx
	var err error
	if a != nil {
		tx, err = s.beginAttempt(ctx, *a)
	} else {
		if err = s.checkAccepting(); err != nil {
			return err
		}
		tx, err = s.db.BeginTx(ctx, nil)
		if err == nil {
			err = s.lockPostgresJob(ctx, tx, id)
		}
	}
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status string
	if err = tx.QueryRowContext(ctx, `SELECT status FROM factory_jobs WHERE id=$1`, id).Scan(&status); err != nil {
		return ErrNotFound
	}
	if status != string(StatusRunning) {
		return ErrInvalidTransition
	}
	var prior string
	err = tx.QueryRowContext(ctx, `SELECT attempt_json FROM factory_job_provider_attempts WHERE job_id=$1`, id).Scan(&prior)
	if err == nil {
		if prior != string(encoded) {
			return ErrInvalidTransition
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return errors.New("read PostgreSQL provider attempt")
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO factory_job_provider_attempts(job_id,attempt_json,updated_at) VALUES($1,$2,clock_timestamp())`, id, string(encoded)); err != nil {
		return errors.New("record PostgreSQL provider attempt")
	}
	return tx.Commit()
}
func (s *PostgresStore) MarkProviderAttemptUncertainForAttempt(a AttemptHandle) error {
	return s.markProviderUncertain(a.JobID, &a)
}
func (s *PostgresStore) markProviderUncertain(id string, a *AttemptHandle) error {
	if a == nil {
		if err := s.checkAccepting(); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var tx *sql.Tx
	var err error
	if a != nil {
		tx, err = s.beginAttempt(ctx, *a)
	} else {
		tx, err = s.db.BeginTx(ctx, nil)
		if err == nil {
			err = s.lockPostgresJob(ctx, tx, id)
		}
	}
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if a == nil {
		var status string
		if err = tx.QueryRowContext(ctx, `SELECT status FROM factory_jobs WHERE id=$1`, id).Scan(&status); errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		} else if err != nil {
			return errors.New("read PostgreSQL job state")
		}
		if status != string(StatusRunning) {
			return ErrInvalidTransition
		}
		var owned bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM factory_rest_worker_ownership WHERE job_id=$1)`, id).Scan(&owned); err != nil {
			return errors.New("read PostgreSQL worker ownership")
		}
		if owned {
			return ErrStaleAttempt
		}
	}
	var raw string
	if err = tx.QueryRowContext(ctx, `SELECT attempt_json FROM factory_job_provider_attempts WHERE job_id=$1`, id).Scan(&raw); errors.Is(err, sql.ErrNoRows) {
		return ErrInvalidTransition
	} else if err != nil {
		return errors.New("read PostgreSQL provider attempt")
	}
	var p ProviderAttempt
	if json.Unmarshal([]byte(raw), &p) != nil || validateProviderAttempt(p) != nil || p.Branch != "factory/job/"+id || !validCommitID(p.Commit) {
		return errors.New("PostgreSQL provider attempt is invalid")
	}
	p.Uncertain = true
	encoded, _ := json.Marshal(p)
	if _, err = tx.ExecContext(ctx, `UPDATE factory_job_provider_attempts SET attempt_json=$2,updated_at=clock_timestamp() WHERE job_id=$1`, id, string(encoded)); err != nil {
		return errors.New("mark PostgreSQL provider attempt uncertain")
	}
	return tx.Commit()
}
func (s *PostgresStore) RecordProviderOutcomeForAttempt(a AttemptHandle, p ProviderOutcome) error {
	if !validAttemptHandle(a) || validateProviderOutcome(p) != nil {
		return ErrInvalidInput
	}
	return s.recordOutcome(a.JobID, &a, p)
}
func (s *PostgresStore) RecordProviderOutcome(id string, p ProviderOutcome) error {
	if validateProviderOutcome(p) != nil {
		return ErrInvalidInput
	}
	return s.recordOutcome(id, nil, p)
}
func (s *PostgresStore) recordOutcome(id string, a *AttemptHandle, p ProviderOutcome) error {
	encoded, _ := json.Marshal(p)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var tx *sql.Tx
	var err error
	if a != nil {
		tx, err = s.beginAttempt(ctx, *a)
	} else {
		if err = s.checkAccepting(); err != nil {
			return err
		}
		tx, err = s.db.BeginTx(ctx, nil)
		if err == nil {
			err = s.lockPostgresJob(ctx, tx, id)
		}
	}
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status string
	if err = tx.QueryRowContext(ctx, `SELECT status FROM factory_jobs WHERE id=$1`, id).Scan(&status); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return errors.New("read PostgreSQL job state")
	}
	if status != string(StatusRunning) {
		return ErrInvalidTransition
	}
	if a == nil {
		var owned bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM factory_rest_worker_ownership WHERE job_id=$1)`, id).Scan(&owned); err != nil {
			return errors.New("read PostgreSQL worker ownership")
		}
		if owned {
			return ErrStaleAttempt
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO factory_job_provider_outcomes(job_id,provider_json,updated_at) VALUES($1,$2,clock_timestamp()) ON CONFLICT(job_id) DO UPDATE SET provider_json=EXCLUDED.provider_json,updated_at=EXCLUDED.updated_at`, id, string(encoded)); err != nil {
		return errors.New("record PostgreSQL provider outcome")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE factory_jobs SET updated_at=clock_timestamp() WHERE id=$1`, id); err != nil {
		return errors.New("update PostgreSQL job timestamp")
	}
	return tx.Commit()
}
func (s *PostgresStore) RecordVerificationEvidenceForAttempt(a AttemptHandle, e VerificationEvidence) error {
	if !validAttemptHandle(a) || validateVerificationEvidence(e) != nil {
		return ErrInvalidInput
	}
	return s.recordEvidence(a.JobID, &a, e)
}
func (s *PostgresStore) RecordVerificationEvidence(id string, e VerificationEvidence) error {
	if validateVerificationEvidence(e) != nil {
		return ErrInvalidInput
	}
	return s.recordEvidence(id, nil, e)
}
func (s *PostgresStore) recordEvidence(id string, a *AttemptHandle, e VerificationEvidence) error {
	encoded, _ := json.Marshal(e)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var tx *sql.Tx
	var err error
	if a != nil {
		tx, err = s.beginAttempt(ctx, *a)
	} else {
		if err = s.checkAccepting(); err != nil {
			return err
		}
		tx, err = s.db.BeginTx(ctx, nil)
		if err == nil {
			err = s.lockPostgresJob(ctx, tx, id)
		}
	}
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status string
	if err = tx.QueryRowContext(ctx, `SELECT status FROM factory_jobs WHERE id=$1`, id).Scan(&status); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return errors.New("read PostgreSQL job state")
	}
	if status != string(StatusRunning) {
		return ErrInvalidTransition
	}
	if a == nil {
		var owned bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM factory_rest_worker_ownership WHERE job_id=$1)`, id).Scan(&owned); err != nil {
			return errors.New("read PostgreSQL worker ownership")
		}
		if owned {
			return ErrStaleAttempt
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO factory_job_verification_evidence(job_id,evidence_json,updated_at) VALUES($1,$2,clock_timestamp()) ON CONFLICT(job_id) DO UPDATE SET evidence_json=EXCLUDED.evidence_json,updated_at=EXCLUDED.updated_at`, id, string(encoded)); err != nil {
		return errors.New("record PostgreSQL verification evidence")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE factory_jobs SET updated_at=clock_timestamp() WHERE id=$1`, id); err != nil {
		return errors.New("update PostgreSQL job timestamp")
	}
	return tx.Commit()
}

func (s *PostgresStore) FinishAttempt(a AttemptHandle, status Status) error {
	if !validAttemptHandle(a) || !status.Terminal() {
		return ErrInvalidInput
	}
	return s.finish(a.JobID, &a, status)
}
func (s *PostgresStore) Finish(id string, status Status) error {
	if !status.Terminal() {
		return ErrInvalidTransition
	}
	return s.finish(id, nil, status)
}
func (s *PostgresStore) finish(id string, a *AttemptHandle, status Status) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var tx *sql.Tx
	var err error
	if a != nil {
		tx, err = s.beginAttempt(ctx, *a)
	} else {
		if err = s.checkOpen(); err != nil {
			return err
		}
		tx, err = s.db.BeginTx(ctx, nil)
		if err == nil {
			err = s.lockPostgresJob(ctx, tx, id)
		}
	}
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if a == nil {
		var exists int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM factory_jobs WHERE id=$1`, id).Scan(&exists); err != nil {
			return errors.New("read PostgreSQL job")
		}
		if exists == 0 {
			return ErrNotFound
		}
		var owned bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM factory_rest_worker_ownership WHERE job_id=$1)`, id).Scan(&owned); err != nil {
			return errors.New("read PostgreSQL worker ownership")
		}
		if owned {
			return ErrStaleAttempt
		}
		s.recoveryMu.RLock()
		_, interrupted := s.recoveryNeeded[id]
		s.recoveryMu.RUnlock()
		if interrupted {
			return ErrInvalidTransition
		}
	}
	res, err := tx.ExecContext(ctx, `UPDATE factory_jobs SET status=$2,cancellation_requested=false,updated_at=clock_timestamp() WHERE id=$1 AND status='running'`, id, status)
	if err != nil {
		return errors.New("finish PostgreSQL job")
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrInvalidTransition
	}
	if err = insertPostgresEvent(ctx, tx, id, string(status), "Job finished", s.config.MaxEventsPerJob); err != nil {
		return errors.New("record PostgreSQL job completion")
	}
	if a != nil {
		if _, err = tx.ExecContext(ctx, `UPDATE factory_rest_attempt_history SET finished_at=clock_timestamp(),status=$3 WHERE job_id=$1 AND generation=$2 AND finished_at IS NULL`, id, a.Generation, status); err != nil {
			return errors.New("finish PostgreSQL attempt history")
		}
		if _, err = tx.ExecContext(ctx, `UPDATE factory_rest_worker_ownership SET lease_until=clock_timestamp() WHERE job_id=$1 AND generation=$2 AND owner_token=$3`, id, a.Generation, a.OwnerToken); err != nil {
			return errors.New("release PostgreSQL worker ownership")
		}
	}
	return tx.Commit()
}

func (s *PostgresStore) Cancel(id string) (Snapshot, error) {
	if err := s.checkAccepting(); err != nil {
		return Snapshot{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, errors.New("PostgreSQL job store unavailable")
	}
	defer tx.Rollback()
	var status string
	var requested bool
	err = tx.QueryRowContext(ctx, `SELECT status,cancellation_requested FROM factory_jobs WHERE id=$1 FOR UPDATE`, id).Scan(&status, &requested)
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, ErrNotFound
	}
	if err != nil {
		return Snapshot{}, errors.New("read PostgreSQL job state")
	}
	switch Status(status) {
	case StatusQueued:
		if _, err = tx.ExecContext(ctx, `UPDATE factory_jobs SET status='canceled',updated_at=clock_timestamp() WHERE id=$1`, id); err != nil {
			return Snapshot{}, errors.New("cancel PostgreSQL queued job")
		}
		if err = insertPostgresEvent(ctx, tx, id, "canceled", "Job canceled before start", s.config.MaxEventsPerJob); err != nil {
			return Snapshot{}, errors.New("record PostgreSQL cancellation")
		}
	case StatusRunning:
		s.recoveryMu.Lock()
		_, interrupted := s.recoveryNeeded[id]
		var owned, activeLease bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM factory_rest_worker_ownership WHERE job_id=$1), EXISTS(SELECT 1 FROM factory_rest_worker_ownership WHERE job_id=$1 AND lease_until>clock_timestamp())`, id).Scan(&owned, &activeLease); err != nil {
			s.recoveryMu.Unlock()
			return Snapshot{}, errors.New("check PostgreSQL worker lease")
		}
		if owned && !activeLease {
			interrupted = true
			s.recoveryNeeded[id] = struct{}{}
		}
		s.recoveryMu.Unlock()
		if interrupted {
			return Snapshot{}, ErrInvalidTransition
		}
		if !requested {
			if _, err = tx.ExecContext(ctx, `UPDATE factory_jobs SET cancellation_requested=true,updated_at=clock_timestamp() WHERE id=$1`, id); err != nil {
				return Snapshot{}, errors.New("request PostgreSQL cancellation")
			}
			if err = insertPostgresEvent(ctx, tx, id, "cancel_requested", "Cancellation requested", s.config.MaxEventsPerJob); err != nil {
				return Snapshot{}, errors.New("record PostgreSQL cancellation request")
			}
		}
	default:
		return Snapshot{}, ErrInvalidTransition
	}
	snap, err := scanPostgresSnapshot(tx.QueryRowContext(ctx, pgSnapshotQuery()+`WHERE j.id=$1`, id))
	if err != nil {
		return Snapshot{}, errors.New("read canceled PostgreSQL job")
	}
	if err = tx.Commit(); err != nil {
		return Snapshot{}, errors.New("commit PostgreSQL cancellation")
	}
	return snap, nil
}

func (s *PostgresStore) ResolveInterrupted(id string, d InterruptedDisposition) error {
	if d != InterruptedDispositionFailed && d != InterruptedDispositionCanceled {
		return ErrInvalidInput
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("PostgreSQL job store unavailable")
	}
	defer tx.Rollback()
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	_, needed := s.recoveryNeeded[id]
	var status string
	if err = tx.QueryRowContext(ctx, `SELECT status FROM factory_jobs WHERE id=$1 FOR UPDATE`, id).Scan(&status); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return errors.New("read PostgreSQL job state")
	}
	if status == string(StatusRunning) {
		var activeLease bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM factory_rest_worker_ownership WHERE job_id=$1 AND lease_until>clock_timestamp())`, id).Scan(&activeLease); err != nil {
			return errors.New("check PostgreSQL interrupted lease")
		}
		if !activeLease {
			needed = true
			s.recoveryNeeded[id] = struct{}{}
		}
	}
	var previous string
	pe := tx.QueryRowContext(ctx, `SELECT disposition FROM factory_job_operator_dispositions WHERE job_id=$1`, id).Scan(&previous)
	if pe == nil {
		if status != string(d) || previous != string(d) {
			return ErrInvalidTransition
		}
		if err = tx.Commit(); err != nil {
			return errors.New("confirm PostgreSQL disposition")
		}
		delete(s.recoveryNeeded, id)
		return nil
	}
	if !errors.Is(pe, sql.ErrNoRows) {
		return errors.New("read PostgreSQL disposition")
	}
	if status != string(StatusRunning) || !needed {
		return ErrInvalidTransition
	}
	var sideEffects int
	if err = tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM factory_job_provider_attempts WHERE job_id=$1)+(SELECT COUNT(*) FROM factory_job_provider_outcomes WHERE job_id=$1)`, id).Scan(&sideEffects); err != nil {
		return errors.New("check PostgreSQL provider side effects")
	}
	if sideEffects != 0 {
		return ErrInvalidTransition
	}
	if _, err = tx.ExecContext(ctx, `UPDATE factory_jobs SET status=$2,updated_at=clock_timestamp() WHERE id=$1`, id, d); err != nil {
		return errors.New("record PostgreSQL disposition")
	}
	if err = insertPostgresEvent(ctx, tx, id, "operator_disposition", string(d), s.config.MaxEventsPerJob); err != nil {
		return errors.New("record PostgreSQL disposition history")
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO factory_job_operator_dispositions(job_id,disposition,updated_at) VALUES($1,$2,clock_timestamp())`, id, d); err != nil {
		return errors.New("persist PostgreSQL disposition")
	}
	if err = tx.Commit(); err != nil {
		return errors.New("commit PostgreSQL disposition")
	}
	delete(s.recoveryNeeded, id)
	return nil
}

func (s *PostgresStore) Recover(ctx context.Context) (RecoveryReport, error) {
	if ctx == nil {
		return RecoveryReport{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return RecoveryReport{}, err
	}
	if err := s.checkOpen(); err != nil {
		return RecoveryReport{}, err
	}
	rows, err := s.db.QueryContext(ctx, pgSnapshotQuery()+`ORDER BY j.queue_sequence`)
	if err != nil {
		return RecoveryReport{}, errors.New("recover PostgreSQL jobs")
	}
	defer rows.Close()
	var report RecoveryReport
	for rows.Next() {
		snap, e := scanPostgresSnapshot(rows)
		if e != nil {
			return RecoveryReport{}, errors.New("decode PostgreSQL recovery record")
		}
		switch RecoveryDispositionFor(snap.Status) {
		case RecoveryResume:
			report.Resume = append(report.Resume, snap)
		case RecoveryNeedsOperator:
			report.NeedsOperator = append(report.NeedsOperator, snap)
		case RecoveryRetainTerminal:
			report.Terminal = append(report.Terminal, snap)
		}
	}
	if err = rows.Err(); err != nil {
		return RecoveryReport{}, errors.New("recover PostgreSQL jobs")
	}
	return report, nil
}
func (s *PostgresStore) OperationalSummary(ctx context.Context) (OperationalSummary, error) {
	if ctx == nil {
		return OperationalSummary{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return OperationalSummary{}, err
	}
	if err := s.checkOpen(); err != nil {
		return OperationalSummary{}, err
	}
	out := OperationalSummary{RecordLimit: s.config.MaxRecords, QueueCapacity: s.config.QueueCapacity}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(*) FILTER(WHERE status='queued'),COUNT(*) FILTER(WHERE status='running'),COUNT(*) FILTER(WHERE status='succeeded'),COUNT(*) FILTER(WHERE status='failed'),COUNT(*) FILTER(WHERE status='canceled') FROM factory_jobs`).Scan(&out.RetainedRecords, &out.Queued, &out.Running, &out.Succeeded, &out.Failed, &out.Canceled); err != nil {
		return OperationalSummary{}, errors.New("read PostgreSQL operational summary")
	}
	s.recoveryMu.RLock()
	out.RecoveryNeeded = len(s.recoveryNeeded)
	s.recoveryMu.RUnlock()
	out.QueueSaturated = out.Queued >= out.QueueCapacity
	return out, nil
}
func (s *PostgresStore) Ping(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidInput
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := s.db.PingContext(ctx); err != nil {
		return errors.New("PostgreSQL job store is unavailable")
	}
	return nil
}

func insertPostgresEvent(ctx context.Context, tx *sql.Tx, id, typ, msg string, limit int) error {
	var seq int64
	if err := tx.QueryRowContext(ctx, `UPDATE factory_jobs SET event_sequence=event_sequence+1 WHERE id=$1 RETURNING event_sequence`, id).Scan(&seq); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_job_events(job_id,sequence,at,type,message) VALUES($1,$2,clock_timestamp(),$3,$4)`, id, seq, typ, msg); err != nil {
		return err
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM factory_job_events WHERE job_id=$1`, id).Scan(&count); err != nil {
		return err
	}
	if count > limit {
		if _, err := tx.ExecContext(ctx, `DELETE FROM factory_job_events WHERE job_id=$1 AND sequence IN (SELECT sequence FROM factory_job_events WHERE job_id=$1 ORDER BY sequence LIMIT $2)`, id, count-limit); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE factory_jobs SET history_truncated=true WHERE id=$1`, id); err != nil {
			return err
		}
	}
	return nil
}
