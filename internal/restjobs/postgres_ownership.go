package restjobs

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"
)

const (
	postgresOwnershipInitializationLockKey int64 = 0x464143544F5259 // "FACTORY", reserved for ownership initialization.
	postgresMinLease                             = 5 * time.Second
	postgresMaxLease                             = 10 * time.Minute
	postgresMaxOwnerIDBytes                      = 128
	postgresOwnerTokenBytes                      = 32
)

var postgresOwnerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)

// PostgresAttempt is the private fencing identity for one claimed job. OwnerToken
// is a credential and is intentionally excluded from JSON serialization.
type PostgresAttempt struct {
	JobID      string `json:"job_id"`
	Generation int64  `json:"generation"`
	OwnerToken string `json:"-"`
	WorkerID   string `json:"worker_id"`
	BootID     string `json:"boot_id"`
}

// PostgresAttemptClaim is the identity issued when a queued job transitions to
// running. The embedded token is never serialized.
type PostgresAttemptClaim struct {
	PostgresAttempt
}

// PostgresOwnership implements an isolated PostgreSQL worker-ownership protocol.
// It is not a job Store and does not admit, requeue, or execute jobs.
type PostgresOwnership struct {
	db *sql.DB
}

// NewPostgresOwnership binds the ownership protocol to a PostgreSQL primary.
func NewPostgresOwnership(db *sql.DB) *PostgresOwnership {
	return &PostgresOwnership{db: db}
}

// Initialize creates the ownership and attempt-history schema in one
// transaction. The foreign keys make initialization fail closed when
// factory_jobs is absent.
func (p *PostgresOwnership) Initialize(ctx context.Context) error {
	if ctx == nil || p == nil || p.db == nil {
		return ErrInvalidInput
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("begin PostgreSQL ownership initialization")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, postgresOwnershipInitializationLockKey); err != nil {
		return errors.New("lock PostgreSQL ownership initialization")
	}
	var ownershipExists, historyExists bool
	if err := tx.QueryRowContext(ctx, `SELECT
		to_regclass('factory_job_ownership') IS NOT NULL,
		to_regclass('factory_job_attempt_history') IS NOT NULL`).Scan(&ownershipExists, &historyExists); err != nil {
		return errors.New("check existing PostgreSQL ownership tables")
	}
	if ownershipExists != historyExists {
		return errors.New("incomplete PostgreSQL ownership schema; fresh initialization required")
	}
	if ownershipExists {
		if err := validatePostgresOwnershipTableColumns(ctx, tx); err != nil {
			return err
		}
		if err := validatePostgresOwnershipConstraints(ctx, tx); err != nil {
			return err
		}
	} else {
		if _, err := tx.ExecContext(ctx, `CREATE TABLE factory_job_ownership (
			job_id text PRIMARY KEY REFERENCES factory_jobs(id),
			generation bigint NOT NULL CHECK (generation > 0),
			owner_token text NOT NULL CHECK (length(owner_token) = 64),
			worker_id text NOT NULL CHECK (length(worker_id) BETWEEN 1 AND 128),
			boot_id text NOT NULL CHECK (length(boot_id) BETWEEN 1 AND 128),
			lease_until timestamptz NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
		)`); err != nil {
			return errors.New("create PostgreSQL ownership table")
		}
		// Worker and boot IDs are operator-chosen labels, not credentials. The
		// bearer owner token is deliberately absent from durable attempt history.
		if _, err := tx.ExecContext(ctx, `CREATE TABLE factory_job_attempt_history (
			job_id text NOT NULL REFERENCES factory_jobs(id),
			generation bigint NOT NULL CHECK (generation > 0),
			worker_id text NOT NULL CHECK (length(worker_id) BETWEEN 1 AND 128),
			boot_id text NOT NULL CHECK (length(boot_id) BETWEEN 1 AND 128),
			claimed_at timestamptz NOT NULL,
			started_at timestamptz NOT NULL,
			lease_until timestamptz NOT NULL,
			finished_at timestamptz,
			status text CHECK (status IN ('succeeded', 'failed', 'canceled')),
			PRIMARY KEY (job_id, generation),
			CHECK ((finished_at IS NULL AND status IS NULL) OR (finished_at IS NOT NULL AND status IS NOT NULL))
		)`); err != nil {
			return errors.New("create PostgreSQL attempt history table")
		}
	}
	var incompleteRunningAttempt bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM factory_jobs AS j
		LEFT JOIN factory_job_ownership AS o ON o.job_id = j.id
		WHERE j.status = 'running' AND (
			o.job_id IS NULL OR NOT EXISTS (
				SELECT 1 FROM factory_job_attempt_history AS h
				WHERE h.job_id = o.job_id AND h.generation = o.generation
				AND h.worker_id = o.worker_id AND h.boot_id = o.boot_id
				AND h.lease_until = o.lease_until AND h.finished_at IS NULL AND h.status IS NULL
			)
		)
	)`).Scan(&incompleteRunningAttempt); err != nil {
		return errors.New("validate PostgreSQL running attempt history")
	}
	if incompleteRunningAttempt {
		return errors.New("running PostgreSQL job lacks matching active ownership history")
	}
	if err := tx.Commit(); err != nil {
		return errors.New("commit PostgreSQL ownership initialization")
	}
	return nil
}

func validatePostgresOwnershipTableColumns(ctx context.Context, tx *sql.Tx) error {
	for table, want := range map[string][]string{
		"factory_job_ownership": {
			"job_id:text:NO", "generation:bigint:NO", "owner_token:text:NO", "worker_id:text:NO",
			"boot_id:text:NO", "lease_until:timestamp with time zone:NO", "updated_at:timestamp with time zone:NO",
		},
		"factory_job_attempt_history": {
			"job_id:text:NO", "generation:bigint:NO", "worker_id:text:NO", "boot_id:text:NO",
			"claimed_at:timestamp with time zone:NO", "started_at:timestamp with time zone:NO",
			"lease_until:timestamp with time zone:NO", "finished_at:timestamp with time zone:YES", "status:text:YES",
		},
	} {
		rows, err := tx.QueryContext(ctx, `SELECT column_name || ':' || data_type || ':' || is_nullable
			FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1
			ORDER BY ordinal_position`, table)
		if err != nil {
			return errors.New("inspect PostgreSQL ownership table columns")
		}
		var got []string
		for rows.Next() {
			var column string
			if err := rows.Scan(&column); err != nil {
				rows.Close()
				return errors.New("read PostgreSQL ownership table columns")
			}
			got = append(got, column)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return errors.New("read PostgreSQL ownership table columns")
		}
		if err := rows.Close(); err != nil {
			return errors.New("close PostgreSQL ownership table columns")
		}
		if len(got) != len(want) {
			return errors.New("incompatible PostgreSQL ownership schema; fresh initialization required")
		}
		for i := range want {
			if got[i] != want[i] {
				return errors.New("incompatible PostgreSQL ownership schema; fresh initialization required")
			}
		}
	}
	return nil
}

func validatePostgresOwnershipConstraints(ctx context.Context, tx *sql.Tx) error {
	want := map[string][]string{
		"factory_job_ownership": {
			"p:PRIMARY KEY (job_id)",
			"f:FOREIGN KEY (job_id) REFERENCES factory_jobs(id)",
			"c:CHECK ((generation > 0))",
			"c:CHECK ((length(owner_token) = 64))",
			"c:CHECK (((length(worker_id) >= 1) AND (length(worker_id) <= 128)))",
			"c:CHECK (((length(boot_id) >= 1) AND (length(boot_id) <= 128)))",
		},
		"factory_job_attempt_history": {
			"p:PRIMARY KEY (job_id, generation)",
			"f:FOREIGN KEY (job_id) REFERENCES factory_jobs(id)",
			"c:CHECK ((generation > 0))",
			"c:CHECK (((length(worker_id) >= 1) AND (length(worker_id) <= 128)))",
			"c:CHECK (((length(boot_id) >= 1) AND (length(boot_id) <= 128)))",
			"c:CHECK ((((finished_at IS NULL) AND (status IS NULL)) OR ((finished_at IS NOT NULL) AND (status IS NOT NULL))))",
			"c:CHECK ((status = ANY (ARRAY['succeeded'::text, 'failed'::text, 'canceled'::text])))",
		},
	}
	for table, wantConstraints := range want {
		rows, err := tx.QueryContext(ctx, `SELECT contype::text || ':' || pg_get_constraintdef(c.oid) || CASE WHEN c.convalidated THEN '' ELSE ':NOT VALID' END
			FROM pg_constraint AS c JOIN pg_namespace AS n ON n.oid = c.connamespace
			WHERE c.conrelid = to_regclass($1) AND n.nspname = current_schema()
				AND c.contype IN ('p', 'f', 'c', 'u')
			ORDER BY c.contype, c.conname`, table)
		if err != nil {
			return errors.New("inspect PostgreSQL ownership table constraints")
		}
		var got []string
		for rows.Next() {
			var constraint string
			if err := rows.Scan(&constraint); err != nil {
				rows.Close()
				return errors.New("read PostgreSQL ownership table constraints")
			}
			got = append(got, constraint)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return errors.New("read PostgreSQL ownership table constraints")
		}
		if err := rows.Close(); err != nil {
			return errors.New("close PostgreSQL ownership table constraints")
		}
		sort.Strings(got)
		sort.Strings(wantConstraints)
		if len(got) != len(wantConstraints) {
			return errors.New("incompatible PostgreSQL ownership schema; fresh initialization required")
		}
		for i := range got {
			if got[i] != wantConstraints[i] {
				return fmt.Errorf("incompatible PostgreSQL ownership schema; fresh initialization required (%s: got %q, want %q)", table, got[i], wantConstraints[i])
			}
		}
	}
	return nil
}

// ClaimNext atomically claims the oldest queued job visible on the primary.
// Expired ownership is intentionally not recovered or reassigned by this API.
func (p *PostgresOwnership) ClaimNext(ctx context.Context, workerID, bootID string, leaseDuration time.Duration) (PostgresAttemptClaim, error) {
	if ctx == nil || p == nil || p.db == nil || !validPostgresOwnerID(workerID) || !validPostgresOwnerID(bootID) || !validPostgresLease(leaseDuration) {
		return PostgresAttemptClaim{}, ErrInvalidInput
	}
	token, err := newPostgresOwnerToken()
	if err != nil {
		return PostgresAttemptClaim{}, errors.New("generate PostgreSQL worker owner token")
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return PostgresAttemptClaim{}, errors.New("begin PostgreSQL job claim")
	}
	defer tx.Rollback()
	var jobID string
	err = tx.QueryRowContext(ctx, `SELECT id
		FROM factory_jobs
		WHERE status = 'queued'
		ORDER BY queue_sequence
		FOR UPDATE SKIP LOCKED
		LIMIT 1`).Scan(&jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return PostgresAttemptClaim{}, ErrNoQueuedJobs
	}
	if err != nil {
		return PostgresAttemptClaim{}, errors.New("select PostgreSQL queued job")
	}
	var generation int64
	err = tx.QueryRowContext(ctx, `INSERT INTO factory_job_ownership
		(job_id, generation, owner_token, worker_id, boot_id, lease_until, updated_at)
		VALUES ($1, 1, $2, $3, $4,
			clock_timestamp() + ($5 * interval '1 microsecond'), clock_timestamp())
		ON CONFLICT (job_id) DO UPDATE SET
			generation = factory_job_ownership.generation + 1,
			owner_token = EXCLUDED.owner_token,
			worker_id = EXCLUDED.worker_id,
			boot_id = EXCLUDED.boot_id,
			lease_until = EXCLUDED.lease_until,
			updated_at = clock_timestamp()
		RETURNING generation`, jobID, token, workerID, bootID, leaseDuration.Microseconds()).Scan(&generation)
	if err != nil {
		return PostgresAttemptClaim{}, errors.New("record PostgreSQL job ownership")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_job_attempt_history
		(job_id, generation, worker_id, boot_id, claimed_at, started_at, lease_until)
		SELECT job_id, generation, worker_id, boot_id, clock_timestamp(), clock_timestamp(), lease_until
		FROM factory_job_ownership WHERE job_id = $1`, jobID); err != nil {
		return PostgresAttemptClaim{}, errors.New("record PostgreSQL attempt history")
	}
	result, err := tx.ExecContext(ctx, `UPDATE factory_jobs SET status = 'running'
		WHERE id = $1 AND status = 'queued'`, jobID)
	if err != nil {
		return PostgresAttemptClaim{}, errors.New("transition PostgreSQL job to running")
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return PostgresAttemptClaim{}, errors.New("transition PostgreSQL job to running")
	}
	if err := tx.Commit(); err != nil {
		return PostgresAttemptClaim{}, errors.New("commit PostgreSQL job claim")
	}
	return PostgresAttemptClaim{PostgresAttempt: PostgresAttempt{
		JobID: jobID, Generation: generation, OwnerToken: token, WorkerID: workerID, BootID: bootID,
	}}, nil
}

// Heartbeat extends a still-live lease using the database clock. An expired
// lease is suspect and cannot be renewed.
func (p *PostgresOwnership) Heartbeat(ctx context.Context, attempt PostgresAttempt, leaseDuration time.Duration) error {
	if !validPostgresAttempt(attempt) || !validPostgresLease(leaseDuration) || ctx == nil || p == nil || p.db == nil {
		return ErrInvalidInput
	}
	tx, err := p.beginCurrentAttempt(ctx, attempt)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE factory_job_ownership AS o SET
		lease_until = clock_timestamp() + ($6 * interval '1 microsecond'),
		updated_at = clock_timestamp()
		WHERE o.job_id = $1 AND o.generation = $2 AND o.owner_token = $3
		AND o.worker_id = $4 AND o.boot_id = $5 AND o.lease_until > clock_timestamp()
		AND EXISTS (SELECT 1 FROM factory_jobs j WHERE j.id = o.job_id AND j.status = 'running')`,
		attempt.JobID, attempt.Generation, attempt.OwnerToken, attempt.WorkerID, attempt.BootID, leaseDuration.Microseconds())
	if err != nil {
		return errors.New("renew PostgreSQL worker lease")
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return ErrStaleAttempt
	}
	result, err = tx.ExecContext(ctx, `UPDATE factory_job_attempt_history AS h SET lease_until = o.lease_until
		FROM factory_job_ownership AS o
		WHERE h.job_id = $1 AND h.generation = $2 AND h.finished_at IS NULL AND h.status IS NULL
		AND o.job_id = h.job_id AND o.generation = h.generation
		AND o.worker_id = $3 AND o.boot_id = $4`, attempt.JobID, attempt.Generation, attempt.WorkerID, attempt.BootID)
	if err != nil {
		return errors.New("renew PostgreSQL attempt history lease")
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return errors.New("renew PostgreSQL attempt history lease")
	}
	if err := tx.Commit(); err != nil {
		return errors.New("commit PostgreSQL worker heartbeat")
	}
	return nil
}

// Finish conditionally transitions a live owned attempt to a terminal status.
func (p *PostgresOwnership) Finish(ctx context.Context, attempt PostgresAttempt, status Status) error {
	if !validPostgresAttempt(attempt) || !status.Terminal() || ctx == nil || p == nil || p.db == nil {
		return ErrInvalidInput
	}
	tx, err := p.beginCurrentAttempt(ctx, attempt)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE factory_jobs AS j SET status = $6
		WHERE j.id = $1 AND j.status = 'running'
		AND EXISTS (SELECT 1 FROM factory_job_ownership o
			WHERE o.job_id = j.id AND o.generation = $2 AND o.owner_token = $3
			AND o.worker_id = $4 AND o.boot_id = $5 AND o.lease_until > clock_timestamp())`,
		attempt.JobID, attempt.Generation, attempt.OwnerToken, attempt.WorkerID, attempt.BootID, status)
	if err != nil {
		return errors.New("finish PostgreSQL job")
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return ErrStaleAttempt
	}
	result, err = tx.ExecContext(ctx, `UPDATE factory_job_attempt_history AS h SET
		finished_at = clock_timestamp(), status = $3,
		lease_until = o.lease_until
		FROM factory_job_ownership AS o
		WHERE h.job_id = $1 AND h.generation = $2 AND h.finished_at IS NULL AND h.status IS NULL
		AND o.job_id = h.job_id AND o.generation = h.generation
		AND o.worker_id = $4 AND o.boot_id = $5`, attempt.JobID, attempt.Generation, status, attempt.WorkerID, attempt.BootID)
	if err != nil {
		return errors.New("finish PostgreSQL attempt history")
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return errors.New("finish PostgreSQL attempt history")
	}
	if err := tx.Commit(); err != nil {
		return errors.New("commit PostgreSQL job finish")
	}
	return nil
}

// ValidateCurrent checks the database-clock lease and running status without
// mutating either record.
func (p *PostgresOwnership) ValidateCurrent(ctx context.Context, attempt PostgresAttempt) error {
	if !validPostgresAttempt(attempt) || ctx == nil || p == nil || p.db == nil {
		return ErrInvalidInput
	}
	var current bool
	err := p.db.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM factory_jobs j JOIN factory_job_ownership o ON o.job_id = j.id
		WHERE j.id = $1 AND j.status = 'running' AND o.generation = $2
		AND o.owner_token = $3 AND o.worker_id = $4 AND o.boot_id = $5
		AND o.lease_until > clock_timestamp())`,
		attempt.JobID, attempt.Generation, attempt.OwnerToken, attempt.WorkerID, attempt.BootID).Scan(&current)
	if err != nil {
		return errors.New("validate PostgreSQL worker attempt")
	}
	if !current {
		return ErrStaleAttempt
	}
	return nil
}

func (p *PostgresOwnership) beginCurrentAttempt(ctx context.Context, attempt PostgresAttempt) (*sql.Tx, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, errors.New("begin PostgreSQL worker mutation")
	}
	var valid bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM factory_jobs j JOIN factory_job_ownership o ON o.job_id = j.id
		WHERE j.id = $1 AND j.status = 'running' AND o.generation = $2
		AND o.owner_token = $3 AND o.worker_id = $4 AND o.boot_id = $5
		AND o.lease_until > clock_timestamp())`,
		attempt.JobID, attempt.Generation, attempt.OwnerToken, attempt.WorkerID, attempt.BootID).Scan(&valid)
	if err != nil {
		_ = tx.Rollback()
		return nil, errors.New("read PostgreSQL worker ownership")
	}
	if !valid {
		_ = tx.Rollback()
		return nil, ErrStaleAttempt
	}
	// Serialize mutations for this attempt. The final UPDATE repeats all fencing
	// and lease predicates so a lease expiring while waiting cannot mutate state.
	_, err = tx.ExecContext(ctx, `SELECT 1 FROM factory_jobs j
		JOIN factory_job_ownership o ON o.job_id = j.id
		WHERE j.id = $1 AND j.status = 'running' AND o.generation = $2
		AND o.owner_token = $3 AND o.worker_id = $4 AND o.boot_id = $5
		AND o.lease_until > clock_timestamp()
		FOR UPDATE OF j, o`,
		attempt.JobID, attempt.Generation, attempt.OwnerToken, attempt.WorkerID, attempt.BootID)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return nil, ErrStaleAttempt
	}
	if err != nil {
		_ = tx.Rollback()
		return nil, errors.New("lock PostgreSQL worker ownership")
	}
	return tx, nil
}

func validPostgresLease(duration time.Duration) bool {
	return duration >= postgresMinLease && duration <= postgresMaxLease
}

func validPostgresOwnerID(value string) bool {
	return len(value) > 0 && len(value) <= postgresMaxOwnerIDBytes && postgresOwnerIDPattern.MatchString(value)
}

func validPostgresAttempt(attempt PostgresAttempt) bool {
	if !validJobID(attempt.JobID) || attempt.Generation <= 0 || !validPostgresOwnerID(attempt.WorkerID) || !validPostgresOwnerID(attempt.BootID) || len(attempt.OwnerToken) != postgresOwnerTokenBytes*2 {
		return false
	}
	decoded, err := hex.DecodeString(attempt.OwnerToken)
	return err == nil && len(decoded) == postgresOwnerTokenBytes
}

func newPostgresOwnerToken() (string, error) {
	var token [postgresOwnerTokenBytes]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(token[:]), nil
}
