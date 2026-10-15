package restjobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresOwnershipValidationAndTokenPrivacy(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  bool
	}{
		{"minimum lease", validPostgresLease(postgresMinLease)},
		{"maximum lease", validPostgresLease(postgresMaxLease)},
		{"lease too short", validPostgresLease(postgresMinLease - time.Nanosecond)},
		{"lease too long", validPostgresLease(postgresMaxLease + time.Nanosecond)},
		{"valid worker identity", validPostgresOwnerID("worker-1.node_a:blue")},
		{"unsafe worker identity", validPostgresOwnerID("worker/one")},
		{"empty boot identity", validPostgresOwnerID("")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := tc.name == "minimum lease" || tc.name == "maximum lease" || tc.name == "valid worker identity"
			if tc.got != want {
				t.Fatalf("validation result = %v, want %v", tc.got, want)
			}
		})
	}

	attempt := PostgresAttempt{JobID: "123e4567-e89b-42d3-a456-426614174000", Generation: 2, OwnerToken: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", WorkerID: "worker-1", BootID: "boot-1"}
	if !validPostgresAttempt(attempt) {
		t.Fatal("valid attempt was rejected")
	}
	encoded, err := json.Marshal(PostgresAttemptClaim{PostgresAttempt: attempt})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) == "" || strings.Contains(string(encoded), attempt.OwnerToken) {
		t.Fatalf("attempt token leaked through JSON: %s", encoded)
	}
	attempt.OwnerToken = "not-a-token"
	if validPostgresAttempt(attempt) {
		t.Fatal("malformed credential accepted")
	}
}

func TestPostgresOwnershipIntegration(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL 18 ownership integration tests")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL integration database: %v", err)
	}
	defer admin.Close()
	var version int
	if err := admin.QueryRow(`SHOW server_version_num`).Scan(&version); err != nil {
		t.Fatalf("read PostgreSQL server version: %v", err)
	}
	if version/10000 != 18 {
		t.Fatalf("PostgreSQL major version = %d, want 18", version/10000)
	}

	t.Run("migration fails closed without canonical table", func(t *testing.T) {
		db := scopedPostgresDB(t, admin, dsn)
		if err := NewPostgresOwnership(db).Migrate(context.Background()); err == nil {
			t.Fatal("migration succeeded without factory_jobs")
		}
		var exists bool
		if err := db.QueryRow(`SELECT to_regclass('factory_job_ownership') IS NOT NULL`).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatal("failed migration left an ownership table behind")
		}
	})

	t.Run("concurrent migrations serialize across connections", func(t *testing.T) {
		schema, err := newPostgresSchemaName()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
			t.Fatalf("create schema: %v", err)
		}
		db1 := postgresDBForSchema(t, dsn, schema)
		db2 := postgresDBForSchema(t, dsn, schema)
		t.Cleanup(func() {
			if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
				t.Errorf("drop test schema: %v", err)
			}
		})
		mustExec(t, db1, `CREATE TABLE factory_jobs (
			id text PRIMARY KEY,
			status text NOT NULL CHECK (status IN ('queued','running','succeeded','failed','canceled')),
			queue_sequence bigint NOT NULL UNIQUE
		) `)

		var pid1, pid2 int
		if err := db1.QueryRow(`SELECT pg_backend_pid()`).Scan(&pid1); err != nil {
			t.Fatal(err)
		}
		if err := db2.QueryRow(`SELECT pg_backend_pid()`).Scan(&pid2); err != nil {
			t.Fatal(err)
		}
		if pid1 == pid2 {
			t.Fatal("independent database pools unexpectedly share a PostgreSQL connection")
		}

		start := make(chan struct{})
		errs := make(chan error, 2)
		var migrations sync.WaitGroup
		for _, db := range []*sql.DB{db1, db2} {
			migrations.Add(1)
			go func(db *sql.DB) {
				defer migrations.Done()
				<-start
				errs <- NewPostgresOwnership(db).Migrate(context.Background())
			}(db)
		}
		close(start)
		migrations.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent migration failed: %v", err)
			}
		}

		var version int
		if err := db1.QueryRow(`SELECT version FROM factory_postgres_ownership_schema WHERE singleton = 1`).Scan(&version); err != nil {
			t.Fatalf("read migrated schema version: %v", err)
		}
		if version != postgresOwnershipSchemaVersion {
			t.Fatalf("schema version = %d, want %d", version, postgresOwnershipSchemaVersion)
		}
		for _, table := range []string{"factory_postgres_ownership_schema", "factory_job_ownership"} {
			var exists bool
			if err := db1.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
				t.Fatalf("check table %s: %v", table, err)
			}
			if !exists {
				t.Fatalf("migration did not create %s", table)
			}
		}

		mustExec(t, db1, `UPDATE factory_postgres_ownership_schema SET version = 2 WHERE singleton = 1`)
		mustExec(t, db1, `DROP TABLE factory_job_ownership`)
		if err := NewPostgresOwnership(db2).Migrate(context.Background()); err == nil || !strings.Contains(err.Error(), "unsupported PostgreSQL ownership schema version 2") {
			t.Fatalf("unsupported schema version migration error = %v, want unsupported-version failure", err)
		}
		var ownershipTableExists bool
		if err := db1.QueryRow(`SELECT to_regclass('factory_job_ownership') IS NOT NULL`).Scan(&ownershipTableExists); err != nil {
			t.Fatal(err)
		}
		if ownershipTableExists {
			t.Fatal("migration created ownership table despite unsupported schema version")
		}
	})

	t.Run("independent processes contend for one queued job", func(t *testing.T) {
		postgresOwnershipProcessClaimTest(t, admin, dsn)
	})

	t.Run("concurrent claim fencing, expiry, and terminal finish", func(t *testing.T) {
		db := scopedPostgresDB(t, admin, dsn)
		ctx := context.Background()
		mustExec(t, db, `CREATE TABLE factory_jobs (
			id text PRIMARY KEY,
			status text NOT NULL CHECK (status IN ('queued','running','succeeded','failed','canceled')),
			queue_sequence bigint NOT NULL UNIQUE
		)`)
		mustExec(t, db, `INSERT INTO factory_jobs(id,status,queue_sequence) VALUES ('123e4567-e89b-42d3-a456-426614174000','queued',1)`)
		ownership := NewPostgresOwnership(db)
		if err := ownership.Migrate(ctx); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		if err := ownership.Migrate(ctx); err != nil {
			t.Fatalf("repeat migration: %v", err)
		}

		start := make(chan struct{})
		claims := make(chan PostgresAttemptClaim, 2)
		errs := make(chan error, 2)
		var workers sync.WaitGroup
		for _, workerID := range []string{"worker-a", "worker-b"} {
			workers.Add(1)
			go func(workerID string) {
				defer workers.Done()
				<-start
				claim, err := ownership.ClaimNext(ctx, workerID, "boot-1", time.Minute)
				if err != nil {
					errs <- err
					return
				}
				claims <- claim
			}(workerID)
		}
		close(start)
		workers.Wait()
		close(claims)
		close(errs)
		var winner PostgresAttemptClaim
		claimCount := 0
		for claim := range claims {
			winner = claim
			claimCount++
		}
		if claimCount != 1 {
			t.Fatalf("successful claims = %d, want exactly one", claimCount)
		}
		for err := range errs {
			if !errors.Is(err, ErrNoQueuedJobs) {
				t.Fatalf("losing claim error = %v, want ErrNoQueuedJobs", err)
			}
		}
		if winner.Generation != 1 || winner.JobID != "123e4567-e89b-42d3-a456-426614174000" || winner.OwnerToken == "" {
			t.Fatalf("unexpected claim: %+v", winner)
		}
		if err := ownership.ValidateCurrent(ctx, winner.PostgresAttempt); err != nil {
			t.Fatalf("new claim not current: %v", err)
		}
		wrongWorker := winner.PostgresAttempt
		wrongWorker.WorkerID = "different-worker"
		if err := ownership.ValidateCurrent(ctx, wrongWorker); !errors.Is(err, ErrStaleAttempt) {
			t.Fatalf("wrong worker identity validation = %v, want ErrStaleAttempt", err)
		}

		// Expiry is a mutation fence only: it does not reclaim or requeue.
		mustExec(t, db, `UPDATE factory_job_ownership SET lease_until = clock_timestamp() - interval '1 second' WHERE job_id = '123e4567-e89b-42d3-a456-426614174000'`)
		if err := ownership.Heartbeat(ctx, winner.PostgresAttempt, time.Minute); !errors.Is(err, ErrStaleAttempt) {
			t.Fatalf("expired heartbeat error = %v, want ErrStaleAttempt", err)
		}
		if err := ownership.Finish(ctx, winner.PostgresAttempt, StatusSucceeded); !errors.Is(err, ErrStaleAttempt) {
			t.Fatalf("expired finish error = %v, want ErrStaleAttempt", err)
		}
		var status string
		if err := db.QueryRow(`SELECT status FROM factory_jobs WHERE id = '123e4567-e89b-42d3-a456-426614174000'`).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "running" {
			t.Fatalf("expired job status = %q, want running", status)
		}
		if _, err := ownership.ClaimNext(ctx, "worker-c", "boot-2", time.Minute); !errors.Is(err, ErrNoQueuedJobs) {
			t.Fatalf("expired running job was unexpectedly reclaimed: %v", err)
		}

		// Only an explicit external/operator requeue can make it claimable again;
		// the retained row must increment generation and fence the old handle.
		mustExec(t, db, `UPDATE factory_jobs SET status = 'queued' WHERE id = '123e4567-e89b-42d3-a456-426614174000'`)
		reclaim, err := ownership.ClaimNext(ctx, "worker-c", "boot-2", time.Minute)
		if err != nil {
			t.Fatalf("claim after explicit requeue: %v", err)
		}
		if reclaim.Generation != winner.Generation+1 {
			t.Fatalf("generation = %d, want %d", reclaim.Generation, winner.Generation+1)
		}
		if err := ownership.Finish(ctx, winner.PostgresAttempt, StatusSucceeded); !errors.Is(err, ErrStaleAttempt) {
			t.Fatalf("old handle finish error = %v, want ErrStaleAttempt", err)
		}
		if err := ownership.Heartbeat(ctx, reclaim.PostgresAttempt, time.Minute); err != nil {
			t.Fatalf("live heartbeat: %v", err)
		}
		if err := ownership.Finish(ctx, reclaim.PostgresAttempt, StatusSucceeded); err != nil {
			t.Fatalf("finish: %v", err)
		}
		if err := ownership.ValidateCurrent(ctx, reclaim.PostgresAttempt); !errors.Is(err, ErrStaleAttempt) {
			t.Fatalf("terminal attempt validation = %v, want ErrStaleAttempt", err)
		}
	})
}

type postgresProcessClaimResult struct {
	Outcome    string `json:"outcome"`
	JobID      string `json:"job_id,omitempty"`
	Generation int64  `json:"generation,omitempty"`
	WorkerID   string `json:"worker_id,omitempty"`
	BootID     string `json:"boot_id,omitempty"`
}

func TestPostgresOwnershipProcessHelper(t *testing.T) {
	if os.Getenv("FACTORY_POSTGRES_CLAIM_HELPER") != "1" {
		return
	}
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	schema := os.Getenv("FACTORY_POSTGRES_CLAIM_SCHEMA")
	jobID := os.Getenv("FACTORY_POSTGRES_CLAIM_JOB_ID")
	workerID := os.Getenv("FACTORY_POSTGRES_CLAIM_WORKER_ID")
	bootID := os.Getenv("FACTORY_POSTGRES_CLAIM_BOOT_ID")
	readyPath := os.Getenv("FACTORY_POSTGRES_CLAIM_READY_FILE")
	releasePath := os.Getenv("FACTORY_POSTGRES_CLAIM_RELEASE_FILE")
	if dsn == "" || schema == "" || jobID == "" || workerID == "" || bootID == "" || readyPath == "" || releasePath == "" {
		t.Fatal("PostgreSQL claim helper configuration is incomplete")
	}

	db := postgresDBForSchema(t, dsn, schema)
	connectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(connectCtx); err != nil {
		t.Fatalf("connect to PostgreSQL test schema: %v", err)
	}
	if err := os.WriteFile(readyPath, []byte("ready"), 0o600); err != nil {
		t.Fatalf("signal ready: %v", err)
	}
	if err := waitForPostgresBarrier(releasePath, 20*time.Second); err != nil {
		t.Fatalf("wait for claim release: %v", err)
	}

	claim, err := NewPostgresOwnership(db).ClaimNext(context.Background(), workerID, bootID, time.Minute)
	result := postgresProcessClaimResult{}
	switch {
	case err == nil:
		if claim.JobID != jobID {
			t.Fatalf("claimed job ID = %q, want %q", claim.JobID, jobID)
		}
		result = postgresProcessClaimResult{
			Outcome: "claimed", JobID: claim.JobID, Generation: claim.Generation,
			WorkerID: claim.WorkerID, BootID: claim.BootID,
		}
	case errors.Is(err, ErrNoQueuedJobs):
		result.Outcome = "ErrNoQueuedJobs"
	default:
		t.Fatalf("claim queued job: %v", err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		t.Fatalf("write claim result: %v", err)
	}
	if result.Outcome == "ErrNoQueuedJobs" {
		t.FailNow()
	}
}

func postgresOwnershipProcessClaimTest(t *testing.T, admin *sql.DB, dsn string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	schema, err := newPostgresSchemaName()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create process-claim schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop process-claim schema: %v", err)
		}
	})

	db := postgresDBForSchema(t, dsn, schema)
	jobID, err := newJobID()
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, `CREATE TABLE factory_jobs (
		id text PRIMARY KEY,
		status text NOT NULL CHECK (status IN ('queued','running','succeeded','failed','canceled')),
		queue_sequence bigint NOT NULL UNIQUE
	)`)
	if _, err := db.ExecContext(ctx, `INSERT INTO factory_jobs(id,status,queue_sequence) VALUES ($1,'queued',1)`, jobID); err != nil {
		t.Fatalf("insert process-claim job: %v", err)
	}
	if err := NewPostgresOwnership(db).Migrate(ctx); err != nil {
		t.Fatalf("migrate process-claim schema: %v", err)
	}

	barrierDir := t.TempDir()
	releasePath := filepath.Join(barrierDir, "release")
	type childProcess struct {
		workerID string
		bootID   string
		ready    string
		cmd      *exec.Cmd
		stdout   *strings.Builder
		stderr   *strings.Builder
		waitErr  error
	}
	children := make([]*childProcess, 2)
	for i, identity := range []struct{ worker, boot string }{
		{worker: "worker-process-a", boot: "boot-process-a"},
		{worker: "worker-process-b", boot: "boot-process-b"},
	} {
		child := &childProcess{
			workerID: identity.worker,
			bootID:   identity.boot,
			ready:    filepath.Join(barrierDir, identity.worker+".ready"),
			stdout:   &strings.Builder{},
			stderr:   &strings.Builder{},
		}
		child.cmd = exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPostgresOwnershipProcessHelper$")
		child.cmd.Env = append(os.Environ(),
			"FACTORY_POSTGRES_CLAIM_HELPER=1",
			"FACTORY_TEST_POSTGRES_URL="+dsn,
			"FACTORY_POSTGRES_CLAIM_SCHEMA="+schema,
			"FACTORY_POSTGRES_CLAIM_JOB_ID="+jobID,
			"FACTORY_POSTGRES_CLAIM_WORKER_ID="+child.workerID,
			"FACTORY_POSTGRES_CLAIM_BOOT_ID="+child.bootID,
			"FACTORY_POSTGRES_CLAIM_READY_FILE="+child.ready,
			"FACTORY_POSTGRES_CLAIM_RELEASE_FILE="+releasePath,
		)
		child.cmd.Stdout = child.stdout
		child.cmd.Stderr = child.stderr
		if err := child.cmd.Start(); err != nil {
			for _, started := range children[:i] {
				_ = started.cmd.Process.Kill()
				_ = started.cmd.Wait()
			}
			t.Fatalf("start claim process %d: %v", i+1, err)
		}
		children[i] = child
	}
	waitChildrenOnExit := true
	defer func() {
		if waitChildrenOnExit {
			for _, child := range children {
				if child != nil && child.cmd.ProcessState == nil {
					_ = child.cmd.Process.Kill()
					_ = child.cmd.Wait()
				}
			}
		}
	}()

	for _, child := range children {
		if err := waitForPostgresBarrierContext(ctx, child.ready); err != nil {
			t.Fatalf("wait for %s readiness: %v", child.workerID, err)
		}
	}
	if err := os.WriteFile(releasePath, []byte("claim"), 0o600); err != nil {
		t.Fatalf("release claim processes: %v", err)
	}
	for _, child := range children {
		child.waitErr = child.cmd.Wait()
	}
	waitChildrenOnExit = false

	var winner *childProcess
	for _, child := range children {
		var result postgresProcessClaimResult
		found := false
		for _, line := range strings.Split(child.stdout.String(), "\n") {
			if strings.HasPrefix(line, `{"outcome":`) {
				if err := json.Unmarshal([]byte(line), &result); err != nil {
					t.Fatalf("decode %s result: %v; stdout=%q", child.workerID, err, child.stdout.String())
				}
				found = true
			}
		}
		if !found {
			t.Fatalf("claim process %s emitted no result: stdout=%q stderr=%q", child.workerID, child.stdout.String(), child.stderr.String())
		}
		switch result.Outcome {
		case "claimed":
			if child.waitErr != nil {
				t.Fatalf("winner process %s exited unsuccessfully: %v; stderr=%q", child.workerID, child.waitErr, child.stderr.String())
			}
			if winner != nil {
				t.Fatalf("both processes claimed job %s", jobID)
			}
			if result.JobID != jobID || result.Generation != 1 || result.WorkerID != child.workerID || result.BootID != child.bootID {
				t.Fatalf("unexpected winner identity: %+v", result)
			}
			winner = child
		case "ErrNoQueuedJobs":
			var exitErr *exec.ExitError
			if !errors.As(child.waitErr, &exitErr) || exitErr.ExitCode() != 1 {
				t.Fatalf("loser process %s exit = %v, want exit code 1 for ErrNoQueuedJobs; stderr=%q", child.workerID, child.waitErr, child.stderr.String())
			}
			if result.JobID != "" || result.Generation != 0 || result.WorkerID != "" || result.BootID != "" {
				t.Fatalf("loser result included claim identity: %+v", result)
			}
		default:
			t.Fatalf("process %s reported unexpected outcome %q", child.workerID, result.Outcome)
		}
	}
	if winner == nil {
		t.Fatal("neither process claimed the queued job")
	}

	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM factory_jobs WHERE id = $1`, jobID).Scan(&status); err != nil {
		t.Fatalf("read claimed job status: %v", err)
	}
	if status != "running" {
		t.Fatalf("claimed job status = %q, want running", status)
	}
	var ownershipCount int
	var generation int64
	var workerID, bootID string
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), MIN(generation), MIN(worker_id), MIN(boot_id)
		FROM factory_job_ownership WHERE job_id = $1`, jobID).Scan(&ownershipCount, &generation, &workerID, &bootID); err != nil {
		t.Fatalf("read persisted claim: %v", err)
	}
	if ownershipCount != 1 || generation != 1 || workerID != winner.workerID || bootID != winner.bootID {
		t.Fatalf("persisted ownership=(count %d, generation %d, worker %q, boot %q), want one generation-1 row for winner %s", ownershipCount, generation, workerID, bootID, winner.workerID)
	}
}

func waitForPostgresBarrier(path string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return waitForPostgresBarrierContext(ctx, path)
}

func waitForPostgresBarrierContext(ctx context.Context, path string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func sqlOpenPGX(dsn string) (*sql.DB, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	return stdlib.OpenDB(*config), nil
}

func scopedPostgresDB(t *testing.T, admin *sql.DB, dsn string) *sql.DB {
	t.Helper()
	schema, err := newPostgresSchemaName()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	db := postgresDBForSchema(t, dsn, schema)
	t.Cleanup(func() {
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})
	return db
}

func postgresDBForSchema(t *testing.T, dsn, schema string) *sql.DB {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustExec(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	if _, err := db.Exec(statement); err != nil {
		t.Fatalf("execute %q: %v", statement, err)
	}
}

func newPostgresSchemaName() (string, error) {
	token, err := newPostgresOwnerToken()
	if err != nil {
		return "", err
	}
	return "ownership_" + token, nil
}
