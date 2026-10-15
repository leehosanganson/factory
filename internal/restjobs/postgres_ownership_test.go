package restjobs

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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

	t.Run("independent processes fence a stale generation after explicit requeue", func(t *testing.T) {
		postgresOwnershipProcessFencingTest(t, admin, dsn)
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
	mode := os.Getenv("FACTORY_POSTGRES_CLAIM_MODE")
	if mode == "" {
		mode = "compete"
	}
	if mode != "compete" && mode != "fence" {
		t.Fatalf("unknown PostgreSQL claim helper mode %q", mode)
	}
	if dsn == "" || schema == "" || jobID == "" || workerID == "" || bootID == "" || readyPath == "" || releasePath == "" {
		t.Fatal("PostgreSQL claim helper configuration is incomplete")
	}

	db := postgresDBForSchema(t, dsn, schema)
	connectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(connectCtx); err != nil {
		t.Fatalf("connect to PostgreSQL test schema: %v", err)
	}
	if mode == "fence" {
		var backendPID int
		if err := db.QueryRowContext(connectCtx, `SELECT pg_backend_pid()`).Scan(&backendPID); err != nil {
			t.Fatalf("read PostgreSQL backend identity: %v", err)
		}
		if err := os.WriteFile(readyPath, []byte(strconv.Itoa(backendPID)), 0o600); err != nil {
			t.Fatalf("signal ready: %v", err)
		}
	} else if err := os.WriteFile(readyPath, []byte("ready"), 0o600); err != nil {
		t.Fatalf("signal ready: %v", err)
	}
	if mode == "compete" {
		if err := waitForPostgresBarrier(releasePath, 20*time.Second); err != nil {
			t.Fatalf("wait for claim release: %v", err)
		}
	}

	opCtx, opCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer opCancel()
	claim, err := NewPostgresOwnership(db).ClaimNext(opCtx, workerID, bootID, time.Minute)
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
	if mode == "fence" {
		if err := waitForPostgresBarrier(releasePath, 20*time.Second); err != nil {
			t.Fatalf("wait for finish release: %v", err)
		}
		finishErr := NewPostgresOwnership(db).Finish(opCtx, claim.PostgresAttempt, StatusSucceeded)
		switch {
		case errors.Is(finishErr, ErrStaleAttempt):
			result.Outcome = "stale_attempt"
		case finishErr == nil:
			result.Outcome = "finished"
		default:
			t.Fatalf("finish claimed job: %v", finishErr)
		}
		if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
			t.Fatalf("write finish result: %v", err)
		}
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

func postgresOwnershipProcessFencingTest(t *testing.T, admin *sql.DB, dsn string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	schema, err := newPostgresSchemaName()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create process-fencing schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop process-fencing schema: %v", err)
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
		t.Fatalf("insert process-fencing job: %v", err)
	}
	if err := NewPostgresOwnership(db).Migrate(ctx); err != nil {
		t.Fatalf("migrate process-fencing schema: %v", err)
	}
	var parentBackendPID int
	if err := db.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&parentBackendPID); err != nil {
		t.Fatalf("read parent PostgreSQL backend identity: %v", err)
	}

	type childProcess struct {
		workerID   string
		bootID     string
		ready      string
		release    string
		cmd        *exec.Cmd
		lines      chan string
		stderrPath string
		stderrFile *os.File
		waitErr    error
	}
	barrierDir := t.TempDir()
	children := make([]*childProcess, 0, 2)
	startChild := func(workerID, bootID string) *childProcess {
		t.Helper()
		child := &childProcess{
			workerID:   workerID,
			bootID:     bootID,
			ready:      filepath.Join(barrierDir, workerID+".ready"),
			release:    filepath.Join(barrierDir, workerID+".release"),
			lines:      make(chan string, 4),
			stderrPath: filepath.Join(barrierDir, workerID+".stderr"),
		}
		stderrFile, err := os.Create(child.stderrPath)
		if err != nil {
			t.Fatalf("create %s stderr capture: %v", workerID, err)
		}
		child.stderrFile = stderrFile
		child.cmd = exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPostgresOwnershipProcessHelper$")
		child.cmd.Env = append(os.Environ(),
			"FACTORY_POSTGRES_CLAIM_HELPER=1",
			"FACTORY_POSTGRES_CLAIM_MODE=fence",
			"FACTORY_TEST_POSTGRES_URL="+dsn,
			"FACTORY_POSTGRES_CLAIM_SCHEMA="+schema,
			"FACTORY_POSTGRES_CLAIM_JOB_ID="+jobID,
			"FACTORY_POSTGRES_CLAIM_WORKER_ID="+workerID,
			"FACTORY_POSTGRES_CLAIM_BOOT_ID="+bootID,
			"FACTORY_POSTGRES_CLAIM_READY_FILE="+child.ready,
			"FACTORY_POSTGRES_CLAIM_RELEASE_FILE="+child.release,
		)
		stdout, err := child.cmd.StdoutPipe()
		if err != nil {
			t.Fatalf("open %s stdout: %v", workerID, err)
		}
		child.cmd.Stderr = child.stderrFile
		if err := child.cmd.Start(); err != nil {
			_ = child.stderrFile.Close()
			t.Fatalf("start %s process: %v", workerID, err)
		}
		children = append(children, child)
		go func() {
			defer close(child.lines)
			reader := bufio.NewReader(stdout)
			for {
				line, err := reader.ReadString('\n')
				if len(line) > 0 {
					child.lines <- strings.TrimSpace(line)
				}
				if err != nil {
					return
				}
			}
		}()
		return child
	}
	defer func() {
		for _, child := range children {
			if child.cmd.ProcessState == nil {
				_ = child.cmd.Process.Kill()
				_ = child.cmd.Wait()
			}
			_ = child.stderrFile.Close()
		}
	}()
	stderr := func(child *childProcess) string {
		t.Helper()
		contents, err := os.ReadFile(child.stderrPath)
		if err != nil {
			return "<stderr unavailable: " + err.Error() + ">"
		}
		return string(contents)
	}
	readLine := func(child *childProcess, stage string) postgresProcessClaimResult {
		t.Helper()
		select {
		case line, ok := <-child.lines:
			for ok && !strings.HasPrefix(line, `{"outcome":`) {
				line, ok = <-child.lines
			}
			if !ok {
				t.Fatalf("process %s exited before %s; stderr=%q", child.workerID, stage, stderr(child))
			}
			if strings.Contains(strings.ToLower(line), "owner_token") || strings.Contains(line, "0123456789abcdef") {
				t.Fatalf("%s output leaked ownership token: %q", child.workerID, line)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal([]byte(line), &fields); err != nil {
				t.Fatalf("decode %s %s output fields: %v; line=%q", child.workerID, stage, err, line)
			}
			for field := range fields {
				switch field {
				case "outcome", "job_id", "generation", "worker_id", "boot_id":
				default:
					t.Fatalf("%s output includes unexpected field %q: %s", child.workerID, field, line)
				}
			}
			var result postgresProcessClaimResult
			if err := json.Unmarshal([]byte(line), &result); err != nil {
				t.Fatalf("decode %s %s result: %v; line=%q", child.workerID, stage, err, line)
			}
			return result
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s %s output: %v", child.workerID, stage, ctx.Err())
			return postgresProcessClaimResult{}
		}
	}
	waitChild := func(child *childProcess) {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- child.cmd.Wait() }()
		select {
		case child.waitErr = <-done:
			if child.waitErr != nil {
				t.Fatalf("%s process failed: %v; stderr=%q", child.workerID, child.waitErr, stderr(child))
			}
		case <-ctx.Done():
			_ = child.cmd.Process.Kill()
			child.waitErr = <-done
			t.Fatalf("timed out waiting for %s process exit: %v", child.workerID, ctx.Err())
		}
	}
	backendPID := func(child *childProcess) int {
		t.Helper()
		if err := waitForPostgresBarrierContext(ctx, child.ready); err != nil {
			t.Fatalf("wait for %s readiness: %v", child.workerID, err)
		}
		contents, err := os.ReadFile(child.ready)
		if err != nil {
			t.Fatalf("read %s backend identity: %v", child.workerID, err)
		}
		pid, err := strconv.Atoi(string(contents))
		if err != nil {
			t.Fatalf("parse %s backend identity %q: %v", child.workerID, contents, err)
		}
		if pid == parentBackendPID {
			t.Fatalf("%s unexpectedly shares parent PostgreSQL backend %d", child.workerID, pid)
		}
		return pid
	}
	assertClaim := func(child *childProcess, result postgresProcessClaimResult, generation int64) {
		t.Helper()
		if result.Outcome != "claimed" || result.JobID != jobID || result.Generation != generation || result.WorkerID != child.workerID || result.BootID != child.bootID {
			t.Fatalf("%s claim = %+v, want job %s generation %d", child.workerID, result, jobID, generation)
		}
	}

	processA := startChild("worker-process-a", "boot-process-a")
	pidA := backendPID(processA)
	assertClaim(processA, readLine(processA, "claim"), 1)
	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM factory_jobs WHERE id = $1`, jobID).Scan(&status); err != nil {
		t.Fatalf("read committed A claim status: %v", err)
	}
	if status != "running" {
		t.Fatalf("A claim status = %q, want committed running", status)
	}

	// This is an explicit test-operator requeue after A's committed claim. It
	// deliberately does not simulate recovery or establish that process A stopped.
	result, err := db.ExecContext(ctx, `UPDATE factory_jobs SET status = 'queued' WHERE id = $1 AND status = 'running'`, jobID)
	if err != nil {
		t.Fatalf("operator-style test requeue after A claim: %v", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		t.Fatalf("operator-style test requeue affected %d rows, error %v; want one", changed, err)
	}

	processB := startChild("worker-process-b", "boot-process-b")
	pidB := backendPID(processB)
	if pidA == pidB {
		t.Fatalf("processes A and B share PostgreSQL backend %d", pidA)
	}
	assertClaim(processB, readLine(processB, "claim"), 2)

	if err := os.WriteFile(processA.release, []byte("finish"), 0o600); err != nil {
		t.Fatalf("release process A finish: %v", err)
	}
	staleFinish := readLine(processA, "finish")
	if staleFinish.Outcome != "stale_attempt" || staleFinish.JobID != jobID || staleFinish.Generation != 1 || staleFinish.WorkerID != processA.workerID || staleFinish.BootID != processA.bootID {
		t.Fatalf("process A finish result = %+v, want ErrStaleAttempt for generation 1", staleFinish)
	}
	waitChild(processA)

	var bCurrent bool
	if err := db.QueryRowContext(ctx, `SELECT j.status = 'running' AND o.generation = 2
		AND o.worker_id = $2 AND o.boot_id = $3 AND o.lease_until > clock_timestamp()
		FROM factory_jobs j JOIN factory_job_ownership o ON o.job_id = j.id WHERE j.id = $1`,
		jobID, processB.workerID, processB.bootID).Scan(&bCurrent); err != nil {
		t.Fatalf("verify B remains current after stale A finish: %v", err)
	}
	if !bCurrent {
		t.Fatal("B is no longer current or job is not running after stale A finish")
	}

	if err := os.WriteFile(processB.release, []byte("finish"), 0o600); err != nil {
		t.Fatalf("release process B finish: %v", err)
	}
	bFinish := readLine(processB, "finish")
	if bFinish.Outcome != "finished" || bFinish.JobID != jobID || bFinish.Generation != 2 || bFinish.WorkerID != processB.workerID || bFinish.BootID != processB.bootID {
		t.Fatalf("process B finish result = %+v, want successful generation-2 finish", bFinish)
	}
	waitChild(processB)

	var ownershipCount int
	var generation int64
	var workerID, bootID string
	if err := db.QueryRowContext(ctx, `SELECT j.status, COUNT(o.job_id), MIN(o.generation), MIN(o.worker_id), MIN(o.boot_id)
		FROM factory_jobs j JOIN factory_job_ownership o ON o.job_id = j.id WHERE j.id = $1 GROUP BY j.status`, jobID).
		Scan(&status, &ownershipCount, &generation, &workerID, &bootID); err != nil {
		t.Fatalf("read terminal process-fencing state: %v", err)
	}
	if status != string(StatusSucceeded) || ownershipCount != 1 || generation != 2 || workerID != processB.workerID || bootID != processB.bootID {
		t.Fatalf("terminal state=(status %q, ownership rows %d, generation %d, worker %q, boot %q), want succeeded with one generation-2 B row", status, ownershipCount, generation, workerID, bootID)
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
