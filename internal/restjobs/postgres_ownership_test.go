package restjobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
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
		for _, table := range []string{"factory_postgres_ownership_schema", "factory_job_attempt_history"} {
			if err := db.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
				t.Fatal(err)
			}
			if exists {
				t.Fatalf("failed migration left %s behind", table)
			}
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
			t.Fatalf("schema version = %d, want fresh schema v1 (%d)", version, postgresOwnershipSchemaVersion)
		}
		for _, table := range []string{"factory_postgres_ownership_schema", "factory_job_ownership", "factory_job_attempt_history"} {
			var exists bool
			if err := db1.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
				t.Fatalf("check table %s: %v", table, err)
			}
			if !exists {
				t.Fatalf("migration did not create %s", table)
			}
		}
		var historyColumns int
		if err := db1.QueryRow(`SELECT count(*) FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'factory_job_attempt_history'`).Scan(&historyColumns); err != nil {
			t.Fatal(err)
		}
		if historyColumns != 9 {
			t.Fatalf("fresh v1 attempt-history columns = %d, want 9", historyColumns)
		}
	})

	t.Run("attempt history write failures roll back claim and finish", func(t *testing.T) {
		db := scopedPostgresDB(t, admin, dsn)
		ctx := context.Background()
		const claimJobID = "123e4567-e89b-42d3-a456-426614174010"
		const finishJobID = "123e4567-e89b-42d3-a456-426614174011"
		mustExec(t, db, `CREATE TABLE factory_jobs (
			id text PRIMARY KEY,
			status text NOT NULL CHECK (status IN ('queued','running','succeeded','failed','canceled')),
			queue_sequence bigint NOT NULL UNIQUE
		)`)
		mustExec(t, db, `INSERT INTO factory_jobs(id,status,queue_sequence) VALUES
			('`+claimJobID+`','queued',1), ('`+finishJobID+`','queued',2)`)
		ownership := NewPostgresOwnership(db)
		if err := ownership.Migrate(ctx); err != nil {
			t.Fatalf("migrate: %v", err)
		}

		mustExec(t, db, `CREATE FUNCTION reject_designated_history_insert() RETURNS trigger
			LANGUAGE plpgsql AS $$ BEGIN
				IF NEW.job_id = '`+claimJobID+`' THEN
					RAISE EXCEPTION 'injected attempt-history insert failure';
				END IF;
				RETURN NEW;
			END $$`)
		mustExec(t, db, `CREATE TRIGGER reject_designated_history_insert
			BEFORE INSERT ON factory_job_attempt_history
			FOR EACH ROW EXECUTE FUNCTION reject_designated_history_insert()`)
		claim, err := ownership.ClaimNext(ctx, "worker-claim-fault", "boot-claim-fault", time.Minute)
		if err == nil || errors.Is(err, ErrNoQueuedJobs) || err.Error() != "record PostgreSQL attempt history" {
			t.Fatalf("ClaimNext() with history insert fault = (%+v, %v), want generic claim failure", claim, err)
		}
		assertPostgresJobStatus(t, db, claimJobID, "queued")
		assertPostgresOwnershipCount(t, db, claimJobID, 0)
		mustExec(t, db, `DROP TRIGGER reject_designated_history_insert ON factory_job_attempt_history`)
		mustExec(t, db, `DROP FUNCTION reject_designated_history_insert()`)

		claim, err = ownership.ClaimNext(ctx, "worker-claim-retry", "boot-claim-retry", time.Minute)
		if err != nil {
			t.Fatalf("ClaimNext() after removing history fault: %v", err)
		}
		if claim.JobID != claimJobID || claim.Generation != 1 {
			t.Fatalf("retry claim = (%s, generation %d), want designated job generation 1", claim.JobID, claim.Generation)
		}
		assertPostgresJobStatus(t, db, claimJobID, "running")
		assertPostgresOwnershipGeneration(t, db, claimJobID, 1)
		assertPostgresAttemptHistory(t, db, claimJobID, 1, claim.WorkerID, claim.BootID, claim.OwnerToken, "")
		assertPostgresHistoryCount(t, db, claimJobID, 1)

		finishClaim, err := ownership.ClaimNext(ctx, "worker-finish-fault", "boot-finish-fault", time.Minute)
		if err != nil {
			t.Fatalf("claim before finish fault: %v", err)
		}
		if finishClaim.JobID != finishJobID || finishClaim.Generation != 1 {
			t.Fatalf("finish test claim = (%s, generation %d), want designated job generation 1", finishClaim.JobID, finishClaim.Generation)
		}
		mustExec(t, db, `CREATE FUNCTION reject_designated_history_update() RETURNS trigger
			LANGUAGE plpgsql AS $$ BEGIN
				IF NEW.job_id = '`+finishJobID+`' THEN
					RAISE EXCEPTION 'injected attempt-history update failure';
				END IF;
				RETURN NEW;
			END $$`)
		mustExec(t, db, `CREATE TRIGGER reject_designated_history_update
			BEFORE UPDATE ON factory_job_attempt_history
			FOR EACH ROW EXECUTE FUNCTION reject_designated_history_update()`)
		if err := ownership.Finish(ctx, finishClaim.PostgresAttempt, StatusSucceeded); err == nil || err.Error() != "finish PostgreSQL attempt history" {
			t.Fatalf("Finish() with history update fault = %v, want generic finish failure", err)
		}
		assertPostgresJobStatus(t, db, finishJobID, "running")
		assertPostgresOwnershipGeneration(t, db, finishJobID, 1)
		assertPostgresAttemptHistory(t, db, finishJobID, 1, finishClaim.WorkerID, finishClaim.BootID, finishClaim.OwnerToken, "")
		assertPostgresHistoryCount(t, db, finishJobID, 1)
		mustExec(t, db, `DROP TRIGGER reject_designated_history_update ON factory_job_attempt_history`)
		mustExec(t, db, `DROP FUNCTION reject_designated_history_update()`)
		if err := ownership.Finish(ctx, finishClaim.PostgresAttempt, StatusSucceeded); err != nil {
			t.Fatalf("Finish() after removing history fault: %v", err)
		}
		assertPostgresJobStatus(t, db, finishJobID, string(StatusSucceeded))
		assertPostgresAttemptHistory(t, db, finishJobID, 1, finishClaim.WorkerID, finishClaim.BootID, finishClaim.OwnerToken, string(StatusSucceeded))
	})

	t.Run("concurrent claim fencing, expiry, and terminal finish", func(t *testing.T) {
		db := scopedPostgresDB(t, admin, dsn)
		ctx := context.Background()
		mustExec(t, db, `CREATE TABLE factory_jobs (
			id text PRIMARY KEY,
			status text NOT NULL CHECK (status IN ('queued','running','succeeded','failed','canceled')),
			queue_sequence bigint NOT NULL UNIQUE
		)`)
		mustExec(t, db, `INSERT INTO factory_jobs(id,status,queue_sequence) VALUES
			('123e4567-e89b-42d3-a456-426614174000','queued',1)`)
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
		var claimedHistoryRows int
		if err := db.QueryRow(`SELECT count(*) FROM factory_job_attempt_history WHERE job_id = $1`, winner.JobID).Scan(&claimedHistoryRows); err != nil {
			t.Fatal(err)
		}
		if claimedHistoryRows != 1 {
			t.Fatalf("history rows after one successful claim and one conflict = %d, want 1", claimedHistoryRows)
		}
		if err := ownership.ValidateCurrent(ctx, winner.PostgresAttempt); err != nil {
			t.Fatalf("new claim not current: %v", err)
		}
		assertPostgresAttemptHistory(t, db, winner.JobID, winner.Generation, winner.WorkerID, winner.BootID, winner.OwnerToken, "")
		wrongWorker := winner.PostgresAttempt
		wrongWorker.WorkerID = "different-worker"
		if err := ownership.ValidateCurrent(ctx, wrongWorker); !errors.Is(err, ErrStaleAttempt) {
			t.Fatalf("wrong worker identity validation = %v, want ErrStaleAttempt", err)
		}

		// Finish the first generation, then an explicit external/operator requeue
		// starts a second durable history row with a new fencing generation.
		if err := ownership.Finish(ctx, winner.PostgresAttempt, StatusFailed); err != nil {
			t.Fatalf("finish first attempt: %v", err)
		}
		assertPostgresAttemptHistory(t, db, winner.JobID, winner.Generation, winner.WorkerID, winner.BootID, winner.OwnerToken, string(StatusFailed))
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
			t.Fatalf("finish second attempt: %v", err)
		}
		if err := ownership.ValidateCurrent(ctx, reclaim.PostgresAttempt); !errors.Is(err, ErrStaleAttempt) {
			t.Fatalf("terminal attempt validation = %v, want ErrStaleAttempt", err)
		}
		assertPostgresAttemptHistory(t, db, winner.JobID, winner.Generation, winner.WorkerID, winner.BootID, winner.OwnerToken, string(StatusFailed))
		assertPostgresAttemptHistory(t, db, reclaim.JobID, reclaim.Generation, reclaim.WorkerID, reclaim.BootID, reclaim.OwnerToken, string(StatusSucceeded))
		var generations int
		if err := db.QueryRow(`SELECT count(*) FROM factory_job_attempt_history WHERE job_id = $1`, winner.JobID).Scan(&generations); err != nil {
			t.Fatal(err)
		}
		if generations != 2 {
			t.Fatalf("retained attempt generations = %d, want 2", generations)
		}

		// Lease expiry fences mutations without reclaiming or creating history.
		mustExec(t, db, `INSERT INTO factory_jobs(id,status,queue_sequence) VALUES ('123e4567-e89b-42d3-a456-426614174001','queued',2)`)
		expiring, err := ownership.ClaimNext(ctx, "worker-expiring", "boot-3", time.Minute)
		if err != nil {
			t.Fatalf("claim expiring job: %v", err)
		}
		mustExec(t, db, `UPDATE factory_job_ownership SET lease_until = clock_timestamp() - interval '1 second' WHERE job_id = '123e4567-e89b-42d3-a456-426614174001'`)
		if err := ownership.Heartbeat(ctx, expiring.PostgresAttempt, time.Minute); !errors.Is(err, ErrStaleAttempt) {
			t.Fatalf("expired heartbeat error = %v, want ErrStaleAttempt", err)
		}
		if err := ownership.Finish(ctx, expiring.PostgresAttempt, StatusSucceeded); !errors.Is(err, ErrStaleAttempt) {
			t.Fatalf("expired finish error = %v, want ErrStaleAttempt", err)
		}
		var status string
		if err := db.QueryRow(`SELECT status FROM factory_jobs WHERE id = $1`, expiring.JobID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "running" {
			t.Fatalf("expired job status = %q, want running", status)
		}
		if _, err := ownership.ClaimNext(ctx, "worker-late", "boot-4", time.Minute); !errors.Is(err, ErrNoQueuedJobs) {
			t.Fatalf("expired running job was unexpectedly reclaimed: %v", err)
		}
		var expiredHistoryCount int
		if err := db.QueryRow(`SELECT count(*) FROM factory_job_attempt_history WHERE job_id = $1`, expiring.JobID).Scan(&expiredHistoryCount); err != nil {
			t.Fatal(err)
		}
		if expiredHistoryCount != 1 {
			t.Fatalf("expired job history rows = %d, want one claim row", expiredHistoryCount)
		}

		// A history-write failure must roll back the job transition as well.
		mustExec(t, db, `INSERT INTO factory_jobs(id,status,queue_sequence) VALUES ('123e4567-e89b-42d3-a456-426614174002','queued',3)`)
		mustExec(t, db, `INSERT INTO factory_job_attempt_history
			(job_id,generation,worker_id,boot_id,claimed_at,started_at,lease_until)
			VALUES ('123e4567-e89b-42d3-a456-426614174002',1,'existing-worker','existing-boot',
			clock_timestamp(),clock_timestamp(),clock_timestamp()+interval '1 minute')`)
		if _, err := ownership.ClaimNext(ctx, "worker-conflict", "boot-5", time.Minute); err == nil {
			t.Fatal("claim with a conflicting history generation succeeded")
		}
		if err := db.QueryRow(`SELECT status FROM factory_jobs WHERE id = '123e4567-e89b-42d3-a456-426614174002'`).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "queued" {
			t.Fatalf("job status after failed claim history write = %q, want queued", status)
		}
		var ownershipRows int
		if err := db.QueryRow(`SELECT count(*) FROM factory_job_ownership WHERE job_id = '123e4567-e89b-42d3-a456-426614174002'`).Scan(&ownershipRows); err != nil {
			t.Fatal(err)
		}
		if ownershipRows != 0 {
			t.Fatalf("ownership rows after failed claim history write = %d, want 0", ownershipRows)
		}
		mustExec(t, db, `DELETE FROM factory_job_attempt_history WHERE job_id = '123e4567-e89b-42d3-a456-426614174002'`)

		finishRollback, err := ownership.ClaimNext(ctx, "worker-finish-failure", "boot-6", time.Minute)
		if err != nil {
			t.Fatalf("claim before finish rollback test: %v", err)
		}
		if _, err := db.Exec(`DELETE FROM factory_job_attempt_history WHERE job_id = $1`, finishRollback.JobID); err != nil {
			t.Fatal(err)
		}
		if err := ownership.Finish(ctx, finishRollback.PostgresAttempt, StatusFailed); err == nil {
			t.Fatal("finish without an active history row succeeded")
		}
		if err := db.QueryRow(`SELECT status FROM factory_jobs WHERE id = $1`, finishRollback.JobID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "running" {
			t.Fatalf("job status after failed finish history write = %q, want running", status)
		}
	})
}

func assertPostgresAttemptHistory(t *testing.T, db *sql.DB, jobID string, generation int64, workerID, bootID, token, wantStatus string) {
	t.Helper()
	var gotWorker, gotBoot string
	var claimedAt, startedAt, leaseUntil time.Time
	var finishedAt sql.NullTime
	var status sql.NullString
	err := db.QueryRow(`SELECT worker_id, boot_id, claimed_at, started_at, lease_until, finished_at, status
		FROM factory_job_attempt_history WHERE job_id = $1 AND generation = $2`, jobID, generation).
		Scan(&gotWorker, &gotBoot, &claimedAt, &startedAt, &leaseUntil, &finishedAt, &status)
	if err != nil {
		t.Fatalf("read attempt history for generation %d: %v", generation, err)
	}
	if gotWorker != workerID || gotBoot != bootID || claimedAt.IsZero() || startedAt.IsZero() || leaseUntil.IsZero() {
		t.Fatalf("attempt history identity/timestamps = (%q, %q, %v, %v, %v)", gotWorker, gotBoot, claimedAt, startedAt, leaseUntil)
	}
	if wantStatus == "" {
		if finishedAt.Valid || status.Valid {
			t.Fatalf("active attempt history has terminal fields: finished=%v status=%v", finishedAt, status)
		}
	} else if !finishedAt.Valid || !status.Valid || status.String != wantStatus {
		t.Fatalf("terminal attempt history = finished %v, status %v; want status %q", finishedAt, status, wantStatus)
	}
	var tokenColumnExists bool
	if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'factory_job_attempt_history' AND column_name = 'owner_token')`).Scan(&tokenColumnExists); err != nil {
		t.Fatal(err)
	}
	if tokenColumnExists {
		t.Fatal("attempt history schema contains bearer owner token column")
	}
	var tokenMatches int
	if err := db.QueryRow(`SELECT count(*) FROM factory_job_attempt_history
		WHERE to_jsonb(factory_job_attempt_history)::text LIKE '%' || $1 || '%'`, token).Scan(&tokenMatches); err != nil {
		t.Fatal(err)
	}
	if tokenMatches != 0 {
		t.Fatal("attempt history data contains bearer owner token")
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

func assertPostgresJobStatus(t *testing.T, db *sql.DB, jobID, want string) {
	t.Helper()
	var status string
	if err := db.QueryRow(`SELECT status FROM factory_jobs WHERE id = $1`, jobID).Scan(&status); err != nil {
		t.Fatalf("read status for job %s: %v", jobID, err)
	}
	if status != want {
		t.Fatalf("job %s status = %q, want %q", jobID, status, want)
	}
}

func assertPostgresOwnershipCount(t *testing.T, db *sql.DB, jobID string, want int) {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM factory_job_ownership WHERE job_id = $1`, jobID).Scan(&count); err != nil {
		t.Fatalf("count ownership rows for job %s: %v", jobID, err)
	}
	if count != want {
		t.Fatalf("job %s ownership rows = %d, want %d", jobID, count, want)
	}
}

func assertPostgresOwnershipGeneration(t *testing.T, db *sql.DB, jobID string, want int64) {
	t.Helper()
	var generation int64
	if err := db.QueryRow(`SELECT generation FROM factory_job_ownership WHERE job_id = $1`, jobID).Scan(&generation); err != nil {
		t.Fatalf("read ownership generation for job %s: %v", jobID, err)
	}
	if generation != want {
		t.Fatalf("job %s ownership generation = %d, want %d", jobID, generation, want)
	}
}

func assertPostgresHistoryCount(t *testing.T, db *sql.DB, jobID string, want int) {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM factory_job_attempt_history WHERE job_id = $1`, jobID).Scan(&count); err != nil {
		t.Fatalf("count attempt history rows for job %s: %v", jobID, err)
	}
	if count != want {
		t.Fatalf("job %s attempt history rows = %d, want %d", jobID, count, want)
	}
}

func newPostgresSchemaName() (string, error) {
	token, err := newPostgresOwnerToken()
	if err != nil {
		return "", err
	}
	return "ownership_" + token, nil
}
