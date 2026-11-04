package restjobs

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSQLiteWorkerSessionRegistrationRotatesShortLivedCapability(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "session-capability.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	first, replay, err := store.RegisterWorkerSession(ctx, "worker-capability", "boot-capability")
	if err != nil || replay || first.Capability == "" || first.CapabilityExpiresAt == nil {
		t.Fatalf("first registration=(%+v,%v,%v)", first, replay, err)
	}
	if got := first.CapabilityExpiresAt.Sub(first.CreatedAt); got < WorkerSessionCapabilityTTL-time.Second || got > WorkerSessionCapabilityTTL+time.Second {
		t.Fatalf("capability lifetime=%s, want %s", got, WorkerSessionCapabilityTTL)
	}
	if got, err := store.AuthenticateWorkerCapability(ctx, "worker-capability", first.Capability); err != nil || got.ID != first.ID {
		t.Fatalf("authenticate first capability=(%+v,%v)", got, err)
	}
	second, replay, err := store.RegisterWorkerSession(ctx, "worker-capability", "boot-capability")
	if err != nil || !replay || second.ID != first.ID || second.Capability == first.Capability {
		t.Fatalf("same-boot rotation=(%+v,%v,%v)", second, replay, err)
	}
	if _, err := store.AuthenticateWorkerCapability(ctx, "worker-capability", first.Capability); !errors.Is(err, ErrNotFound) {
		t.Fatalf("superseded capability error=%v, want invalid", err)
	}
	if got, err := store.AuthenticateWorkerCapability(ctx, "worker-capability", second.Capability); err != nil || got.ID != second.ID {
		t.Fatalf("authenticate replacement capability=(%+v,%v)", got, err)
	}
	if _, err := store.db.Exec(`UPDATE factory_worker_sessions SET capability_expires_at=? WHERE id=?`, time.Now().UTC().Add(-time.Second).Format("2006-01-02T15:04:05.000000000Z07:00"), first.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateWorkerCapability(ctx, "worker-capability", second.Capability); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired capability error=%v, want invalid", err)
	}
	var stored string
	if err := store.db.QueryRow(`SELECT capability_hash FROM factory_worker_sessions WHERE id=?`, first.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stored, first.Capability) || strings.Contains(stored, second.Capability) {
		t.Fatal("session store contains a raw capability")
	}
}

func TestSQLiteWorkerSessionCapabilityAuditFailuresRollBackIssuanceAndRotation(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "session-capability-audit-failure.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	if _, err := store.db.Exec(`CREATE TRIGGER reject_capability_issuance BEFORE INSERT ON factory_worker_session_audit WHEN NEW.event='capability_issued' BEGIN SELECT RAISE(ABORT,'injected audit failure'); END`); err != nil {
		t.Fatal(err)
	}
	const issuePrincipal = "worker-failed-issuance"
	if _, _, err := store.RegisterWorkerSession(ctx, issuePrincipal, "boot-issuance"); err == nil {
		t.Fatal("capability issuance succeeded despite injected audit-write failure")
	}
	var sessions, auditRows int
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_sessions WHERE principal_id=?`, issuePrincipal).Scan(&sessions); err != nil || sessions != 0 {
		t.Fatalf("failed issuance left %d session rows (query error %v), want none", sessions, err)
	}
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_session_audit WHERE principal_id=?`, issuePrincipal).Scan(&auditRows); err != nil || auditRows != 0 {
		t.Fatalf("failed issuance left %d audit rows (query error %v), want none", auditRows, err)
	}
	if _, err := store.db.Exec(`DROP TRIGGER reject_capability_issuance`); err != nil {
		t.Fatal(err)
	}

	const rotationPrincipal = "worker-failed-rotation"
	prior, replay, err := store.RegisterWorkerSession(ctx, rotationPrincipal, "boot-rotation")
	if err != nil || replay {
		t.Fatalf("initial registration=(%+v,%v,%v)", prior, replay, err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER reject_capability_rotation BEFORE INSERT ON factory_worker_session_audit WHEN NEW.event='capability_rotated' BEGIN SELECT RAISE(ABORT,'injected audit failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, rotationPrincipal, "boot-rotation"); err == nil {
		t.Fatal("capability rotation succeeded despite injected audit-write failure")
	}
	if got, err := store.AuthenticateWorkerCapability(ctx, rotationPrincipal, prior.Capability); err != nil || got.ID != prior.ID {
		t.Fatalf("failed rotation did not preserve the prior valid capability: session=%+v err=%v", got, err)
	}
	var storedHash string
	if err := store.db.QueryRow(`SELECT capability_hash FROM factory_worker_sessions WHERE id=?`, prior.ID).Scan(&storedHash); err != nil {
		t.Fatal(err)
	}
	priorHash := sha256.Sum256([]byte(prior.Capability))
	if storedHash != hex.EncodeToString(priorHash[:]) {
		t.Fatal("failed rotation changed the stored capability")
	}
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_session_audit WHERE session_id=? AND event='capability_rotated'`, prior.ID).Scan(&auditRows); err != nil || auditRows != 0 {
		t.Fatalf("failed rotation left %d rotation audit rows (query error %v), want none", auditRows, err)
	}
}

func TestSQLiteWorkerSessionsAreAtomicIdempotentRetirableAndAudited(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "sessions.db")
	store, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	const contenders = 16
	var wg sync.WaitGroup
	results := make(chan WorkerSession, contenders)
	errs := make(chan error, contenders)
	for range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			session, replay, err := store.RegisterWorkerSession(ctx, "worker-1", "boot-1")
			if err == nil && replay && session.ID == "" {
				err = errors.New("idempotent registration returned no identity")
			}
			if err != nil {
				errs <- err
				return
			}
			results <- session
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent registration: %v", err)
	}
	var session WorkerSession
	for candidate := range results {
		if session.ID == "" {
			session = candidate
		} else if candidate.ID != session.ID {
			t.Fatalf("concurrent registrations created %q and %q", session.ID, candidate.ID)
		}
	}
	if session.ID == "" {
		t.Fatal("no session registered")
	}
	replay, isReplay, err := store.RegisterWorkerSession(ctx, "worker-1", "boot-1")
	if err != nil || !isReplay || replay.ID != session.ID {
		t.Fatalf("same-boot retry=(%+v,%v,%v), want same session replay", replay, isReplay, err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, "worker-1", "boot-2"); !errors.Is(err, ErrWorkerSessionConflict) {
		t.Fatalf("different boot registration error=%v, want conflict", err)
	}
	retired, err := store.RetireWorkerSession(ctx, "worker-1", "boot-1", session.ID)
	if err != nil || retired.Status != "retired" || retired.RetiredAt == nil {
		t.Fatalf("retirement=(%+v,%v)", retired, err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, "worker-1", "boot-1"); !errors.Is(err, ErrWorkerSessionConflict) {
		t.Fatalf("re-register retired principal/boot pair error=%v, want conflict", err)
	}
	next, nextReplay, err := store.RegisterWorkerSession(ctx, "worker-1", "boot-2")
	if err != nil || nextReplay || next.ID == session.ID {
		t.Fatalf("post-retirement registration=(%+v,%v,%v)", next, replay, err)
	}
	var auditCount int
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_session_audit WHERE event IN ('registered', 'retired')`).Scan(&auditCount); err != nil || auditCount != 3 {
		t.Fatalf("registration/retirement audit events=%d err=%v; want registered, retired, registered", auditCount, err)
	}
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_session_audit WHERE event IN ('capability_issued', 'capability_rotated')`).Scan(&auditCount); err != nil || auditCount < 3 {
		t.Fatalf("capability audit events=%d err=%v; want issuance and retries recorded", auditCount, err)
	}
	var digest, expiresAt string
	if err := store.db.QueryRow(`SELECT capability_hash,capability_expires_at FROM factory_worker_session_audit WHERE session_id=? AND event='capability_issued'`, session.ID).Scan(&digest, &expiresAt); err != nil {
		t.Fatal(err)
	}
	firstDigest := sha256.Sum256([]byte(session.Capability))
	if digest != hex.EncodeToString(firstDigest[:]) || expiresAt == "" || strings.Contains(digest, session.Capability) {
		t.Fatal("issuance audit does not contain the matching digest and expiry")
	}
	rotatedDigest := sha256.Sum256([]byte(replay.Capability))
	var rotationCount int
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_session_audit WHERE session_id=? AND event='capability_rotated' AND capability_hash=? AND capability_expires_at IS NOT NULL`, session.ID, hex.EncodeToString(rotatedDigest[:])).Scan(&rotationCount); err != nil || rotationCount != 1 {
		t.Fatalf("rotation audit records for replacement digest=%d err=%v, want 1", rotationCount, err)
	}
	if _, err := store.db.Exec(`UPDATE factory_worker_session_audit SET event='tampered'`); err == nil {
		t.Fatal("audit update succeeded despite append-only trigger")
	}
	if _, err := store.db.Exec(`DELETE FROM factory_worker_session_audit`); err == nil {
		t.Fatal("audit delete succeeded despite append-only trigger")
	}
}

func TestSQLiteWorkerSessionSurvivesStoreReopen(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "sessions-reopen.db")
	store, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	created, replay, err := store.RegisterWorkerSession(context.Background(), "worker-reopen", "boot-reopen")
	if err != nil || replay {
		t.Fatalf("register=(%+v,%v,%v)", created, replay, err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, replay, err := reopened.RegisterWorkerSession(context.Background(), "worker-reopen", "boot-reopen")
	if err != nil || !replay || got.ID != created.ID {
		t.Fatalf("reopen retry=(%+v,%v,%v), want persisted session", got, replay, err)
	}
}

func TestSQLiteWorkerSessionConcurrentSameBootRegistrationIsIdempotent(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "sessions-same-boot-race.db")
	first, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	stores := []*SQLiteStore{first, second}
	var wg sync.WaitGroup
	results := make(chan struct {
		session WorkerSession
		replay  bool
		err     error
	}, len(stores))
	for _, candidate := range stores {
		wg.Add(1)
		go func(candidate *SQLiteStore) {
			defer wg.Done()
			session, replay, err := candidate.RegisterWorkerSession(context.Background(), "worker-same", "boot-same")
			results <- struct {
				session WorkerSession
				replay  bool
				err     error
			}{session, replay, err}
		}(candidate)
	}
	wg.Wait()
	close(results)
	var id string
	for result := range results {
		if result.err != nil {
			t.Fatalf("same-boot concurrent registration: %v", result.err)
		}
		if id == "" {
			id = result.session.ID
		} else if id != result.session.ID {
			t.Fatalf("same-boot registrations returned %q and %q", id, result.session.ID)
		}
	}
	if id == "" {
		t.Fatal("concurrent registration created no session")
	}
}

func TestSQLiteWorkerSessionConcurrentDifferentBootHasOneWinner(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "sessions-race.db")
	first, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	stores := []*SQLiteStore{first, second}
	var wg sync.WaitGroup
	results := make(chan error, len(stores))
	for i, candidate := range stores {
		wg.Add(1)
		go func(i int, candidate *SQLiteStore) {
			defer wg.Done()
			_, _, err := candidate.RegisterWorkerSession(context.Background(), "worker-race", fmt.Sprintf("boot-%d", i))
			results <- err
		}(i, candidate)
	}
	wg.Wait()
	close(results)
	successes, conflicts := 0, 0
	for err := range results {
		if err == nil {
			successes++
		} else if errors.Is(err, ErrWorkerSessionConflict) {
			conflicts++
		} else {
			t.Errorf("registration error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d; want one each", successes, conflicts)
	}
}

func TestWorkerSessionStoreRejectsInvalidIdentityAndUnknownRetirement(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "sessions-invalid.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, pair := range [][2]string{{"bad/id", "boot"}, {"worker", "bad/boot"}, {"", "boot"}} {
		if _, _, err := store.RegisterWorkerSession(context.Background(), pair[0], pair[1]); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("RegisterWorkerSession(%q,%q) error=%v", pair[0], pair[1], err)
		}
	}
	if _, err := store.RetireWorkerSession(context.Background(), "worker", "boot", "unknown"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown retirement error=%v", err)
	}
}

func TestSQLiteWorkerSessionAndWebhookMigrationsUpgradeFromV14(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "pre-v15.db")
	dsn, err := sqliteDSN(path)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE factory_schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	files, err := MigrationNames()
	if err != nil {
		t.Fatal(err)
	}
	for index, name := range files[:14] {
		payload, err := migrationFiles.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(payload)); err != nil {
			t.Fatalf("apply SQLite migration %s: %v", name, err)
		}
		if _, err := db.Exec(`INSERT INTO factory_schema_migrations(version,name,applied_at) VALUES(?,?,?)`, index+1, filepath.Base(name), "2026-01-01T00:00:00Z"); err != nil {
			t.Fatal(err)
		}
	}
	const legacyCreatedAt = "2025-05-06T07:08:09Z"
	const legacyRequest = `{"repository":"legacy-widget","task":"preserve before migration"}`
	const memberRequest = `{"repository":"legacy-widget","task":"preserve thread member"}`
	for _, job := range []struct {
		id, request string
		sequence    int
	}{
		{"legacy-job-1", legacyRequest, 1},
		{"legacy-job-2", memberRequest, 2},
	} {
		if _, err := db.Exec(`INSERT INTO factory_jobs(id,request_json,status,created_at,updated_at,queue_sequence) VALUES(?,?,'queued',?,?,?)`, job.id, job.request, legacyCreatedAt, legacyCreatedAt, job.sequence); err != nil {
			t.Fatalf("seed pre-upgrade SQLite job %s: %v", job.id, err)
		}
	}
	if _, err := db.Exec(`INSERT INTO factory_threads(id,submitter,created_at,initial_job_id) VALUES(?,?,?,?)`, "legacy-thread", "legacy-shared", legacyCreatedAt, "legacy-job-1"); err != nil {
		t.Fatalf("seed pre-upgrade SQLite Thread: %v", err)
	}
	for position, jobID := range []string{"legacy-job-1", "legacy-job-2"} {
		if _, err := db.Exec(`INSERT INTO factory_thread_jobs(thread_id,job_id,position,created_at) VALUES(?,?,?,?)`, "legacy-thread", jobID, position+1, legacyCreatedAt); err != nil {
			t.Fatalf("seed pre-upgrade SQLite Thread membership for %s: %v", jobID, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	var version int
	if err := store.db.QueryRow(`SELECT MAX(version) FROM factory_schema_migrations`).Scan(&version); err != nil || version != 18 {
		t.Fatalf("upgraded SQLite schema version=%d err=%v; want 18", version, err)
	}
	var retainedAudit int
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_session_audit WHERE event IN ('registered', 'retired')`).Scan(&retainedAudit); err != nil || retainedAudit != 0 {
		t.Fatalf("unexpected legacy session audit rows after migration: count=%d err=%v", retainedAudit, err)
	}
	if _, replay, err := store.RegisterWorkerSession(context.Background(), "legacy-worker", "post-upgrade-boot"); err != nil || replay {
		t.Fatalf("register against migrated SQLite schema: replay=%v err=%v", replay, err)
	}
	assertUpgradedLegacyRecords(t, store)
	body := sha256.Sum256([]byte("pre-v15 SQLite webhook"))
	if _, replay, err := store.AdmitGitHubIssueWebhook(context.Background(), "post-upgrade-delivery", fmt.Sprintf("%x", body), "issue-owner", Request{Repository: "widget", Task: "migrated webhook"}); err != nil || replay {
		t.Fatalf("webhook admission against migrated SQLite schema: replay=%v err=%v", replay, err)
	}
}

func TestPostgresWorkerSessionAndWebhookMigrationsUpgradeFromV10(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL migration upgrade integration")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	db := scopedPostgresDB(t, admin, dsn)
	files, err := postgresMigrationNames()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE factory_rest_schema_migrations (version integer PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		t.Fatal(err)
	}
	for index, name := range files[:10] {
		payload, err := postgresMigrationFiles.ReadFile("postgres_migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(payload)); err != nil {
			t.Fatalf("apply PostgreSQL migration %s: %v", name, err)
		}
		if _, err := db.Exec(`INSERT INTO factory_rest_schema_migrations(version,name) VALUES($1,$2)`, index+1, name); err != nil {
			t.Fatal(err)
		}
	}
	var startingVersion int
	if err := db.QueryRow(`SELECT MAX(version) FROM factory_rest_schema_migrations`).Scan(&startingVersion); err != nil || startingVersion != 10 {
		t.Fatalf("starting PostgreSQL schema version=%d err=%v; want 10", startingVersion, err)
	}
	const legacyCreatedAt = "2025-05-06T07:08:09Z"
	const legacyRequest = `{"repository":"legacy-widget","task":"preserve before migration"}`
	const memberRequest = `{"repository":"legacy-widget","task":"preserve thread member"}`
	for _, job := range []struct {
		id, request string
		sequence    int64
	}{
		{"legacy-job-1", legacyRequest, 1},
		{"legacy-job-2", memberRequest, 2},
	} {
		if _, err := db.Exec(`INSERT INTO factory_jobs(id,request_json,status,created_at,updated_at,queue_sequence) VALUES($1,$2,'queued',$3,$3,$4)`, job.id, job.request, legacyCreatedAt, job.sequence); err != nil {
			t.Fatalf("seed pre-upgrade PostgreSQL job %s: %v", job.id, err)
		}
	}
	if _, err := db.Exec(`INSERT INTO factory_threads(id,submitter,created_at,initial_job_id) VALUES($1,$2,$3,$4)`, "legacy-thread", "legacy-shared", legacyCreatedAt, "legacy-job-1"); err != nil {
		t.Fatalf("seed pre-upgrade PostgreSQL Thread: %v", err)
	}
	for position, jobID := range []string{"legacy-job-1", "legacy-job-2"} {
		if _, err := db.Exec(`INSERT INTO factory_thread_jobs(thread_id,job_id,position,created_at) VALUES($1,$2,$3,$4)`, "legacy-thread", jobID, position+1, legacyCreatedAt); err != nil {
			t.Fatalf("seed pre-upgrade PostgreSQL Thread membership for %s: %v", jobID, err)
		}
	}
	if _, err := db.Exec(`SELECT setval('factory_rest_job_sequence', 100)`); err != nil {
		t.Fatalf("advance PostgreSQL queue sequence past seeded legacy Jobs: %v", err)
	}
	store, err := NewPostgresStore(db, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.CloseStore() })
	var version int
	if err := db.QueryRow(`SELECT MAX(version) FROM factory_rest_schema_migrations`).Scan(&version); err != nil || version != 14 {
		t.Fatalf("upgraded PostgreSQL schema version=%d err=%v; want 14", version, err)
	}
	var retainedAudit int
	if err := db.QueryRow(`SELECT count(*) FROM factory_worker_session_audit WHERE event IN ('registered', 'retired')`).Scan(&retainedAudit); err != nil || retainedAudit != 0 {
		t.Fatalf("unexpected legacy session audit rows after migration: count=%d err=%v", retainedAudit, err)
	}
	if _, replay, err := store.RegisterWorkerSession(context.Background(), "legacy-worker", "post-upgrade-boot"); err != nil || replay {
		t.Fatalf("register against migrated PostgreSQL schema: replay=%v err=%v", replay, err)
	}
	assertUpgradedLegacyRecords(t, store)
	body := sha256.Sum256([]byte("pre-v11 PostgreSQL webhook"))
	if _, replay, err := store.AdmitGitHubIssueWebhook(context.Background(), "post-upgrade-delivery", fmt.Sprintf("%x", body), "issue-owner", Request{Repository: "widget", Task: "migrated webhook"}); err != nil || replay {
		t.Fatalf("webhook admission against migrated PostgreSQL schema: replay=%v err=%v", replay, err)
	}
}

func assertUpgradedLegacyRecords(t *testing.T, store interface {
	Get(string) (Snapshot, error)
	GetThread(string) (ThreadSnapshot, error)
}) {
	t.Helper()
	for _, want := range []struct {
		id, task string
	}{
		{"legacy-job-1", "preserve before migration"},
		{"legacy-job-2", "preserve thread member"},
	} {
		got, err := store.Get(want.id)
		if err != nil {
			t.Fatalf("read pre-upgrade Job %s after migration: %v", want.id, err)
		}
		if got.ID != want.id || got.Request.Repository != "legacy-widget" || got.Request.Task != want.task || got.Status != StatusQueued || got.Submitter != legacySharedPrincipalID {
			t.Errorf("pre-upgrade Job %s after migration = %+v; content, status, and legacy attribution must survive", want.id, got)
		}
	}
	thread, err := store.GetThread("legacy-thread")
	if err != nil {
		t.Fatalf("read pre-upgrade Thread after migration: %v", err)
	}
	if thread.Thread.ID != "legacy-thread" || thread.Thread.Submitter != legacySharedPrincipalID || len(thread.Thread.JobIDs) != 2 || thread.Thread.JobIDs[0] != "legacy-job-1" || thread.Thread.JobIDs[1] != "legacy-job-2" || len(thread.Jobs) != 2 {
		t.Fatalf("pre-upgrade Thread after migration = %+v; thread and ordered membership must survive", thread)
	}
	if thread.Jobs[0].Request.Task != "preserve before migration" || thread.Jobs[1].Request.Task != "preserve thread member" {
		t.Fatalf("pre-upgrade Thread member content after migration = %+v; member Jobs must remain unchanged", thread.Jobs)
	}
}

func TestPostgresWorkerSessionIntegration(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL worker session integration")
	}
	ctx := context.Background()
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	db := scopedPostgresDB(t, admin, dsn)
	store, err := NewPostgresStore(db, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	id, err := newJobID()
	if err != nil {
		t.Fatal(err)
	}
	principal := "test-worker-" + id
	created, replay, err := store.RegisterWorkerSession(ctx, principal, "boot-one")
	if err != nil || replay {
		t.Fatalf("register=(%+v,%v,%v)", created, replay, err)
	}
	if _, err := db.ExecContext(ctx, `CREATE FUNCTION reject_worker_capability_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected worker capability audit failure'; END; $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER reject_worker_capability_rotation BEFORE INSERT ON factory_worker_session_audit FOR EACH ROW WHEN (NEW.event = 'capability_rotated') EXECUTE FUNCTION reject_worker_capability_audit()`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, principal, "boot-one"); err == nil {
		t.Fatal("PostgreSQL capability rotation succeeded despite audit-write failure")
	}
	if _, err := store.AuthenticateWorkerCapability(ctx, principal, created.Capability); err != nil {
		t.Fatalf("failed PostgreSQL rotation invalidated previous capability: %v", err)
	}
	var failedRotationAudits int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM factory_worker_session_audit WHERE session_id=$1 AND event='capability_rotated'`, created.ID).Scan(&failedRotationAudits); err != nil || failedRotationAudits != 0 {
		t.Fatalf("failed PostgreSQL rotation audit rows=%d err=%v, want 0", failedRotationAudits, err)
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER reject_worker_capability_rotation ON factory_worker_session_audit`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER reject_worker_capability_issuance BEFORE INSERT ON factory_worker_session_audit FOR EACH ROW WHEN (NEW.event = 'capability_issued') EXECUTE FUNCTION reject_worker_capability_audit()`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, principal+"-failed-issue", "boot-one"); err == nil {
		t.Fatal("PostgreSQL capability issuance succeeded despite audit-write failure")
	}
	var failedSessionRows int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM factory_worker_sessions WHERE principal_id=$1`, principal+"-failed-issue").Scan(&failedSessionRows); err != nil || failedSessionRows != 0 {
		t.Fatalf("failed PostgreSQL issuance session rows=%d err=%v, want 0 after rollback", failedSessionRows, err)
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER reject_worker_capability_issuance ON factory_worker_session_audit`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DROP FUNCTION reject_worker_capability_audit()`); err != nil {
		t.Fatal(err)
	}
	again, replay, err := store.RegisterWorkerSession(ctx, principal, "boot-one")
	if err != nil || !replay || again.ID != created.ID || again.Capability == created.Capability {
		t.Fatalf("retry=(%+v,%v,%v)", again, replay, err)
	}
	if _, err := store.AuthenticateWorkerCapability(ctx, principal, created.Capability); !errors.Is(err, ErrNotFound) {
		t.Fatalf("prior PostgreSQL capability error=%v, want invalid", err)
	}
	if got, err := store.AuthenticateWorkerCapability(ctx, principal, again.Capability); err != nil || got.ID != again.ID {
		t.Fatalf("replacement PostgreSQL capability=(%+v,%v)", got, err)
	}
	var issuedDigest string
	var issuedExpiry time.Time
	issuedHash := sha256.Sum256([]byte(created.Capability))
	if err := db.QueryRowContext(ctx, `SELECT capability_hash, capability_expires_at FROM factory_worker_session_audit WHERE session_id=$1 AND event='capability_issued'`, created.ID).Scan(&issuedDigest, &issuedExpiry); err != nil {
		t.Fatal(err)
	}
	if issuedDigest != hex.EncodeToString(issuedHash[:]) || created.CapabilityExpiresAt == nil || !issuedExpiry.Equal(created.CapabilityExpiresAt.Truncate(time.Microsecond)) {
		t.Fatalf("PostgreSQL issuance audit digest/expiry=(%q,%s), want matching digest and returned expiry %v", issuedDigest, issuedExpiry, created.CapabilityExpiresAt)
	}
	rotatedHash := sha256.Sum256([]byte(again.Capability))
	var rotatedExpiry time.Time
	if err := db.QueryRowContext(ctx, `SELECT capability_expires_at FROM factory_worker_session_audit WHERE session_id=$1 AND event='capability_rotated' AND capability_hash=$2`, created.ID, hex.EncodeToString(rotatedHash[:])).Scan(&rotatedExpiry); err != nil {
		t.Fatal(err)
	}
	if again.CapabilityExpiresAt == nil || !rotatedExpiry.Equal(again.CapabilityExpiresAt.Truncate(time.Microsecond)) {
		t.Fatalf("PostgreSQL rotation audit expiry=%s, want returned expiry %v", rotatedExpiry, again.CapabilityExpiresAt)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, principal+"-wrong", "boot-one"); err != nil {
		t.Fatalf("register separate PostgreSQL principal: %v", err)
	}
	if _, err := store.AuthenticateWorkerCapability(ctx, principal+"-wrong", again.Capability); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-principal PostgreSQL capability error=%v, want invalid", err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, principal, "boot-two"); !errors.Is(err, ErrWorkerSessionConflict) {
		t.Fatalf("competing boot error=%v", err)
	}
	if _, err := store.RetireWorkerSession(ctx, principal, "boot-one", created.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, principal, "boot-one"); !errors.Is(err, ErrWorkerSessionConflict) {
		t.Fatalf("re-register retired principal/boot pair error=%v, want conflict", err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, principal, "boot-two"); err != nil {
		t.Fatalf("register different boot after explicit retirement: %v", err)
	}
}
