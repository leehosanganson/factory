package restjobs

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

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
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_session_audit`).Scan(&auditCount); err != nil || auditCount != 3 {
		t.Fatalf("audit events=%d err=%v; want registered, retired, registered", auditCount, err)
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
	if err := store.db.QueryRow(`SELECT MAX(version) FROM factory_schema_migrations`).Scan(&version); err != nil || version != 16 {
		t.Fatalf("upgraded SQLite schema version=%d err=%v; want 16", version, err)
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
	if err := db.QueryRow(`SELECT MAX(version) FROM factory_rest_schema_migrations`).Scan(&version); err != nil || version != 12 {
		t.Fatalf("upgraded PostgreSQL schema version=%d err=%v; want 12", version, err)
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
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
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
	again, replay, err := store.RegisterWorkerSession(ctx, principal, "boot-one")
	if err != nil || !replay || again.ID != created.ID {
		t.Fatalf("retry=(%+v,%v,%v)", again, replay, err)
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
