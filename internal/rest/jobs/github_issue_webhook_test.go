package restjobs

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestSQLiteGitHubWebhookAdmissionIsAtomicDurableAndDeliveryIdempotent(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	config := Config{QueueCapacity: 8, MaxConcurrentJobs: 2, MaxRecords: 16, MaxEventsPerJob: 8}
	store, err := OpenSQLiteStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	issue := 42
	request := Request{Repository: "widget", Task: "bounded issue task", Issue: &issue}
	body := sha256.Sum256([]byte("signed delivery body"))
	first, replay, err := store.AdmitGitHubIssueWebhook(context.Background(), "delivery-42", fmt.Sprintf("%x", body), "issue-owner", request)
	if err != nil || replay {
		t.Fatalf("initial admission=(%+v,%t,%v), want new Thread+Job", first.Thread, replay, err)
	}
	if first.Thread.Submitter != "issue-owner" || len(first.Jobs) != 1 || first.Thread.JobIDs[0] != first.Jobs[0].ID || first.Jobs[0].Submitter != "issue-owner" || first.Jobs[0].Request.Issue == nil || *first.Jobs[0].Request.Issue != issue {
		t.Fatalf("webhook admission did not preserve owner and canonical Thread/Job membership: %+v", first)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenSQLiteStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	again, replay, err := store.AdmitGitHubIssueWebhook(context.Background(), "delivery-42", fmt.Sprintf("%x", body), "issue-owner", request)
	if err != nil || !replay || again.Thread.ID != first.Thread.ID || again.Jobs[0].ID != first.Jobs[0].ID {
		t.Fatalf("reopened exact delivery retry=(%+v,%t,%v), want original durable Thread+Job", again, replay, err)
	}
	changed := sha256.Sum256([]byte("different signed delivery body"))
	if _, _, err := store.AdmitGitHubIssueWebhook(context.Background(), "delivery-42", fmt.Sprintf("%x", changed), "issue-owner", request); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed-body retry error=%v, want idempotency conflict", err)
	}
}

func TestSQLiteGitHubWebhookConcurrentDeliveryAdmitsOneThreadAndJob(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	config := Config{QueueCapacity: 16, MaxConcurrentJobs: 2, MaxRecords: 32, MaxEventsPerJob: 8}
	stores := make([]*SQLiteStore, 2)
	for i := range stores {
		store, err := OpenSQLiteStore(path, config)
		if err != nil {
			t.Fatal(err)
		}
		stores[i] = store
		defer store.CloseStore()
	}
	body := sha256.Sum256([]byte("one payload"))
	request := Request{Repository: "widget", Task: "one issue task"}
	const attempts = 12
	start := make(chan struct{})
	results := make(chan struct {
		snapshot ThreadSnapshot
		replay   bool
		err      error
	}, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			snapshot, replay, err := stores[index%len(stores)].AdmitGitHubIssueWebhook(context.Background(), "same-delivery", fmt.Sprintf("%x", body), "issue-owner", request)
			results <- struct {
				snapshot ThreadSnapshot
				replay   bool
				err      error
			}{snapshot, replay, err}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	var id string
	created, replayed := 0, 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent webhook admission: %v", result.err)
		}
		if len(result.snapshot.Jobs) != 1 || len(result.snapshot.Thread.JobIDs) != 1 {
			t.Fatalf("concurrent admission produced partial/multiple membership: %+v", result.snapshot)
		}
		if id == "" {
			id = result.snapshot.Thread.ID
		} else if id != result.snapshot.Thread.ID {
			t.Fatalf("same delivery admitted different Threads %q and %q", id, result.snapshot.Thread.ID)
		}
		if result.replay {
			replayed++
		} else {
			created++
		}
	}
	if created != 1 || replayed != attempts-1 {
		t.Fatalf("concurrent admissions created=%d replayed=%d, want 1 and %d", created, replayed, attempts-1)
	}
	var jobs, threads, members, keys, events int
	for query, target := range map[string]*int{
		"SELECT COUNT(*) FROM factory_jobs":                           &jobs,
		"SELECT COUNT(*) FROM factory_threads":                        &threads,
		"SELECT COUNT(*) FROM factory_thread_jobs":                    &members,
		"SELECT COUNT(*) FROM factory_thread_idempotency":             &keys,
		"SELECT COUNT(*) FROM factory_job_events WHERE type='queued'": &events,
	} {
		if err := stores[0].db.QueryRow(query).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	if jobs != 1 || threads != 1 || members != 1 || keys != 1 || events != 1 {
		t.Fatalf("partial or duplicate admission rows: jobs=%d threads=%d members=%d keys=%d queued_events=%d", jobs, threads, members, keys, events)
	}
	var storedDigest string
	if err := stores[0].db.QueryRow(`SELECT body_sha256 FROM factory_github_issue_webhook_deliveries WHERE delivery_id='same-delivery'`).Scan(&storedDigest); err != nil {
		t.Fatal(err)
	}
	if storedDigest != fmt.Sprintf("%x", body) || strings.Contains(storedDigest, "one payload") {
		t.Fatal("webhook delivery record did not retain only the stable digest")
	}
}

func TestPostgresGitHubWebhookAdmissionIsAtomicAndDeliveryIdempotent(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL 18 webhook integration tests")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	var version int
	if err := admin.QueryRow(`SHOW server_version_num`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version < 180000 || version >= 190000 {
		t.Fatalf("PostgreSQL version %d is outside the supported integration test version", version)
	}
	firstDB := scopedPostgresDB(t, admin, dsn)
	schema := currentTestSchema(t, firstDB)
	secondDB := postgresDBForSchema(t, dsn, schema)
	first, err := NewPostgresStore(firstDB, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewPostgresStore(secondDB, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.CloseStore(); _ = second.CloseStore() })
	body := sha256.Sum256([]byte("postgres delivery"))
	request := Request{Repository: "widget", Task: "postgres issue work"}
	initial, replay, err := first.AdmitGitHubIssueWebhook(context.Background(), "pg-delivery", fmt.Sprintf("%x", body), "issue-owner", request)
	if err != nil || replay {
		t.Fatalf("initial admission=(%+v,%t,%v)", initial, replay, err)
	}
	again, replay, err := second.AdmitGitHubIssueWebhook(context.Background(), "pg-delivery", fmt.Sprintf("%x", body), "issue-owner", request)
	if err != nil || !replay || again.Thread.ID != initial.Thread.ID || again.Jobs[0].ID != initial.Jobs[0].ID {
		t.Fatalf("cross-connection same-delivery retry=(%+v,%t,%v)", again, replay, err)
	}
	changed := sha256.Sum256([]byte("different PostgreSQL payload"))
	if _, _, err := second.AdmitGitHubIssueWebhook(context.Background(), "pg-delivery", fmt.Sprintf("%x", changed), "issue-owner", request); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed PostgreSQL body error=%v", err)
	}
	var jobs, threads, deliveries int
	for query, target := range map[string]*int{
		`SELECT COUNT(*) FROM factory_jobs`:                            &jobs,
		`SELECT COUNT(*) FROM factory_threads`:                         &threads,
		`SELECT COUNT(*) FROM factory_github_issue_webhook_deliveries`: &deliveries,
	} {
		if err := firstDB.QueryRow(query).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	if jobs != 1 || threads != 1 || deliveries != 1 {
		t.Fatalf("PostgreSQL delivery identity not atomic: jobs=%d threads=%d deliveries=%d", jobs, threads, deliveries)
	}
	boundedConfig := testConfig()
	boundedConfig.QueueCapacity = 1
	boundedConfig.MaxConcurrentJobs = 1
	boundedConfig.MaxRecords = 1
	bounded, err := NewPostgresStore(scopedPostgresDB(t, admin, dsn), boundedConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bounded.CloseStore() })
	if _, _, err := bounded.Admit("fill-capacity", Request{Repository: "widget", Task: "fills capacity"}); err != nil {
		t.Fatal(err)
	}
	other := sha256.Sum256([]byte("must roll back"))
	if _, _, err := bounded.AdmitGitHubIssueWebhook(context.Background(), "pg-rollback", fmt.Sprintf("%x", other), "issue-owner", Request{Repository: "widget", Task: "over capacity"}); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("PostgreSQL rollback admission error=%v", err)
	}
	var rollbackDeliveries, rollbackThreads, rollbackJobs int
	for query, target := range map[string]*int{
		`SELECT COUNT(*) FROM factory_github_issue_webhook_deliveries`: &rollbackDeliveries,
		`SELECT COUNT(*) FROM factory_threads`:                         &rollbackThreads,
		`SELECT COUNT(*) FROM factory_jobs`:                            &rollbackJobs,
	} {
		if err := bounded.db.QueryRow(query).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	if rollbackDeliveries != 0 || rollbackThreads != 0 || rollbackJobs != 1 {
		t.Fatalf("PostgreSQL failed admission left partial state: deliveries=%d threads=%d jobs=%d", rollbackDeliveries, rollbackThreads, rollbackJobs)
	}
}

func TestSQLiteGitHubWebhookAdmissionFailureLeavesNoPartialRows(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "jobs.db"), Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	first := Request{Repository: "widget", Task: "fills retained record cap"}
	if _, _, err := store.AdmitThreadAs("ordinary", "alice", first); err != nil {
		t.Fatal(err)
	}
	body := sha256.Sum256([]byte("must roll back"))
	if _, _, err := store.AdmitGitHubIssueWebhook(context.Background(), "rollback-delivery", fmt.Sprintf("%x", body), "issue-owner", Request{Repository: "widget", Task: "cannot admit"}); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("capacity rejection error=%v, want registry_full", err)
	}
	for _, table := range []string{"factory_github_issue_webhook_deliveries"} {
		var count int
		if err := store.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("failed admission left %d rows in %s (err=%v)", count, table, err)
		}
	}
	var threads, jobs int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM factory_threads`).Scan(&threads); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM factory_jobs`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if threads != 1 || jobs != 1 {
		t.Fatalf("failed admission left partial Thread or Job state: threads=%d jobs=%d", threads, jobs)
	}
}
