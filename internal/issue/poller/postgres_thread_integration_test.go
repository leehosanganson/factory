package poller

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/leehosanganson/factory/internal/rest/jobs"
)

func TestPostgresIssueAdmissionCreatesOneThreadAtomically(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL issue-poller integration tests")
	}
	adminConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*adminConfig)
	t.Cleanup(func() { _ = admin.Close() })
	var version int
	if err := admin.QueryRow(`SHOW server_version_num`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version < 180000 || version >= 190000 {
		t.Fatalf("PostgreSQL server_version_num=%d, integration coverage requires PostgreSQL 18", version)
	}

	t.Run("cross-connection same event and distinct keys", func(t *testing.T) {
		db1 := issuePollerTestDB(t, admin, dsn, "issue_thread_race")
		schema := currentIssuePollerSchema(t, db1)
		db2 := postgresDBForIssuePollerSchema(t, dsn, schema)
		config := restjobs.Config{QueueCapacity: 8, MaxConcurrentJobs: 2, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 256 << 10, RegistryBytes: 256 << 20}
		first, err := restjobs.NewPostgresStore(db1, config)
		if err != nil {
			t.Fatal(err)
		}
		second, err := restjobs.NewPostgresStore(db2, config)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = first.CloseStore(); _ = second.CloseStore() })
		ctx := context.Background()
		req := issueThreadRequest(77, strings.Repeat("a", 64))

		start := make(chan struct{})
		type result struct {
			snapshot restjobs.ThreadSnapshot
			replay   bool
			err      error
		}
		const callers = 8
		results := make(chan result, callers)
		var wg sync.WaitGroup
		for i := 0; i < callers; i++ {
			store := first
			if i%2 == 1 {
				store = second
			}
			wg.Add(1)
			go func(i int, store *restjobs.PostgresStore) {
				defer wg.Done()
				<-start
				snapshot, replay, err := store.AdmitIssuePollerThread(ctx, fmt.Sprintf("issue-key-%d", i), "github-issue-poller", req, "ready")
				results <- result{snapshot, replay, err}
			}(i, store)
		}
		close(start)
		wg.Wait()
		close(results)
		var threadID, jobID string
		created, replays := 0, 0
		for got := range results {
			if got.err != nil {
				t.Fatalf("concurrent issue admission: %v", got.err)
			}
			if threadID == "" {
				threadID, jobID = got.snapshot.Thread.ID, got.snapshot.Jobs[0].ID
			}
			if got.snapshot.Thread.ID != threadID || len(got.snapshot.Jobs) != 1 || got.snapshot.Jobs[0].ID != jobID || got.snapshot.Thread.JobIDs[0] != jobID {
				t.Fatalf("concurrent admission returned inconsistent Thread/Job pair: %+v", got.snapshot)
			}
			if got.replay {
				replays++
			} else {
				created++
			}
		}
		if created != 1 || replays != callers-1 {
			t.Fatalf("created=%d replays=%d, want one new and %d attaches", created, replays, callers-1)
		}
		// A third key attaches the same source/version to exactly the same pair.
		attached, replay, err := second.AdmitIssuePollerThread(ctx, "issue-key-third", "github-issue-poller", req, "ready")
		if err != nil || !replay || attached.Thread.ID != threadID || attached.Jobs[0].ID != jobID {
			t.Fatalf("distinct-key retry=(%+v,%t,%v)", attached, replay, err)
		}
		assertIssueThreadCounts(t, db1, req, jobID, threadID, 1, 1)
		// The bounded label ledger is not the source/version dedupe authority.
		if _, err := db1.Exec(`DELETE FROM factory_issue_poller_outcomes WHERE tracker='github' AND source_repository='source/repo' AND issue_number=77 AND version=$1`, req.IssueProvenance.Version); err != nil {
			t.Fatal(err)
		}
		afterLedgerLoss, replay, err := first.AdmitIssuePollerThread(ctx, "issue-key-after-ledger-loss", "github-issue-poller", req, "ready")
		if err != nil || !replay || afterLedgerLoss.Thread.ID != threadID || afterLedgerLoss.Jobs[0].ID != jobID {
			t.Fatalf("retry after label-ledger eviction=(%+v,%t,%v)", afterLedgerLoss, replay, err)
		}
		assertIssueThreadCounts(t, db1, req, jobID, threadID, 1, 1)
		var jobKeys, threadKeys, queuedEvents int
		if err := db1.QueryRow(`SELECT COUNT(*) FROM factory_job_idempotency WHERE job_id=$1`, jobID).Scan(&jobKeys); err != nil {
			t.Fatal(err)
		}
		if err := db1.QueryRow(`SELECT COUNT(*) FROM factory_thread_idempotency WHERE thread_id=$1`, threadID).Scan(&threadKeys); err != nil {
			t.Fatal(err)
		}
		if err := db1.QueryRow(`SELECT COUNT(*) FROM factory_job_events WHERE job_id=$1 AND type='queued'`, jobID).Scan(&queuedEvents); err != nil {
			t.Fatal(err)
		}
		wantKeys := callers + 2 // concurrent callers, one third-key retry, and one post-ledger retry
		if jobKeys != wantKeys || threadKeys != wantKeys || queuedEvents != 1 {
			t.Fatalf("key/event counts job=%d thread=%d queued=%d; want %d,%d,1", jobKeys, threadKeys, queuedEvents, wantKeys, wantKeys)
		}
	})

	t.Run("exact existing poller job gets attached without duplicate event", func(t *testing.T) {
		db := issuePollerTestDB(t, admin, dsn, "issue_thread_existing")
		store, err := restjobs.NewPostgresStore(db, restjobs.Config{QueueCapacity: 8, MaxConcurrentJobs: 2, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 256 << 10, RegistryBytes: 256 << 20})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.CloseStore() })
		ctx := context.Background()
		req := issueThreadRequest(78, strings.Repeat("b", 64))
		prior, replay, err := store.AdmitIssuePoller(ctx, "legacy-poller-key", principalID, req, "ready")
		if err != nil || replay {
			t.Fatalf("existing Job admission=(%+v,%t,%v)", prior, replay, err)
		}
		thread, replay, err := store.AdmitIssuePollerThread(ctx, "thread-key-different", principalID, req, "ready")
		if err != nil || !replay || len(thread.Jobs) != 1 || thread.Jobs[0].ID != prior.ID {
			t.Fatalf("Thread attach=(%+v,%t,%v)", thread, replay, err)
		}
		assertIssueThreadCounts(t, db, req, prior.ID, thread.Thread.ID, 1, 1)
		var events int
		if err := db.QueryRow(`SELECT COUNT(*) FROM factory_job_events WHERE job_id=$1 AND type='queued'`, prior.ID).Scan(&events); err != nil {
			t.Fatal(err)
		}
		if events != 1 {
			t.Fatalf("queued events=%d, want no duplicate event", events)
		}
	})

	t.Run("unproven pre-existing membership is not attached", func(t *testing.T) {
		db := issuePollerTestDB(t, admin, dsn, "issue_thread_membership")
		config := restjobs.Config{QueueCapacity: 8, MaxConcurrentJobs: 2, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 256 << 10, RegistryBytes: 256 << 20}
		store, err := restjobs.NewPostgresStore(db, config)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.CloseStore() })
		ctx := context.Background()
		req := issueThreadRequest(82, strings.Repeat("f", 64))
		job, replay, err := store.AdmitIssuePoller(ctx, "pre-existing-poller-key", principalID, req, "ready")
		if err != nil || replay {
			t.Fatalf("existing Job=(%+v,%t,%v)", job, replay, err)
		}
		otherThread, _, err := store.AdmitThreadAs("other-thread", principalID, restjobs.Request{Repository: "destination", Task: "unrelated member"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO factory_thread_jobs(thread_id,job_id,position) VALUES($1,$2,2)`, otherThread.Thread.ID, job.ID); err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.AdmitIssuePollerThread(ctx, "attempt-attach", principalID, req, "ready"); !errors.Is(err, restjobs.ErrProvenanceConflict) {
			t.Fatalf("unproven membership error=%v, want provenance conflict", err)
		}
		var threads, threadKeys, members, pollerRows int
		if err := db.QueryRow(`SELECT COUNT(*) FROM factory_threads`).Scan(&threads); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM factory_thread_idempotency WHERE principal_id=$1 AND key='attempt-attach'`, principalID).Scan(&threadKeys); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM factory_thread_jobs WHERE job_id=$1`, job.ID).Scan(&members); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM factory_issue_poller_outcomes WHERE job_id=$1`, job.ID).Scan(&pollerRows); err != nil {
			t.Fatal(err)
		}
		if threads != 1 || threadKeys != 0 || members != 1 || pollerRows != 1 {
			t.Fatalf("unproven attach changed durable state: threads=%d key=%d memberships=%d label obligations=%d", threads, threadKeys, members, pollerRows)
		}
	})

	t.Run("same-principal request conflict does not create a Thread", func(t *testing.T) {
		db := issuePollerTestDB(t, admin, dsn, "issue_thread_request_conflict")
		store, err := restjobs.NewPostgresStore(db, restjobs.Config{QueueCapacity: 8, MaxConcurrentJobs: 2, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 256 << 10, RegistryBytes: 256 << 20})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.CloseStore() })
		ctx := context.Background()
		request := issueThreadRequest(85, strings.Repeat("3", 64))
		if _, _, err := store.AdmitIssuePoller(ctx, "existing-request", principalID, request, "ready"); err != nil {
			t.Fatal(err)
		}
		conflict := request
		conflict.Task += " changed"
		if _, _, err := store.AdmitIssuePollerThread(ctx, "conflicting-request", principalID, conflict, "ready"); !errors.Is(err, restjobs.ErrProvenanceConflict) {
			t.Fatalf("changed request error=%v, want provenance conflict", err)
		}
		var threads, threadKeys, members, labels int
		if err := db.QueryRow(`SELECT COUNT(*) FROM factory_threads`).Scan(&threads); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM factory_thread_idempotency WHERE key='conflicting-request'`).Scan(&threadKeys); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM factory_thread_jobs`).Scan(&members); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM factory_issue_poller_outcomes`).Scan(&labels); err != nil {
			t.Fatal(err)
		}
		if threads != 0 || threadKeys != 0 || members != 0 || labels != 1 {
			t.Fatalf("request conflict mutated state: threads=%d keys=%d members=%d labels=%d", threads, threadKeys, members, labels)
		}
	})

	t.Run("queue capacity failure rolls back the whole admission", func(t *testing.T) {
		db := issuePollerTestDB(t, admin, dsn, "issue_thread_queue_full")
		config := restjobs.Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 256 << 10, RegistryBytes: 256 << 20}
		store, err := restjobs.NewPostgresStore(db, config)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.CloseStore() })
		ctx := context.Background()
		first := issueThreadRequest(83, strings.Repeat("1", 64))
		if _, _, err := store.AdmitIssuePollerThread(ctx, "queue-first", principalID, first, "ready"); err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.AdmitAs("queue-filler", "another-principal", restjobs.Request{Repository: "destination", Task: "fill active queue capacity"}); err != nil {
			t.Fatal(err)
		}
		blocked := issueThreadRequest(84, strings.Repeat("2", 64))
		if _, _, err := store.AdmitIssuePollerThread(ctx, "queue-blocked", principalID, blocked, "ready"); !errors.Is(err, restjobs.ErrQueueFull) {
			t.Fatalf("queue capacity error=%v, want ErrQueueFull", err)
		}
		assertNoIssueAdmissionRows(t, db, blocked)
	})

	t.Run("capacity and foreign provenance conflicts leave no partial Thread rows", func(t *testing.T) {
		db := issuePollerTestDB(t, admin, dsn, "issue_thread_failure")
		config := restjobs.Config{QueueCapacity: 600, MaxConcurrentJobs: 2, MaxRecords: 700, MaxEventsPerJob: 8, MaxTaskBytes: 256 << 10, RegistryBytes: 256 << 20}
		store, err := restjobs.NewPostgresStore(db, config)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.CloseStore() })
		ctx := context.Background()
		foreign := issueThreadRequest(81, strings.Repeat("e", 64))
		if _, _, err := store.AdmitAs("foreign-key", "some-other-principal", foreign); err != nil {
			t.Fatal(err)
		}
		// Fill the bounded pending-label ledger, then assert the combined
		// admission transaction rolls back Job, provenance, and Thread rows.
		blocked := issueThreadRequest(80, strings.Repeat("d", 64))
		if _, _, err := store.AdmitIssuePollerThread(ctx, "foreign-conflict", principalID, foreign, "ready"); !errors.Is(err, restjobs.ErrProvenanceConflict) {
			t.Fatalf("foreign provenance error=%v, want conflict", err)
		}
		var foreignThreadKeys, foreignLedger int
		if err := db.QueryRow(`SELECT COUNT(*) FROM factory_thread_idempotency WHERE principal_id=$1 AND key='foreign-conflict'`, principalID).Scan(&foreignThreadKeys); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM factory_issue_poller_outcomes WHERE tracker=$1 AND source_repository=$2 AND issue_number=$3 AND version=$4`, foreign.IssueProvenance.Tracker, foreign.IssueProvenance.SourceRepository, foreign.IssueProvenance.IssueNumber, foreign.IssueProvenance.Version).Scan(&foreignLedger); err != nil {
			t.Fatal(err)
		}
		if foreignThreadKeys != 0 || foreignLedger != 0 {
			t.Fatalf("foreign provenance conflict created thread key=%d label ledger=%d", foreignThreadKeys, foreignLedger)
		}
		// Fill the bounded pending-label ledger, then assert the combined
		// admission transaction rolls back Job, provenance, and Thread rows.
		for i := 0; i < 500; i++ {
			request := issueThreadRequest(1000+i, fmt.Sprintf("%064x", i+1))
			request.IssueProvenance.URL = fmt.Sprintf("https://github.com/source/repo/issues/%d", 1000+i)
			if _, _, err := store.AdmitIssuePoller(ctx, fmt.Sprintf("pending-%d", i), principalID, request, "ready"); err != nil {
				t.Fatalf("fill pending label ledger %d: %v", i, err)
			}
		}
		if _, _, err := store.AdmitIssuePollerThread(ctx, "capacity-second", principalID, blocked, "ready"); err == nil {
			t.Fatal("admission succeeded with a full bounded label ledger")
		}
		assertNoIssueAdmissionRows(t, db, blocked)
	})
}

func issueThreadRequest(number int, version string) restjobs.Request {
	return restjobs.Request{Repository: "destination", Task: fmt.Sprintf("bounded issue task %d", number), Issue: &number, IssueProvenance: &restjobs.IssueProvenance{Tracker: "github", SourceRepository: "source/repo", IssueNumber: number, Version: version, URL: fmt.Sprintf("https://github.com/source/repo/issues/%d", number), Title: "Issue title"}}
}

func issuePollerTestDB(t *testing.T, admin *sql.DB, dsn, prefix string) *sql.DB {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func currentIssuePollerSchema(t *testing.T, db *sql.DB) string {
	t.Helper()
	var schema string
	if err := db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	return schema
}

func postgresDBForIssuePollerSchema(t *testing.T, dsn, schema string) *sql.DB {
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

func assertIssueThreadCounts(t *testing.T, db *sql.DB, request restjobs.Request, jobID, threadID string, wantJobs, wantThreads int) {
	t.Helper()
	var jobs, threads, members, provenance, labels int
	if err := db.QueryRow(`SELECT COUNT(*) FROM factory_jobs WHERE id=$1`, jobID).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM factory_threads WHERE id=$1 AND initial_job_id=$2`, threadID, jobID).Scan(&threads); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM factory_thread_jobs WHERE thread_id=$1 AND job_id=$2`, threadID, jobID).Scan(&members); err != nil {
		t.Fatal(err)
	}
	p := request.IssueProvenance
	if err := db.QueryRow(`SELECT COUNT(*) FROM factory_job_issue_provenance WHERE tracker=$1 AND source_repository=$2 AND issue_number=$3 AND version=$4 AND job_id=$5`, p.Tracker, p.SourceRepository, p.IssueNumber, p.Version, jobID).Scan(&provenance); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM factory_issue_poller_outcomes WHERE tracker=$1 AND source_repository=$2 AND issue_number=$3 AND version=$4 AND job_id=$5 AND label='ready'`, p.Tracker, p.SourceRepository, p.IssueNumber, p.Version, jobID).Scan(&labels); err != nil {
		t.Fatal(err)
	}
	if jobs != wantJobs || threads != wantThreads || members != 1 || provenance != 1 || labels != 1 {
		t.Fatalf("rows jobs=%d threads=%d members=%d provenance=%d labels=%d", jobs, threads, members, provenance, labels)
	}
}

func assertNoIssueAdmissionRows(t *testing.T, db *sql.DB, request restjobs.Request) {
	t.Helper()
	assertNoIssueThreadRows(t, db, request)
	p := request.IssueProvenance
	var jobs, ledger int
	if err := db.QueryRow(`SELECT COUNT(*) FROM factory_job_issue_provenance WHERE tracker=$1 AND source_repository=$2 AND issue_number=$3 AND version=$4`, p.Tracker, p.SourceRepository, p.IssueNumber, p.Version).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM factory_issue_poller_outcomes WHERE tracker=$1 AND source_repository=$2 AND issue_number=$3 AND version=$4`, p.Tracker, p.SourceRepository, p.IssueNumber, p.Version).Scan(&ledger); err != nil {
		t.Fatal(err)
	}
	if jobs != 0 || ledger != 0 {
		t.Fatalf("failed admission left provenance=%d ledger=%d rows", jobs, ledger)
	}
}

func assertNoIssueThreadRows(t *testing.T, db *sql.DB, request restjobs.Request) {
	t.Helper()
	p := request.IssueProvenance
	var jobs, threads, members, ledger int
	if err := db.QueryRow(`SELECT COUNT(*) FROM factory_jobs WHERE request_json::jsonb->>'task'=$1`, request.Task).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM factory_threads t JOIN factory_jobs j ON j.id=t.initial_job_id WHERE j.request_json::jsonb->>'task'=$1`, request.Task).Scan(&threads); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM factory_thread_jobs m JOIN factory_jobs j ON j.id=m.job_id WHERE j.request_json::jsonb->>'task'=$1`, request.Task).Scan(&members); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM factory_issue_poller_outcomes WHERE tracker=$1 AND source_repository=$2 AND issue_number=$3 AND version=$4`, p.Tracker, p.SourceRepository, p.IssueNumber, p.Version).Scan(&ledger); err != nil {
		t.Fatal(err)
	}
	if jobs != 0 || threads != 0 || members != 0 || ledger != 0 {
		t.Fatalf("failed Thread admission left matching jobs=%d threads=%d members=%d ledger=%d", jobs, threads, members, ledger)
	}
}
