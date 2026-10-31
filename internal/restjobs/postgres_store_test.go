package restjobs

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

func TestPostgresStoreIntegration(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL 18 store integration tests")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var version int
	if err := admin.QueryRow(`SHOW server_version_num`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version < 180000 || version >= 190000 {
		t.Fatalf("PostgreSQL server_version_num=%d, integration coverage requires PostgreSQL 18", version)
	}

	t.Run("Thread persistence, ownership, and recovery projection", func(t *testing.T) {
		db1 := scopedPostgresDB(t, admin, dsn)
		schema := currentTestSchema(t, db1)
		db2 := postgresDBForSchema(t, dsn, schema)
		first, err := NewPostgresStore(db1, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		second, err := NewPostgresStore(db2, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = first.CloseStore(); _ = second.CloseStore() })

		request := Request{Repository: " widget ", Task: "  persist Thread  "}
		created, replay, err := first.AdmitThreadAs("thread-key", " alice ", request)
		if err != nil || replay {
			t.Fatalf("initial Thread admission=(%+v,%t,%v), want new pair", created, replay, err)
		}
		if len(created.Thread.JobIDs) != 1 || len(created.Jobs) != 1 || created.Thread.JobIDs[0] != created.Jobs[0].ID || created.Thread.Status != ThreadStatusInProgress {
			t.Fatalf("initial Thread snapshot=%+v, want one queued member", created)
		}
		memberSnapshot, secondJob, memberReplay, err := first.AdmitThreadJobAs(created.Thread.ID, "member-key", "alice", Request{Repository: "widget", Task: "follow-up"})
		if err != nil || memberReplay || len(memberSnapshot.Jobs) != 2 || memberSnapshot.Thread.JobIDs[1] != secondJob.ID {
			t.Fatalf("Thread member admission=(%+v,%+v,%t,%v), want ordered second Job", memberSnapshot, secondJob, memberReplay, err)
		}
		if memberSnapshot.Thread.Status != ThreadStatusInProgress {
			t.Fatalf("Thread after member admission status=%s, want in_progress", memberSnapshot.Thread.Status)
		}
		memberSnapshot, replayedMember, memberReplay, err := second.AdmitThreadJobAs(created.Thread.ID, " member-key ", "alice", Request{Repository: " widget ", Task: " follow-up "})
		if err != nil || !memberReplay || replayedMember.ID != secondJob.ID || len(memberSnapshot.Jobs) != 2 || memberSnapshot.Thread.JobIDs[1] != secondJob.ID {
			t.Fatalf("cross-store Thread member replay=(%+v,%+v,%t,%v)", memberSnapshot, replayedMember, memberReplay, err)
		}
		if _, _, _, err := second.AdmitThreadJobAs(created.Thread.ID, "foreign-member", "bob", Request{Repository: "widget", Task: "foreign"}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("foreign Thread member admission error=%v, want not found", err)
		}
		completed, err := first.CompleteThreadAs(created.Thread.ID, "alice")
		if err != nil || completed.Thread.CompletedAt == nil || completed.Thread.Status != ThreadStatusInProgress {
			t.Fatalf("explicit completion=(%+v,%v), want completed metadata independent of aggregate status", completed.Thread, err)
		}
		repeatedCompletion, err := second.CompleteThreadAs(created.Thread.ID, "alice")
		if err != nil || repeatedCompletion.Thread.CompletedAt == nil || !repeatedCompletion.Thread.CompletedAt.Equal(*completed.Thread.CompletedAt) {
			t.Fatalf("cross-connection completion replay=(%+v,%v), want same timestamp", repeatedCompletion.Thread, err)
		}
		if _, _, _, err := second.AdmitThreadJobAs(created.Thread.ID, "post-completion", "alice", Request{Repository: "widget", Task: "must reject"}); !errors.Is(err, ErrThreadClosed) {
			t.Fatalf("append after completion=%v, want ErrThreadClosed", err)
		}
		if _, _, _, err := second.AdmitThreadJobAs(created.Thread.ID, "member-key", "alice", Request{Repository: "widget", Task: "follow-up"}); err != nil {
			t.Fatalf("replay admitted member after completion=%v, want idempotent replay", err)
		}
		raceThread, _, err := first.AdmitThreadAs("completion-race", "alice", Request{Repository: "widget", Task: "race initial"})
		if err != nil {
			t.Fatal(err)
		}
		startRace := make(chan struct{})
		type raceResult struct {
			thread ThreadSnapshot
			job    Snapshot
			err    error
		}
		completeResult := make(chan raceResult, 1)
		addResult := make(chan raceResult, 1)
		go func() {
			<-startRace
			snapshot, err := first.CompleteThreadAs(raceThread.Thread.ID, "alice")
			completeResult <- raceResult{thread: snapshot, err: err}
		}()
		go func() {
			<-startRace
			_, job, _, err := second.AdmitThreadJobAs(raceThread.Thread.ID, "completion-race-member", "alice", Request{Repository: "widget", Task: "race member"})
			addResult <- raceResult{job: job, err: err}
		}()
		close(startRace)
		closed, appended := <-completeResult, <-addResult
		if closed.err != nil || closed.thread.Thread.CompletedAt == nil {
			t.Fatalf("concurrent PostgreSQL completion=(%+v,%v), want successful close", closed.thread.Thread, closed.err)
		}
		if appended.err != nil && !errors.Is(appended.err, ErrThreadClosed) {
			t.Fatalf("concurrent PostgreSQL append error=%v, want success or ErrThreadClosed", appended.err)
		}
		finalRace, err := second.GetThread(raceThread.Thread.ID)
		expectedJobs := 1
		if appended.err == nil {
			expectedJobs++
		}
		if err != nil || finalRace.Thread.CompletedAt == nil || len(finalRace.Jobs) != expectedJobs {
			t.Fatalf("PostgreSQL concurrent final state=(%+v,%v), want close plus either admitted member or rejection", finalRace, err)
		}
		replayed, replay, err := second.AdmitThreadAs(" thread-key ", "alice", Request{Repository: "widget", Task: "persist Thread"})
		if err != nil || !replay || replayed.Thread.ID != created.Thread.ID || replayed.Jobs[0].ID != created.Jobs[0].ID {
			t.Fatalf("cross-store normalized replay=(%+v,%t,%v)", replayed, replay, err)
		}
		if _, _, err := first.AdmitThreadAs("thread-key", "alice", Request{Repository: "widget", Task: "changed"}); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("changed payload error=%v, want idempotency conflict", err)
		}
		otherPrincipal, replay, err := second.AdmitThreadAs("thread-key", "bob", request)
		if err != nil || replay || otherPrincipal.Thread.ID == created.Thread.ID || otherPrincipal.Jobs[0].ID == created.Jobs[0].ID {
			t.Fatalf("principal-scoped key admission=(%+v,%t,%v), want distinct pair", otherPrincipal, replay, err)
		}
		otherKey, replay, err := first.AdmitThreadAs("another-key", "alice", request)
		if err != nil || replay || otherKey.Thread.ID == created.Thread.ID || otherKey.Jobs[0].ID == created.Jobs[0].ID {
			t.Fatalf("distinct key admission=(%+v,%t,%v), want distinct pair", otherKey, replay, err)
		}
		if _, _, err := first.Admit("thread-key", request); err != nil {
			t.Fatalf("Thread key collided with ordinary Job namespace: %v", err)
		}
		if _, err := first.CompleteThreadAs(created.Thread.ID, "bob"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("foreign owner completion error=%v, want non-disclosing not found", err)
		}
		const callers = 8
		start := make(chan struct{})
		type concurrentResult struct {
			snapshot ThreadSnapshot
			replay   bool
			err      error
		}
		results := make(chan concurrentResult, callers)
		for i := 0; i < callers; i++ {
			store := first
			if i%2 == 1 {
				store = second
			}
			go func(store *PostgresStore) {
				<-start
				snapshot, replay, err := store.AdmitThreadAs("concurrent-thread", "alice", Request{Repository: "widget", Task: "concurrent"})
				results <- concurrentResult{snapshot: snapshot, replay: replay, err: err}
			}(store)
		}
		close(start)
		var concurrentID, concurrentJobID string
		createdCount := 0
		for i := 0; i < callers; i++ {
			got := <-results
			if got.err != nil {
				t.Fatalf("concurrent Thread admission: %v", got.err)
			}
			if concurrentID == "" {
				concurrentID, concurrentJobID = got.snapshot.Thread.ID, got.snapshot.Jobs[0].ID
			}
			if got.snapshot.Thread.ID != concurrentID || got.snapshot.Jobs[0].ID != concurrentJobID {
				t.Fatalf("concurrent admission returned different Thread/Job pairs")
			}
			if !got.replay {
				createdCount++
			}
		}
		if createdCount != 1 {
			t.Fatalf("concurrent new admissions=%d, want exactly one", createdCount)
		}
		if _, err := first.GetThreadForPrincipal(created.Thread.ID, "bob"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("foreign owner read error=%v, want not found", err)
		}
		if _, err := second.CompleteThreadAs(created.Thread.ID, "bob"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("foreign completion error=%v, want non-disclosing not found", err)
		}
		issueNumber := 7
		for label, invalid := range map[string]Request{
			"issue":      {Repository: "widget", Task: "task", Issue: &issueNumber},
			"provenance": provenanceRequest("task"),
		} {
			if _, _, err := first.AdmitThreadAs("invalid-"+label, "alice", invalid); !errors.Is(err, ErrInvalidInput) {
				t.Errorf("issue metadata %s error=%v, want invalid input", label, err)
			}
		}

		if _, err := first.Cancel(secondJob.ID); err != nil {
			t.Fatalf("cancel follow-up Thread Job: %v", err)
		}
		claim, err := first.ClaimNextAttempt()
		if err != nil || claim.Job.ID != created.Jobs[0].ID {
			t.Fatalf("claim initial Thread Job=(%+v,%v)", claim, err)
		}
		live, err := second.GetThread(created.Thread.ID)
		if err != nil || live.Thread.Status != ThreadStatusInProgress {
			t.Fatalf("live shared-worker Thread=(%+v,%v), want in_progress", live, err)
		}
		if _, err := db1.Exec(`UPDATE factory_rest_worker_ownership SET lease_until=clock_timestamp()-interval '1 second' WHERE job_id=$1`, claim.Job.ID); err != nil {
			t.Fatal(err)
		}
		interrupted, err := second.GetThread(created.Thread.ID)
		if err != nil || interrupted.Thread.Status != ThreadStatusNeedAttention {
			t.Fatalf("expired shared-worker Thread=(%+v,%v), want need_attention", interrupted, err)
		}
		if _, err := db1.Exec(`UPDATE factory_rest_worker_ownership SET lease_until=clock_timestamp()+interval '1 hour' WHERE job_id=$1`, claim.Job.ID); err != nil {
			t.Fatal(err)
		}
		if err := first.FinishAttempt(claim.Attempt, StatusFailed); err != nil {
			t.Fatal(err)
		}
		finished, err := second.GetThread(created.Thread.ID)
		if err != nil || finished.Thread.Status != ThreadStatusDone || finished.Jobs[0].Status != StatusFailed {
			t.Fatalf("terminal shared-worker Thread=(%+v,%v), want done", finished, err)
		}

		// The canonical Job may be evicted by retention; its Thread and key
		// namespace record must disappear with it.
		if _, err := db1.Exec(`DELETE FROM factory_job_events WHERE job_id=$1`, otherKey.Jobs[0].ID); err != nil {
			t.Fatal(err)
		}
		if _, err := db1.Exec(`DELETE FROM factory_jobs WHERE id=$1`, otherKey.Jobs[0].ID); err != nil {
			t.Fatal(err)
		}
		var threads, keys int
		if err := db1.QueryRow(`SELECT COUNT(*) FROM factory_threads WHERE id=$1`, otherKey.Thread.ID).Scan(&threads); err != nil {
			t.Fatal(err)
		}
		if err := db1.QueryRow(`SELECT COUNT(*) FROM factory_thread_idempotency WHERE principal_id='alice' AND key='another-key'`).Scan(&keys); err != nil {
			t.Fatal(err)
		}
		if threads != 0 || keys != 0 {
			t.Fatalf("Job delete left Thread rows=%d idempotency rows=%d, want cascaded deletion", threads, keys)
		}
	})

	t.Run("shared Store contract", func(t *testing.T) {
		runStoreContract(t, func(t *testing.T) Store {
			db := scopedPostgresDB(t, admin, dsn)
			store, err := NewPostgresStore(db, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.CloseStore() })
			return store
		})
	})

	t.Run("closing one shared server leaves queued work for another instance", func(t *testing.T) {
		db1 := scopedPostgresDB(t, admin, dsn)
		schema := currentTestSchema(t, db1)
		db2 := postgresDBForSchema(t, dsn, schema)
		first, err := NewPostgresStore(db1, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		second, err := NewPostgresStore(db2, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		first.cancelQueuedOnClose = false
		job, _, err := first.Admit("shared-instance-close", Request{Repository: "widget", Task: "remain available to peer"})
		if err != nil {
			t.Fatal(err)
		}
		if err := first.CloseStore(); err != nil {
			t.Fatal(err)
		}
		got, err := second.Get(job.ID)
		if err != nil || got.Status != StatusQueued {
			t.Fatalf("job after first server close=(%+v,%v), want queued for peer", got, err)
		}
		claim, err := second.ClaimNextAttempt()
		if err != nil || claim.Job.ID != job.ID {
			t.Fatalf("peer claim=(%+v,%v), want queued job %q", claim, err, job.ID)
		}
		if err := second.CloseStore(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("independent control-plane connections serialize admission and claims", func(t *testing.T) {
		db1 := scopedPostgresDB(t, admin, dsn)
		schema := currentTestSchema(t, db1)
		db2 := postgresDBForSchema(t, dsn, schema)
		first, err := NewPostgresStore(db1, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		second, err := NewPostgresStore(db2, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = first.CloseStore(); _ = second.CloseStore() })
		const callers = 12
		start := make(chan struct{})
		ids := make(chan string, callers)
		errs := make(chan error, callers)
		var wg sync.WaitGroup
		for i := 0; i < callers; i++ {
			store := first
			if i%2 == 1 {
				store = second
			}
			wg.Add(1)
			go func(store *PostgresStore) {
				defer wg.Done()
				<-start
				job, _, err := store.Admit("one-key", Request{Repository: "widget", Task: "same concurrent request"})
				if err != nil {
					errs <- err
					return
				}
				ids <- job.ID
			}(store)
		}
		close(start)
		wg.Wait()
		close(ids)
		close(errs)
		for err := range errs {
			t.Errorf("concurrent admission: %v", err)
		}
		var id string
		for candidate := range ids {
			if id == "" {
				id = candidate
			}
			if id != candidate {
				t.Fatalf("admissions returned %q and %q", id, candidate)
			}
		}
		if id == "" {
			t.Fatal("no admission returned an ID")
		}

		var claims sync.WaitGroup
		claimStart := make(chan struct{})
		results := make(chan error, 2)
		for _, store := range []*PostgresStore{first, second} {
			claims.Add(1)
			go func(store *PostgresStore) {
				defer claims.Done()
				<-claimStart
				claim, err := store.ClaimNextAttempt()
				if err == nil {
					if claim.Job.ID != id || claim.Attempt.Generation != 1 {
						results <- errors.New("claim returned unexpected identity")
					} else {
						results <- nil
					}
					return
				}
				if !errors.Is(err, ErrNoQueuedJobs) && !errors.Is(err, ErrNoWorkerSlots) {
					results <- err
					return
				}
				results <- nil
			}(store)
		}
		close(claimStart)
		claims.Wait()
		close(results)
		for err := range results {
			if err != nil {
				t.Errorf("competing claim: %v", err)
			}
		}
		var running int
		if err := db1.QueryRow(`SELECT COUNT(*) FROM factory_jobs WHERE id=$1 AND status='running'`, id).Scan(&running); err != nil || running != 1 {
			t.Fatalf("claimed running records=(%d,%v)", running, err)
		}
	})

	t.Run("operator resolution and provider reconciliation remain conservative", func(t *testing.T) {
		db := scopedPostgresDB(t, admin, dsn)
		schema := currentTestSchema(t, db)
		store, err := NewPostgresStore(db, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		plain, _, err := store.Admit("plain-interrupted", Request{Repository: "widget", Task: "operator disposition"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimNextAttempt(); err != nil {
			t.Fatal(err)
		}
		providerJob, _, err := store.Admit("provider-interrupted", Request{Repository: "widget", Task: "read-only provider confirmation"})
		if err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimNextAttempt()
		if err != nil || claim.Job.ID != providerJob.ID {
			t.Fatalf("provider claim=(%+v,%v)", claim, err)
		}
		attempt := ProviderAttempt{Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + providerJob.ID, Commit: "abcdef0", Uncertain: true}
		if err := store.RecordProviderAttemptForAttempt(claim.Attempt, attempt); err != nil {
			t.Fatal(err)
		}
		if err := store.CloseStore(); err != nil {
			t.Fatal(err)
		}
		db2 := postgresDBForSchema(t, dsn, schema)
		if _, err := db2.Exec(`UPDATE factory_rest_worker_ownership SET lease_until=clock_timestamp()-interval '1 second'`); err != nil {
			t.Fatal(err)
		}
		reopened, err := NewPostgresStore(db2, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.CloseStore()
		if err := reopened.ResolveInterrupted(plain.ID, InterruptedDispositionCanceled); err != nil {
			t.Fatalf("resolve no-side-effect interrupted job: %v", err)
		}
		if got, err := reopened.Get(plain.ID); err != nil || got.Status != StatusCanceled {
			t.Fatalf("disposition result=(%+v,%v)", got, err)
		}
		if err := reopened.ResolveInterrupted(providerJob.ID, InterruptedDispositionFailed); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("provider-side-effect disposition=%v, want rejected", err)
		}
		outcome := ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 7, URL: "https://github.com/acme/widget/pull/7", Branch: attempt.Branch, Commit: attempt.Commit, State: "open"}
		if err := reopened.ReconcileProviderOutcome(providerJob.ID, outcome); err != nil {
			t.Fatalf("reconcile provider identity: %v", err)
		}
		if got, err := reopened.Get(providerJob.ID); err != nil || got.Status != StatusSucceeded || got.Provider == nil || *got.Provider != outcome {
			t.Fatalf("reconciled result=(%+v,%v)", got, err)
		}
	})

	t.Run("expired running work remains unresolved across reopen", func(t *testing.T) {
		db := scopedPostgresDB(t, admin, dsn)
		schema := currentTestSchema(t, db)
		config := Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 4, MaxEventsPerJob: 8}
		store, err := NewPostgresStore(db, config)
		if err != nil {
			t.Fatal(err)
		}
		job, _, err := store.Admit("restart-running", Request{Repository: "widget", Task: "must not replay"})
		if err != nil {
			t.Fatal(err)
		}
		queued, _, err := store.Admit("restart-queued", Request{Repository: "widget", Task: "eligible queued work"})
		if err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimNextAttempt()
		if err != nil {
			t.Fatal(err)
		}
		// Simulate an unclean process exit so the queued job remains eligible.
		if err := store.db.Close(); err != nil {
			t.Fatal(err)
		}
		db2 := postgresDBForSchema(t, dsn, schema)
		if _, err := db2.Exec(`UPDATE factory_rest_worker_ownership SET lease_until=clock_timestamp()-interval '1 second' WHERE job_id=$1`, job.ID); err != nil {
			t.Fatal(err)
		}
		reopened, err := NewPostgresStore(db2, config)
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.CloseStore()
		if !reopened.RecoveryNeeded(job.ID) {
			t.Fatal("expired running attempt was not classified for operator review")
		}
		got, err := reopened.Get(job.ID)
		if err != nil || got.Status != StatusRunning {
			t.Fatalf("recovered job=(%+v,%v), want still running", got, err)
		}
		resumed, err := reopened.ClaimNextAttempt()
		if err != nil || resumed.Job.ID != queued.ID {
			t.Fatalf("claim queued job while recovery-needed work is held=(%+v,%v), want %q", resumed, err, queued.ID)
		}
		stillRunning, err := reopened.Get(job.ID)
		if err != nil || stillRunning.Status != StatusRunning {
			t.Fatalf("recovery-needed job after claim=(%+v,%v), want still running", stillRunning, err)
		}
		if _, err := reopened.ClaimNextAttempt(); !errors.Is(err, ErrNoQueuedJobs) && !errors.Is(err, ErrNoWorkerSlots) {
			t.Fatalf("claim after queued work consumed=%v, want no replayable jobs or worker capacity", err)
		}
		if err := reopened.HeartbeatAttempt(claim.Attempt); !errors.Is(err, ErrStaleAttempt) {
			t.Fatalf("old heartbeat=%v, want fenced", err)
		}
	})

	t.Run("close waits for an in-flight admission before canceling queued jobs", func(t *testing.T) {
		db := scopedPostgresDB(t, admin, dsn)
		store, err := NewPostgresStore(db, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.CloseStore() })
		lockTx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lockTx.Exec(`SELECT pg_advisory_xact_lock($1)`, postgresStoreMigrationLock+1); err != nil {
			t.Fatal(err)
		}
		type admissionResult struct {
			job Snapshot
			err error
		}
		admitted := make(chan admissionResult, 1)
		go func() {
			job, _, err := store.AdmitAs("close-race", "operator", Request{Repository: "widget", Task: "must not remain queued after close"})
			admitted <- admissionResult{job: job, err: err}
		}()
		deadline := time.Now().Add(5 * time.Second)
		for store.admitMu.TryLock() {
			store.admitMu.Unlock()
			if time.Now().After(deadline) {
				_ = lockTx.Rollback()
				t.Fatal("admission did not enter before timeout")
			}
			time.Sleep(5 * time.Millisecond)
		}
		closed := make(chan struct{})
		go func() {
			store.Close()
			close(closed)
		}()
		if err := lockTx.Commit(); err != nil {
			t.Fatal(err)
		}
		result := <-admitted
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not finish after admission was unblocked")
		}
		if result.err != nil {
			if !errors.Is(result.err, ErrManagerClosed) {
				t.Fatalf("admission error=%v, want success or closed", result.err)
			}
			return
		}
		got, err := store.Get(result.job.ID)
		if err != nil || got.Status != StatusCanceled {
			t.Fatalf("successful admission after Close=(%+v,%v), want canceled rather than queued", got, err)
		}
		_ = store.CloseStore()
	})

	t.Run("wait-claim contexts cancel a database-blocked claim without claiming", func(t *testing.T) {
		for _, attemptAware := range []bool{false, true} {
			name := "WaitClaim"
			if attemptAware {
				name = "WaitClaimAttempt"
			}
			t.Run(name, func(t *testing.T) {
				db := scopedPostgresDB(t, admin, dsn)
				store, err := NewPostgresStore(db, testConfig())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.CloseStore() })
				job, _, err := store.Admit("cancel-blocked-"+name, Request{Repository: "widget", Task: "canceled wait must not claim"})
				if err != nil {
					t.Fatal(err)
				}
				lockTx, err := db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := lockTx.Exec(`SELECT pg_advisory_xact_lock($1)`, postgresStoreMigrationLock+2); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				if attemptAware {
					_, err = store.WaitClaimAttempt(ctx)
				} else {
					_, err = store.WaitClaim(ctx)
				}
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("wait claim error=%v, want context deadline", err)
				}
				if err := lockTx.Rollback(); err != nil {
					t.Fatal(err)
				}
				got, err := store.Get(job.ID)
				if err != nil || got.Status != StatusQueued {
					t.Fatalf("job after canceled wait=(%+v,%v), want queued", got, err)
				}
				claim, err := store.ClaimNextAttempt()
				if err != nil || claim.Job.ID != job.ID {
					t.Fatalf("direct claim after canceled wait=(%+v,%v), want %q", claim, err, job.ID)
				}
				_ = store.CloseStore()
			})
		}
	})
}

func TestPostgresThreadNotificationAtomicTransitionAndConcurrentStores(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL 18 notification integration tests")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db1 := scopedPostgresDB(t, admin, dsn)
	schema := currentTestSchema(t, db1)
	db2 := postgresDBForSchema(t, dsn, schema)
	first, err := NewPostgresStore(db1, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewPostgresStore(db2, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseStore()
	defer second.CloseStore()
	created, _, err := first.AdmitThreadAs("notify", "alice", Request{Repository: "widget", Task: "not in notification"})
	if err != nil {
		t.Fatal(err)
	}
	follow, job, _, err := first.AdmitThreadJobAs(created.Thread.ID, "follow", "alice", Request{Repository: "widget", Task: "follow-up"})
	if err != nil || len(follow.Jobs) != 2 {
		t.Fatalf("member admission=(%+v,%v)", follow, err)
	}
	for range 2 {
		if _, err := first.ClaimNext(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db1.Exec(`CREATE FUNCTION reject_thread_notification() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected notification failure'; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := db1.Exec(`CREATE TRIGGER reject_thread_notification BEFORE INSERT ON factory_thread_notifications FOR EACH ROW EXECUTE FUNCTION reject_thread_notification()`); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, item := range []struct {
		store *PostgresStore
		id    string
	}{{first, created.Jobs[0].ID}, {second, job.ID}} {
		wg.Add(1)
		go func(s *PostgresStore, id string) { defer wg.Done(); errs <- s.Finish(id, StatusFailed) }(item.store, item.id)
	}
	wg.Wait()
	close(errs)
	failedWrites := 0
	for err := range errs {
		if err == nil {
			continue
		} // A non-final member may commit without creating a notification.
		failedWrites++
		if errors.Is(err, ErrInvalidTransition) {
			t.Errorf("final transition failed before notification persistence: %v", err)
		}
	}
	if failedWrites != 1 {
		t.Fatalf("notification failure count=%d, want only final aggregate transition aborted", failedWrites)
	}
	for _, statement := range []string{`DROP TRIGGER reject_thread_notification ON factory_thread_notifications`, `DROP FUNCTION reject_thread_notification()`} {
		if _, err := db1.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	for _, item := range []struct {
		store *PostgresStore
		id    string
	}{{first, created.Jobs[0].ID}, {second, job.ID}} {
		if err := item.store.Finish(item.id, StatusFailed); err != nil && !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("retry after notification failure: %v", err)
		}
	}
	page, err := second.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10)
	if err != nil || len(page.Notifications) != 1 {
		t.Fatalf("PostgreSQL inbox=(%+v,%v), want one Done cycle", page, err)
	}
	restartedDB := postgresDBForSchema(t, dsn, schema)
	first, err = NewPostgresStore(restartedDB, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseStore()
	page, err = first.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10)
	if err != nil || len(page.Notifications) != 1 {
		t.Fatalf("restarted PostgreSQL inbox=(%+v,%v)", page, err)
	}
	if foreign, err := first.ListNotificationsForPrincipal(context.Background(), "bob", 0, 10); err != nil || len(foreign.Notifications) != 0 {
		t.Fatalf("foreign inbox=(%+v,%v)", foreign, err)
	}
	_, reopenedJob, _, err := first.AdmitThreadJobAs(created.Thread.ID, "reopen", "alice", Request{Repository: "widget", Task: "reopen"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := first.Finish(reopenedJob.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	page, err = first.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10)
	if err != nil || len(page.Notifications) != 2 {
		t.Fatalf("reopened Done inbox=(%+v,%v), want two cycles", page, err)
	}
}

func TestPostgresWorkerIdentityAndSafeProgressIntegration(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL 18 store integration tests")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	db := scopedPostgresDB(t, admin, dsn)
	store, err := NewPostgresStore(db, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.CloseStore() })
	job, _, err := store.Admit("worker-progress", Request{Repository: "widget", Task: "bounded progress"})
	if err != nil {
		t.Fatal(err)
	}
	store.workerID, store.bootID = "worker-phase2b", "boot-test"
	claim, err := store.ClaimNextAttempt()
	if err != nil {
		t.Fatal(err)
	}
	progress := ProgressSummary{Stage: "implement", State: "started"}
	if err := store.RecordProgressForAttempt(claim.Attempt, progress); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Get(job.ID)
	if err != nil || snapshot.Progress == nil || *snapshot.Progress != progress || snapshot.Worker == nil || snapshot.Worker.WorkerID != "worker-phase2b" {
		t.Fatalf("safe metadata=%+v err=%v", snapshot, err)
	}
	var workerID, bootID string
	if err := db.QueryRow(`SELECT worker_id,boot_id FROM factory_rest_attempt_history WHERE job_id=$1`, job.ID).Scan(&workerID, &bootID); err != nil || workerID != "worker-phase2b" || bootID != "boot-test" {
		t.Fatalf("attempt attribution=%q/%q err=%v", workerID, bootID, err)
	}
	if err := store.FinishAttempt(claim.Attempt, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresStoreCloseAndHealthValidation(t *testing.T) {
	if _, err := NewPostgresStore(nil, testConfig()); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("nil db error=%v", err)
	}
	store := &PostgresStore{db: &sql.DB{}, config: testConfig()}
	if err := store.Ping(nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("nil health context=%v", err)
	}
	if _, err := store.ListJobs(context.Background(), 0, 0, 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid list request=%v", err)
	}
}

func currentTestSchema(t *testing.T, db *sql.DB) string {
	t.Helper()
	var schema string
	if err := db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	return schema
}
