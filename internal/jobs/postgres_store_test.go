package restjobs

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresProviderRecoveryReadUsesOneConsistentSnapshot(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL provider recovery read coverage")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	db := scopedPostgresDB(t, admin, dsn)
	store, err := NewPostgresStore(db, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	job, _, err := store.Admit("provider-recovery-read", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimNextAttempt()
	if err != nil {
		t.Fatal(err)
	}
	attempt := ProviderAttempt{Generation: claim.Attempt.Generation, Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + job.ID, Commit: "abc1234"}
	if err := store.RecordProviderAttemptForAttempt(claim.Attempt, attempt); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkProviderAttemptUncertainForAttempt(claim.Attempt); err != nil {
		t.Fatal(err)
	}
	attempt.Uncertain = true
	if err := store.FinishAttempt(claim.Attempt, StatusFailed); err != nil {
		t.Fatal(err)
	}
	outcome := ProviderOutcome{Provider: "github", Repository: attempt.Repository, Number: 42, URL: "https://github.com/acme/widget/pull/42", Branch: attempt.Branch, Commit: attempt.Commit, State: "open"}
	if err := store.ObserveProviderOutcome(job.ID, outcome); err != nil {
		t.Fatal(err)
	}
	got, err := store.ReadProviderRecovery(job.ID)
	if err != nil || got.Snapshot.Status != StatusFailed || got.Snapshot.Provider != nil || got.Attempt != attempt || got.Observation == nil || got.Observation.Outcome != outcome || got.Observation.ObservedAt.IsZero() {
		t.Fatalf("combined PostgreSQL recovery read=(%+v,%v), want unresolved failed snapshot and matching attempt/observation", got, err)
	}
}

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
		attempt := ProviderAttempt{Generation: claim.Attempt.Generation, Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + providerJob.ID, Commit: "abcdef0", Uncertain: true}
		if err := store.RecordProviderAttemptForAttempt(claim.Attempt, attempt); err != nil {
			t.Fatal(err)
		}
		persistedAttempt, err := store.ProviderAttempt(providerJob.ID)
		if err != nil || persistedAttempt.Generation != claim.Attempt.Generation {
			t.Fatalf("persisted PostgreSQL provider generation = %d, %v; want %d", persistedAttempt.Generation, err, claim.Attempt.Generation)
		}
		mismatched := attempt
		mismatched.Generation++
		if err := store.RecordProviderAttemptForAttempt(claim.Attempt, mismatched); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("mismatched PostgreSQL generation = %v, want ErrInvalidInput", err)
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
		if err := reopened.ReconcileProviderOutcome(providerJob.ID, outcome); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("legacy reconciliation method error=%v, want unresolved conflict", err)
		}
		if err := reopened.ObserveProviderOutcome(providerJob.ID, outcome); err != nil {
			t.Fatalf("record provider observation: %v", err)
		}
		if err := reopened.ObserveProviderOutcome(providerJob.ID, outcome); err != nil {
			t.Fatalf("repeat provider observation: %v", err)
		}
		if got, err := reopened.Get(providerJob.ID); err != nil || got.Status != StatusRunning || got.Provider != nil {
			t.Fatalf("provider observation changed job=(%+v,%v)", got, err)
		}
		observed, err := reopened.ProviderObservation(providerJob.ID)
		if err != nil || observed.Outcome != outcome || observed.ObservedAt.IsZero() {
			t.Fatalf("persisted provider observation=(%+v,%v)", observed, err)
		}
		history, err := reopened.History(providerJob.ID)
		if err != nil {
			t.Fatal(err)
		}
		observations := 0
		for _, event := range history.Events {
			if event.Type == "provider_observed" {
				observations++
			}
			if event.Type == string(StatusSucceeded) || event.Type == "provider_reconciled" {
				t.Fatalf("provider observation created success history: %+v", history.Events)
			}
		}
		if observations != 1 {
			t.Fatalf("provider observation audit events=%d history=%+v", observations, history.Events)
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

type failAfterCommitDriver struct{ base driver.Driver }
type failAfterCommitConn struct{ driver.Conn }
type failAfterCommitTx struct{ driver.Tx }

func (d failAfterCommitDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.base.Open(name)
	if err != nil {
		return nil, err
	}
	return failAfterCommitConn{Conn: conn}, nil
}
func (c failAfterCommitConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	conn, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, errors.New("underlying driver does not support BeginTx")
	}
	tx, err := conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return failAfterCommitTx{Tx: tx}, nil
}
func (tx failAfterCommitTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	return errors.New("injected response loss after successful commit")
}

func TestPostgresScheduleDispatchRetryAfterLostCommitResponseDoesNotDuplicate(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL ambiguous-commit schedule test")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db := scopedPostgresDB(t, admin, dsn)
	schema := currentTestSchema(t, db)
	config := testConfig()
	store, err := NewPostgresStore(db, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	schedule, _, err := store.CreateSchedule("alice", "ambiguous-schedule", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "ambiguous scheduled Job"})
	if err != nil {
		t.Fatal(err)
	}
	occurrence := schedule.NextOccurrence()
	if _, err := db.Exec(`UPDATE factory_schedules SET next_occurrence_at=$1 WHERE id=$2`, occurrence, schedule.ID); err != nil {
		t.Fatal(err)
	}
	name := "factory-fail-after-commit-response"
	sql.Register(name, failAfterCommitDriver{base: stdlib.GetDefaultDriver()})
	pgConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	pgConfig.RuntimeParams["search_path"] = schema
	uncertainDB, err := sql.Open(name, pgConfig.ConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer uncertainDB.Close()
	uncertainStore := &PostgresStore{db: uncertainDB, config: config, recoveryNeeded: make(map[string]struct{})}
	if err := uncertainStore.DispatchDueScheduleOccurrences(context.Background(), occurrence, 1); err == nil {
		t.Fatal("first dispatch result unexpectedly succeeded; driver must fail after committing")
	}
	if err := store.DispatchDueScheduleOccurrences(context.Background(), occurrence, 1); err != nil {
		t.Fatalf("retry after uncertain commit response: %v", err)
	}
	page, err := store.ListJobs(context.Background(), 0, 0, 8)
	if err != nil || len(page.Jobs) != 1 {
		t.Fatalf("Jobs after ambiguous retry=(%d,%v); want exactly one", len(page.Jobs), err)
	}
	job, err := store.Get(page.Jobs[0].ID)
	if err != nil || job.Request.Task != "ambiguous scheduled Job" {
		t.Fatalf("committed scheduled Job=(%+v,%v)", job, err)
	}
}

func TestPostgresScheduleRunMappingRecordsRevisionAndOccurrence(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL schedule run mapping test")
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
	defer store.CloseStore()
	schedule, _, err := store.CreateSchedule("alice", "schedule-run-mapping", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "scheduled work"})
	if err != nil {
		t.Fatal(err)
	}
	occurrence := schedule.NextOccurrence()
	if err := store.DispatchScheduleOccurrences(context.Background(), occurrence, 1); err != nil {
		t.Fatal(err)
	}
	var mappedSchedule string
	var mappedOccurrence time.Time
	var mappedRevision int64
	var jobID string
	if err := store.db.QueryRow(`SELECT schedule_id,occurrence_at,schedule_revision,job_id FROM factory_schedule_runs WHERE schedule_id=$1`, schedule.ID).Scan(&mappedSchedule, &mappedOccurrence, &mappedRevision, &jobID); err != nil {
		t.Fatal(err)
	}
	if mappedSchedule != schedule.ID || !mappedOccurrence.Equal(occurrence) || mappedRevision != schedule.Revision {
		t.Fatalf("mapping=(%s,%s,%d), want (%s,%s,%d)", mappedSchedule, mappedOccurrence, mappedRevision, schedule.ID, occurrence, schedule.Revision)
	}
	if _, err := store.db.Exec(`DELETE FROM factory_schedules WHERE id=$1`, schedule.ID); err == nil {
		t.Fatal("schedule deletion succeeded while run provenance references it; want default restrict")
	}
	page, err := store.ListScheduleRuns(schedule.ID, "alice", false, 0, 10)
	if err != nil || len(page.Runs) != 1 || page.Runs[0].JobID != jobID {
		t.Fatalf("schedule runs=(%+v,%v), want mapping-backed canonical Job", page, err)
	}
}

func TestPostgresScheduleRunsAssociateOnlyRetainedAdmittedJobs(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL schedule runs test")
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
	defer store.CloseStore()
	schedule, _, err := store.CreateSchedule("alice", "schedule-runs", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "scheduled work"})
	if err != nil {
		t.Fatal(err)
	}
	occurrence := schedule.NextOccurrence()
	forgedJob, _, err := store.AdmitAs("forged-before-schedule", "alice", Request{Repository: "widget", Task: "forged scheduled work"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE factory_job_idempotency SET key=$1 WHERE principal_id=$2 AND job_id=$3`, scheduleOccurrenceKey(schedule.ID, schedule.Revision, occurrence.Add(-time.Minute)), "alice", forgedJob.ID); err != nil {
		t.Fatal(err)
	}
	page, err := store.ListScheduleRuns(schedule.ID, "alice", false, 0, 10)
	if err != nil || len(page.Runs) != 0 {
		t.Fatalf("runs before dispatcher admission=(%+v,%v), want none despite matching key", page, err)
	}
	if err := store.DispatchScheduleOccurrences(context.Background(), occurrence, 1); err != nil {
		t.Fatal(err)
	}
	page, err = store.ListScheduleRuns(schedule.ID, "alice", false, 0, 10)
	if err != nil || len(page.Runs) != 1 || !page.Runs[0].OccurrenceAt.Equal(occurrence) {
		t.Fatalf("scheduled runs=(%+v,%v)", page, err)
	}
	var mappedSchedule string
	var mappedOccurrence time.Time
	var mappedRevision int64
	if err := store.db.QueryRow(`SELECT schedule_id,occurrence_at,schedule_revision FROM factory_schedule_runs WHERE job_id=$1`, page.Runs[0].JobID).Scan(&mappedSchedule, &mappedOccurrence, &mappedRevision); err != nil {
		t.Fatal(err)
	}
	if mappedSchedule != schedule.ID || !mappedOccurrence.Equal(occurrence) || mappedRevision != schedule.Revision {
		t.Fatalf("schedule run mapping=(%s,%s,%d), want (%s,%s,%d)", mappedSchedule, mappedOccurrence, mappedRevision, schedule.ID, occurrence, schedule.Revision)
	}
	if _, err := store.ListScheduleRuns(schedule.ID, "bob", false, 0, 10); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign schedule runs error=%v, want not found", err)
	}
	if _, err := store.db.Exec(`DELETE FROM factory_job_idempotency WHERE principal_id=$1 AND job_id=$2`, "alice", page.Runs[0].JobID); err != nil {
		t.Fatal(err)
	}
	page, err = store.ListScheduleRuns(schedule.ID, "alice", false, 0, 10)
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("run without idempotency identity=(%+v,%v), want mapping-backed run", page, err)
	}
	if _, err := store.db.Exec(`DELETE FROM factory_job_events WHERE job_id=$1`, page.Runs[0].JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`DELETE FROM factory_jobs WHERE id=$1`, page.Runs[0].JobID); err != nil {
		t.Fatal(err)
	}
	page, err = store.ListScheduleRuns(schedule.ID, "alice", false, 0, 10)
	if err != nil || len(page.Runs) != 0 {
		t.Fatalf("run after canonical Job deletion=(%+v,%v), want empty after provenance cascade", page, err)
	}
}

func TestPostgresScheduleRegistryFullRetainsPendingOccurrence(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL schedule capacity tests")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db := scopedPostgresDB(t, admin, dsn)
	config := testConfig()
	config.QueueCapacity, config.MaxConcurrentJobs, config.MaxRecords = 4, 1, 4
	store, err := NewPostgresStore(db, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	for i := range 4 {
		job, _, err := store.AdmitAs("fill-"+string(rune('a'+i)), "alice", Request{Repository: "widget", Task: "fills retained registry"})
		if err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimNextAttempt()
		if err != nil || claim.Job.ID != job.ID {
			t.Fatalf("claim=%+v err=%v", claim, err)
		}
		if err := store.FinishAttempt(claim.Attempt, StatusSucceeded); err != nil {
			t.Fatal(err)
		}
	}
	schedule, _, err := store.CreateSchedule("alice", "registry-full", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "scheduled Job"})
	if err != nil {
		t.Fatal(err)
	}
	occurrence := schedule.NextOccurrence()
	if _, err := db.Exec(`UPDATE factory_schedules SET next_occurrence_at=$1 WHERE id=$2`, occurrence, schedule.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.DispatchDueScheduleOccurrences(context.Background(), occurrence, 1); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetSchedule(schedule.ID)
	if err != nil || got.PendingOccurrence() == nil || !got.PendingOccurrence().Equal(occurrence) {
		t.Fatalf("pending occurrence=(%+v,%v); want %s", got.PendingOccurrence(), err, occurrence)
	}
	page, err := store.ListJobs(context.Background(), 0, 0, 8)
	if err != nil || len(page.Jobs) != 4 {
		t.Fatalf("retained Jobs=(%d,%v); want four capacity-filling records", len(page.Jobs), err)
	}
}

func TestPostgresScheduleStorePersistsAndEnforcesRevision(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL schedule integration tests")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db := scopedPostgresDB(t, admin, dsn)
	store, err := NewPostgresStore(db, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	input := ScheduleInput{Cron: "0 9 * * 1-5", Repository: "widget", Task: "durable task"}
	created, replay, err := store.CreateSchedule("alice", "schedule", input)
	if err != nil || replay {
		t.Fatalf("create=(%+v,%t,%v)", created, replay, err)
	}
	if got, err := store.MutateSchedule(created.ID, "alice", 1, SchedulePause, nil); err != nil || got.Status != SchedulePaused || got.Revision != 2 {
		t.Fatalf("pause=(%+v,%v)", got, err)
	}
	if _, err := store.MutateSchedule(created.ID, "alice", 1, ScheduleResume, nil); !errors.Is(err, ErrScheduleConflict) {
		t.Fatalf("stale revision error=%v", err)
	}
	if got, err := store.GetSchedule(created.ID); err != nil || got.Status != SchedulePaused {
		t.Fatalf("read schedule=(%+v,%v)", got, err)
	}
	page, err := store.ListScheduleHistory(created.ID, "admin", true, 0, MaxScheduleHistoryLimit)
	if err != nil || len(page.Events) != 2 || page.Events[0].Action != ScheduleCreated || page.Events[1].Action != SchedulePauseAction || page.Events[1].Actor != "alice" {
		t.Fatalf("schedule audit=(%+v,%v)", page, err)
	}
	if _, err := store.MutateScheduleAs(created.ID, "operator-one", true, 2, ScheduleResume, nil); err != nil {
		t.Fatalf("operator resume=%v", err)
	}
	page, err = store.ListScheduleHistory(created.ID, "admin", true, 0, MaxScheduleHistoryLimit)
	if err != nil || len(page.Events) != 3 || page.Events[2].Actor != "operator-one" || page.Events[2].Creator != "alice" {
		t.Fatalf("operator audit=(%+v,%v)", page, err)
	}
}

func TestPostgresScheduleOccurrenceAdmissionIsAtomicAcrossIndependentStores(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL for PostgreSQL schedule occurrence tests")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db1 := scopedPostgresDB(t, admin, dsn)
	schema := currentTestSchema(t, db1)
	db2 := postgresDBForSchema(t, dsn, schema)
	config := testConfig()
	config.QueueCapacity, config.MaxConcurrentJobs, config.MaxRecords = 8, 2, 16
	first, err := NewPostgresStore(db1, config)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewPostgresStore(db2, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if first != nil {
			_ = first.CloseStore()
		}
	}()
	defer second.CloseStore()
	schedule, _, err := first.CreateSchedule("alice", "occurrence", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "one occurrence"})
	if err != nil {
		t.Fatal(err)
	}
	occurrence := schedule.NextOccurrence()
	if _, err := db1.Exec(`UPDATE factory_schedules SET next_occurrence_at=$1 WHERE id=$2`, occurrence, schedule.ID); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, store := range []*PostgresStore{first, second} {
		go func(store *PostgresStore) {
			<-start
			errs <- store.DispatchDueScheduleOccurrences(context.Background(), occurrence, 16)
		}(store)
	}
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	jobs, err := first.ListJobs(context.Background(), 0, 0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Jobs) != 1 {
		t.Fatalf("jobs=%d, want exactly one occurrence Job", len(jobs.Jobs))
	}
	if err := first.CloseStore(); err != nil {
		t.Fatal(err)
	}
	first = nil
	first, err = NewPostgresStore(postgresDBForSchema(t, dsn, schema), config)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := first.GetSchedule(schedule.ID)
	if err != nil || reopened.PendingOccurrence() != nil || reopened.NextOccurrence().Before(occurrence) {
		t.Fatalf("reopened schedule=(%+v,%v)", reopened, err)
	}
	job, err := second.Get(jobs.Jobs[0].ID)
	if err != nil || job.Submitter != "alice" || job.Request.Task != "one occurrence" {
		t.Fatalf("job=(%+v,%v)", job, err)
	}
}

func TestPostgresScheduleMutationAndDispatchSerializeWithCurrentRevision(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL schedule mutation tests")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db := scopedPostgresDB(t, admin, dsn)
	config := testConfig()
	config.QueueCapacity, config.MaxConcurrentJobs, config.MaxRecords = 4, 1, 8
	store, err := NewPostgresStore(db, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	schedule, _, err := store.CreateSchedule("alice", "schedule-race", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "old task"})
	if err != nil {
		t.Fatal(err)
	}
	occurrence := schedule.NextOccurrence()
	if _, err := db.Exec(`UPDATE factory_schedules SET next_occurrence_at=$1 WHERE id=$2`, occurrence, schedule.ID); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	type outcome struct {
		mutation bool
		err      error
	}
	results := make(chan outcome, 2)
	go func() {
		<-start
		_, err := store.MutateSchedule(schedule.ID, "alice", schedule.Revision, ScheduleEdit, &ScheduleInput{Cron: "0 9 * * *", Repository: "widget", Task: "new task"})
		results <- outcome{mutation: true, err: err}
	}()
	go func() {
		<-start
		results <- outcome{err: store.DispatchDueScheduleOccurrences(context.Background(), occurrence, 1)}
	}()
	close(start)
	for range 2 {
		if result := <-results; result.err != nil {
			t.Fatalf("race participant (mutation=%t) failed: %v", result.mutation, result.err)
		}
	}
	current, err := store.GetSchedule(schedule.ID)
	if err != nil || current.Revision != schedule.Revision+1 || current.Task != "new task" {
		t.Fatalf("schedule after race=(%+v,%v); want user edit at revision 2", current, err)
	}
	page, err := store.ListJobs(context.Background(), 0, 0, 8)
	if err != nil || len(page.Jobs) > 1 {
		t.Fatalf("jobs after race=(%+v,%v); want zero or one old occurrence", page.Jobs, err)
	}
	if len(page.Jobs) == 1 {
		job, err := store.Get(page.Jobs[0].ID)
		if err != nil || job.Request.Task != "old task" {
			t.Fatalf("committed occurrence=(%+v,%v); want immutable old task", job, err)
		}
	}
}

func TestPostgresThreadAdmissionAndDispatchShareCapacityGate(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL admission concurrency tests")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db1 := scopedPostgresDB(t, admin, dsn)
	schema := currentTestSchema(t, db1)
	store1, err := NewPostgresStore(db1, Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	store2, err := NewPostgresStore(postgresDBForSchema(t, dsn, schema), Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store1.CloseStore()
	defer store2.CloseStore()
	thread, _, err := store1.AdmitThreadAs("capacity-thread", "alice", Request{Repository: "widget", Task: "initial Thread job"})
	if err != nil {
		t.Fatal(err)
	}
	schedule, _, err := store1.CreateSchedule("alice", "capacity-schedule", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "scheduled job"})
	if err != nil {
		t.Fatal(err)
	}
	occurrence := schedule.NextOccurrence()
	if _, err := db1.Exec(`UPDATE factory_schedules SET next_occurrence_at=$1 WHERE id=$2`, occurrence, schedule.ID); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		_, _, _, err := store2.AdmitThreadJobAs(thread.Thread.ID, "member", "alice", Request{Repository: "widget", Task: "Thread member job"})
		results <- err
	}()
	go func() {
		<-start
		results <- store1.DispatchDueScheduleOccurrences(context.Background(), occurrence, 1)
	}()
	close(start)
	var queueFull int
	for range 2 {
		if err := <-results; err != nil {
			if errors.Is(err, ErrQueueFull) {
				queueFull++
				continue
			}
			t.Fatalf("concurrent admission/dispatch: %v", err)
		}
	}
	page, err := store1.ListJobs(context.Background(), 0, 0, 8)
	if err != nil || len(page.Jobs) != 2 || queueFull > 1 {
		t.Fatalf("retained jobs=(%d,%v), queueFull=%d; want initial Thread Job plus at most one competing admission", len(page.Jobs), err, queueFull)
	}
	current, err := store1.GetSchedule(schedule.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.PendingOccurrence() == nil {
		// If dispatch won the gate, the Thread admission must have seen full capacity.
		var memberCount int
		if err := db1.QueryRow(`SELECT COUNT(*) FROM factory_thread_jobs WHERE thread_id=$1`, thread.Thread.ID).Scan(&memberCount); err != nil || memberCount != 1 || queueFull != 1 {
			t.Fatalf("dispatch won but Thread member count=%d queueFull=%d err=%v; want no appended member and one capacity rejection", memberCount, queueFull, err)
		}
	} else if !current.PendingOccurrence().Equal(occurrence) {
		t.Fatalf("pending occurrence=%s; want %s", current.PendingOccurrence(), occurrence)
	} else {
		// If the member admission won, dispatch must preserve its oldest occurrence.
		var memberCount int
		if err := db1.QueryRow(`SELECT COUNT(*) FROM factory_thread_jobs WHERE thread_id=$1`, thread.Thread.ID).Scan(&memberCount); err != nil || memberCount != 2 {
			t.Fatalf("Thread admission won but member count=%d err=%v; want appended member", memberCount, err)
		}
	}
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
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
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
