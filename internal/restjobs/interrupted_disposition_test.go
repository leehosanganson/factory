package restjobs

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func TestSQLiteStoreResolvesInterruptedJobsWithoutProviderAttempt(t *testing.T) {
	for _, disposition := range []InterruptedDisposition{InterruptedDispositionFailed, InterruptedDispositionCanceled} {
		t.Run(string(disposition), func(t *testing.T) {
			path := filepath.Join(privateSQLiteDir(t), "jobs.db")
			seed, err := OpenSQLiteStore(path, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			job, _, err := seed.Admit("interrupted", Request{Repository: "widget", Task: "retain this task"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := seed.ClaimNext(); err != nil {
				t.Fatal(err)
			}
			if err := seed.AddEvent(job.ID, "verification", "prior evidence remains"); err != nil {
				t.Fatal(err)
			}
			if err := seed.RecordVerificationEvidence(job.ID, VerificationEvidence{Limitations: []string{LimitationAgentNotVerdict}}); err != nil {
				t.Fatal(err)
			}
			if err := seed.db.Close(); err != nil {
				t.Fatal(err)
			}

			store, err := OpenSQLiteStore(path, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			if err := store.ResolveInterrupted(job.ID, disposition); err != nil {
				t.Fatalf("resolve interrupted job: %v", err)
			}
			if err := store.CloseStore(); err != nil {
				t.Fatal(err)
			}
			store, err = OpenSQLiteStore(path, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			defer store.CloseStore()
			if err := store.ResolveInterrupted(job.ID, disposition); err != nil {
				t.Fatalf("repeat identical resolution after restart: %v", err)
			}
			got, err := store.Get(job.ID)
			if err != nil || got.Status != Status(disposition) || got.Provider != nil || got.Verification == nil || len(got.Verification.Limitations) != 1 {
				t.Fatalf("resolved job = (%+v, %v); disposition/evidence not retained", got, err)
			}
			report, err := store.Recover(t.Context())
			if err != nil || len(report.NeedsOperator) != 0 || len(report.Terminal) != 1 || report.Terminal[0].ID != job.ID {
				t.Fatalf("recovery report after disposition = (%+v, %v)", report, err)
			}
			summary, err := store.OperationalSummary(t.Context())
			if err != nil || summary.RecoveryNeeded != 0 || summary.Running != 0 {
				t.Fatalf("summary after disposition = (%+v, %v)", summary, err)
			}
			history, err := store.History(job.ID)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, event := range history.Events {
				if event.Type == "operator_disposition" {
					count++
					if event.Message != string(disposition) {
						t.Errorf("disposition event = %+v; want fixed selected disposition", event)
					}
				}
			}
			if count != 1 {
				t.Fatalf("operator disposition audit events=%d history=%+v", count, history.Events)
			}
		})
	}
}

func TestSQLiteStoreRejectsInterruptedDispositionWithProviderSideEffect(t *testing.T) {
	for _, sideEffect := range []string{"attempt", "outcome"} {
		t.Run(sideEffect, func(t *testing.T) {
			path := filepath.Join(privateSQLiteDir(t), "jobs.db")
			seed, err := OpenSQLiteStore(path, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			job, _, err := seed.Admit("provider-interrupted", Request{Repository: "widget", Task: "task"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := seed.ClaimNext(); err != nil {
				t.Fatal(err)
			}
			attempt := ProviderAttempt{Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + job.ID, Commit: "abc1234"}
			if sideEffect == "attempt" {
				if err := seed.RecordProviderAttempt(job.ID, attempt); err != nil {
					t.Fatal(err)
				}
			} else {
				outcome := ProviderOutcome{Provider: "github", Repository: attempt.Repository, Number: 7, URL: "https://github.com/acme/widget/pull/7", Branch: attempt.Branch, Commit: attempt.Commit, State: "open"}
				if err := seed.RecordProviderOutcome(job.ID, outcome); err != nil {
					t.Fatal(err)
				}
			}
			if err := seed.db.Close(); err != nil {
				t.Fatal(err)
			}
			store, err := OpenSQLiteStore(path, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			defer store.CloseStore()
			if err := store.ResolveInterrupted(job.ID, InterruptedDispositionFailed); !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("disposition with persisted provider %s = %v; want ErrInvalidTransition", sideEffect, err)
			}
			got, err := store.Get(job.ID)
			if err != nil || got.Status != StatusRunning {
				t.Fatalf("rejected disposition changed job = (%+v, %v)", got, err)
			}
			report, err := store.Recover(t.Context())
			if err != nil || len(report.NeedsOperator) != 1 || report.NeedsOperator[0].ID != job.ID {
				t.Fatalf("rejected disposition cleared recovery = (%+v, %v)", report, err)
			}
		})
	}
}

func TestSQLiteStoreRejectsConflictingOrStaleInterruptedDispositions(t *testing.T) {
	store := interruptedSQLiteJob(t)
	defer store.CloseStore()
	job, err := store.Recover(t.Context())
	if err != nil || len(job.NeedsOperator) != 1 {
		t.Fatalf("recovery report = (%+v, %v)", job, err)
	}
	id := job.NeedsOperator[0].ID
	if err := store.ResolveInterrupted(id, InterruptedDispositionFailed); err != nil {
		t.Fatal(err)
	}
	if err := store.ResolveInterrupted(id, InterruptedDispositionCanceled); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("conflicting resolution = %v; want ErrInvalidTransition", err)
	}
	if err := store.ResolveInterrupted("missing-job", InterruptedDispositionFailed); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing-job resolution = %v; want ErrNotFound", err)
	}
	if err := store.ResolveInterrupted(id, InterruptedDisposition("succeeded")); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unsupported success disposition = %v; want ErrInvalidInput", err)
	}
}

func TestSQLiteStoreInterruptedDispositionIsAtomicOnHistoryFailure(t *testing.T) {
	store := interruptedSQLiteJob(t)
	defer store.CloseStore()
	report, err := store.Recover(t.Context())
	if err != nil || len(report.NeedsOperator) != 1 {
		t.Fatalf("recovery report = (%+v, %v)", report, err)
	}
	id := report.NeedsOperator[0].ID
	before, err := store.History(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER reject_disposition_event BEFORE INSERT ON factory_job_events WHEN NEW.type='operator_disposition' BEGIN SELECT RAISE(ABORT,'injected audit failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := store.ResolveInterrupted(id, InterruptedDispositionCanceled); err == nil {
		t.Fatal("resolution succeeded despite injected audit-write failure")
	}
	got, err := store.Get(id)
	if err != nil || got.Status != StatusRunning {
		t.Fatalf("atomicity failure changed job status = (%+v, %v)", got, err)
	}
	after, err := store.History(id)
	if err != nil || len(after.Events) != len(before.Events) {
		t.Fatalf("atomicity failure changed history: before=%+v after=%+v err=%v", before, after, err)
	}
	report, err = store.Recover(t.Context())
	if err != nil || len(report.NeedsOperator) != 1 || report.NeedsOperator[0].ID != id {
		t.Fatalf("atomicity failure cleared recovery state = (%+v, %v)", report, err)
	}
}

func TestSQLiteStoreConcurrentInterruptedDispositionUsesCompareAndSet(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	first := interruptedSQLiteJobAt(t, path)
	defer first.CloseStore()
	second, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer second.CloseStore()
	report, err := first.Recover(t.Context())
	if err != nil || len(report.NeedsOperator) != 1 {
		t.Fatalf("recovery report = (%+v, %v)", report, err)
	}
	id := report.NeedsOperator[0].ID
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, request := range []struct {
		store       *SQLiteStore
		disposition InterruptedDisposition
	}{{first, InterruptedDispositionFailed}, {second, InterruptedDispositionCanceled}} {
		wg.Add(1)
		go func(store *SQLiteStore, disposition InterruptedDisposition) {
			defer wg.Done()
			<-start
			results <- store.ResolveInterrupted(id, disposition)
		}(request.store, request.disposition)
	}
	close(start)
	wg.Wait()
	close(results)
	wins, conflicts := 0, 0
	for err := range results {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrInvalidTransition):
			conflicts++
		default:
			t.Errorf("concurrent resolution error = %v", err)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("concurrent resolutions: winner=%d conflict=%d, want one each", wins, conflicts)
	}
	got, err := first.Get(id)
	if err != nil || (got.Status != StatusFailed && got.Status != StatusCanceled) {
		t.Fatalf("final concurrent disposition = (%+v, %v)", got, err)
	}
}

func TestSQLiteStoreFinishCannotClearStartupRecoveryClassification(t *testing.T) {
	store := interruptedSQLiteJob(t)
	report, err := store.Recover(t.Context())
	if err != nil || len(report.NeedsOperator) != 1 {
		t.Fatalf("recovery report = (%+v, %v)", report, err)
	}
	id := report.NeedsOperator[0].ID
	if err := store.Finish(id, StatusFailed); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("worker Finish on startup recovery job = %v; want ErrInvalidTransition", err)
	}
	got, err := store.Get(id)
	if err != nil || got.Status != StatusRunning {
		t.Fatalf("worker Finish changed startup recovery job = (%+v, %v)", got, err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteStoreRejectsDispositionForRunningJobNotFoundAtStartup(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "jobs.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	job, _, err := store.Admit("not-recovery-needed", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := store.ResolveInterrupted(job.ID, InterruptedDispositionFailed); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("disposition for current worker = %v; want ErrInvalidTransition", err)
	}
	got, err := store.Get(job.ID)
	if err != nil || got.Status != StatusRunning {
		t.Fatalf("stale disposition changed active job = (%+v, %v)", got, err)
	}
}

func TestLocalManagerRejectsInterruptedDisposition(t *testing.T) {
	store, err := NewManager(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Admit("memory-recovery", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := store.ResolveInterrupted(job.ID, InterruptedDispositionFailed); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("memory disposition = %v; want ErrInvalidTransition", err)
	}
}

func interruptedSQLiteJob(t *testing.T) *SQLiteStore {
	t.Helper()
	return interruptedSQLiteJobAt(t, filepath.Join(privateSQLiteDir(t), "jobs.db"))
}

func interruptedSQLiteJobAt(t *testing.T, path string) *SQLiteStore {
	t.Helper()
	seed, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = seed.Admit("disposition", Request{Repository: "widget", Task: "task"})
	if err != nil {
		_ = seed.CloseStore()
		t.Fatal(err)
	}
	if _, err := seed.ClaimNext(); err != nil {
		_ = seed.CloseStore()
		t.Fatal(err)
	}
	if err := seed.db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = store.CloseStore()
	})
	return store
}
