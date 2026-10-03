package restjobs

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestLocalManagerRecordsProviderOutcomeBeforeTerminalSuccess(t *testing.T) {
	store, err := NewManager(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Admit("provider", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordProviderOutcome(job.ID, ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 5, URL: "https://github.com/acme/widget/pull/5", Branch: "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb", Commit: "abc123", State: "open"}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("record while queued = %v", err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	want := ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 5, URL: "https://github.com/acme/widget/pull/5", Branch: "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb", Commit: "abc123", State: "open"}
	if err := store.RecordProviderOutcome(job.ID, want); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(job.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(job.ID)
	if err != nil || got.Provider == nil || *got.Provider != want {
		t.Fatalf("persisted outcome = %+v, %v", got.Provider, err)
	}
}

func TestSQLiteExplicitReconciliationIsAtomicAndIdempotent(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	store, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Admit("reconcile", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	attempt := ProviderAttempt{Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + job.ID, Commit: "abc1234"}
	if err := store.RecordProviderAttempt(job.ID, attempt); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkProviderAttemptUncertain(job.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(job.ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	attempt.Uncertain = true
	if got, err := store.ProviderAttempt(job.ID); err != nil || got != attempt {
		t.Fatalf("persisted attempt = %+v, %v", got, err)
	}
	outcome := ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 71, URL: "https://github.com/acme/widget/pull/71", Branch: attempt.Branch, Commit: attempt.Commit, State: "open"}
	if err := store.ReconcileProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("reconcile confirmed outcome: %v", err)
	}
	if err := store.ReconcileProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("repeat reconciliation: %v", err)
	}
	got, err := store.Get(job.ID)
	if err != nil || got.Status != StatusSucceeded || got.Provider == nil || *got.Provider != outcome {
		t.Fatalf("reconciled job = %+v, %v", got, err)
	}
	history, err := store.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range history.Events {
		if event.Type == "provider_reconciled" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("provider reconciliation audit events=%d history=%+v", count, history.Events)
	}
}

func TestSQLiteReconciliationCompletesInterruptedRunningJobOnlyAfterProviderConfirmation(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	seed, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer seed.CloseStore()
	job, _, err := seed.Admit("interrupted-provider", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	attempt := ProviderAttempt{Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + job.ID, Commit: "abc1234"}
	if err := seed.RecordProviderAttempt(job.ID, attempt); err != nil {
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
	defer store.CloseStore()
	outcome := ProviderOutcome{Provider: "github", Repository: attempt.Repository, Number: 72, URL: "https://github.com/acme/widget/pull/72", Branch: attempt.Branch, Commit: attempt.Commit, State: "open"}
	if err := store.ReconcileProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("reconcile interrupted job: %v", err)
	}
	if err := store.ReconcileProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("repeat confirmed reconciliation: %v", err)
	}
	got, err := store.Get(job.ID)
	if err != nil || got.Status != StatusSucceeded || got.Provider == nil || *got.Provider != outcome || got.Verification == nil {
		t.Fatalf("reconciled interrupted job = %+v, %v", got, err)
	}
	report, err := store.Recover(t.Context())
	if err != nil || len(report.NeedsOperator) != 0 || len(report.Terminal) != 1 {
		t.Fatalf("recovery after confirmation = (%+v, %v)", report, err)
	}
	history, err := store.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range history.Events {
		if event.Type == "provider_reconciled" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("provider reconciliation audit events=%d history=%+v", count, history.Events)
	}
}

func TestSQLiteReconciliationAcceptsOnlyPersistedInterruptedProviderOutcome(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	seed, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer seed.CloseStore()
	job, _, err := seed.Admit("interrupted-provider-outcome", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	outcome := ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 72, URL: "https://github.com/acme/widget/pull/72", Branch: "factory/job/" + job.ID, Commit: "abc1234", State: "open"}
	if err := seed.RecordProviderOutcome(job.ID, outcome); err != nil {
		t.Fatal(err)
	}
	if err := seed.db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	if err := store.ReconcileProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("confirm already-persisted provider outcome: %v", err)
	}
	got, err := store.Get(job.ID)
	if err != nil || got.Status != StatusSucceeded || got.Provider == nil || *got.Provider != outcome {
		t.Fatalf("persisted-outcome reconciliation=(%+v,%v)", got, err)
	}
}

func TestSQLiteConcurrentInterruptedProviderReconciliationIsIdempotent(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	first, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := first.Admit("concurrent-interrupted-provider", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	attempt := ProviderAttempt{Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + job.ID, Commit: "abc1234"}
	if err := first.RecordProviderAttempt(job.ID, attempt); err != nil {
		t.Fatal(err)
	}
	if err := first.db.Close(); err != nil {
		t.Fatal(err)
	}
	first, err = OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseStore()
	second, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer second.CloseStore()
	outcome := ProviderOutcome{Provider: "github", Repository: attempt.Repository, Number: 72, URL: "https://github.com/acme/widget/pull/72", Branch: attempt.Branch, Commit: attempt.Commit, State: "open"}
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, store := range []*SQLiteStore{first, second} {
		go func(store *SQLiteStore) {
			<-start
			results <- store.ReconcileProviderOutcome(job.ID, outcome)
		}(store)
	}
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("concurrent identical reconciliation: %v", err)
		}
	}
	got, err := first.Get(job.ID)
	if err != nil || got.Status != StatusSucceeded || got.Provider == nil || *got.Provider != outcome {
		t.Fatalf("concurrent reconciliation result=(%+v,%v)", got, err)
	}
	history, err := first.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	reconciled := 0
	for _, event := range history.Events {
		if event.Type == "provider_reconciled" {
			reconciled++
		}
	}
	if reconciled != 1 {
		t.Fatalf("concurrent reconciliation audit events=%d history=%+v", reconciled, history)
	}
}

func TestSQLiteInterruptedReconciliationIsAtomicAndConflictsWithDisposition(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	seed, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer seed.CloseStore()
	job, _, err := seed.Admit("interrupted-atomic", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	attempt := ProviderAttempt{Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + job.ID, Commit: "abc1234"}
	if err := seed.RecordProviderAttempt(job.ID, attempt); err != nil {
		t.Fatal(err)
	}
	if err := seed.db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	before, err := store.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER reject_provider_reconcile BEFORE INSERT ON factory_job_events WHEN NEW.type='provider_reconciled' BEGIN SELECT RAISE(ABORT,'injected audit failure'); END`); err != nil {
		t.Fatal(err)
	}
	outcome := ProviderOutcome{Provider: "github", Repository: attempt.Repository, Number: 72, URL: "https://github.com/acme/widget/pull/72", Branch: attempt.Branch, Commit: attempt.Commit, State: "open"}
	if err := store.ReconcileProviderOutcome(job.ID, outcome); err == nil {
		t.Fatal("interrupted reconciliation committed after audit-write failure")
	}
	got, err := store.Get(job.ID)
	if err != nil || got.Status != StatusRunning || got.Provider != nil {
		t.Fatalf("failed provider reconciliation changed job = (%+v, %v)", got, err)
	}
	after, err := store.History(job.ID)
	if err != nil || len(after.Events) != len(before.Events) {
		t.Fatalf("failed provider reconciliation changed history: before=%+v after=%+v err=%v", before, after, err)
	}
	report, err := store.Recover(t.Context())
	if err != nil || len(report.NeedsOperator) != 1 {
		t.Fatalf("failed provider reconciliation cleared recovery classification: (%+v, %v)", report, err)
	}
	if _, err := store.db.Exec(`DROP TRIGGER reject_provider_reconcile`); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("reconcile after removing audit fault: %v", err)
	}
	conflicting := outcome
	conflicting.Number++
	conflicting.URL = "https://github.com/acme/widget/pull/73"
	if err := store.ReconcileProviderOutcome(job.ID, conflicting); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("stale provider reconciliation with different identity=%v; want invalid transition", err)
	}
	got, err = store.Get(job.ID)
	if err != nil || got.Status != StatusSucceeded || got.Provider == nil || *got.Provider != outcome {
		t.Fatalf("stale provider reconciliation changed success: (%+v, %v)", got, err)
	}
}

func TestSQLiteExplicitReconciliationAcceptsConfirmedAttemptAndRejectsMissingAttempt(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "jobs.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	job, _, err := store.Admit("confirmed-attempt", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = store.ClaimNext()
	attempt := ProviderAttempt{Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + job.ID, Commit: "abc1234"}
	if err := store.RecordProviderAttempt(job.ID, attempt); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(job.ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	outcome := ProviderOutcome{Provider: "github", Repository: attempt.Repository, Number: 71, URL: "https://github.com/acme/widget/pull/71", Branch: attempt.Branch, Commit: attempt.Commit, State: "open"}
	if err := store.ReconcileProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("reconcile matching recorded attempt: %v", err)
	}
	if err := store.ReconcileProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("repeat reconciliation: %v", err)
	}
	got, err := store.Get(job.ID)
	if err != nil || got.Status != StatusSucceeded || got.Provider == nil || *got.Provider != outcome {
		t.Fatalf("reconciled confirmed-attempt job = %+v, %v", got, err)
	}
	history, err := store.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	reconciledEvents := 0
	for _, event := range history.Events {
		if event.Type == "provider_reconciled" {
			reconciledEvents++
		}
	}
	if reconciledEvents != 1 {
		t.Fatalf("reconciliation audit events=%d history=%+v", reconciledEvents, history.Events)
	}

	withoutAttempt, _, err := store.Admit("missing-attempt", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(withoutAttempt.ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	missingOutcome := outcome
	missingOutcome.Branch = "factory/job/" + withoutAttempt.ID
	if err := store.ReconcileProviderOutcome(withoutAttempt.ID, missingOutcome); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("missing-attempt reconciliation error=%v, want ErrInvalidTransition", err)
	}
	unchanged, err := store.Get(withoutAttempt.ID)
	if err != nil || unchanged.Status != StatusFailed || unchanged.Provider != nil {
		t.Fatalf("missing-attempt reconciliation changed job: %+v %v", unchanged, err)
	}
}

func TestSQLiteExplicitReconciliationFailsClosedOnIdentityMismatch(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "jobs.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	job, _, err := store.Admit("reconcile-mismatch", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = store.ClaimNext()
	attempt := ProviderAttempt{Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + job.ID, Commit: "abc1234"}
	if err := store.RecordProviderAttempt(job.ID, attempt); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkProviderAttemptUncertain(job.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(job.ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	outcome := ProviderOutcome{Provider: "github", Repository: attempt.Repository, Number: 71, URL: "https://github.com/acme/widget/pull/71", Branch: attempt.Branch, Commit: "different", State: "open"}
	if err := store.ReconcileProviderOutcome(job.ID, outcome); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("mismatched commit error=%v, want invalid input", err)
	}
	got, err := store.Get(job.ID)
	if err != nil || got.Status != StatusFailed || got.Provider != nil {
		t.Fatalf("mismatch changed job state: %+v %v", got, err)
	}
}

func TestLocalManagerRejectsUnsafeProviderOutcome(t *testing.T) {
	store, _ := NewManager(testConfig())
	job, _, _ := store.Admit("provider", Request{Repository: "widget", Task: "task"})
	_, _ = store.ClaimNext()
	for _, outcome := range []ProviderOutcome{{Provider: "github", Repository: "acme/widget", Number: 5, URL: "http://github.com/acme/widget/pull/5", Branch: "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb", Commit: "sha", State: "open"}, {Provider: "github", Repository: "acme/widget", Number: 5, URL: "https://evil.test/acme/widget/pull/5", Branch: "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb", Commit: "sha", State: "open"}} {
		if err := store.RecordProviderOutcome(job.ID, outcome); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("unsafe outcome error = %v", err)
		}
	}
}
