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

func TestSQLiteExplicitReconciliationRejectsUncertainAttemptWithoutConfirmation(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "jobs.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	job, _, err := store.Admit("not-uncertain", Request{Repository: "widget", Task: "task"})
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
	if err := store.ReconcileProviderOutcome(job.ID, outcome); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("non-uncertain reconciliation error=%v", err)
	}
	if err := store.MarkProviderAttemptUncertain(job.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("mark failed attempt uncertain error=%v", err)
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
