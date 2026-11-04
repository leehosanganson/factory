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

func TestSQLiteProviderObservationIsAtomicAndIdempotent(t *testing.T) {
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
	if err := store.ObserveProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("record provider observation: %v", err)
	}
	if err := store.ObserveProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("repeat provider observation: %v", err)
	}
	got, err := store.Get(job.ID)
	if err != nil || got.Status != StatusFailed || got.Provider != nil {
		t.Fatalf("provider observation resolved job = %+v, %v", got, err)
	}
	observation, err := store.ProviderObservation(job.ID)
	if err != nil || observation.Outcome != outcome || observation.ObservedAt.IsZero() {
		t.Fatalf("persisted provider observation = (%+v,%v)", observation, err)
	}
	history, err := store.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range history.Events {
		if event.Type == "provider_observed" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("provider observation audit events=%d history=%+v", count, history.Events)
	}
}

func TestSQLiteProviderObservationRetainsInterruptedRunningClassification(t *testing.T) {
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
	if err := store.ObserveProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("record provider observation: %v", err)
	}
	if err := store.ObserveProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("repeat provider observation: %v", err)
	}
	got, err := store.Get(job.ID)
	if err != nil || got.Status != StatusRunning || got.Provider != nil || got.Verification == nil {
		t.Fatalf("provider observation changed interrupted job = %+v, %v", got, err)
	}
	report, err := store.Recover(t.Context())
	if err != nil || len(report.NeedsOperator) != 1 || len(report.Terminal) != 0 {
		t.Fatalf("recovery after observation = (%+v, %v)", report, err)
	}
	observation, err := store.ProviderObservation(job.ID)
	if err != nil || observation.Outcome != outcome || observation.ObservedAt.IsZero() {
		t.Fatalf("persisted provider observation = (%+v,%v)", observation, err)
	}
	history, err := store.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range history.Events {
		if event.Type == "provider_observed" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("provider observation audit events=%d history=%+v", count, history.Events)
	}
}

func TestSQLiteProviderObservationRejectsExistingProviderOutcome(t *testing.T) {
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
	if err := store.ObserveProviderOutcome(job.ID, outcome); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("persisted provider outcome is not an unresolved attempt: %v", err)
	}
	got, err := store.Get(job.ID)
	if err != nil || got.Status != StatusRunning || got.Provider == nil || *got.Provider != outcome {
		t.Fatalf("provider observation changed the already-persisted provider outcome=(%+v,%v)", got, err)
	}
}

func TestSQLiteConcurrentInterruptedProviderObservationIsIdempotent(t *testing.T) {
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
			results <- store.ObserveProviderOutcome(job.ID, outcome)
		}(store)
	}
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("concurrent identical reconciliation: %v", err)
		}
	}
	got, err := first.Get(job.ID)
	if err != nil || got.Status != StatusRunning || got.Provider != nil {
		t.Fatalf("concurrent provider observations changed job=(%+v,%v)", got, err)
	}
	observation, err := first.ProviderObservation(job.ID)
	if err != nil || observation.Outcome != outcome {
		t.Fatalf("concurrent provider observation=(%+v,%v)", observation, err)
	}
	history, err := first.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	observed := 0
	for _, event := range history.Events {
		if event.Type == "provider_observed" {
			observed++
		}
	}
	if observed != 1 {
		t.Fatalf("concurrent observation audit events=%d history=%+v", observed, history)
	}
}

func TestSQLiteInterruptedProviderObservationIsAtomicAndConflictsWithDisposition(t *testing.T) {
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
	if _, err := store.db.Exec(`CREATE TRIGGER reject_provider_reconcile BEFORE INSERT ON factory_job_events WHEN NEW.type='provider_observed' BEGIN SELECT RAISE(ABORT,'injected audit failure'); END`); err != nil {
		t.Fatal(err)
	}
	outcome := ProviderOutcome{Provider: "github", Repository: attempt.Repository, Number: 72, URL: "https://github.com/acme/widget/pull/72", Branch: attempt.Branch, Commit: attempt.Commit, State: "open"}
	if err := store.ObserveProviderOutcome(job.ID, outcome); err == nil {
		t.Fatal("provider observation committed after audit-write failure")
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
	if err := store.ObserveProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("record observation after removing audit fault: %v", err)
	}
	conflicting := outcome
	conflicting.Number++
	conflicting.URL = "https://github.com/acme/widget/pull/73"
	if err := store.ObserveProviderOutcome(job.ID, conflicting); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("conflicting provider observation=%v; want invalid transition", err)
	}
	got, err = store.Get(job.ID)
	if err != nil || got.Status != StatusRunning || got.Provider != nil {
		t.Fatalf("provider observation resolved job: (%+v, %v)", got, err)
	}
	observation, err := store.ProviderObservation(job.ID)
	if err != nil || observation.Outcome != outcome {
		t.Fatalf("observation did not survive successful retry=(%+v,%v)", observation, err)
	}
}

func TestSQLiteProviderObservationRequiresAttemptAndRetainsFailedState(t *testing.T) {
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
	if err := store.ObserveProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("record matching provider observation: %v", err)
	}
	if err := store.ObserveProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("repeat provider observation: %v", err)
	}
	got, err := store.Get(job.ID)
	if err != nil || got.Status != StatusFailed || got.Provider != nil {
		t.Fatalf("provider observation resolved failed job = %+v, %v", got, err)
	}
	observation, err := store.ProviderObservation(job.ID)
	if err != nil || observation.Outcome != outcome {
		t.Fatalf("observation was not retained = (%+v,%v)", observation, err)
	}
	history, err := store.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	observedEvents := 0
	for _, event := range history.Events {
		if event.Type == "provider_observed" {
			observedEvents++
		}
	}
	if observedEvents != 1 {
		t.Fatalf("provider observation audit events=%d history=%+v", observedEvents, history.Events)
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
	if err := store.ObserveProviderOutcome(withoutAttempt.ID, missingOutcome); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("missing-attempt reconciliation error=%v, want ErrInvalidTransition", err)
	}
	unchanged, err := store.Get(withoutAttempt.ID)
	if err != nil || unchanged.Status != StatusFailed || unchanged.Provider != nil {
		t.Fatalf("missing-attempt reconciliation changed job: %+v %v", unchanged, err)
	}
}

func TestSQLiteProviderObservationFailsClosedOnIdentityMismatch(t *testing.T) {
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
	if err := store.ObserveProviderOutcome(job.ID, outcome); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("mismatched commit error=%v, want invalid input", err)
	}
	got, err := store.Get(job.ID)
	if err != nil || got.Status != StatusFailed || got.Provider != nil {
		t.Fatalf("mismatch changed job state: %+v %v", got, err)
	}
}

func TestSQLiteProviderObservationIsSeparateAtomicAndIdempotent(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "jobs.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	job, _, err := store.Admit("observation-storage", Request{Repository: "widget", Task: "task"})
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
	if err := store.Finish(job.ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	outcome := ProviderOutcome{Provider: "github", Repository: attempt.Repository, Number: 71, URL: "https://github.com/acme/widget/pull/71", Branch: attempt.Branch, Commit: attempt.Commit, State: "open"}
	if _, err := store.ProviderObservation(job.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("observation before lookup=(%+v,%v), want absent", ProviderObservation{}, err)
	}
	if err := store.ObserveProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("record provider observation: %v", err)
	}
	if err := store.ObserveProviderOutcome(job.ID, outcome); err != nil {
		t.Fatalf("repeat identical observation: %v", err)
	}
	observed, err := store.ProviderObservation(job.ID)
	if err != nil || observed.Outcome != outcome || observed.ObservedAt.IsZero() {
		t.Fatalf("persisted provider observation=(%+v,%v)", observed, err)
	}
	got, err := store.Get(job.ID)
	if err != nil || got.Status != StatusFailed || got.Provider != nil {
		t.Fatalf("provider observation changed job outcome=(%+v,%v)", got, err)
	}
	history, err := store.History(job.ID)
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

	conflict := outcome
	conflict.Number++
	conflict.URL = "https://github.com/acme/widget/pull/72"
	if err := store.ObserveProviderOutcome(job.ID, conflict); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("conflicting observation error=%v, want ErrInvalidTransition", err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER reject_provider_observation BEFORE INSERT ON factory_job_events WHEN NEW.type='provider_observed' BEGIN SELECT RAISE(ABORT,'injected audit failure'); END`); err != nil {
		t.Fatal(err)
	}
	other, _, err := store.Admit("observation-atomicity", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	otherAttempt := ProviderAttempt{Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + other.ID, Commit: "abc1234"}
	if err := store.RecordProviderAttempt(other.ID, otherAttempt); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(other.ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	otherOutcome := outcome
	otherOutcome.Branch = otherAttempt.Branch
	if err := store.ObserveProviderOutcome(other.ID, otherOutcome); err == nil {
		t.Fatal("observation committed when audit insertion failed")
	}
	if _, err := store.ProviderObservation(other.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("observation survived audit failure=(%+v,%v)", ProviderObservation{}, err)
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
