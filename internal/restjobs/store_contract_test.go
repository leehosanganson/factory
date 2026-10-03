package restjobs

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestLocalManagerSatisfiesStoreContract(t *testing.T) {
	runStoreContract(t, func(t *testing.T) Store {
		t.Helper()
		store, err := NewManager(testConfig())
		if err != nil {
			t.Fatal(err)
		}
		return store
	})
}

func runStoreContract(t *testing.T, newStore func(*testing.T) Store) {
	t.Helper()
	t.Run("admission replay and conflict", func(t *testing.T) {
		store := newStore(t)
		first, replay, err := store.Admit("contract-key", Request{Repository: "widget", Task: "task"})
		if err != nil || replay {
			t.Fatalf("initial admission = (%+v, %v, %v), want a new job", first, replay, err)
		}
		again, replay, err := store.Admit("contract-key", Request{Repository: "widget", Task: "task"})
		if err != nil || !replay || again.ID != first.ID {
			t.Fatalf("identical admission = (%+v, %v, %v), want same job replay", again, replay, err)
		}
		if _, _, err := store.Admit("contract-key", Request{Repository: "widget", Task: "different"}); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("changed payload error = %v, want ErrIdempotencyConflict", err)
		}
	})

	t.Run("concurrent identical admissions share one job", func(t *testing.T) {
		store := newStore(t)
		const callers = 16
		type result struct {
			job    Snapshot
			replay bool
			err    error
		}
		start := make(chan struct{})
		results := make(chan result, callers)
		var wg sync.WaitGroup
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				job, replay, err := store.Admit("shared-key", Request{Repository: "widget", Task: "same task"})
				results <- result{job: job, replay: replay, err: err}
			}()
		}
		close(start)
		wg.Wait()
		close(results)

		var jobID string
		newAdmissions := 0
		for result := range results {
			if result.err != nil {
				t.Errorf("concurrent Admit() error: %v", result.err)
				continue
			}
			if jobID == "" {
				jobID = result.job.ID
			}
			if result.job.ID != jobID {
				t.Errorf("concurrent admissions returned IDs %q and %q", jobID, result.job.ID)
			}
			if !result.replay {
				newAdmissions++
			}
		}
		if jobID == "" {
			t.Fatal("no concurrent admission returned a job")
		}
		if newAdmissions != 1 {
			t.Errorf("new admissions = %d, want exactly one", newAdmissions)
		}
		if _, _, err := store.Admit("shared-key", Request{Repository: "widget", Task: "different"}); !errors.Is(err, ErrIdempotencyConflict) {
			t.Errorf("changed-payload replay error = %v, want conflict", err)
		}
	})

	t.Run("lifecycle and history", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("lifecycle-key", Request{Repository: "widget", Task: "task"})
		if err != nil {
			t.Fatal(err)
		}
		claimed, err := store.ClaimNext()
		if err != nil || claimed.ID != job.ID || claimed.Status != StatusRunning {
			t.Fatalf("claim = (%+v, %v), want running job %q", claimed, err, job.ID)
		}
		if err := store.AddEvent(job.ID, "verification", "tests passed"); err != nil {
			t.Fatal(err)
		}
		verification := VerificationEvidence{
			Checks:      []VerificationCheck{{Name: "check-01", Outcome: "passed"}},
			Limitations: []string{LimitationAgentNotVerdict},
		}
		if err := store.RecordVerificationEvidence(job.ID, verification); err != nil {
			t.Fatal(err)
		}
		if err := store.Finish(job.ID, StatusSucceeded); err != nil {
			t.Fatal(err)
		}
		got, err := store.Get(job.ID)
		if err != nil || got.Status != StatusSucceeded || got.Verification == nil || len(got.Verification.Checks) != 1 || got.Verification.Checks[0] != verification.Checks[0] || len(got.Verification.Limitations) != 1 || got.Verification.Limitations[0] != LimitationAgentNotVerdict {
			t.Fatalf("Get() = (%+v, %v), want succeeded job with durable verification evidence", got, err)
		}
		history, err := store.History(job.ID)
		if err != nil || len(history.Events) != 4 || history.Events[2].Type != "verification" || history.Events[3].Type != string(StatusSucceeded) {
			t.Fatalf("History() = (%+v, %v), want queued/running/verification/succeeded events", history, err)
		}
		report, err := store.Recover(context.Background())
		if err != nil || len(report.Terminal) != 1 || report.Terminal[0].ID != job.ID || len(report.Resume) != 0 || len(report.NeedsOperator) != 0 {
			t.Fatalf("Recover() = (%+v, %v), want terminal job retained without replay", report, err)
		}
	})

	t.Run("close cancels queued work", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("queued-key", Request{Repository: "widget", Task: "task"})
		if err != nil {
			t.Fatal(err)
		}
		store.Close()
		got, err := store.Get(job.ID)
		if err != nil || got.Status != StatusCanceled {
			t.Fatalf("Get() after Close() = (%+v, %v), want canceled", got, err)
		}
		if _, err := store.WaitClaim(context.Background()); !errors.Is(err, ErrManagerClosed) {
			t.Fatalf("WaitClaim() after Close() = %v, want ErrManagerClosed", err)
		}
	})
}

func TestRecoveryDispositionRequiresOperatorForRunningJobs(t *testing.T) {
	tests := []struct {
		status Status
		want   RecoveryDisposition
	}{
		{StatusQueued, RecoveryResume},
		{StatusRunning, RecoveryNeedsOperator},
		{StatusSucceeded, RecoveryRetainTerminal},
		{StatusFailed, RecoveryRetainTerminal},
		{StatusCanceled, RecoveryRetainTerminal},
		{Status("unknown"), RecoveryNeedsOperator},
	}
	for _, test := range tests {
		t.Run(string(test.status), func(t *testing.T) {
			if got := RecoveryDispositionFor(test.status); got != test.want {
				t.Fatalf("RecoveryDispositionFor(%q) = %q, want %q", test.status, got, test.want)
			}
		})
	}
}

func TestLocalStoreRecoveryReportsQueuedAsResumableAndRunningAsUncertain(t *testing.T) {
	store, err := NewManager(Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 4})
	if err != nil {
		t.Fatal(err)
	}
	queued, _, err := store.Admit("queued", Request{Repository: "widget", Task: "queued"})
	if err != nil {
		t.Fatal(err)
	}
	running, _, err := store.Admit("running", Request{Repository: "widget", Task: "running"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimNext()
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != queued.ID {
		t.Fatalf("first claimed job = %q, want FIFO queued job %q", claimed.ID, queued.ID)
	}
	report, err := store.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Resume) != 1 || report.Resume[0].ID != running.ID {
		t.Fatalf("resumable jobs = %+v, want queued job %q", report.Resume, running.ID)
	}
	if len(report.NeedsOperator) != 1 || report.NeedsOperator[0].ID != claimed.ID {
		t.Fatalf("operator-recovery jobs = %+v, want claimed job %q", report.NeedsOperator, claimed.ID)
	}
}

func TestLocalManagerOperationalSummaryIsBoundedAndAggregated(t *testing.T) {
	store, err := NewManager(Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 3, MaxEventsPerJob: 4})
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := store.Admit("summary-1", Request{Repository: "widget", Task: "secret task one"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	second, _, err := store.Admit("summary-2", Request{Repository: "widget", Task: "secret task two"})
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.OperationalSummary(context.Background())
	if err != nil || before.RetainedRecords != 2 || before.RecordLimit != 3 || before.QueueCapacity != 1 || before.Queued != 1 || before.Running != 1 || !before.QueueSaturated {
		t.Fatalf("summary with active and queued jobs = (%+v, %v)", before, err)
	}
	if _, err := store.OperationalSummary(nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("OperationalSummary(nil) error = %v, want ErrInvalidInput", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.OperationalSummary(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("OperationalSummary(canceled context) error = %v, want context.Canceled", err)
	}
	if err := store.Finish(first.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(second.ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	third, _, err := store.Admit("summary-3", Request{Repository: "widget", Task: "secret task three"})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := store.OperationalSummary(context.Background())
	if err != nil || summary.RetainedRecords != 3 || summary.Queued != 1 || summary.Running != 0 || summary.Succeeded != 1 || summary.Failed != 1 || summary.Canceled != 0 || !summary.QueueSaturated {
		t.Fatalf("terminal/queued summary = (%+v, %v)", summary, err)
	}
	store.Close()
	afterClose, err := store.OperationalSummary(context.Background())
	if err != nil || afterClose.Canceled != 1 || afterClose.QueueSaturated {
		t.Fatalf("summary after queued cancellation = (%+v, %v), third=%s", afterClose, err, third.ID)
	}
}

func TestStoreRecoveryHonorsCanceledContext(t *testing.T) {
	store, err := NewManager(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Admit("cancel-recovery", Request{Repository: "widget", Task: "task"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Recover(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Recover() error = %v, want context.Canceled", err)
	}
}

func TestWaitClaimUsesWorkerOwnershipLimit(t *testing.T) {
	store, err := NewManager(Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 4})
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := store.Admit("first", Request{Repository: "widget", Task: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := store.Admit("second", Request{Repository: "widget", Task: "second"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.WaitClaim(context.Background())
	if err != nil || claimed.ID != first.ID {
		t.Fatalf("first claim = (%+v, %v), want %q", claimed, err, first.ID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := store.WaitClaim(ctx)
		result <- err
	}()
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("claim while worker is owned = %v, want context cancellation", err)
	}
	if err := store.Finish(first.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.WaitClaim(context.Background())
	if err != nil || claimed.ID != second.ID {
		t.Fatalf("claim after ownership released = (%+v, %v), want %q", claimed, err, second.ID)
	}
}
