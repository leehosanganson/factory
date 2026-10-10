package restjobs

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
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

func setListTestCreatedAt(t *testing.T, store Store, ids ...string) {
	t.Helper()
	const tied = "2026-10-10T12:00:00Z"
	switch typed := store.(type) {
	case *LocalManager:
		typed.mu.Lock()
		for _, id := range ids {
			entry := typed.jobs[id]
			entry.snapshot.CreatedAt = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
		}
		typed.mu.Unlock()
	case *SQLiteStore:
		for _, id := range ids {
			if _, err := typed.db.Exec(`UPDATE factory_jobs SET created_at=? WHERE id=?`, tied, id); err != nil {
				t.Fatal(err)
			}
		}
	default:
		t.Fatalf("unsupported test store %T", store)
	}
}

func runStoreContract(t *testing.T, newStore func(*testing.T) Store) {
	t.Helper()
	t.Run("bounded listing has stable admission order and snapshot boundary", func(t *testing.T) {
		store := newStore(t)
		first, _, err := store.Admit("list-first", Request{Repository: "widget", Task: "private first task"})
		if err != nil {
			t.Fatal(err)
		}
		second, _, err := store.Admit("list-second", Request{Repository: "widget", Task: "private second task"})
		if err != nil {
			t.Fatal(err)
		}
		setListTestCreatedAt(t, store, first.ID, second.ID)
		if _, err := store.ClaimNext(); err != nil {
			t.Fatal(err)
		}
		if err := store.Finish(first.ID, StatusSucceeded); err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimNext(); err != nil {
			t.Fatal(err)
		}
		page, err := store.ListJobs(context.Background(), 0, 0, 1)
		if err != nil || len(page.Jobs) != 1 || page.Jobs[0].ID != first.ID || !page.HasMore || page.SnapshotSequence == 0 || page.NextSequence == 0 {
			t.Fatalf("first list page = (%+v, %v), want first admitted job and continuation", page, err)
		}
		startConcurrent := make(chan struct{})
		listed := make(chan struct {
			page JobPage
			err  error
		}, 1)
		concurrentAdmission := make(chan error, 1)
		go func() {
			<-startConcurrent
			result, err := store.ListJobs(context.Background(), page.NextSequence, page.SnapshotSequence, 1)
			listed <- struct {
				page JobPage
				err  error
			}{page: result, err: err}
		}()
		go func() {
			<-startConcurrent
			_, _, err := store.Admit("list-fourth", Request{Repository: "widget", Task: "concurrent admission"})
			concurrentAdmission <- err
		}()
		close(startConcurrent)
		nextResult := <-listed
		if err := <-concurrentAdmission; err != nil {
			t.Fatal(err)
		}
		next := nextResult.page
		third, _, err := store.Admit("list-fresh", Request{Repository: "widget", Task: "fresh snapshot"})
		if err != nil {
			t.Fatal(err)
		}
		if nextResult.err != nil || len(next.Jobs) != 1 || next.Jobs[0].ID != second.ID || next.Jobs[0].Status != StatusRunning || next.HasMore {
			t.Fatalf("continuation page = (%+v, %v), want second admitted job only", next, nextResult.err)
		}
		encoded, err := json.Marshal(next.Jobs[0])
		if err != nil || strings.Contains(string(encoded), "private second task") || strings.Contains(string(encoded), "request") {
			t.Fatalf("listed summary exposed request payload: %s err=%v", encoded, err)
		}
		all, err := store.ListJobs(context.Background(), 0, 0, 10)
		if err != nil || len(all.Jobs) != 4 || all.Jobs[3].ID != third.ID {
			t.Fatalf("fresh list = (%+v, %v), want concurrent admission on fresh snapshot", all, err)
		}
	})

	t.Run("stream state is atomic, context-aware, and preserves bounded history", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("stream-state", Request{Repository: "widget", Task: "private"})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, history, err := store.EventStreamState(context.Background(), job.ID)
		if err != nil || snapshot.Status != StatusQueued || history.JobID != job.ID || len(history.Events) != 1 || history.LatestSequence != 1 {
			t.Fatalf("initial stream state=(%+v,%+v,%v)", snapshot, history, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, err := store.EventStreamState(ctx, job.ID); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled stream state error=%v, want context.Canceled", err)
		}
	})

	t.Run("history cursor is monotonic across bounded retention", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("event-cursor", Request{Repository: "widget", Task: "private task"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimNext(); err != nil {
			t.Fatal(err)
		}
		if err := store.Finish(job.ID, StatusSucceeded); err != nil {
			t.Fatal(err)
		}
		history, err := store.History(job.ID)
		if err != nil || len(history.Events) == 0 {
			t.Fatalf("retained history = (%+v, %v)", history, err)
		}
		encoded, err := json.Marshal(history)
		if err != nil || strings.Contains(string(encoded), "latest_sequence") || strings.Contains(string(encoded), `"sequence"`) {
			t.Fatalf("internal cursors changed the public history JSON: %s err=%v", encoded, err)
		}
		for index, event := range history.Events {
			if event.Sequence == 0 || (index > 0 && event.Sequence <= history.Events[index-1].Sequence) {
				t.Fatalf("event sequence at index %d = %d, history=%+v", index, event.Sequence, history.Events)
			}
		}
		if history.LatestSequence < history.Events[len(history.Events)-1].Sequence {
			t.Fatalf("latest event cursor %d precedes retained events %+v", history.LatestSequence, history.Events)
		}
	})

	t.Run("empty listing and terminal retention", func(t *testing.T) {
		store := newStore(t)
		empty, err := store.ListJobs(context.Background(), 0, 0, 2)
		if err != nil || len(empty.Jobs) != 0 || empty.HasMore || empty.SnapshotSequence != 0 {
			t.Fatalf("empty list = (%+v, %v), want an empty page", empty, err)
		}
		job, _, err := store.Admit("list-terminal", Request{Repository: "widget", Task: "terminal"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimNext(); err != nil {
			t.Fatal(err)
		}
		if err := store.Finish(job.ID, StatusFailed); err != nil {
			t.Fatal(err)
		}
		listed, err := store.ListJobs(context.Background(), 0, 0, 2)
		if err != nil || len(listed.Jobs) != 1 || listed.Jobs[0].ID != job.ID || listed.Jobs[0].Status != StatusFailed {
			t.Fatalf("terminal list = (%+v, %v), want retained failed job", listed, err)
		}
	})

	t.Run("invalid pagination and canceled context are rejected", func(t *testing.T) {
		store := newStore(t)
		for _, tc := range []struct {
			after, snapshot uint64
			limit           int
		}{
			{limit: 0}, {limit: -1}, {after: 2, snapshot: 1, limit: 1}, {snapshot: ^uint64(0), limit: 1},
		} {
			if _, err := store.ListJobs(context.Background(), tc.after, tc.snapshot, tc.limit); !errors.Is(err, ErrInvalidInput) {
				t.Errorf("ListJobs(%d,%d,%d) error = %v, want ErrInvalidInput", tc.after, tc.snapshot, tc.limit, err)
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := store.ListJobs(ctx, 0, 0, 1); !errors.Is(err, context.Canceled) {
			t.Errorf("ListJobs(canceled) error = %v, want context.Canceled", err)
		}
	})

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

	t.Run("cancel queued work without claiming it", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("cancel-queued", Request{Repository: "widget", Task: "task"})
		if err != nil {
			t.Fatal(err)
		}
		canceled, err := store.Cancel(job.ID)
		if err != nil || canceled.Status != StatusCanceled {
			t.Fatalf("Cancel() = (%+v, %v), want canceled queued job", canceled, err)
		}
		if _, err := store.ClaimNext(); !errors.Is(err, ErrNoQueuedJobs) {
			t.Fatalf("ClaimNext() after queued cancellation = %v, want ErrNoQueuedJobs", err)
		}
		history, err := store.History(job.ID)
		if err != nil || len(history.Events) != 2 || history.Events[1].Type != string(StatusCanceled) {
			t.Fatalf("canceled queued history = (%+v, %v), want queued/canceled", history, err)
		}
		if _, err := store.Cancel(job.ID); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("repeated terminal Cancel() = %v, want ErrInvalidTransition", err)
		}
	})

	t.Run("request running cancellation without terminalizing early", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("cancel-running", Request{Repository: "widget", Task: "task"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimNext(); err != nil {
			t.Fatal(err)
		}
		requested, err := store.Cancel(job.ID)
		if err != nil || requested.Status != StatusRunning || !requested.CancellationRequested {
			t.Fatalf("Cancel() = (%+v, %v), want running with cancellation requested", requested, err)
		}
		repeated, err := store.Cancel(job.ID)
		if err != nil || repeated.Status != StatusRunning || !repeated.CancellationRequested {
			t.Fatalf("repeated Cancel() = (%+v, %v), want same pending request", repeated, err)
		}
		history, err := store.History(job.ID)
		if err != nil || len(history.Events) != 3 || history.Events[2].Type != "cancel_requested" {
			t.Fatalf("pending cancellation history = (%+v, %v), want one cancel_requested event", history, err)
		}
		if err := store.Finish(job.ID, StatusSucceeded); err != nil {
			t.Fatal(err)
		}
		finished, err := store.Get(job.ID)
		if err != nil || finished.Status != StatusSucceeded || finished.CancellationRequested {
			t.Fatalf("finished cancellation = (%+v, %v), want executor-selected terminal state", finished, err)
		}
		history, err = store.History(job.ID)
		if err != nil || len(history.Events) != 4 || history.Events[3].Type != string(StatusSucceeded) {
			t.Fatalf("finished cancellation history = (%+v, %v), want terminal event after request", history, err)
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
