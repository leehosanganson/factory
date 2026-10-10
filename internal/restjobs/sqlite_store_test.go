package restjobs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func privateSQLiteDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestSQLiteStoreSatisfiesSharedContract(t *testing.T) {
	runStoreContract(t, func(t *testing.T) Store {
		t.Helper()
		store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "jobs.db"), testConfig())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.CloseStore() })
		return store
	})
}

func TestSQLiteListingIncludesInterruptedAndTerminalJobsWithoutReplayingThem(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	config := testConfig()
	store, err := OpenSQLiteStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	interrupted, _, err := store.Admit("list-interrupted", Request{Repository: "widget", Task: "retained interrupted task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	terminal, _, err := store.Admit("list-terminal", Request{Repository: "widget", Task: "retained terminal task"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(interrupted.ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(terminal.ID, StatusCanceled); err != nil {
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenSQLiteStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	// A separate running record simulates an interrupted process after its
	// database connection is closed without orderly lifecycle transitions.
	queued, _, err := store.Admit("list-running", Request{Repository: "widget", Task: "interrupted task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := store.db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenSQLiteStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	page, err := store.ListJobs(context.Background(), 0, 0, 10)
	if err != nil || len(page.Jobs) != 3 {
		t.Fatalf("retained SQLite jobs=(%+v,%v), want three", page, err)
	}
	foundRunning := false
	foundTerminal := false
	for _, summary := range page.Jobs {
		foundRunning = foundRunning || (summary.ID == queued.ID && summary.Status == StatusRunning)
		foundTerminal = foundTerminal || (summary.ID == terminal.ID && summary.Status == StatusCanceled)
	}
	if !foundRunning || !foundTerminal {
		t.Fatalf("listing omitted interrupted or terminal records: %+v", page.Jobs)
	}
}

func TestSQLiteCancellationRequestInterruptedByRestartIsNotAcceptedAsLive(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	store, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Admit("cancel-before-restart", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if snapshot, err := store.Cancel(job.ID); err != nil || !snapshot.CancellationRequested {
		t.Fatalf("pre-restart cancellation=(%+v,%v)", snapshot, err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.CloseStore()
	if _, err := reopened.Cancel(job.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("cancellation of interrupted job=%v, want ErrInvalidTransition", err)
	}
	retained, err := reopened.Get(job.ID)
	if err != nil || retained.Status != StatusRunning || !retained.CancellationRequested {
		t.Fatalf("restart changed interrupted cancellation=%+v err=%v", retained, err)
	}
}

func TestSQLiteStoreCapacityRejectedKeyCanBeReused(t *testing.T) {
	config := Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 4, MaxEventsPerJob: 4}
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "jobs.db"), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()

	first, _, err := store.Admit("first", Request{Repository: "widget", Task: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	queued, _, err := store.Admit("queued", Request{Repository: "widget", Task: "queued"})
	if err != nil {
		t.Fatal(err)
	}
	request := Request{Repository: "widget", Task: "retry after capacity frees"}
	if _, _, err := store.Admit("retry-key", request); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full capacity error = %v, want ErrQueueFull", err)
	}
	if _, err := store.Get("retry-key"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("capacity-rejected request was recorded: %v", err)
	}
	if err := store.Finish(first.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimNext()
	if err != nil || claimed.ID != queued.ID {
		t.Fatalf("claim after capacity frees = (%+v, %v), want %q", claimed, err, queued.ID)
	}
	if err := store.Finish(queued.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	retryRequest := Request{Repository: "widget", Task: "updated retry after capacity frees"}
	retried, replay, err := store.Admit("retry-key", retryRequest)
	if err != nil || replay {
		t.Fatalf("retry after capacity frees = (%+v, %v, %v), want new admission", retried, replay, err)
	}
	again, replay, err := store.Admit("retry-key", retryRequest)
	if err != nil || !replay || again.ID != retried.ID {
		t.Fatalf("accepted retry replay = (%+v, %v, %v), want same job replay", again, replay, err)
	}
}

func TestSQLiteStoreEnforcesRetainedRecordLimitWithoutEvictingTerminalHistory(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "jobs.db"), Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	for _, key := range []string{"one", "two"} {
		job, _, err := store.Admit(key, Request{Repository: "widget", Task: key})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimNext(); err != nil {
			t.Fatal(err)
		}
		if err := store.Finish(job.ID, StatusSucceeded); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := store.Admit("three", Request{Repository: "widget", Task: "three"}); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("third admission error=%v, want ErrRegistryFull", err)
	}
}

func TestSQLiteOperationalSummaryCountsStatusesAndRecoveryNeeded(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	config := Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 3, MaxEventsPerJob: 4}
	seed, err := OpenSQLiteStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	running, _, err := seed.Admit("summary-running", Request{Repository: "widget", Task: "private task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := seed.db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenSQLiteStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	queued, _, err := store.Admit("summary-queued", Request{Repository: "widget", Task: "private queue item"})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := store.OperationalSummary(context.Background())
	if err != nil || summary.RetainedRecords != 2 || summary.Queued != 1 || summary.Running != 1 || summary.RecoveryNeeded != 1 || !summary.QueueSaturated {
		t.Fatalf("summary = (%+v, %v)", summary, err)
	}
	if _, err := store.OperationalSummary(nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("OperationalSummary(nil) error = %v, want ErrInvalidInput", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.OperationalSummary(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("OperationalSummary(canceled context) error = %v, want context.Canceled", err)
	}
	if err := store.ResolveInterrupted(running.ID, InterruptedDispositionCanceled); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(queued.ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	summary, err = store.OperationalSummary(context.Background())
	if err != nil || summary.RetainedRecords != 2 || summary.Queued != 0 || summary.Running != 0 || summary.Succeeded != 0 || summary.Failed != 1 || summary.Canceled != 1 || summary.RecoveryNeeded != 0 || summary.QueueSaturated {
		t.Fatalf("terminal summary = (%+v, %v)", summary, err)
	}
}

func TestSQLiteStoreBackupRejectsReplacingItsSource(t *testing.T) {
	dir := privateSQLiteDir(t)
	path := filepath.Join(dir, "jobs.db")
	store, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	if err := store.Backup(context.Background(), path); err == nil {
		t.Fatal("Backup() replacing its open source database succeeded")
	}
	if err := store.Ping(context.Background()); err != nil {
		t.Fatalf("source store unavailable after rejected backup: %v", err)
	}
}

func TestBackupSQLiteDatabaseDoesNotChangeQueuedJobs(t *testing.T) {
	dir := privateSQLiteDir(t)
	sourcePath := filepath.Join(dir, "jobs.db")
	store, err := OpenSQLiteStore(sourcePath, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Admit("backup-queued", Request{Repository: "widget", Task: "still queued"})
	if err != nil {
		_ = store.CloseStore()
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		_ = store.CloseStore()
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	if err := BackupSQLiteDatabase(context.Background(), sourcePath, filepath.Join(dir, "copy.db")); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLiteStore(sourcePath, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.CloseStore()
	got, err := reopened.Get(job.ID)
	if err != nil || got.Status != StatusRunning {
		t.Fatalf("source job after backup = (%+v, %v), want running", got, err)
	}
}

func TestSQLiteStoreBackupRestoresProviderOutcomeHistoryAndIdempotency(t *testing.T) {
	dir := privateSQLiteDir(t)
	databasePath := filepath.Join(dir, "jobs.db")
	store, err := OpenSQLiteStore(databasePath, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Admit("backup-key", Request{Repository: "widget", Task: "preserve this"})
	if err != nil {
		_ = store.CloseStore()
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		_ = store.CloseStore()
		t.Fatal(err)
	}
	if err := store.AddEvent(job.ID, "verification", "checks recorded"); err != nil {
		_ = store.CloseStore()
		t.Fatal(err)
	}
	outcome := ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 7, URL: "https://github.com/acme/widget/pull/7", Branch: "factory/job/" + job.ID, Commit: "abc123", State: "open"}
	if err := store.RecordProviderOutcome(job.ID, outcome); err != nil {
		_ = store.CloseStore()
		t.Fatal(err)
	}
	if err := store.Finish(job.ID, StatusSucceeded); err != nil {
		_ = store.CloseStore()
		t.Fatal(err)
	}

	backupPath := filepath.Join(dir, "backup.db")
	if err := store.Backup(context.Background(), backupPath); err != nil {
		_ = store.CloseStore()
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}

	restored, err := OpenSQLiteStore(backupPath, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.CloseStore()
	got, err := restored.Get(job.ID)
	if err != nil || got.Status != StatusSucceeded || got.Provider == nil || *got.Provider != outcome {
		t.Fatalf("restored job = (%+v, %v)", got, err)
	}
	history, err := restored.History(job.ID)
	if err != nil || len(history.Events) != 4 || history.Events[2].Type != "verification" {
		t.Fatalf("restored history = (%+v, %v)", history, err)
	}
	replay, repeated, err := restored.Admit("backup-key", Request{Repository: "widget", Task: "preserve this"})
	if err != nil || !repeated || replay.ID != job.ID {
		t.Fatalf("restored idempotent replay = (%+v, %v, %v)", replay, repeated, err)
	}
	report, err := restored.Recover(context.Background())
	if err != nil || len(report.Terminal) != 1 || report.Terminal[0].ID != job.ID {
		t.Fatalf("restored recovery report = (%+v, %v)", report, err)
	}
}

func TestSQLiteStorePersistsJobHistoryAndIdempotencyAcrossRestart(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	store, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Admit("restart-key", Request{Repository: "widget", Task: "persist this"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := store.AddEvent(job.ID, "verification", "recorded"); err != nil {
		t.Fatal(err)
	}
	// Simulate process loss rather than orderly shutdown.
	if err := store.db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.CloseStore()
	got, err := reopened.Get(job.ID)
	if err != nil || got.Status != StatusRunning || got.Request.Task != "persist this" {
		t.Fatalf("persisted job = (%+v, %v)", got, err)
	}
	history, err := reopened.History(job.ID)
	if err != nil || len(history.Events) != 3 || history.Events[2].Type != "verification" {
		t.Fatalf("persisted history = (%+v, %v)", history, err)
	}
	replay, repeated, err := reopened.Admit("restart-key", Request{Repository: "widget", Task: "persist this"})
	if err != nil || !repeated || replay.ID != job.ID {
		t.Fatalf("replayed admission = (%+v, %v, %v)", replay, repeated, err)
	}
	if _, _, err := reopened.Admit("restart-key", Request{Repository: "widget", Task: "different"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed-payload replay error = %v, want conflict", err)
	}
}

func TestSQLiteStoreRecoveryResumesQueuedButHoldsRunningForOperator(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	store, err := OpenSQLiteStore(path, Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 4, MaxEventsPerJob: 8})
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
	claimed, err := store.ClaimNext()
	if err != nil || claimed.ID != first.ID {
		t.Fatalf("claim = (%+v, %v), want %q", claimed, err, first.ID)
	}
	// Simulate an unclean process stop by closing the pool without applying the
	// clean-shutdown queued cancellation policy.
	if err := store.db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLiteStore(path, Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 4, MaxEventsPerJob: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.CloseStore()
	report, err := reopened.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Resume) != 1 || report.Resume[0].ID != second.ID {
		t.Fatalf("resume = %+v, want queued job %q", report.Resume, second.ID)
	}
	if len(report.NeedsOperator) != 1 || report.NeedsOperator[0].ID != first.ID {
		t.Fatalf("needs operator = %+v, want interrupted job %q", report.NeedsOperator, first.ID)
	}
}

func TestSQLiteStoreCloseCancelsQueuedButPreservesRunningForRecovery(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	store, err := OpenSQLiteStore(path, Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 4, MaxEventsPerJob: 8})
	if err != nil {
		t.Fatal(err)
	}
	running, _, err := store.Admit("running", Request{Repository: "widget", Task: "running"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	queued, _, err := store.Admit("queued", Request{Repository: "widget", Task: "queued"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(queued.ID); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("Get after close = %v", err)
	}
	reopened, err := OpenSQLiteStore(path, Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 4, MaxEventsPerJob: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.CloseStore()
	job, err := reopened.Get(running.ID)
	if err != nil || job.Status != StatusRunning {
		t.Fatalf("running job after shutdown = (%+v,%v)", job, err)
	}
	job, err = reopened.Get(queued.ID)
	if err != nil || job.Status != StatusCanceled {
		t.Fatalf("queued job after shutdown = (%+v,%v)", job, err)
	}
}

func TestSQLiteStoreConcurrentConnectionsShareIdempotency(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	stores := make([]*SQLiteStore, 2)
	for i := range stores {
		store, err := OpenSQLiteStore(path, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		stores[i] = store
		defer store.CloseStore()
	}

	type admission struct {
		job    Snapshot
		replay bool
		err    error
	}
	const callers = 16
	start := make(chan struct{})
	results := make(chan admission, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(store *SQLiteStore) {
			defer wg.Done()
			<-start
			job, replay, err := store.Admit("shared-key", Request{Repository: "widget", Task: "same"})
			results <- admission{job: job, replay: replay, err: err}
		}(stores[i%len(stores)])
	}
	close(start)
	wg.Wait()
	close(results)

	var firstID string
	newAdmissions := 0
	for result := range results {
		if result.err != nil {
			t.Errorf("concurrent cross-connection admission: %v", result.err)
			continue
		}
		if firstID == "" {
			firstID = result.job.ID
		}
		if result.job.ID != firstID {
			t.Errorf("concurrent admissions returned IDs %q and %q", firstID, result.job.ID)
		}
		if !result.replay {
			newAdmissions++
		}
	}
	if firstID == "" {
		t.Fatal("no concurrent admission returned a job")
	}
	if newAdmissions != 1 {
		t.Errorf("new admissions = %d, want exactly one", newAdmissions)
	}
	if _, _, err := stores[0].Admit("shared-key", Request{Repository: "widget", Task: "different"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Errorf("changed-payload replay error = %v, want conflict", err)
	}
}

func TestSQLiteStoreRejectsInvalidPathAndUnavailableDatabase(t *testing.T) {
	if _, err := OpenSQLiteStore("", testConfig()); err == nil {
		t.Fatal("empty path accepted")
	}
	if _, err := OpenSQLiteStore(filepath.Join(t.TempDir(), "missing", "jobs.db"), testConfig()); err == nil {
		t.Fatal("missing database parent silently accepted")
	}
}

func TestSQLiteStorePersistsVerificationEvidenceAcrossRestart(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	store, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Admit("verification-key", Request{Repository: "widget", Task: "verify"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	want := VerificationEvidence{
		Checks:      []VerificationCheck{{Name: "check-01", Outcome: VerificationPassed}},
		Limitations: []string{LimitationAgentNotVerdict},
	}
	if err := store.RecordVerificationEvidence(job.ID, want); err != nil {
		t.Fatal(err)
	}
	if err := store.db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.CloseStore()
	got, err := reopened.Get(job.ID)
	if err != nil || got.Verification == nil || len(got.Verification.Checks) != 1 || got.Verification.Checks[0] != want.Checks[0] || len(got.Verification.Limitations) != 1 || got.Verification.Limitations[0] != want.Limitations[0] {
		t.Fatalf("verification evidence = (%+v,%v)", got.Verification, err)
	}
}

func TestSQLiteStorePersistsProviderOutcomeAcrossRestart(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	store, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Admit("provider-key", Request{Repository: "widget", Task: "publish"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	want := ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 5, URL: "https://github.com/acme/widget/pull/5", Branch: "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb", Commit: "abc123", State: "open"}
	if err := store.RecordProviderOutcome(job.ID, want); err != nil {
		t.Fatal(err)
	}
	if err := store.db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.CloseStore()
	got, err := reopened.Get(job.ID)
	if err != nil || got.Provider == nil || *got.Provider != want {
		t.Fatalf("provider outcome = (%+v,%v)", got.Provider, err)
	}
}

func TestSQLiteStoreRejectsDatabaseSymlink(t *testing.T) {
	dir := privateSQLiteDir(t)
	target := filepath.Join(dir, "target.db")
	if err := os.WriteFile(target, []byte("not a database"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.db")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLiteStore(link, testConfig()); err == nil {
		t.Fatal("symlink database path accepted")
	}
}

func TestSQLiteStoreCloseCancelsQueuedButPreservesRunning(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	store, err := OpenSQLiteStore(path, Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 4, MaxEventsPerJob: 8})
	if err != nil {
		t.Fatal(err)
	}
	running, _, err := store.Admit("running", Request{Repository: "widget", Task: "running"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	queued, _, err := store.Admit("queued", Request{Repository: "widget", Task: "queued"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLiteStore(path, Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 4, MaxEventsPerJob: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.CloseStore()
	got, err := reopened.Get(running.ID)
	if err != nil || got.Status != StatusRunning {
		t.Fatalf("running job = (%+v, %v)", got, err)
	}
	got, err = reopened.Get(queued.ID)
	if err != nil || got.Status != StatusCanceled {
		t.Fatalf("queued job = (%+v, %v)", got, err)
	}
}

func TestSQLiteServerStoreReleasesOwnershipOnClose(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	first, err := OpenSQLiteServerStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLiteServerStore(path, testConfig()); err == nil {
		t.Fatal("second server store opened while ownership was held")
	}
	if err := first.CloseStore(); err != nil {
		t.Fatalf("close first server store: %v", err)
	}
	second, err := OpenSQLiteServerStore(path, testConfig())
	if err != nil {
		t.Fatalf("open server store after orderly close: %v", err)
	}
	if err := second.CloseStore(); err != nil {
		t.Fatalf("close second server store: %v", err)
	}
}

func TestSQLiteServerRejectsPathReplacementWhileOwnerLive(t *testing.T) {
	const (
		helperResultStart = "FACTORY_SQLITE_PATH_REPLACEMENT_RESULT_BEGIN"
		helperResultEnd   = "FACTORY_SQLITE_PATH_REPLACEMENT_RESULT_END"
	)
	if role := os.Getenv("FACTORY_SQLITE_PATH_REPLACEMENT_HELPER"); role != "" {
		path := os.Getenv("FACTORY_SQLITE_PATH_REPLACEMENT_PATH")
		store, err := OpenSQLiteServerStore(path, testConfig())
		if role == "competitor" {
			if err != nil {
				fmt.Printf("\n%s\nB_OPEN_ERROR: %v\n%s\n", helperResultStart, err, helperResultEnd)
				return
			}
			fmt.Printf("\n%s\nB_OPENED\n%s\n", helperResultStart, helperResultEnd)
			_ = store.CloseStore()
			return
		}
		if err != nil {
			t.Fatalf("open live owner store: %v", err)
		}
		job, _, err := store.Admit("original-live-job", Request{Repository: "widget", Task: "remain running while the configured path is replaced"})
		if err != nil {
			t.Fatalf("admit live owner job: %v", err)
		}
		if _, err := store.ClaimNext(); err != nil {
			t.Fatalf("claim live owner job: %v", err)
		}
		if err := os.WriteFile(os.Getenv("FACTORY_SQLITE_PATH_REPLACEMENT_READY"), []byte(job.ID), 0o600); err != nil {
			t.Fatalf("signal live owner readiness: %v", err)
		}
		select {}
	}

	root := t.TempDir()
	cacheDir := filepath.Join(root, "cache")
	t.Setenv("HOME", root)
	t.Setenv("XDG_CACHE_HOME", cacheDir)
	subprocessEnv := make([]string, 0, len(os.Environ())+2)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key != "HOME" && key != "XDG_CACHE_HOME" {
			subprocessEnv = append(subprocessEnv, entry)
		}
	}
	subprocessEnv = append(subprocessEnv, "HOME="+root, "XDG_CACHE_HOME="+cacheDir)

	configuredDir := filepath.Join(root, "configured")
	if err := os.Mkdir(configuredDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(configuredDir, "jobs.db")
	ready := filepath.Join(root, "owner-ready")
	owner := exec.Command(os.Args[0], "-test.run=^TestSQLiteServerRejectsPathReplacementWhileOwnerLive$")
	owner.Env = append(subprocessEnv, "FACTORY_SQLITE_PATH_REPLACEMENT_HELPER=owner", "FACTORY_SQLITE_PATH_REPLACEMENT_PATH="+path, "FACTORY_SQLITE_PATH_REPLACEMENT_READY="+ready)
	if err := owner.Start(); err != nil {
		t.Fatalf("start owner process: %v", err)
	}
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- owner.Wait() }()
	t.Cleanup(func() {
		if owner.ProcessState == nil {
			_ = owner.Process.Kill()
			<-ownerDone
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if owner.ProcessState != nil {
			t.Fatalf("owner process exited before readiness: %v", <-ownerDone)
		}
		time.Sleep(10 * time.Millisecond)
	}
	jobID, err := os.ReadFile(ready)
	if err != nil {
		t.Fatalf("owner did not become ready: %v", err)
	}

	replacementDir := filepath.Join(root, "replacement")
	if err := os.Mkdir(replacementDir, 0o700); err != nil {
		t.Fatal(err)
	}
	replacementPath := filepath.Join(replacementDir, "jobs.db")
	replacement, err := OpenSQLiteStore(replacementPath, testConfig())
	if err != nil {
		t.Fatalf("initialize replacement database: %v", err)
	}
	if err := replacement.CloseStore(); err != nil {
		t.Fatalf("close replacement database: %v", err)
	}
	parkedDir := filepath.Join(root, "original")
	if err := os.Rename(configuredDir, parkedDir); err != nil {
		t.Fatalf("move live database directory aside: %v", err)
	}
	if err := os.Mkdir(configuredDir, 0o700); err != nil {
		t.Fatalf("recreate configured database directory: %v", err)
	}
	if err := os.Rename(replacementPath, path); err != nil {
		t.Fatalf("bind replacement database at configured path: %v", err)
	}

	competitor := exec.Command(os.Args[0], "-test.run=^TestSQLiteServerRejectsPathReplacementWhileOwnerLive$")
	competitor.Env = append(subprocessEnv, "FACTORY_SQLITE_PATH_REPLACEMENT_HELPER=competitor", "FACTORY_SQLITE_PATH_REPLACEMENT_PATH="+path)
	output, err := competitor.CombinedOutput()
	if err != nil {
		t.Fatalf("replacement-path competitor process: %v: %s", err, output)
	}
	outputText := string(output)
	startMarker := helperResultStart + "\n"
	start := strings.Index(outputText, startMarker)
	if start < 0 {
		t.Fatalf("replacement-path competitor output missing start marker: %q", outputText)
	}
	start += len(startMarker)
	endMarker := "\n" + helperResultEnd
	end := strings.Index(outputText[start:], endMarker)
	if end < 0 {
		t.Fatalf("replacement-path competitor output missing end marker: %q", outputText)
	}
	result := strings.TrimSpace(outputText[start : start+end])
	if !strings.HasPrefix(result, "B_OPEN_ERROR: ") || !strings.Contains(result, "already owned by another server") {
		t.Fatalf("replacement-path open result = %s, want ownership conflict", result)
	}

	original, err := OpenSQLiteStore(filepath.Join(parkedDir, "jobs.db"), testConfig())
	if err != nil {
		t.Fatalf("observe original live database after competitor attempt: %v", err)
	}
	live, err := original.Get(string(jobID))
	_ = original.CloseStore()
	if err != nil || live.Status != StatusRunning {
		t.Fatalf("original live job after competitor attempt = (%+v, %v), want running", live, err)
	}
}

func TestSQLiteStoreOpenRejectsNonPrivateDirectoryAndFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLiteStore(filepath.Join(dir, "jobs.db"), testConfig()); err == nil {
		t.Fatal("broad database directory accepted")
	}
	private := privateSQLiteDir(t)
	path := filepath.Join(private, "jobs.db")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLiteStore(path, testConfig()); err == nil {
		t.Fatal("broad database file accepted")
	}
}

func TestSQLiteStoreRecoveryHonorsCanceledContext(t *testing.T) {
	store, err := OpenSQLiteStore(":memory:", testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Recover(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Recover() error = %v, want context.Canceled", err)
	}
}

func TestSQLiteStoreWaitClaimReturnsOnClose(t *testing.T) {
	store, err := OpenSQLiteStore(":memory:", testConfig())
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, err := store.WaitClaim(context.Background()); result <- err }()
	store.Close()
	select {
	case err := <-result:
		if !errors.Is(err, ErrManagerClosed) {
			t.Fatalf("WaitClaim() = %v, want closed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitClaim did not wake after Close")
	}
	_ = store.CloseStore()
}

func TestSQLiteStoreRejectsConcurrentProcessAndRecoversAfterUncleanExit(t *testing.T) {
	if os.Getenv("FACTORY_SQLITE_LOCK_HELPER") == "1" {
		store, err := OpenSQLiteServerStore(os.Getenv("FACTORY_SQLITE_LOCK_OPEN_PATH"), testConfig())
		if os.Getenv("FACTORY_SQLITE_LOCK_COMPETITOR") == "1" {
			if err == nil {
				_ = store.CloseStore()
				t.Fatal("competing process opened the live SQLite store")
			}
			if !strings.Contains(err.Error(), "already owned by another server") {
				t.Fatalf("competing process error = %v, want ownership conflict", err)
			}
			return
		}
		if err != nil {
			t.Fatalf("owner open SQLite store: %v", err)
		}
		job, _, err := store.Admit("live-process-job", Request{Repository: "widget", Task: "remain live while owner holds the database"})
		if err != nil {
			t.Fatalf("owner admit job: %v", err)
		}
		if _, err := store.ClaimNext(); err != nil {
			t.Fatalf("owner claim job: %v", err)
		}
		if err := os.WriteFile(os.Getenv("FACTORY_SQLITE_LOCK_READY"), []byte(job.ID), 0o600); err != nil {
			t.Fatalf("owner signal readiness: %v", err)
		}
		select {}
	}

	dir := privateSQLiteDir(t)
	path := filepath.Join(dir, "jobs.db")
	alias := filepath.Join(dir, "jobs-alias.db")
	ready := filepath.Join(dir, "owner-ready")
	owner := exec.Command(os.Args[0], "-test.run=^TestSQLiteStoreRejectsConcurrentProcessAndRecoversAfterUncleanExit$")
	owner.Env = append(os.Environ(), "FACTORY_SQLITE_LOCK_HELPER=1", "FACTORY_SQLITE_LOCK_OPEN_PATH="+path, "FACTORY_SQLITE_LOCK_READY="+ready)
	if err := owner.Start(); err != nil {
		t.Fatalf("start owner process: %v", err)
	}
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- owner.Wait() }()
	t.Cleanup(func() {
		if owner.ProcessState == nil {
			_ = owner.Process.Kill()
			<-ownerDone
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if owner.ProcessState != nil {
			t.Fatalf("owner process exited before readiness: %v", <-ownerDone)
		}
		time.Sleep(10 * time.Millisecond)
	}
	jobID, err := os.ReadFile(ready)
	if err != nil {
		t.Fatalf("owner did not become ready: %v", err)
	}

	if err := os.Link(path, alias); err != nil {
		t.Fatalf("create hard-link alias: %v", err)
	}
	originalInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat database: %v", err)
	}
	aliasInfo, err := os.Stat(alias)
	if err != nil || !os.SameFile(originalInfo, aliasInfo) {
		t.Fatalf("database alias does not refer to the same file: stat err = %v", err)
	}
	competitor := exec.Command(os.Args[0], "-test.run=^TestSQLiteStoreRejectsConcurrentProcessAndRecoversAfterUncleanExit$")
	competitor.Env = append(os.Environ(), "FACTORY_SQLITE_LOCK_HELPER=1", "FACTORY_SQLITE_LOCK_COMPETITOR=1", "FACTORY_SQLITE_LOCK_OPEN_PATH="+alias)
	if output, err := competitor.CombinedOutput(); err != nil {
		t.Fatalf("competing process should fail closed cleanly: %v: %s", err, output)
	}
	samePathCompetitor := exec.Command(os.Args[0], "-test.run=^TestSQLiteStoreRejectsConcurrentProcessAndRecoversAfterUncleanExit$")
	samePathCompetitor.Env = append(os.Environ(), "FACTORY_SQLITE_LOCK_HELPER=1", "FACTORY_SQLITE_LOCK_COMPETITOR=1", "FACTORY_SQLITE_LOCK_OPEN_PATH="+path)
	if output, err := samePathCompetitor.CombinedOutput(); err != nil {
		t.Fatalf("same-path competing process should fail closed cleanly: %v: %s", err, output)
	}
	observer, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatalf("open read-only test observer: %v", err)
	}
	live, err := observer.Get(string(jobID))
	_ = observer.CloseStore()
	if err != nil || live.Status != StatusRunning {
		t.Fatalf("live job after rejected competitor = (%+v, %v), want running", live, err)
	}

	if err := owner.Process.Kill(); err != nil {
		t.Fatalf("kill owner to simulate unclean exit: %v", err)
	}
	<-ownerDone
	store, err := OpenSQLiteServerStore(path, testConfig())
	if err != nil {
		t.Fatalf("reopen after owner exit: %v", err)
	}
	defer store.CloseStore()
	job, err := store.Get(string(jobID))
	if err != nil || job.Status != StatusRunning {
		t.Fatalf("running job after unclean exit = (%+v, %v)", job, err)
	}
	report, err := store.Recover(context.Background())
	if err != nil || len(report.NeedsOperator) != 1 || report.NeedsOperator[0].ID != string(jobID) || len(report.Resume) != 0 {
		t.Fatalf("unclean-exit recovery = (%+v, %v), want live job held for operator", report, err)
	}
}
