package restjobs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
	first, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseStore()
	second, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer second.CloseStore()

	job, replay, err := first.Admit("shared-key", Request{Repository: "widget", Task: "same"})
	if err != nil || replay {
		t.Fatalf("first admission = (%+v, %v, %v)", job, replay, err)
	}
	again, replay, err := second.Admit("shared-key", Request{Repository: "widget", Task: "same"})
	if err != nil || !replay || again.ID != job.ID {
		t.Fatalf("cross-connection replay = (%+v, %v, %v)", again, replay, err)
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
