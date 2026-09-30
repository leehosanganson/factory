package factory

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestIssueLifecycleReconcilesObservationHistoryAndRemainsTerminal(t *testing.T) {
	root := filepath.Join(t.TempDir(), "observations")
	store, err := NewLocalIssueObservationStore(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "lifecycle-request"
	at := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	record := func(version, state string, updated time.Time) {
		t.Helper()
		if added, err := store.Record(ctx, key, testIssueSnapshot(version, state, version, updated)); err != nil || !added {
			t.Fatalf("Record(%s) = %v, %v; want new observation", version, added, err)
		}
	}
	reconcile := func(want string) IssueLifecycleState {
		t.Helper()
		state, err := store.Reconcile(ctx, key)
		if err != nil || state.Status != want {
			t.Fatalf("Reconcile() = %+v, %v; want %s", state, err, want)
		}
		return state
	}

	record("open-1", "open", at)
	baseline := reconcile("baseline")
	if baseline.BaselineVersion != "open-1" || baseline.LatestVersion != "open-1" {
		t.Fatalf("baseline state = %+v", baseline)
	}
	if added, err := store.Record(ctx, key, testIssueSnapshot("open-1", "open", "open-1", at)); err != nil || added {
		t.Fatalf("duplicate Record() = %v, %v; want idempotent duplicate", added, err)
	}
	if got := reconcile("baseline"); got.UpdatedAt != baseline.UpdatedAt {
		t.Fatalf("duplicate reconciliation rewrote state timestamp: before=%s after=%s", baseline.UpdatedAt, got.UpdatedAt)
	}

	record("open-2", "open", at.Add(time.Hour))
	waiting := reconcile("waiting_for_human")
	if waiting.BaselineVersion != "open-1" || waiting.LatestVersion != "open-2" {
		t.Fatalf("waiting state = %+v", waiting)
	}
	record("open-3", "open", at.Add(2*time.Hour))
	if got := reconcile("waiting_for_human"); got.LatestVersion != "open-3" {
		t.Fatalf("additional update while waiting = %+v", got)
	}
	record("closed", "closed", at.Add(3*time.Hour))
	if got := reconcile("stopped"); got.LatestVersion != "closed" {
		t.Fatalf("closure state = %+v", got)
	}
	record("reopened", "open", at.Add(4*time.Hour))
	if got := reconcile("stopped"); got.LatestVersion != "reopened" {
		t.Fatalf("reopened issue must stay stopped while preserving latest observation: %+v", got)
	}

	reopened, err := NewLocalIssueObservationStore(root)
	if err != nil {
		t.Fatal(err)
	}
	state, err := reopened.Reconcile(ctx, key)
	if err != nil || state.Status != "stopped" || state.LatestVersion != "reopened" {
		t.Fatalf("reopened store lifecycle = %+v, %v", state, err)
	}
	observations, err := reopened.List(ctx, key)
	if err != nil || len(observations) != 5 {
		t.Fatalf("reopened history = %d observations, %v; want all five snapshots", len(observations), err)
	}
}

func TestIssueLifecycleRecoversFromMissingStateAndHistoricalRefreshChronology(t *testing.T) {
	root := filepath.Join(t.TempDir(), "observations")
	store, err := NewLocalIssueObservationStore(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "recovery-request"
	// Provider UpdatedAt order intentionally differs from persistence chronology.
	for _, snapshot := range []IssueSnapshot{
		testIssueSnapshot("older-provider-time", "open", "first observed", time.Date(2025, 2, 2, 0, 0, 0, 0, time.UTC)),
		testIssueSnapshot("newer-provider-time", "open", "second observed", time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)),
	} {
		if _, err := store.Record(ctx, key, snapshot); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	state, err := store.Reconcile(ctx, key)
	if err != nil || state.Status != "waiting_for_human" || state.BaselineVersion != "older-provider-time" {
		t.Fatalf("historical refresh reconciliation = %+v, %v", state, err)
	}
	statePath := store.lifecyclePath(key)
	if err := os.Remove(statePath); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after the immutable observation write but before lifecycle persistence.
	if _, err := store.Record(ctx, key, testIssueSnapshot("closed-version", "closed", "closed", time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewLocalIssueObservationStore(root)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := reopened.Reconcile(ctx, key)
	if err != nil || recovered.Status != "stopped" || recovered.BaselineVersion != "older-provider-time" || recovered.LatestVersion != "closed-version" {
		t.Fatalf("recovered lifecycle = %+v, %v", recovered, err)
	}
}

func TestIssueLifecycleClosureFromBaselinePreservesOpenBaseline(t *testing.T) {
	store, err := NewLocalIssueObservationStore(filepath.Join(t.TempDir(), "observations"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "close-from-baseline"
	openedAt := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := store.Record(ctx, key, testIssueSnapshot("open-version", "open", "Issue", openedAt)); err != nil {
		t.Fatal(err)
	}
	baseline, err := store.Reconcile(ctx, key)
	if err != nil || baseline.Status != "baseline" || baseline.BaselineVersion != "open-version" {
		t.Fatalf("initial baseline state = %+v, %v", baseline, err)
	}
	if _, err := store.Record(ctx, key, testIssueSnapshot("closed-version", "closed", "Issue", openedAt.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	state, err := store.Reconcile(ctx, key)
	if err != nil || state.Status != "stopped" || state.BaselineVersion != "open-version" {
		t.Fatalf("closed baseline state = %+v, %v; want stopped with open baseline", state, err)
	}
}

func TestIssueLifecycleClosureCanBeFirstObservation(t *testing.T) {
	store, err := NewLocalIssueObservationStore(filepath.Join(t.TempDir(), "observations"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Record(context.Background(), "closed-first", testIssueSnapshot("closed", "closed", "Done", time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	state, err := store.Reconcile(context.Background(), "closed-first")
	if err != nil || state.Status != "stopped" || state.BaselineVersion != "" {
		t.Fatalf("first closed observation state = %+v, %v", state, err)
	}
}

func TestIssueLifecycleStoreRejectsSymlinkAndCorruptState(t *testing.T) {
	ctx := context.Background()
	t.Run("symlink", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "observations")
		store, err := NewLocalIssueObservationStore(root)
		if err != nil {
			t.Fatal(err)
		}
		key := "symlink-state"
		if _, err := store.Record(ctx, key, testIssueSnapshot("v1", "open", "Issue", time.Now().UTC())); err != nil {
			t.Fatal(err)
		}
		victim := filepath.Join(t.TempDir(), "victim")
		if err := os.WriteFile(victim, []byte("untouched"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(victim, store.lifecyclePath(key)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Reconcile(ctx, key); err == nil {
			t.Fatal("Reconcile accepted lifecycle state symlink")
		}
		data, err := os.ReadFile(victim)
		if err != nil || string(data) != "untouched" {
			t.Fatalf("symlink target changed to %q, err=%v", data, err)
		}
	})
	t.Run("corruption", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "observations")
		store, err := NewLocalIssueObservationStore(root)
		if err != nil {
			t.Fatal(err)
		}
		key := "corrupt-state"
		if _, err := store.Record(ctx, key, testIssueSnapshot("v1", "open", "Issue", time.Now().UTC())); err != nil {
			t.Fatal(err)
		}
		state := IssueLifecycleState{DeduplicationKey: key, Status: "surprise", LatestVersion: "v1", UpdatedAt: time.Now().UTC()}
		if err := writeJSONAtomic(root, filepath.Base(store.lifecyclePath(key)), issueLifecycleRecord{RecordVersion: issueLifecycleRecordVersion, State: state}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Reconcile(ctx, key); err == nil || !strings.Contains(err.Error(), "invalid issue lifecycle state") {
			t.Fatalf("Reconcile corruption error = %v", err)
		}
	})
}

func TestIssueLifecycleStorePrivateStateAndConcurrentReconciliation(t *testing.T) {
	root := filepath.Join(t.TempDir(), "observations")
	first, err := NewLocalIssueObservationStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Record(context.Background(), "concurrent-state", testIssueSnapshot("v1", "open", "Issue", time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Reconcile(context.Background(), "concurrent-state"); err != nil {
		t.Fatal(err)
	}
	checkPrivateMode(t, first.lifecyclePath("concurrent-state"))
	second, err := NewLocalIssueObservationStore(root)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, store := range []*LocalIssueObservationStore{first, second} {
		wg.Add(1)
		go func(store *LocalIssueObservationStore) {
			defer wg.Done()
			state, err := store.Reconcile(context.Background(), "concurrent-state")
			if err == nil && state.Status != "baseline" {
				err = os.ErrInvalid
			}
			errs <- err
		}(store)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent reconciliation: %v", err)
		}
	}
}
