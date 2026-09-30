package factory

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLocalIssueObservationStoreReopensAndPreservesVersionHistory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "work-observations")
	first, err := NewLocalIssueObservationStore(root)
	if err != nil {
		t.Fatal(err)
	}
	older := testIssueSnapshot("v1", "open", "Initial report", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	newer := testIssueSnapshot("v2", "closed", "Resolved report", time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC))
	if recorded, err := first.Record(context.Background(), "request-1", newer); err != nil || !recorded {
		t.Fatalf("Record(newer) = %v, %v; want newly recorded", recorded, err)
	}
	if recorded, err := first.Record(context.Background(), "request-1", older); err != nil || !recorded {
		t.Fatalf("Record(older) = %v, %v; want newly recorded", recorded, err)
	}

	reopened, err := NewLocalIssueObservationStore(root)
	if err != nil {
		t.Fatal(err)
	}
	observations, err := reopened.List(context.Background(), "request-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 2 || observations[0].Snapshot.Version != "v1" || observations[1].Snapshot.Version != "v2" {
		t.Fatalf("reopened history = %+v; want both ordered snapshot versions", observations)
	}
	if observations[0].Snapshot.State != "open" || observations[1].Snapshot.State != "closed" {
		t.Fatalf("history did not preserve snapshot fields: %+v", observations)
	}
	if observations[0].ObservedAt.IsZero() || observations[1].ObservedAt.IsZero() {
		t.Fatalf("history omitted observation timestamps: %+v", observations)
	}
}

func TestLocalIssueObservationStorePreservesRapidRecordChronology(t *testing.T) {
	store, err := NewLocalIssueObservationStore(filepath.Join(t.TempDir(), "observations"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "rapid-records"
	versions := []string{"z-open-baseline", "y-open-update", "a-closed"}
	states := []string{"open", "open", "closed"}
	wantStatuses := []string{"baseline", "waiting_for_human", "stopped"}
	providerUpdatedAt := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, version := range versions {
		if recorded, err := store.Record(ctx, key, testIssueSnapshot(version, states[i], version, providerUpdatedAt)); err != nil || !recorded {
			t.Fatalf("Record(%s) = %v, %v; want newly recorded", version, recorded, err)
		}
		state, err := store.Reconcile(ctx, key)
		if err != nil || state.Status != wantStatuses[i] {
			t.Fatalf("Reconcile() after %s = %+v, %v; want %s", version, state, err, wantStatuses[i])
		}
	}

	observations, err := store.List(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != len(versions) {
		t.Fatalf("List() returned %d observations; want %d", len(observations), len(versions))
	}
	observedAtByVersion := make(map[string]time.Time, len(observations))
	for _, observation := range observations {
		observedAtByVersion[observation.Snapshot.Version] = observation.ObservedAt
	}
	for i := 1; i < len(versions); i++ {
		if !observedAtByVersion[versions[i-1]].Before(observedAtByVersion[versions[i]]) {
			t.Errorf("ObservedAt timestamps are not strictly increasing for record order: %s then %s", observedAtByVersion[versions[i-1]], observedAtByVersion[versions[i]])
		}
	}
	state, err := store.Reconcile(ctx, key)
	if err != nil || state.Status != "stopped" || state.BaselineVersion != "z-open-baseline" || state.LatestVersion != "a-closed" {
		t.Fatalf("final lifecycle = %+v, %v; want record-order baseline and terminal close", state, err)
	}
}

func TestLocalIssueObservationStoreDuplicateVersionIsIdempotentAndConflictFails(t *testing.T) {
	store, err := NewLocalIssueObservationStore(filepath.Join(t.TempDir(), "observations"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := testIssueSnapshot("same-version", "open", "Issue", time.Now().UTC())
	if recorded, err := store.Record(context.Background(), "request-1", snapshot); err != nil || !recorded {
		t.Fatalf("first Record() = %v, %v; want newly recorded", recorded, err)
	}
	beforeDuplicate, err := store.List(context.Background(), "request-1")
	if err != nil || len(beforeDuplicate) != 1 {
		t.Fatalf("List() before duplicate = %+v, %v", beforeDuplicate, err)
	}
	if recorded, err := store.Record(context.Background(), "request-1", snapshot); err != nil || recorded {
		t.Fatalf("duplicate Record() = %v, %v; want idempotent duplicate", recorded, err)
	}
	afterDuplicate, err := store.List(context.Background(), "request-1")
	if err != nil || len(afterDuplicate) != 1 || !afterDuplicate[0].ObservedAt.Equal(beforeDuplicate[0].ObservedAt) {
		t.Fatalf("duplicate changed observation timestamp: before=%+v after=%+v err=%v", beforeDuplicate, afterDuplicate, err)
	}
	changed := snapshot
	changed.Title = "different contents with a reused version"
	if _, err := store.Record(context.Background(), "request-1", changed); err == nil || !strings.Contains(err.Error(), "version conflicts") {
		t.Fatalf("conflicting version error = %v; want conflict", err)
	}
	observations, err := store.List(context.Background(), "request-1")
	if err != nil || len(observations) != 1 || observations[0].Snapshot.Title != snapshot.Title {
		t.Fatalf("history after duplicate/conflict = %+v, %v", observations, err)
	}
}

func TestLocalIssueObservationStoreUsesPrivateFilesAndRejectsSymlinks(t *testing.T) {
	root := filepath.Join(t.TempDir(), "observations")
	store, err := NewLocalIssueObservationStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if recorded, err := store.Record(context.Background(), "request-1", testIssueSnapshot("v1", "open", "Issue", time.Now().UTC())); err != nil || !recorded {
		t.Fatalf("Record() = %v, %v", recorded, err)
	}
	checkPrivateMode(t, root)
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var recordPath string
	for _, entry := range entries {
		if entry.Name() != "observations.lock" {
			recordPath = filepath.Join(root, entry.Name())
			break
		}
	}
	if recordPath == "" {
		t.Fatal("observation record not found")
	}
	checkPrivateMode(t, recordPath)

	t.Run("record symlink", func(t *testing.T) {
		otherRoot := filepath.Join(t.TempDir(), "observations")
		other, err := NewLocalIssueObservationStore(otherRoot)
		if err != nil {
			t.Fatal(err)
		}
		victim := filepath.Join(t.TempDir(), "victim.json")
		if err := os.WriteFile(victim, []byte("untouched"), 0o600); err != nil {
			t.Fatal(err)
		}
		linkPath := other.recordPath("request-1", "v1")
		if err := os.Symlink(victim, linkPath); err != nil {
			t.Fatal(err)
		}
		if _, err := other.Record(context.Background(), "request-1", testIssueSnapshot("v1", "open", "Issue", time.Now().UTC())); err == nil {
			t.Fatal("Record() accepted a symlink record")
		}
		data, err := os.ReadFile(victim)
		if err != nil || string(data) != "untouched" {
			t.Fatalf("symlink target changed: %q, %v", data, err)
		}
	})

	t.Run("store root symlink", func(t *testing.T) {
		base := t.TempDir()
		target := filepath.Join(base, "real")
		if err := os.Mkdir(target, 0o700); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(base, "link")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		if _, err := NewLocalIssueObservationStore(link); err == nil {
			t.Fatal("constructor accepted a symlink root")
		}
	})
}

func TestLocalIssueObservationStoreRejectsCorruptRecords(t *testing.T) {
	store, err := NewLocalIssueObservationStore(filepath.Join(t.TempDir(), "observations"))
	if err != nil {
		t.Fatal(err)
	}
	if recorded, err := store.Record(context.Background(), "request-1", testIssueSnapshot("v1", "open", "Issue", time.Now().UTC())); err != nil || !recorded {
		t.Fatalf("Record() = %v, %v", recorded, err)
	}
	path := store.recordPath("request-1", "v1")
	if err := os.WriteFile(path, []byte(`{"record_version":1,"observation":`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.List(context.Background(), "request-1"); err == nil || !strings.Contains(err.Error(), "invalid issue observation record") {
		t.Fatalf("List() error = %v; want corrupt record error", err)
	}
}

func TestLocalIssueObservationStoreIsolatesRequestKeys(t *testing.T) {
	store, err := NewLocalIssueObservationStore(filepath.Join(t.TempDir(), "observations"))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"request-1", "request-2"} {
		if _, err := store.Record(context.Background(), key, testIssueSnapshot("v1", "open", key, time.Now().UTC())); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.List(context.Background(), "request-1")
	if err != nil || len(got) != 1 || got[0].Snapshot.Title != "request-1" {
		t.Fatalf("request-1 history = %+v, %v", got, err)
	}
}

func testIssueSnapshot(version, state, title string, updated time.Time) IssueSnapshot {
	return IssueSnapshot{
		Repository: "acme/widget", Number: 42, Title: title, Body: "details", State: state,
		URL: "https://github.com/acme/widget/issues/42", UpdatedAt: updated, Version: version,
	}
}

func checkPrivateMode(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Errorf("permissions for %s = %o; want private", path, info.Mode().Perm())
	}
}
