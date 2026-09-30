package factory

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHumanDirectionRequiresReconciledWaitingOpenLatestVersion(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalIssueObservationStore(filepath.Join(t.TempDir(), "observations"))
	if err != nil {
		t.Fatal(err)
	}
	key := "direction-gates"
	attempt := func(version, instruction string) error {
		t.Helper()
		_, _, err := store.RecordDirection(ctx, key, version, instruction)
		return err
	}
	if err := attempt("v1", "please clarify"); err == nil || !strings.Contains(err.Error(), "without issue observations") {
		t.Fatalf("direction without observations error = %v", err)
	}

	if _, err := store.Record(ctx, key, testIssueSnapshot("v1", "open", "baseline", time.Now())); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := attempt("v1", "please clarify"); err == nil || !strings.Contains(err.Error(), "not waiting for human direction") {
		t.Fatalf("baseline direction error = %v", err)
	}

	if _, err := store.Record(ctx, key, testIssueSnapshot("v2", "open", "changed", time.Now().Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	waiting, err := store.Reconcile(ctx, key)
	if err != nil || waiting.Status != "waiting_for_human" || waiting.LatestVersion != "v2" {
		t.Fatalf("waiting lifecycle = %+v, %v", waiting, err)
	}
	direction, recorded, err := store.RecordDirection(ctx, key, "v2", "Please keep the compatibility behavior.")
	if err != nil || !recorded || direction.IssueVersion != "v2" {
		t.Fatalf("RecordDirection() = %+v, %v, %v; want new pinned direction", direction, recorded, err)
	}
	duplicate, recorded, err := store.RecordDirection(ctx, key, "v2", direction.Instruction)
	if err != nil || recorded || duplicate != direction {
		t.Fatalf("exact repeat = %+v, %v, %v; want same idempotent record", duplicate, recorded, err)
	}
	if err := attempt("v2", "replace the previous instruction"); err == nil || !strings.Contains(err.Error(), "different human direction") {
		t.Fatalf("conflicting direction error = %v", err)
	}

	if _, err := store.Record(ctx, key, testIssueSnapshot("v3", "open", "changed again", time.Now().Add(2*time.Second))); err != nil {
		t.Fatal(err)
	}
	duplicate, recorded, err = store.RecordDirection(ctx, key, "v2", direction.Instruction)
	if err != nil || recorded || duplicate != direction {
		t.Fatalf("retry after newer observation = %+v, %v, %v; want original idempotent record", duplicate, recorded, err)
	}
	if err := attempt("v2", "different after newer observation"); err == nil || !strings.Contains(err.Error(), "different human direction") {
		t.Fatalf("conflicting retry after newer observation error = %v", err)
	}
	if _, err := store.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, recorded, err := store.RecordDirection(ctx, key, "v3", "Address the newest evidence."); err != nil || !recorded {
		t.Fatalf("new-version direction recorded=%v err=%v", recorded, err)
	}

	if _, err := store.Record(ctx, key, testIssueSnapshot("v4", "closed", "closed", time.Now().Add(3*time.Second))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := attempt("v4", "issue is closed"); err == nil || !strings.Contains(err.Error(), "not the latest open observation") {
		t.Fatalf("closed issue direction error = %v", err)
	}
	v3, recorded, err := store.RecordDirection(ctx, key, "v3", "Address the newest evidence.")
	if err != nil || recorded || v3.IssueVersion != "v3" {
		t.Fatalf("retry after closure = %+v, %v, %v; want original idempotent record", v3, recorded, err)
	}
	if err := attempt("v3", "different after closure"); err == nil || !strings.Contains(err.Error(), "different human direction") {
		t.Fatalf("conflicting retry after closure error = %v", err)
	}
	listed, err := store.ListDirections(ctx, key)
	if err != nil || len(listed) != 2 || listed[0].IssueVersion != "v2" || listed[1].IssueVersion != "v3" {
		t.Fatalf("directions = %+v, %v; want ordered directions for v2 and v3", listed, err)
	}
}

func TestHumanDirectionRejectsInvalidPayloadWithoutWriting(t *testing.T) {
	store, err := NewLocalIssueObservationStore(filepath.Join(t.TempDir(), "observations"))
	if err != nil {
		t.Fatal(err)
	}
	for _, instruction := range []string{"", " \t\n", string([]byte{0xff}), strings.Repeat("x", MaxHumanDirectionBytes+1)} {
		if _, _, err := store.RecordDirection(context.Background(), "bad-direction", "v1", instruction); err == nil {
			t.Errorf("RecordDirection accepted invalid instruction with length %d", len(instruction))
		}
	}
	directions, err := store.ListDirections(context.Background(), "bad-direction")
	if err != nil || len(directions) != 0 {
		t.Fatalf("invalid instructions left records: %+v, %v", directions, err)
	}
}

func TestHumanDirectionPersistenceOrderingPermissionsAndUnsafeRecords(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "observations")
	store, err := NewLocalIssueObservationStore(root)
	if err != nil {
		t.Fatal(err)
	}
	key := "direction-persistence"
	for i, version := range []string{"v1", "z-first", "a-second"} {
		if _, err := store.Record(ctx, key, testIssueSnapshot(version, "open", version, time.Now().Add(time.Duration(i)*time.Second))); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Reconcile(ctx, key); err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			if _, recorded, err := store.RecordDirection(ctx, key, version, "Direction for "+version); err != nil || !recorded {
				t.Fatalf("record %s: recorded=%v err=%v", version, recorded, err)
			}
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var directionPath string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "direction-") {
			directionPath = filepath.Join(root, entry.Name())
			checkPrivateMode(t, directionPath)
		}
	}
	if directionPath == "" {
		t.Fatal("no direction record was persisted")
	}
	reopened, err := NewLocalIssueObservationStore(root)
	if err != nil {
		t.Fatal(err)
	}
	directions, err := reopened.ListDirections(ctx, key)
	if err != nil || len(directions) != 2 || directions[0].IssueVersion != "z-first" || directions[1].IssueVersion != "a-second" {
		t.Fatalf("reopened directions = %+v, %v; want chronological z-first/a-second records", directions, err)
	}
	if !directions[1].RecordedAt.After(directions[0].RecordedAt) {
		t.Fatalf("direction timestamps do not preserve recording order: %+v", directions)
	}

	t.Run("symlink", func(t *testing.T) {
		linkRoot := filepath.Join(t.TempDir(), "observations")
		linkStore, err := NewLocalIssueObservationStore(linkRoot)
		if err != nil {
			t.Fatal(err)
		}
		for _, version := range []string{"base", "v1"} {
			if _, err := linkStore.Record(ctx, "link-key", testIssueSnapshot(version, "open", version, time.Now())); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := linkStore.Reconcile(ctx, "link-key"); err != nil {
			t.Fatal(err)
		}
		victim := filepath.Join(t.TempDir(), "victim")
		if err := os.WriteFile(victim, []byte("untouched"), 0o600); err != nil {
			t.Fatal(err)
		}
		linkPath := linkStore.directionPath("link-key", "v1")
		if err := os.Symlink(victim, linkPath); err != nil {
			t.Fatal(err)
		}
		if _, _, err := linkStore.RecordDirection(ctx, "link-key", "v1", "direction"); err == nil {
			t.Fatal("RecordDirection accepted a direction-record symlink")
		}
		if got, err := os.ReadFile(victim); err != nil || string(got) != "untouched" {
			t.Fatalf("symlink victim changed to %q, err=%v", got, err)
		}
	})

	t.Run("corrupt schema", func(t *testing.T) {
		corruptRoot := filepath.Join(t.TempDir(), "observations")
		corruptStore, err := NewLocalIssueObservationStore(corruptRoot)
		if err != nil {
			t.Fatal(err)
		}
		path := corruptStore.directionPath("corrupt-key", "v1")
		bad, _ := json.Marshal(humanDirectionRecord{RecordVersion: humanDirectionRecordVersion + 1, Direction: HumanDirection{DeduplicationKey: "corrupt-key", IssueVersion: "v1", Instruction: "bad", RecordedAt: time.Now()}})
		if err := os.WriteFile(path, bad, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := corruptStore.ListDirections(ctx, "corrupt-key"); err == nil || !strings.Contains(err.Error(), "invalid human direction record") {
			t.Fatalf("corrupt direction error = %v", err)
		}
	})
}

func TestHumanDirectionRequiresPersistedLifecycleToBePresentAndCurrent(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalIssueObservationStore(filepath.Join(t.TempDir(), "observations"))
	if err != nil {
		t.Fatal(err)
	}
	key := "missing-lifecycle"
	for _, version := range []string{"v1", "v2"} {
		if _, err := store.Record(ctx, key, testIssueSnapshot(version, "open", version, time.Now())); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := store.RecordDirection(ctx, key, "v2", "no lifecycle"); err == nil {
		t.Fatal("RecordDirection repaired or accepted a missing persisted lifecycle")
	}
	if _, err := store.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(store.lifecyclePath(key)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordDirection(ctx, key, "v2", "still missing"); err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing lifecycle error = %v; want not-exist", err)
	}
	if directions, err := store.ListDirections(ctx, key); err != nil || len(directions) != 0 {
		t.Fatalf("invalid lifecycle created a direction: %+v, %v", directions, err)
	}
}
