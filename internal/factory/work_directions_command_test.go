package factory

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestWorkRespondAndDirectionsAreLocalAndLeaveQueueAndLifecycleUnchanged(t *testing.T) {
	cfg, key := enqueueTestWorkRequest(t, "github", "42")
	store, err := openIssueObservationStore(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"base", "changed"} {
		if _, err := store.Record(context.Background(), key, testIssueSnapshot(version, "open", version, time.Now())); err != nil {
			t.Fatal(err)
		}
	}
	beforeLifecycle, err := store.Reconcile(context.Background(), key)
	if err != nil || beforeLifecycle.Status != "waiting_for_human" {
		t.Fatalf("precondition lifecycle = %+v, %v", beforeLifecycle, err)
	}
	queue, err := openWorkQueue(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	beforeQueue, err := queue.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}

	trackerCalls := 0
	tracker := issueTrackerFunc(func(context.Context, string, string) (IssueSnapshot, error) {
		trackerCalls++
		return IssueSnapshot{}, nil
	})
	var output strings.Builder
	if err := WorkCommandWithIssueTracker(context.Background(), []string{"respond", key, "--version", "changed", "--instruction", "Keep existing behavior; do not remove compatibility."}, cfg, &output, tracker); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Direction: recorded") || !strings.Contains(output.String(), "No lifecycle, queue, or engineering action was taken.") {
		t.Fatalf("respond output = %q", output.String())
	}
	output.Reset()
	if err := WorkCommandWithIssueTracker(context.Background(), []string{"respond", key, "--version", "changed", "--instruction", "Keep existing behavior; do not remove compatibility."}, cfg, &output, tracker); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Direction: already recorded") {
		t.Fatalf("repeat respond output = %q; want idempotent confirmation", output.String())
	}
	output.Reset()
	if err := WorkCommandWithIssueTracker(context.Background(), []string{"respond", key, "--version", "changed", "--instruction", "Overwrite the previous direction."}, cfg, &output, tracker); err == nil || !strings.Contains(err.Error(), "different human direction") {
		t.Fatalf("conflicting CLI response error = %v", err)
	}
	output.Reset()
	if err := WorkCommandWithIssueTracker(context.Background(), []string{"directions", key}, cfg, &output, tracker); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Keep existing behavior; do not remove compatibility.") || !strings.Contains(output.String(), "Issue version: changed") {
		t.Fatalf("directions output = %q", output.String())
	}
	if trackerCalls != 0 {
		t.Fatalf("local commands made %d issue tracker calls", trackerCalls)
	}

	afterLifecycle, err := store.Reconcile(context.Background(), key)
	if err != nil || afterLifecycle != beforeLifecycle {
		t.Fatalf("direction changed lifecycle: before=%+v after=%+v err=%v", beforeLifecycle, afterLifecycle, err)
	}
	queue, err = openWorkQueue(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	afterQueue, err := queue.Get(context.Background(), key)
	if err != nil || afterQueue != beforeQueue {
		t.Fatalf("direction changed queue: before=%+v after=%+v err=%v", beforeQueue, afterQueue, err)
	}
}

func TestWorkRespondRejectsWithoutChangingRecordsOrCallingProvider(t *testing.T) {
	cfg, key := enqueueTestWorkRequest(t, "github", "42")
	store, err := openIssueObservationStore(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Record(context.Background(), key, testIssueSnapshot("baseline", "open", "baseline", time.Now())); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	calls := 0
	tracker := issueTrackerFunc(func(context.Context, string, string) (IssueSnapshot, error) {
		calls++
		return IssueSnapshot{}, nil
	})
	for _, args := range [][]string{
		{"respond", key, "--version", "baseline", "--instruction", "not waiting"},
		{"respond", key, "--version", "stale", "--instruction", "stale"},
	} {
		var output strings.Builder
		if err := WorkCommandWithIssueTracker(context.Background(), args, cfg, &output, tracker); err == nil {
			t.Errorf("respond %v succeeded outside waiting lifecycle", args)
		}
	}
	directions, err := store.ListDirections(context.Background(), key)
	if err != nil || len(directions) != 0 {
		t.Fatalf("rejected response created directions: %+v, %v", directions, err)
	}
	if calls != 0 {
		t.Fatalf("rejected local commands made %d provider calls", calls)
	}
}

func TestWorkRespondOutputSanitizesInstruction(t *testing.T) {
	cfg, key := enqueueTestWorkRequest(t, "github", "42")
	store, err := openIssueObservationStore(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"base", "changed"} {
		if _, err := store.Record(context.Background(), key, testIssueSnapshot(version, "open", version, time.Now())); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	instruction := "Safe" + string([]byte{0x1b}) + "[31mText\nInjected: true"
	var output strings.Builder
	if err := WorkCommand(context.Background(), []string{"respond", key, "--version", "changed", "--instruction", instruction}, cfg, &output); err != nil {
		t.Fatal(err)
	}
	if strings.ContainsRune(output.String(), 0x1b) || strings.Contains(output.String(), "\nInjected: true") || !strings.Contains(output.String(), "Instruction: SafeText Injected: true") {
		t.Fatalf("direction output was not sanitized: %q", output.String())
	}
}
