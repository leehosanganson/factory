package factory

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type workWatchStore struct {
	recorded map[string]IssueSnapshot
	order    []string
	calls    int
	err      error
}

func (s *workWatchStore) Record(_ context.Context, _ string, snapshot IssueSnapshot) (bool, error) {
	s.calls++
	if s.err != nil {
		return false, s.err
	}
	if prior, exists := s.recorded[snapshot.Version]; exists {
		if !sameIssueSnapshot(prior, snapshot) {
			return false, errors.New("conflicting snapshot")
		}
		return false, nil
	}
	s.recorded[snapshot.Version] = snapshot
	s.order = append(s.order, snapshot.Version)
	return true, nil
}

func (s *workWatchStore) List(context.Context, string) ([]IssueObservation, error) {
	observations := make([]IssueObservation, 0, len(s.recorded))
	seen := make(map[string]bool)
	for _, version := range s.order {
		if snapshot, ok := s.recorded[version]; ok && !seen[version] {
			observations = append(observations, IssueObservation{Snapshot: snapshot, ObservedAt: snapshot.UpdatedAt})
			seen[version] = true
		}
	}
	for version, snapshot := range s.recorded {
		if !seen[version] {
			observations = append(observations, IssueObservation{Snapshot: snapshot, ObservedAt: snapshot.UpdatedAt})
		}
	}
	return observations, nil
}

func (s *workWatchStore) RecordDirection(context.Context, string, string, string) (HumanDirection, bool, error) {
	return HumanDirection{}, false, errors.New("directions unsupported by watch test store")
}

func (s *workWatchStore) ListDirections(context.Context, string) ([]HumanDirection, error) {
	return nil, errors.New("directions unsupported by watch test store")
}

func (s *workWatchStore) Reconcile(ctx context.Context, key string) (IssueLifecycleState, error) {
	observations, err := s.List(ctx, key)
	if err != nil {
		return IssueLifecycleState{}, err
	}
	if len(observations) == 0 {
		return IssueLifecycleState{}, errors.New("no observations")
	}
	state := IssueLifecycleState{Status: "baseline", LatestVersion: observations[0].Snapshot.Version}
	for _, observation := range observations {
		if observation.Snapshot.State == "closed" {
			state.Status = "stopped"
		} else if observation.Snapshot.Version != state.LatestVersion && state.Status != "stopped" {
			state.Status = "waiting_for_human"
		}
		state.LatestVersion = observation.Snapshot.Version
	}
	return state, nil
}

func TestWorkWatchDoesNotFetchAfterPersistedClosedObservation(t *testing.T) {
	item := WorkItem{Request: WorkRequest{DeduplicationKey: "test-request", Repository: "acme/widget", IssueID: "42"}}
	store := &workWatchStore{recorded: map[string]IssueSnapshot{
		"open-version":   testIssueSnapshot("open-version", "open", "Open", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
		"closed-version": testIssueSnapshot("closed-version", "closed", "Done", time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)),
	}, order: []string{"open-version", "closed-version"}}
	tracker := issueTrackerFunc(func(context.Context, string, string) (IssueSnapshot, error) {
		t.Fatal("terminal watch fetched from provider")
		return IssueSnapshot{}, nil
	})
	var out strings.Builder
	if err := watchIssue(context.Background(), item, tracker, store, time.Second, &out, func(context.Context, time.Duration) error {
		t.Fatal("terminal watch waited for another poll")
		return nil
	}); err != nil {
		t.Fatalf("watch persisted terminal state: %v", err)
	}
	if got := out.String(); got != "Lifecycle: stopped\n" {
		t.Fatalf("terminal watch output = %q, want explicit stopped lifecycle only", got)
	}
	if store.calls != 0 || len(store.recorded) != 2 {
		t.Fatalf("terminal watch changed observations: record calls=%d observations=%d", store.calls, len(store.recorded))
	}
}

func TestWorkWatchResumesPollingFromPersistedOpenBaseline(t *testing.T) {
	item := WorkItem{Request: WorkRequest{DeduplicationKey: "test-request", Repository: "acme/widget", IssueID: "42"}}
	baseline := testIssueSnapshot("open-version-1", "open", "Open", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	updated := testIssueSnapshot("open-version-2", "open", "Updated", time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC))
	store := &workWatchStore{recorded: map[string]IssueSnapshot{baseline.Version: baseline}, order: []string{baseline.Version}}
	fetches := 0
	tracker := issueTrackerFunc(func(context.Context, string, string) (IssueSnapshot, error) {
		fetches++
		return updated, nil
	})
	waits := 0
	var out strings.Builder
	err := watchIssue(context.Background(), item, tracker, store, time.Second, &out, func(context.Context, time.Duration) error {
		waits++
		return context.Canceled
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("open-baseline watch error = %v, want context.Canceled", err)
	}
	if fetches != 1 || waits != 1 || store.calls != 1 {
		t.Fatalf("open-baseline watch fetches/waits/records = %d/%d/%d, want 1/1/1", fetches, waits, store.calls)
	}
	for _, want := range []string{"Title: Updated", "Observation: newly recorded", "Lifecycle: waiting_for_human"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("open-baseline watch output missing %q: %s", want, out.String())
		}
	}
}

func TestWorkWatchRecordsUpdatesAndStopsAfterClosedObservation(t *testing.T) {
	cfg, key := enqueueTestWorkRequest(t, "github", "42")
	item, err := readWorkItem(t, cfg, key)
	if err != nil {
		t.Fatal(err)
	}
	before := item
	snapshots := []IssueSnapshot{
		testIssueSnapshot("version-1", "open", "Initial", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
		testIssueSnapshot("version-2", "open", "Updated", time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)),
		testIssueSnapshot("version-3", "closed", "Done", time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC)),
	}
	fetches, waits := 0, 0
	tracker := issueTrackerFunc(func(context.Context, string, string) (IssueSnapshot, error) {
		snapshot := snapshots[fetches]
		fetches++
		return snapshot, nil
	})
	store := &workWatchStore{recorded: make(map[string]IssueSnapshot)}
	var out strings.Builder
	interval := 17 * time.Second
	err = watchIssue(context.Background(), item, tracker, store, interval, &out, func(_ context.Context, got time.Duration) error {
		waits++
		if got != interval {
			t.Fatalf("wait interval = %s, want %s", got, interval)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if fetches != 3 || waits != 2 || store.calls != 3 || len(store.recorded) != 3 {
		t.Fatalf("watch fetches/waits/records = %d/%d/%d, versions=%d", fetches, waits, store.calls, len(store.recorded))
	}
	for _, want := range []string{"Title: Initial", "Title: Updated", "Title: Done", "Observation: newly recorded", "Lifecycle: baseline", "Lifecycle: waiting_for_human", "Lifecycle: stopped"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("watch output missing %q: %s", want, out.String())
		}
	}
	after, err := readWorkItem(t, cfg, key)
	if err != nil || after != before {
		t.Fatalf("watch changed queued request: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestWorkWatchReportsDuplicateVersionsAndCancellationStopsNextFetch(t *testing.T) {
	cfg, key := enqueueTestWorkRequest(t, "github", "42")
	item, err := readWorkItem(t, cfg, key)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := testIssueSnapshot("same-version", "open", "Still open", time.Now().UTC())
	fetches := 0
	tracker := issueTrackerFunc(func(context.Context, string, string) (IssueSnapshot, error) {
		fetches++
		return snapshot, nil
	})
	store := &workWatchStore{recorded: make(map[string]IssueSnapshot)}
	ctx, cancel := context.WithCancel(context.Background())
	var out strings.Builder
	err = watchIssue(ctx, item, tracker, store, time.Second, &out, func(context.Context, time.Duration) error {
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("watch cancellation error = %v, want context.Canceled", err)
	}
	if fetches != 1 || store.calls != 1 || !strings.Contains(out.String(), "Observation: newly recorded") {
		t.Fatalf("canceled watch fetched=%d recorded=%d output=%s", fetches, store.calls, out.String())
	}

	store = &workWatchStore{recorded: map[string]IssueSnapshot{snapshot.Version: snapshot}}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	out.Reset()
	err = watchIssue(ctx, item, tracker, store, time.Second, &out, func(context.Context, time.Duration) error {
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) || fetches != 2 || store.calls != 1 || !strings.Contains(out.String(), "Observation: duplicate") {
		t.Fatalf("duplicate watch err=%v fetches=%d records=%d output=%s", err, fetches, store.calls, out.String())
	}
}

func TestWorkWatchExitsOnProviderOrStoreFailure(t *testing.T) {
	item := WorkItem{Request: WorkRequest{DeduplicationKey: "test-request", Repository: "acme/widget", IssueID: "42"}}
	providerErr := errors.New("provider unavailable")
	tracker := issueTrackerFunc(func(context.Context, string, string) (IssueSnapshot, error) { return IssueSnapshot{}, providerErr })
	store := &workWatchStore{recorded: make(map[string]IssueSnapshot)}
	if err := watchIssue(context.Background(), item, tracker, store, time.Second, &strings.Builder{}, func(context.Context, time.Duration) error { t.Fatal("waited after provider failure"); return nil }); !errors.Is(err, providerErr) {
		t.Fatalf("provider error = %v, want wrapped provider failure", err)
	}

	storeErr := errors.New("disk full")
	tracker = issueTrackerFunc(func(context.Context, string, string) (IssueSnapshot, error) {
		return testIssueSnapshot("version", "open", "Open", time.Now().UTC()), nil
	})
	store = &workWatchStore{err: storeErr, recorded: make(map[string]IssueSnapshot)}
	if err := watchIssue(context.Background(), item, tracker, store, time.Second, &strings.Builder{}, func(context.Context, time.Duration) error { t.Fatal("waited after store failure"); return nil }); !errors.Is(err, storeErr) {
		t.Fatalf("store error = %v, want wrapped store failure", err)
	}
}

func TestParseWorkWatchRequiresPositiveGoDuration(t *testing.T) {
	for _, args := range [][]string{{}, {"key", "--interval"}, {"key", "--interval", "bad"}, {"key", "--interval", "0s"}, {"key", "--interval", "-1m"}, {"key", "--interval", "1m", "extra"}} {
		if _, _, err := parseWorkWatch(args); err == nil {
			t.Errorf("parseWorkWatch(%q) succeeded; want validation error", args)
		}
	}
	key, interval, err := parseWorkWatch([]string{"my-key"})
	if err != nil || key != "my-key" || interval != time.Minute {
		t.Fatalf("default interval parse = %q, %s, %v", key, interval, err)
	}
	key, interval, err = parseWorkWatch([]string{"my-key", "--interval", "250ms"})
	if err != nil || key != "my-key" || interval != 250*time.Millisecond {
		t.Fatalf("override interval parse = %q, %s, %v", key, interval, err)
	}
}

func TestWaitWorkWatchIntervalStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	err := waitWorkWatchInterval(ctx, time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wait cancellation error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("canceled watch wait took %s; want prompt cancellation", elapsed)
	}
}

func TestWorkWatchPreflightRejectsMissingAndUnsupportedBeforeFetch(t *testing.T) {
	calls := 0
	tracker := issueTrackerFunc(func(context.Context, string, string) (IssueSnapshot, error) {
		calls++
		return IssueSnapshot{}, nil
	})
	for _, tc := range []struct {
		name, provider, key string
		missing             bool
	}{
		{name: "missing", key: "missing", missing: true},
		{name: "unsupported", provider: "azure-boards", key: "test-request"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{StateDir: t.TempDir()}
			key := tc.key
			if !tc.missing {
				cfg, key = enqueueTestWorkRequest(t, tc.provider, "42")
			}
			var out strings.Builder
			err := WorkCommandWithIssueTracker(context.Background(), []string{"watch", key, "--interval", "1ms"}, cfg, &out, tracker)
			if err == nil {
				t.Fatal("watch preflight unexpectedly succeeded")
			}
			if tc.missing && !strings.Contains(err.Error(), "not found") || !tc.missing && !strings.Contains(err.Error(), "only github is supported") {
				t.Fatalf("preflight error = %v", err)
			}
		})
	}
	if calls != 0 {
		t.Fatalf("preflight invoked tracker %d times", calls)
	}
}

func readWorkItem(t *testing.T, cfg Config, key string) (WorkItem, error) {
	t.Helper()
	queue, err := openWorkQueue(cfg.StateDir)
	if err != nil {
		return WorkItem{}, err
	}
	defer queue.Close()
	return queue.Get(context.Background(), key)
}
