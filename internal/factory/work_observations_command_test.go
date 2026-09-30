package factory

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestWorkRefreshRejectsMissingAndUnsupportedBeforeFetching(t *testing.T) {
	calls := 0
	tracker := issueTrackerFunc(func(context.Context, string, string) (IssueSnapshot, error) {
		calls++
		return IssueSnapshot{}, nil
	})
	for _, tc := range []struct {
		name, provider, key, want string
	}{
		{"missing", "", "missing", `work request "missing" not found`},
		{"unsupported", "azure-boards", "test-request", `only github is supported`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{StateDir: t.TempDir()}
			key := tc.key
			if tc.provider != "" {
				cfg, key = enqueueTestWorkRequest(t, tc.provider, "42")
			}
			var out strings.Builder
			err := WorkCommandWithIssueTracker(context.Background(), []string{"refresh", key}, cfg, &out, tracker)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("refresh error = %v; want %q", err, tc.want)
			}
		})
	}
	if calls != 0 {
		t.Fatalf("preflight failures invoked tracker %d times", calls)
	}
}

func TestWorkRefreshRecordsThenReportsDuplicateWithoutChangingQueue(t *testing.T) {
	cfg, key := enqueueTestWorkRequest(t, "github", "42")
	queue, err := openWorkQueue(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	before, err := queue.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	_ = queue.Close()
	snapshot := testIssueSnapshot("version-1", "open", "Initial title", time.Date(2025, 2, 3, 4, 5, 6, 0, time.UTC))
	calls := 0
	tracker := issueTrackerFunc(func(_ context.Context, repository, issue string) (IssueSnapshot, error) {
		calls++
		if repository != "acme/widget" || issue != "42" {
			t.Fatalf("tracker request = %q#%q; want queued identity", repository, issue)
		}
		return snapshot, nil
	})
	for i, wantStatus := range []string{"newly recorded", "duplicate"} {
		var out strings.Builder
		if err := WorkCommandWithIssueTracker(context.Background(), []string{"refresh", key}, cfg, &out, tracker); err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"Issue snapshot:", "Title: Initial title", "State: open", "Version: version-1", "Observation: " + wantStatus} {
			if !strings.Contains(out.String(), want) {
				t.Errorf("refresh #%d output missing %q: %s", i+1, want, out.String())
			}
		}
	}
	if calls != 2 {
		t.Fatalf("explicit refresh called tracker %d times; want one call per invocation", calls)
	}
	store, err := openIssueObservationStore(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	observations, err := store.List(context.Background(), key)
	if err != nil || len(observations) != 1 {
		t.Fatalf("persisted refresh history = %+v, %v; want one version", observations, err)
	}
	queue, err = openWorkQueue(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	after, err := queue.Get(context.Background(), key)
	if err != nil || after != before {
		t.Fatalf("refresh changed queued request: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestWorkRefreshFetchFailureDoesNotPersist(t *testing.T) {
	cfg, key := enqueueTestWorkRequest(t, "github", "42")
	tracker := issueTrackerFunc(func(context.Context, string, string) (IssueSnapshot, error) {
		return IssueSnapshot{}, errors.New("GitHub unavailable")
	})
	var out strings.Builder
	err := WorkCommandWithIssueTracker(context.Background(), []string{"refresh", key}, cfg, &out, tracker)
	if err == nil || !strings.Contains(err.Error(), "refresh issue: GitHub unavailable") {
		t.Fatalf("refresh error = %v; want fetch failure", err)
	}
	store, err := openIssueObservationStore(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	observations, err := store.List(context.Background(), key)
	if err != nil || len(observations) != 0 {
		t.Fatalf("fetch failure persisted observations: %+v, %v", observations, err)
	}
}

func TestWorkHistoryListsSnapshotsWithoutFetchingOrChangingQueue(t *testing.T) {
	cfg, key := enqueueTestWorkRequest(t, "github", "42")
	queue, err := openWorkQueue(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	before, err := queue.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	_ = queue.Close()
	store, err := openIssueObservationStore(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range []IssueSnapshot{
		testIssueSnapshot("version-1", "open", "Earlier title", time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)),
		testIssueSnapshot("version-2", "closed", "Latest title", time.Date(2025, 2, 2, 0, 0, 0, 0, time.UTC)),
	} {
		if _, err := store.Record(context.Background(), key, snapshot); err != nil {
			t.Fatal(err)
		}
	}
	calls := 0
	tracker := issueTrackerFunc(func(context.Context, string, string) (IssueSnapshot, error) {
		calls++
		return IssueSnapshot{}, nil
	})
	var out strings.Builder
	if err := WorkCommandWithIssueTracker(context.Background(), []string{"history", key}, cfg, &out, tracker); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("history invoked tracker %d times", calls)
	}
	for _, want := range []string{
		"Title: Earlier title", "State: open", "Updated: 2025-02-01T00:00:00Z", "Version: version-1", "URL: https://github.com/acme/widget/issues/42",
		"Title: Latest title", "State: closed", "Updated: 2025-02-02T00:00:00Z", "Version: version-2", "Lifecycle: stopped",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("history output missing %q: %s", want, out.String())
		}
	}
	queue, err = openWorkQueue(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	after, err := queue.Get(context.Background(), key)
	if err != nil || before != after {
		t.Fatalf("history changed queued request: before=%+v after=%+v err=%v", before, after, err)
	}
}
