package factory

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type issueTrackerFunc func(context.Context, string, string) (IssueSnapshot, error)

func (f issueTrackerFunc) GetIssue(ctx context.Context, repository, issue string) (IssueSnapshot, error) {
	return f(ctx, repository, issue)
}

func TestWorkIssueRejectsMissingRequestBeforeFetching(t *testing.T) {
	calls := 0
	tracker := issueTrackerFunc(func(context.Context, string, string) (IssueSnapshot, error) {
		calls++
		return IssueSnapshot{}, nil
	})
	var out strings.Builder
	err := WorkCommandWithIssueTracker(context.Background(), []string{"issue", "missing"}, Config{StateDir: t.TempDir()}, &out, tracker)
	if err == nil || !strings.Contains(err.Error(), `work request "missing" not found`) {
		t.Fatalf("issue inspection error = %v, want missing request", err)
	}
	if calls != 0 || out.Len() != 0 {
		t.Fatalf("missing request made %d tracker calls and wrote %q", calls, out.String())
	}
}

func TestWorkIssueRejectsUnsupportedTrackerBeforeFetching(t *testing.T) {
	cfg, key := enqueueTestWorkRequest(t, "azure-boards", "42")
	calls := 0
	tracker := issueTrackerFunc(func(context.Context, string, string) (IssueSnapshot, error) {
		calls++
		return IssueSnapshot{}, nil
	})
	var out strings.Builder
	err := WorkCommandWithIssueTracker(context.Background(), []string{"issue", key}, cfg, &out, tracker)
	if err == nil || !strings.Contains(err.Error(), `only github is supported`) {
		t.Fatalf("issue inspection error = %v, want unsupported tracker", err)
	}
	if calls != 0 {
		t.Fatalf("unsupported tracker invoked IssueTracker %d times", calls)
	}
	if !strings.Contains(out.String(), "Tracker: azure-boards") || !strings.Contains(out.String(), "Dedup key: "+key) {
		t.Fatalf("queued request identity should be displayed: %s", out.String())
	}
}

func TestWorkIssueFetchesAndRendersOneSnapshotWithoutChangingQueuedItem(t *testing.T) {
	cfg, key := enqueueTestWorkRequest(t, "github", "42")
	queue, err := openWorkQueue(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	before, err := queue.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}

	updatedAt := time.Date(2025, 3, 4, 5, 6, 7, 123000000, time.FixedZone("offset", 2*60*60))
	calls := 0
	tracker := issueTrackerFunc(func(_ context.Context, repository, issue string) (IssueSnapshot, error) {
		calls++
		if repository != "acme/widget" || issue != "42" {
			t.Fatalf("IssueTracker.GetIssue(%q, %q), want queued repository and issue", repository, issue)
		}
		return IssueSnapshot{
			Repository: repository, Number: 42, Title: "Fix widget handling", State: "open",
			UpdatedAt: updatedAt, Version: "snapshot-version", URL: "https://github.com/acme/widget/issues/42",
		}, nil
	})
	var out strings.Builder
	if err := WorkCommandWithIssueTracker(context.Background(), []string{"issue", key}, cfg, &out, tracker); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("IssueTracker called %d times, want exactly one snapshot", calls)
	}
	for _, want := range []string{
		"Dedup key: " + key, "Tracker: github", "Issue: 42", "Repository: acme/widget",
		"Issue snapshot:", "Title: Fix widget handling", "State: open",
		"Updated: 2025-03-04T03:06:07.123Z", "Version: snapshot-version",
		"URL: https://github.com/acme/widget/issues/42",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("issue output missing %q: %s", want, out.String())
		}
	}

	queue, err = openWorkQueue(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	after, err := queue.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("issue inspection changed queued item:\nbefore: %+v\nafter:  %+v", before, after)
	}
	if _, err := os.Stat(filepath.Join(cfg.StateDir, "factory", "work-requests", "records")); err != nil {
		t.Fatalf("queued record directory missing after inspection: %v", err)
	}
}

func TestWorkIssueSanitizesUntrustedTitleBeforeTerminalOutput(t *testing.T) {
	cfg, key := enqueueTestWorkRequest(t, "github", "42")
	tracker := issueTrackerFunc(func(_ context.Context, repository, issue string) (IssueSnapshot, error) {
		return IssueSnapshot{
			Repository: repository, Number: 42, Title: "safe" + string([]byte{0x1b}) + "[31mRED" + string([]byte{0x1b}) + "[0m\nInjected: yes", State: "open",
			UpdatedAt: time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC),
			Version:   "snapshot-version", URL: "https://github.com/acme/widget/issues/42",
		}, nil
	})
	var out strings.Builder
	if err := WorkCommandWithIssueTracker(context.Background(), []string{"issue", key}, cfg, &out, tracker); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); strings.ContainsRune(got, 0x1b) || strings.Contains(got, "\nInjected: yes") || !strings.Contains(got, "Title: safeREDInjected: yes") {
		t.Fatalf("issue title was not safely sanitized: %q", got)
	}
}

func TestWorkIssueReportsTrackerFailure(t *testing.T) {
	cfg, key := enqueueTestWorkRequest(t, "github", "42")
	tracker := issueTrackerFunc(func(context.Context, string, string) (IssueSnapshot, error) {
		return IssueSnapshot{}, errors.New("GitHub unavailable")
	})
	var out strings.Builder
	err := WorkCommandWithIssueTracker(context.Background(), []string{"issue", key}, cfg, &out, tracker)
	if err == nil || !strings.Contains(err.Error(), "inspect issue: GitHub unavailable") {
		t.Fatalf("issue inspection error = %v, want wrapped tracker failure", err)
	}
	if !strings.Contains(out.String(), "Dedup key: "+key) || strings.Contains(out.String(), "Issue snapshot:") {
		t.Fatalf("tracker failure output should retain queued identity but omit snapshot: %s", out.String())
	}
}

func enqueueTestWorkRequest(t *testing.T, provider, issue string) (Config, string) {
	t.Helper()
	cfg := Config{StateDir: t.TempDir()}
	queue, err := openWorkQueue(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	request := WorkRequest{
		TrackerProvider: provider, IssueID: issue, CodeHostProvider: "github",
		Repository: "acme/widget", DeduplicationKey: "test-request",
	}
	if err := queue.Enqueue(context.Background(), request); err != nil {
		_ = queue.Close()
		t.Fatal(err)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
	return cfg, request.DeduplicationKey
}
