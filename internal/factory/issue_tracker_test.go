package factory

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestGitHubIssueTrackerGetIssueMapsOpenAndClosedIssues(t *testing.T) {
	tests := []struct {
		number, title, body, state string
		wantNumber                 int
	}{
		{"7", "Open issue", "Repro steps", "open", 7},
		{"8", "Closed issue", "Resolved", "closed", 8},
	}
	calls := 0
	tracker := newGitHubIssueTracker(func(_ context.Context, args ...string) ([]byte, error) {
		calls++
		wantArgs := "api --method GET repos/acme/widget/issues/" + tests[calls-1].number
		if strings.Join(args, " ") != wantArgs {
			t.Fatalf("gh args = %q, want %q", args, wantArgs)
		}
		test := tests[calls-1]
		return []byte(`{"id":101,"number":` + test.number + `,"title":"` + test.title + `","body":"` + test.body + `","state":"` + test.state + `","html_url":"https://github.com/acme/widget/issues/` + test.number + `","updated_at":"2025-01-02T03:04:05Z"}`), nil
	})

	for _, test := range tests {
		snapshot, err := tracker.GetIssue(context.Background(), "acme/widget", test.number)
		if err != nil {
			t.Fatalf("GetIssue() error = %v", err)
		}
		if snapshot.Repository != "acme/widget" || snapshot.Number != test.wantNumber || snapshot.State != test.state || snapshot.Title != test.title || snapshot.Body != test.body {
			t.Errorf("unexpected identity/content/state: %+v", snapshot)
		}
		if snapshot.Title == "" || snapshot.URL == "" || snapshot.UpdatedAt.IsZero() || len(snapshot.Version) != 64 {
			t.Errorf("snapshot omitted issue content or version data: %+v", snapshot)
		}
	}
}

func TestGitHubIssueTrackerVersionChangesWhenIssueUpdates(t *testing.T) {
	response := `{"id":101,"number":7,"title":"Original","body":"Body","state":"open","html_url":"https://github.com/acme/widget/issues/7","updated_at":"2025-01-02T03:04:05Z"}`
	tracker := newGitHubIssueTracker(func(context.Context, ...string) ([]byte, error) {
		return []byte(response), nil
	})
	first, err := tracker.GetIssue(context.Background(), "acme/widget", "7")
	if err != nil {
		t.Fatal(err)
	}
	response = `{"id":101,"number":7,"title":"Changed title","body":"Body","state":"open","html_url":"https://github.com/acme/widget/issues/7","updated_at":"2025-01-03T03:04:05Z"}`
	second, err := tracker.GetIssue(context.Background(), "acme/widget", "7")
	if err != nil {
		t.Fatal(err)
	}
	if first.Version == second.Version || !second.UpdatedAt.After(first.UpdatedAt) || first.Title == second.Title {
		t.Fatalf("issue update was not reflected in snapshot version: first=%+v second=%+v", first, second)
	}
}

func TestGitHubIssueTrackerRejectsInvalidRepositoryAndIssueBeforeCommand(t *testing.T) {
	called := false
	tracker := newGitHubIssueTracker(func(context.Context, ...string) ([]byte, error) {
		called = true
		return nil, nil
	})
	for _, test := range []struct{ repository, issue string }{
		{"", "1"}, {"owner", "1"}, {"owner/repo/extra", "1"}, {"owner/repo?x", "1"},
		{"owner/repo", "0"}, {"owner/repo", "-1"}, {"owner/repo", "1x"}, {"owner/repo", "999999999999999999999999"},
	} {
		if _, err := tracker.GetIssue(context.Background(), test.repository, test.issue); err == nil {
			t.Errorf("GetIssue(%q, %q) accepted invalid reference", test.repository, test.issue)
		}
	}
	if called {
		t.Fatal("invalid references invoked the gh command")
	}
}

func TestGitHubIssueTrackerReportsAPIAndJSONFailures(t *testing.T) {
	tracker := newGitHubIssueTracker(func(context.Context, ...string) ([]byte, error) {
		return nil, errors.New("gh api failed: repository not found")
	})
	if _, err := tracker.GetIssue(context.Background(), "acme/widget", "7"); err == nil || !strings.Contains(err.Error(), "repository not found") {
		t.Fatalf("GetIssue() error = %v, want API failure detail", err)
	}
	tracker = newGitHubIssueTracker(func(context.Context, ...string) ([]byte, error) {
		return []byte("not json"), nil
	})
	if _, err := tracker.GetIssue(context.Background(), "acme/widget", "7"); err == nil || !strings.Contains(err.Error(), "invalid GitHub issue JSON") {
		t.Fatalf("GetIssue() error = %v, want invalid response error", err)
	}
}

func TestGitHubIssueTrackerIdentifiesPullRequests(t *testing.T) {
	tracker := newGitHubIssueTracker(func(context.Context, ...string) ([]byte, error) {
		return []byte(`{"id":101,"number":7,"title":"PR","body":"","state":"open","html_url":"https://github.com/acme/widget/pull/7","updated_at":"2025-01-02T03:04:05Z","pull_request":{"url":"https://api.github.com/repos/acme/widget/pulls/7"}}`), nil
	})
	_, err := tracker.GetIssue(context.Background(), "acme/widget", "7")
	if !errors.Is(err, ErrNotIssue) {
		t.Fatalf("GetIssue() error = %v, want ErrNotIssue", err)
	}
}

func TestGitHubIssueTrackerPreservesUpdateTimestampPrecision(t *testing.T) {
	updated := time.Date(2025, 1, 2, 3, 4, 5, 123000000, time.UTC)
	tracker := newGitHubIssueTracker(func(context.Context, ...string) ([]byte, error) {
		return []byte(`{"id":101,"number":7,"title":"Issue","body":"","state":"open","html_url":"https://github.com/acme/widget/issues/7","updated_at":"2025-01-02T03:04:05.123Z"}`), nil
	})
	snapshot, err := tracker.GetIssue(context.Background(), "acme/widget", "7")
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.UpdatedAt.Equal(updated) {
		t.Fatalf("UpdatedAt = %s, want %s", snapshot.UpdatedAt, updated)
	}
}
