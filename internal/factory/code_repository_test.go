package factory

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestGitHubCodeRepositoryGetPullRequestMapsOpenAndClosedSnapshots(t *testing.T) {
	for _, state := range []string{"open", "closed"} {
		t.Run(state, func(t *testing.T) {
			called := false
			host := newGitHubCodeRepository(func(_ context.Context, args ...string) ([]byte, error) {
				called = true
				if got, want := strings.Join(args, " "), "api --method GET repos/acme/widget/pulls/17"; got != want {
					t.Fatalf("gh args = %q, want only read request %q", got, want)
				}
				return []byte(`{"number":17,"title":"Improve widget","body":"Details","state":"` + state + `","html_url":"https://github.com/acme/widget/pull/17","updated_at":"2025-02-03T04:05:06.123Z","head":{"ref":"feature/widget","sha":"head-sha"},"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"acme/widget"}}}`), nil
			})

			snapshot, err := host.GetPullRequest(context.Background(), "acme/widget", 17)
			if err != nil {
				t.Fatalf("GetPullRequest() error = %v", err)
			}
			if !called || snapshot.Repository != "acme/widget" || snapshot.Number != 17 || snapshot.Title != "Improve widget" || snapshot.Body != "Details" || snapshot.State != state || snapshot.URL != "https://github.com/acme/widget/pull/17" || snapshot.HeadBranch != "feature/widget" || snapshot.BaseBranch != "main" || snapshot.HeadCommit != "head-sha" || snapshot.BaseCommit != "base-sha" {
				t.Fatalf("unexpected snapshot or command state: called=%v snapshot=%+v", called, snapshot)
			}
			if snapshot.UpdatedAt.IsZero() || len(snapshot.Version) != 64 {
				t.Fatalf("snapshot omitted update/version fields: %+v", snapshot)
			}
			wantUpdated := time.Date(2025, 2, 3, 4, 5, 6, 123000000, time.UTC)
			if !snapshot.UpdatedAt.Equal(wantUpdated) {
				t.Errorf("UpdatedAt = %s, want %s", snapshot.UpdatedAt, wantUpdated)
			}
		})
	}
}

func TestGitHubCodeRepositoryVersionChangesWhenPullRequestUpdates(t *testing.T) {
	response := `{"number":17,"title":"Original","body":"Details","state":"open","html_url":"https://github.com/acme/widget/pull/17","updated_at":"2025-02-03T04:05:06Z","head":{"ref":"feature/widget","sha":"head-sha"},"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"acme/widget"}}}`
	host := newGitHubCodeRepository(func(context.Context, ...string) ([]byte, error) { return []byte(response), nil })
	first, err := host.GetPullRequest(context.Background(), "acme/widget", 17)
	if err != nil {
		t.Fatal(err)
	}
	response = `{"number":17,"title":"Updated title","body":"Details","state":"open","html_url":"https://github.com/acme/widget/pull/17","updated_at":"2025-02-04T04:05:06Z","head":{"ref":"feature/widget","sha":"new-head-sha"},"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"acme/widget"}}}`
	second, err := host.GetPullRequest(context.Background(), "acme/widget", 17)
	if err != nil {
		t.Fatal(err)
	}
	if first.Version == second.Version || !second.UpdatedAt.After(first.UpdatedAt) || first.Title == second.Title || first.HeadCommit == second.HeadCommit {
		t.Fatalf("PR update was not reflected in snapshot version: first=%+v second=%+v", first, second)
	}
}

func TestGitHubCodeRepositoryRejectsInvalidReferencesBeforeCommand(t *testing.T) {
	called := false
	host := newGitHubCodeRepository(func(context.Context, ...string) ([]byte, error) {
		called = true
		return nil, nil
	})
	for _, test := range []struct {
		repository string
		number     int
	}{
		{"", 1}, {"owner", 1}, {"owner/repo/extra", 1}, {"owner/repo?x", 1}, {"owner/..", 1},
		{"owner/repo", 0}, {"owner/repo", -1},
	} {
		if _, err := host.GetPullRequest(context.Background(), test.repository, test.number); err == nil {
			t.Errorf("GetPullRequest(%q, %d) accepted invalid reference", test.repository, test.number)
		}
	}
	if called {
		t.Fatal("invalid references invoked the gh command")
	}
}

func TestGitHubCodeRepositoryReportsAPIAndJSONErrors(t *testing.T) {
	host := newGitHubCodeRepository(func(context.Context, ...string) ([]byte, error) { return nil, errors.New("repository not found") })
	if _, err := host.GetPullRequest(context.Background(), "acme/widget", 17); err == nil || !strings.Contains(err.Error(), "repository not found") {
		t.Fatalf("GetPullRequest() error = %v, want API failure detail", err)
	}
	host = newGitHubCodeRepository(func(context.Context, ...string) ([]byte, error) { return []byte("not json"), nil })
	if _, err := host.GetPullRequest(context.Background(), "acme/widget", 17); err == nil || !strings.Contains(err.Error(), "invalid GitHub pull request JSON") {
		t.Fatalf("GetPullRequest() error = %v, want invalid response error", err)
	}
}

func TestGitHubCodeRepositoryRejectsMismatchedIdentityAndURL(t *testing.T) {
	base := `"number":17,"title":"PR","body":"","state":"open","html_url":"https://github.com/acme/widget/pull/17","updated_at":"2025-02-03T04:05:06Z","head":{"ref":"feature","sha":"head"},"base":{"ref":"main","sha":"base","repo":{"full_name":"acme/widget"}}`
	for _, test := range []struct {
		name, response, wantError string
	}{
		{"number", strings.Replace(base, `"number":17`, `"number":18`, 1), "unexpected identity"},
		{"repository", strings.Replace(base, `"full_name":"acme/widget"`, `"full_name":"other/repo"`, 1), "unexpected identity"},
		{"URL", strings.Replace(base, "/pull/17", "/pull/18", 1), "unexpected URL"},
		{"scheme", strings.Replace(base, "https://", "http://", 1), "unexpected URL"},
		{"host", strings.Replace(base, "github.com", "evil.example", 1), "unexpected URL"},
	} {
		t.Run(test.name, func(t *testing.T) {
			host := newGitHubCodeRepository(func(context.Context, ...string) ([]byte, error) { return []byte("{" + test.response + "}"), nil })
			if _, err := host.GetPullRequest(context.Background(), "acme/widget", 17); err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("GetPullRequest() error = %v, want %q", err, test.wantError)
			}
		})
	}
}
