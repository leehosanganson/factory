package poller

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestGitHubCandidatesAreIssuesWithExactLabel(t *testing.T) {
	calls := 0
	client := newGitHub("scoped-token", func(_ context.Context, token, endpoint string, extra ...string) ([]byte, error) {
		calls++
		if token != "scoped-token" || endpoint != "repos/acme/repo/issues" || len(extra) != 8 || extra[3] != "labels=needs work" || extra[7] != "page=2" {
			t.Fatalf("credential/query not scoped: token=%q endpoint=%q args=%v", token, endpoint, extra)
		}
		return []byte(`[{"id":1,"number":3,"title":"Issue","body":"body","state":"open","html_url":"https://github.com/acme/repo/issues/3","updated_at":"2026-01-01T00:00:00Z","labels":[{"name":"needs work"}]},{"id":2,"number":4,"title":"PR","body":"","state":"open","html_url":"https://github.com/acme/repo/issues/4","updated_at":"2026-01-01T00:00:00Z","pull_request":{"url":"x"},"labels":[{"name":"needs work"}]},{"id":3,"number":5,"title":"Other","body":"","state":"open","html_url":"https://github.com/acme/repo/issues/5","updated_at":"2026-01-01T00:00:00Z","labels":[{"name":"Needs work"}]}]`), nil
	})
	items, err := client.Candidates(context.Background(), "acme/repo", "needs work", 10, 2)
	if err != nil || calls != 1 || len(items.Issues) != 1 || items.Issues[0].Number != 3 || items.HasNext {
		t.Fatalf("items=%+v calls=%d err=%v", items, calls, err)
	}
}

func TestGitHubRefetchRejectsPRAndRemoveUsesScopedExactLabel(t *testing.T) {
	calls := 0
	client := newGitHub("token", func(_ context.Context, _ string, endpoint string, extra ...string) ([]byte, error) {
		calls++
		if calls == 1 {
			return []byte(`{"id":1,"number":2,"title":"PR","state":"open","html_url":"https://github.com/acme/repo/issues/2","updated_at":"2026-01-01T00:00:00Z","pull_request":{"url":"x"}}`), nil
		}
		if endpoint != "repos/acme/repo/issues/2/labels/needs%20work" || len(extra) != 2 || extra[0] != "--method" || extra[1] != "DELETE" {
			t.Fatalf("unexpected label DELETE request: endpoint=%q args=%v", endpoint, extra)
		}
		return nil, nil
	})
	if _, _, isPR, err := client.GetIssue(context.Background(), "acme/repo", 2); err != nil || !isPR {
		t.Fatalf("PR refetch=(%t,%v)", isPR, err)
	}
	if err := client.RemoveLabel(context.Background(), "acme/repo", 2, "needs work"); err != nil {
		t.Fatal(err)
	}
}

func TestGitHubErrorsDoNotExposeCredentialOrProviderDetails(t *testing.T) {
	client := newGitHub("private-token", func(context.Context, string, string, ...string) ([]byte, error) {
		return nil, errors.New("private-token provider internals")
	})
	if _, err := client.Candidates(context.Background(), "acme/repo", "ready", 10, 1); err == nil || strings.Contains(err.Error(), "private-token") || strings.Contains(err.Error(), "internals") {
		t.Fatalf("provider detail leaked: %v", err)
	}
}

func TestGitHubCandidateResponseRejectsPullRequestNullMarker(t *testing.T) {
	client := newGitHub("token", func(context.Context, string, string, ...string) ([]byte, error) {
		return []byte(`[{"id":1,"number":2,"title":"Issue","state":"open","html_url":"https://github.com/acme/repo/issues/2","updated_at":"2026-01-01T00:00:00Z","pull_request":null,"labels":[{"name":"ready"}]}]`), nil
	})
	items, err := client.Candidates(context.Background(), "acme/repo", "ready", 10, 1)
	if err != nil || len(items.Issues) != 1 || items.HasNext {
		t.Fatalf("candidate with null PR marker rejected: items=%v err=%v", items, err)
	}
}

func TestCleanEnvDropsAmbientGitHubCredentials(t *testing.T) {
	got := cleanEnv([]string{"PATH=/bin", "GH_TOKEN=ambient", "GH_CONFIG_DIR=/home/user/.config/gh", "GITHUB_TOKEN=other", "HOME=/home/user"})
	joined := strings.Join(got, "\n")
	if strings.Contains(joined, "GH_TOKEN") || strings.Contains(joined, "GH_CONFIG_DIR") || strings.Contains(joined, "GITHUB_TOKEN") || !strings.Contains(joined, "PATH=/bin") {
		t.Fatalf("unexpected child environment %q", joined)
	}
}
