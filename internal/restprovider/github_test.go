package restprovider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const testJobID = "2c9131bb-2cde-4c9d-aaf3-bcc675e482bb"

func TestGitHubPublisherCreatesIdempotentJobPR(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer provider-test-secret" {
			t.Errorf("authorization header = %q", r.Header.Get("Authorization"))
		}
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[]`))
			return
		}
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload["head"] != "factory/job/"+testJobID || payload["base"] != "main" {
			t.Errorf("create payload=%v", payload)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"number":19,"title":"Improve widget","body":"<!-- factory-job:` + testJobID + ` -->","state":"open","html_url":"https://github.com/acme/widget/pull/19","head":{"ref":"factory/job/` + testJobID + `","sha":"commit-sha"},"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"acme/widget"}}}`))
	}))
	defer server.Close()
	publisher := newGitHubPublisher("provider-test-secret", server.Client(), server.URL, func(context.Context, string, string, string) error { return nil })
	out, err := publisher.Publish(context.Background(), PublishRequest{JobID: testJobID, Repository: "acme/widget", Worktree: "/private/worktree", Commit: "commit-sha", Title: "Improve widget", Summary: "Verified", BaseBranch: "main"})
	if err != nil || out.Number != 19 || out.URL != "https://github.com/acme/widget/pull/19" || calls != 2 {
		t.Fatalf("Publish()=(%+v,%v), calls=%d", out, err, calls)
	}
}

func TestGitHubPublisherReconcileIsReadOnlyAndRequiresUniqueExactMatch(t *testing.T) {
	gets, writes := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writes++
			http.Error(w, "writes forbidden", http.StatusMethodNotAllowed)
			return
		}
		gets++
		_, _ = w.Write([]byte(`[{"number":19,"state":"open","body":"<!-- factory-job:` + testJobID + ` -->","html_url":"https://github.com/acme/widget/pull/19","head":{"ref":"factory/job/` + testJobID + `","sha":"commit-sha"},"base":{"ref":"main","repo":{"full_name":"acme/widget"}}}]`))
	}))
	defer server.Close()
	publisher := newGitHubPublisher("secret", server.Client(), server.URL, func(context.Context, string, string, string) error { writes++; return nil })
	request := PublishRequest{JobID: testJobID, Repository: "acme/widget", Worktree: "/private/worktree", Branch: JobBranch(testJobID), Commit: "commit-sha", Title: "task", BaseBranch: "main"}
	outcome, err := publisher.Reconcile(context.Background(), request)
	if err != nil || outcome.Number != 19 || gets != 1 || writes != 0 {
		t.Fatalf("Reconcile=(%+v,%v) GET=%d writes=%d", outcome, err, gets, writes)
	}
}

func TestGitHubPublisherRejectsUnmarkedExistingPullRequest(t *testing.T) {
	request := PublishRequest{JobID: testJobID, Repository: "acme/widget", Commit: "commit-sha", BaseBranch: "main"}
	pr := PullRequest{Number: 19, State: "open", URL: "https://github.com/acme/widget/pull/19"}
	pr.Head.Ref = JobBranch(testJobID)
	pr.Head.SHA = "commit-sha"
	pr.Base.Ref = "main"
	pr.Base.Repo.FullName = "acme/widget"
	if validPullRequest(pr, request, JobBranch(testJobID)) {
		t.Fatal("unmarked pull request treated as Factory-owned")
	}
}

func TestGitHubPublisherRejectsInvalidRepositoryBeforePush(t *testing.T) {
	called := false
	publisher := newGitHubPublisher("secret", http.DefaultClient, "https://api.github.com", func(context.Context, string, string, string) error { called = true; return nil })
	_, err := publisher.Publish(context.Background(), PublishRequest{JobID: testJobID, Repository: "acme/../other", Worktree: "/tmp/worktree", Commit: "sha", Title: "title", BaseBranch: "main"})
	if err == nil || called {
		t.Fatalf("invalid scope: error=%v pushed=%v", err, called)
	}
}

func TestGitHubPublisherReconcileRejectsDuplicateMatchesAndMalformedBodies(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "duplicate", body: `[{"number":19,"state":"open","body":"<!-- factory-job:` + testJobID + ` -->","html_url":"https://github.com/acme/widget/pull/19","head":{"ref":"factory/job/` + testJobID + `","sha":"commit-sha"},"base":{"ref":"main","repo":{"full_name":"acme/widget"}}},{"number":20,"state":"open","body":"<!-- factory-job:` + testJobID + ` -->","html_url":"https://github.com/acme/widget/pull/20","head":{"ref":"factory/job/` + testJobID + `","sha":"commit-sha"},"base":{"ref":"main","repo":{"full_name":"acme/widget"}}}]`},
		{name: "malformed", body: `[{`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(tc.body)) }))
			defer server.Close()
			publisher := newGitHubPublisher("secret", server.Client(), server.URL, func(context.Context, string, string, string) error { return nil })
			_, err := publisher.Reconcile(context.Background(), PublishRequest{JobID: testJobID, Repository: "acme/widget", Worktree: "/private/worktree", Branch: JobBranch(testJobID), Commit: "commit-sha", Title: "task", BaseBranch: "main"})
			if err == nil {
				t.Fatal("ambiguous or malformed provider response was accepted")
			}
		})
	}
}

func TestGitHubPublisherRedactsProviderToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "provider-test-secret detail", http.StatusForbidden)
	}))
	defer server.Close()
	publisher := newGitHubPublisher("provider-test-secret", server.Client(), server.URL, func(context.Context, string, string, string) error { return nil })
	_, err := publisher.Publish(context.Background(), PublishRequest{JobID: testJobID, Repository: "acme/widget", Worktree: "/tmp/worktree", Commit: "sha", Title: "title", BaseBranch: "main"})
	if err == nil || strings.Contains(err.Error(), "provider-test-secret") {
		t.Fatalf("error leaked token: %v", err)
	}
}

func TestGitHubPublisherMarksProviderCreateFailureUncertain(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`[]`))
			return
		}
		http.Error(w, "failure", http.StatusInternalServerError)
	}))
	defer server.Close()
	publisher := newGitHubPublisher("secret", server.Client(), server.URL, func(context.Context, string, string, string) error { return nil })
	_, err := publisher.Publish(context.Background(), PublishRequest{JobID: testJobID, Repository: "acme/widget", Worktree: "/tmp/worktree", Commit: "sha", Title: "title", BaseBranch: "main"})
	if !errors.Is(err, ErrUncertain) {
		t.Fatalf("create error = %v, want uncertain", err)
	}
}

func TestGitHubPublisherUpdatesExistingJobPR(t *testing.T) {
	patched := false
	patches := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`[{"number":19,"state":"open","body":"<!-- factory-job:` + testJobID + ` -->","html_url":"https://github.com/acme/widget/pull/19","head":{"ref":"factory/job/` + testJobID + `","sha":"commit-sha"},"base":{"ref":"main","repo":{"full_name":"acme/widget"}}}]`))
			return
		}
		if r.Method != http.MethodPatch || r.URL.Path != "/repos/acme/widget/pulls/19" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		patches++
		if patches == 1 {
			http.Error(w, "lost response", http.StatusInternalServerError)
			return
		}
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload["title"] != "Updated title" || payload["body"] != "" {
			t.Errorf("update payload=%v", payload)
		}
		patched = true
		_, _ = w.Write([]byte(`{"number":19,"state":"open","body":"<!-- factory-job:` + testJobID + ` -->","html_url":"https://github.com/acme/widget/pull/19","head":{"ref":"factory/job/` + testJobID + `","sha":"commit-sha"},"base":{"ref":"main","repo":{"full_name":"acme/widget"}}}`))
	}))
	defer server.Close()
	publisher := newGitHubPublisher("secret", server.Client(), server.URL, func(context.Context, string, string, string) error { return nil })
	out, err := publisher.Publish(context.Background(), PublishRequest{JobID: testJobID, Repository: "acme/widget", Worktree: "/tmp/worktree", Commit: "commit-sha", Title: "Updated title", Summary: "Updated summary", BaseBranch: "main"})
	if err != nil || out.Number != 19 || !patched || patches != 2 {
		t.Fatalf("out=%+v err=%v patched=%v attempts=%d", out, err, patched, patches)
	}
}

func TestGitPushBranchRefusesForeignRemote(t *testing.T) {
	repo := t.TempDir()
	runTestGit(t, repo, "init", "-q")
	if err := os.WriteFile(filepath.Join(repo, "seed"), []byte("seed"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repo, "add", "seed")
	runTestGit(t, repo, "commit", "-m", "seed")
	runTestGit(t, repo, "remote", "add", "origin", "https://github.com/other/repo.git")
	worktree := filepath.Join(t.TempDir(), "worktree")
	runTestGit(t, repo, "worktree", "add", "--detach", "-q", worktree, "HEAD")
	if err := GitPushBranch(context.Background(), "acme/widget", worktree, "factory/job/"+testJobID); err == nil {
		t.Fatal("foreign remote was pushed")
	}
}

func runTestGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", dir}, args...)...)
	command.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Factory", "GIT_AUTHOR_EMAIL=factory@localhost", "GIT_COMMITTER_NAME=Factory", "GIT_COMMITTER_EMAIL=factory@localhost")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
}

func TestGitHubPublisherReconcilesLostCreateResponseThroughGitHubAPI(t *testing.T) {
	const token = "provider-test-secret"
	var gets, posts, writes int
	var created PullRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get("Accept") != "application/vnd.github+json" || r.Header.Get("X-GitHub-Api-Version") != "2022-11-28" {
			t.Error("GitHub API authentication or version headers missing or incorrect")
		}
		if r.URL.Path != "/repos/acme/widget/pulls" {
			t.Errorf("GitHub API path=%q", r.URL.Path)
		}
		switch r.Method {
		case http.MethodGet:
			gets++
			if r.URL.Query().Get("head") != "acme:"+JobBranch(testJobID) || r.URL.Query().Get("state") != "all" || r.URL.Query().Get("per_page") != "100" || r.URL.Query().Get("page") != "1" {
				t.Errorf("GitHub lookup query=%v", r.URL.Query())
			}
			if gets == 1 {
				_, _ = w.Write([]byte(`[]`))
				return
			}
			if gets == 2 || gets == 3 {
				// The create was accepted, but both automatic and operator lookups
				// initially fail. The response body deliberately contains the token.
				http.Error(w, "temporary provider outage containing "+token, http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode([]PullRequest{created})
		case http.MethodPost:
			posts++
			var payload map[string]string
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode create request: %v", err)
				return
			}
			if payload["title"] != "Implement safely" || payload["head"] != JobBranch(testJobID) || payload["base"] != "main" || payload["body"] != "Verified outcome\n\n<!-- factory-job:"+testJobID+" -->" {
				t.Errorf("GitHub create payload=%v", payload)
			}
			created = PullRequest{Number: 42, State: "open", Body: payload["body"], URL: "https://github.com/acme/widget/pull/42"}
			created.Head.Ref = payload["head"]
			created.Head.SHA = "0123456789abcdef"
			created.Base.Ref = payload["base"]
			created.Base.Repo.FullName = "acme/widget"
			// Model GitHub committing the PR before the response is lost in transit.
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("test server does not support hijacking")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("hijack accepted create response: %v", err)
				return
			}
			_ = conn.Close()
		default:
			writes++
			t.Errorf("unexpected GitHub API write method %s", r.Method)
			http.Error(w, "writes forbidden", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	publisher := newGitHubPublisher(token, server.Client(), server.URL, func(context.Context, string, string, string) error { return nil })
	request := PublishRequest{JobID: testJobID, Repository: "acme/widget", Worktree: "/private/worktree", Commit: "0123456789abcdef", Title: "Implement safely", Summary: "Verified outcome", BaseBranch: "main"}
	if _, err := publisher.Publish(context.Background(), request); !errors.Is(err, ErrUncertain) {
		t.Fatalf("Publish() error=%v, want uncertain after accepted create and failed lookup", err)
	} else if strings.Contains(err.Error(), token) {
		t.Fatalf("uncertain create error leaked provider token: %v", err)
	}
	if _, err := publisher.Reconcile(context.Background(), request); err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("unresolved read-only Reconcile() error=%v, want a sanitized failure", err)
	}
	outcome, err := publisher.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("read-only Reconcile() error=%v", err)
	}
	want := Outcome{Provider: "github", Repository: "acme/widget", Number: 42, URL: "https://github.com/acme/widget/pull/42", Branch: JobBranch(testJobID), Commit: "0123456789abcdef", State: "open"}
	if outcome != want {
		t.Fatalf("Reconcile() outcome=%+v, want %+v", outcome, want)
	}
	if gets != 4 || posts != 1 || writes != 0 {
		t.Fatalf("GitHub API calls: GET=%d POST=%d other writes=%d; want 4, 1, 0", gets, posts, writes)
	}
}

func TestGitHubPublisherReconcileRejectsMismatchedGitHubResponsesReadOnly(t *testing.T) {
	valid := `{"number":42,"state":"open","body":"<!-- factory-job:` + testJobID + ` -->","html_url":"https://github.com/acme/widget/pull/42","head":{"ref":"factory/job/` + testJobID + `","sha":"0123456789abcdef"},"base":{"ref":"main","repo":{"full_name":"acme/widget"}}}`
	for _, tc := range []struct {
		name         string
		status       int
		body         string
		wantMismatch string
		wantErr      string
	}{
		{name: "permission denied", status: http.StatusForbidden, body: `{"message":"secret provider detail"}`, wantErr: "provider reconciliation is unavailable"},
		{name: "rate limited", status: http.StatusTooManyRequests, body: `{"message":"secret provider detail"}`, wantErr: "provider reconciliation is unavailable"},
		{name: "malformed response", status: http.StatusOK, body: `[{`, wantErr: "provider reconciliation is unavailable"},
		{name: "repository mismatch", status: http.StatusOK, body: strings.Replace(valid, `"full_name":"acme/widget"`, `"full_name":"other/widget"`, 1), wantMismatch: `"full_name":"other/widget"`, wantErr: "provider reconciliation is unavailable"},
		{name: "branch mismatch", status: http.StatusOK, body: strings.Replace(valid, `"ref":"factory/job/`+testJobID+`"`, `"ref":"factory/job/other"`, 1), wantMismatch: `"ref":"factory/job/other"`, wantErr: "provider reconciliation is unavailable"},
	} {
		if tc.wantMismatch != "" && (tc.body == valid || !strings.Contains(tc.body, tc.wantMismatch)) {
			t.Fatalf("%s fixture does not contain its intended mismatch %q", tc.name, tc.wantMismatch)
		}
		t.Run(tc.name, func(t *testing.T) {
			var writes int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes++
					http.Error(w, "writes forbidden", http.StatusMethodNotAllowed)
					return
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			publisher := newGitHubPublisher("provider-test-secret", server.Client(), server.URL, func(context.Context, string, string, string) error {
				writes++
				return nil
			})
			request := PublishRequest{JobID: testJobID, Repository: "acme/widget", Worktree: "/private/worktree", Branch: JobBranch(testJobID), Commit: "0123456789abcdef", Title: "task", BaseBranch: "main"}
			_, err := publisher.Reconcile(context.Background(), request)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) || strings.Contains(err.Error(), "provider-test-secret") || strings.Contains(err.Error(), "secret provider detail") {
				t.Fatalf("Reconcile() error=%v, want sanitized %q", err, tc.wantErr)
			}
			if writes != 0 {
				t.Fatalf("read-only reconciliation performed %d provider writes", writes)
			}
		})
	}
}

func TestGitHubPublisherReconcilesAfterAmbiguousCreate(t *testing.T) {
	gets, posts := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			gets++
			if gets == 1 {
				_, _ = w.Write([]byte(`[]`))
				return
			}
			_, _ = w.Write([]byte(`[{"number":19,"state":"open","body":"<!-- factory-job:` + testJobID + ` -->","html_url":"https://github.com/acme/widget/pull/19","head":{"ref":"factory/job/` + testJobID + `","sha":"commit-sha"},"base":{"ref":"main","repo":{"full_name":"acme/widget"}}}]`))
			return
		}
		posts++
		http.Error(w, "ambiguous failure", http.StatusInternalServerError)
	}))
	defer server.Close()
	publisher := newGitHubPublisher("secret", server.Client(), server.URL, func(context.Context, string, string, string) error { return nil })
	out, err := publisher.Publish(context.Background(), PublishRequest{JobID: testJobID, Repository: "acme/widget", Worktree: "/tmp/worktree", Commit: "commit-sha", Title: "task", BaseBranch: "main"})
	if err != nil || out.Number != 19 || posts != 1 || gets != 2 {
		t.Fatalf("out=%+v err=%v GET=%d POST=%d", out, err, gets, posts)
	}
}
