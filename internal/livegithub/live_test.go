package livegithub

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/leehosanganson/factory/internal/restprovider"
)

func TestLiveGitHubPublishReconcile(t *testing.T) {
	if os.Getenv(EnvOptIn) != "true" {
		t.Skip("live GitHub test requires explicit opt-in")
	}
	cfg, err := LoadProviderConfig(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	jobID, err := workflowJobID(os.Getenv("GITHUB_RUN_ID"), os.Getenv("GITHUB_RUN_ATTEMPT"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cleanupClient := safeHTTPClient(20 * time.Second)
	commit := ""
	worktree := ""
	cleanupToken := cfg.Token
	t.Cleanup(func() {
		if commit == "" {
			return
		}
		cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if cleanupToken == "" {
			t.Errorf("bounded cleanup has no pre-minted sandbox-scoped token")
			return
		}
		if err := cleanupOwnedPullRequest(cleanupCtx, cleanupClient, cleanupToken, cfg.Repository, cfg.ApprovedOrganization, worktree, jobID, commit, "https://api.github.com"); err != nil {
			t.Errorf("bounded cleanup of only this run's PR/branch failed: %v", err)
		}
	})
	if err := verifyApprovedOrganization(ctx, safeHTTPClient(20*time.Second), cfg.Token, cfg.Repository, cfg.ApprovedOrganization, "https://api.github.com"); err != nil {
		t.Fatalf("sandbox repository organization identity could not be confirmed: %v", err)
	}
	worktree = prepareWorktree(t, cfg.Repository, cfg.Token)
	request := restprovider.PublishRequest{
		JobID: jobID, Repository: cfg.Repository, Worktree: worktree,
		Commit:     gitTest(t, ctx, worktree, "rev-parse", "HEAD"),
		Title:      "Factory live provider verification " + jobID,
		Summary:    "Disposable, uniquely identified live GitHub App provider test. This PR is closed by bounded cleanup and must never be merged.",
		BaseBranch: "main",
	}
	commit = request.Commit
	if err := writeWorkflowCleanupState(jobID, commit); err != nil {
		t.Fatalf("cannot arrange always-run cleanup: %v", err)
	}
	client := safeHTTPClient(20 * time.Second)
	responseLoss := newCreateResponseLossTransport(newDirectGitHubTransport(), cfg.Token, request)
	client.Transport = responseLoss
	publisher := restprovider.NewGitHubPublisherWithTokenPush(cfg.Token, client, restprovider.GitPushBranchWithToken)
	outcome, err := publisher.Publish(ctx, request)
	if err != nil {
		t.Fatalf("publish/reconcile after simulated response loss failed (no blind retry was attempted): %v", err)
	}
	if responseLoss.forwarded != 1 {
		t.Fatalf("PR create requests forwarded to GitHub = %d, want exactly one", responseLoss.forwarded)
	}
	if outcome.Repository != cfg.Repository || outcome.Branch != restprovider.JobBranch(jobID) || outcome.Commit != request.Commit || outcome.State != "open" || outcome.Number <= 0 {
		t.Fatalf("publisher returned an unexpected PR identity: %+v", outcome)
	}
	confirmed, err := publisher.Reconcile(ctx, request)
	if err != nil || confirmed != outcome {
		t.Fatalf("read-only reconciliation=(%+v,%v), want published outcome %+v", confirmed, err, outcome)
	}
	if responseLoss.forwarded != 1 {
		t.Fatalf("reconciliation caused another PR create: forwarded POST count=%d", responseLoss.forwarded)
	}
	t.Logf("confirmed unique sandbox PR %s#%d through publish and read-only reconcile", cfg.Repository, outcome.Number)
}

func TestVerifyApprovedOrganizationRequiresExactGitHubOrganizationIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, response string
		wantErr        bool
	}{
		{"approved exact organization", `{"full_name":"` + sandboxRepo + `","owner":{"login":"` + approvedOrganization + `","type":"Organization"}}`, false},
		{"user account", `{"full_name":"` + sandboxRepo + `","owner":{"login":"` + approvedOrganization + `","type":"User"}}`, true},
		{"different organization", `{"full_name":"` + sandboxRepo + `","owner":{"login":"other-org","type":"Organization"}}`, true},
		{"different repository", `{"full_name":"other-org/other-repo","owner":{"login":"` + approvedOrganization + `","type":"Organization"}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/repos/"+sandboxRepo {
					t.Errorf("unexpected organization verification request: %s %s", r.Method, r.URL.Path)
				}
				_, _ = io.WriteString(w, tc.response)
			}))
			defer server.Close()
			err := verifyApprovedOrganization(context.Background(), server.Client(), "token", sandboxRepo, approvedOrganization, server.URL)
			if (err != nil) != tc.wantErr {
				t.Fatalf("verification error=%v wantErr=%t", err, tc.wantErr)
			}
		})
	}
}

func TestCleanupOwnedPullRequestOnlyMutatesExactRunIdentity(t *testing.T) {
	jobID, err := workflowJobID("42", "1")
	if err != nil {
		t.Fatal(err)
	}
	worktree, bare := testCleanupWorktree(t, "sandbox-owner/test-factory-live-sandbox", jobID)
	commit := gitTest(t, context.Background(), worktree, "rev-parse", "HEAD")
	var closed bool
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/sandbox-owner/test-factory-live-sandbox":
			_, _ = io.WriteString(w, `{"full_name":"sandbox-owner/test-factory-live-sandbox","owner":{"login":"`+approvedOrganization+`","type":"Organization"}}`)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/sandbox-owner/test-factory-live-sandbox/pulls":
			_, _ = io.WriteString(w, `[{"number":1,"state":"open","body":"<!-- factory-job:`+jobID+` -->","html_url":"https://github.com/sandbox-owner/test-factory-live-sandbox/pull/1","head":{"ref":"factory/job/`+jobID+`","sha":"`+commit+`"},"base":{"ref":"main","repo":{"full_name":"sandbox-owner/test-factory-live-sandbox"}}}]`)
		case r.Method == http.MethodPatch && r.URL.Path == "/repos/sandbox-owner/test-factory-live-sandbox/pulls/1":
			closed = true
			_, _ = io.WriteString(w, `{"number":1,"state":"closed","body":"<!-- factory-job:`+jobID+` -->","html_url":"https://github.com/sandbox-owner/test-factory-live-sandbox/pull/1","head":{"ref":"factory/job/`+jobID+`","sha":"`+commit+`"},"base":{"ref":"main","repo":{"full_name":"sandbox-owner/test-factory-live-sandbox"}}}`)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/sandbox-owner/test-factory-live-sandbox/git/ref/heads/factory/job/"+jobID:
			_, _ = io.WriteString(w, `{"object":{"sha":"`+commit+`"}}`)
		case r.Method == http.MethodDelete:
			t.Errorf("cleanup used unconditional REST branch deletion: %s %s", r.Method, r.URL.Path)
		default:
			t.Errorf("unexpected cleanup request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected", http.StatusBadRequest)
		}
	})
	defer server.Close()
	client := &http.Client{Timeout: time.Second}
	if err := cleanupOwnedPullRequest(context.Background(), client, "token", "sandbox-owner/test-factory-live-sandbox", approvedOrganization, worktree, jobID, commit, server.URL); err != nil {
		t.Fatal(err)
	}
	if !closed {
		t.Fatalf("cleanup did not close exact owned pull request: closed=%v", closed)
	}
	command := exec.Command("git", "--git-dir", bare, "show-ref", "--verify", "refs/heads/"+restprovider.JobBranch(jobID))
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("CAS cleanup left branch in bare remote: %s", output)
	}
}

func testCleanupWorktree(t *testing.T, repository, jobID string) (string, string) {
	t.Helper()
	root := t.TempDir()
	bare := filepath.Join(root, "remote.git")
	seed := filepath.Join(root, "seed")
	if output, err := exec.Command("git", "init", "--bare", "-q", bare).CombinedOutput(); err != nil {
		t.Fatalf("initialize test bare remote: %v: %s", err, output)
	}
	if output, err := exec.Command("git", "init", "-q", seed).CombinedOutput(); err != nil {
		t.Fatalf("initialize test seed: %v: %s", err, output)
	}
	if err := os.WriteFile(filepath.Join(seed, "file"), []byte("owned commit"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, context.Background(), seed, "add", "file")
	command := exec.Command("git", "-C", seed, "-c", "user.name=Factory", "-c", "user.email=factory@localhost", "commit", "-qm", "owned commit")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("create test commit: %v: %s", err, output)
	}
	commit := gitTest(t, context.Background(), seed, "rev-parse", "HEAD")
	branch := restprovider.JobBranch(jobID)
	gitTest(t, context.Background(), seed, "push", bare, "HEAD:refs/heads/"+branch)
	worktree := filepath.Join(root, "worktree")
	gitTest(t, context.Background(), seed, "worktree", "add", "--detach", "-q", worktree, commit)
	gitTest(t, context.Background(), worktree, "remote", "add", "origin", "git@github.com:"+repository+".git")
	sshPath := filepath.Join(root, "ssh")
	if err := os.WriteFile(sshPath, []byte("#!/bin/sh\nexec git-receive-pack "+bare+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	gitTest(t, context.Background(), worktree, "config", "core.sshCommand", sshPath)
	return worktree, bare
}

func TestCleanupRefusesMultiplePRsWithSameOwnedIdentity(t *testing.T) {
	jobID, err := workflowJobID("44", "1")
	if err != nil {
		t.Fatal(err)
	}
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, `[{"number":1,"state":"open","body":"<!-- factory-job:`+jobID+` -->","html_url":"https://github.com/sandbox-owner/test-factory-live-sandbox/pull/1","head":{"ref":"factory/job/`+jobID+`","sha":"`+strings.Repeat("a", 40)+`"},"base":{"ref":"main","repo":{"full_name":"sandbox-owner/test-factory-live-sandbox"}}},{"number":2,"state":"open","body":"<!-- factory-job:`+jobID+` -->","html_url":"https://github.com/sandbox-owner/test-factory-live-sandbox/pull/2","head":{"ref":"factory/job/`+jobID+`","sha":"`+strings.Repeat("a", 40)+`"},"base":{"ref":"main","repo":{"full_name":"sandbox-owner/test-factory-live-sandbox"}}}]`)
			return
		}
		t.Errorf("cleanup wrote after finding duplicate owned PRs: %s %s", r.Method, r.URL.Path)
	})
	defer server.Close()
	if err := cleanupOwnedPullRequest(context.Background(), server.Client(), "token", sandboxRepo, approvedOrganization, t.TempDir(), jobID, strings.Repeat("a", 40), server.URL); err == nil {
		t.Fatal("cleanup accepted multiple PRs with the same owned identity")
	}
}

func TestCleanupRefusesUnmarkedPRUsingTheRunBranch(t *testing.T) {
	jobID, err := workflowJobID("47", "1")
	if err != nil {
		t.Fatal(err)
	}
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, `[{"number":9,"state":"open","body":"unmarked","html_url":"https://github.com/sandbox-owner/test-factory-live-sandbox/pull/9","head":{"ref":"factory/job/`+jobID+`","sha":"`+strings.Repeat("a", 40)+`"},"base":{"ref":"main","repo":{"full_name":"sandbox-owner/test-factory-live-sandbox"}}}]`)
			return
		}
		t.Errorf("cleanup mutated unmarked PR: %s %s", r.Method, r.URL.Path)
	})
	defer server.Close()
	if err := cleanupOwnedPullRequest(context.Background(), server.Client(), "token", sandboxRepo, approvedOrganization, t.TempDir(), jobID, strings.Repeat("a", 40), server.URL); err == nil {
		t.Fatal("cleanup accepted an unmarked PR sharing this run's branch")
	}
}

func TestCleanupRefusesPullRequestBranchIdentityMismatch(t *testing.T) {
	jobID, err := workflowJobID("45", "1")
	if err != nil {
		t.Fatal(err)
	}
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, `[{"number":7,"state":"open","body":"<!-- factory-job:`+jobID+` -->","html_url":"https://github.com/sandbox-owner/test-factory-live-sandbox/pull/7","head":{"ref":"factory/job/`+jobID+`","sha":"`+strings.Repeat("b", 40)+`"},"base":{"ref":"main","repo":{"full_name":"sandbox-owner/test-factory-live-sandbox"}}}]`)
			return
		}
		t.Errorf("cleanup mutated mismatched PR: %s %s", r.Method, r.URL.Path)
	})
	defer server.Close()
	if err := cleanupOwnedPullRequest(context.Background(), server.Client(), "token", sandboxRepo, approvedOrganization, t.TempDir(), jobID, strings.Repeat("a", 40), server.URL); err == nil {
		t.Fatal("cleanup accepted a PR whose branch was tied to a different commit")
	}
}

func TestCleanupRefusesMalformedCommitBeforeNetwork(t *testing.T) {
	called := false
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		called = true
		return nil, errors.New("must not issue request")
	})}
	jobID, err := workflowJobID("46", "1")
	if err != nil {
		t.Fatal(err)
	}
	if err := cleanupOwnedPullRequest(context.Background(), client, "token", sandboxRepo, approvedOrganization, t.TempDir(), jobID, "not-a-git-sha", "https://api.github.com"); err == nil || called {
		t.Fatalf("cleanup accepted malformed commit or issued request: err=%v called=%v", err, called)
	}
}

func TestCleanupRefusesOrganizationMismatchBeforeMutations(t *testing.T) {
	jobID, err := workflowJobID("48", "1")
	if err != nil {
		t.Fatal(err)
	}
	var mutations int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/repos/"+sandboxRepo {
			mutations++
			t.Errorf("organization mismatch was followed by request: %s %s", r.Method, r.URL.Path)
			return
		}
		_, _ = io.WriteString(w, `{"full_name":"`+sandboxRepo+`","owner":{"login":"another-org","type":"Organization"}}`)
	}))
	defer server.Close()
	if err := cleanupOwnedPullRequest(context.Background(), server.Client(), "token", sandboxRepo, approvedOrganization, t.TempDir(), jobID, strings.Repeat("a", 40), server.URL); err == nil || mutations != 0 {
		t.Fatalf("cleanup accepted organization mismatch or continued to mutations: err=%v mutations=%d", err, mutations)
	}
}

func TestCleanupRefusesBranchWithUnexpectedCommit(t *testing.T) {
	jobID, err := workflowJobID("42", "1")
	if err != nil {
		t.Fatal(err)
	}
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, `{"object":{"sha":"`+strings.Repeat("b", 40)+`"}}`)
			return
		}
		t.Errorf("cleanup wrote after branch identity mismatch: %s %s", r.Method, r.URL.Path)
	})
	defer server.Close()
	if err := deleteOwnedBranch(context.Background(), server.Client(), "token", "sandbox-owner/test-factory-live-sandbox", t.TempDir(), restprovider.JobBranch(jobID), strings.Repeat("a", 40), server.URL); err == nil {
		t.Fatal("cleanup accepted a branch whose commit did not match this run")
	}
}

func TestCleanupFromWorkflowEnvironment(t *testing.T) {
	if os.Getenv("FACTORY_LIVE_GITHUB_CLEANUP") != "true" {
		t.Skip("workflow cleanup step is not enabled")
	}
	if os.Getenv(EnvOptIn) != "true" {
		t.Fatal("workflow cleanup requires explicit live-test opt-in")
	}
	commit := os.Getenv("FACTORY_LIVE_GITHUB_COMMIT")
	if commit == "" {
		t.Skip("no remote commit was recorded; nothing is eligible for cleanup")
	}
	cfg, err := LoadProviderConfig(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	jobID, err := workflowJobID(os.Getenv("GITHUB_RUN_ID"), os.Getenv("GITHUB_RUN_ATTEMPT"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if !isGitCommitID(commit) {
		t.Fatal("workflow cleanup commit identity is malformed")
	}
	worktree := t.TempDir()
	gitTest(t, ctx, worktree, "init", "-q")
	gitTest(t, ctx, worktree, "remote", "add", "origin", "https://github.com/"+cfg.Repository+".git")
	if err := cleanupOwnedPullRequest(ctx, safeHTTPClient(20*time.Second), cfg.Token, cfg.Repository, cfg.ApprovedOrganization, worktree, jobID, commit, "https://api.github.com"); err != nil {
		t.Fatal(err)
	}
}

func safeHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Transport: newDirectGitHubTransport(), Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func newTestServer(handler http.HandlerFunc) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/repos/"+sandboxRepo {
			_, _ = io.WriteString(w, `{"full_name":"`+sandboxRepo+`","owner":{"login":"sandbox-owner","type":"Organization"}}`)
			return
		}
		handler(w, r)
	}))
}

func prepareWorktree(t *testing.T, repository, token string) string {
	t.Helper()
	root := t.TempDir()
	gitTest(t, context.Background(), root, "init", "-b", "main")
	gitTest(t, context.Background(), root, "remote", "add", "origin", "https://github.com/"+repository+".git")
	gitWithToken(t, root, token, "-c", "core.hooksPath=/dev/null", "-c", "credential.helper=", "-c", "credential.helper=!f() { printf 'username=x-access-token\\npassword=%s\\n' \"$FACTORY_GITHUB_TOKEN\"; }; f", "fetch", "--depth=1", "origin", "refs/heads/main")
	gitTest(t, context.Background(), root, "checkout", "-B", "main", "FETCH_HEAD")
	gitTest(t, context.Background(), root, "config", "user.name", "Factory live test")
	gitTest(t, context.Background(), root, "config", "user.email", "factory-live-test@localhost")
	if err := os.WriteFile(filepath.Join(root, "factory-live-test.txt"), []byte("disposable live provider verification\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, context.Background(), root, "add", "factory-live-test.txt")
	gitTest(t, context.Background(), root, "commit", "-m", "Add disposable live provider test file")
	return root
}

func gitTest(t *testing.T, ctx context.Context, dir string, args ...string) string {
	t.Helper()
	command := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	command.Env = safeGitEnvironment("")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git operation failed (%s)", strings.Join(args, " "))
	}
	return strings.TrimSpace(string(output))
}

func gitWithToken(t *testing.T, dir, token string, args ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", dir}, args...)...)
	command.Env = safeGitEnvironment(token)
	if _, err := command.CombinedOutput(); err != nil {
		t.Fatalf("authenticated Git fetch failed: %v", err)
	}
}

func safeGitEnvironment(token string) []string {
	allowed := map[string]string{}
	for _, value := range os.Environ() {
		name, content, ok := strings.Cut(value, "=")
		if ok && (name == "PATH" || name == "HOME" || name == "LANG") {
			allowed[name] = content
		}
	}
	result := []string{"GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null"}
	for _, name := range []string{"PATH", "HOME", "LANG"} {
		if value, ok := allowed[name]; ok {
			result = append(result, name+"="+value)
		}
	}
	if token != "" {
		result = append(result, "FACTORY_GITHUB_TOKEN="+token)
	}
	return result
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func workflowJobID(runID, attempt string) (string, error) {
	if runID == "" || attempt == "" || strings.ContainsAny(runID+attempt, "\r\n") {
		return "", errors.New("GitHub Actions run identity is unavailable")
	}
	hash := sha256.Sum256([]byte(fmt.Sprintf("factory-live-github:%s:%s", runID, attempt)))
	hash[6] = hash[6]&0x0f | 0x40
	hash[8] = hash[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", hash[0:4], hash[4:6], hash[6:8], hash[8:10], hash[10:16]), nil
}

func cleanupOwnedPullRequest(ctx context.Context, client *http.Client, token, repository, approvedOrganization, worktree, jobID, expectedCommit, apiOrigin string) error {
	if ctx == nil || ctx.Err() != nil || client == nil || token == "" || !restprovider.ValidRepository(repository) || !filepath.IsAbs(worktree) || !validRepositoryComponent(approvedOrganization) || !restprovider.JobIDValid(jobID) || !isGitCommitID(expectedCommit) || !validAPIOrigin(apiOrigin) {
		return errors.New("live GitHub cleanup configuration is invalid")
	}
	if err := verifyApprovedOrganization(ctx, client, token, repository, approvedOrganization, apiOrigin); err != nil {
		return err
	}
	baseURL, err := url.JoinPath(apiOrigin, "repos", repository, "pulls")
	if err != nil {
		return errors.New("live GitHub cleanup endpoint is invalid")
	}
	owner, _, _ := strings.Cut(repository, "/")
	var prs []restprovider.PullRequest
	for page := 1; page <= 10; page++ {
		query := url.Values{"head": {owner + ":" + restprovider.JobBranch(jobID)}, "state": {"open"}, "per_page": {"100"}, "page": {fmt.Sprint(page)}}
		var current []restprovider.PullRequest
		if err := apiJSON(ctx, client, token, http.MethodGet, baseURL+"?"+query.Encode(), nil, &current); err != nil {
			return err
		}
		prs = append(prs, current...)
		if len(current) < 100 {
			break
		}
		if page == 10 {
			return errors.New("live GitHub cleanup exceeded its bounded PR page limit")
		}
	}
	marker := "<!-- factory-job:" + jobID + " -->"
	owned := 0
	for _, pr := range prs {
		if pr.Head.Ref == restprovider.JobBranch(jobID) && strings.Contains(pr.Body, marker) && pr.Base.Ref == "main" && pr.Base.Repo.FullName == repository && pr.Head.SHA == expectedCommit {
			owned++
		}
	}
	if owned > 1 {
		return errors.New("live GitHub cleanup found multiple PRs with this run's identity")
	}
	for _, pr := range prs {
		if pr.Head.Ref == restprovider.JobBranch(jobID) && !strings.Contains(pr.Body, marker) {
			return errors.New("live GitHub cleanup found a pull request with this branch but a different job marker")
		}
		if pr.Head.Ref == restprovider.JobBranch(jobID) && strings.Contains(pr.Body, marker) && (pr.Head.SHA != expectedCommit || pr.Base.Ref != "main" || pr.Base.Repo.FullName != repository) {
			return errors.New("live GitHub cleanup found a pull request with this run marker but a different commit or repository")
		}
		if pr.Number <= 0 || pr.Head.Ref != restprovider.JobBranch(jobID) || pr.Head.SHA != expectedCommit || !strings.Contains(pr.Body, marker) || pr.Base.Ref != "main" || pr.Base.Repo.FullName != repository || pr.URL != fmt.Sprintf("https://github.com/%s/pull/%d", repository, pr.Number) {
			continue
		}
		if strings.EqualFold(pr.State, "open") {
			endpoint, err := url.JoinPath(baseURL, fmt.Sprint(pr.Number))
			if err != nil {
				return errors.New("live GitHub cleanup PR endpoint is invalid")
			}
			body, _ := json.Marshal(map[string]string{"state": "closed"})
			var closed restprovider.PullRequest
			if err := apiJSON(ctx, client, token, http.MethodPatch, endpoint, body, &closed); err != nil {
				return err
			}
			if closed.Number != pr.Number || !strings.EqualFold(closed.State, "closed") || closed.Head.Ref != restprovider.JobBranch(jobID) || closed.Head.SHA != expectedCommit || !strings.Contains(closed.Body, marker) || closed.Base.Ref != "main" || closed.Base.Repo.FullName != repository {
				return errors.New("live GitHub cleanup could not confirm closing its owned PR")
			}
		}
	}
	return deleteOwnedBranch(ctx, client, token, repository, worktree, restprovider.JobBranch(jobID), expectedCommit, apiOrigin)
}

var errGitHubNotFound = errors.New("GitHub resource not found")

func isLoopbackOrigin(origin string) bool {
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	return parsed.Hostname() == "127.0.0.1" || parsed.Hostname() == "localhost"
}

func validAPIOrigin(origin string) bool {
	parsed, err := url.Parse(origin)
	if err != nil || parsed.User != nil || parsed.Path != "" && parsed.Path != "/" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	return parsed.Scheme == "https" && parsed.Host == "api.github.com" || isLoopbackOrigin(origin)
}

func verifyApprovedOrganization(ctx context.Context, client *http.Client, token, repository, approvedOrganization, apiOrigin string) error {
	if ctx == nil || ctx.Err() != nil || client == nil || token == "" || !restprovider.ValidRepository(repository) || !validRepositoryComponent(approvedOrganization) || !validAPIOrigin(apiOrigin) {
		return errors.New("live GitHub organization verification configuration is invalid")
	}
	endpoint, err := url.JoinPath(apiOrigin, "repos", repository)
	if err != nil {
		return errors.New("live GitHub organization verification endpoint is invalid")
	}
	var result struct {
		FullName string `json:"full_name"`
		Owner    struct {
			Login string `json:"login"`
			Type  string `json:"type"`
		} `json:"owner"`
	}
	if err := apiJSON(ctx, client, token, http.MethodGet, endpoint, nil, &result); err != nil {
		return errors.New("live GitHub sandbox repository identity could not be confirmed")
	}
	if result.FullName != repository || result.Owner.Login != approvedOrganization || result.Owner.Type != "Organization" {
		return errors.New("live GitHub repository is not owned by the approved organization")
	}
	return nil
}

func deleteOwnedBranch(ctx context.Context, client *http.Client, token, repository, worktree, branch, expectedCommit, apiOrigin string) error {
	refEndpoint, err := url.JoinPath(apiOrigin, "repos", repository, "git", "ref", "heads", branch)
	if err != nil {
		return errors.New("live GitHub cleanup branch verification endpoint is invalid")
	}
	var ref struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	if err := apiJSON(ctx, client, token, http.MethodGet, refEndpoint, nil, &ref); err != nil {
		if errors.Is(err, errGitHubNotFound) {
			return nil
		}
		return err
	}
	if !isGitCommitID(expectedCommit) || ref.Object.SHA != expectedCommit {
		return errors.New("live GitHub cleanup refused to delete a branch not matching this run's commit")
	}
	if err := restprovider.GitDeleteBranchWithToken(ctx, token, repository, worktree, branch, expectedCommit); err != nil {
		return errors.New("live GitHub cleanup branch deletion failed; the branch may have changed and was not deleted")
	}
	return nil
}

func apiJSON(ctx context.Context, client *http.Client, token, method, endpoint string, body []byte, target any) error {
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return errors.New("live GitHub cleanup request is invalid")
	}
	setGitHubHeaders(request, token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("live GitHub cleanup request failed")
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return errGitHubNotFound
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("live GitHub cleanup returned HTTP %d", response.StatusCode)
	}
	if target == nil {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(target); err != nil {
		return errors.New("live GitHub cleanup response was invalid")
	}
	return nil
}

func setGitHubHeaders(request *http.Request, token string) {
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
}

func isGitCommitID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func writeWorkflowCleanupState(jobID, commit string) error {
	if !restprovider.JobIDValid(jobID) || commit == "" || strings.ContainsAny(commit, "\r\n") || !isGitCommitID(commit) {
		return errors.New("invalid cleanup identity")
	}
	path := os.Getenv("GITHUB_ENV")
	if path != "" {
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			return errors.New("workflow cleanup environment is unavailable")
		}
		if _, err := fmt.Fprintf(file, "FACTORY_LIVE_GITHUB_COMMIT=%s\n", commit); err != nil {
			file.Close()
			return errors.New("workflow cleanup identity could not be recorded")
		}
		if err := file.Close(); err != nil {
			return errors.New("workflow cleanup environment close failed")
		}
	}
	return nil
}
