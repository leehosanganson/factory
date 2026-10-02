package restprovider

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestGitPushBranchRejectsForeignAndMultiplePushRemotes(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-q")
	if err := os.WriteFile(filepath.Join(root, "seed"), []byte("seed"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "add", "seed")
	runGit(t, root, "-c", "user.name=Factory", "-c", "user.email=factory@localhost", "commit", "-m", "seed")
	runGit(t, root, "remote", "add", "origin", "https://github.com/other/repo.git")
	worktree := filepath.Join(t.TempDir(), "worktree")
	runGit(t, root, "worktree", "add", "--detach", "-q", worktree, "HEAD")
	if err := GitPushBranch(context.Background(), "acme/widget", worktree, JobBranch("2c9131bb-2cde-4c9d-aaf3-bcc675e482bb")); err == nil {
		t.Fatal("foreign remote was accepted")
	}
	runGit(t, root, "remote", "set-url", "--add", "--push", "origin", "https://github.com/acme/widget.git")
	if err := GitPushBranch(context.Background(), "acme/widget", worktree, JobBranch("2c9131bb-2cde-4c9d-aaf3-bcc675e482bb")); err == nil {
		t.Fatal("multiple push remotes were accepted")
	}
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", dir}, args...)...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
}

func TestMatchesGitHubRepositoryRejectsCredentialsAndEscapes(t *testing.T) {
	for _, raw := range []string{"https://token@github.com/acme/widget.git", "https://github.com/acme%2Fwidget.git", "https://github.com/acme/widget.git?token=x", "https://github.com/acme/widget.git/extra"} {
		if MatchesGitHubRepository(raw, "acme/widget") {
			t.Errorf("unsafe URL accepted: %q", raw)
		}
	}
}

func TestProviderGitEnvironmentExcludesCredentials(t *testing.T) {
	env := gitProviderEnvironment([]string{"PATH=/bin", "HOME=/tmp", "GITHUB_TOKEN=secret", "GH_TOKEN=secret", "HTTPS_PROXY=http://proxy", "SSH_AUTH_SOCK=/tmp/agent.sock"})
	for _, value := range env {
		if value == "GITHUB_TOKEN=secret" || value == "GH_TOKEN=secret" || value == "HTTPS_PROXY=http://proxy" {
			t.Errorf("unsafe env retained: %q", value)
		}
	}
	foundSocket := false
	for _, value := range env {
		if value == "SSH_AUTH_SOCK=/tmp/agent.sock" {
			foundSocket = true
		}
	}
	if !foundSocket {
		t.Fatal("SSH agent socket was not preserved")
	}
}

func TestTokenCredentialPushDoesNotReturnTokenInError(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-q")
	if err := GitPushBranchWithToken(context.Background(), "sensitive-token", "acme/widget", root, "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb"); err == nil || err.Error() == "sensitive-token" {
		t.Fatalf("unexpected push error: %v", err)
	}
}

func TestProviderTokenGitEnvironmentIsScoped(t *testing.T) {
	got := gitProviderEnvironment([]string{"PATH=/bin", "HOME=/tmp", "GITHUB_TOKEN=secret", "GH_TOKEN=secret"})
	if len(got) != 2 || got[0] != "PATH=/bin" || got[1] != "HOME=/tmp" {
		t.Fatalf("environment=%q", got)
	}
}
