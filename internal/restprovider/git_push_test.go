package restprovider

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

func TestGitDeleteBranchWithTokenUsesLeaseCAS(t *testing.T) {
	for _, race := range []bool{false, true} {
		t.Run(map[bool]string{false: "expected SHA deletes branch", true: "replacement survives stale lease"}[race], func(t *testing.T) {
			repository, worktree, bare, expectedSHA, replacementSHA := setupDeleteBranchRemote(t, race)
			if err := GitDeleteBranchWithToken(context.Background(), "sensitive-token", repository, worktree, "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb", expectedSHA); (err != nil) != race {
				t.Fatalf("delete error=%v, race=%t", err, race)
			}
			ref, err := exec.Command(testGitExecutable, "--git-dir", bare, "rev-parse", "--verify", "refs/heads/factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb").CombinedOutput()
			if race {
				if err != nil || strings.TrimSpace(string(ref)) != replacementSHA {
					t.Fatalf("replacement ref=%q err=%v, want %s", ref, err, replacementSHA)
				}
			} else if err == nil {
				t.Fatalf("branch still exists at %s", strings.TrimSpace(string(ref)))
			}
		})
	}
}

func TestGitDeleteBranchRejectsInvalidIdentity(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-q")
	for _, tc := range []struct{ repository, branch, sha string }{
		{"other/repo", "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb", strings.Repeat("a", 40)},
		{"acme/widget", "main", strings.Repeat("a", 40)},
		{"acme/widget", "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb", "not-a-sha"},
	} {
		if err := GitDeleteBranchWithToken(context.Background(), "sensitive-token", tc.repository, root, tc.branch, tc.sha); err == nil {
			t.Errorf("accepted repository=%q branch=%q sha=%q", tc.repository, tc.branch, tc.sha)
		}
	}
}

func TestGitDeleteBranchErrorDoesNotLeakToken(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-q")
	err := GitDeleteBranchWithToken(context.Background(), "sensitive-token", "acme/widget", root, "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb", strings.Repeat("a", 40))
	if err == nil || strings.Contains(err.Error(), "sensitive-token") {
		t.Fatalf("unsafe delete error: %v", err)
	}
}

func TestGitDeleteBranchRejectsForeignAndMultipleRemotes(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-q")
	runGit(t, root, "remote", "add", "origin", "https://github.com/other/repo.git")
	branch, sha := "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb", strings.Repeat("a", 40)
	if err := GitDeleteBranchWithToken(context.Background(), "token", "acme/widget", root, branch, sha); err == nil {
		t.Fatal("foreign remote was accepted for branch deletion")
	}
	runGit(t, root, "remote", "set-url", "origin", "https://github.com/acme/widget.git")
	runGit(t, root, "remote", "set-url", "--add", "--push", "origin", "https://github.com/other/repo.git")
	if err := GitDeleteBranchWithToken(context.Background(), "token", "acme/widget", root, branch, sha); err == nil {
		t.Fatal("multiple push remotes were accepted for branch deletion")
	}
}

var testGitExecutable, _ = exec.LookPath("git")

func setupDeleteBranchRemote(t *testing.T, race bool) (repository, worktree, bare, expectedSHA, replacementSHA string) {
	t.Helper()
	root := t.TempDir()
	bare = filepath.Join(root, "remote.git")
	runGit(t, root, "init", "--bare", "-q", bare)
	seed := filepath.Join(root, "seed")
	runGit(t, root, "init", "-q", seed)
	runGit(t, seed, "config", "user.name", "Factory")
	runGit(t, seed, "config", "user.email", "factory@localhost")
	if err := os.WriteFile(filepath.Join(seed, "file"), []byte("expected"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, seed, "add", "file")
	runGit(t, seed, "commit", "-qm", "expected")
	expectedSHA = strings.TrimSpace(string(gitOutputForTest(t, seed, "rev-parse", "HEAD")))
	if err := os.WriteFile(filepath.Join(seed, "file"), []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, seed, "commit", "-qam", "replacement")
	replacementSHA = strings.TrimSpace(string(gitOutputForTest(t, seed, "rev-parse", "HEAD")))
	branch := "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb"
	runGit(t, seed, "push", bare, expectedSHA+":refs/heads/seed", replacementSHA+":refs/heads/replacement")
	runGit(t, bare, "update-ref", "refs/heads/"+branch, expectedSHA)
	worktree = filepath.Join(root, "worktree")
	runGit(t, seed, "worktree", "add", "--detach", "-q", worktree, expectedSHA)
	runGit(t, worktree, "remote", "add", "origin", "git@github.com:acme/widget.git")
	sshPath := filepath.Join(root, "ssh")
	beforeReceive := ":"
	if race {
		beforeReceive = testGitExecutable + " --git-dir=" + bare + " update-ref refs/heads/" + branch + " " + replacementSHA
	}
	ssh := "#!/bin/sh\n" + beforeReceive + "\nexec git-receive-pack " + bare + "\n"
	if err := os.WriteFile(sshPath, []byte(ssh), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(sshPath, 0o700); err != nil {
		t.Fatal(err)
	}
	runGit(t, worktree, "config", "core.sshCommand", sshPath)
	return "acme/widget", worktree, bare, expectedSHA, replacementSHA
}

func gitOutputForTest(t *testing.T, dir string, args ...string) []byte {
	t.Helper()
	output, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
	return output
}
