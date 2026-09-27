package factory

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMonitorWorktreePathIncludesReadableBranchAndCollisionResistantIdentity(t *testing.T) {
	root := t.TempDir()
	first := monitorPRWorktreePath(root, "repo-one", "feature/readability", 27, "20260518T120000-0123456789ab")
	second := monitorPRWorktreePath(root, "repo-two", "feature/readability", 27, "20260518T120000-0123456789ab")
	if filepath.Base(first) != "checkout" || filepath.Base(filepath.Dir(first)) != "20260518T120000-0123456789ab" {
		t.Fatalf("unexpected worktree path %q", first)
	}
	if !strings.Contains(filepath.Base(filepath.Dir(filepath.Dir(first))), "feature-readability-") {
		t.Fatalf("readable branch component missing from %q", first)
	}
	if !strings.HasPrefix(first, filepath.Join(root, "monitor-pr")+string(filepath.Separator)) {
		t.Fatalf("branch path escaped monitor root: %q", first)
	}
	if first == second {
		t.Fatal("different repository identities collided")
	}
}

func TestMonitorUsesIsolatedBranchWhenPRBranchIsCheckedOutElsewhere(t *testing.T) {
	base := canonicalTestPath(t, t.TempDir())
	bare, repo := filepath.Join(base, "remote.git"), filepath.Join(base, "repo")
	runTestCommand(t, base, "git", "init", "--bare", bare)
	runTestCommand(t, base, "git", "clone", bare, repo)
	runTestCommand(t, repo, "git", "checkout", "-b", "feature")
	runTestCommand(t, repo, "git", "config", "user.name", "Factory Test")
	runTestCommand(t, repo, "git", "config", "user.email", "factory@example.test")
	if err := os.WriteFile(filepath.Join(repo, "tracked"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, repo, "git", "add", "tracked")
	runTestCommand(t, repo, "git", "commit", "-m", "initial")
	runTestCommand(t, repo, "git", "push", "-u", "origin", "feature")
	head, err := runGit(context.Background(), repo, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, repo, "git", "branch", "main", head)
	runTestCommand(t, repo, "git", "checkout", "main")
	userCheckout := filepath.Join(base, "user-checkout")
	runTestCommand(t, repo, "git", "worktree", "add", userCheckout, "feature")
	stateRoot := filepath.Join(base, "state", "factory", "detached-jobs")
	store, err := NewJobStore(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	id := "20260518T120000-0123456789ab"
	worktree, baseline, err := setupExistingPRWorktree(store, repo, "feature", head, 27, id)
	if err != nil {
		t.Fatal(err)
	}
	if baseline != head || worktree == userCheckout || worktree == repo || !strings.HasPrefix(worktree, stateRoot+string(filepath.Separator)) {
		t.Fatalf("monitor did not create its own checkout: path=%q baseline=%q", worktree, baseline)
	}
	branch, err := runGit(context.Background(), worktree, "branch", "--show-current")
	if err != nil || branch != "factory-monitor/"+id {
		t.Fatalf("monitor branch = %q, err=%v", branch, err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "tracked"), []byte("agent edit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(userCheckout, "tracked")); err != nil || string(got) != "baseline\n" {
		t.Fatalf("agent edit leaked into user-owned PR checkout: %q err=%v", got, err)
	}
	if err := validateMonitorWorktree(&monitorJob{ID: id, RepoRoot: repo, PR: 27, HeadBranch: "feature", Worktree: worktree, WorkerBranch: branch, OwnWorktree: true}, stateRoot); err != nil {
		t.Fatalf("Factory-owned isolated worktree rejected: %v", err)
	}
	if err := validateMonitorWorktree(&monitorJob{ID: id, RepoRoot: repo, PR: 27, HeadBranch: "feature", Worktree: userCheckout, WorkerBranch: "feature", OwnWorktree: false}, stateRoot); err == nil {
		t.Fatal("user-owned checkout was accepted as monitor worktree")
	}
}

func TestSafeBranchPathComponentIsReadableBoundedAndCannotEscape(t *testing.T) {
	for _, tc := range []struct {
		branch string
		want   string
	}{
		{branch: "feature/my-readability-change", want: "feature-my-readability-change"},
		{branch: "../../escape", want: "escape"},
		{branch: "---", want: "branch"},
		{branch: strings.Repeat("a", 80), want: strings.Repeat("a", 48)},
	} {
		t.Run(tc.branch, func(t *testing.T) {
			got := safeBranchPathComponent(tc.branch)
			if got != tc.want {
				t.Fatalf("safe branch component = %q, want %q", got, tc.want)
			}
			if filepath.Base(got) != got || strings.Contains(got, "..") || len(got) > 48 {
				t.Fatalf("unsafe branch component %q", got)
			}
		})
	}
}
