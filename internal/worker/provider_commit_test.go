package restworker

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestPrepareProviderBranchRejectsEmptyChanges(t *testing.T) {
	_, workspace, _ := newExecutorFixture(t, "10s", restJobOutputLimit, &executorTestAgent{})
	if _, err := prepareProviderBranch(context.Background(), workspace.workspace.WorktreePath, executorJobID); err == nil {
		t.Fatal("empty changes committed")
	}
}

func TestPrepareProviderBranchCommitsInWorktreeOnly(t *testing.T) {
	_, workspace, repository := newExecutorFixture(t, "10s", restJobOutputLimit, &executorTestAgent{})
	if err := os.WriteFile(filepath.Join(workspace.workspace.WorktreePath, "provider-result"), []byte("verified"), 0o600); err != nil {
		t.Fatal(err)
	}
	commit, err := prepareProviderBranch(context.Background(), workspace.workspace.WorktreePath, executorJobID)
	if err != nil {
		t.Fatal(err)
	}
	branch, err := exec.Command("git", "-C", workspace.workspace.WorktreePath, "branch", "--show-current").Output()
	if err != nil || string(branch) != "factory/job/"+executorJobID+"\n" {
		t.Fatalf("branch=%q err=%v", branch, err)
	}
	if len(commit) < 7 || len(commit) > 64 {
		t.Fatalf("commit=%q", commit)
	}
	status, err := exec.Command("git", "-C", repository, "status", "--porcelain").Output()
	if err != nil || len(status) != 0 {
		t.Fatalf("base checkout changed: %q err=%v", status, err)
	}
}

func TestPrepareProviderBranchPreservesExistingBranch(t *testing.T) {
	_, workspace, _ := newExecutorFixture(t, "10s", restJobOutputLimit, &executorTestAgent{})
	branch := "factory/job/" + executorJobID
	if output, err := exec.Command("git", "-C", workspace.root, "branch", branch).CombinedOutput(); err != nil {
		t.Fatalf("create branch: %v: %s", err, output)
	}
	if err := os.WriteFile(filepath.Join(workspace.workspace.WorktreePath, "provider-result"), []byte("verified"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareProviderBranch(context.Background(), workspace.workspace.WorktreePath, executorJobID); err == nil {
		t.Fatal("existing branch overwritten")
	}
}

func TestPrepareProviderBranchUsesServiceIdentity(t *testing.T) {
	_, workspace, _ := newExecutorFixture(t, "10s", restJobOutputLimit, &executorTestAgent{})
	if err := os.WriteFile(filepath.Join(workspace.workspace.WorktreePath, "provider-result"), []byte("verified"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareProviderBranch(context.Background(), workspace.workspace.WorktreePath, executorJobID); err != nil {
		t.Fatal(err)
	}
	identity, err := exec.Command("git", "-C", workspace.workspace.WorktreePath, "log", "-1", "--format=%an <%ae>").Output()
	if err != nil || string(identity) != "Factory <factory@localhost>\n" {
		t.Fatalf("identity=%q err=%v", identity, err)
	}
}

func TestPrepareProviderBranchSuppressesConfiguredHooks(t *testing.T) {
	_, workspace, _ := newExecutorFixture(t, "10s", restJobOutputLimit, &executorTestAgent{})
	hooksDir := filepath.Join(workspace.root, "hooks")
	if err := os.Mkdir(hooksDir, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(workspace.root, "hook marker with spaces")
	hook := filepath.Join(hooksDir, "pre-commit")
	script := "#!/bin/sh\nprintf 'ran\\n' > \"" + marker + "\"\nexit 1\n"
	if err := os.WriteFile(hook, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("git", "-C", workspace.workspace.WorktreePath, "config", "core.hooksPath", hooksDir).CombinedOutput(); err != nil {
		t.Fatalf("configure shared hooks path: %v: %s", err, output)
	}

	if output, err := exec.Command("git", "-C", workspace.workspace.WorktreePath, "commit", "--allow-empty", "-m", "verify configured hook").CombinedOutput(); err == nil {
		t.Fatalf("ordinary empty commit succeeded despite failing hook: %s", output)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("ordinary commit did not run pre-commit hook: %v", err)
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}

	change := []byte("provider change")
	if err := os.WriteFile(filepath.Join(workspace.workspace.WorktreePath, "provider-result"), change, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareProviderBranch(context.Background(), workspace.workspace.WorktreePath, executorJobID); err != nil {
		t.Fatalf("prepare provider branch: %v", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("provider commit ran configured hook (stat error: %v)", err)
	}
	committed, err := exec.Command("git", "-C", workspace.workspace.WorktreePath, "show", "HEAD:provider-result").Output()
	if err != nil || string(committed) != string(change) {
		t.Fatalf("committed provider change=%q err=%v", committed, err)
	}
}

func TestProviderCommitDoesNotWriteCompletionMarker(t *testing.T) {
	_, workspace, _ := newExecutorFixture(t, "10s", restJobOutputLimit, &executorTestAgent{})
	if err := os.WriteFile(filepath.Join(workspace.workspace.WorktreePath, "provider-result"), []byte("verified"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareProviderBranch(context.Background(), workspace.workspace.WorktreePath, executorJobID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(workspace.workspace.WorktreePath), ".completion.json")); err == nil {
		t.Fatal("completion marker exists before provider result")
	}
}
