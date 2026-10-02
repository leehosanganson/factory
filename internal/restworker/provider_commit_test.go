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
