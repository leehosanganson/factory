package factory

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestImplementationJobSlugSanitizesAndBoundsDescription(t *testing.T) {
	for _, tc := range []struct {
		name        string
		description string
		want        string
	}{
		{name: "punctuation and path separators", description: "  Add/readable: job names!  ", want: "add-readable-job-names"},
		{name: "non ASCII text", description: "你好", want: "task"},
		{name: "bounded", description: strings.Repeat("a", 80), want: strings.Repeat("a", 48)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := implementationJobSlug(tc.description)
			if got != tc.want {
				t.Fatalf("implementationJobSlug(%q) = %q, want %q", tc.description, got, tc.want)
			}
			if len(got) > 48 || strings.ContainsAny(got, "/\\") {
				t.Fatalf("slug is not a bounded safe path component: %q", got)
			}
		})
	}
}

func TestCreateImplementationWorktreeUsesReadableUniqueJobName(t *testing.T) {
	repo := initTestGitRepo(t)
	head, err := runGit(context.Background(), repo, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	stateRoot := filepath.Join(t.TempDir(), "detached-jobs")
	store, err := NewJobStore(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(filepath.Dir(repo), "configured-worktrees")
	description := "Add/readable job names"
	ids := []string{"a1b2c3d4-1111-4111-8111-111111111111", "b2c3d4e5-2222-4222-8222-222222222222"}
	paths := make(map[string]bool)
	branches := make(map[string]bool)
	for _, id := range ids {
		worktree, branch, err := createImplementationWorktreeAtParent(repo, repo, head, description, id, parent)
		if err != nil {
			t.Fatalf("create worktree for %s: %v", id, err)
		}
		if filepath.Base(worktree) != id[:4]+"-add-readable-job-names" {
			t.Errorf("worktree directory = %q, want short ID and readable slug", filepath.Base(worktree))
		}
		if filepath.Dir(worktree) != parent {
			t.Errorf("worktree parent = %q, want configured parent %q", filepath.Dir(worktree), parent)
		}
		if branch != "factory-job-add-readable-job-names-"+id {
			t.Errorf("work branch = %q, want readable hyphenated namespace with full id", branch)
		}
		if paths[worktree] || branches[branch] {
			t.Errorf("repeated task descriptions collided at path %q or branch %q", worktree, branch)
		}
		paths[worktree], branches[branch] = true, true

		gotBranch, err := runGit(context.Background(), worktree, "branch", "--show-current")
		if err != nil || gotBranch != branch {
			t.Errorf("checked-out branch = %q, err=%v; want %q", gotBranch, err, branch)
		}
		gotRoot, err := runGit(context.Background(), worktree, "rev-parse", "--show-toplevel")
		if err != nil || canonicalTestPath(t, gotRoot) != canonicalTestPath(t, worktree) {
			t.Errorf("worktree root = %q, err=%v; want %q", gotRoot, err, worktree)
		}
		if _, err := os.Stat(filepath.Join(worktree, "README.md")); err != nil {
			t.Errorf("worktree does not contain repository checkout: %v", err)
		}
		record := JobRecord{ID: id, Type: implementationJobType, Worktree: worktree, WorkBranch: branch}
		if err := store.CreateJob(record); err != nil {
			t.Fatal(err)
		}
		stored, err := store.GetJob(id)
		if err != nil || stored.Worktree != worktree || stored.WorkBranch != branch {
			t.Errorf("stored worktree metadata = %q/%q, err=%v; want exact %q/%q", stored.Worktree, stored.WorkBranch, err, worktree, branch)
		}
	}
}

func TestLegacyImplementationJobWorktreeMetadataRemainsUnchanged(t *testing.T) {
	store, err := NewJobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	legacy := JobRecord{
		ID: "legacy-id", Type: implementationJobType, Status: "complete",
		Worktree:   filepath.Join(store.Root(), "legacy-id", "worktree"),
		WorkBranch: "factory-job/legacy-id",
	}
	if err := store.CreateJob(legacy); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetJob(legacy.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Worktree != legacy.Worktree || got.WorkBranch != legacy.WorkBranch {
		t.Fatalf("legacy worktree metadata changed: worktree=%q branch=%q", got.Worktree, got.WorkBranch)
	}
}
