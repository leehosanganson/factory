package factory

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveWorktreeParent(t *testing.T) {
	primary := filepath.Join(t.TempDir(), "my-repo")
	for _, tc := range []struct {
		name     string
		template string
		want     string
	}{
		{name: "default", want: filepath.Join(filepath.Dir(primary), "my-repo.worktrees")},
		{name: "relative template", template: "../{repo}-workers", want: filepath.Join(filepath.Dir(primary), "my-repo-workers")},
		{name: "relative based on primary", template: "workers", want: filepath.Join(primary, "workers")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveWorktreeParent(tc.template, primary)
			if err != nil || got != tc.want {
				t.Fatalf("resolveWorktreeParent(%q, %q) = %q, %v; want %q", tc.template, primary, got, err, tc.want)
			}
		})
	}
	absoluteRoot := t.TempDir()
	got, err := resolveWorktreeParent(filepath.Join(absoluteRoot, "{repo}-workers"), primary)
	if want := filepath.Join(absoluteRoot, "my-repo-workers"); err != nil || got != want {
		t.Fatalf("absolute worktree parent = %q, %v; want %q", got, err, want)
	}
}

func TestValidateWorktreeParentRejectsPrimaryAndInvokingLinkedCheckout(t *testing.T) {
	base := t.TempDir()
	primary := filepath.Join(base, "primary")
	linked := filepath.Join(base, "linked")
	outside := filepath.Join(base, "worktrees")
	for _, path := range []string{primary, linked, outside} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, checkout := range []string{primary, linked} {
		if _, err := validateWorktreeParent(filepath.Join(checkout, "nested"), primary, linked); err == nil {
			t.Errorf("parent inside checkout %q was accepted", checkout)
		}
	}
	if got, err := validateWorktreeParent(outside, primary, linked); err != nil || got != resolvedTestPath(t, outside) {
		t.Fatalf("external parent = %q, err=%v", got, err)
	}
	alias := filepath.Join(base, "worktree-alias")
	if err := os.Symlink(linked, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := validateWorktreeParent(filepath.Join(alias, "nested"), primary, linked); err == nil {
		t.Fatal("symlinked parent inside invoking checkout was accepted")
	}
}

func TestJobWorktreeNameRequiresUUIDPrefixAndSafeSlug(t *testing.T) {
	got, err := jobWorktreeName("../../Feature!!", "a1b2c3d4-1111-4111-8111-111111111111")
	if err != nil || got != "a1b2-feature" || filepath.Base(got) != got {
		t.Fatalf("jobWorktreeName = %q, %v", got, err)
	}
	for _, id := range []string{"not-an-id", "g1230000", "abc"} {
		if _, err := jobWorktreeName("task", id); err == nil {
			t.Errorf("invalid ID %q accepted", id)
		}
	}
}

func TestAvailableJobWorktreeNameExtendsPrefixWhenFourCharacterNameCollides(t *testing.T) {
	parent := t.TempDir()
	firstID := "a1b2c3d4-1111-4111-8111-111111111111"
	secondID := "a1b2c3e5-2222-4222-8222-222222222222"
	firstName, err := availableJobWorktreeName(parent, "same task", firstID, "")
	if err != nil || firstName != "a1b2-same-task" {
		t.Fatalf("first worktree name = %q, err=%v", firstName, err)
	}
	if err := os.Mkdir(filepath.Join(parent, firstName), 0o700); err != nil {
		t.Fatal(err)
	}
	secondName, err := availableJobWorktreeName(parent, "same task", secondID, "")
	if err != nil || secondName != "a1b2c-same-task" {
		t.Fatalf("collision fallback = %q, err=%v; want deterministic longer prefix", secondName, err)
	}
	if secondName == firstName {
		t.Fatal("colliding worktree name silently aliased an existing task")
	}
}
