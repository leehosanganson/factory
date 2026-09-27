package factory

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestMonitorWorktreePathIncludesReadableBranchAndCollisionResistantIdentity(t *testing.T) {
	root := t.TempDir()
	first := monitorPRWorktreePath(root, "repo-one", "feature/readability", 27)
	second := monitorPRWorktreePath(root, "repo-two", "feature/readability", 27)
	if filepath.Dir(filepath.Dir(first)) != filepath.Join(root, "monitor-pr") || filepath.Base(first) != "checkout" {
		t.Fatalf("unexpected worktree path %q", first)
	}
	if !strings.Contains(filepath.Base(filepath.Dir(first)), "feature-readability-") {
		t.Fatalf("readable branch component missing from %q", first)
	}
	if filepath.Dir(filepath.Dir(first)) != filepath.Join(root, "monitor-pr") {
		t.Fatalf("branch path escaped monitor root: %q", first)
	}
	if first == second {
		t.Fatal("different repository identities collided")
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
