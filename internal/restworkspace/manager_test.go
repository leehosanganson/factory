package restworkspace

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testJobID = "01234567-89ab-4cde-8fab-0123456789ab"

func testManager(t *testing.T, now *time.Time, runner func(context.Context, string, ...string) error) (*Manager, string, string) {
	t.Helper()
	base := t.TempDir()
	repo := filepath.Join(base, "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	gitTest(t, repo, "init", "-q")
	gitTest(t, repo, "config", "user.email", "test@example.com")
	gitTest(t, repo, "config", "user.name", "Workspace Test")
	if err := os.WriteFile(filepath.Join(repo, "original.txt"), []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, repo, "add", ".")
	gitTest(t, repo, "commit", "-qm", "initial")
	root := filepath.Join(base, "results")
	manager, err := New(Config{RepositoryRoot: repo, ResultsRoot: root, Now: func() time.Time { return *now }, RunGit: runner})
	if err != nil {
		t.Fatal(err)
	}
	return manager, repo, root
}

func gitTest(t *testing.T, root string, args ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root}, args...)...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
}

func TestCreateMakesDetachedIsolatedWorktree(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	manager, repo, _ := testManager(t, &now, nil)
	before := gitText(t, repo, "rev-parse", "HEAD")
	workspace, err := manager.Create(testJobID)
	if err != nil {
		t.Fatal(err)
	}
	if workspace.JobID != testJobID || workspace.WorktreePath != filepath.Join(filepath.Dir(workspace.WorktreePath), "worktree") {
		t.Fatalf("unexpected workspace: %+v", workspace)
	}
	if got := gitText(t, repo, "rev-parse", "HEAD"); got != before {
		t.Fatalf("original checkout moved: %s != %s", got, before)
	}
	if got := gitText(t, workspace.WorktreePath, "rev-parse", "HEAD"); got != before {
		t.Fatalf("worktree not at checkout HEAD: %s", got)
	}
	if branch := gitText(t, workspace.WorktreePath, "branch", "--show-current"); branch != "" {
		t.Fatalf("worktree created branch %q", branch)
	}
	if err := os.WriteFile(filepath.Join(workspace.WorktreePath, "job.txt"), []byte("job"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(repo, "job.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("job edit appeared in original checkout: %v", err)
	}
	for _, path := range []string{workspace.StatePath, workspace.OutputPath} {
		if err := verifyProtectedDir(path); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := manager.Create("../../escape"); err == nil {
		t.Fatal("accepted traversal job ID")
	}
}

func TestMarkSucceededWritesAtomicProtectedMarker(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	manager, _, _ := testManager(t, &now, nil)
	if _, err := manager.Create(testJobID); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkSucceeded(testJobID); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(manager.root, testJobID, markerName)
	if err := verifyProtectedFile(path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("marker mode = %o", info.Mode().Perm())
	}
	if _, err := os.Lstat(filepath.Join(manager.root, testJobID, ".completion.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary marker remains: %v", err)
	}
	if err := manager.MarkSucceeded(testJobID); err == nil {
		t.Fatal("overwrote existing completion marker")
	}
}

func TestSweepRetainsAtBoundaryAndRemovesOnlyStrictlyOlderThan24Hours(t *testing.T) {
	now := time.Date(2026, 1, 3, 3, 4, 5, 0, time.UTC)
	manager, _, _ := testManager(t, &now, nil)
	ids := []string{"01234567-89ab-4cde-8fab-0123456789a1", "01234567-89ab-4cde-8fab-0123456789a2"}
	for _, id := range ids {
		if _, err := manager.Create(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := manager.MarkSucceeded(ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkSucceeded(ids[1]); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(manager.root, ids[0], markerName)
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.Replace(string(data), "2026-01-03T03:04:05Z", "2026-01-02T03:04:05Z", 1))
	if err := os.WriteFile(marker, data, 0o600); err != nil {
		t.Fatal(err)
	}
	// Exactly 24 hours old is retained; one nanosecond older expires.
	boundary := now.Add(-24 * time.Hour)
	data = []byte(strings.Replace(string(data), "2026-01-02T03:04:05Z", boundary.Format(time.RFC3339Nano), 1))
	if err := os.WriteFile(marker, data, 0o600); err != nil {
		t.Fatal(err)
	}
	report := manager.Sweep()
	if report.Removed != 0 || report.Retained != 2 {
		t.Fatalf("boundary sweep: %+v", report)
	}
	now = now.Add(time.Nanosecond)
	report = manager.Sweep()
	if report.Removed != 1 || report.Retained != 1 {
		t.Fatalf("expiry sweep: %+v", report)
	}
	if _, err := os.Lstat(filepath.Join(manager.root, ids[0])); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired result remains: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(manager.root, ids[1])); err != nil {
		t.Fatalf("boundary result removed: %v", err)
	}
}

func TestSweepRetainsFailedCanceledOrOrphanedAndMalformedItems(t *testing.T) {
	now := time.Now().UTC()
	manager, _, _ := testManager(t, &now, nil)
	ids := []string{"01234567-89ab-4cde-8fab-0123456789a1", "01234567-89ab-4cde-8fab-0123456789a2"}
	for _, id := range ids {
		if _, err := manager.Create(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := manager.MarkSucceeded(ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(manager.root, ids[0], markerName)); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(manager.root, "malformed"), 0o700); err != nil {
		t.Fatal(err)
	}
	report := manager.Sweep()
	if report.Removed != 0 || report.Retained != 3 || len(report.Errors) < 2 {
		t.Fatalf("unexpected report: %+v", report)
	}
}

func TestSweepRetainsTamperedMarkerIdentityAndSymlink(t *testing.T) {
	now := time.Date(2026, 1, 3, 3, 4, 5, 0, time.UTC)
	manager, _, _ := testManager(t, &now, nil)
	ids := []string{"01234567-89ab-4cde-8fab-0123456789a1", "01234567-89ab-4cde-8fab-0123456789a2", "01234567-89ab-4cde-8fab-0123456789a3"}
	for _, id := range ids {
		if _, err := manager.Create(id); err != nil {
			t.Fatal(err)
		}
		if err := manager.MarkSucceeded(id); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range ids {
		path := filepath.Join(manager.root, id, markerName)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		data = []byte(strings.Replace(string(data), "2026-01-03T03:04:05Z", "2026-01-01T03:04:04Z", 1))
		if id == ids[0] {
			data = []byte(strings.Replace(string(data), ids[0], ids[1], 1))
		} else if id == ids[1] {
			data = []byte(strings.Replace(string(data), `"repository":"`, `"repository":"/wrong/`, 1))
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	worktree := filepath.Join(manager.root, ids[2], "worktree")
	if err := os.RemoveAll(worktree); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, worktree); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Lstat(worktree); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("worktree symlink setup failed: %v", err)
	}
	report := manager.Sweep()
	if report.Removed != 0 || report.Retained != 3 {
		t.Fatalf("tampered items not retained: %+v", report)
	}
	if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
		t.Fatalf("symlink target changed: %v, %v", entries, err)
	}
}

func TestSweepRetainsWrongRepositoryAndGitCleanupFailure(t *testing.T) {
	now := time.Date(2026, 1, 3, 3, 4, 5, 0, time.UTC)
	fail := false
	runner := func(ctx context.Context, root string, args ...string) error {
		if fail && len(args) >= 2 && args[0] == "worktree" && args[1] == "remove" {
			return errors.New("injected cleanup failure")
		}
		return runGit(ctx, root, args...)
	}
	manager, repo, _ := testManager(t, &now, runner)
	for _, id := range []string{"01234567-89ab-4cde-8fab-0123456789a1", "01234567-89ab-4cde-8fab-0123456789a2"} {
		if _, err := manager.Create(id); err != nil {
			t.Fatal(err)
		}
		if err := manager.MarkSucceeded(id); err != nil {
			t.Fatal(err)
		}
	}
	first := filepath.Join(manager.root, "01234567-89ab-4cde-8fab-0123456789a1", markerName)
	data, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	wrongRepo := filepath.Join(filepath.Dir(repo), "different")
	data = []byte(strings.Replace(string(data), repo, wrongRepo, 1))
	data = []byte(strings.Replace(string(data), "2026-01-03T03:04:05Z", "2026-01-01T03:04:04Z", 1))
	if err := os.WriteFile(first, data, 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(manager.root, "01234567-89ab-4cde-8fab-0123456789a2", markerName)
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.Replace(string(data), "2026-01-03T03:04:05Z", "2026-01-01T03:04:04Z", 1))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	fail = true
	report := manager.Sweep()
	if report.Removed != 0 || report.Retained != 2 || len(report.Errors) != 2 {
		t.Fatalf("cleanup failure/wrong repo report: %+v", report)
	}
	for _, id := range []string{"01234567-89ab-4cde-8fab-0123456789a1", "01234567-89ab-4cde-8fab-0123456789a2"} {
		if _, err := os.Lstat(filepath.Join(manager.root, id)); err != nil {
			t.Fatalf("job was removed: %v", err)
		}
	}
}

func TestMarkerRejectsGroupReadablePermissions(t *testing.T) {
	now := time.Now().UTC()
	manager, _, _ := testManager(t, &now, nil)
	if _, err := manager.Create(testJobID); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkSucceeded(testJobID); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(manager.root, testJobID, markerName)
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.readCompletion(filepath.Join(manager.root, testJobID), testJobID); err == nil {
		t.Fatal("accepted group-readable completion metadata")
	}
}

func TestNewRejectsSymlinkInResultsRootPath(t *testing.T) {
	now := time.Now().UTC()
	manager, _, _ := testManager(t, &now, nil)
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(manager.root, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{RepositoryRoot: manager.repoRoot, ResultsRoot: filepath.Join(alias, "nested"), Now: func() time.Time { return now }}); err == nil {
		t.Fatal("accepted results root through symlink ancestor")
	}
}

func TestVerifyResultDirectoryRejectsOutsideAndSymlink(t *testing.T) {
	now := time.Now().UTC()
	manager, _, _ := testManager(t, &now, nil)
	workspace, err := manager.Create(testJobID)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.VerifyResultDirectory(filepath.Dir(workspace.WorktreePath)); err != nil {
		t.Fatal(err)
	}
	if err := manager.VerifyResultDirectory(t.TempDir()); err == nil {
		t.Fatal("accepted arbitrary path")
	}
	link := filepath.Join(manager.root, "01234567-89ab-4cde-8fab-0123456789ac")
	if err := os.Symlink(filepath.Dir(workspace.WorktreePath), link); err != nil {
		t.Fatal(err)
	}
	if err := manager.VerifyResultDirectory(link); err == nil {
		t.Fatal("accepted symlink result path")
	}
}

func gitText(t *testing.T, root string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root}, args...)...)
	output, err := command.Output()
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return strings.TrimSpace(string(output))
}
