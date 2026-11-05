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
	var err error
	base, err = filepath.EvalSymlinks(base)
	if err != nil {
		t.Fatal(err)
	}
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
	serverRoot := filepath.Join(base, "server")
	if err := os.Mkdir(serverRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(serverRoot, "results")
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

func TestCreateRevisionMaterializesCapturedTreeInPrivateRepository(t *testing.T) {
	now := time.Now().UTC()
	manager, repo, _ := testManager(t, &now, nil)
	if err := os.WriteFile(filepath.Join(repo, "history.txt"), []byte("reachable source history"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, repo, "add", "history.txt")
	gitTest(t, repo, "commit", "-qm", "reachable source history")
	gitTest(t, repo, "branch", "extra-branch")
	gitTest(t, repo, "tag", "extra-tag")
	revision, err := manager.CaptureContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "original.txt"), []byte("changed after capture"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, repo, "add", "original.txt")
	gitTest(t, repo, "commit", "-qm", "later source")

	workspace, err := manager.CreateRevisionContext(context.Background(), testJobID, revision)
	if err != nil {
		t.Fatal(err)
	}
	if got := gitText(t, workspace.WorktreePath, "rev-parse", "HEAD"); got != revision.Commit {
		t.Fatalf("materialized commit = %s, want captured %s", got, revision.Commit)
	}
	if got := gitText(t, workspace.WorktreePath, "rev-parse", "HEAD^{tree}"); got != revision.Tree {
		t.Fatalf("materialized tree = %s, want captured %s", got, revision.Tree)
	}
	if got, err := os.ReadFile(filepath.Join(workspace.WorktreePath, "original.txt")); err != nil || string(got) != "original" {
		t.Fatalf("materialized source = %q, err=%v; want captured bytes", got, err)
	}
	if got := gitText(t, workspace.WorktreePath, "for-each-ref", "--format=%(refname)"); got != "" {
		t.Fatalf("private repository copied source refs: %q", got)
	}
	if got := gitText(t, repo, "rev-list", "--count", revision.Commit); got != "2" {
		t.Fatalf("fixture lacks reachable source history: count=%q", got)
	}
	if got := gitText(t, workspace.WorktreePath, "rev-list", "--count", "HEAD"); got != "1" {
		t.Fatalf("private repository retained unrelated source history: count=%q", got)
	}
	gitDir, err := os.Lstat(filepath.Join(workspace.WorktreePath, ".git"))
	if err != nil || !gitDir.IsDir() {
		t.Fatalf("workspace is not a private Git repository: info=%v err=%v", gitDir, err)
	}
	if err := manager.MarkSucceeded(testJobID); err != nil {
		t.Fatalf("MarkSucceeded() for private repository: %v", err)
	}
}

func TestCaptureRejectsUnsupportedGitSourceFeatures(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{name: "checkout filter", setup: func(t *testing.T, repo string) {
			if err := os.WriteFile(filepath.Join(repo, ".gitattributes"), []byte("*.txt filter=custom\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			gitTest(t, repo, "add", ".gitattributes")
			gitTest(t, repo, "commit", "-qm", "filter")
		}},
		{name: "global checkout filter", setup: func(t *testing.T, repo string) {
			t.Setenv("HOME", t.TempDir())
			globalAttributes := filepath.Join(filepath.Dir(repo), "global-attributes")
			if err := os.WriteFile(globalAttributes, []byte("*.txt filter=global\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			gitTest(t, repo, "config", "--global", "core.attributesFile", globalAttributes)
			if err := os.WriteFile(filepath.Join(repo, "filtered.txt"), []byte("filtered"), 0o600); err != nil {
				t.Fatal(err)
			}
			gitTest(t, repo, "add", "filtered.txt")
			gitTest(t, repo, "commit", "-qm", "global filter")
		}},
		{name: "git lfs pointer without filter attributes", setup: func(t *testing.T, repo string) {
			if err := os.WriteFile(filepath.Join(repo, "large.bin"), []byte("version https://git-lfs.github.com/spec/v1\noid sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\nsize 1\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			gitTest(t, repo, "add", ".")
			gitTest(t, repo, "commit", "-qm", "lfs")
		}},
		{name: "git lfs filter attributes", setup: func(t *testing.T, repo string) {
			if err := os.WriteFile(filepath.Join(repo, ".gitattributes"), []byte("*.bin filter=lfs diff=lfs merge=lfs -text\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(repo, "large.bin"), []byte("version https://git-lfs.github.com/spec/v1\noid sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\nsize 1\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			gitTest(t, repo, "add", ".")
			gitTest(t, repo, "commit", "-qm", "lfs")
		}},
		{name: "submodule", setup: func(t *testing.T, repo string) {
			child := filepath.Join(filepath.Dir(repo), "child")
			if err := os.Mkdir(child, 0o700); err != nil {
				t.Fatal(err)
			}
			gitTest(t, child, "init", "-q")
			gitTest(t, child, "config", "user.email", "test@example.com")
			gitTest(t, child, "config", "user.name", "Workspace Test")
			if err := os.WriteFile(filepath.Join(child, "child.txt"), []byte("child"), 0o600); err != nil {
				t.Fatal(err)
			}
			gitTest(t, child, "add", ".")
			gitTest(t, child, "commit", "-qm", "child")
			gitTest(t, repo, "-c", "protocol.file.allow=always", "submodule", "add", "-q", child, "nested")
			gitTest(t, repo, "commit", "-qam", "submodule")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().UTC()
			manager, repo, _ := testManager(t, &now, nil)
			test.setup(t, repo)
			if test.name == "git lfs pointer without filter attributes" {
				if err := os.Remove(filepath.Join(repo, "large.bin")); err != nil {
					t.Fatal(err)
				}
				commit := gitText(t, repo, "rev-parse", "HEAD")
				pointer, err := exec.Command("git", "-C", repo, "cat-file", "blob", commit+":large.bin").Output()
				if err != nil || !isGitLFSPointer(pointer) {
					t.Fatalf("captured tree does not contain the exact LFS pointer fixture: pointer=%q err=%v", pointer, err)
				}
				oldOrder := exec.Command("git", "-C", repo, "grep", "--no-textconv", "-I", "-l", "-F", "-z", "--", "version https://git-lfs.github.com/spec/v1", commit)
				output, err := oldOrder.Output()
				if err != nil || len(output) == 0 {
					t.Fatalf("old argument order did not find the captured pointer: output=%q err=%v", output, err)
				}
			}
			if _, err := manager.CaptureContext(context.Background()); err == nil {
				t.Fatal("CaptureContext() accepted unsupported source features")
			}
		})
	}
}

func TestCreateRevisionPreservesRawTreeBytesWithAmbientGitConfiguration(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	now := time.Now().UTC()
	manager, repo, _ := testManager(t, &now, nil)
	lineEndings := []byte("first\r\nsecond\n")
	if err := os.WriteFile(filepath.Join(repo, "line-endings.txt"), lineEndings, 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, repo, "add", "line-endings.txt")
	gitTest(t, repo, "commit", "-qm", "raw line endings")
	revision, err := manager.CaptureContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	gitTest(t, repo, "config", "--global", "core.autocrlf", "true")
	gitTest(t, repo, "config", "--global", "core.attributesFile", filepath.Join(t.TempDir(), "ambient-attributes"))
	if err := os.WriteFile(filepath.Join(t.TempDir(), "ambient-attributes"), []byte("*.txt filter=ambient\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	workspace, err := manager.CreateRevisionContext(context.Background(), testJobID, revision)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(workspace.WorktreePath, "line-endings.txt"))
	if err != nil || string(got) != string(lineEndings) {
		t.Fatalf("materialized raw tree bytes = %q, err=%v; want %q", got, err, lineEndings)
	}
}

func TestRetainedWorkspaceRestoresSourceRevisionAfterManagerRestart(t *testing.T) {
	now := time.Now().UTC()
	manager, repo, root := testManager(t, &now, nil)
	workspace, err := manager.Create(testJobID)
	if err != nil {
		t.Fatal(err)
	}
	revision := workspace.SourceRevision
	if revision.Commit == "" || revision.Tree == "" {
		t.Fatalf("Create() source revision = %+v, want captured commit and tree", revision)
	}
	recordPath := filepath.Join(manager.root, testJobID, sourceRevisionName)
	info, err := os.Stat(recordPath)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("source identity permissions = %v, err=%v; want 0600", info, err)
	}
	data, err := os.ReadFile(recordPath)
	if err != nil || strings.Contains(string(data), "path") {
		t.Fatalf("source identity contains unexpected path or cannot be read: data=%q err=%v", data, err)
	}

	manager, err = New(Config{RepositoryRoot: repo, ResultsRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	retained, err := manager.RetainedWorkspace(testJobID)
	if err != nil {
		t.Fatalf("RetainedWorkspace() after manager restart: %v", err)
	}
	if retained.SourceRevision != revision {
		t.Fatalf("retained source revision = %+v, want %+v", retained.SourceRevision, revision)
	}
}

func TestRetainedWorkspaceRejectsPrivateWorkspaceWithMissingSourceIdentity(t *testing.T) {
	now := time.Now().UTC()
	manager, repo, root := testManager(t, &now, nil)
	workspace, err := manager.Create(testJobID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(manager.root, testJobID, sourceRevisionName)); err != nil {
		t.Fatal(err)
	}

	manager, err = New(Config{RepositoryRoot: repo, ResultsRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.RetainedWorkspace(testJobID); err == nil {
		t.Fatalf("RetainedWorkspace() accepted private workspace %q without its source identity", workspace.WorktreePath)
	}
}

func TestRetainedWorkspaceAcceptsDescendantCommitAfterProviderPreparation(t *testing.T) {
	now := time.Now().UTC()
	manager, repo, root := testManager(t, &now, nil)
	workspace, err := manager.Create(testJobID)
	if err != nil {
		t.Fatal(err)
	}
	gitTest(t, workspace.WorktreePath, "config", "user.email", "test@example.com")
	gitTest(t, workspace.WorktreePath, "config", "user.name", "Workspace Test")
	if err := os.WriteFile(filepath.Join(workspace.WorktreePath, "provider.txt"), []byte("prepared"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, workspace.WorktreePath, "add", "provider.txt")
	gitTest(t, workspace.WorktreePath, "commit", "-qm", "provider preparation")
	manager, err = New(Config{RepositoryRoot: repo, ResultsRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	retained, err := manager.RetainedWorkspace(testJobID)
	if err != nil {
		t.Fatalf("RetainedWorkspace() rejected descendant HEAD: %v", err)
	}
	if retained.SourceRevision != workspace.SourceRevision {
		t.Fatalf("retained source revision = %+v, want original %+v", retained.SourceRevision, workspace.SourceRevision)
	}
}

func TestRetainedWorkspaceRejectsUnrelatedHead(t *testing.T) {
	now := time.Now().UTC()
	manager, repo, root := testManager(t, &now, nil)
	workspace, err := manager.Create(testJobID)
	if err != nil {
		t.Fatal(err)
	}
	gitTest(t, workspace.WorktreePath, "checkout", "--orphan", "unrelated")
	gitTest(t, workspace.WorktreePath, "rm", "-rf", ".")
	if err := os.WriteFile(filepath.Join(workspace.WorktreePath, "unrelated.txt"), []byte("unrelated"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, workspace.WorktreePath, "add", "unrelated.txt")
	gitTest(t, workspace.WorktreePath, "-c", "user.email=test@example.com", "-c", "user.name=Workspace Test", "commit", "-qm", "unrelated")
	manager, err = New(Config{RepositoryRoot: repo, ResultsRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.RetainedWorkspace(testJobID); err == nil {
		t.Fatal("RetainedWorkspace() accepted unrelated HEAD")
	}
}

func TestRetainedWorkspaceRejectsTamperedSourceIdentity(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, string, SourceRevision)
	}{
		{name: "duplicate field", mutate: func(t *testing.T, path string, revision SourceRevision) {
			t.Helper()
			content := `{"version":1,"job_id":"` + testJobID + `","commit":"` + revision.Commit + `","tree":"` + revision.Tree + `","version":1}`
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unknown field", mutate: func(t *testing.T, path string, revision SourceRevision) {
			t.Helper()
			content := `{"version":1,"job_id":"` + testJobID + `","commit":"` + revision.Commit + `","tree":"` + revision.Tree + `","path":"secret"}`
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "trailing data", mutate: func(t *testing.T, path string, revision SourceRevision) {
			t.Helper()
			content := `{"version":1,"job_id":"` + testJobID + `","commit":"` + revision.Commit + `","tree":"` + revision.Tree + `"} {}`
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "wrong job", mutate: func(t *testing.T, path string, revision SourceRevision) {
			t.Helper()
			content := `{"version":1,"job_id":"11234567-89ab-4cde-8fab-0123456789ab","commit":"` + revision.Commit + `","tree":"` + revision.Tree + `"}`
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "invalid commit ID", mutate: func(t *testing.T, path string, revision SourceRevision) {
			t.Helper()
			content := `{"version":1,"job_id":"` + testJobID + `","commit":"` + strings.Repeat("z", len(revision.Commit)) + `","tree":"` + revision.Tree + `"}`
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "invalid job identity", mutate: func(t *testing.T, path string, revision SourceRevision) {
			t.Helper()
			content := `{"version":1,"job_id":"not-a-uuid","commit":"` + revision.Commit + `","tree":"` + revision.Tree + `"}`
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "malformed JSON", mutate: func(t *testing.T, path string, _ SourceRevision) {
			t.Helper()
			if err := os.WriteFile(path, []byte(`{"version":`), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "nonregular file", mutate: func(t *testing.T, path string, _ SourceRevision) {
			t.Helper()
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unsupported version", mutate: func(t *testing.T, path string, revision SourceRevision) {
			t.Helper()
			content := `{"version":2,"job_id":"` + testJobID + `","commit":"` + revision.Commit + `","tree":"` + revision.Tree + `"}`
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "commit tree mismatch", mutate: func(t *testing.T, path string, revision SourceRevision) {
			t.Helper()
			content := `{"version":1,"job_id":"` + testJobID + `","commit":"` + revision.Commit + `","tree":"` + strings.Repeat("0", len(revision.Tree)) + `"}`
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "insecure permissions", mutate: func(t *testing.T, path string, _ SourceRevision) {
			t.Helper()
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink", mutate: func(t *testing.T, path string, _ SourceRevision) {
			t.Helper()
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(filepath.Dir(path), "state"), path); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().UTC()
			manager, repo, root := testManager(t, &now, nil)
			workspace, err := manager.Create(testJobID)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(manager.root, testJobID, sourceRevisionName)
			test.mutate(t, path, workspace.SourceRevision)
			manager, err = New(Config{RepositoryRoot: repo, ResultsRoot: root})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := manager.RetainedWorkspace(testJobID); err == nil {
				t.Fatal("RetainedWorkspace() accepted tampered source identity")
			}
		})
	}
}

func TestCreateRevisionFailsWhenSourceIdentityCannotBePublished(t *testing.T) {
	now := time.Now().UTC()
	var manager *Manager
	runner := func(ctx context.Context, root string, args ...string) error {
		if err := runGit(ctx, root, args...); err != nil {
			return err
		}
		for _, arg := range args {
			if arg == "checkout" {
				return os.WriteFile(filepath.Join(manager.root, testJobID, sourceRevisionName), []byte("occupied"), 0o600)
			}
		}
		return nil
	}
	manager, _, _ = testManager(t, &now, runner)
	if _, err := manager.Create(testJobID); err == nil {
		t.Fatal("Create() succeeded without publishing source identity")
	}
	if _, err := os.Stat(filepath.Join(manager.root, testJobID, "worktree", ".git")); err != nil {
		t.Fatalf("failed creation did not retain materialized workspace: %v", err)
	}
}

func TestRetainedWorkspaceAcceptsLegacyLinkedWorktreeForReconciliation(t *testing.T) {
	now := time.Now().UTC()
	manager, repo, _ := testManager(t, &now, nil)
	jobDir := filepath.Join(manager.root, testJobID)
	if err := os.Mkdir(jobDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"state", "output"} {
		if err := os.Mkdir(filepath.Join(jobDir, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	legacyWorktree := filepath.Join(jobDir, "worktree")
	gitTest(t, repo, "worktree", "add", "--detach", legacyWorktree, "HEAD")
	workspace, err := manager.RetainedWorkspace(testJobID)
	if err != nil {
		t.Fatalf("RetainedWorkspace() rejected legacy linked worktree: %v", err)
	}
	if workspace.WorktreePath != legacyWorktree || workspace.JobID != testJobID {
		t.Fatalf("RetainedWorkspace() = %+v, want legacy path %q", workspace, legacyWorktree)
	}
	if workspace.SourceRevision != (SourceRevision{}) {
		t.Fatalf("legacy workspace source revision = %+v, want empty", workspace.SourceRevision)
	}
	if got, err := os.ReadFile(filepath.Join(workspace.WorktreePath, "original.txt")); err != nil || string(got) != "original" {
		t.Fatalf("legacy linked worktree content = %q, err=%v", got, err)
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

func TestCreateContextCanceledBeforeProvisioningDoesNotRunGit(t *testing.T) {
	now := time.Now().UTC()
	var calls int
	runner := func(context.Context, string, ...string) error {
		calls++
		return nil
	}
	manager, _, _ := testManager(t, &now, runner)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := manager.CreateContext(ctx, testJobID); !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateContext() error = %v, want context.Canceled", err)
	}
	if calls != 0 {
		t.Fatalf("Git runner called %d times for an already canceled context", calls)
	}
	if _, err := os.Lstat(filepath.Join(manager.root, testJobID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created result directory for canceled creation: %v", err)
	}
}

func TestCreateContextCancellationDuringGitRetainsPartialWorktree(t *testing.T) {
	now := time.Now().UTC()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int
	runner := func(runCtx context.Context, root string, args ...string) error {
		calls++
		if len(args) == 0 || args[0] != "init" {
			return errors.New("unexpected Git command")
		}
		if err := runGit(runCtx, root, args...); err != nil {
			return err
		}
		cancel()
		return runCtx.Err()
	}
	manager, _, _ := testManager(t, &now, runner)

	if _, err := manager.CreateContext(ctx, testJobID); !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateContext() error = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("Git runner called %d times, want one repository initialization", calls)
	}
	jobDir := filepath.Join(manager.root, testJobID)
	if _, err := os.Lstat(filepath.Join(jobDir, "worktree")); err != nil {
		t.Fatalf("partial worktree was not retained: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(jobDir, markerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created success marker for canceled creation: %v", err)
	}
}

func TestCreateContextCreatesDetachedWorktree(t *testing.T) {
	now := time.Now().UTC()
	manager, _, _ := testManager(t, &now, nil)
	workspace, err := manager.CreateContext(context.Background(), testJobID)
	if err != nil {
		t.Fatalf("CreateContext() error = %v", err)
	}
	if err := verifyWorktreeDir(workspace.WorktreePath); err != nil {
		t.Fatalf("CreateContext() worktree is invalid: %v", err)
	}
	if err := manager.MarkSucceeded(testJobID); err != nil {
		t.Fatalf("MarkSucceeded() after context creation: %v", err)
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

func TestCreateRejectsReplacedConfiguredRepositoryBeforeCreatingWorkspace(t *testing.T) {
	now := time.Now().UTC()
	manager, repo, _ := testManager(t, &now, nil)
	replacement := createSiblingCheckout(t, repo)
	repoBackup := swapConfiguredCheckout(t, repo, replacement)
	if _, err := manager.Create(testJobID); err == nil {
		t.Fatal("accepted a different valid checkout at the configured path")
	}
	jobDir := filepath.Join(manager.root, testJobID)
	if _, err := os.Lstat(jobDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created result directory for a rejected repository identity: %v", err)
	}
	restoreConfiguredCheckout(t, repo, repoBackup, replacement)
	if _, err := manager.Create(testJobID); err != nil {
		t.Fatalf("rejected restored original repository identity: %v", err)
	}
}

func TestMarkSucceededRejectsReplacedConfiguredRepositoryWithoutMarker(t *testing.T) {
	now := time.Now().UTC()
	manager, repo, _ := testManager(t, &now, nil)
	workspace, err := manager.Create(testJobID)
	if err != nil {
		t.Fatal(err)
	}
	replacement := createSiblingCheckout(t, repo)
	repoBackup := swapConfiguredCheckout(t, repo, replacement)
	if err := manager.MarkSucceeded(testJobID); err == nil {
		t.Fatal("marked success after configured repository identity changed")
	}
	if _, err := os.Lstat(workspace.WorktreePath); err != nil {
		t.Fatalf("worktree was modified or removed after rejection: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(manager.root, testJobID, markerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created completion marker for a rejected repository identity: %v", err)
	}
	restoreConfiguredCheckout(t, repo, repoBackup, replacement)
	if err := manager.MarkSucceeded(testJobID); err != nil {
		t.Fatalf("rejected restored original repository identity: %v", err)
	}
}

func TestCreateRetainsWorktreeIfRepositoryChangesDuringGitAdd(t *testing.T) {
	now := time.Now().UTC()
	var manager *Manager
	var repo, replacement, repoBackup string
	swapped := false
	runner := func(ctx context.Context, root string, args ...string) error {
		if err := runGit(ctx, root, args...); err != nil {
			return err
		}
		if len(args) > 0 && args[0] == "init" {
			repoBackup = swapConfiguredCheckout(t, repo, replacement)
			swapped = true
		}
		return nil
	}
	manager, repo, _ = testManager(t, &now, runner)
	replacement = createSiblingCheckout(t, repo)
	defer func() {
		if swapped {
			restoreConfiguredCheckout(t, repo, repoBackup, replacement)
		}
	}()

	workspace, err := manager.Create(testJobID)
	if err == nil {
		t.Fatal("accepted repository identity changed during private repository initialization")
	}
	if !swapped {
		t.Fatal("private repository initialization did not execute")
	}
	if _, err := os.Lstat(workspace.WorktreePath); err == nil {
		t.Fatal("expected failed Create to return an empty workspace")
	}
	worktree := filepath.Join(manager.root, testJobID, "worktree")
	if _, err := os.Lstat(worktree); err != nil {
		t.Fatalf("did not retain just-created private repository after identity change: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(manager.root, testJobID, markerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created completion marker after identity change: %v", err)
	}
}

func createSiblingCheckout(t *testing.T, repo string) string {
	t.Helper()
	replacement := filepath.Join(filepath.Dir(repo), "replacement-repo")
	if err := os.Mkdir(replacement, 0o700); err != nil {
		t.Fatal(err)
	}
	gitTest(t, replacement, "init", "-q")
	gitTest(t, replacement, "config", "user.email", "test@example.com")
	gitTest(t, replacement, "config", "user.name", "Workspace Test")
	if err := os.WriteFile(filepath.Join(replacement, "replacement.txt"), []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, replacement, "add", ".")
	gitTest(t, replacement, "commit", "-qm", "replacement")
	return replacement
}

func swapConfiguredCheckout(t *testing.T, repo, replacement string) string {
	t.Helper()
	repoBackup := repo + "-original"
	if err := os.Rename(repo, repoBackup); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, repo); err != nil {
		_ = os.Rename(repoBackup, repo)
		t.Fatal(err)
	}
	return repoBackup
}

func restoreConfiguredCheckout(t *testing.T, repo, repoBackup, replacement string) {
	t.Helper()
	if err := os.Rename(repo, replacement); err != nil {
		t.Errorf("restore replacement checkout location: %v", err)
		return
	}
	if err := os.Rename(repoBackup, repo); err != nil {
		t.Errorf("restore original repository checkout: %v", err)
	}
}

func TestSweepContextReturnsWhenManagerLockIsHeld(t *testing.T) {
	now := time.Now().UTC()
	manager, _, _ := testManager(t, &now, nil)
	manager.mu.Lock()
	defer manager.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	report := manager.SweepContext(ctx)
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("SweepContext blocked for %s after lock-wait deadline", elapsed)
	}
	if report.Removed != 0 || report.Retained != 0 || len(report.Errors) != 1 {
		t.Fatalf("lock-wait cancellation report = %+v", report)
	}
}

func TestSweepContextCancelsBlockedPrivateRepositoryGitAndRetainsWorkspace(t *testing.T) {
	now := time.Date(2026, 1, 3, 3, 4, 5, 0, time.UTC)
	enteredGit := make(chan struct{})
	releaseGit := make(chan struct{})
	var manager *Manager
	runner := func(ctx context.Context, root string, args ...string) error {
		if len(args) > 0 && args[0] == "rev-parse" && root != manager.repoRoot {
			close(enteredGit)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-releaseGit:
				return errors.New("blocked fake Git released")
			}
		}
		return runGit(ctx, root, args...)
	}
	manager, _, _ = testManager(t, &now, runner)
	if _, err := manager.Create(testJobID); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkSucceeded(testJobID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(25 * time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan SweepReport, 1)
	go func() { done <- manager.SweepContext(ctx) }()
	select {
	case <-enteredGit:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep did not reach blocked private repository Git operation")
	}
	cancel()
	select {
	case report := <-done:
		if report.Removed != 0 || report.Retained != 1 {
			t.Fatalf("canceled sweep report = %+v", report)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SweepContext did not return after Git cancellation")
	}
	close(releaseGit)
	if _, err := os.Stat(filepath.Join(manager.root, testJobID)); err != nil {
		t.Fatalf("canceled cleanup removed workspace: %v", err)
	}
}

func TestSweepRemovesExpiredPrivateRepository(t *testing.T) {
	now := time.Date(2026, 1, 3, 3, 4, 5, 0, time.UTC)
	manager, _, _ := testManager(t, &now, nil)
	workspace, err := manager.Create(testJobID)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkSucceeded(testJobID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(25 * time.Hour)
	report := manager.Sweep()
	if report.Removed != 1 || report.Retained != 0 || len(report.Errors) != 0 {
		t.Fatalf("expired private repository sweep = %+v", report)
	}
	if _, err := os.Lstat(workspace.WorktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired private repository remains: %v", err)
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

func TestSweepRetainsWrongRepositoryMetadataAndRemovesExpiredPrivateRepository(t *testing.T) {
	now := time.Date(2026, 1, 3, 3, 4, 5, 0, time.UTC)
	manager, repo, _ := testManager(t, &now, nil)
	ids := []string{"01234567-89ab-4cde-8fab-0123456789a1", "01234567-89ab-4cde-8fab-0123456789a2"}
	for _, id := range ids {
		if _, err := manager.Create(id); err != nil {
			t.Fatal(err)
		}
		if err := manager.MarkSucceeded(id); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(manager.root, id, markerName)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		data = []byte(strings.Replace(string(data), "2026-01-03T03:04:05Z", "2026-01-01T03:04:04Z", 1))
		if id == ids[0] {
			data = []byte(strings.Replace(string(data), repo, filepath.Join(filepath.Dir(repo), "different"), 1))
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	report := manager.Sweep()
	if report.Removed != 1 || report.Retained != 1 || len(report.Errors) != 1 {
		t.Fatalf("wrong repository/expired workspace sweep: %+v", report)
	}
	if _, err := os.Lstat(filepath.Join(manager.root, ids[0])); err != nil {
		t.Fatalf("wrong-repository job was removed: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(manager.root, ids[1])); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired job remains: %v", err)
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

func TestNewCreatesMissingServerDirectoriesPrivately(t *testing.T) {
	now := time.Now().UTC()
	base := t.TempDir()
	base, err := filepath.EvalSymlinks(base)
	if err != nil {
		t.Fatal(err)
	}
	repo := filepath.Join(base, "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	gitTest(t, repo, "init", "-q")
	serverParent := filepath.Join(base, "new-server")
	root := filepath.Join(serverParent, "results")
	if _, err := New(Config{RepositoryRoot: repo, ResultsRoot: root, Now: func() time.Time { return now }}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{serverParent, root} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o700 {
			t.Fatalf("new app-owned directory %s mode = %o, want 700", path, info.Mode().Perm())
		}
	}
}

func TestNewRejectsFilesystemRootWithoutChangingItsMode(t *testing.T) {
	now := time.Now().UTC()
	base := t.TempDir()
	base, err := filepath.EvalSymlinks(base)
	if err != nil {
		t.Fatal(err)
	}
	repo := filepath.Join(base, "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	gitTest(t, repo, "init", "-q")
	before, err := os.Stat(string(filepath.Separator))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{RepositoryRoot: repo, ResultsRoot: string(filepath.Separator), Now: func() time.Time { return now }}); err == nil {
		t.Fatal("accepted filesystem root as results root")
	}
	after, err := os.Stat(string(filepath.Separator))
	if err != nil {
		t.Fatal(err)
	}
	if before.Mode().Perm() != after.Mode().Perm() {
		t.Fatalf("filesystem root mode changed from %o to %o", before.Mode().Perm(), after.Mode().Perm())
	}
}

func TestNewRejectsExistingBroadResultsRootWithoutChangingMode(t *testing.T) {
	now := time.Now().UTC()
	manager, repo, _ := testManager(t, &now, nil)
	broadParent := filepath.Join(filepath.Dir(manager.root), "broad-parent")
	if err := os.Mkdir(broadParent, 0o700); err != nil {
		t.Fatal(err)
	}
	broad := filepath.Join(broadParent, "broad-results")
	if err := os.Mkdir(broad, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(broad, 0o755); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(broad)
	if err != nil {
		t.Fatal(err)
	}
	if before.Mode().Perm() != 0o755 {
		t.Fatalf("fixture results root mode = %o, want 755", before.Mode().Perm())
	}
	if _, err := New(Config{RepositoryRoot: repo, ResultsRoot: broad, Now: func() time.Time { return now }}); err == nil {
		t.Fatal("accepted existing non-private results root")
	}
	after, err := os.Stat(broad)
	if err != nil {
		t.Fatal(err)
	}
	if before.Mode().Perm() != after.Mode().Perm() {
		t.Fatalf("existing results root mode changed from %o to %o", before.Mode().Perm(), after.Mode().Perm())
	}
}

func TestNewRejectsBroadResultsParentWithoutChangingModes(t *testing.T) {
	now := time.Now().UTC()
	manager, repo, _ := testManager(t, &now, nil)
	parent := filepath.Join(filepath.Dir(manager.root), "broad-parent")
	if err := os.Mkdir(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(parent)
	if err != nil {
		t.Fatal(err)
	}
	if before.Mode().Perm() != 0o755 {
		t.Fatalf("fixture parent mode = %o, want 755", before.Mode().Perm())
	}
	root := filepath.Join(parent, "server-results")
	if _, err := New(Config{RepositoryRoot: repo, ResultsRoot: root, Now: func() time.Time { return now }}); err == nil {
		t.Fatal("accepted results root without a private server-specific parent")
	}
	info, err := os.Stat(parent)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != before.Mode().Perm() {
		t.Fatalf("existing parent mode changed from %o to %o", before.Mode().Perm(), info.Mode().Perm())
	}
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("results root should not have been created under a broad parent: %v", err)
	}
}

func TestSweepRetainsWhenChildIdentityChangesDuringCleanup(t *testing.T) {
	now := time.Date(2026, 1, 3, 3, 4, 5, 0, time.UTC)
	manager, _, _ := testManager(t, &now, nil)
	if _, err := manager.Create(testJobID); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkSucceeded(testJobID); err != nil {
		t.Fatal(err)
	}
	jobDir := filepath.Join(manager.root, testJobID)
	path := filepath.Join(jobDir, "output")
	outside := t.TempDir()
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	now = now.Add(25 * time.Hour)
	report := manager.Sweep()
	if report.Removed != 0 || report.Retained != 1 || len(report.Errors) != 1 {
		t.Fatalf("changed child was not retained: %+v", report)
	}
	if _, err := os.Lstat(jobDir); err != nil {
		t.Fatalf("job directory removed after child identity change: %v", err)
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatalf("symlink target was changed: entries=%v err=%v", entries, err)
	}
}

func TestVerifyRootRejectsReplacedRootIdentity(t *testing.T) {
	now := time.Now().UTC()
	manager, _, _ := testManager(t, &now, nil)
	root := manager.root
	if err := os.Rename(root, root+"-original"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := manager.verifyRoot(); err == nil {
		t.Fatal("accepted results root replaced by a different inode")
	}
}

func TestNewRejectsSymlinkedExistingAncestorWithoutTouchingTarget(t *testing.T) {
	now := time.Now().UTC()
	manager, repo, _ := testManager(t, &now, nil)
	target := filepath.Join(filepath.Dir(manager.root), "private-target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(filepath.Dir(manager.root), "alias")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	results := filepath.Join(alias, "nested", "results")
	if _, err := New(Config{RepositoryRoot: repo, ResultsRoot: results, Now: func() time.Time { return now }}); err == nil {
		t.Fatal("accepted path beneath a symlinked ancestor")
	}
	if _, err := os.Lstat(filepath.Join(target, "nested")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created path in symlink target: %v", err)
	}
}

func TestCreateAndSweepSerializeWithinManager(t *testing.T) {
	now := time.Now().UTC()
	enteredGit := make(chan struct{})
	releaseGit := make(chan struct{})
	runner := func(ctx context.Context, root string, args ...string) error {
		if len(args) > 0 && args[0] == "init" {
			close(enteredGit)
			<-releaseGit
		}
		return runGit(ctx, root, args...)
	}
	manager, _, _ := testManager(t, &now, runner)
	createErr := make(chan error, 1)
	go func() {
		_, err := manager.Create(testJobID)
		createErr <- err
	}()
	<-enteredGit
	sweepStarted := make(chan struct{})
	sweepDone := make(chan struct{})
	go func() {
		close(sweepStarted)
		manager.Sweep()
		close(sweepDone)
	}()
	<-sweepStarted
	select {
	case <-sweepDone:
		t.Fatal("sweep ran while create held the manager lock")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseGit)
	if err := <-createErr; err != nil {
		t.Fatal(err)
	}
	<-sweepDone
	if err := manager.VerifyResultDirectory(filepath.Join(manager.root, testJobID)); err != nil {
		t.Fatalf("concurrent sweep interfered with create: %v", err)
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
