package restserver

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateRepositoryRootsAcceptsExactGitRootAndLinkedWorktree(t *testing.T) {
	requireGit(t)
	root := initGitRepository(t, filepath.Join(canonicalTempDir(t), "repo"))
	config := Config{Repositories: map[string]string{"main": root}}
	if err := config.ValidateRepositoryRoots(); err != nil {
		t.Fatalf("valid Git root rejected: %v", err)
	}

	linked := filepath.Join(canonicalTempDir(t), "linked")
	command := exec.Command("git", "-C", root, "worktree", "add", "-b", "linked-test", linked)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("create linked worktree: %v\n%s", err, output)
	}
	t.Cleanup(func() {
		_ = exec.Command("git", "-C", root, "worktree", "remove", "--force", linked).Run()
	})
	config.Repositories = map[string]string{"linked": linked}
	if err := config.ValidateRepositoryRoots(); err != nil {
		t.Fatalf("valid linked worktree root rejected: %v", err)
	}
}

func TestValidateRepositoryRootsRejectsInvalidRootsWithoutLeakingPaths(t *testing.T) {
	requireGit(t)
	temp := canonicalTempDir(t)
	valid := initGitRepository(t, filepath.Join(temp, "repo"))
	nonGit := filepath.Join(temp, "not-git")
	if err := os.Mkdir(nonGit, 0o700); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(temp, "missing")
	link := filepath.Join(temp, "repo-link")
	if err := os.Symlink(valid, link); err != nil {
		t.Fatal(err)
	}
	linkedParent := filepath.Join(temp, "linked-parent")
	if err := os.Symlink(temp, linkedParent); err != nil {
		t.Fatal(err)
	}
	ancestorLink := filepath.Join(linkedParent, filepath.Base(valid))
	subdirectory := filepath.Join(valid, "nested")
	if err := os.Mkdir(subdirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		root string
	}{
		{"non-Git directory", nonGit},
		{"subdirectory", subdirectory},
		{"symlink path", link},
		{"symlink ancestor", ancestorLink},
		{"missing path", missing},
		{"noncanonical path", valid + string(os.PathSeparator) + ".." + string(os.PathSeparator) + filepath.Base(valid)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const alias = "private-alias"
			err := (Config{Repositories: map[string]string{alias: tc.root}}).ValidateRepositoryRoots()
			if err == nil {
				t.Fatal("invalid repository root was accepted")
			}
			if !strings.Contains(err.Error(), alias) {
				t.Fatalf("error does not identify alias: %v", err)
			}
			if strings.Contains(err.Error(), tc.root) {
				t.Fatalf("error leaked configured root: %v", err)
			}
		})
	}
}

func TestValidateRepositoryRootsRejectsBareRepository(t *testing.T) {
	requireGit(t)
	bare := filepath.Join(canonicalTempDir(t), "bare.git")
	command := exec.Command("git", "init", "--bare", bare)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("initialize bare repository: %v\n%s", err, output)
	}
	err := (Config{Repositories: map[string]string{"bare": bare}}).ValidateRepositoryRoots()
	if err == nil || !strings.Contains(err.Error(), `repository "bare"`) {
		t.Fatalf("bare repository validation error = %v", err)
	}
}

func canonicalTempDir(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temporary directory: %v", err)
	}
	return root
}

func initGitRepository(t *testing.T, root string) string {
	t.Helper()
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("git", "init", root)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("initialize Git repository: %v\n%s", err, output)
	}
	command = exec.Command("git", "-C", root, "config", "user.email", "test@example.invalid")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("configure Git repository: %v\n%s", err, output)
	}
	command = exec.Command("git", "-C", root, "config", "user.name", "Test")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("configure Git repository: %v\n%s", err, output)
	}
	if err := os.WriteFile(filepath.Join(root, "README"), []byte("test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	command = exec.Command("git", "-C", root, "add", "README")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("stage Git fixture: %v\n%s", err, output)
	}
	command = exec.Command("git", "-C", root, "commit", "-m", "test fixture")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("commit Git fixture: %v\n%s", err, output)
	}
	return root
}

func requireGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skipf("Git executable unavailable; repository-root validation requires Git: %v", err)
	}
}
