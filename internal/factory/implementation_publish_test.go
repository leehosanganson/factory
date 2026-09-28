package factory

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublishImplementationCreatesCommitPushAndPRFromIsolatedChanges(t *testing.T) {
	target := initTestGitRepo(t)
	baseline := strings.TrimSpace(runPublishTestGit(t, "-C", target, "rev-parse", "HEAD"))
	remote := filepath.Join(t.TempDir(), "origin.git")
	runPublishTestGit(t, "init", "--bare", remote)
	runPublishTestGit(t, "-C", target, "remote", "add", "origin", remote)
	branch, worktree := "factory-implement-test", filepath.Join(t.TempDir(), "worktree")
	runPublishTestGit(t, "-C", target, "worktree", "add", "-b", branch, worktree, baseline)
	if err := os.WriteFile(filepath.Join(worktree, "new file.md"), []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ghPath := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(ghPath, 0o700); err != nil {
		t.Fatal(err)
	}
	capture := filepath.Join(t.TempDir(), "gh-args")
	if err := os.WriteFile(filepath.Join(ghPath, "gh"), []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > "+capture+"\nprintf 'https://github.com/example/repo/pull/42\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", ghPath+string(os.PathListSeparator)+os.Getenv("PATH"))
	result, err := publishImplementation(context.Background(), worktree, target, baseline, publishTestTargetBranch(t, target), branch, "Add carefully", [][]string{{"make", "test"}})
	if err != nil || !strings.Contains(result.Message, "Published implementation PR") || !strings.Contains(result.Message, "https://github.com/example/repo/pull/42") {
		t.Fatalf("publication = %+v, %v", result, err)
	}
	remoteHead := strings.TrimSpace(runPublishTestGit(t, "--git-dir", remote, "rev-parse", "refs/heads/"+branch))
	localHead := strings.TrimSpace(runPublishTestGit(t, "-C", worktree, "rev-parse", "HEAD"))
	if remoteHead != localHead || localHead == baseline {
		t.Fatalf("push heads: baseline=%s local=%s remote=%s", baseline, localHead, remoteHead)
	}
	args, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"pr\ncreate\n--title\nAdd carefully", "## Task", "## Changed files", "new file.md", "make test"} {
		if !strings.Contains(string(args), want) {
			t.Errorf("gh arguments missing %q: %s", want, args)
		}
	}
	if head := strings.TrimSpace(runPublishTestGit(t, "-C", target, "rev-parse", "HEAD")); head != baseline {
		t.Fatalf("target HEAD changed: %s != %s", head, baseline)
	}
}

func TestPublishImplementationNoopSkipsGitHubAndPush(t *testing.T) {
	target := initTestGitRepo(t)
	baseline := strings.TrimSpace(runPublishTestGit(t, "-C", target, "rev-parse", "HEAD"))
	remote := filepath.Join(t.TempDir(), "origin.git")
	runPublishTestGit(t, "init", "--bare", remote)
	runPublishTestGit(t, "-C", target, "remote", "add", "origin", remote)
	branch, worktree := "factory-implement-noop", filepath.Join(t.TempDir(), "worktree")
	runPublishTestGit(t, "-C", target, "worktree", "add", "-b", branch, worktree, baseline)
	result, err := publishImplementation(context.Background(), worktree, target, baseline, publishTestTargetBranch(t, target), branch, "No changes", nil)
	if err != nil || !strings.Contains(result.Message, "no commit or PR") {
		t.Fatalf("no-op = %+v, %v", result, err)
	}
	if head := strings.TrimSpace(runPublishTestGit(t, "-C", worktree, "rev-parse", "HEAD")); head != baseline {
		t.Fatalf("no-op changed HEAD: %s", head)
	}
}

func TestPublishImplementationRejectsRemoteBranchCollision(t *testing.T) {
	target := initTestGitRepo(t)
	baseline := strings.TrimSpace(runPublishTestGit(t, "-C", target, "rev-parse", "HEAD"))
	remote := filepath.Join(t.TempDir(), "origin.git")
	runPublishTestGit(t, "init", "--bare", remote)
	runPublishTestGit(t, "-C", target, "remote", "add", "origin", remote)
	branch, worktree := "factory-implement-collision", filepath.Join(t.TempDir(), "worktree")
	runPublishTestGit(t, "-C", target, "worktree", "add", "-b", branch, worktree, baseline)
	if err := os.WriteFile(filepath.Join(worktree, "change.txt"), []byte("workflow\\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runPublishTestGit(t, "-C", worktree, "push", "origin", "HEAD:refs/heads/"+branch)
	_, err := publishImplementation(context.Background(), worktree, target, baseline, publishTestTargetBranch(t, target), branch, "Collision", nil)
	if err == nil || !strings.Contains(err.Error(), "already exists") || !strings.Contains(err.Error(), "switch -c") || !strings.Contains(err.Error(), "gh pr create --head") {
		t.Fatalf("remote collision recovery = %v", err)
	}
	localHead := strings.TrimSpace(runPublishTestGit(t, "-C", worktree, "rev-parse", "HEAD"))
	remoteHead := strings.TrimSpace(runPublishTestGit(t, "--git-dir", remote, "rev-parse", "refs/heads/"+branch))
	if localHead == baseline || remoteHead == localHead {
		t.Fatalf("collision should retain a local commit without overwriting the remote: baseline=%s local=%s remote=%s", baseline, localHead, remoteHead)
	}
}

func TestPublishImplementationDoesNotOverwriteBranchCreatedAfterAvailabilityCheck(t *testing.T) {
	target := initTestGitRepo(t)
	baseline := strings.TrimSpace(runPublishTestGit(t, "-C", target, "rev-parse", "HEAD"))
	remote := filepath.Join(t.TempDir(), "origin.git")
	runPublishTestGit(t, "init", "--bare", remote)
	runPublishTestGit(t, "-C", target, "remote", "add", "origin", remote)
	branch, worktree := "factory-implement-race", filepath.Join(t.TempDir(), "worktree")
	runPublishTestGit(t, "-C", target, "worktree", "add", "-b", branch, worktree, baseline)
	if err := os.WriteFile(filepath.Join(worktree, "change.txt"), []byte("workflow\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Make the baseline object available in the bare remote so the wrapper can
	// create a conflicting branch after ls-remote reports that it is absent.
	runPublishTestGit(t, "-C", target, "push", "origin", baseline+":refs/heads/seed")
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	wrapperDir := t.TempDir()
	wrapper := fmt.Sprintf(`#!/bin/sh
output=$(%q "$@")
status=$?
printf '%%s' "$output"
case "$*" in
  *"ls-remote"*) %q --git-dir %q update-ref refs/heads/%s %s;;
esac
exit "$status"
`, realGit, realGit, remote, branch, baseline)
	if err := os.WriteFile(filepath.Join(wrapperDir, "git"), []byte(wrapper), 0o700); err != nil {
		t.Fatal(err)
	}
	ghPath := t.TempDir()
	if err := os.WriteFile(filepath.Join(ghPath, "gh"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", wrapperDir+string(os.PathListSeparator)+ghPath+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, err = publishImplementation(context.Background(), worktree, target, baseline, publishTestTargetBranch(t, target), branch, "Race", nil)
	if err == nil || !strings.Contains(err.Error(), "push failed") || !strings.Contains(err.Error(), "retained locally") {
		t.Fatalf("concurrent branch creation should refuse publication and retain commit: %v", err)
	}
	localHead := strings.TrimSpace(runPublishTestGit(t, "-C", worktree, "rev-parse", "HEAD"))
	remoteHead := strings.TrimSpace(runPublishTestGit(t, "--git-dir", remote, "rev-parse", "refs/heads/"+branch))
	if localHead == baseline || remoteHead != baseline || remoteHead == localHead {
		t.Fatalf("race changed remote ref: baseline=%s local=%s remote=%s", baseline, localHead, remoteHead)
	}
}

func TestPublishImplementationStaleBaselineDoesNotCommitOrPush(t *testing.T) {
	target := initTestGitRepo(t)
	baseline := strings.TrimSpace(runPublishTestGit(t, "-C", target, "rev-parse", "HEAD"))
	remote := filepath.Join(t.TempDir(), "origin.git")
	runPublishTestGit(t, "init", "--bare", remote)
	runPublishTestGit(t, "-C", target, "remote", "add", "origin", remote)
	branch, worktree := "factory-implement-stale", filepath.Join(t.TempDir(), "worktree")
	runPublishTestGit(t, "-C", target, "worktree", "add", "-b", branch, worktree, baseline)
	if err := os.WriteFile(filepath.Join(worktree, "change.txt"), []byte("workflow\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "concurrent.txt"), []byte("advance\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runPublishTestGit(t, "-C", target, "add", "concurrent.txt")
	runPublishTestGit(t, "-C", target, "commit", "-m", "advance")
	_, err := publishImplementation(context.Background(), worktree, target, baseline, publishTestTargetBranch(t, target), branch, "Stale task", nil)
	if err == nil || !strings.Contains(err.Error(), "baseline changed") {
		t.Fatalf("stale error = %v", err)
	}
	if head := strings.TrimSpace(runPublishTestGit(t, "-C", worktree, "rev-parse", "HEAD")); head != baseline {
		t.Fatalf("stale commit created: %s", head)
	}
	if _, err := exec.Command("git", "--git-dir", remote, "show-ref", "--verify", "refs/heads/"+branch).CombinedOutput(); err == nil {
		t.Fatal("stale branch was pushed")
	}
}

func TestPublishImplementationMissingPrerequisitesRetainCommitWithoutPushAndGiveCompleteRecovery(t *testing.T) {
	for _, missing := range []string{"gh", "origin"} {
		t.Run(missing, func(t *testing.T) {
			target := initTestGitRepo(t)
			baseline := strings.TrimSpace(runPublishTestGit(t, "-C", target, "rev-parse", "HEAD"))
			remote := filepath.Join(t.TempDir(), "origin.git")
			runPublishTestGit(t, "init", "--bare", remote)
			if missing != "origin" {
				runPublishTestGit(t, "-C", target, "remote", "add", "origin", remote)
			}
			branch, worktree := "factory-implement-missing-"+missing, filepath.Join(t.TempDir(), "worktree")
			runPublishTestGit(t, "-C", target, "worktree", "add", "-b", branch, worktree, baseline)
			if err := os.WriteFile(filepath.Join(worktree, "change.txt"), []byte("workflow\\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			ghPath := t.TempDir()
			if err := os.WriteFile(filepath.Join(ghPath, "gh"), []byte("#!/bin/sh\\nprintf 'https://github.com/example/repo/pull/1\\n'\\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", ghPath+string(os.PathListSeparator)+os.Getenv("PATH"))
			oldLookPath := implementationLookPath
			if missing == "gh" {
				implementationLookPath = func(name string) (string, error) {
					if name == "gh" {
						return "", exec.ErrNotFound
					}
					return oldLookPath(name)
				}
				t.Cleanup(func() { implementationLookPath = oldLookPath })
			}
			_, err := publishImplementation(context.Background(), worktree, target, baseline, publishTestTargetBranch(t, target), branch, "Recover prerequisites", nil)
			if err == nil || !strings.Contains(err.Error(), "no push was made") || !strings.Contains(err.Error(), "push --force-with-lease") || !strings.Contains(err.Error(), "gh pr create") {
				t.Fatalf("recovery details = %v", err)
			}
			if missing == "gh" && strings.Index(err.Error(), "install and authenticate") > strings.Index(err.Error(), "gh pr create") {
				t.Fatalf("PR recovery must be conditional on installing gh first: %v", err)
			}
			localHead := strings.TrimSpace(runPublishTestGit(t, "-C", worktree, "rev-parse", "HEAD"))
			if localHead == baseline {
				t.Fatal("prerequisite check ran before the local commit was retained")
			}
			if _, err := exec.Command("git", "--git-dir", remote, "show-ref", "--verify", "refs/heads/"+branch).CombinedOutput(); err == nil {
				t.Fatal("prerequisite failure pushed a branch")
			}
		})
	}
}

func TestPublishImplementationPRFailureRetainsPushedCommitAndRecoveryDetails(t *testing.T) {
	target := initTestGitRepo(t)
	baseline := strings.TrimSpace(runPublishTestGit(t, "-C", target, "rev-parse", "HEAD"))
	remote := filepath.Join(t.TempDir(), "origin.git")
	runPublishTestGit(t, "init", "--bare", remote)
	runPublishTestGit(t, "-C", target, "remote", "add", "origin", remote)
	branch, worktree := "factory-implement-pr-failure", filepath.Join(t.TempDir(), "worktree")
	runPublishTestGit(t, "-C", target, "worktree", "add", "-b", branch, worktree, baseline)
	if err := os.WriteFile(filepath.Join(worktree, "change.txt"), []byte("workflow\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ghPath := t.TempDir()
	if err := os.WriteFile(filepath.Join(ghPath, "gh"), []byte("#!/bin/sh\necho unavailable >&2\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", ghPath+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, err := publishImplementation(context.Background(), worktree, target, baseline, publishTestTargetBranch(t, target), branch, "Recover me", nil)
	if err == nil || !strings.Contains(err.Error(), "PR creation failed") || !strings.Contains(err.Error(), "Commit:") || !strings.Contains(err.Error(), "gh pr create") {
		t.Fatalf("recovery = %v", err)
	}
	local := strings.TrimSpace(runPublishTestGit(t, "-C", worktree, "rev-parse", "HEAD"))
	remoteHead := strings.TrimSpace(runPublishTestGit(t, "--git-dir", remote, "rev-parse", "refs/heads/"+branch))
	if local == baseline || remoteHead != local {
		t.Fatalf("pushed commit not retained: baseline=%s local=%s remote=%s", baseline, local, remoteHead)
	}
}

func TestRunAutomaticImplementationPreservesWorktreeWhenGitCleanupFails(t *testing.T) {
	target := initTestGitRepo(t)
	baseline := strings.TrimSpace(runPublishTestGit(t, "-C", target, "rev-parse", "HEAD"))
	remote := filepath.Join(t.TempDir(), "origin.git")
	runPublishTestGit(t, "init", "--bare", remote)
	runPublishTestGit(t, "-C", target, "remote", "add", "origin", remote)
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte("#!/bin/sh\nprintf 'https://github.com/example/repo/pull/9\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	agent := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\nprintf 'PASS\\n'\nprintf 'published content\\n' > generated.txt\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	originalCommand := implementationCommand
	cleanupFailure := errors.New("simulated worktree removal failure")
	implementationCommand = func(ctx context.Context, dir, name string, args ...string) (string, error) {
		if name == "git" && len(args) >= 2 && args[0] == "worktree" && args[1] == "remove" {
			return "", cleanupFailure
		}
		return originalCommand(ctx, dir, name, args...)
	}
	t.Cleanup(func() { implementationCommand = originalCommand })
	cfg := DefaultConfig()
	cfg.Command = agent
	cfg.Args = []string{"{system_prompt}", "{task}"}
	cfg.StateDir = filepath.Join(t.TempDir(), "state")
	var output strings.Builder
	if err := RunAutomaticImplementation(context.Background(), cfg, target, "Generate output", strings.NewReader(""), &output, false, false); err != nil {
		t.Fatalf("run: %v\n%s", err, output.String())
	}
	if !strings.Contains(output.String(), "Published implementation PR") || !strings.Contains(output.String(), "cleanup failed; worktree was preserved") || !strings.Contains(output.String(), cleanupFailure.Error()) {
		t.Fatalf("publication or cleanup outcome was not reported: %s", output.String())
	}
	const retainedMarker = "worktree was preserved at "
	lineStart := strings.Index(output.String(), retainedMarker)
	var retainedPath string
	if lineStart >= 0 {
		start := lineStart + len(retainedMarker)
		end := strings.Index(output.String()[start:], ": ")
		if end >= 0 {
			retainedPath = output.String()[start : start+end]
		}
	}
	if retainedPath == "" {
		t.Fatalf("retained worktree path missing from output: %s", output.String())
	}
	if info, err := os.Stat(retainedPath); err != nil || !info.IsDir() {
		t.Fatalf("worktree was not preserved at %s: %v", retainedPath, err)
	}
	listed := runPublishTestGit(t, "-C", target, "worktree", "list", "--porcelain")
	if !strings.Contains(listed, "worktree "+retainedPath+"\n") {
		t.Fatalf("Git no longer registers retained worktree %s:\n%s", retainedPath, listed)
	}
	if head := strings.TrimSpace(runPublishTestGit(t, "-C", target, "rev-parse", "HEAD")); head != baseline {
		t.Fatalf("target HEAD changed: %s != %s", head, baseline)
	}
}

func TestRequireImplementationCheckoutNeedsGitRootAndNamedBranch(t *testing.T) {
	if err := requireImplementationCheckout(context.Background(), t.TempDir()); err == nil || !strings.Contains(err.Error(), "Git checkout") {
		t.Fatalf("non-Git error = %v", err)
	}
	target := initTestGitRepo(t)
	if err := requireImplementationCheckout(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(target, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := requireImplementationCheckout(context.Background(), nested); err == nil || !strings.Contains(err.Error(), "checkout root") {
		t.Fatalf("nested path error = %v", err)
	}
}

func TestRequireCleanCheckoutRejectsUntrackedChanges(t *testing.T) {
	target := initTestGitRepo(t)
	if err := requireCleanCheckout(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "untracked.txt"), []byte("protect me"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := requireImplementationCheckout(context.Background(), target); err != nil {
		t.Fatalf("Git prerequisite should permit dirty checkout when publication is disabled: %v", err)
	}
	if err := requireCleanCheckout(context.Background(), target); err == nil || !strings.Contains(err.Error(), "clean Git checkout") {
		t.Fatalf("dirty error = %v", err)
	}
}

func TestRunAutomaticImplementationIsolatesWorkflowAndPublishes(t *testing.T) {
	target := initTestGitRepo(t)
	baseline := strings.TrimSpace(runPublishTestGit(t, "-C", target, "rev-parse", "HEAD"))
	remote := filepath.Join(t.TempDir(), "origin.git")
	runPublishTestGit(t, "init", "--bare", remote)
	runPublishTestGit(t, "-C", target, "remote", "add", "origin", remote)
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	agent := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\nprintf 'PASS\\n'\nprintf 'workflow output\\n' > generated.txt\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.Command = agent
	cfg.Args = []string{"{system_prompt}", "{task}"}
	cfg.StateDir = filepath.Join(t.TempDir(), "state")
	cfg.PipelineChecks = nil
	var output strings.Builder
	if err := RunAutomaticImplementation(context.Background(), cfg, target, "Generate output", strings.NewReader(""), &output, false, false); err != nil {
		t.Fatalf("run: %v\n%s", err, output.String())
	}
	if !strings.Contains(output.String(), "Published implementation PR") {
		t.Fatalf("not published: %s", output.String())
	}
	if worktreeLine := strings.Split(output.String(), "Published implementation PR")[0]; strings.Contains(worktreeLine, "Implementation worktree retained") {
		t.Fatalf("successful publication unexpectedly retained worktree: %s", output.String())
	}
	listed := runPublishTestGit(t, "-C", target, "worktree", "list", "--porcelain")
	if strings.Contains(listed, filepath.Join(filepath.Dir(target), ".factory-implement-")) {
		t.Fatalf("successful publication left temporary implementation worktree registered:\n%s", listed)
	}
	if head := strings.TrimSpace(runPublishTestGit(t, "-C", target, "rev-parse", "HEAD")); head != baseline {
		t.Fatalf("target HEAD changed: %s", head)
	}
	if status := runPublishTestGit(t, "-C", target, "status", "--porcelain"); status != "" {
		t.Fatalf("target dirty: %s", status)
	}
	refs := runPublishTestGit(t, "--git-dir", remote, "for-each-ref", "--format=%(refname:short)")
	if !strings.Contains(refs, "factory-implement-") {
		t.Fatalf("no published branch: %s", refs)
	}
}

func TestRunAutomaticImplementationReturnsPublicationCancellationAndInterruptsManagedRun(t *testing.T) {
	target := initTestGitRepo(t)
	remote := filepath.Join(t.TempDir(), "origin.git")
	runPublishTestGit(t, "init", "--bare", remote)
	runPublishTestGit(t, "-C", target, "remote", "add", "origin", remote)
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	agent := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\nprintf 'PASS\\n'\nprintf 'generated\\n' > generated.txt\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.Command = agent
	cfg.Args = []string{"{system_prompt}", "{task}"}
	cfg.StateDir = filepath.Join(t.TempDir(), "state")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	originalCommand := implementationCommand
	canceled := false
	implementationCommand = func(commandCtx context.Context, dir, name string, args ...string) (string, error) {
		output, err := originalCommand(commandCtx, dir, name, args...)
		if name == "git" && len(args) > 0 && args[0] == "push" && err == nil {
			canceled = true
			cancel()
		}
		return output, err
	}
	t.Cleanup(func() { implementationCommand = originalCommand })
	var output strings.Builder
	err := RunAutomaticImplementation(ctx, cfg, target, "Generate output", strings.NewReader("yes\nyes\nyes\nyes\n"), &output, true, true)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want cancellation: %s", err, output.String())
	}
	if !canceled {
		t.Fatal("test did not cancel during publication")
	}
	runRoot, err := StateRoot(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(runRoot)
	if err != nil || len(entries) != 1 {
		t.Fatalf("managed run entries = %v, err=%v", entries, err)
	}
	state, err := readManagedState(filepath.Join(runRoot, entries[0].Name()))
	if err != nil || state.Status != "interrupted" {
		t.Fatalf("managed run state = %+v, err=%v; want interrupted", state, err)
	}
	if status := runPublishTestGit(t, "-C", target, "status", "--porcelain"); status != "" {
		t.Fatalf("canceled publication dirtied invoking checkout: %s", status)
	}
	worktrees := runPublishTestGit(t, "-C", target, "worktree", "list", "--porcelain")
	if !strings.Contains(worktrees, ".factory-implement-") {
		t.Fatalf("canceled publication did not preserve recovery worktree:\n%s", worktrees)
	}
	for _, line := range strings.Split(worktrees, "\n") {
		if strings.HasPrefix(line, "worktree ") && strings.Contains(line, ".factory-implement-") {
			worktree := strings.TrimPrefix(line, "worktree ")
			head := strings.TrimSpace(runPublishTestGit(t, "-C", worktree, "rev-parse", "HEAD"))
			if head == strings.TrimSpace(runPublishTestGit(t, "-C", target, "rev-parse", "HEAD")) {
				t.Fatal("canceled publication did not retain its local commit")
			}
		}
	}
}

func TestRunAutomaticImplementationPreservesWorktreeWhenPublicationIsUnavailable(t *testing.T) {
	target := initTestGitRepo(t)
	agent := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\nprintf 'PASS\\n'\nprintf 'recovery\\n' > generated.txt\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.Command = agent
	cfg.Args = []string{"{system_prompt}", "{task}"}
	cfg.StateDir = filepath.Join(t.TempDir(), "state")
	var output strings.Builder
	if err := RunAutomaticImplementation(context.Background(), cfg, target, "Generate output", strings.NewReader(""), &output, false, false); err != nil {
		t.Fatalf("run: %v: %s", err, output.String())
	}
	if !strings.Contains(output.String(), "completed unpublished") {
		t.Fatalf("publication failure was not reported: %s", output.String())
	}
	worktreePath := ""
	for _, line := range strings.Split(output.String(), "\n") {
		if strings.HasPrefix(line, "Worktree: ") {
			worktreePath = strings.TrimPrefix(line, "Worktree: ")
		}
	}
	if worktreePath == "" {
		t.Fatalf("recovery worktree path missing: %s", output.String())
	}
	if info, err := os.Stat(worktreePath); err != nil || !info.IsDir() {
		t.Fatalf("unpublished recovery worktree was not preserved at %s: %v", worktreePath, err)
	}
}

func publishTestTargetBranch(t *testing.T, target string) string {
	t.Helper()
	return strings.TrimSpace(runPublishTestGit(t, "-C", target, "symbolic-ref", "--quiet", "--short", "HEAD"))
}

func runPublishTestGit(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
	return string(output)
}
