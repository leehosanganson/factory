package factory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCleanPristineUsesIsolatedWorktreeAndPublishesOnlyItsChanges(t *testing.T) {
	repo := newCleanRepo(t)
	record := filepath.Join(t.TempDir(), "check-workdirs")
	writeCleanFile(t, repo.work, ".gitignore", "check-workdirs\n")
	writeCleanFile(t, repo.work, "Makefile", "fmt test vet:\n\t@printf '%s\\n' \"$$PWD\" >> \""+record+"\"\n")
	gitClean(t, repo.work, "add", ".gitignore", "Makefile")
	gitClean(t, repo.work, "commit", "-m", "add test checks")
	gitClean(t, repo.work, "push")
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	agent := &cleanIsolatedWorkspaceAgent{target: repo.work}
	workflow := cleanTestWorkflow(repo.work, agent)
	workflow.SetupWorktree = nil
	workflow.Git = nil
	workflow.GitContext = nil
	workflow.Make = nil
	if err := workflow.Run(""); err != nil {
		t.Fatalf("isolated pristine clean failed: %v", err)
	}
	if agent.workdir == "" || agent.workdir == repo.work || isWithin(repo.work, agent.workdir) {
		t.Fatalf("agent ran outside an external isolated worktree: target=%q agent=%q", repo.work, agent.workdir)
	}
	if _, err := os.Stat(agent.workdir); !os.IsNotExist(err) {
		t.Fatalf("temporary worktree was not removed after run: %v", err)
	}
	lines, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	for _, workdir := range strings.Fields(string(lines)) {
		if workdir != agent.workdir {
			t.Errorf("check ran in %q, want isolated worktree %q", workdir, agent.workdir)
		}
	}
	if len(strings.Fields(string(lines))) != 3 {
		t.Fatalf("checks ran %d times, want fmt/test/vet: %q", len(strings.Fields(string(lines))), lines)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "show", "--format=", "--name-only", "HEAD"))); got != "README.md\nfix.txt" {
		t.Fatalf("published paths = %q, want only isolated agent changes", got)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD^"))); got != initial {
		t.Fatalf("published commit parent = %q, want baseline %q", got, initial)
	}
	if got, err := os.ReadFile(filepath.Join(repo.work, "fix.txt")); err != nil || string(got) != "created in isolated workspace\n" {
		t.Fatalf("isolated addition not transferred: %q, %v", got, err)
	}
}

func TestCleanPristineTransfersTrackedDeletionsRenamesAndUntrackedAdditions(t *testing.T) {
	repo := newCleanRepo(t)
	writeCleanFile(t, repo.work, "remove.txt", "delete me\n")
	writeCleanFile(t, repo.work, "old.txt", "rename me\n")
	gitClean(t, repo.work, "add", "remove.txt", "old.txt")
	gitClean(t, repo.work, "commit", "-m", "add transfer fixture")
	gitClean(t, repo.work, "push")
	workflow := cleanTestWorkflow(repo.work, &cleanPristineTransferAgent{})
	workflow.SetupWorktree = nil
	workflow.Git = nil
	workflow.GitContext = nil
	if err := workflow.Run(""); err != nil {
		t.Fatalf("isolated changes failed to transfer and publish: %v", err)
	}
	paths := strings.TrimSpace(string(gitClean(t, repo.work, "diff-tree", "--no-commit-id", "--name-only", "--no-renames", "-r", "HEAD")))
	if paths != "README.md\nnew.txt\nold.txt\nremove.txt" {
		t.Fatalf("commit paths = %q; want modified, added, renamed source/destination, and deleted paths", paths)
	}
	if _, err := os.Stat(filepath.Join(repo.work, "remove.txt")); !os.IsNotExist(err) {
		t.Fatalf("deleted path remains after transfer: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(repo.work, "new.txt")); err != nil || string(got) != "renamed content\n" {
		t.Fatalf("renamed destination content = %q, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(repo.work, "old.txt")); !os.IsNotExist(err) {
		t.Fatalf("renamed source remains after transfer: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(repo.work, "README.md")); err != nil || string(got) != "modified in isolated worktree\n" {
		t.Fatalf("tracked modification content = %q, %v", got, err)
	}
}

func TestCleanPristineRejectsExternalChangesWithoutPublishingThem(t *testing.T) {
	for _, mode := range []string{"added", "modified"} {
		t.Run(mode, func(t *testing.T) {
			repo := newCleanRepo(t)
			agent := &cleanIsolatedWorkspaceAgent{target: repo.work, externalChange: mode}
			workflow := cleanTestWorkflow(repo.work, agent)
			workflow.SetupWorktree = nil
			workflow.Git = nil
			workflow.GitContext = nil
			initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
			err := workflow.Run("")
			if err == nil || !strings.Contains(err.Error(), "original checkout changed") {
				t.Fatalf("external mutation error = %v, want publish refusal", err)
			}
			if agent.workdir == "" || agent.workdir == repo.work || isWithin(repo.work, agent.workdir) {
				t.Fatalf("agent did not receive isolated worktree: target=%q agent=%q", repo.work, agent.workdir)
			}
			if got := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD"))); got != initial {
				t.Fatalf("external mutation was committed: got %s want %s", got, initial)
			}
			if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initial {
				t.Fatalf("external mutation was pushed: got %s want %s", got, initial)
			}
			if mode == "added" {
				if got, err := os.ReadFile(filepath.Join(repo.work, "external.txt")); err != nil || string(got) != "user addition\n" {
					t.Fatalf("user addition was not preserved: %q, %v", got, err)
				}
			} else if got, err := os.ReadFile(filepath.Join(repo.work, "README.md")); err != nil || string(got) != "user modification\n" {
				t.Fatalf("user modification was not preserved: %q, %v", got, err)
			}
			if _, err := os.Stat(filepath.Join(repo.work, "fix.txt")); !os.IsNotExist(err) {
				t.Fatalf("isolated candidate leaked into checkout despite external-change refusal: %v", err)
			}
		})
	}
}

func TestCleanLegacyGitInjectionWithoutGitAtRefusesPristineBeforeAgents(t *testing.T) {
	for _, injection := range []string{"Git", "GitContext"} {
		t.Run(injection, func(t *testing.T) {
			repo := newCleanRepo(t)
			agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
			workflow := cleanTestWorkflow(repo.work, agent)
			workflow.SetupWorktree = nil
			callback := func(name string, args ...string) ([]byte, error) {
				cmd := exec.Command(name, args...)
				cmd.Dir = repo.work
				return cmd.Output()
			}
			if injection == "Git" {
				workflow.Git = callback
			} else {
				workflow.GitContext = func(_ context.Context, name string, args ...string) ([]byte, error) {
					return callback(name, args...)
				}
			}

			err := workflow.Run("")
			if err == nil || !strings.Contains(err.Error(), "requires GitAt") {
				t.Fatalf("legacy %s pristine error = %v, want workdir-aware GitAt refusal", injection, err)
			}
			if len(agent.calls) != 0 {
				t.Fatalf("agent ran before missing worktree setup refusal: %v", agent.calls)
			}
		})
	}
}

func TestCleanRejectsCustomSetupCheckoutThatIsNotAWorktreeAndCleansIt(t *testing.T) {
	repo := newCleanRepo(t)
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
	cleanupCalled := false
	workflow := cleanTestWorkflow(repo.work, agent)
	workflow.SetupWorktree = func(_ context.Context, target, _ string) (string, func() error, error) {
		path := filepath.Join(t.TempDir(), "ordinary-clone")
		if output, err := exec.Command("git", "clone", "--no-hardlinks", target, path).CombinedOutput(); err != nil {
			return "", nil, fmt.Errorf("clone test checkout: %w: %s", err, output)
		}
		return path, func() error {
			cleanupCalled = true
			return os.RemoveAll(path)
		}, nil
	}

	err := workflow.Run("")
	if err == nil || !strings.Contains(err.Error(), "is not a registered Git worktree") {
		t.Fatalf("ordinary-clone setup error = %v, want real-worktree rejection", err)
	}
	if !cleanupCalled {
		t.Fatal("ordinary-clone setup cleanup was not executed")
	}
	if len(agent.calls) != 0 {
		t.Fatalf("agent ran before ordinary-clone refusal: %v", agent.calls)
	}
}

func TestCleanRejectsCustomSetupWorktreeAtWrongHeadAndCleansIt(t *testing.T) {
	repo := newCleanRepo(t)
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
	cleanupCalled := false
	workflow := cleanTestWorkflow(repo.work, agent)
	workflow.SetupWorktree = func(ctx context.Context, target, _ string) (string, func() error, error) {
		path, cleanup, err := createCleanWorktree(ctx, target, initial)
		if err != nil {
			return "", nil, err
		}
		writeCleanFilePathRaw(path, "wrong.txt", "different HEAD\n")
		gitClean(t, path, "add", "wrong.txt")
		gitClean(t, path, "commit", "-m", "wrong HEAD")
		return path, func() error {
			cleanupCalled = true
			return cleanup()
		}, nil
	}

	err := workflow.Run("")
	if err == nil || !strings.Contains(err.Error(), "does not match baseline") {
		t.Fatalf("wrong-HEAD setup error = %v, want baseline rejection", err)
	}
	if !cleanupCalled {
		t.Fatal("wrong-HEAD setup cleanup was not executed")
	}
	if len(agent.calls) != 0 {
		t.Fatalf("agent ran before wrong-HEAD worktree refusal: %v", agent.calls)
	}
}

func TestCleanRejectsCustomSetupWorktreeWithDirtyStatusAndCleansIt(t *testing.T) {
	repo := newCleanRepo(t)
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
	cleanupCalled := false
	workflow := cleanTestWorkflow(repo.work, agent)
	workflow.SetupWorktree = func(ctx context.Context, target, head string) (string, func() error, error) {
		path, cleanup, err := createCleanWorktree(ctx, target, head)
		if err != nil {
			return "", nil, err
		}
		writeCleanFilePathRaw(path, "dirty.txt", "untracked\n")
		return path, func() error {
			cleanupCalled = true
			return cleanup()
		}, nil
	}

	err := workflow.Run("")
	if err == nil || !strings.Contains(err.Error(), "not clean before agent start") {
		t.Fatalf("dirty-worktree setup error = %v, want clean-status rejection", err)
	}
	if !cleanupCalled {
		t.Fatal("dirty-worktree setup cleanup was not executed")
	}
	if len(agent.calls) != 0 {
		t.Fatalf("agent ran before dirty-worktree refusal: %v", agent.calls)
	}
}

func TestCleanGitAtReceivesOriginalAndIsolatedWorkdirs(t *testing.T) {
	repo := newCleanRepo(t)
	workflow := cleanTestWorkflow(repo.work, &cleanNoopAgent{})
	var workdirs []string
	workflow.GitAt = func(ctx context.Context, workdir, name string, args ...string) ([]byte, error) {
		workdirs = append(workdirs, filepath.Clean(workdir))
		cmd := exec.CommandContext(ctx, name, args...)
		cmd.Dir = workdir
		return cmd.Output()
	}
	if err := workflow.Run(""); err != nil {
		t.Fatalf("GitAt pristine run failed: %v", err)
	}
	seenOriginal, seenIsolated := false, false
	for _, workdir := range workdirs {
		seenOriginal = seenOriginal || workdir == filepath.Clean(repo.work)
		seenIsolated = seenIsolated || workdir != filepath.Clean(repo.work)
	}
	if !seenOriginal || !seenIsolated {
		t.Fatalf("GitAt workdirs = %v, want original %q and isolated checkout", workdirs, repo.work)
	}
}

func TestCleanFinalizesStateAfterWorktreeCleanup(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cleanupErr error
		wantStatus string
	}{
		{name: "cleanup failure", cleanupErr: errors.New("worktree cleanup failed"), wantStatus: "failed"},
		{name: "cleanup success", wantStatus: "complete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := newCleanRepo(t)
			stateDir := filepath.Join(filepath.Dir(repo.work), "state")
			workflow := cleanTestWorkflow(repo.work, &cleanNoopAgent{})
			workflow.Config.StateDir = stateDir
			workflow.SetupWorktree = func(ctx context.Context, target, head string) (string, func() error, error) {
				path, cleanup, err := createCleanWorktree(ctx, target, head)
				if err != nil {
					return "", nil, err
				}
				return path, func() error {
					if err := cleanup(); err != nil {
						return err
					}
					return tc.cleanupErr
				}, nil
			}
			var terminalEvent WorkflowEvent
			workflow.Observer = WorkflowObserverFunc(func(event WorkflowEvent) error {
				if event.Type == "workflow.transition" {
					terminalEvent = event
				}
				return nil
			})

			err := workflow.Run("")
			if tc.cleanupErr != nil {
				if !errors.Is(err, tc.cleanupErr) {
					t.Fatalf("clean error = %v, want cleanup error %v", err, tc.cleanupErr)
				}
			} else if err != nil {
				t.Fatalf("successful clean returned error: %v", err)
			}
			state, _ := readCleanState(t, stateDir)
			if state.Status != tc.wantStatus {
				t.Fatalf("persisted status = %q, want %q", state.Status, tc.wantStatus)
			}
			if terminalEvent.Type != "workflow.transition" || !strings.Contains(terminalEvent.Message, tc.wantStatus) {
				t.Fatalf("terminal event = %+v, want status %q", terminalEvent, tc.wantStatus)
			}
			if tc.cleanupErr != nil && !strings.Contains(terminalEvent.Message, tc.cleanupErr.Error()) {
				t.Fatalf("failed terminal event = %+v, want cleanup failure", terminalEvent)
			}
		})
	}
}

func TestCleanSetupErrorRunsAndJoinsCleanupError(t *testing.T) {
	repo := newCleanRepo(t)
	setupErr := errors.New("worktree setup failed")
	cleanupErr := errors.New("worktree cleanup failed")
	cleanupCalled := false
	workflow := cleanTestWorkflow(repo.work, &cleanNoopAgent{})
	workflow.SetupWorktree = func(context.Context, string, string) (string, func() error, error) {
		return "", func() error {
			cleanupCalled = true
			return cleanupErr
		}, setupErr
	}

	err := workflow.Run("")
	if !errors.Is(err, setupErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("setup error = %v, want setup and cleanup errors joined", err)
	}
	if !cleanupCalled {
		t.Fatal("setup cleanup callback was not executed")
	}
}

func TestCleanRejectsSetupReturningOriginalRootBeforeAgents(t *testing.T) {
	repo := newCleanRepo(t)
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
	workflow := cleanTestWorkflow(repo.work, agent)
	cleanupCalled := false
	workflow.SetupWorktree = func(_ context.Context, target, _ string) (string, func() error, error) {
		return target, func() error {
			cleanupCalled = true
			return nil
		}, nil
	}

	err := workflow.Run("")
	if err == nil || !strings.Contains(err.Error(), "must be distinct") {
		t.Fatalf("same-root setup error = %v, want distinct-root refusal", err)
	}
	if !cleanupCalled {
		t.Fatal("same-root setup cleanup callback was not executed")
	}
	if len(agent.calls) != 0 {
		t.Fatalf("agent ran before same-root setup refusal: %v", agent.calls)
	}
}

func TestCleanDirtyWorktreeRunsSafeModeWithoutPublishing(t *testing.T) {
	repo := newCleanRepo(t)
	writeCleanFile(t, repo.work, "README.md", "staged user work\n")
	gitClean(t, repo.work, "add", "README.md")
	writeCleanFile(t, repo.work, "README.md", "unstaged user work\n")
	writeCleanFile(t, repo.work, "untracked.txt", "untracked user work\n")
	initialHead := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))

	// Dirty safe mode must not depend on any publishing destination.
	gitClean(t, repo.work, "branch", "--unset-upstream")
	gitClean(t, repo.work, "remote", "remove", "origin")
	initialStatus := string(gitClean(t, repo.work, "status", "--porcelain=v1", "-z", "--no-renames", "--untracked-files=all"))

	var output bytes.Buffer
	agent := &cleanRecordingAgent{out: &output}
	workflow := cleanTestWorkflow(repo.work, agent)
	workflow.Out = &output
	workflow.Gate = true
	workflow.In = strings.NewReader("yes\nyes\nyes\n")
	var makeTargets []string
	workflow.Make = func(target string, _ ...string) error {
		scope := cleanTestGitScope(repo.work, nil)
		defer scope()
		makeTargets = append(makeTargets, target)
		if !strings.Contains(strings.ToLower(output.String()), "dirty") || !strings.Contains(strings.ToLower(output.String()), "may") {
			t.Fatalf("dirty-worktree warning was not shown before make %s: %q", target, output.String())
		}
		return nil
	}
	addCalls, pushCalls, commitCalls := 0, 0, 0
	workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
		scope := cleanTestGitScope(repo.work, args)
		defer scope()
		if name == "git" && len(args) > 0 {
			switch args[0] {
			case "add":
				addCalls++
			case "push":
				pushCalls++
			case "commit":
				commitCalls++
			}
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = workdir
		return cmd.Output()
	}

	if err := workflow.Run(""); err != nil {
		t.Fatalf("dirty safe mode failed: %v", err)
	}
	if got, want := strings.Join(agent.calls, ","), "review,fix,document"; got != want {
		t.Fatalf("workflow stages = %s, want %s", got, want)
	}
	if got, want := strings.Join(makeTargets, ","), "fmt,test,vet"; got != want {
		t.Fatalf("make targets = %s, want %s", got, want)
	}
	if addCalls != 0 || pushCalls != 0 || commitCalls != 0 {
		t.Fatalf("dirty safe mode staged or published work: adds=%d commits=%d pushes=%d", addCalls, commitCalls, pushCalls)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD"))); got != initialHead {
		t.Fatalf("dirty safe mode changed HEAD: got %s want %s", got, initialHead)
	}
	if got := string(gitClean(t, repo.work, "status", "--porcelain=v1", "-z", "--no-renames", "--untracked-files=all")); got != initialStatus {
		t.Fatalf("pre-existing dirty status changed: got %q want %q", got, initialStatus)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "show", ":README.md"))); got != "staged user work" {
		t.Fatalf("staged user content changed: %q", got)
	}
	if got, err := os.ReadFile(filepath.Join(repo.work, "README.md")); err != nil || string(got) != "unstaged user work\n" {
		t.Fatalf("unstaged user content changed: %q, %v", got, err)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "show", "HEAD:README.md"))); got != "initial" {
		t.Fatalf("committed user baseline changed: %q", got)
	}
	if got, err := os.ReadFile(filepath.Join(repo.work, "untracked.txt")); err != nil || string(got) != "untracked user work\n" {
		t.Fatalf("untracked user content changed: %q, %v", got, err)
	}
	warning := strings.ToLower(output.String())
	if !strings.Contains(warning, "dirty") || !strings.Contains(warning, "may") || !strings.Contains(warning, "agent") || !strings.Contains(warning, "format") {
		t.Fatalf("output did not clearly warn about agent/formatter impact on dirty work: %q", output.String())
	}
	prompt := strings.ToLower(output.String())
	if strings.Contains(prompt, "approve to commit and push") || !strings.Contains(prompt, "approve to verify and complete without committing or pushing changes") {
		t.Fatalf("dirty final approval did not clearly describe safe mode: %q", output.String())
	}
	if !strings.Contains(prompt, "changes remain uncommitted and were not pushed") {
		t.Fatalf("dirty safe-mode outcome was not stated: %q", output.String())
	}
}

func TestCleanRefusesUnsafeInitialRepositoriesBeforeAgents(t *testing.T) {
	for _, mode := range []string{"upstream-ahead", "diverged", "detached"} {
		t.Run(mode, func(t *testing.T) {
			repo := newCleanRepo(t)
			switch mode {
			case "upstream-ahead", "diverged":
				other := cloneCleanRepo(t, repo.bare)
				writeCleanFile(t, other, "remote.txt", "remote\n")
				gitClean(t, other, "add", "remote.txt")
				gitClean(t, other, "commit", "-m", "remote")
				gitClean(t, other, "push", "origin", "HEAD:main")
				gitClean(t, repo.work, "fetch", "origin")
				if mode == "diverged" {
					writeCleanFile(t, repo.work, "local.txt", "local\n")
					gitClean(t, repo.work, "add", "local.txt")
					gitClean(t, repo.work, "commit", "-m", "local")
				}
			case "detached":
				gitClean(t, repo.work, "checkout", "--detach", "HEAD")
			}
			agent := &fakeAgent{outputs: map[string][]string{}}
			workflow := cleanTestWorkflow(repo.work, agent)
			if err := workflow.Run(""); err == nil {
				t.Fatal("unsafe initial repository accepted")
			}
			if len(agent.calls) != 0 {
				t.Fatalf("agent ran before initial safety checks: %v", agent.calls)
			}
		})
	}
}

func TestCleanRefusesMultipleConfiguredMergeRefsBeforeAgentsOrPublication(t *testing.T) {
	repo := newCleanRepo(t)
	initial := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main")))
	gitClean(t, repo.work, "config", "--add", "branch.main.merge", "refs/heads/other")
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
	workflow := cleanTestWorkflow(repo.work, agent)
	pushCalls := 0
	workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
		scope := cleanTestGitScope(repo.work, args)
		defer scope()
		if name == "git" && len(args) > 0 && args[0] == "push" {
			pushCalls++
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = workdir
		return cmd.Output()
	}

	if err := workflow.Run(""); err == nil || !strings.Contains(err.Error(), "exactly one configured upstream branch") {
		t.Fatalf("multiple merge refs error = %v, want ambiguous upstream rejection", err)
	}
	if len(agent.calls) != 0 {
		t.Fatalf("agent ran before ambiguous upstream refusal: %v", agent.calls)
	}
	if pushCalls != 0 {
		t.Fatalf("ambiguous upstream invoked push %d times", pushCalls)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initial {
		t.Fatalf("ambiguous upstream changed remote main: got %s want %s", got, initial)
	}
}

func TestCleanRefusesConfiguredButUnresolvedUpstreamBeforeAgents(t *testing.T) {
	for _, mode := range []string{"missing-ref", "missing-remote", "missing-merge", "invalid-merge"} {
		t.Run(mode, func(t *testing.T) {
			repo := newCleanRepo(t)
			switch mode {
			case "missing-ref":
				gitClean(t, repo.work, "update-ref", "-d", "refs/remotes/origin/main")
			case "missing-remote":
				gitClean(t, repo.work, "branch", "--unset-upstream")
				gitClean(t, repo.work, "config", "branch.main.remote", "origin")
			case "missing-merge":
				gitClean(t, repo.work, "branch", "--unset-upstream")
				gitClean(t, repo.work, "config", "branch.main.merge", "refs/heads/main")
			case "invalid-merge":
				gitClean(t, repo.work, "config", "branch.main.merge", "refs/tags/main")
			}
			agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
			if err := cleanTestWorkflow(repo.work, agent).Run(""); err == nil {
				t.Fatal("clean accepted configured but unresolved or malformed upstream")
			}
			if len(agent.calls) != 0 {
				t.Fatalf("agent ran before configured-upstream refusal: %v", agent.calls)
			}
		})
	}
}

func TestCleanRevalidatesUpstreamConfiguration(t *testing.T) {
	for _, mode := range []string{"upstream-appears", "partial-upstream"} {
		t.Run(mode, func(t *testing.T) {
			repo := newCleanRepo(t)
			gitClean(t, repo.work, "branch", "--unset-upstream")
			workflow := cleanTestWorkflow(repo.work, &cleanNoopAgent{})
			pushCalls := 0
			workflow.Make = func(target string, _ ...string) error {
				scope := cleanTestGitScope(repo.work, nil)
				defer scope()
				if target == "fmt" {
					if mode == "upstream-appears" {
						gitClean(t, repo.work, "branch", "--set-upstream-to=origin/main")
					} else {
						gitClean(t, repo.work, "config", "branch.main.remote", "origin")
					}
				}
				return nil
			}
			workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
				scope := cleanTestGitScope(repo.work, args)
				defer scope()
				if name == "git" && len(args) > 0 && args[0] == "push" {
					pushCalls++
				}
				cmd := exec.Command(name, args...)
				cmd.Dir = workdir
				return cmd.Output()
			}
			if err := workflow.Run(""); err == nil {
				t.Fatal("clean accepted changed upstream configuration")
			}
			if pushCalls != 0 {
				t.Fatalf("push invoked %d times after upstream configuration changed", pushCalls)
			}
		})
	}
}

func TestCleanWithoutUpstreamCreatesSameNameOriginBranch(t *testing.T) {
	repo := newCleanRepo(t)
	gitClean(t, repo.work, "branch", "--unset-upstream")
	gitClean(t, repo.work, "branch", "-m", "clean-topic")
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	if got := string(gitClean(t, repo.bare, "for-each-ref", "--format=%(refname)", "refs/heads/clean-topic")); got != "" {
		t.Fatalf("test setup unexpectedly has remote destination: %q", got)
	}

	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
	if err := cleanTestWorkflow(repo.work, agent).Run(""); err != nil {
		t.Fatalf("clean without upstream failed: %v", err)
	}
	if len(agent.calls) == 0 {
		t.Fatal("agent was not run for clean without upstream")
	}
	remoteHead := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/clean-topic")))
	if remoteHead == initial {
		t.Fatal("checked clean result was not pushed to the same-name origin branch")
	}
	parent := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/clean-topic^")))
	if parent != initial {
		t.Fatalf("pushed clean result parent = %s, want initial HEAD %s", parent, initial)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initial {
		t.Fatalf("fallback push changed origin/main: got %s want %s", got, initial)
	}
}

func TestCleanWithoutUpstreamRefusesExistingSameNameOriginBranchBeforeAgents(t *testing.T) {
	repo := newCleanRepo(t)
	gitClean(t, repo.work, "branch", "--unset-upstream")
	initial := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main")))
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}

	err := cleanTestWorkflow(repo.work, agent).Run("")
	if err == nil {
		t.Fatal("clean accepted an existing same-name origin branch without an upstream")
	}
	if len(agent.calls) != 0 {
		t.Fatalf("agent ran before existing-destination refusal: %v", agent.calls)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initial {
		t.Fatalf("refused clean updated origin/main: got %s want %s", got, initial)
	}
}

func TestCleanWithoutUpstreamFailsClosedForUnavailableOrAmbiguousOrigin(t *testing.T) {
	for _, mode := range []string{"missing-origin", "multiple-push-urls"} {
		t.Run(mode, func(t *testing.T) {
			repo := newCleanRepo(t)
			gitClean(t, repo.work, "branch", "--unset-upstream")
			initial := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main")))
			switch mode {
			case "missing-origin":
				gitClean(t, repo.work, "remote", "remove", "origin")
			case "multiple-push-urls":
				redirect := filepath.Join(t.TempDir(), "redirect.git")
				gitClean(t, "", "init", "--bare", "--initial-branch=main", redirect)
				gitClean(t, repo.work, "remote", "set-url", "--push", "origin", repo.bare)
				gitClean(t, repo.work, "remote", "set-url", "--add", "--push", "origin", redirect)
			}
			agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
			if err := cleanTestWorkflow(repo.work, agent).Run(""); err == nil {
				t.Fatal("clean accepted an unavailable or ambiguous origin destination")
			}
			if len(agent.calls) != 0 {
				t.Fatalf("agent ran before destination validation: %v", agent.calls)
			}
			if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initial {
				t.Fatalf("destination validation failure changed origin/main: got %s want %s", got, initial)
			}
		})
	}
}

func TestCleanWithoutUpstreamRefusesDestinationCreatedDuringRun(t *testing.T) {
	t.Run("before final validation", func(t *testing.T) {
		repo := newCleanRepo(t)
		gitClean(t, repo.work, "branch", "--unset-upstream")
		gitClean(t, repo.work, "branch", "-m", "clean-topic")
		initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
		agent := &cleanDestinationRaceAgent{work: repo.work}

		err := cleanTestWorkflow(repo.work, agent).Run("")
		if err == nil {
			t.Fatal("clean accepted a same-name origin branch created during the run")
		}
		if len(agent.calls) == 0 {
			t.Fatal("test agent did not run before destination-race check")
		}
		if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/clean-topic"))); got != initial {
			t.Fatalf("clean advanced a destination created during the run: got %s want %s", got, initial)
		}
	})

	for _, mode := range []string{"existing-commits", "cleanup-commit"} {
		t.Run("at push boundary/"+mode, func(t *testing.T) {
			repo := newCleanRepo(t)
			gitClean(t, repo.work, "branch", "--unset-upstream")
			gitClean(t, repo.work, "branch", "-m", "clean-topic")
			competing := cloneCleanRepo(t, repo.bare)
			writeCleanFile(t, competing, "competitor.txt", "created by competing writer\n")
			gitClean(t, competing, "add", "competitor.txt")
			gitClean(t, competing, "commit", "-m", "competing destination")
			competingHead := strings.TrimSpace(string(gitClean(t, competing, "rev-parse", "HEAD")))

			var agent Agent = &cleanNoopAgent{}
			if mode == "cleanup-commit" {
				agent = &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
			}
			workflow := cleanTestWorkflow(repo.work, agent)
			workflow.Make = func(target string, _ ...string) error {
				if target == "fmt" {
					gitClean(t, competing, "push", "origin", "HEAD:refs/heads/clean-topic")
				}
				return nil
			}
			pushArgs := []string(nil)
			workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
				scope := cleanTestGitScope(repo.work, args)
				defer scope()
				if name == "git" && len(args) > 0 && args[0] == "push" {
					pushArgs = append([]string(nil), args...)
				}
				cmd := exec.Command(name, args...)
				cmd.Dir = workdir
				return cmd.Output()
			}

			err := workflow.Run("")
			if err == nil || (!strings.Contains(err.Error(), "push clean") && !strings.Contains(err.Error(), "upstream appeared") && !strings.Contains(err.Error(), "push destination changed") && !strings.Contains(err.Error(), "already exists")) {
				t.Fatalf("push-boundary race error = %v, want destination revalidation or push failure", err)
			}
			if len(pushArgs) != 0 && !hasCleanPushLease(pushArgs, "refs/heads/clean-topic") {
				t.Fatalf("fallback push lacks empty expected-value lease: %v", pushArgs)
			}
			if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/clean-topic"))); got != competingHead {
				t.Fatalf("clean replaced competing destination: got %s want %s", got, competingHead)
			}
		})
	}
}

func TestCleanCommitDoesNotIncludeChangesOutsideApprovedPaths(t *testing.T) {
	repo := newCleanRepo(t)
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
	workflow := cleanTestWorkflow(repo.work, agent)
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
		scope := cleanTestGitScope(repo.work, args)
		defer scope()
		if name == "git" && len(args) > 0 && args[0] == "commit" {
			writeCleanFilePathRaw(repo.work, "README.md", "concurrent edit outside commit\n")
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = workdir
		return cmd.Output()
	}
	if err := workflow.Run(""); err == nil {
		t.Fatal("concurrent worktree change did not block push")
	}
	committed := strings.TrimSpace(string(gitClean(t, repo.work, "show", "--format=", "--name-only", "HEAD")))
	if committed != "README.md\nfix.txt" {
		t.Fatalf("concurrent path leaked into commit: %q", committed)
	}
	remote := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main")))
	if remote != initial {
		t.Fatalf("concurrent change resulted in push: initial=%s remote=%s", initial, remote)
	}
	if got := string(gitClean(t, repo.work, "status", "--porcelain")); got == "" {
		t.Fatal("concurrent README change should remain unpushed and uncommitted")
	}
}

func TestCleanRefusesIndexMutationWhilePublicationApprovalIsPending(t *testing.T) {
	repo := newCleanRepo(t)
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	workflow := cleanTestWorkflow(repo.work, &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}})
	workflow.In = &cleanApprovalMutationReader{mutate: func() {
		cmd := exec.Command("git", "hash-object", "-w", "--stdin")
		cmd.Dir = repo.work
		cmd.Stdin = strings.NewReader("changed during approval\n")
		blob, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		gitClean(t, repo.work, "update-index", "--cacheinfo", "100644,"+strings.TrimSpace(string(blob))+",fix.txt")
	}}
	if err := workflow.Run(""); err == nil || !strings.Contains(err.Error(), "staged clean outputs changed after publication approval") {
		t.Fatalf("approval-time index mutation error = %v", err)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD"))); got != initial {
		t.Fatalf("approval-time mutation created a commit: initial=%s head=%s", initial, got)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initial {
		t.Fatalf("approval-time mutation was pushed: initial=%s remote=%s", initial, got)
	}
}

type cleanApprovalMutationReader struct {
	mutate func()
	read   bool
}

func (r *cleanApprovalMutationReader) Read(p []byte) (int, error) {
	if !r.read {
		r.read = true
		r.mutate()
		return copy(p, "yes\n"), nil
	}
	return 0, io.EOF
}

func TestCleanRefusesCommitWithIndexMutationAndNeverPushes(t *testing.T) {
	repo := newCleanRepo(t)
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	workflow := cleanTestWorkflow(repo.work, &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}})
	workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
		scope := cleanTestGitScope(repo.work, args)
		defer scope()
		if name == "git" && len(args) > 0 && args[0] == "commit" {
			cmd := exec.Command("git", "hash-object", "-w", "--stdin")
			cmd.Dir = workdir
			cmd.Stdin = strings.NewReader("altered staged content\n")
			blob, err := cmd.Output()
			if err != nil {
				return nil, err
			}
			gitClean(t, repo.work, "update-index", "--cacheinfo", "100644,"+strings.TrimSpace(string(blob))+",fix.txt")
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = workdir
		return cmd.Output()
	}
	if err := workflow.Run(""); err == nil || !strings.Contains(err.Error(), "committed clean outputs differ from inventory") {
		t.Fatalf("index mutation commit error = %v", err)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "show", "HEAD:fix.txt"))); got != "altered staged content" {
		t.Fatalf("intercepted commit content = %q", got)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initial {
		t.Fatalf("index mutation commit was pushed: initial=%s remote=%s", initial, got)
	}
}

func TestCleanRefusesMergeCommitWithInitialHeadAsFirstParent(t *testing.T) {
	repo := newCleanRepo(t)
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	workflow := cleanTestWorkflow(repo.work, &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}})
	workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
		scope := cleanTestGitScope(repo.work, args)
		defer scope()
		if name == "git" && len(args) > 0 && args[0] == "commit" {
			mergeTree := strings.TrimSpace(string(gitClean(t, repo.work, "write-tree")))
			secondParent := strings.TrimSpace(string(gitClean(t, repo.work, "commit-tree", initial+"^{tree}", "-p", initial, "-m", "second parent")))
			merge := strings.TrimSpace(string(gitClean(t, repo.work, "commit-tree", mergeTree, "-p", initial, "-p", secondParent, "-m", "injected merge")))
			gitClean(t, repo.work, "update-ref", "HEAD", merge)
			return nil, nil
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = workdir
		return cmd.Output()
	}
	if err := workflow.Run(""); err == nil || !strings.Contains(err.Error(), "exactly one parent") {
		t.Fatalf("merge cleanup commit error = %v", err)
	}
	parents := strings.Fields(string(gitClean(t, repo.work, "rev-list", "--parents", "-n", "1", "HEAD")))
	if len(parents) != 3 || parents[1] != initial {
		t.Fatalf("injected commit parents = %v, want merge with initial HEAD as first parent", parents)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initial {
		t.Fatalf("merge cleanup commit was pushed: initial=%s remote=%s", initial, got)
	}
}

func TestCleanPushUsesCapturedURLWhenRemoteConfigChangesAtPush(t *testing.T) {
	repo := newCleanRepo(t)
	redirect := filepath.Join(t.TempDir(), "redirect.git")
	gitClean(t, "", "init", "--bare", "--initial-branch=main", redirect)
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	workflow := cleanTestWorkflow(repo.work, &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}})
	pushCalls := 0
	workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
		scope := cleanTestGitScope(repo.work, args)
		defer scope()
		if name == "git" && len(args) > 0 && args[0] == "push" {
			pushCalls++
			if args[2] != repo.bare || args[3] != strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))+":refs/heads/main" {
				return nil, fmt.Errorf("push did not use captured URL and exact refspec: %q", args)
			}
			gitClean(t, repo.work, "remote", "set-url", "origin", redirect)
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = workdir
		return cmd.Output()
	}
	if err := workflow.Run(""); err != nil {
		t.Fatal(err)
	}
	if pushCalls != 1 {
		t.Fatalf("push invocations = %d, want 1", pushCalls)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got == initial {
		t.Fatal("captured destination did not receive cleanup commit")
	}
	if got := string(gitClean(t, redirect, "for-each-ref", "--format=%(refname)", "refs/heads")); got != "" {
		t.Fatalf("redirected destination received a ref: %q", got)
	}
}

func TestCleanFailsClosedWhenRawRemoteURLsCannotBeInspected(t *testing.T) {
	repo := newCleanRepo(t)
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
	workflow := cleanTestWorkflow(repo.work, agent)
	workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
		scope := cleanTestGitScope(repo.work, args)
		defer scope()
		if name == "git" && len(args) == 4 && args[0] == "config" && args[1] == "--null" && args[2] == "--get-all" && strings.HasPrefix(args[3], "remote.origin.") {
			return nil, fmt.Errorf("simulated config inspection failure")
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = workdir
		return cmd.Output()
	}

	if err := workflow.Run(""); err == nil || !strings.Contains(err.Error(), "inspect configured push URLs") {
		t.Fatalf("clean error = %v, want fail-closed raw URL inspection error", err)
	}
	if len(agent.calls) != 0 {
		t.Fatalf("agent ran before raw destination inspection failure: %v", agent.calls)
	}
}

func TestCleanRefusesInitialURLRewriteOfRawRemoteURL(t *testing.T) {
	for _, mode := range []string{"push-url", "fallback-url", "push-instead-of"} {
		t.Run(mode, func(t *testing.T) {
			repo := newCleanRepo(t)
			redirect := filepath.Join(t.TempDir(), "redirect.git")
			gitClean(t, "", "init", "--bare", "--initial-branch=main", redirect)
			rawURL := repo.bare
			rewriteRule := "insteadOf"
			if mode == "push-url" {
				gitClean(t, repo.work, "config", "remote.origin.pushurl", rawURL)
			}
			if mode == "push-instead-of" {
				rewriteRule = "pushInsteadOf"
			}
			gitClean(t, repo.work, "config", "url."+redirect+"."+rewriteRule, rawURL)

			if got := strings.TrimSpace(string(gitClean(t, repo.work, "remote", "get-url", "--push", "origin"))); got != redirect {
				t.Fatalf("test setup did not reproduce Git URL rewriting: got %q, want %q", got, redirect)
			}
			agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
			wantError := "matching Git url.*.insteadOf rule"
			if mode == "push-instead-of" {
				wantError = "matching Git url.*.pushInsteadOf rule"
			}
			if err := cleanTestWorkflow(repo.work, agent).Run(""); err == nil || !strings.Contains(err.Error(), wantError) {
				t.Fatalf("clean error = %v, want rejection of the matching raw-URL rewrite", err)
			}
			if len(agent.calls) != 0 {
				t.Fatalf("agent ran before rewritten destination was rejected: %v", agent.calls)
			}
			if got := strings.TrimSpace(string(gitClean(t, redirect, "for-each-ref", "--format=%(refname)", "refs/heads"))); got != "" {
				t.Fatalf("rewritten destination was modified: %q", got)
			}
		})
	}
}

func TestCleanRefusesMatchingURLRewriteAtPushBoundary(t *testing.T) {
	for _, mode := range []string{"ahead-only", "cleanup-commit", "config-unavailable"} {
		t.Run(mode, func(t *testing.T) {
			repo := newCleanRepo(t)
			redirect := filepath.Join(t.TempDir(), "redirect.git")
			gitClean(t, "", "init", "--bare", "--initial-branch=main", redirect)
			initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
			agent := Agent(&cleanNoopAgent{})
			if mode == "ahead-only" {
				writeCleanFile(t, repo.work, "ahead.txt", "local work\n")
				gitClean(t, repo.work, "add", "ahead.txt")
				gitClean(t, repo.work, "commit", "-m", "existing local work")
			} else {
				agent = &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
			}
			workflow := cleanTestWorkflow(repo.work, agent)
			pushCalls := 0
			rewriteAdded := false
			workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
				scope := cleanTestGitScope(repo.work, args)
				defer scope()
				if name == "git" && len(args) == 3 && args[0] == "config" && args[1] == "--null" && args[2] == "--list" && !rewriteAdded {
					rewriteAdded = true
					if mode == "config-unavailable" {
						return nil, fmt.Errorf("simulated config inspection failure")
					}
					gitClean(t, repo.work, "config", "url."+redirect+".insteadOf", repo.bare)
				}
				if name == "git" && len(args) > 0 && args[0] == "push" {
					pushCalls++
				}
				cmd := exec.Command(name, args...)
				cmd.Dir = workdir
				return cmd.Output()
			}
			err := workflow.Run("")
			wantError := "matching Git url.*.insteadOf rule"
			if mode == "config-unavailable" {
				wantError = "cannot inspect Git URL rewrite configuration"
			}
			if err == nil || !strings.Contains(err.Error(), wantError) {
				t.Fatalf("URL rewrite error = %v, want %q", err, wantError)
			}
			if !rewriteAdded {
				t.Fatal("URL rewrite was not inserted at the push boundary")
			}
			if pushCalls != 0 {
				t.Fatalf("push invoked %d times despite matching URL rewrite", pushCalls)
			}
			if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initial {
				t.Fatalf("intended remote changed: got %s want %s", got, initial)
			}
			if got := string(gitClean(t, redirect, "for-each-ref", "--format=%(refname)", "refs/heads")); got != "" {
				t.Fatalf("redirected remote received a ref: %q", got)
			}
		})
	}
}

func TestCleanRefusesUpstreamChangeAfterCommit(t *testing.T) {
	repo := newCleanRepo(t)
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	workflow := cleanTestWorkflow(repo.work, &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}})
	workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
		scope := cleanTestGitScope(repo.work, args)
		defer scope()
		if name == "git" && len(args) > 0 && args[0] == "commit" {
			gitClean(t, repo.work, "config", "branch.main.remote", "redirect")
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = workdir
		return cmd.Output()
	}
	if err := workflow.Run(""); err == nil || !strings.Contains(err.Error(), "upstream changed after commit") {
		t.Fatalf("changed upstream error = %v", err)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initial {
		t.Fatalf("changed upstream was pushed: initial=%s remote=%s", initial, got)
	}
}

func TestCleanPushIgnoresPushRemoteAndConfiguredPushRefspec(t *testing.T) {
	repo := newCleanRepo(t)
	redirect := filepath.Join(t.TempDir(), "redirect.git")
	gitClean(t, "", "init", "--bare", "--initial-branch=main", redirect)
	gitClean(t, repo.work, "remote", "add", "redirect", redirect)
	gitClean(t, repo.work, "config", "branch.main.pushRemote", "redirect")
	gitClean(t, repo.work, "config", "remote.pushDefault", "redirect")
	gitClean(t, repo.work, "config", "remote.origin.push", "+refs/heads/*:refs/heads/*")
	gitClean(t, repo.work, "config", "remote.origin.push", "+refs/heads/main:refs/heads/injected", "--add")
	gitClean(t, repo.work, "config", "remote.redirect.push", "refs/heads/main:refs/heads/injected")

	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	workflow := cleanTestWorkflow(repo.work, &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}})
	if err := workflow.Run(""); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got == initial {
		t.Fatal("configured upstream branch did not receive clean commit")
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "for-each-ref", "--format=%(refname)", "refs/heads"))); got != "refs/heads/main" {
		t.Fatalf("configured push refspec added unexpected refs: %q", got)
	}
	if got := string(gitClean(t, redirect, "for-each-ref", "--format=%(refname)", "refs/heads")); got != "" {
		t.Fatalf("custom push destination received a ref: %q", got)
	}
}

func TestCleanDetectsSamePathMutationDuringStagingAndNeverPushes(t *testing.T) {
	repo := newCleanRepo(t)
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	workflow := cleanTestWorkflow(repo.work, &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}})
	workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
		scope := cleanTestGitScope(repo.work, args)
		defer scope()
		if name == "git" && len(args) > 0 && args[0] == "add" {
			writeCleanFilePath(repo.work, "fix.txt", "concurrent same-path edit\n")
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = workdir
		return cmd.Output()
	}
	if err := workflow.Run(""); err == nil || !strings.Contains(err.Error(), "changed during staging") {
		t.Fatalf("same-path staging race error = %v", err)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initial {
		t.Fatalf("same-path staging race pushed: initial=%s remote=%s", initial, got)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD"))); got != initial {
		t.Fatalf("same-path staging race committed unexpectedly: initial=%s head=%s", initial, got)
	}
}

func TestCleanCommitsRenameAsSourceAndDestinationPaths(t *testing.T) {
	repo := newCleanRepo(t)
	writeCleanFile(t, repo.work, "before.txt", "rename me\n")
	gitClean(t, repo.work, "add", "before.txt")
	gitClean(t, repo.work, "commit", "-m", "add rename source")
	gitClean(t, repo.work, "push")
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	workflow := cleanTestWorkflow(repo.work, &cleanRenameOnlyAgent{work: repo.work})
	if err := workflow.Run(""); err != nil {
		t.Fatal(err)
	}
	want := "after.txt\nbefore.txt"
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "diff-tree", "--no-commit-id", "--name-only", "--no-renames", "-r", "HEAD"))); got != want {
		t.Fatalf("rename commit paths = %q, want %q", got, want)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got == initial {
		t.Fatal("rename commit was not pushed")
	}
}

func TestCleanFormatterEffectsAreIncludedWithinRunScope(t *testing.T) {
	repo := newCleanRepo(t)
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
	workflow := cleanTestWorkflow(repo.work, agent)
	workflow.Make = func(target string, _ ...string) error {
		scope := cleanTestGitScope(repo.work, nil)
		defer scope()
		if target != "fmt" {
			return nil
		}
		path := filepath.Join(agent.work, "fix.go")
		if err := os.WriteFile(path, []byte("package sample\nfunc value() {\nprintln(\"ok\")\nprintln(\"done\")\n}\n"), 0o644); err != nil {
			return err
		}
		cmd := exec.Command("gofmt", "-w", path)
		return cmd.Run()
	}
	if err := workflow.Run(""); err != nil {
		t.Fatal(err)
	}
	formatted, err := os.ReadFile(filepath.Join(repo.work, "fix.go"))
	if err != nil {
		t.Fatal(err)
	}
	if string(formatted) != "package sample\n\nfunc value() {\n\tprintln(\"ok\")\n\tprintln(\"done\")\n}\n" {
		t.Fatalf("formatter output = %q", formatted)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "show", "HEAD:fix.go"))); got != strings.TrimSpace(string(formatted)) {
		t.Fatalf("committed formatter output differs: %q", got)
	}
}

func TestCleanRunContextCancellationInterruptsAndPersistsWorkflow(t *testing.T) {
	repo := newCleanRepo(t)
	stateDir := filepath.Join(filepath.Dir(repo.work), "state")
	blockingAgent := &cleanBlockingAgent{started: make(chan struct{})}
	workflow := cleanTestWorkflow(repo.work, blockingAgent)
	workflow.Config.StateDir = stateDir
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- workflow.RunContext(ctx, "task") }()
	select {
	case <-blockingAgent.started:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("clean workflow agent did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "workflow interrupted") {
			t.Fatalf("canceled clean error = %v, want context-canceled interruption", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("clean workflow did not stop promptly after cancellation")
	}
	if status := workflowRunStatus(t, stateDir); status != "interrupted" {
		t.Fatalf("canceled clean run status = %q, want interrupted", status)
	}
}

func TestCleanVerificationCancellationInterruptsWithoutPublishing(t *testing.T) {
	repo := newCleanRepo(t)
	stateDir := filepath.Join(filepath.Dir(repo.work), "state")
	var output bytes.Buffer
	workflow := cleanTestWorkflow(repo.work, &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}})
	workflow.Config.StateDir = stateDir
	workflow.Out = &output
	var checkEvents []WorkflowEvent
	var terminalEvents []WorkflowEvent
	workflow.Observer = WorkflowObserverFunc(func(event WorkflowEvent) error {
		if event.Type == "check.started" || event.Type == "check.completed" {
			checkEvents = append(checkEvents, event)
		}
		if event.Type == "workflow.transition" && strings.Contains(event.Message, "interrupted") {
			terminalEvents = append(terminalEvents, event)
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var makeTargets []string
	workflow.Make = func(target string, _ ...string) error {
		scope := cleanTestGitScope(repo.work, nil)
		defer scope()
		makeTargets = append(makeTargets, target)
		if target == "fmt" {
			cancel()
		}
		return nil
	}
	publishCalls := 0
	workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
		scope := cleanTestGitScope(repo.work, args)
		defer scope()
		if name == "git" && len(args) > 0 {
			switch args[0] {
			case "add", "commit", "push":
				publishCalls++
			}
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = workdir
		return cmd.Output()
	}

	err := workflow.RunContext(ctx, "task")
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "clean verification interrupted") {
		t.Fatalf("canceled verification error = %v, want clean verification interruption", err)
	}
	if status := workflowRunStatus(t, stateDir); status != "interrupted" {
		t.Fatalf("canceled verification run status = %q, want interrupted", status)
	}
	if got := strings.Join(makeTargets, ","); got != "fmt" {
		t.Fatalf("verification targets after cancellation = %q, want only fmt", got)
	}
	if publishCalls != 0 {
		t.Fatalf("canceled verification staged or published work: git add/commit/push calls=%d", publishCalls)
	}
	if len(checkEvents) != 2 || checkEvents[0].Type != "check.started" || checkEvents[1].Type != "check.completed" || checkEvents[1].Outcome != "canceled" || checkEvents[1].ExitCode == nil || !checkEvents[0].StartedAt.Equal(checkEvents[1].StartedAt) || !checkEvents[1].StartedAt.Before(checkEvents[1].EndedAt) {
		t.Fatalf("canceled clean check lifecycle = %+v, want started and canceled completion", checkEvents)
	}
	_, runDir := readCleanState(t, stateDir)
	assertPipelineCheckEvents(t, runDir, []workflowEventRecord{
		{Type: "check.started", Command: []string{"make", "fmt"}, Transcript: "clean-check-01-fmt.log"},
		{Type: "check.completed", Outcome: "canceled", ExitCode: intPointer(-1), Command: []string{"make", "fmt"}, Transcript: "clean-check-01-fmt.log"},
	})
	var localInterrupted []workflowEventRecord
	for _, event := range readPipelineCheckEvents(t, runDir) {
		if event.Type == "workflow.transition" && strings.Contains(event.Message, "interrupted") {
			localInterrupted = append(localInterrupted, event)
		}
	}
	if len(localInterrupted) != 1 || len(terminalEvents) != 1 || terminalEvents[0].RunID != localInterrupted[0].RunID {
		t.Fatalf("interrupted terminal transition count local=%d observer=%+v; want exactly one matching event", len(localInterrupted), terminalEvents)
	}
	if strings.Contains(output.String(), "Clean verification succeeded") || strings.Contains(output.String(), "Clean changes committed") || strings.Contains(output.String(), "Clean completed") {
		t.Fatalf("canceled verification emitted a success line: %q", output.String())
	}
}

func TestCleanCancellationDuringGitCheckStopsBeforePublishing(t *testing.T) {
	repo := newCleanRepo(t)
	stateDir := filepath.Join(filepath.Dir(repo.work), "state")
	workflow := cleanTestWorkflow(repo.work, &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}})
	workflow.Config.StateDir = stateDir
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gitStarted := make(chan struct{})
	releaseGit := make(chan struct{})
	var startedOnce sync.Once
	var mu sync.Mutex
	var publishCalls []string
	workflow.GitAt = func(ctx context.Context, workdir, name string, args ...string) ([]byte, error) {
		scope := cleanTestGitScope(repo.work, args)
		defer scope()
		if name == "git" && len(args) > 0 {
			if args[0] == "diff" && len(args) == 5 && args[1] == "--name-only" && args[4] == "HEAD" {
				startedOnce.Do(func() { close(gitStarted) })
				select {
				case <-releaseGit:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			if args[0] == "add" || args[0] == "commit" || args[0] == "push" {
				mu.Lock()
				publishCalls = append(publishCalls, args[0])
				mu.Unlock()
			}
		}
		cmd := exec.CommandContext(ctx, name, args...)
		cmd.Dir = workdir
		return cmd.Output()
	}

	result := make(chan error, 1)
	go func() { result <- workflow.RunContext(ctx, "task") }()
	select {
	case <-gitStarted:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("clean workflow did not reach the blocking Git check")
	}
	cancel()
	close(releaseGit)
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Git check error = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("clean workflow did not stop after the blocking Git check returned")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(publishCalls) != 0 {
		t.Fatalf("canceled Git check continued into irreversible Git operations: %v", publishCalls)
	}
	if status := workflowRunStatus(t, stateDir); status != "interrupted" {
		t.Fatalf("canceled Git check status = %q, want interrupted", status)
	}
}

func TestCleanTerminalProgressUsesSpinnerAndClearsLine(t *testing.T) {
	repo := newCleanRepo(t)
	var output bytes.Buffer
	workflow := cleanTestWorkflow(repo.work, &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}})
	workflow.Terminal = true
	workflow.Out = &output

	if err := workflow.Run(""); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); !strings.Contains(got, "⠋ REVIEW\033[0m") || !strings.Contains(got, "RECENT ACTIVITY") || !strings.Contains(got, "\r\033[2K") {
		t.Fatalf("terminal progress spinner or cleanup missing from output: %q", got)
	}
}

func TestCleanPushesExistingLocalCommitsAlongsideCleanupCommit(t *testing.T) {
	repo := newCleanRepo(t)
	writeCleanFile(t, repo.work, "ahead.txt", "local work\n")
	gitClean(t, repo.work, "add", "ahead.txt")
	gitClean(t, repo.work, "commit", "-m", "existing local work")
	localHead := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	workflow := cleanTestWorkflow(repo.work, &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}})
	if err := workflow.Run(""); err != nil {
		t.Fatal(err)
	}
	finalHead := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD^"))); got != localHead {
		t.Fatalf("cleanup commit parent = %s, want captured local HEAD %s", got, localHead)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != finalHead {
		t.Fatalf("remote HEAD = %s, want pushed final HEAD %s", got, finalHead)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "show", "HEAD:ahead.txt"))); got != "local work" {
		t.Fatalf("pre-existing local commit content was lost: %q", got)
	}
}

func TestCleanPushesExistingLocalCommitsWithoutCreatingEmptyCommit(t *testing.T) {
	repo := newCleanRepo(t)
	writeCleanFile(t, repo.work, "ahead.txt", "local work\n")
	gitClean(t, repo.work, "add", "ahead.txt")
	gitClean(t, repo.work, "commit", "-m", "existing local work")
	localHead := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	workflow := cleanTestWorkflow(repo.work, &cleanNoopAgent{})
	if err := workflow.Run(""); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD"))); got != localHead {
		t.Fatalf("no-op cleanup changed local HEAD: got %s want %s", got, localHead)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != localHead {
		t.Fatalf("existing commits were not pushed: got %s want %s", got, localHead)
	}
}

func TestCleanVerificationFailureDoesNotPushExistingAheadCommits(t *testing.T) {
	repo := newCleanRepo(t)
	initialUpstream := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "@{upstream}")))
	writeCleanFile(t, repo.work, "ahead.txt", "local work\n")
	gitClean(t, repo.work, "add", "ahead.txt")
	gitClean(t, repo.work, "commit", "-m", "existing local work")
	localHead := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	workflow := cleanTestWorkflow(repo.work, &cleanNoopAgent{})
	workflow.Make = func(target string, _ ...string) error {
		scope := cleanTestGitScope(repo.work, nil)
		defer scope()
		if target == "test" {
			return fmt.Errorf("intentional test failure")
		}
		return nil
	}
	if err := workflow.Run(""); err == nil || !strings.Contains(err.Error(), "make test") {
		t.Fatalf("verification error = %v", err)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initialUpstream {
		t.Fatalf("verification failure pushed existing commits: remote=%s expected initial upstream %s", got, initialUpstream)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD"))); got != localHead {
		t.Fatalf("verification failure changed local HEAD: got %s want %s", got, localHead)
	}
}

func TestCleanSyncedNoopDoesNotCreateCommitOrPush(t *testing.T) {
	repo := newCleanRepo(t)
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	workflow := cleanTestWorkflow(repo.work, &cleanNoopAgent{})
	pushCalls := 0
	workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
		scope := cleanTestGitScope(repo.work, args)
		defer scope()
		if name == "git" && len(args) > 0 && args[0] == "push" {
			pushCalls++
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = workdir
		return cmd.Output()
	}
	if err := workflow.Run(""); err != nil {
		t.Fatal(err)
	}
	if pushCalls != 0 {
		t.Fatalf("synced no-op invoked push %d times", pushCalls)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD"))); got != initial {
		t.Fatalf("synced no-op created commit: got %s want %s", got, initial)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initial {
		t.Fatalf("synced no-op changed upstream: got %s want %s", got, initial)
	}
}

func TestCleanSuccessfulAgentStagesDoNotRequireEvaluatorProtocol(t *testing.T) {
	repo := newCleanRepo(t)
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{
		"review":   {"no evaluator verdict"},
		"fix":      {"no evaluator verdict"},
		"document": {"no evaluator verdict"},
	}}
	workflow := cleanTestWorkflow(repo.work, agent)
	if err := workflow.Run(""); err != nil {
		t.Fatalf("successful agent stages should proceed without evaluator output: %v", err)
	}
	if got, want := strings.Join(agent.calls, ","), "review,fix,document"; got != want {
		t.Fatalf("clean pipeline calls = %s, want %s", got, want)
	}
}

func TestCleanCommitsAndPushesOnlyRunChanges(t *testing.T) {
	repo := newCleanRepo(t)
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
	workflow := cleanTestWorkflow(repo.work, agent)
	if err := workflow.Run(""); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(agent.calls, ","), "review,fix,document"; got != want {
		t.Fatalf("clean pipeline calls = %s, want %s", got, want)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "show", "--format=", "--name-only", "HEAD"))); got != "README.md\nfix.txt" {
		t.Fatalf("commit paths = %q", got)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "log", "-1", "--format=%s", "refs/heads/main"))); got != "chore: clean and verify changes" {
		t.Fatalf("upstream commit = %q", got)
	}
	if got := string(gitClean(t, repo.work, "status", "--porcelain")); got != "" {
		t.Fatalf("worktree remains dirty after successful run: %q", got)
	}
}

func TestCleanGateDoesNotRetryFailedStageOrPromptForRetry(t *testing.T) {
	repo := newCleanRepo(t)
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{"review": {"ERROR: unavailable", "must not execute"}}}
	var output bytes.Buffer
	workflow := cleanTestWorkflow(repo.work, agent)
	workflow.Gate = true
	workflow.In = strings.NewReader(strings.Repeat("yes\n", 8))
	workflow.Out = &output
	if err := workflow.Run(""); err == nil || !strings.Contains(err.Error(), "review agent failed") {
		t.Fatalf("failed clean review should end workflow: %v", err)
	}
	if len(agent.calls) != 1 || agent.calls[0] != "review" {
		t.Fatalf("clean agent calls = %v, want one review invocation", agent.calls)
	}
	if strings.Contains(output.String(), "Type exactly yes") || strings.Contains(output.String(), "Retry this stage?") {
		t.Fatalf("failed stage emitted retry/approval prompt: %s", output.String())
	}
}

func TestCleanOwnsTerminalStateUntilChecksAndReportsCheckOutput(t *testing.T) {
	repo := newCleanRepo(t)
	writeCleanFile(t, repo.work, "dirty.txt", "preserve safe mode\n")
	stateDir := filepath.Join(filepath.Dir(repo.work), "state")
	makefile := "fmt test vet:\n\t@printf 'check-output-%s\\n' \"$@\"\n"
	writeCleanFile(t, repo.work, "Makefile", makefile)
	store, err := NewJobStore(filepath.Join(filepath.Dir(repo.work), "jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "clean-job", Type: "clean", Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession("clean-job", "clean-session", "running"); err != nil {
		t.Fatal(err)
	}
	observer := JobSessionObserver{Store: store, JobID: "clean-job", SessionID: "clean-session"}
	var output bytes.Buffer
	workflow := cleanTestWorkflow(repo.work, &cleanNoopAgent{})
	workflow.Config.StateDir = stateDir
	workflow.Out = &output
	workflow.Make = nil
	var observedChecks []WorkflowEvent
	workflow.Observer = WorkflowObserverFunc(func(event WorkflowEvent) error {
		if event.Type == "check.started" || event.Type == "check.completed" {
			observedChecks = append(observedChecks, event)
			if status := workflowRunStatus(t, stateDir); status != "running" {
				t.Errorf("run status at %s = %q, want running until checks finish", event.Stage, status)
			}
			if event.Message != "" || event.Transcript == "" || len(event.Command) != 2 || event.Command[0] != "make" || event.StartedAt.IsZero() {
				t.Errorf("check event lacks structured metadata or includes output: %+v", event)
			}
			if event.Type == "check.started" {
				if event.Outcome != "" || event.ExitCode != nil || !event.EndedAt.IsZero() {
					t.Errorf("check start contains completion fields: %+v", event)
				}
			} else if event.Outcome != "success" || event.ExitCode == nil || *event.ExitCode != 0 || !event.StartedAt.Before(event.EndedAt) {
				t.Errorf("check completion metadata = %+v", event)
			}
		}
		return observer.ObserveWorkflowEvent(event)
	})
	if err := workflow.Run(""); err != nil {
		t.Fatal(err)
	}
	if status := workflowRunStatus(t, stateDir); status != "complete" {
		t.Fatalf("clean status after checks = %q, want complete", status)
	}
	events, err := store.SessionEvents("clean-job", "clean-session")
	if err != nil {
		t.Fatal(err)
	}
	var checkCompletions []SessionEvent
	for _, event := range events {
		if event.Type == "check.completed" {
			checkCompletions = append(checkCompletions, event)
		}
	}
	if len(checkCompletions) != 3 {
		t.Fatalf("check completion events = %d, want three: %+v", len(checkCompletions), events)
	}
	for i, target := range []string{"fmt", "test", "vet"} {
		if strings.Contains(checkCompletions[i].Message, "check-output-") {
			t.Errorf("make %s observer event leaked output: %+v", target, checkCompletions[i])
		}
		if checkCompletions[i].Type != "check.completed" {
			t.Errorf("make %s event has type %q", target, checkCompletions[i].Type)
		}
	}
	state, runDir := readCleanState(t, stateDir)
	if len(state.Checks) != 3 {
		t.Fatalf("persisted checks = %d, want three", len(state.Checks))
	}
	var localEvents []workflowEventRecord
	for _, event := range readPipelineCheckEvents(t, runDir) {
		if event.Type == "check.started" || event.Type == "check.completed" {
			localEvents = append(localEvents, event)
		}
	}
	if len(localEvents) != 6 {
		t.Fatalf("local check lifecycle events = %d, want six: %+v", len(localEvents), localEvents)
	}
	if len(observedChecks) != 6 {
		t.Fatalf("observer check lifecycle events = %d, want six: %+v", len(observedChecks), observedChecks)
	}
	for index, target := range []string{"fmt", "test", "vet"} {
		started, completed := localEvents[index*2], localEvents[index*2+1]
		check := state.Checks[index]
		logName := fmt.Sprintf("clean-check-%02d-%s.log", index+1, target)
		observedStarted, observedCompleted := observedChecks[index*2], observedChecks[index*2+1]
		if started.Type != "check.started" || completed.Type != "check.completed" || started.Command[1] != target || completed.Command[1] != target || started.Transcript != logName || completed.Transcript != logName || started.Message != "" || completed.Message != "" || completed.Outcome != "success" || completed.ExitCode == nil || *completed.ExitCode != 0 || started.StartedAt == nil || completed.StartedAt == nil || completed.EndedAt == nil || !started.StartedAt.Equal(check.StartedAt) || !completed.EndedAt.Equal(check.EndedAt) {
			t.Errorf("local check lifecycle %s mismatch: started=%+v completed=%+v result=%+v", target, started, completed, check)
		}
		if observedStarted.Type != started.Type || observedCompleted.Type != completed.Type || strings.Join(observedStarted.Command, "\x00") != strings.Join(started.Command, "\x00") || observedCompleted.Outcome != completed.Outcome || observedCompleted.ExitCode == nil || *observedCompleted.ExitCode != 0 || observedStarted.Transcript != started.Transcript || observedCompleted.Transcript != completed.Transcript || observedStarted.Message != "" || observedCompleted.Message != "" {
			t.Errorf("observer check lifecycle %s differs from structured local events: %+v / %+v", target, observedStarted, observedCompleted)
		}
	}
	localData, err := os.ReadFile(filepath.Join(runDir, "workflow-events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(localData), "check-output-") {
		t.Fatalf("local workflow events leaked command output: %s", localData)
	}
}

func TestCleanRejectsUnrelatedMutationDuringChecks(t *testing.T) {
	repo := newCleanRepo(t)
	initial := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main")))
	workflow := cleanTestWorkflow(repo.work, &cleanNoopAgent{})
	workflow.Make = func(target string, _ ...string) error {
		scope := cleanTestGitScope(repo.work, nil)
		defer scope()
		if target == "test" {
			writeCleanFilePath(repo.work, "external-work.txt", "must not be published\n")
		}
		return nil
	}
	if err := workflow.Run(""); err == nil || !strings.Contains(err.Error(), "unexpected worktree paths") {
		t.Fatalf("clean error = %v, want fail-closed rejection for an unrelated mutation", err)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initial {
		t.Fatalf("external work was published: initial=%s remote=%s", initial, got)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD"))); got != initial {
		t.Fatalf("external work was committed: initial=%s local=%s", initial, got)
	}
}

func TestCleanRequiresTerminalForPublicationApproval(t *testing.T) {
	repo := newCleanRepo(t)
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	workflow := cleanTestWorkflow(repo.work, &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}})
	workflow.Terminal = false
	workflow.In = strings.NewReader("yes\n")
	gitOperations := []string(nil)
	workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
		scope := cleanTestGitScope(repo.work, args)
		defer scope()
		if name == "git" && len(args) > 0 && (args[0] == "commit" || args[0] == "push") {
			gitOperations = append(gitOperations, args[0])
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = workdir
		return cmd.Output()
	}

	err := workflow.Run("")
	if err == nil || !strings.Contains(err.Error(), "requires an interactive terminal") {
		t.Fatalf("clean error = %v, want actionable terminal requirement", err)
	}
	if len(gitOperations) != 0 {
		t.Fatalf("non-terminal yes attempted publication operations: %v", gitOperations)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD"))); got != initial {
		t.Fatalf("non-terminal approval created a commit: got %s want %s", got, initial)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initial {
		t.Fatalf("non-terminal approval pushed changes: got %s want %s", got, initial)
	}
}

func TestCleanRequiresApprovalBeforePublishingAndRejectsWithoutCommit(t *testing.T) {
	repo := newCleanRepo(t)
	initial := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main")))
	workflow := cleanTestWorkflow(repo.work, &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}})
	workflow.In = strings.NewReader("no\n")
	var output bytes.Buffer
	workflow.Out = &output
	if err := workflow.Run(""); err == nil || !strings.Contains(err.Error(), "not approved") {
		t.Fatalf("clean error = %v, want publication rejection", err)
	}
	if !strings.Contains(output.String(), "Approve to commit and push pristine clean changes") {
		t.Fatalf("mandatory approval prompt missing: %q", output.String())
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main"))); got != initial {
		t.Fatalf("rejected clean advanced remote: initial=%s remote=%s", initial, got)
	}
	if got := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD"))); got != initial {
		t.Fatalf("rejected clean created a commit: initial=%s local=%s", initial, got)
	}
}

func TestCleanPersistsCompleteCheckLogsAndStaysRunningThroughPublication(t *testing.T) {
	repo := newCleanRepo(t)
	stateDir := filepath.Join(filepath.Dir(repo.work), "state")
	writeCleanFile(t, repo.work, "Makefile", "fmt test vet:\n\t@i=0; while [ $$i -lt 1200 ]; do printf 'large-check-transcript\\n'; i=$$((i+1)); done\n")
	workflow := cleanTestWorkflow(repo.work, &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}})
	workflow.Config.StateDir = stateDir
	workflow.In = strings.NewReader("yes\n")
	var output bytes.Buffer
	workflow.Out = &output
	workflow.Make = nil
	workflow.Observer = WorkflowObserverFunc(func(event WorkflowEvent) error {
		if event.Type == "check.started" || event.Type == "check.completed" {
			if status := workflowRunStatus(t, stateDir); status != "running" {
				t.Errorf("run status at %s = %q, want running", event.Type, status)
			}
		}
		return nil
	})
	workflow.GitAt = func(_ context.Context, workdir, name string, args ...string) ([]byte, error) {
		scope := cleanTestGitScope(repo.work, args)
		defer scope()
		if name == "git" && len(args) > 0 && args[0] == "push" {
			if status := workflowRunStatus(t, stateDir); status != "running" {
				t.Errorf("run status before push = %q, want running", status)
			}
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = workdir
		return cmd.Output()
	}
	if err := workflow.Run(""); err != nil {
		t.Fatal(err)
	}
	state, runDir := readCleanState(t, stateDir)
	if state.Status != "complete" || len(state.Checks) != 3 {
		t.Fatalf("final state = status %q, checks %d; want complete and three results", state.Status, len(state.Checks))
	}
	for _, check := range state.Checks {
		data, err := os.ReadFile(filepath.Join(runDir, check.Log))
		if err != nil {
			t.Fatal(err)
		}
		if len(data) <= evaluatorOutputLimit || !strings.Contains(string(data), "large-check-transcript") {
			t.Errorf("make %s log length/content = %d bytes, want full transcript over %d bytes", check.Command[len(check.Command)-1], len(data), evaluatorOutputLimit)
		}
	}
}

func TestCleanCheckOpenFailurePersistsFailureLifecycle(t *testing.T) {
	repo := newCleanRepo(t)
	writeCleanFile(t, repo.work, "dirty.txt", "safe mode\n")
	stateDir := filepath.Join(filepath.Dir(repo.work), "state")
	openErr := errors.New("clean transcript unavailable")
	var observed []WorkflowEvent
	workflow := cleanTestWorkflow(repo.work, &cleanNoopAgent{})
	workflow.Config.StateDir = stateDir
	workflow.OpenCheckLog = func(string) (*os.File, error) { return nil, openErr }
	workflow.Observer = WorkflowObserverFunc(func(event WorkflowEvent) error {
		if strings.HasPrefix(event.Type, "check.") {
			observed = append(observed, event)
		}
		return nil
	})

	err := workflow.Run("")
	if !errors.Is(err, openErr) {
		t.Fatalf("clean error = %v, want transcript open failure", err)
	}
	state, runDir := readCleanState(t, stateDir)
	if state.Status != "failed" || len(state.Checks) != 1 || state.Checks[0].ExitCode == 0 || state.Checks[0].ExitCode != -1 {
		t.Fatalf("clean state = %+v, want failed check with nonzero result", state)
	}
	wantEvents := []workflowEventRecord{
		{Type: "check.started", Command: []string{"make", "fmt"}, Transcript: "clean-check-01-fmt.log", StartedAt: timePointer(state.Checks[0].StartedAt)},
		{Type: "check.completed", Outcome: "failure", ExitCode: intPointer(-1), Command: []string{"make", "fmt"}, Transcript: "clean-check-01-fmt.log", StartedAt: timePointer(state.Checks[0].StartedAt), EndedAt: timePointer(state.Checks[0].EndedAt)},
	}
	assertPipelineCheckEvents(t, runDir, wantEvents)
	if len(observed) != 2 || observed[0].Type != "check.started" || observed[1].Type != "check.completed" || observed[1].Outcome != "failure" || observed[1].ExitCode == nil || *observed[1].ExitCode != -1 {
		t.Fatalf("observer check lifecycle = %+v, want started and failed completion", observed)
	}
}

func TestCleanRunOutputUsesExactRunPathWhenDisplayedPathIsTruncated(t *testing.T) {
	repo := newCleanRepo(t)
	writeCleanFile(t, repo.work, "dirty.txt", "safe mode\n")
	stateRoot := filepath.Join(filepath.Dir(repo.work), strings.Repeat("state-dir-", 8), strings.Repeat("nested-", 8), "state")
	if err := os.MkdirAll(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	workflow := cleanTestWorkflow(repo.work, &cleanNoopAgent{})
	workflow.Config.StateDir = stateRoot
	workflow.Out = &output
	if err := workflow.Run(""); err != nil {
		t.Fatalf("clean with long run directory failed: %v", err)
	}
	state, runDir := readCleanState(t, stateRoot)
	if len(state.Checks) != 3 {
		t.Fatalf("persisted checks = %d, want all checks", len(state.Checks))
	}
	for _, check := range state.Checks {
		if _, err := os.Stat(filepath.Join(runDir, check.Log)); err != nil {
			t.Errorf("check transcript missing at exact run path %q: %v", filepath.Join(runDir, check.Log), err)
		}
	}
	var displayedRun string
	for _, line := range strings.Split(output.String(), "\n") {
		if strings.HasPrefix(line, "Run: ") {
			displayedRun = strings.TrimPrefix(line, "Run: ")
			break
		}
	}
	if displayedRun == "" || displayedRun == runDir || len([]rune(displayedRun)) >= len([]rune(runDir)) || len([]rune(displayedRun)) > progressLogLineLimit {
		t.Fatalf("long run path was not safely truncated in progress output: got %q for %q", displayedRun, runDir)
	}
}

func TestCleanCheckCloseFailurePersistsFailureAndTerminalEvent(t *testing.T) {
	repo := newCleanRepo(t)
	writeCleanFile(t, repo.work, "dirty.txt", "safe mode\n")
	stateDir := filepath.Join(filepath.Dir(repo.work), "state")
	closeErr := errors.New("clean transcript close failed")
	oldClose := cleanCheckCloseFile
	cleanCheckCloseFile = func(file *os.File) error {
		if err := file.Close(); err != nil {
			return err
		}
		return closeErr
	}
	t.Cleanup(func() { cleanCheckCloseFile = oldClose })
	var observed []WorkflowEvent
	workflow := cleanTestWorkflow(repo.work, &cleanNoopAgent{})
	workflow.Config.StateDir = stateDir
	workflow.Observer = WorkflowObserverFunc(func(event WorkflowEvent) error {
		observed = append(observed, event)
		return nil
	})

	err := workflow.Run("")
	if !errors.Is(err, closeErr) {
		t.Fatalf("clean error = %v, want transcript close failure", err)
	}
	state, runDir := readCleanState(t, stateDir)
	if state.Status != "failed" || len(state.Checks) != 1 || state.Checks[0].ExitCode != -1 {
		t.Fatalf("clean state = %+v, want persisted failed check", state)
	}
	var checkEvents []workflowEventRecord
	var localTerminalEvent bool
	for _, event := range readPipelineCheckEvents(t, runDir) {
		if strings.HasPrefix(event.Type, "check.") {
			checkEvents = append(checkEvents, event)
		}
		if event.Type == "workflow.transition" && strings.Contains(event.Message, closeErr.Error()) {
			localTerminalEvent = true
		}
	}
	if len(checkEvents) != 2 || checkEvents[0].Type != "check.started" || checkEvents[1].Type != "check.completed" || checkEvents[1].Outcome != "failure" || checkEvents[1].ExitCode == nil || *checkEvents[1].ExitCode != -1 {
		t.Fatalf("local check lifecycle = %+v, want failed completion", checkEvents)
	}
	var observerCompletion, terminalEvent bool
	for _, event := range observed {
		if event.Type == "check.completed" && event.Outcome == "failure" && event.ExitCode != nil && *event.ExitCode == -1 {
			observerCompletion = true
		}
		if event.Type == "workflow.transition" && strings.Contains(event.Message, closeErr.Error()) {
			terminalEvent = true
		}
	}
	if !observerCompletion || !terminalEvent || !localTerminalEvent {
		t.Fatalf("missing observer/local check failure or failed terminal transition: observed=%+v localTerminal=%t", observed, localTerminalEvent)
	}
}

func TestCleanVerificationFailureNeverPushes(t *testing.T) {
	repo := newCleanRepo(t)
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	stateDir := filepath.Join(filepath.Dir(repo.work), "state")
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
	workflow := cleanTestWorkflow(repo.work, agent)
	workflow.Config.StateDir = stateDir
	var checkEvents []WorkflowEvent
	workflow.Observer = WorkflowObserverFunc(func(event WorkflowEvent) error {
		if event.Type == "check.started" || event.Type == "check.completed" {
			checkEvents = append(checkEvents, event)
		}
		return nil
	})
	workflow.Make = func(target string, _ ...string) error {
		scope := cleanTestGitScope(repo.work, nil)
		defer scope()
		if target == "test" {
			return fmt.Errorf("intentional test failure")
		}
		return nil
	}
	if err := workflow.Run(""); err == nil || !strings.Contains(err.Error(), "make test") {
		t.Fatalf("verification error = %v", err)
	}
	if status := workflowRunStatus(t, stateDir); status != "failed" {
		t.Fatalf("failed clean verification status = %q, want failed", status)
	}
	if len(checkEvents) != 4 || checkEvents[0].Type != "check.started" || checkEvents[1].Type != "check.completed" || checkEvents[1].Outcome != "success" || checkEvents[2].Type != "check.started" || checkEvents[3].Type != "check.completed" || checkEvents[3].Outcome != "failure" || checkEvents[3].ExitCode == nil || *checkEvents[3].ExitCode != -1 || checkEvents[3].Transcript != "clean-check-02-test.log" {
		t.Fatalf("clean success/failure observer lifecycle = %+v", checkEvents)
	}
	_, runDir := readCleanState(t, stateDir)
	assertPipelineCheckEvents(t, runDir, []workflowEventRecord{
		{Type: "check.started", Command: []string{"make", "fmt"}, Transcript: "clean-check-01-fmt.log"},
		{Type: "check.completed", Outcome: "success", ExitCode: intPointer(0), Command: []string{"make", "fmt"}, Transcript: "clean-check-01-fmt.log"},
		{Type: "check.started", Command: []string{"make", "test"}, Transcript: "clean-check-02-test.log"},
		{Type: "check.completed", Outcome: "failure", ExitCode: intPointer(-1), Command: []string{"make", "test"}, Transcript: "clean-check-02-test.log"},
	})
	remote := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main")))
	if remote != initial {
		t.Fatalf("verification failure pushed changes: initial=%s remote=%s", initial, remote)
	}
}

func TestCleanNormalPushRejectsRemoteDivergence(t *testing.T) {
	repo := newCleanRepo(t)
	other := filepath.Join(t.TempDir(), "other")
	gitClean(t, "", "clone", repo.bare, other)
	gitClean(t, other, "config", "user.name", "Clean Test")
	gitClean(t, other, "config", "user.email", "clean@example.test")
	writeCleanFile(t, other, "remote.txt", "remote\n")
	gitClean(t, other, "add", "remote.txt")
	gitClean(t, other, "commit", "-m", "remote advance")
	gitClean(t, other, "push", "origin", "HEAD:main")

	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
	workflow := cleanTestWorkflow(repo.work, agent)
	if err := workflow.Run(""); err == nil || !strings.Contains(err.Error(), "push clean commit") {
		t.Fatalf("diverged normal push error = %v", err)
	}
	remoteHead := strings.TrimSpace(string(gitClean(t, repo.bare, "rev-parse", "refs/heads/main")))
	remotePaths := strings.TrimSpace(string(gitClean(t, repo.bare, "show", "--format=", "--name-only", "refs/heads/main")))
	if remotePaths != "remote.txt" {
		t.Fatalf("failed push changed remote contents: head=%s paths=%q", remoteHead, remotePaths)
	}
}

func TestTransferCleanPathsRollsBackPartialTrackedAndUntrackedChanges(t *testing.T) {
	target := newCleanRepo(t)
	source := cloneCleanRepo(t, target.bare)
	writeCleanFilePathRaw(source, "README.md", "tracked source change\n")
	writeCleanFilePathRaw(source, "new.txt", "new isolated file\n")
	writeCleanFilePathRaw(source, "z-existing.txt", "isolated replacement attempt\n")
	writeCleanFilePathRaw(target.work, "z-existing.txt", "preserve pre-existing file\n")
	paths := []string{"README.md", "new.txt", "z-existing.txt"}
	expected := map[string]cleanFileSnapshot{
		"README.md":      {mode: "100644", content: []byte("tracked source change\n")},
		"new.txt":        {mode: "100644", content: []byte("new isolated file\n")},
		"z-existing.txt": {mode: "100644", content: []byte("isolated replacement attempt\n")},
	}
	if err := transferCleanPaths(target.work, source, paths, expected); err == nil {
		t.Fatal("transfer unexpectedly replaced a pre-existing destination")
	}
	if got := string(mustReadCleanFile(t, filepath.Join(target.work, "README.md"))); got != "initial\n" {
		t.Fatalf("tracked patch remained after transfer failure: %q", got)
	}
	if _, err := os.Lstat(filepath.Join(target.work, "new.txt")); !os.IsNotExist(err) {
		t.Fatalf("partial untracked addition remains: err=%v", err)
	}
	if got := string(mustReadCleanFile(t, filepath.Join(target.work, "z-existing.txt"))); got != "preserve pre-existing file\n" {
		t.Fatalf("pre-existing destination changed: %q", got)
	}
}

func mustReadCleanFile(t *testing.T, path string) []byte {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return content
}

type cleanPristineTransferAgent struct{}

func (*cleanPristineTransferAgent) Run(stage, _, _, workdir, log string) error {
	if stage == "fix" {
		if err := os.Rename(filepath.Join(workdir, "old.txt"), filepath.Join(workdir, "new.txt")); err != nil {
			return err
		}
		if err := os.Remove(filepath.Join(workdir, "remove.txt")); err != nil {
			return err
		}
		writeCleanFilePath(workdir, "new.txt", "renamed content\n")
	}
	if stage == "document" {
		writeCleanFilePath(workdir, "README.md", "modified in isolated worktree\n")
	}
	return os.WriteFile(log, []byte("PASS\n"), 0o600)
}

func (*cleanPristineTransferAgent) RunWithContext(ctx context.Context, stage, prompt, task, workdir, log string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return (&cleanPristineTransferAgent{}).Run(stage, prompt, task, workdir, log)
}

func (*cleanPristineTransferAgent) RunWithOutputContext(ctx context.Context, stage, prompt, task, workdir, log string) (string, error) {
	if err := (&cleanPristineTransferAgent{}).RunWithContext(ctx, stage, prompt, task, workdir, log); err != nil {
		return "", err
	}
	return "PASS\n", nil
}

type cleanIsolatedWorkspaceAgent struct {
	target         string
	externalChange string
	workdir        string
}

func (a *cleanIsolatedWorkspaceAgent) Run(stage, _, _, workdir, log string) error {
	a.workdir = workdir
	if stage == "review" {
		switch a.externalChange {
		case "added":
			writeCleanFilePathRaw(a.target, "external.txt", "user addition\n")
		case "modified":
			writeCleanFilePathRaw(a.target, "README.md", "user modification\n")
		}
	}
	if stage == "fix" {
		writeCleanFilePath(workdir, "fix.txt", "created in isolated workspace\n")
	}
	if stage == "document" {
		writeCleanFilePath(workdir, "README.md", "updated by isolated clean\n")
	}
	return os.WriteFile(log, []byte("PASS\n"), 0o600)
}

func (a *cleanIsolatedWorkspaceAgent) RunWithContext(ctx context.Context, stage, prompt, task, workdir, log string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.Run(stage, prompt, task, workdir, log)
}

func (a *cleanIsolatedWorkspaceAgent) RunWithOutputContext(ctx context.Context, stage, prompt, task, workdir, log string) (string, error) {
	if err := a.RunWithContext(ctx, stage, prompt, task, workdir, log); err != nil {
		return "", err
	}
	return "PASS\n", nil
}

type cleanRepo struct {
	work string
	bare string
}

func newCleanRepo(t *testing.T) cleanRepo {
	t.Helper()
	base := t.TempDir()
	bare := filepath.Join(base, "remote.git")
	work := filepath.Join(base, "work")
	gitClean(t, "", "init", "--bare", "--initial-branch=main", bare)
	gitClean(t, "", "clone", bare, work)
	gitClean(t, work, "config", "user.name", "Clean Test")
	gitClean(t, work, "config", "user.email", "clean@example.test")
	writeCleanFile(t, work, "README.md", "initial\n")
	gitClean(t, work, "add", "README.md")
	gitClean(t, work, "commit", "-m", "initial")
	gitClean(t, work, "push", "-u", "origin", "main")
	return cleanRepo{work: work, bare: bare}
}

func cloneCleanRepo(t *testing.T, bare string) string {
	t.Helper()
	work := filepath.Join(t.TempDir(), "clone")
	gitClean(t, "", "clone", bare, work)
	gitClean(t, work, "config", "user.name", "Clean Test")
	gitClean(t, work, "config", "user.email", "clean@example.test")
	return work
}

func readCleanState(t *testing.T, stateDir string) (State, string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(stateDir, "runs"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("clean run entries = %v, %v; want one", entries, err)
	}
	runDir := filepath.Join(stateDir, "runs", entries[0].Name())
	data, err := os.ReadFile(filepath.Join(runDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	return state, runDir
}

func hasCleanPushLease(args []string, ref string) bool {
	for _, arg := range args {
		if arg == "--force-with-lease="+ref+":" {
			return true
		}
	}
	return false
}

func cleanTestWorkflow(work string, agent Agent) CleanWorkflow {
	return CleanWorkflow{
		Agent: stdoutProtocolTestAgent{Agent: agent}, Config: Config{StateDir: filepath.Join(filepath.Dir(work), "state")},
		In: strings.NewReader("yes\n"), Out: io.Discard, Workdir: work, Terminal: true,
		Make: func(string, ...string) error { return nil },
		SetupWorktree: func(ctx context.Context, target, head string) (string, func() error, error) {
			path, cleanup, err := createCleanWorktree(ctx, target, head)
			if err != nil {
				return "", nil, err
			}
			cleanTestWorkdirs.Store(filepath.Clean(target), path)

			wrappedCleanup := func() error {
				defer cleanTestWorkdirs.Delete(filepath.Clean(target))
				defer cleanTestPublishing.Delete(filepath.Clean(target))
				return cleanup()
			}
			switch a := agent.(type) {
			case *cleanWriterAgent:
				a.work = path
			case *cleanRenameOnlyAgent:
				a.work = path
			case *cleanDestinationRaceAgent:
				a.work = path
				a.target = target
			}
			return path, wrappedCleanup, nil
		},
	}
}

var cleanTestWorkdirs sync.Map
var cleanTestWorkdirScopes sync.Map
var cleanTestPublishing sync.Map

func cleanTestGitScope(target string, args []string) func() {
	key := filepath.Clean(target)
	if _, ok := cleanTestWorkdirs.Load(key); !ok {
		return func() {}
	}
	if len(args) > 0 && args[0] == "add" {
		cleanTestPublishing.Store(key, true)
		return func() {}
	}
	if _, publishing := cleanTestPublishing.Load(key); publishing {
		return func() {}
	}
	path, _ := cleanTestWorkdirs.Load(key)
	previous, hadPrevious := cleanTestWorkdirScopes.Load(key)
	cleanTestWorkdirScopes.Store(key, path)
	return func() {
		if hadPrevious {
			cleanTestWorkdirScopes.Store(key, previous)
		} else {
			cleanTestWorkdirScopes.Delete(key)
		}
	}
}

func cleanTestWorkdir(target string) string {
	key := filepath.Clean(target)
	if _, publishing := cleanTestPublishing.Load(key); publishing {
		return target
	}
	if path, active := cleanTestWorkdirScopes.Load(key); active {
		return path.(string)
	}
	return target
}

type stdoutProtocolTestAgent struct {
	Agent
}

func (a stdoutProtocolTestAgent) RunWithContext(ctx context.Context, stage, prompt, task, workdir, logPath string) error {
	return runAgentWithContext(ctx, a.Agent, stage, prompt, task, workdir, logPath)
}

func (a stdoutProtocolTestAgent) RunWithOutputContext(ctx context.Context, stage, prompt, task, workdir, logPath string) (string, error) {
	err := runAgentWithContext(ctx, a.Agent, stage, prompt, task, workdir, logPath)
	output, readErr := os.ReadFile(logPath)
	if readErr != nil {
		return "", readErr
	}
	return string(output), err
}

type cleanRecordingAgent struct {
	out   *bytes.Buffer
	calls []string
}

func (a *cleanRecordingAgent) Run(stage, _, _, _, log string) error {
	warning := strings.ToLower(a.out.String())
	if !strings.Contains(warning, "dirty") || !strings.Contains(warning, "may") || !strings.Contains(warning, "agent") || !strings.Contains(warning, "format") {
		return fmt.Errorf("dirty-worktree warning was not shown before %s agent stage", stage)
	}
	a.calls = append(a.calls, stage)
	return os.WriteFile(log, []byte("PASS\n"), 0o600)
}

type cleanDestinationRaceAgent struct {
	work   string
	target string
	calls  []string
}

func (a *cleanDestinationRaceAgent) Run(stage, _, _, _, log string) error {
	a.calls = append(a.calls, stage)
	if stage == "review" {
		cmd := exec.Command("git", "push", "origin", "HEAD:refs/heads/clean-topic")
		cmd.Dir = a.target
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("create competing destination: %w: %s", err, out)
		}
	}
	if stage == "fix" {
		writeCleanFilePath(a.work, "fix.txt", "fixed\n")
	}
	if stage == "document" {
		writeCleanFilePath(a.work, "README.md", "updated by clean\n")
	}
	return os.WriteFile(log, []byte("PASS\n"), 0o600)
}

type cleanBlockingAgent struct {
	started chan struct{}
}

func (a *cleanBlockingAgent) Run(stage, prompt, task, workdir, log string) error {
	return a.RunWithContext(context.Background(), stage, prompt, task, workdir, log)
}

func (a *cleanBlockingAgent) RunWithContext(ctx context.Context, _, _, _, _, _ string) error {
	select {
	case <-a.started:
	default:
		close(a.started)
	}
	<-ctx.Done()
	return ctx.Err()
}

type cleanNoopAgent struct{}

func (*cleanNoopAgent) Run(_, _, _, _, log string) error {
	return os.WriteFile(log, []byte("PASS\n"), 0o600)
}

type cleanRenameOnlyAgent struct {
	work string
}

func (a *cleanRenameOnlyAgent) Run(stage, _, _, _, log string) error {
	if stage == "fix" {
		if err := os.Rename(filepath.Join(a.work, "before.txt"), filepath.Join(a.work, "after.txt")); err != nil {
			return err
		}
	}
	return os.WriteFile(log, []byte("PASS\n"), 0o600)
}

type cleanWriterAgent struct {
	work    string
	outputs map[string][]string
	calls   []string
}

func (a *cleanWriterAgent) Run(stage, _, _, _, log string) error {
	a.calls = append(a.calls, stage)
	if stage == "fix" {
		writeCleanFilePath(a.work, "fix.txt", "fixed\n")
	}
	if stage == "document" {
		writeCleanFilePath(a.work, "README.md", "updated by clean\n")
	}
	output := "PASS\n"
	if queued := a.outputs[stage]; len(queued) != 0 {
		output = queued[0]
		a.outputs[stage] = queued[1:]
	}
	if strings.HasPrefix(output, "ERROR:") {
		return errors.New(strings.TrimPrefix(output, "ERROR:"))
	}
	return os.WriteFile(log, []byte(output), 0o600)
}

func gitClean(t *testing.T, dir string, args ...string) []byte {
	t.Helper()
	if dir == "" {
		dir = t.TempDir()
	}
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func writeCleanFile(t *testing.T, dir, name, content string) {
	t.Helper()
	writeCleanFilePath(dir, name, content)
}

func writeCleanFilePath(dir, name, content string) {
	writeCleanFilePathRaw(cleanTestWorkdir(dir), name, content)
}

func writeCleanFilePathRaw(dir, name, content string) {
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		panic(fmt.Sprintf("create test file directory: %v", err))
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		panic(fmt.Sprintf("write test file: %v", err))
	}
}
