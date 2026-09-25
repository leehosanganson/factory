package factory

import (
	"bytes"
	"context"
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
		makeTargets = append(makeTargets, target)
		if !strings.Contains(strings.ToLower(output.String()), "dirty") || !strings.Contains(strings.ToLower(output.String()), "may") {
			t.Fatalf("dirty-worktree warning was not shown before make %s: %q", target, output.String())
		}
		return nil
	}
	addCalls, pushCalls, commitCalls := 0, 0, 0
	workflow.Git = func(name string, args ...string) ([]byte, error) {
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
		cmd.Dir = repo.work
		return cmd.Output()
	}

	if err := workflow.Run(""); err != nil {
		t.Fatalf("dirty safe mode failed: %v", err)
	}
	if got, want := strings.Join(agent.calls, ","), "review,evaluate,fix,evaluate,document,evaluate"; got != want {
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
				if target == "fmt" {
					if mode == "upstream-appears" {
						gitClean(t, repo.work, "branch", "--set-upstream-to=origin/main")
					} else {
						gitClean(t, repo.work, "config", "branch.main.remote", "origin")
					}
				}
				return nil
			}
			workflow.Git = func(name string, args ...string) ([]byte, error) {
				if name == "git" && len(args) > 0 && args[0] == "push" {
					pushCalls++
				}
				cmd := exec.Command(name, args...)
				cmd.Dir = repo.work
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
			pushArgs := []string(nil)
			workflow.Git = func(name string, args ...string) ([]byte, error) {
				if name == "git" && len(args) > 0 && args[0] == "push" {
					pushArgs = append([]string(nil), args...)
					gitClean(t, competing, "push", "origin", "HEAD:refs/heads/clean-topic")
				}
				cmd := exec.Command(name, args...)
				cmd.Dir = repo.work
				return cmd.Output()
			}

			err := workflow.Run("")
			if err == nil || !strings.Contains(err.Error(), "push clean") {
				t.Fatalf("push-boundary race error = %v, want push failure", err)
			}
			if !hasCleanPushLease(pushArgs, "refs/heads/clean-topic") {
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
	workflow.Git = func(name string, args ...string) ([]byte, error) {
		if name == "git" && len(args) > 0 && args[0] == "commit" {
			writeCleanFilePath(repo.work, "README.md", "concurrent edit outside commit\n")
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = repo.work
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

func TestCleanRefusesCommitWithIndexMutationAndNeverPushes(t *testing.T) {
	repo := newCleanRepo(t)
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	workflow := cleanTestWorkflow(repo.work, &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}})
	workflow.Git = func(name string, args ...string) ([]byte, error) {
		if name == "git" && len(args) > 0 && args[0] == "commit" {
			cmd := exec.Command("git", "hash-object", "-w", "--stdin")
			cmd.Dir = repo.work
			cmd.Stdin = strings.NewReader("altered staged content\n")
			blob, err := cmd.Output()
			if err != nil {
				return nil, err
			}
			gitClean(t, repo.work, "update-index", "--cacheinfo", "100644,"+strings.TrimSpace(string(blob))+",fix.txt")
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = repo.work
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
	workflow.Git = func(name string, args ...string) ([]byte, error) {
		if name == "git" && len(args) > 0 && args[0] == "commit" {
			mergeTree := strings.TrimSpace(string(gitClean(t, repo.work, "write-tree")))
			secondParent := strings.TrimSpace(string(gitClean(t, repo.work, "commit-tree", initial+"^{tree}", "-p", initial, "-m", "second parent")))
			merge := strings.TrimSpace(string(gitClean(t, repo.work, "commit-tree", mergeTree, "-p", initial, "-p", secondParent, "-m", "injected merge")))
			gitClean(t, repo.work, "update-ref", "HEAD", merge)
			return nil, nil
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = repo.work
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
	workflow.Git = func(name string, args ...string) ([]byte, error) {
		if name == "git" && len(args) > 0 && args[0] == "push" {
			pushCalls++
			if args[2] != repo.bare || args[3] != strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))+":refs/heads/main" {
				return nil, fmt.Errorf("push did not use captured URL and exact refspec: %q", args)
			}
			gitClean(t, repo.work, "remote", "set-url", "origin", redirect)
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = repo.work
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
	workflow.Git = func(name string, args ...string) ([]byte, error) {
		if name == "git" && len(args) == 4 && args[0] == "config" && args[1] == "--null" && args[2] == "--get-all" {
			return nil, fmt.Errorf("simulated config inspection failure")
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = repo.work
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
			workflow.Git = func(name string, args ...string) ([]byte, error) {
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
				cmd.Dir = repo.work
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
	workflow.Git = func(name string, args ...string) ([]byte, error) {
		if name == "git" && len(args) > 0 && args[0] == "commit" {
			gitClean(t, repo.work, "config", "branch.main.remote", "redirect")
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = repo.work
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
	workflow.Git = func(name string, args ...string) ([]byte, error) {
		if name == "git" && len(args) > 0 && args[0] == "add" {
			writeCleanFilePath(repo.work, "fix.txt", "concurrent same-path edit\n")
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = repo.work
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
		if target != "fmt" {
			return nil
		}
		path := filepath.Join(repo.work, "fix.go")
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var makeTargets []string
	workflow.Make = func(target string, _ ...string) error {
		makeTargets = append(makeTargets, target)
		if target == "fmt" {
			cancel()
		}
		return nil
	}
	publishCalls := 0
	workflow.Git = func(name string, args ...string) ([]byte, error) {
		if name == "git" && len(args) > 0 {
			switch args[0] {
			case "add", "commit", "push":
				publishCalls++
			}
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = repo.work
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
	workflow.GitContext = func(ctx context.Context, name string, args ...string) ([]byte, error) {
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
		cmd.Dir = repo.work
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
	if got := output.String(); !strings.Contains(got, "⠋\033[0m review 1/4") || !strings.Contains(got, "\r\033[2K") {
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
	workflow.Git = func(name string, args ...string) ([]byte, error) {
		if name == "git" && len(args) > 0 && args[0] == "push" {
			pushCalls++
		}
		cmd := exec.Command(name, args...)
		cmd.Dir = repo.work
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

func TestCleanCommitsAndPushesOnlyRunChanges(t *testing.T) {
	repo := newCleanRepo(t)
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
	workflow := cleanTestWorkflow(repo.work, agent)
	if err := workflow.Run(""); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(agent.calls, ","), "review,evaluate,fix,evaluate,document,evaluate"; got != want {
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

func TestCleanGatePromptsOnRetriesAndApprovals(t *testing.T) {
	repo := newCleanRepo(t)
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{"evaluate": {"FAIL\nreview incomplete", "PASS\n", "PASS\n", "PASS\n"}}}
	var output bytes.Buffer
	workflow := cleanTestWorkflow(repo.work, agent)
	workflow.Gate = true
	workflow.In = strings.NewReader("yes\nyes\nyes\nyes\nyes\nyes\nyes\n")
	workflow.Out = &output
	if err := workflow.Run(""); err != nil {
		t.Fatal(err)
	}
	if strings.Count(output.String(), "Type exactly yes") != 4 {
		t.Fatalf("expected approvals for retry and all passing stages; output=%s", output.String())
	}
}

func TestCleanVerificationFailureNeverPushes(t *testing.T) {
	repo := newCleanRepo(t)
	initial := strings.TrimSpace(string(gitClean(t, repo.work, "rev-parse", "HEAD")))
	stateDir := filepath.Join(filepath.Dir(repo.work), "state")
	agent := &cleanWriterAgent{work: repo.work, outputs: map[string][]string{}}
	workflow := cleanTestWorkflow(repo.work, agent)
	workflow.Config.StateDir = stateDir
	workflow.Make = func(target string, _ ...string) error {
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
		In: strings.NewReader(""), Out: io.Discard, Workdir: work,
		Make: func(string, ...string) error { return nil },
	}
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
	work  string
	calls []string
}

func (a *cleanDestinationRaceAgent) Run(stage, _, _, _, log string) error {
	a.calls = append(a.calls, stage)
	if stage == "review" {
		cmd := exec.Command("git", "push", "origin", "HEAD:refs/heads/clean-topic")
		cmd.Dir = a.work
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
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		panic(fmt.Sprintf("create test file directory: %v", err))
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		panic(fmt.Sprintf("write test file: %v", err))
	}
}
