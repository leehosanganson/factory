package factory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type implementationPublication struct {
	Status  string
	Message string
}

var implementationLookPath = exec.LookPath

var implementationCommand = func(ctx context.Context, dir, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	configureProcessCancellation(cmd)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

func requireImplementationCheckout(ctx context.Context, target string) error {
	root, err := implementationCommand(ctx, target, "git", "rev-parse", "--show-toplevel")
	if err != nil {
		return fmt.Errorf("implementation requires a Git checkout: %w", err)
	}
	root = strings.TrimSpace(root)
	resolvedRoot, err := resolvedPath(root)
	resolvedTarget, targetErr := resolvedPath(target)
	if err != nil || targetErr != nil || resolvedRoot != resolvedTarget {
		return fmt.Errorf("implementation requires the target to be a Git checkout root")
	}
	branch, err := implementationCommand(ctx, target, "git", "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil || strings.TrimSpace(branch) == "" {
		return fmt.Errorf("implementation requires a checkout on a named branch")
	}
	return nil
}

// ValidateImplementationCheckout checks the prerequisite shared by all implementation modes.
func ValidateImplementationCheckout(target string) error {
	return requireImplementationCheckout(context.Background(), target)
}

func requireCleanCheckout(ctx context.Context, target string) error {
	if err := requireImplementationCheckout(ctx, target); err != nil {
		return err
	}
	status, err := implementationCommand(ctx, target, "git", "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return fmt.Errorf("inspect target checkout status: %w", err)
	}
	if status != "" {
		return fmt.Errorf("automatic PR publication requires a clean Git checkout (staged, modified, and untracked files must be absent)")
	}
	return nil
}

func RunAutomaticImplementation(ctx context.Context, cfg Config, target, task string, in io.Reader, out io.Writer, terminal, gate bool) error {
	if err := requireCleanCheckout(ctx, target); err != nil {
		return err
	}
	baseline, err := implementationCommand(ctx, target, "git", "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	baseline = strings.TrimSpace(baseline)
	targetBranch, err := implementationCommand(ctx, target, "git", "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil || strings.TrimSpace(targetBranch) == "" {
		return fmt.Errorf("automatic PR publication requires a checkout on a named branch")
	}
	targetBranch = strings.TrimSpace(targetBranch)
	id, err := newJobID()
	if err != nil {
		return err
	}
	branch := "factory-implement-" + id
	parent, err := os.MkdirTemp(filepath.Dir(target), ".factory-implement-")
	if err != nil {
		return fmt.Errorf("create implementation worktree parent: %w", err)
	}
	worktree := filepath.Join(parent, "worktree")
	if _, err := implementationCommand(ctx, target, "git", "worktree", "add", "-b", branch, worktree, baseline); err != nil {
		_ = os.RemoveAll(parent)
		return fmt.Errorf("create isolated implementation worktree: %w", err)
	}
	var runDir string
	workflow := Workflow{
		Agent: Runner{Config: cfg}, Config: cfg, In: in, Out: out, Workdir: worktree, Terminal: terminal, Gate: gate,
		RequireComplete: gate && cfg.AutoPublish, DeferCompletion: cfg.AutoPublish, Managed: gate,
		RunCreated: func(dir, _ string) { runDir = dir },
	}
	runErr := workflow.RunContext(ctx, task)
	if runErr == nil && cfg.AutoPublish {
		state, stateErr := readManagedState(runDir)
		if stateErr != nil {
			runErr = stateErr
		} else {
			observe := func(event WorkflowEvent) error { return persistWorkflowEvent(runDir, event) }
			runErr = runPipelineChecks(ctx, cfg.PipelineChecks, worktree, runDir, &state, observe, processPipelineCheckRunner{})
			if runErr != nil {
				status := "failed"
				if ctx.Err() != nil || errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
					status = "interrupted"
				}
				runErr = errors.Join(runErr, finalizeAutomaticImplementationRun(runDir, status, runErr.Error()))
			}
		}
	}
	if runErr != nil {
		fmt.Fprintf(out, "Implementation worktree retained after workflow did not complete: %s (branch %s)\n", worktree, branch)
		return runErr
	}
	result, publishErr := publishImplementation(ctx, worktree, target, baseline, targetBranch, branch, task, cfg.PipelineChecks)
	if publishErr != nil {
		fmt.Fprintf(out, "Workflow completed; PR publication did not complete.\n%s\n", publishErr)
		status, summary := "complete", publishErr.Error()
		if ctx.Err() != nil || errors.Is(publishErr, context.Canceled) || errors.Is(publishErr, context.DeadlineExceeded) {
			status = "interrupted"
		}
		finalizeErr := finalizeAutomaticImplementationRun(runDir, status, summary)
		if ctx.Err() != nil {
			return errors.Join(ctx.Err(), finalizeErr)
		}
		return finalizeErr
	}
	if result.Message != "" {
		fmt.Fprintln(out, result.Message)
	}
	if err := finalizeAutomaticImplementationRun(runDir, "complete", result.Message); err != nil {
		return err
	}
	if result.Status != "published" {
		return nil
	}
	if err := cleanupPublishedImplementationWorktree(target, worktree); err != nil {
		fmt.Fprintf(out, "Published result is intact, but cleanup failed; worktree was preserved at %s: %v\n", worktree, err)
		return nil
	}
	return nil
}

func finalizeAutomaticImplementationRun(runDir, status, summary string) error {
	if runDir == "" {
		return nil
	}
	state, err := readManagedState(runDir)
	if err != nil {
		return fmt.Errorf("read implementation run state: %w", err)
	}
	state.Status = status
	if err := writeState(runDir, &state); err != nil {
		return fmt.Errorf("persist implementation run status: %w", err)
	}
	message := status
	if summary != "" {
		message = summary
	}
	return persistWorkflowEvent(runDir, WorkflowEvent{RunID: state.ID, Type: "workflow.transition", Stage: state.Stage, Message: message})
}

func cleanupPublishedImplementationWorktree(repository, worktree string) error {
	if _, err := implementationCommand(context.Background(), repository, "git", "worktree", "remove", "--force", worktree); err != nil {
		return fmt.Errorf("remove implementation worktree: %w", err)
	}
	parent := filepath.Dir(worktree)
	entries, err := os.ReadDir(parent)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect implementation worktree parent %s: %w", parent, err)
	}
	if len(entries) == 0 {
		if err := os.Remove(parent); err != nil {
			return fmt.Errorf("remove empty implementation worktree parent %s: %w", parent, err)
		}
	}
	return nil
}

func validateImplementationTarget(ctx context.Context, target, baseline, targetBranch string) error {
	if err := requireCleanCheckout(ctx, target); err != nil {
		return fmt.Errorf("the invoking checkout is no longer clean: %w", err)
	}
	currentHead, err := implementationCommand(ctx, target, "git", "rev-parse", "HEAD")
	if err != nil || strings.TrimSpace(currentHead) != baseline {
		return fmt.Errorf("the invoking checkout baseline changed (expected %s, got %s)", baseline, strings.TrimSpace(currentHead))
	}
	currentBranch, err := implementationCommand(ctx, target, "git", "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil || strings.TrimSpace(currentBranch) != targetBranch {
		return fmt.Errorf("the invoking checkout branch changed (expected %s, got %s)", targetBranch, strings.TrimSpace(currentBranch))
	}
	return nil
}

func publishImplementation(ctx context.Context, worktree, target, baseline, targetBranch, taskBranch, task string, checks [][]string) (implementationPublication, error) {
	unpublished := func(reason string) (implementationPublication, error) {
		return implementationPublication{}, fmt.Errorf("completed unpublished: %s\nWorktree: %s\nBranch: %s", reason, worktree, taskBranch)
	}
	if err := ctx.Err(); err != nil {
		return unpublished(fmt.Sprintf("interrupted before publication (%v); no automatic retry will occur", err))
	}
	if err := validateImplementationTarget(ctx, target, baseline, targetBranch); err != nil {
		return unpublished(err.Error())
	}
	root, err := implementationCommand(ctx, worktree, "git", "rev-parse", "--show-toplevel")
	resolvedRoot, rootErr := resolvedPath(strings.TrimSpace(root))
	resolvedWorktree, worktreeErr := resolvedPath(worktree)
	if err != nil || rootErr != nil || worktreeErr != nil || resolvedRoot != resolvedWorktree {
		return unpublished("the implementation worktree is unavailable or invalid")
	}
	branch, err := implementationCommand(ctx, worktree, "git", "branch", "--show-current")
	if err != nil || strings.TrimSpace(branch) != taskBranch {
		return unpublished("the implementation branch changed unexpectedly")
	}
	head, err := implementationCommand(ctx, worktree, "git", "rev-parse", "HEAD")
	if err != nil || strings.TrimSpace(head) != baseline {
		return unpublished("the implementation workflow changed HEAD; expected its captured baseline")
	}
	status, err := implementationCommand(ctx, worktree, "git", "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return unpublished("could not inspect implementation changes: " + err.Error())
	}
	if status == "" {
		return implementationPublication{Status: "no-op", Message: "Implementation workflow made no changes; no commit or PR was created."}, nil
	}
	if err := ctx.Err(); err != nil {
		return unpublished(fmt.Sprintf("interrupted before staging/commit (%v); no automatic retry will occur", err))
	}
	if _, err := implementationCommand(ctx, worktree, "git", "add", "-A"); err != nil {
		return unpublished(fmt.Sprintf("could not stage workflow changes: %v\nRecovery: inspect with git -C %s status --short, then stage and commit on branch %s", err, publicationShellQuote(worktree), taskBranch))
	}
	pathsOutput, err := implementationCommand(ctx, worktree, "git", "diff", "--cached", "--name-only", "-z")
	if err != nil {
		return unpublished("could not summarize staged workflow changes: " + err.Error())
	}
	paths := splitGitPaths(pathsOutput)
	if len(paths) == 0 {
		return implementationPublication{Status: "no-op", Message: "Implementation workflow made no changes; no commit or PR was created."}, nil
	}
	if err := ctx.Err(); err != nil {
		return unpublished(fmt.Sprintf("interrupted before commit (%v); no automatic retry will occur", err))
	}
	if err := validateImplementationTarget(ctx, target, baseline, targetBranch); err != nil {
		return unpublished(err.Error())
	}
	if _, err := implementationCommand(ctx, worktree, "git", "commit", "-m", task); err != nil {
		return unpublished(fmt.Sprintf("could not commit workflow changes: %v\nRecovery: git -C %s status --short; git -C %s commit -m %s", err, publicationShellQuote(worktree), publicationShellQuote(worktree), publicationShellQuote(task)))
	}
	commit, err := implementationCommand(ctx, worktree, "git", "rev-parse", "HEAD")
	if err != nil {
		return unpublished("commit was created but its SHA could not be read: " + err.Error())
	}
	commit = strings.TrimSpace(commit)
	pushCommand := fmt.Sprintf("git -C %s push --force-with-lease=refs/heads/%s: origin %s:%s", publicationShellQuote(worktree), publicationShellQuote(taskBranch), publicationShellQuote(taskBranch), publicationShellQuote(taskBranch))
	prCommand := fmt.Sprintf("cd %s && gh pr create --title %s --body %s", publicationShellQuote(worktree), publicationShellQuote(task), publicationShellQuote(implementationPRBody(task, paths, checks)))
	if err := validateImplementationTarget(ctx, target, baseline, targetBranch); err != nil {
		return unpublished(fmt.Sprintf("commit %s was created but push was refused: %v\nCommit: %s\nPush command: %s", commit, err, commit, pushCommand))
	}
	if err := ctx.Err(); err != nil {
		return unpublished(fmt.Sprintf("interrupted after local commit %s and before push (%v); no automatic retry will occur\nCommit: %s\nPush command: %s", commit, err, commit, pushCommand))
	}
	_, ghErr := implementationLookPath("gh")
	_, originErr := implementationCommand(ctx, worktree, "git", "remote", "get-url", "origin")
	if ghErr != nil && originErr != nil {
		return unpublished(fmt.Sprintf("commit %s is retained locally; `gh` and origin are unavailable, so no push was made\nCommit: %s\nRecovery: install and authenticate `gh`, configure origin with your repository URL, then run:\n  git -C %s remote add origin <repository-url>\n  %s\n  %s", commit, commit, publicationShellQuote(worktree), pushCommand, prCommand))
	}
	if ghErr != nil {
		return unpublished(fmt.Sprintf("commit %s is retained locally; GitHub CLI `gh` is unavailable, so no push was made\nCommit: %s\nRecovery: install and authenticate `gh`, then run:\n  %s\n  %s", commit, commit, pushCommand, prCommand))
	}
	if originErr != nil {
		return unpublished(fmt.Sprintf("commit %s is retained locally; origin remote is unavailable, so no push was made\nCommit: %s\nRecovery: configure origin with your repository URL, then run:\n  git -C %s remote add origin <repository-url>\n  %s\n  %s", commit, commit, publicationShellQuote(worktree), pushCommand, prCommand))
	}
	remoteBranch, err := implementationCommand(ctx, worktree, "git", "ls-remote", "--heads", "origin", "refs/heads/"+taskBranch)
	if err != nil {
		return unpublished(fmt.Sprintf("commit %s is retained locally; could not verify remote task-branch availability, so no push was made: %v\nCommit: %s\nRecovery: verify origin and that refs/heads/%s is unused, then run:\n  %s\n  %s", commit, err, commit, taskBranch, pushCommand, prCommand))
	}
	if strings.TrimSpace(remoteBranch) != "" {
		uniqueBranch := taskBranch + "-recovery-" + strings.TrimSpace(commit)[:12]
		uniquePush := fmt.Sprintf("git -C %s push --force-with-lease=refs/heads/%s: origin %s:%s", publicationShellQuote(worktree), publicationShellQuote(uniqueBranch), publicationShellQuote(uniqueBranch), publicationShellQuote(uniqueBranch))
		uniquePR := fmt.Sprintf("cd %s && gh pr create --head %s --title %s --body %s", publicationShellQuote(worktree), publicationShellQuote(uniqueBranch), publicationShellQuote(task), publicationShellQuote(implementationPRBody(task, paths, checks)))
		return unpublished(fmt.Sprintf("commit %s is retained locally; remote task branch %s already exists, so no push was made and it will not be overwritten\nCommit: %s\nRecovery: create and use a unique local branch, then push that new ref and create the PR:\n  git -C %s switch -c %s\n  %s\n  %s", commit, taskBranch, commit, publicationShellQuote(worktree), publicationShellQuote(uniqueBranch), uniquePush, uniquePR))
	}
	if err := ctx.Err(); err != nil {
		return unpublished(fmt.Sprintf("commit %s is retained locally; push was canceled (%v)\nCommit: %s\nRecovery push: %s\nPR command after push: %s", commit, err, commit, pushCommand, prCommand))
	}
	if _, err := implementationCommand(ctx, worktree, "git", "push", "--porcelain", "--force-with-lease=refs/heads/"+taskBranch+":", "origin", taskBranch+":"+taskBranch); err != nil {
		return unpublished(fmt.Sprintf("commit %s is retained locally but push failed: %v\nCommit: %s\nRecovery push: %s", commit, err, commit, pushCommand))
	}
	if err := ctx.Err(); err != nil {
		return unpublished(fmt.Sprintf("commit %s was pushed but PR creation was interrupted (%v); no automatic retry will occur\nCommit: %s\nPR command: cd %s && gh pr create --title %s --body %s", commit, err, commit, publicationShellQuote(worktree), publicationShellQuote(task), publicationShellQuote(implementationPRBody(task, paths, checks))))
	}
	body := implementationPRBody(task, paths, checks)
	prOutput, err := implementationCommand(ctx, worktree, "gh", "pr", "create", "--title", task, "--body", body)
	if err != nil {
		return unpublished(fmt.Sprintf("commit %s was pushed but PR creation failed: %v\nCommit: %s\nPR command: %s", commit, err, commit, prCommand))
	}
	prURL := strings.TrimSpace(prOutput)
	return implementationPublication{Status: "published", Message: fmt.Sprintf("Published implementation PR for task %q (commit %s): %s", task, commit, prURL)}, nil
}

func splitGitPaths(value string) []string {
	parts := strings.Split(value, "\x00")
	paths := make([]string, 0, len(parts))
	for _, path := range parts {
		if path != "" {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	return paths
}

func publicationShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func implementationPRBody(task string, paths []string, checks [][]string) string {
	var body strings.Builder
	fmt.Fprintf(&body, "## Task\n%s\n\n## Changed files\n", task)
	for _, path := range paths {
		fmt.Fprintf(&body, "- `%s`\n", strings.ReplaceAll(path, "`", "\\`"))
	}
	body.WriteString("\n## Pipeline checks\n")
	if len(checks) == 0 {
		body.WriteString("- No pipeline checks configured\n")
	} else {
		for _, check := range checks {
			fmt.Fprintf(&body, "- Passed: `%s`\n", strings.Join(check, " "))
		}
	}
	return body.String()
}
