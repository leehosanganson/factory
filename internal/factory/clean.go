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
	"sort"
	"strings"
)

// CleanWorkflow reviews and verifies a checkout, publishing only from pristine mode; dirty safe mode never stages, commits, or pushes.
type CleanWorkflow struct {
	Agent      Agent
	Config     Config
	In         io.Reader
	Out        io.Writer
	Workdir    string
	Terminal   bool
	Gate       bool
	Git        func(string, ...string) ([]byte, error)
	GitContext func(context.Context, string, ...string) ([]byte, error)
	Make       func(string, ...string) error
}

type cleanBaseline struct {
	dirty        bool
	branch       string
	upstream     string
	head         string
	upstreamHead string
	remote       string
	remoteRef    string
	pushURL      string
}

type cleanFileSnapshot struct {
	mode    string
	content []byte
}

func (w CleanWorkflow) Run(task string) error {
	return w.RunContext(context.Background(), task)
}

func (w CleanWorkflow) RunContext(ctx context.Context, task string) (runErr error) {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("clean interrupted: %w", err)
	}
	root, err := canonicalPath(w.Workdir)
	if err != nil {
		return fmt.Errorf("resolve target directory: %w", err)
	}
	git := func(name string, args ...string) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var output []byte
		var gitErr error
		switch {
		case w.GitContext != nil:
			output, gitErr = w.GitContext(ctx, name, args...)
		case w.Git != nil:
			output, gitErr = w.Git(name, args...)
		default:
			cmd := exec.CommandContext(ctx, name, args...)
			configureProcessCancellation(cmd)
			cmd.Dir = root
			output, gitErr = cmd.Output()
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return output, gitErr
	}
	baseline, err := inspectCleanBaseline(git)
	if err != nil {
		return err
	}
	if baseline.dirty {
		fmt.Fprintln(w.Out, "Warning: the initial worktree/index is dirty. Existing changes may be affected by agents or formatters; Factory will not commit or push anything in this run.")
	}
	makeTarget := w.Make
	if makeTarget == nil {
		makeTarget = func(target string, _ ...string) error {
			cmd := exec.CommandContext(ctx, "make", target)
			configureProcessCancellation(cmd)
			cmd.Dir = root
			cmd.Stdout, cmd.Stderr = w.Out, w.Out
			return cmd.Run()
		}
	}
	if strings.TrimSpace(task) == "" {
		task = "Review the current repository and work performed in this clean run. Fix every issue found, then document the changes. Preserve the user's intent and avoid unrelated changes."
	}
	finalApproval := "commit and push clean changes"
	if baseline.dirty {
		finalApproval = "verify and complete without committing or pushing changes"
	}
	runOutput := &cleanRunOutput{out: w.Out}
	workflow := Workflow{Agent: w.Agent, Config: w.Config, In: w.In, Out: runOutput, Workdir: root, Terminal: w.Terminal, Gate: w.Gate, RequireComplete: true, Stages: []string{"review", "fix", "document"}, FinalApproval: finalApproval}
	if err := workflow.RunContext(ctx, task); err != nil {
		return err
	}
	defer func() {
		if runErr == nil || runOutput.runDir == "" {
			return
		}
		data, readErr := os.ReadFile(filepath.Join(runOutput.runDir, "state.json"))
		if readErr != nil {
			return
		}
		var state State
		if json.Unmarshal(data, &state) != nil || state.Status != "complete" {
			return
		}
		state.Status = "failed"
		if ctx.Err() != nil || errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
			state.Status = "interrupted"
		}
		_ = writeState(runOutput.runDir, &state)
	}()
	for _, target := range []string{"fmt", "test", "vet"} {
		if err := cleanVerificationInterrupted(ctx, runOutput.runDir); err != nil {
			return err
		}
		fmt.Fprintf(w.Out, "Running make %s\n", target)
		if err := makeTarget(target); err != nil {
			if interrupted := cleanVerificationInterrupted(ctx, runOutput.runDir); interrupted != nil {
				return interrupted
			}
			return fmt.Errorf("make %s: %w", target, err)
		}
		if err := cleanVerificationInterrupted(ctx, runOutput.runDir); err != nil {
			return err
		}
	}
	if err := cleanVerificationInterrupted(ctx, runOutput.runDir); err != nil {
		return err
	}
	if baseline.dirty {
		fmt.Fprintln(w.Out, "Clean verification succeeded. Changes remain uncommitted and were not pushed.")
		return nil
	}
	paths, err := cleanChangedPaths(git)
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		if err := revalidateClean(git, baseline, nil, nil); err != nil {
			return err
		}
		if baseline.upstream != "" && baseline.head == baseline.upstreamHead {
			fmt.Fprintln(w.Out, "Clean completed with no changes; branch is already synced.")
			return nil
		}
		if err := validateBeforePush(git, baseline, baseline.head); err != nil {
			return err
		}
		if err := cleanContextCheck(ctx); err != nil {
			return err
		}
		if _, err := git("git", cleanPushArgs(baseline, baseline.head)...); err != nil {
			return fmt.Errorf("push clean branch: %w", err)
		}
		fmt.Fprintf(w.Out, "Existing branch commits pushed (%s).\n", baseline.head)
		return nil
	}
	snapshot, err := snapshotCleanPaths(git, root, paths)
	if err != nil {
		return fmt.Errorf("snapshot clean changes: %w", err)
	}
	if err := revalidateClean(git, baseline, paths, nil); err != nil {
		return err
	}
	if err := verifyCleanSnapshot(git, root, paths, snapshot); err != nil {
		return fmt.Errorf("clean outputs changed before staging; refusing commit: %w", err)
	}
	if err := cleanContextCheck(ctx); err != nil {
		return err
	}
	if _, err := git("git", append([]string{"add", "-A", "--"}, paths...)...); err != nil {
		return fmt.Errorf("stage clean changes: %w", err)
	}
	if err := verifyCleanSnapshot(git, root, paths, snapshot); err != nil {
		return fmt.Errorf("clean outputs changed during staging; refusing commit: %w", err)
	}
	if err := revalidateClean(git, baseline, paths, paths); err != nil {
		return err
	}
	staged, err := git("git", "diff", "--cached", "--name-only", "-z", "--no-renames", "HEAD")
	if err != nil {
		return fmt.Errorf("inspect staged paths: %w", err)
	}
	if !samePaths(paths, nulPaths(staged)) {
		return fmt.Errorf("staged paths changed during clean; refusing commit")
	}
	if err := verifyStagedSnapshot(git, paths, snapshot); err != nil {
		return fmt.Errorf("staged clean outputs differ from inventory; refusing commit: %w", err)
	}
	stagedStatus, err := git("git", "status", "--porcelain=v1", "-z", "--no-renames", "--untracked-files=all")
	if err != nil || !samePaths(statusPaths(stagedStatus), paths) {
		return fmt.Errorf("worktree paths changed after staging; refusing commit")
	}
	if err := revalidateClean(git, baseline, paths, paths); err != nil {
		return err
	}
	unstaged, err := git("git", "diff", "--name-only", "-z", "--no-renames")
	if err != nil || len(unstaged) != 0 {
		return fmt.Errorf("unstaged changes appeared before commit; refusing commit")
	}
	if err := verifyCleanSnapshot(git, root, paths, snapshot); err != nil {
		return fmt.Errorf("clean outputs changed before commit; refusing commit: %w", err)
	}
	if err := cleanContextCheck(ctx); err != nil {
		return err
	}
	if _, err := git("git", "commit", "-m", "chore: clean and verify changes"); err != nil {
		return fmt.Errorf("commit clean changes: %w", err)
	}
	committedHead, err := git("git", "rev-parse", "HEAD")
	if err != nil {
		return fmt.Errorf("inspect clean commit: %w", err)
	}
	parents, err := git("git", "rev-list", "--parents", "-n", "1", strings.TrimSpace(string(committedHead)))
	if err != nil {
		return fmt.Errorf("inspect clean commit parents: %w", err)
	}
	parentFields := strings.Fields(string(parents))
	if len(parentFields) != 2 || parentFields[1] != baseline.head {
		return fmt.Errorf("clean commit must have exactly one parent equal to the initial HEAD; refusing push")
	}
	if _, err := git("git", "merge-base", "--is-ancestor", baseline.head, strings.TrimSpace(string(committedHead))); err != nil {
		return fmt.Errorf("clean commit does not descend from the initial HEAD; refusing push")
	}
	committedPaths, err := git("git", "diff-tree", "--no-commit-id", "--name-only", "--no-renames", "-r", "-z", "HEAD")
	if err != nil || !samePaths(paths, nulPaths(committedPaths)) {
		return fmt.Errorf("clean commit paths differ from run changes; refusing push")
	}
	if err := verifyCommittedSnapshot(git, paths, snapshot, strings.TrimSpace(string(committedHead))); err != nil {
		return fmt.Errorf("committed clean outputs differ from inventory; refusing push: %w", err)
	}
	if err := validateBeforePush(git, baseline, strings.TrimSpace(string(committedHead))); err != nil {
		return err
	}
	if err := cleanContextCheck(ctx); err != nil {
		return err
	}
	if _, err := git("git", cleanPushArgs(baseline, strings.TrimSpace(string(committedHead)))...); err != nil {
		return fmt.Errorf("push clean commit: %w", err)
	}
	fmt.Fprintf(w.Out, "Clean changes committed (%s) and pushed.\n", strings.TrimSpace(string(committedHead)))
	return nil
}

type cleanRunOutput struct {
	out    io.Writer
	runDir string
	line   bytes.Buffer
}

func (w *cleanRunOutput) Write(data []byte) (int, error) {
	written, err := w.out.Write(data)
	if err != nil {
		return written, err
	}
	for _, b := range data[:written] {
		if b == '\n' {
			line := w.line.String()
			w.line.Reset()
			if strings.HasPrefix(line, "Run: ") {
				w.runDir = strings.TrimPrefix(line, "Run: ")
			}
		} else {
			w.line.WriteByte(b)
		}
	}
	return written, nil
}

func cleanVerificationInterrupted(ctx context.Context, runDir string) error {
	if err := ctx.Err(); err != nil {
		if runDir != "" {
			data, readErr := os.ReadFile(filepath.Join(runDir, "state.json"))
			if readErr == nil {
				var state State
				if json.Unmarshal(data, &state) == nil {
					state.Status = "interrupted"
					_ = writeState(runDir, &state)
				}
			}
		}
		return fmt.Errorf("clean verification interrupted: %w", err)
	}
	return nil
}

func cleanContextCheck(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("clean interrupted: %w", err)
	}
	return nil
}

func inspectCleanBaseline(git func(string, ...string) ([]byte, error)) (cleanBaseline, error) {
	status, err := git("git", "status", "--porcelain=v1", "-z", "--no-renames", "--untracked-files=all")
	if err != nil {
		return cleanBaseline{}, fmt.Errorf("inspect initial git status: %w", err)
	}
	branch, err := git("git", "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil || strings.TrimSpace(string(branch)) == "" {
		return cleanBaseline{}, fmt.Errorf("factory clean requires a current non-detached branch")
	}
	branchName := strings.TrimSpace(string(branch))
	head, err := git("git", "rev-parse", "HEAD")
	if err != nil {
		return cleanBaseline{}, fmt.Errorf("inspect local HEAD: %w", err)
	}
	headName := strings.TrimSpace(string(head))
	if len(status) != 0 {
		return cleanBaseline{dirty: true, branch: branchName, head: headName}, nil
	}
	remote, remoteRef, configuredUpstream, err := cleanUpstreamConfig(git, branchName)
	if err != nil {
		return cleanBaseline{}, err
	}
	upstream, upstreamErr := git("git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}")
	upstreamName := strings.TrimSpace(string(upstream))
	if configuredUpstream {
		if upstreamErr != nil || upstreamName == "" {
			return cleanBaseline{}, fmt.Errorf("factory clean cannot resolve the configured upstream")
		}
		resolvedMerge, err := cleanResolvedUpstreamMerge(git, branchName)
		if err != nil {
			return cleanBaseline{}, err
		}
		if resolvedMerge != remoteRef {
			return cleanBaseline{}, fmt.Errorf("factory clean configured merge ref %s does not match resolved upstream ref %s", remoteRef, resolvedMerge)
		}
		if remote == "." || strings.TrimSpace(remote) == "" {
			return cleanBaseline{}, fmt.Errorf("factory clean requires a non-local configured upstream remote")
		}
	} else {
		if upstreamErr == nil && upstreamName != "" {
			return cleanBaseline{}, fmt.Errorf("factory clean found an upstream without branch upstream configuration")
		}
		remote = "origin"
		remoteRef = "refs/heads/" + branchName
	}
	pushURL, explicitPushURL, err := cleanRawPushURL(git, remote)
	if err != nil {
		return cleanBaseline{}, err
	}
	if err := validateNoURLRewrite(git, pushURL, !explicitPushURL); err != nil {
		return cleanBaseline{}, err
	}
	if !configuredUpstream {
		if err := ensureCleanRemoteRefAbsent(git, pushURL, remoteRef); err != nil {
			return cleanBaseline{}, err
		}
	}
	var upstreamHead []byte
	if configuredUpstream {
		upstreamHead, err = git("git", "rev-parse", "@{upstream}")
		if err != nil {
			return cleanBaseline{}, fmt.Errorf("inspect upstream HEAD: %w", err)
		}
		head = bytes.TrimSpace(head)
		upstreamHead = bytes.TrimSpace(upstreamHead)
		if _, err := git("git", "merge-base", "--is-ancestor", string(upstreamHead), string(head)); err != nil {
			return cleanBaseline{}, fmt.Errorf("factory clean requires configured upstream to be an ancestor of local HEAD; upstream-ahead or diverged branches are refused")
		}
	}
	return cleanBaseline{branch: branchName, upstream: upstreamName, head: headName, upstreamHead: string(upstreamHead), remote: remote, remoteRef: remoteRef, pushURL: pushURL}, nil
}

func validateCleanDestination(git func(string, ...string) ([]byte, error), baseline cleanBaseline, context string) error {
	remote, remoteRef, configuredUpstream, configErr := cleanUpstreamConfig(git, baseline.branch)
	if configErr != nil {
		return fmt.Errorf("upstream configuration changed %s: %w", context, configErr)
	}
	upstream, upstreamErr := git("git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}")
	upstreamName := strings.TrimSpace(string(upstream))
	if baseline.upstream != "" {
		if !configuredUpstream || upstreamErr != nil || upstreamName == "" || upstreamName != baseline.upstream {
			return fmt.Errorf("upstream changed %s", context)
		}
		resolvedMerge, err := cleanResolvedUpstreamMerge(git, baseline.branch)
		if err != nil || resolvedMerge != remoteRef {
			return fmt.Errorf("upstream merge ref changed %s", context)
		}
		if remote != baseline.remote || remoteRef != baseline.remoteRef {
			return fmt.Errorf("upstream remote or branch changed %s", context)
		}
	} else {
		if configuredUpstream || (upstreamErr == nil && upstreamName != "") {
			return fmt.Errorf("upstream appeared or cannot be verified %s", context)
		}
		if baseline.remote != "origin" || baseline.remoteRef != "refs/heads/"+baseline.branch {
			return fmt.Errorf("fallback push destination changed %s", context)
		}
	}
	pushURL, explicitPushURL, err := cleanRawPushURL(git, baseline.remote)
	if err != nil || pushURL != baseline.pushURL {
		return fmt.Errorf("push destination changed %s", context)
	}
	if err := validateNoURLRewrite(git, pushURL, !explicitPushURL); err != nil {
		return err
	}
	if baseline.upstream == "" {
		if err := ensureCleanRemoteRefAbsent(git, baseline.pushURL, baseline.remoteRef); err != nil {
			return err
		}
	}
	return nil
}

func cleanPushArgs(baseline cleanBaseline, head string) []string {
	args := []string{"push", "--no-follow-tags"}
	if baseline.upstream == "" {
		// An empty expected value permits creation only; it cannot replace an existing fallback ref.
		args = append(args, "--force-with-lease="+baseline.remoteRef+":")
	}
	return append(args, baseline.pushURL, head+":"+baseline.remoteRef)
}

func ensureCleanRemoteRefAbsent(git func(string, ...string) ([]byte, error), pushURL, remoteRef string) error {
	refs, err := git("git", "ls-remote", "--heads", pushURL, remoteRef)
	if err != nil {
		return fmt.Errorf("cannot verify fallback remote destination is absent; refusing push: %w", err)
	}
	if len(bytes.TrimSpace(refs)) != 0 {
		return fmt.Errorf("fallback remote destination %s already exists; refusing push", remoteRef)
	}
	return nil
}

// revalidateClean checks the same branch and baseline before and after staging.
func revalidateClean(git func(string, ...string) ([]byte, error), baseline cleanBaseline, worktreePaths, stagedPaths []string) error {
	branch, err := git("git", "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil || strings.TrimSpace(string(branch)) != baseline.branch {
		return fmt.Errorf("branch changed or became detached during clean")
	}
	if err := validateCleanDestination(git, baseline, "during clean"); err != nil {
		return err
	}
	head, err := git("git", "rev-parse", "HEAD")
	if err != nil || strings.TrimSpace(string(head)) != baseline.head {
		return fmt.Errorf("local HEAD changed during clean")
	}
	if baseline.upstream != "" {
		upstreamHead, err := git("git", "rev-parse", "@{upstream}")
		if err != nil || strings.TrimSpace(string(upstreamHead)) != baseline.upstreamHead {
			return fmt.Errorf("upstream HEAD changed during clean")
		}
	}
	status, err := git("git", "status", "--porcelain=v1", "-z", "--no-renames", "--untracked-files=all")
	if err != nil {
		return fmt.Errorf("revalidate git status: %w", err)
	}
	if !samePaths(statusPaths(status), worktreePaths) {
		return fmt.Errorf("git worktree paths changed during clean; refusing to commit or push")
	}
	index, err := git("git", "diff", "--cached", "--name-only", "-z", "--no-renames")
	if err != nil {
		return fmt.Errorf("inspect git index: %w", err)
	}
	if !samePaths(nulPaths(index), stagedPaths) {
		return fmt.Errorf("git index changed during clean; refusing to commit or push")
	}
	return nil
}

func validateBeforePush(git func(string, ...string) ([]byte, error), baseline cleanBaseline, committedHead string) error {
	branch, err := git("git", "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil || strings.TrimSpace(string(branch)) != baseline.branch {
		return fmt.Errorf("branch changed or became detached after commit; refusing push")
	}
	if err := validateCleanDestination(git, baseline, "after commit; refusing push"); err != nil {
		return err
	}
	if baseline.upstream != "" {
		upstreamHead, err := git("git", "rev-parse", "@{upstream}")
		if err != nil || strings.TrimSpace(string(upstreamHead)) != baseline.upstreamHead {
			return fmt.Errorf("upstream advanced after commit; refusing push")
		}
	}
	head, err := git("git", "rev-parse", "HEAD")
	if err != nil || strings.TrimSpace(string(head)) != committedHead {
		return fmt.Errorf("local HEAD changed after commit; refusing push")
	}
	if _, err := git("git", "merge-base", "--is-ancestor", baseline.head, committedHead); err != nil {
		return fmt.Errorf("final HEAD does not descend from the initial HEAD; refusing push")
	}
	status, err := git("git", "status", "--porcelain=v1", "-z", "--no-renames", "--untracked-files=all")
	if err != nil || len(status) != 0 {
		return fmt.Errorf("worktree is not clean after commit; refusing push")
	}
	return nil
}

func validateNoURLRewrite(git func(string, ...string) ([]byte, error), pushURL string, allowPushInsteadOf bool) error {
	data, err := git("git", "config", "--null", "--list")
	if err != nil {
		return fmt.Errorf("cannot inspect Git URL rewrite configuration; refusing push")
	}
	for _, entry := range nulPaths(data) {
		key, value, ok := strings.Cut(entry, "\n")
		if !ok {
			return fmt.Errorf("cannot inspect Git URL rewrite configuration; refusing push")
		}
		key = strings.ToLower(key)
		if !strings.HasPrefix(key, "url.") || !strings.HasPrefix(pushURL, value) {
			continue
		}
		switch {
		case strings.HasSuffix(key, ".insteadof"):
			return fmt.Errorf("factory clean found a matching Git url.*.insteadOf rule for the raw push URL; refusing push to prevent URL redirection")
		case allowPushInsteadOf && strings.HasSuffix(key, ".pushinsteadof"):
			return fmt.Errorf("factory clean found a matching Git url.*.pushInsteadOf rule for the raw remote URL; refusing push to prevent URL redirection")
		}
	}
	return nil
}

func cleanRawPushURL(git func(string, ...string) ([]byte, error), remote string) (string, bool, error) {
	key := "remote." + remote + ".pushurl"
	data, err := git("git", "config", "--null", "--get-all", key)
	explicitPushURL := err == nil
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
			return "", false, fmt.Errorf("inspect configured push URLs for remote %s: %w", remote, err)
		}
		key = "remote." + remote + ".url"
		data, err = git("git", "config", "--null", "--get-all", key)
		if err != nil {
			return "", false, fmt.Errorf("inspect configured remote URLs for remote %s: %w", remote, err)
		}
	}
	urls := nulPaths(data)
	if len(urls) != 1 || strings.TrimSpace(urls[0]) == "" {
		return "", false, fmt.Errorf("factory clean requires exactly one raw push URL for remote %s", remote)
	}
	return urls[0], explicitPushURL, nil
}

func cleanUpstreamConfig(git func(string, ...string) ([]byte, error), branch string) (string, string, bool, error) {
	remote, remoteMissing, err := cleanConfigValue(git, "branch."+branch+".remote")
	if err != nil {
		return "", "", false, fmt.Errorf("inspect configured upstream remote: %w", err)
	}
	merges, mergeMissing, err := cleanConfigValues(git, "branch."+branch+".merge")
	if err != nil {
		return "", "", false, fmt.Errorf("inspect configured upstream branch: %w", err)
	}
	if len(merges) > 1 {
		return "", "", true, fmt.Errorf("factory clean requires exactly one configured upstream branch")
	}
	merge := ""
	if len(merges) == 1 {
		merge = merges[0]
	}
	if remoteMissing && mergeMissing {
		return "", "", false, nil
	}
	if remoteMissing || mergeMissing {
		return "", "", true, fmt.Errorf("factory clean requires both configured upstream remote and branch")
	}
	remoteName, ref := strings.TrimSpace(remote), strings.TrimSpace(merge)
	if remoteName == "" || ref == "" || !strings.HasPrefix(ref, "refs/heads/") {
		return "", "", true, fmt.Errorf("factory clean requires a configured remote branch upstream")
	}
	return remoteName, ref, true, nil
}

func cleanResolvedUpstreamMerge(git func(string, ...string) ([]byte, error), branch string) (string, error) {
	merge, err := git("git", "for-each-ref", "--format=%(upstream:remoteref)", "refs/heads/"+branch)
	if err != nil {
		return "", fmt.Errorf("factory clean cannot inspect the resolved upstream branch: %w", err)
	}
	ref := strings.TrimSpace(string(merge))
	if ref == "" {
		return "", fmt.Errorf("factory clean cannot resolve the configured upstream branch")
	}
	return ref, nil
}

func cleanConfigValue(git func(string, ...string) ([]byte, error), key string) (string, bool, error) {
	value, err := git("git", "config", "--get", key)
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return "", true, nil
		}
		return "", false, err
	}
	return string(value), false, nil
}

func cleanConfigValues(git func(string, ...string) ([]byte, error), key string) ([]string, bool, error) {
	data, err := git("git", "config", "--null", "--get-all", key)
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return nil, true, nil
		}
		return nil, false, err
	}
	return nulPaths(data), false, nil
}

func lineValues(data []byte) []string {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "\n")
}

func snapshotCleanPaths(git func(string, ...string) ([]byte, error), root string, paths []string) (map[string]cleanFileSnapshot, error) {
	snapshot := make(map[string]cleanFileSnapshot, len(paths))
	for _, path := range paths {
		fullPath := filepath.Join(root, filepath.FromSlash(path))
		info, err := os.Lstat(fullPath)
		if os.IsNotExist(err) {
			snapshot[path] = cleanFileSnapshot{}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect %q: %w", path, err)
		}
		entry := cleanFileSnapshot{mode: snapshotMode(info.Mode())}
		switch {
		case info.Mode().IsRegular():
			entry.content, err = os.ReadFile(fullPath)
		case info.Mode()&os.ModeSymlink != 0:
			var target string
			target, err = os.Readlink(fullPath)
			entry.content = []byte(target)
		case info.IsDir():
			var output []byte
			output, err = git("git", "-C", fullPath, "rev-parse", "HEAD")
			if err == nil {
				entry.content = bytes.TrimSpace(output)
			}
		default:
			return nil, fmt.Errorf("unsupported file type for %q", path)
		}
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", path, err)
		}
		snapshot[path] = entry
	}
	return snapshot, nil
}

func snapshotMode(mode os.FileMode) string {
	switch {
	case mode&os.ModeSymlink != 0:
		return "120000"
	case mode.IsDir():
		return "160000"
	case mode.IsRegular() && mode&0o111 != 0:
		return "100755"
	case mode.IsRegular():
		return "100644"
	default:
		return "unsupported"
	}
}

func verifyCleanSnapshot(git func(string, ...string) ([]byte, error), root string, paths []string, expected map[string]cleanFileSnapshot) error {
	actual, err := snapshotCleanPaths(git, root, paths)
	if err != nil {
		return err
	}
	for _, path := range paths {
		if want, got := expected[path], actual[path]; want.mode != got.mode || !bytes.Equal(want.content, got.content) {
			return fmt.Errorf("%q differs from the inventory snapshot", path)
		}
	}
	return nil
}

func verifyStagedSnapshot(git func(string, ...string) ([]byte, error), paths []string, expected map[string]cleanFileSnapshot) error {
	for _, path := range paths {
		entries, err := git("git", "ls-files", "--stage", "-z", "--", path)
		if err != nil {
			return err
		}
		want := expected[path]
		if want.mode == "" {
			if len(entries) != 0 {
				return fmt.Errorf("deleted path %q remains staged", path)
			}
			continue
		}
		fields := nulPaths(entries)
		if len(fields) != 1 {
			return fmt.Errorf("expected one staged entry for %q", path)
		}
		metadata, stagedPath, ok := strings.Cut(fields[0], "\t")
		parts := strings.Fields(metadata)
		if !ok || stagedPath != path || len(parts) != 3 || parts[0] != want.mode {
			return fmt.Errorf("staged type or mode differs for %q", path)
		}
		if want.mode == "160000" {
			if parts[1] != string(want.content) {
				return fmt.Errorf("staged submodule commit differs for %q", path)
			}
		} else {
			content, err := git("git", "cat-file", "blob", parts[1])
			if err != nil {
				return err
			}
			if !bytes.Equal(content, want.content) {
				return fmt.Errorf("staged content differs for %q", path)
			}
		}
	}
	return nil
}

func verifyCommittedSnapshot(git func(string, ...string) ([]byte, error), paths []string, expected map[string]cleanFileSnapshot, commit string) error {
	for _, path := range paths {
		entries, err := git("git", "ls-tree", "-z", "--full-tree", commit, "--", ":(literal)"+path)
		if err != nil {
			return err
		}
		fields := nulPaths(entries)
		want := expected[path]
		if want.mode == "" {
			if len(fields) != 0 {
				return fmt.Errorf("deleted path %q remains in committed tree", path)
			}
			continue
		}
		if len(fields) != 1 {
			return fmt.Errorf("expected one committed tree entry for %q", path)
		}
		metadata, committedPath, ok := strings.Cut(fields[0], "\t")
		parts := strings.Fields(metadata)
		if !ok || committedPath != path || len(parts) != 3 || parts[0] != want.mode {
			return fmt.Errorf("committed type or mode differs for %q", path)
		}
		if want.mode == "160000" {
			if parts[1] != "commit" || parts[2] != string(want.content) {
				return fmt.Errorf("committed submodule differs for %q", path)
			}
			continue
		}
		if parts[1] != "blob" {
			return fmt.Errorf("committed object type differs for %q", path)
		}
		content, err := git("git", "cat-file", "blob", parts[2])
		if err != nil {
			return err
		}
		if !bytes.Equal(content, want.content) {
			return fmt.Errorf("committed content differs for %q", path)
		}
	}
	return nil
}

func cleanChangedPaths(git func(string, ...string) ([]byte, error)) ([]string, error) {
	tracked, err := git("git", "diff", "--name-only", "-z", "--no-renames", "HEAD")
	if err != nil {
		return nil, fmt.Errorf("inspect changed tracked paths: %w", err)
	}
	untracked, err := git("git", "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return nil, fmt.Errorf("inspect untracked paths: %w", err)
	}
	return uniqueSorted(append(nulPaths(tracked), nulPaths(untracked)...)), nil
}

func statusPaths(data []byte) []string {
	fields := nulPaths(data)
	paths := make([]string, 0, len(fields))
	for _, field := range fields {
		if len(field) >= 4 {
			paths = append(paths, field[3:])
		}
	}
	return uniqueSorted(paths)
}

func nulPaths(data []byte) []string {
	if len(data) == 0 {
		return nil
	}
	parts := strings.Split(string(data), "\x00")
	if parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

func uniqueSorted(paths []string) []string {
	seen := make(map[string]bool, len(paths))
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		if path != "" && !seen[path] {
			seen[path] = true
			out = append(out, path)
		}
	}
	sort.Strings(out)
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func samePaths(a, b []string) bool {
	a, b = uniqueSorted(a), uniqueSorted(b)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
