package factory

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// CleanWorkflow reviews and verifies changes from a pristine checkout, then pushes existing branch commits and commits/pushes any cleanup edits.
type CleanWorkflow struct {
	Agent    Agent
	Config   Config
	In       io.Reader
	Out      io.Writer
	Workdir  string
	Terminal bool
	Gate     bool
	Git      func(string, ...string) ([]byte, error)
	Make     func(string, ...string) error
}

type cleanBaseline struct {
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
	root, err := canonicalPath(w.Workdir)
	if err != nil {
		return fmt.Errorf("resolve target directory: %w", err)
	}
	git := w.Git
	if git == nil {
		git = func(name string, args ...string) ([]byte, error) {
			cmd := exec.Command(name, args...)
			cmd.Dir = root
			return cmd.Output()
		}
	}
	baseline, err := inspectCleanBaseline(git)
	if err != nil {
		return err
	}
	makeTarget := w.Make
	if makeTarget == nil {
		makeTarget = func(target string, _ ...string) error {
			cmd := exec.Command("make", target)
			cmd.Dir = root
			cmd.Stdout, cmd.Stderr = w.Out, w.Out
			return cmd.Run()
		}
	}
	if strings.TrimSpace(task) == "" {
		task = "Review the current repository and work performed in this clean run. Fix every issue found, then document the changes. Preserve the user's intent and avoid unrelated changes."
	}
	workflow := Workflow{Agent: w.Agent, Config: w.Config, In: w.In, Out: w.Out, Workdir: root, Terminal: w.Terminal, Gate: w.Gate, RequireComplete: true, Stages: []string{"review", "fix", "document"}, FinalApproval: "commit and push clean changes"}
	if err := workflow.Run(task); err != nil {
		return err
	}
	for _, target := range []string{"fmt", "test", "vet"} {
		fmt.Fprintf(w.Out, "Running make %s\n", target)
		if err := makeTarget(target); err != nil {
			return fmt.Errorf("make %s: %w", target, err)
		}
	}
	paths, err := cleanChangedPaths(git)
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		if err := revalidateClean(git, baseline, nil, nil); err != nil {
			return err
		}
		if baseline.head == baseline.upstreamHead {
			fmt.Fprintln(w.Out, "Clean completed with no changes; branch is already synced.")
			return nil
		}
		if err := validateBeforePush(git, baseline, baseline.head); err != nil {
			return err
		}
		if err := validateNoURLRewrite(git, baseline.pushURL); err != nil {
			return err
		}
		if _, err := git("git", "push", "--no-follow-tags", baseline.pushURL, baseline.head+":"+baseline.remoteRef); err != nil {
			return fmt.Errorf("push clean branch: %w", err)
		}
		fmt.Fprintf(w.Out, "Existing branch commits pushed (%s).\n", baseline.head)
		return nil
	}
	snapshot, err := snapshotCleanPaths(root, paths)
	if err != nil {
		return fmt.Errorf("snapshot clean changes: %w", err)
	}
	if err := revalidateClean(git, baseline, paths, nil); err != nil {
		return err
	}
	if err := verifyCleanSnapshot(root, paths, snapshot); err != nil {
		return fmt.Errorf("clean outputs changed before staging; refusing commit: %w", err)
	}
	if _, err := git("git", append([]string{"add", "-A", "--"}, paths...)...); err != nil {
		return fmt.Errorf("stage clean changes: %w", err)
	}
	if err := verifyCleanSnapshot(root, paths, snapshot); err != nil {
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
	if err := verifyCleanSnapshot(root, paths, snapshot); err != nil {
		return fmt.Errorf("clean outputs changed before commit; refusing commit: %w", err)
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
	if err := validateNoURLRewrite(git, baseline.pushURL); err != nil {
		return err
	}
	if _, err := git("git", "push", "--no-follow-tags", baseline.pushURL, strings.TrimSpace(string(committedHead))+":"+baseline.remoteRef); err != nil {
		return fmt.Errorf("push clean commit: %w", err)
	}
	fmt.Fprintf(w.Out, "Clean changes committed (%s) and pushed.\n", strings.TrimSpace(string(committedHead)))
	return nil
}

func inspectCleanBaseline(git func(string, ...string) ([]byte, error)) (cleanBaseline, error) {
	status, err := git("git", "status", "--porcelain=v1", "-z", "--no-renames", "--untracked-files=all")
	if err != nil {
		return cleanBaseline{}, fmt.Errorf("inspect initial git status: %w", err)
	}
	if len(status) != 0 {
		return cleanBaseline{}, fmt.Errorf("factory clean requires a completely clean initial worktree (staged, unstaged, and untracked changes found)")
	}
	branch, err := git("git", "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil || strings.TrimSpace(string(branch)) == "" {
		return cleanBaseline{}, fmt.Errorf("factory clean requires a current non-detached branch")
	}
	upstream, err := git("git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}")
	if err != nil || strings.TrimSpace(string(upstream)) == "" {
		return cleanBaseline{}, fmt.Errorf("factory clean requires a configured upstream")
	}
	remote, remoteRef, err := cleanUpstream(git, strings.TrimSpace(string(branch)))
	if err != nil {
		return cleanBaseline{}, err
	}
	if remote == "." || strings.TrimSpace(remote) == "" {
		return cleanBaseline{}, fmt.Errorf("factory clean requires a non-local configured upstream remote")
	}
	pushURLs, err := git("git", "remote", "get-url", "--push", "--all", remote)
	if err != nil {
		return cleanBaseline{}, fmt.Errorf("inspect upstream push destination: %w", err)
	}
	urls := lineValues(pushURLs)
	if len(urls) != 1 || strings.TrimSpace(urls[0]) == "" {
		return cleanBaseline{}, fmt.Errorf("factory clean requires exactly one push URL for upstream remote %s", remote)
	}
	head, err := git("git", "rev-parse", "HEAD")
	if err != nil {
		return cleanBaseline{}, fmt.Errorf("inspect local HEAD: %w", err)
	}
	upstreamHead, err := git("git", "rev-parse", "@{upstream}")
	if err != nil {
		return cleanBaseline{}, fmt.Errorf("inspect upstream HEAD: %w", err)
	}
	head = bytes.TrimSpace(head)
	upstreamHead = bytes.TrimSpace(upstreamHead)
	if _, err := git("git", "merge-base", "--is-ancestor", string(upstreamHead), string(head)); err != nil {
		return cleanBaseline{}, fmt.Errorf("factory clean requires configured upstream to be an ancestor of local HEAD; upstream-ahead or diverged branches are refused")
	}
	return cleanBaseline{branch: strings.TrimSpace(string(branch)), upstream: strings.TrimSpace(string(upstream)), head: string(head), upstreamHead: string(upstreamHead), remote: remote, remoteRef: remoteRef, pushURL: urls[0]}, nil
}

// revalidateClean checks the same branch and baseline before and after staging.
func revalidateClean(git func(string, ...string) ([]byte, error), baseline cleanBaseline, worktreePaths, stagedPaths []string) error {
	branch, err := git("git", "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil || strings.TrimSpace(string(branch)) != baseline.branch {
		return fmt.Errorf("branch changed or became detached during clean")
	}
	upstream, err := git("git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}")
	if err != nil || strings.TrimSpace(string(upstream)) != baseline.upstream {
		return fmt.Errorf("upstream changed during clean")
	}
	remote, remoteRef, err := cleanUpstream(git, baseline.branch)
	if err != nil || remote != baseline.remote || remoteRef != baseline.remoteRef {
		return fmt.Errorf("upstream remote or branch changed during clean")
	}
	pushURLs, err := git("git", "remote", "get-url", "--push", "--all", baseline.remote)
	if err != nil || !equalStrings(lineValues(pushURLs), []string{baseline.pushURL}) {
		return fmt.Errorf("upstream push destination changed during clean")
	}
	head, err := git("git", "rev-parse", "HEAD")
	if err != nil || strings.TrimSpace(string(head)) != baseline.head {
		return fmt.Errorf("local HEAD changed during clean")
	}
	upstreamHead, err := git("git", "rev-parse", "@{upstream}")
	if err != nil || strings.TrimSpace(string(upstreamHead)) != baseline.upstreamHead {
		return fmt.Errorf("upstream HEAD changed during clean")
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
	upstream, err := git("git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}")
	if err != nil || strings.TrimSpace(string(upstream)) != baseline.upstream {
		return fmt.Errorf("upstream changed after commit; refusing push")
	}
	remote, remoteRef, err := cleanUpstream(git, baseline.branch)
	if err != nil || remote != baseline.remote || remoteRef != baseline.remoteRef {
		return fmt.Errorf("upstream remote or branch changed after commit; refusing push")
	}
	pushURLs, err := git("git", "remote", "get-url", "--push", "--all", baseline.remote)
	if err != nil || !equalStrings(lineValues(pushURLs), []string{baseline.pushURL}) {
		return fmt.Errorf("upstream push destination changed after commit; refusing push")
	}
	upstreamHead, err := git("git", "rev-parse", "@{upstream}")
	if err != nil || strings.TrimSpace(string(upstreamHead)) != baseline.upstreamHead {
		return fmt.Errorf("upstream advanced after commit; refusing push")
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

func validateNoURLRewrite(git func(string, ...string) ([]byte, error), pushURL string) error {
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
		if strings.HasPrefix(key, "url.") && strings.HasSuffix(key, ".insteadof") && strings.HasPrefix(pushURL, value) {
			return fmt.Errorf("factory clean found a matching Git url.*.insteadOf rule for the captured push URL; refusing push to prevent URL redirection")
		}
	}
	return nil
}

func cleanUpstream(git func(string, ...string) ([]byte, error), branch string) (string, string, error) {
	remote, err := git("git", "config", "--get", "branch."+branch+".remote")
	if err != nil {
		return "", "", fmt.Errorf("inspect configured upstream remote: %w", err)
	}
	merge, err := git("git", "config", "--get", "branch."+branch+".merge")
	if err != nil {
		return "", "", fmt.Errorf("inspect configured upstream branch: %w", err)
	}
	remoteName, ref := strings.TrimSpace(string(remote)), strings.TrimSpace(string(merge))
	if remoteName == "" || ref == "" || !strings.HasPrefix(ref, "refs/heads/") {
		return "", "", fmt.Errorf("factory clean requires a configured remote branch upstream")
	}
	return remoteName, ref, nil
}

func lineValues(data []byte) []string {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "\n")
}

func snapshotCleanPaths(root string, paths []string) (map[string]cleanFileSnapshot, error) {
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
			cmd := exec.Command("git", "-C", fullPath, "rev-parse", "HEAD")
			var output []byte
			output, err = cmd.Output()
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

func verifyCleanSnapshot(root string, paths []string, expected map[string]cleanFileSnapshot) error {
	actual, err := snapshotCleanPaths(root, paths)
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
