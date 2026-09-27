package factory

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
)

func repositoryOrTarget(repository, target string) string {
	if repository != "" {
		return repository
	}
	return target
}

func createImplementationWorktree(stateRoot, repository, target, head, id string) (string, string, error) {
	branch := "factory-job/" + id
	worktree := filepath.Join(stateRoot, id, "worktree")
	if _, err := runGit(context.Background(), repository, "worktree", "add", "-b", branch, worktree, head); err != nil {
		return "", "", fmt.Errorf("create isolated implementation worktree: %w", err)
	}
	return worktree, branch, nil
}

func defaultBranch(repository string) (string, error) {
	ref, err := runGit(context.Background(), repository, "symbolic-ref", "--quiet", "--short", "refs/remotes/origin/HEAD")
	if err == nil && strings.HasPrefix(ref, "origin/") {
		return strings.TrimPrefix(ref, "origin/"), nil
	}
	for _, candidate := range []string{"main", "master", "init"} {
		if _, err := runGit(context.Background(), repository, "show-ref", "--verify", "--quiet", "refs/heads/"+candidate); err == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("cannot determine the repository default branch (set origin/HEAD or create main/master)")
}
