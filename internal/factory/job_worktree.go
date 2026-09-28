package factory

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const implementationSlugTimeout = 10 * time.Second
const implementationSlugOutputLimit = 256

const implementationSlugPrompt = `Propose a concise slug for this task description. Output only a few lowercase kebab-case words (letters and digits separated by single hyphens), no explanation or punctuation.

Task description:
`

func repositoryOrTarget(repository, target string) string {
	if repository != "" {
		return repository
	}
	return target
}

func createImplementationWorktree(stateRoot, repository, target, head, description, id string) (string, string, error) {
	return createImplementationWorktreeWithSlug(stateRoot, repository, target, head, implementationJobSlug(description), id)
}

func createImplementationWorktreeWithSlug(stateRoot, repository, target, head, slug, id string) (string, string, error) {
	return createImplementationWorktreeAtParent(repository, target, head, slug, id, stateRoot)
}

func createImplementationWorktreeAtParent(repository, target, head, slug, id, parent string) (string, string, error) {
	branch := "factory-job-" + implementationJobSlug(slug) + "-" + id
	primary, err := primaryWorktree(repository)
	if err != nil {
		return "", "", err
	}
	parent, err = validateWorktreeParent(parent, primary, target)
	if err != nil {
		return "", "", err
	}
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", "", fmt.Errorf("create worktree parent: %w", err)
	}
	parent, err = validateWorktreeParent(parent, primary, target)
	if err != nil {
		return "", "", err
	}
	name, err := availableJobWorktreeName(parent, slug, id, "")
	if err != nil {
		return "", "", err
	}
	worktree := filepath.Join(parent, name)
	if _, err := runGit(context.Background(), repository, "worktree", "add", "-b", branch, worktree, head); err != nil {
		return "", "", fmt.Errorf("create isolated implementation worktree: %w", err)
	}
	return worktree, branch, nil
}

func implementationJobSlugWithFallback(parent context.Context, runner Runner, description, workdir, logPath string) string {
	fallback := implementationJobSlug(description)
	ctx, cancel := context.WithTimeout(parent, implementationSlugTimeout)
	defer cancel()
	slug, err := proposeImplementationJobSlug(ctx, runner, description, workdir, logPath)
	if err != nil {
		return fallback
	}
	return slug
}

func proposeImplementationJobSlug(ctx context.Context, runner Runner, description, workdir, logPath string) (string, error) {
	output, err := runner.RunWithOutputLimitContext(ctx, "slug", implementationSlugPrompt, description, workdir, logPath, implementationSlugOutputLimit)
	if err != nil {
		return "", err
	}
	if len(output) > implementationSlugOutputLimit {
		return "", fmt.Errorf("agent slug response exceeds %d bytes", implementationSlugOutputLimit)
	}
	output = strings.TrimSuffix(output, "\n")
	output = strings.TrimSuffix(output, "\r")
	if strings.ContainsAny(output, "\r\n") {
		return "", fmt.Errorf("agent slug response must be a single line")
	}
	output = strings.TrimSpace(output)
	if output == "" {
		return "", fmt.Errorf("agent returned an empty slug")
	}
	if strings.IndexFunc(output, func(char rune) bool {
		return char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9'
	}) == -1 {
		return "", fmt.Errorf("agent slug did not contain any ASCII letters or digits")
	}
	return implementationJobSlug(output), nil
}

func implementationJobName(description, id string) string {
	return implementationJobSlug(description) + "-" + id
}

func implementationJobSlug(description string) string {
	var builder strings.Builder
	lastDash := false
	for _, char := range strings.ToLower(description) {
		if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' {
			builder.WriteRune(char)
			lastDash = false
		} else if !lastDash {
			builder.WriteByte('-')
			lastDash = true
		}
		if builder.Len() >= 48 {
			break
		}
	}
	if slug := strings.Trim(builder.String(), "-"); slug != "" {
		return slug
	}
	return "task"
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
