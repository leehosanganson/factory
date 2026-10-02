package restprovider

import (
	"context"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func GitPushBranch(ctx context.Context, repository, worktree, branch string) error {
	return GitPushBranchWithToken(ctx, "", repository, worktree, branch)
}

func GitPushBranchWithToken(ctx context.Context, token, repository, worktree, branch string) error {
	if ctx == nil || ctx.Err() != nil || !ValidRepository(repository) || !filepath.IsAbs(worktree) || !IsJobBranch(branch) {
		return errors.New("invalid GitHub branch push request")
	}
	remote, err := gitOutput(ctx, worktree, "remote", "get-url", "--push", "--all", "origin")
	if err != nil || strings.Count(remote, "\n") != 1 || !strings.HasSuffix(remote, "\n") {
		return errors.New("GitHub branch push remote is ambiguous")
	}
	remote = strings.TrimSuffix(remote, "\n")
	if !MatchesGitHubRepository(remote, repository) {
		return errors.New("GitHub branch push remote does not match the configured repository")
	}
	args := []string{"push", "--porcelain", "--no-follow-tags", remote, "HEAD:refs/heads/" + branch}
	if token != "" {
		helper := "!f() { printf 'username=x-access-token\\npassword=%s\\n' \"$FACTORY_GITHUB_TOKEN\"; }; f"
		args = append([]string{"-c", "core.hooksPath=/dev/null", "-c", "credential.helper=", "-c", "credential.helper=" + helper}, args...)
	}
	if _, err := gitOutputWithToken(ctx, worktree, token, args...); err != nil {
		return errors.New("GitHub branch push failed")
	}
	return nil
}

func gitOutput(ctx context.Context, worktree string, args ...string) (string, error) {
	return gitOutputWithToken(ctx, worktree, "", args...)
}

func gitOutputWithToken(ctx context.Context, worktree, token string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", worktree}, args...)...)
	cmd.Env = gitProviderEnvironment(os.Environ())
	cmd.Env = append(cmd.Env, "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	if token != "" {
		cmd.Env = append(cmd.Env, "FACTORY_GITHUB_TOKEN="+token)
	}
	output, err := cmd.Output()
	if err != nil {
		return "", errors.New("Git operation failed")
	}
	return string(output), nil
}

func gitProviderEnvironment(source []string) []string {
	allowed := map[string]string{}
	for _, entry := range source {
		name, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		switch name {
		case "PATH", "HOME", "LANG", "SSH_AUTH_SOCK", "SSH_AGENT_PID":
			allowed[name] = value
		}
	}
	result := make([]string, 0, len(allowed))
	for _, name := range []string{"PATH", "HOME", "LANG", "SSH_AUTH_SOCK", "SSH_AGENT_PID"} {
		if value, ok := allowed[name]; ok {
			result = append(result, name+"="+value)
		}
	}
	return result
}

func MatchesGitHubRepository(raw, repository string) bool {
	var candidate string
	if strings.HasPrefix(raw, "git@github.com:") {
		candidate = strings.TrimPrefix(raw, "git@github.com:")
	} else {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.User != nil || parsed.Host != "github.com" || (parsed.Scheme != "https" && parsed.Scheme != "ssh") || parsed.RawQuery != "" || parsed.Fragment != "" {
			return false
		}
		candidate = strings.TrimPrefix(parsed.EscapedPath(), "/")
	}
	if strings.Contains(candidate, "%") || strings.Contains(candidate, "/../") || strings.HasSuffix(candidate, "/") {
		return false
	}
	candidate = strings.TrimSuffix(candidate, ".git")
	return candidate == repository
}

func IsJobBranch(branch string) bool {
	const prefix = "factory/job/"
	if len(branch) != len(prefix)+36 || branch[:len(prefix)] != prefix {
		return false
	}
	return jobIDPattern.MatchString(branch[len(prefix):])
}
