package factory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	prReferencePattern = regexp.MustCompile(`(?i)(https://github\.com/[^\s]+/pull/[0-9]+|\b(?:pull\s+request|pr)\s+#([0-9]+)\b)`)
)

func parsePRReference(description string) (int, string, error) {
	matches := prReferencePattern.FindAllStringSubmatch(description, -1)
	if len(matches) == 0 {
		return 0, "", nil
	}
	selectedNumber, selectedRepo := 0, ""
	for _, match := range matches {
		candidate := strings.TrimRight(match[1], ".,;:!?)]}\"'")
		repo, number, err := parsePRURL(candidate)
		if err != nil {
			if match[2] == "" {
				return 0, "", fmt.Errorf("invalid GitHub pull request URL in task description")
			}
			number, err = strconv.Atoi(match[2])
			if err != nil || number < 1 {
				return 0, "", fmt.Errorf("invalid pull request number in task description")
			}
			repo = ""
		}
		if selectedNumber != 0 && (selectedNumber != number || (selectedRepo != "" && repo != "" && !strings.EqualFold(selectedRepo, repo))) {
			return 0, "", fmt.Errorf("task description contains competing pull request references; specify only one PR")
		}
		selectedNumber = number
		if repo != "" {
			selectedRepo = repo
		}
	}
	return selectedNumber, selectedRepo, nil
}

func repositoryIdentity(repository string) (string, error) {
	commonDir, err := runGit(context.Background(), repository, "rev-parse", "--git-common-dir")
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(commonDir) {
		commonDir = filepath.Join(repository, commonDir)
	}
	return canonicalPath(commonDir)
}

func startMonitor(args []string, cfg Config, workdir, root string, out io.Writer) error {
	description := strings.TrimSpace(strings.Join(args, " "))
	if description == "" {
		return fmt.Errorf("description cannot be empty")
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	poll := os.Getenv("FACTORY_MONITOR_POLL_INTERVAL")
	if poll != "" {
		if parsed, err := time.ParseDuration(poll); err != nil || parsed < time.Second {
			return fmt.Errorf("FACTORY_MONITOR_POLL_INTERVAL must be at least 1s")
		}
	}
	repository, err := runGit(context.Background(), workdir, "rev-parse", "--show-toplevel")
	if err != nil {
		return fmt.Errorf("not inside a git repository: %w", err)
	}
	repository, err = canonicalPath(repository)
	if err != nil {
		return err
	}
	primary, err := primaryWorktree(repository)
	if err != nil {
		return err
	}
	commonRepository, err := repositoryIdentity(repository)
	if err != nil {
		return err
	}
	statePath, err := canonicalPath(root)
	if err != nil {
		return err
	}
	if isWithin(repository, statePath) {
		return fmt.Errorf("monitor state must be outside target repository")
	}
	origin, err := runGit(context.Background(), primary, "remote", "get-url", "origin")
	if err != nil {
		return fmt.Errorf("origin remote is required: %w", err)
	}
	baseRepo := repoNameFromURL(origin)
	if baseRepo == "" {
		return fmt.Errorf("cannot determine GitHub repository from origin URL")
	}
	selected, selectedRepo, err := parsePRReference(description)
	if err != nil {
		return err
	}
	var info monitorSnapshot
	if selected != 0 {
		if selectedRepo != "" && !strings.EqualFold(selectedRepo, baseRepo) {
			return fmt.Errorf("GitHub PR URL repository %s does not match origin repository %s", selectedRepo, baseRepo)
		}
		if selectedRepo == "" {
			selectedRepo = baseRepo
		}
		data, err := runGH(context.Background(), "pr", "view", strconv.Itoa(selected), "--repo", selectedRepo, "--json", "number,state,url,headRefName,headRefOid,headRepository,baseRefName,baseRefOid")
		if err != nil {
			return fmt.Errorf("cannot read selected PR #%d: %w", selected, err)
		}
		if err := json.Unmarshal(data, &info); err != nil {
			return err
		}
		gotRepo, gotNumber, parseErr := parsePRURL(info.URL)
		if parseErr != nil || gotNumber != selected || !strings.EqualFold(gotRepo, selectedRepo) {
			return fmt.Errorf("selected PR reference does not match returned PR metadata")
		}
	} else {
		branch, branchErr := runGit(context.Background(), workdir, "branch", "--show-current")
		if branchErr != nil || branch == "" {
			return fmt.Errorf("no explicit PR reference supplied and no local PR branch is checked out; specify a GitHub PR URL or PR number")
		}
		if _, localErr := runGit(context.Background(), workdir, "show-ref", "--verify", "--quiet", "refs/heads/"+branch); localErr != nil {
			return fmt.Errorf("no explicit PR reference supplied and local branch %q does not exist", branch)
		}
		data, err := runGH(context.Background(), "pr", "view", "--json", "number,state,url,headRefName,headRefOid,headRepository,baseRefName,baseRefOid")
		if err != nil {
			return fmt.Errorf("no explicit PR reference supplied and current local branch %q has no identifiable PR; specify a GitHub PR URL or PR number", branch)
		}
		if err := json.Unmarshal(data, &info); err != nil {
			return err
		}
		if info.HeadRefName != branch {
			return fmt.Errorf("current local branch %q does not match the identified PR branch", branch)
		}
	}
	prBase, number, err := parsePRURL(info.URL)
	if err != nil || info.Number <= 0 || number != info.Number || !strings.EqualFold(info.State, "OPEN") || info.HeadRefName == "" || info.HeadRefOID == "" || info.BaseRefOID == "" || info.HeadRepository == nil {
		return fmt.Errorf("selected PR metadata is incomplete or closed")
	}
	baseRepo = prBase
	headRepo := info.HeadRepository.NameWithOwner
	repoData, err := runGH(context.Background(), "repo", "view", headRepo, "--json", "url,sshUrl")
	if err != nil {
		return fmt.Errorf("cannot validate PR head repository URL: %w", err)
	}
	var repoURL ghRepository
	if err := json.Unmarshal(repoData, &repoURL); err != nil {
		return err
	}
	if !sameRepoURL(origin, repoURL.URL) && !sameRepoURL(origin, repoURL.SSHURL) {
		return fmt.Errorf("origin does not point to PR head repository %s", headRepo)
	}
	store, err := NewJobStore(root)
	if err != nil {
		return err
	}
	unlockAdmission, err := store.LockRepositoryAdmission(commonRepository)
	if err != nil {
		return err
	}
	defer unlockAdmission()
	branchUnlock, acquired, err := store.TryLockBranch(commonRepository, info.HeadRefName)
	if err != nil {
		return err
	}
	if !acquired {
		return fmt.Errorf("PR branch %q is already reserved by detached work", info.HeadRefName)
	}
	defer branchUnlock()
	jobs, err := store.ListJobs()
	if err != nil {
		return err
	}
	for _, existing := range jobs {
		if existing.Type == monitorJobType && existing.Monitor != nil && existing.Monitor.RepoRoot == primary && existing.Monitor.PR == info.Number && isMonitorActive(existing.Status) && (existing.Status == "queued" || workerFresh(*existing.Monitor) && processAlive(existing.Monitor.PID)) {
			return fmt.Errorf("an active monitor already monitors %s#%d (%s)", baseRepo, info.Number, existing.ID)
		}
		if existing.Type == implementationJobType && existing.RepositoryPath == commonRepository && existing.TargetBranch == info.HeadRefName && !isTerminalStatus(existing.Status) {
			return fmt.Errorf("PR branch %q is reserved by implementation job %s", info.HeadRefName, existing.ID)
		}
	}
	if err := preparePrimaryForMonitor(repository, primary, info.HeadRefName); err != nil {
		return err
	}
	targetBranch, err := runGit(context.Background(), primary, "branch", "--show-current")
	if err != nil {
		return err
	}
	targetHead, err := runGit(context.Background(), primary, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	baseline := info.HeadRefOID
	id, err := monitorID()
	if err != nil {
		return err
	}
	dir := filepath.Join(store.Root(), id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	job := &monitorJob{ID: id, Description: description, RepoRoot: primary, Repo: baseRepo, PR: info.Number, HeadRepo: headRepo, HeadBranch: info.HeadRefName, BaseRepo: baseRepo, BaseBranch: info.BaseRefName, BaseSHA: info.BaseRefOID, OriginURL: origin, HeadRepoURL: repoURL.URL, HeartbeatPath: filepath.Join(dir, "heartbeat"), BaselineHead: baseline, TargetBaseline: targetHead, TargetBranch: targetBranch, Status: "queued", CreatedAt: time.Now().UTC()}
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TaskDescription: description, TargetPath: primary, RepositoryPath: commonRepository, Status: "queued", Monitor: job}); err != nil {
		return err
	}
	if _, err := store.CreateSession(id, monitorSessionID, "queued"); err != nil {
		return err
	}
	_ = appendMonitorLog(dir, "PR metadata registered; deferred branch setup until an actionable event.")
	pid, err := monitorWorkerLauncher(id, root)
	if err != nil {
		job.Status = "failed"
		_ = saveMonitorJob(dir, job)
		return err
	}
	if err := registerMonitorWorkerIfMissing(id, dir, pid); err != nil {
		return err
	}
	fmt.Fprintf(out, "Started monitor %s for %s#%d (%s).\n", id, baseRepo, info.Number, info.HeadRefName)
	return nil
}

func primaryWorktree(repository string) (string, error) {
	data, err := runGit(context.Background(), repository, "worktree", "list", "--porcelain")
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(data, "\n") {
		if strings.HasPrefix(line, "worktree ") {
			return canonicalPath(strings.TrimPrefix(line, "worktree "))
		}
	}
	return "", fmt.Errorf("cannot identify primary checkout")
}

func repoNameFromURL(raw string) string {
	value := strings.TrimSpace(raw)
	if strings.Contains(value, "://") {
		u, err := url.Parse(value)
		if err != nil {
			return ""
		}
		value = u.Path
	} else if at := strings.LastIndex(value, ":"); at >= 0 {
		value = value[at+1:]
	}
	value = strings.Trim(strings.TrimSuffix(value, ".git"), "/")
	parts := strings.Split(value, "/")
	if len(parts) < 2 {
		return ""
	}
	return parts[len(parts)-2] + "/" + parts[len(parts)-1]
}

func preparePrimaryForMonitor(repository, primary, prBranch string) error {
	branch, err := runGit(context.Background(), primary, "branch", "--show-current")
	if err != nil {
		return err
	}
	if branch == prBranch {
		status, err := runGit(context.Background(), primary, "status", "--porcelain")
		if err != nil {
			return err
		}
		if status != "" {
			return fmt.Errorf("primary checkout has dirty changes on PR branch %q; clean it before monitoring", prBranch)
		}
		defaultName, err := defaultBranch(repository)
		if err != nil || defaultName == prBranch {
			return fmt.Errorf("cannot determine a distinct default branch for primary checkout")
		}
		if _, err := runGit(context.Background(), primary, "switch", defaultName); err != nil {
			return fmt.Errorf("switch primary checkout to default branch %q: %w", defaultName, err)
		}
	}
	status, err := runGit(context.Background(), primary, "status", "--porcelain")
	if err != nil {
		return err
	}
	if status != "" {
		return fmt.Errorf("primary checkout has dirty changes; clean it before monitoring")
	}
	return nil
}

func worktreeBranchOwner(repository, branch string) (string, bool, error) {
	data, err := runGit(context.Background(), repository, "worktree", "list", "--porcelain")
	if err != nil {
		return "", false, err
	}
	path := ""
	for _, line := range strings.Split(data, "\n") {
		if strings.HasPrefix(line, "worktree ") {
			path = strings.TrimPrefix(line, "worktree ")
			continue
		}
		if line == "" {
			path = ""
			continue
		}
		if line == "branch refs/heads/"+branch {
			canonical, err := canonicalPath(path)
			return canonical, true, err
		}
	}
	return "", false, nil
}

func monitorPRWorktreePath(root, repositoryIdentityValue, branch string, pr int) string {
	sum := sha256.Sum256([]byte(repositoryIdentityValue + "\x00" + strconv.Itoa(pr)))
	return filepath.Join(root, "monitor-pr", safeBranchPathComponent(branch)+"-"+hex.EncodeToString(sum[:]), "checkout")
}

func safeBranchPathComponent(branch string) string {
	var builder strings.Builder
	lastDash := false
	for _, char := range branch {
		valid := char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-'
		if valid {
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
	value := strings.Trim(builder.String(), ".-_-")
	if value == "" {
		return "branch"
	}
	return value
}

func setupExistingPRWorktree(store *JobStore, repository, primary, branch, head string, pr int, createIfAbsent bool) (string, string, error) {
	local, localErr := runGit(context.Background(), repository, "rev-parse", "refs/heads/"+branch)
	if localErr != nil {
		if !createIfAbsent {
			return "", "", nil
		}
		remote, err := runGit(context.Background(), repository, "remote", "get-url", "origin")
		if err != nil {
			return "", "", err
		}
		if _, err := runGit(context.Background(), repository, "fetch", "--no-tags", remote, "refs/heads/"+branch); err != nil {
			return "", "", fmt.Errorf("fetch PR branch after actionable event: %w", err)
		}
		fetched, err := runGit(context.Background(), repository, "rev-parse", "FETCH_HEAD")
		if err != nil {
			return "", "", err
		}
		if fetched != head {
			return "", "", errors.New("remote PR head does not match validated GitHub head")
		}
		if _, err := runGit(context.Background(), repository, "branch", branch, head); err != nil {
			return "", "", fmt.Errorf("create local PR branch: %w", err)
		}
		local = head
	}
	if local != head {
		if _, err := runGit(context.Background(), repository, "merge-base", "--is-ancestor", local, head); err != nil {
			return "", "", fmt.Errorf("local PR branch %q is ahead of or divergent from validated PR head; refusing stale branch", branch)
		}
	}
	remote, err := runGit(context.Background(), repository, "remote", "get-url", "origin")
	if err != nil {
		return "", "", err
	}
	if _, err := runGit(context.Background(), repository, "fetch", "--no-tags", remote, "refs/heads/"+branch); err != nil {
		return "", "", fmt.Errorf("fetch validated PR branch: %w", err)
	}
	fetched, err := runGit(context.Background(), repository, "rev-parse", "FETCH_HEAD")
	if err != nil {
		return "", "", err
	}
	if fetched != head {
		return "", "", errors.New("remote PR head does not match the validated GitHub head; refusing stale branch")
	}
	owner, exists, err := worktreeBranchOwner(repository, branch)
	if err != nil {
		return "", "", err
	}
	if exists {
		status, err := runGit(context.Background(), owner, "status", "--porcelain")
		if err != nil {
			return "", "", err
		}
		if status != "" {
			return "", "", fmt.Errorf("worktree %s owning PR branch %q is dirty", owner, branch)
		}
		current, err := runGit(context.Background(), owner, "rev-parse", "HEAD")
		if err != nil {
			return "", "", err
		}
		if current != head {
			if _, err := runGit(context.Background(), owner, "merge", "--ff-only", head); err != nil {
				return "", "", fmt.Errorf("fast-forward PR worktree: %w", err)
			}
		}
		return owner, head, nil
	}
	if local != head && !exists {
		if _, err := runGit(context.Background(), repository, "update-ref", "refs/heads/"+branch, head, local); err != nil {
			return "", "", fmt.Errorf("fast-forward local PR branch: %w", err)
		}
	}
	identity, err := repositoryIdentity(repository)
	if err != nil {
		return "", "", err
	}
	path := monitorPRWorktreePath(store.Root(), identity, branch, pr)
	canonicalRoot, err := canonicalPath(store.Root())
	if err != nil {
		return "", "", err
	}
	canonicalParent, err := canonicalPath(filepath.Dir(path))
	if err != nil {
		return "", "", err
	}
	if !isWithin(canonicalRoot, canonicalParent) {
		return "", "", fmt.Errorf("monitor worktree path escapes job state root")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", "", err
	}
	if _, err := runGit(context.Background(), repository, "worktree", "add", path, branch); err != nil {
		return "", "", fmt.Errorf("create linked worktree for PR branch: %w", err)
	}
	return path, head, nil
}
