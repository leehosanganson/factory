package factory

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWorkerMarksSnapshotRetryCapRecoverableAndResetRelaunches(t *testing.T) {
	base := canonicalTestPath(t, t.TempDir())
	bare := filepath.Join(base, "remote.git")
	repo := filepath.Join(base, "repo")
	runTestCommand(t, base, "git", "init", "--bare", bare)
	runTestCommand(t, base, "git", "clone", bare, repo)
	runTestCommand(t, repo, "git", "checkout", "-b", "main")
	runTestCommand(t, repo, "git", "config", "user.name", "Factory Test")
	runTestCommand(t, repo, "git", "config", "user.email", "factory@example.test")
	if err := os.WriteFile(filepath.Join(repo, "tracked"), []byte("clean\\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, repo, "git", "add", "tracked")
	runTestCommand(t, repo, "git", "commit", "-m", "initial")
	runTestCommand(t, repo, "git", "push", "-u", "origin", "main")
	head, err := runGit(context.Background(), repo, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "jobs")
	id := "20260518T120001-0123456789ab"
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	job := &babysitJob{ID: id, RepoRoot: repo, Repo: "team/repo", PR: 7, HeadRepo: "team/repo", HeadBranch: "main", BaseRepo: "team/repo", BaseBranch: "main", OriginURL: bare, BaselineHead: head, TargetBaseline: head, Status: "running", SnapshotFailures: 7}
	if err := saveBabysitJob(dir, job); err != nil {
		t.Fatal(err)
	}
	gh := filepath.Join(base, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
	oldDelay, oldLauncher := babysitRetryDelay, babysitWorkerLauncher
	babysitRetryDelay = func(int) time.Duration { return 0 }
	launched := 0
	babysitWorkerLauncher = func(gotID, gotRoot string) (int, error) {
		launched++
		if gotID != id || gotRoot != root {
			t.Errorf("launcher got %s %s", gotID, gotRoot)
		}
		return 0, nil
	}
	t.Cleanup(func() { babysitRetryDelay, babysitWorkerLauncher = oldDelay, oldLauncher })
	if err := runBabysitWorker(id, root, Config{Command: "/bin/true", Args: []string{"{task}", "{system_prompt}"}}); err != nil {
		t.Fatal(err)
	}
	failed, err := readBabysitJob(dir)
	if err != nil || failed.Status != "recoverable_failure" || failed.SnapshotFailures != babysitSnapshotMaxRetries {
		t.Fatalf("job did not enter recoverable failure at retry cap: job=%+v err=%v", failed, err)
	}
	if err := babysitAction("reset", root, id, Config{}, nil, io.Discard); err != nil {
		t.Fatal(err)
	}
	reset, err := readBabysitJob(dir)
	if err != nil || reset.Status != "starting" || reset.SnapshotFailures != 0 || launched != 1 {
		t.Fatalf("reset semantics failed: job=%+v launches=%d err=%v", reset, launched, err)
	}
}

func TestMonitorAgentUsesConfiguredTimeoutAndParentCancellation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ctx         func() (context.Context, context.CancelFunc)
		timeout     string
		actionLimit time.Duration
		hangAt      string
		maxElapsed  time.Duration
	}{
		{name: "configured agent timeout", ctx: func() (context.Context, context.CancelFunc) { return context.Background(), func() {} }, timeout: "100ms", hangAt: "monitor", maxElapsed: 3 * time.Second},
		{name: "monitor action ceiling", ctx: func() (context.Context, context.CancelFunc) { return context.Background(), func() {} }, timeout: "3s", actionLimit: 100 * time.Millisecond, hangAt: "monitor", maxElapsed: time.Second},
		{name: "parent cancellation", ctx: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 100*time.Millisecond)
		}, timeout: "3s", hangAt: "monitor", maxElapsed: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previousActionLimit := babysitAgentActionTimeout
			if tc.actionLimit > 0 {
				babysitAgentActionTimeout = tc.actionLimit
			}
			t.Cleanup(func() { babysitAgentActionTimeout = previousActionLimit })
			base := t.TempDir()
			bare := filepath.Join(base, "remote.git")
			repo := filepath.Join(base, "repo")
			runTestCommand(t, base, "git", "init", "--bare", bare)
			runTestCommand(t, base, "git", "clone", bare, repo)
			runTestCommand(t, repo, "git", "checkout", "-b", "feature")
			runTestCommand(t, repo, "git", "config", "user.name", "Factory Test")
			runTestCommand(t, repo, "git", "config", "user.email", "factory@example.test")
			if err := os.WriteFile(filepath.Join(repo, "tracked"), []byte("clean\\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			runTestCommand(t, repo, "git", "add", "tracked")
			runTestCommand(t, repo, "git", "commit", "-m", "initial")
			runTestCommand(t, repo, "git", "push", "-u", "origin", "feature")
			head, err := runGit(context.Background(), repo, "rev-parse", "HEAD")
			if err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(base, "job")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			script := filepath.Join(base, "agent")
			scriptBody := `#!/bin/sh
if [ "$HANG_AT" = monitor ]; then exec sleep 30; fi
printf changed > tracked
printf 'FACTORY_STATUS=FIXED\n'
exit 0
`
			if err := os.WriteFile(script, []byte(scriptBody), 0o700); err != nil {
				t.Fatal(err)
			}
			job := &babysitJob{ID: "20260518T120005-0123456789ab", Description: "monitor", RepoRoot: repo, Repo: "team/repo", PR: 7, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", OriginURL: bare, BaselineHead: head, TargetBaseline: head, Status: "running", Attempts: 1, CreatedAt: time.Now().UTC()}
			if err := saveBabysitJob(dir, job); err != nil {
				t.Fatal(err)
			}
			worktree := filepath.Join(base, "worktree")
			runTestCommand(t, repo, "git", "worktree", "add", "-b", "worker", worktree, head)
			job.Worktree, job.WorkerBranch = worktree, "worker"
			ctx, cancel := tc.ctx()
			defer cancel()
			t.Setenv("HANG_AT", tc.hangAt)
			started := time.Now()
			err = processBabysitEventContext(ctx, dir, job, Config{Command: script, Args: []string{"{stage}", "{task}", "{system_prompt}"}, AgentTimeout: tc.timeout}, &babysitSnapshot{HeadRefOID: head}, strings.Repeat("a", 64))
			if err == nil {
				t.Fatal("monitor agent unexpectedly completed")
			}
			if elapsed := time.Since(started); elapsed > tc.maxElapsed {
				t.Fatalf("canceled monitor call took %s to return", elapsed)
			}
		})
	}
}

func TestSnapshotRetryDelayIsCappedExponential(t *testing.T) {
	if got := snapshotRetryDelay(1); got != time.Second {
		t.Fatalf("first delay = %s, want 1s", got)
	}
	if got := snapshotRetryDelay(4); got != 8*time.Second {
		t.Fatalf("fourth delay = %s, want 8s", got)
	}
	if got := snapshotRetryDelay(8); got != time.Minute {
		t.Fatalf("eighth delay = %s, want capped 1m", got)
	}
}

func TestOwnedLockReclaimsOnlyDeadOwners(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "lock")
	if _, err := acquireOwnedLock(path); err != nil {
		t.Fatal(err)
	}
	if _, err := acquireOwnedLock(path); err == nil {
		t.Fatal("second owner acquired a live lock")
	}
	lockPath := filepath.Join(root, "stale")
	if err := os.Mkdir(lockPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(lockPath, "owner.json"), []byte(`{"pid":99999999}`), 0o600); err != nil {
		t.Fatal(err)
	}
	unlock, err := acquireOwnedLock(lockPath)
	if err != nil {
		t.Fatalf("dead lock was not reclaimed: %v", err)
	}
	unlock()
	unknown := filepath.Join(root, "unknown")
	if err := os.Mkdir(unknown, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := acquireOwnedLock(unknown); err == nil {
		t.Fatal("lock without verifiable owner was stolen")
	}
}

func TestDetachedWorkerLifecycleStopRecoveryAndClosedPR(t *testing.T) {
	base := canonicalTestPath(t, t.TempDir())
	bare := filepath.Join(base, "remote.git")
	repo := filepath.Join(base, "repo")
	runTestCommand(t, base, "git", "init", "--bare", bare)
	runTestCommand(t, base, "git", "clone", bare, repo)
	runTestCommand(t, repo, "git", "checkout", "-b", "feature")
	runTestCommand(t, repo, "git", "config", "user.name", "Factory Test")
	runTestCommand(t, repo, "git", "config", "user.email", "factory@example.test")
	if err := os.WriteFile(filepath.Join(repo, "tracked"), []byte("clean\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, repo, "git", "add", "tracked")
	runTestCommand(t, repo, "git", "commit", "-m", "initial")
	runTestCommand(t, repo, "git", "push", "-u", "origin", "feature")
	head, err := runGit(context.Background(), repo, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "state", "factory", "jobs")
	id := "20260518T120002-0123456789ab"
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	responsePath := filepath.Join(base, "snapshot.json")
	closedResponse := `{"number":7,"state":"CLOSED","title":"Fix","url":"https://github.com/team/repo/pull/7","headRefName":"feature","headRefOid":"` + head + `","headRepository":{"nameWithOwner":"team/repo"},"baseRefName":"main","baseRefOid":"` + head + `","comments":[],"statusCheckRollup":[]}`
	activeResponse := strings.Replace(closedResponse, `"state":"CLOSED"`, `"state":"OPEN"`, 1)
	activeResponse = strings.Replace(activeResponse, `"statusCheckRollup":[]`, `"statusCheckRollup":[{"state":"FAILURE"}]`, 1)
	if err := os.WriteFile(responsePath, []byte(activeResponse), 0o600); err != nil {
		t.Fatal(err)
	}
	gh := filepath.Join(base, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\ncase \"$*\" in *baseRepository*) echo 'unsupported JSON field: baseRepository' >&2; exit 2;; esac\n[ ! -f \"$GH_FAIL\" ] || exit 1\ncat \"$GH_RESPONSE\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	failMarker := filepath.Join(base, "gh-fail")
	t.Setenv("GH_FAIL", failMarker)
	t.Setenv("GH_RESPONSE", responsePath)
	t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FACTORY_BABYSIT_POLL_INTERVAL", "1s")
	agentStarted := filepath.Join(base, "agent-started")
	t.Setenv("AGENT_STARTED", agentStarted)
	agent := filepath.Join(base, "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\nif [ \"$1\" = monitor ]; then touch \"$AGENT_STARTED\"; exec sleep 30; fi\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	job := &babysitJob{ID: id, Description: "monitor", RepoRoot: repo, Repo: "team/repo", PR: 7, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", BaseSHA: head, OriginURL: bare, BaselineHead: head, TargetBaseline: head, Status: "running", CreatedAt: time.Now().UTC()}
	if err := saveBabysitJob(dir, job); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Command: agent, Args: []string{"{stage}", "{task}", "{system_prompt}"}, StateDir: filepath.Join(base, "state")}
	workerDone := make(chan error, 1)
	go func() { workerDone <- runBabysitWorker(id, root, cfg) }()
	waitForJobStatus(t, dir, "running")
	waitForFile(t, agentStarted)
	if err := babysitAction("stop", root, id, cfg, nil, io.Discard); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		actions, _ := os.ReadFile(filepath.Join(dir, "actions.log"))
		agentLog, _ := os.ReadFile(filepath.Join(dir, "agent.log"))
		t.Fatalf("worker did not stop promptly; actions=%s agent=%s", actions, agentLog)
	}
	stopped, _ := readBabysitJob(dir)
	if stopped.Status != "stopped" {
		t.Fatalf("worker status after stop = %q", stopped.Status)
	}
	if stopped.Worktree != "" {
		_, _ = runGit(context.Background(), repo, "worktree", "remove", "--force", stopped.Worktree)
		stopped.Worktree, stopped.WorkerBranch = "", ""
	}
	stopped.Status, stopped.StopRequested, stopped.SnapshotFailures = "running", false, 7
	if err := saveBabysitJob(dir, stopped); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(filepath.Join(dir, "stop.requested"))
	if err := os.WriteFile(failMarker, []byte("fail"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldDelay, oldLauncher := babysitRetryDelay, babysitWorkerLauncher
	babysitRetryDelay = func(int) time.Duration { return 0 }
	var nextWorker chan error
	babysitWorkerLauncher = func(gotID, gotRoot string) (int, error) {
		nextWorker = make(chan error, 1)
		go func() { nextWorker <- runBabysitWorker(gotID, gotRoot, cfg) }()
		return 0, nil
	}
	t.Cleanup(func() { babysitRetryDelay, babysitWorkerLauncher = oldDelay, oldLauncher })
	if err := runBabysitWorker(id, root, cfg); err != nil {
		t.Fatal(err)
	}
	recoverable, err := readBabysitJob(dir)
	if err != nil || recoverable.Status != "recoverable_failure" || recoverable.SnapshotFailures != 8 {
		t.Fatalf("snapshot retry cap status=%+v err=%v", recoverable, err)
	}
	if err := os.Remove(failMarker); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(responsePath, []byte(closedResponse), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := babysitAction("reset", root, id, cfg, nil, io.Discard); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-nextWorker:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reset worker did not poll closed PR")
	}
	closed, err := readBabysitJob(dir)
	if err != nil || closed.Status != "closed" {
		t.Fatalf("closed PR status=%+v err=%v", closed, err)
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("file did not appear: %s", path)
}

func waitForJobStatus(t *testing.T, dir, status string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, err := readBabysitJob(dir)
		if err == nil && job.Status == status && job.Snapshot != "" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	job, _ := readBabysitJob(dir)
	t.Fatalf("job did not reach %s with a polled snapshot: %+v", status, job)
}

func TestBabysitIDsAndAtomicMetadata(t *testing.T) {
	root := t.TempDir()
	id, err := babysitID()
	if err != nil {
		t.Fatal(err)
	}
	if !babysitIDPattern.MatchString(id) {
		t.Fatalf("generated invalid id %q", id)
	}
	if _, err := babysitJobDir(root, "../escape"); err == nil {
		t.Fatal("path traversal id accepted")
	}
	dir := filepath.Join(root, id)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	job := &babysitJob{ID: id, Description: "track PR", Status: "running", CreatedAt: time.Now().UTC()}
	if err := saveBabysitJob(dir, job); err != nil {
		t.Fatal(err)
	}
	loaded, err := readBabysitJob(dir)
	if err != nil || loaded.Description != job.Description {
		t.Fatalf("metadata round trip: %#v %v", loaded, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "job.json" {
		t.Fatalf("temporary metadata leaked: %v", entries)
	}
}

func TestBabysitApprovalBindsScopeToExactSnapshotAndInvalidatesOnChange(t *testing.T) {
	base := canonicalTestPath(t, t.TempDir())
	gh := filepath.Join(base, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\ncase \"$*\" in *baseRepository*) echo 'unsupported JSON field: baseRepository' >&2; exit 2;; esac\ncat \"$GH_RESPONSE\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
	response := `{"number":7,"state":"OPEN","title":"Fix","url":"https://github.com/team/repo/pull/7","headRefName":"feature","headRefOid":"head123","headRepository":{"nameWithOwner":"team/repo"},"baseRefName":"main","baseRefOid":"base123","comments":[],"statusCheckRollup":[]}`
	file := filepath.Join(base, "response.json")
	if err := os.WriteFile(file, []byte(response), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GH_RESPONSE", file)
	cfg := Config{StateDir: filepath.Join(base, "state")}
	root, err := babysitRoot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	id := "20260518T120003-0123456789ab"
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	job := &babysitJob{ID: id, Repo: "team/repo", PR: 7, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", BaseSHA: "base123", BaselineHead: "head123", Status: "awaiting_approval", PendingSignature: "placeholder", Proposal: "Scope requested", CreatedAt: time.Now().UTC()}
	_, signature, err := readSnapshot(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	job.PendingSignature = signature
	if err := saveBabysitJob(dir, job); err != nil {
		t.Fatal(err)
	}
	if err := babysitAction("approve", root, id, cfg, strings.NewReader("y\nFix only README typo\n"), &strings.Builder{}); err != nil {
		t.Fatal(err)
	}
	approved, err := readBabysitJob(dir)
	if err != nil {
		t.Fatal(err)
	}
	if approved.ApprovalSignature != signature || approved.ApprovalScope != "Fix only README typo" || approved.Status != "running" {
		t.Fatalf("approval not bound to exact scope/snapshot: %#v", approved)
	}
	changed := strings.Replace(response, `"comments":[]`, `"comments":[{"body":"changed"}]`, 1)
	if err := os.WriteFile(file, []byte(changed), 0o600); err != nil {
		t.Fatal(err)
	}
	approved.Status = "awaiting_approval"
	approved.PendingSignature = signature
	approved.Proposal = "Proposal for prior snapshot"
	if err := saveBabysitJob(dir, approved); err != nil {
		t.Fatal(err)
	}
	if err := babysitAction("approve", root, id, cfg, strings.NewReader("y\nBroader scope\n"), &strings.Builder{}); err == nil {
		t.Fatal("changed proposal snapshot was approved")
	}
	invalidated, err := readBabysitJob(dir)
	if err != nil {
		t.Fatal(err)
	}
	if invalidated.ApprovalSignature != "" || invalidated.ApprovalScope != "" || invalidated.Proposal != "" {
		t.Fatalf("stale approval persisted: %#v", invalidated)
	}
}

func TestBabysitSnapshotValidationAndSignature(t *testing.T) {
	root := t.TempDir()
	gh := filepath.Join(root, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\ncase \"$*\" in *baseRepository*) echo 'unsupported JSON field: baseRepository' >&2; exit 2;; esac\ncat \"$GH_RESPONSE\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	response := `{"number":7,"state":"OPEN","title":"Fix","url":"https://github.com/team/repo/pull/7","headRefName":"feature","headRefOid":"abc123","headRepository":{"nameWithOwner":"team/fork"},"baseRefName":"main","baseRefOid":"base123","comments":[],"statusCheckRollup":[]}`
	file := filepath.Join(root, "response.json")
	if err := os.WriteFile(file, []byte(response), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GH_RESPONSE", file)
	job := &babysitJob{PR: 7, Repo: "team/repo", HeadRepo: "team/fork", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", BaseSHA: "base123"}
	first, sig, err := readSnapshot(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	second, sig2, err := readSnapshot(context.Background(), job)
	if err != nil || sig != sig2 || first.HeadRefOID != second.HeadRefOID {
		t.Fatalf("identical snapshot changed: %q %q %v", sig, sig2, err)
	}
	for _, tc := range []struct {
		name, changed string
	}{
		{name: "head branch", changed: strings.Replace(response, `"headRefName":"feature"`, `"headRefName":"other"`, 1)},
		{name: "PR number", changed: strings.Replace(response, `"number":7`, `"number":8`, 1)},
		{name: "URL number", changed: strings.Replace(response, "/pull/7", "/pull/8", 1)},
		{name: "base repository", changed: strings.Replace(response, "/team/repo/pull/7", "/attacker/repo/pull/7", 1)},
		{name: "head repository", changed: strings.Replace(response, `"nameWithOwner":"team/fork"`, `"nameWithOwner":"attacker/repo"`, 1)},
		{name: "base branch", changed: strings.Replace(response, `"baseRefName":"main"`, `"baseRefName":"other"`, 1)},
		{name: "base SHA", changed: strings.Replace(response, `"baseRefOid":"base123"`, `"baseRefOid":"other123"`, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(file, []byte(tc.changed), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := readSnapshot(context.Background(), job); err == nil {
				t.Fatalf("changed %s was accepted", tc.name)
			}
		})
	}
}

func TestParsePRURL(t *testing.T) {
	for _, tc := range []struct {
		url      string
		wantRepo string
		wantNum  int
		wantErr  bool
	}{
		{url: "https://github.com/team/repo/pull/17", wantRepo: "team/repo", wantNum: 17},
		{url: "https://github.example.test/team/repo-name/pull/42/", wantRepo: "team/repo-name", wantNum: 42},
		{url: "https://github.com/team/repo/pull/0", wantErr: true},
		{url: "https://github.com/team/repo/issues/17", wantErr: true},
		{url: "https://github.com/team/repo/pull/not-a-number", wantErr: true},
		{url: "https://github.com/team/repo/pull/17?redirect=attacker", wantErr: true},
		{url: "https://github.com/team%2Fattacker/repo/pull/17", wantErr: true},
		{url: "http://github.com/team/repo/pull/17", wantErr: true},
	} {
		t.Run(tc.url, func(t *testing.T) {
			gotRepo, gotNum, err := parsePRURL(tc.url)
			if (err != nil) != tc.wantErr || gotRepo != tc.wantRepo || gotNum != tc.wantNum {
				t.Fatalf("parsePRURL(%q) = %q, %d, %v", tc.url, gotRepo, gotNum, err)
			}
		})
	}
}

func TestBabysitProtocolRequiresOneExactFinalStatus(t *testing.T) {
	for _, tc := range []struct{ out, want string }{{"tests ok\nFACTORY_STATUS=FIXED\n", "FIXED"}, {"FACTORY_STATUS=FIXED\n", "FIXED"}, {"FACTORY_STATUS=FIXED\nFACTORY_STATUS=APPROVAL_REQUIRED\n", ""}, {"FACTORY_STATUS=FIXED extra\n", ""}} {
		got, _ := agentProtocol(tc.out)
		if got != tc.want {
			t.Errorf("protocol(%q)=%q want %q", tc.out, got, tc.want)
		}
	}
}

func TestBabysitChangedPathsIncludeStagedUntrackedAndNoRenameEndpoints(t *testing.T) {
	repo := newBabysitRepo(t)
	if err := os.WriteFile(filepath.Join(repo, "tracked.go"), []byte("package test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, repo, "git", "add", "tracked.go")
	runTestCommand(t, repo, "git", "commit", "-m", "initial")
	if err := os.WriteFile(filepath.Join(repo, "tracked.go"), []byte("package changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "new file.txt"), []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	paths, err := changedPaths(repo)
	if err != nil {
		t.Fatal(err)
	}
	if !sameStringSet(paths, []string{"new file.txt", "tracked.go"}) {
		t.Fatalf("changed paths = %q", paths)
	}
	if err := os.Rename(filepath.Join(repo, "tracked.go"), filepath.Join(repo, "renamed.go")); err != nil {
		t.Fatal(err)
	}
	paths, err = changedPaths(repo)
	if err != nil {
		t.Fatal(err)
	}
	if !sameStringSet(paths, []string{"new file.txt", "renamed.go", "tracked.go"}) {
		t.Fatalf("rename endpoints missing from changed paths: %q", paths)
	}
	for _, path := range []string{"../escape", "/tmp/x", ".git/config", "a/../b"} {
		if validChangePath(path) {
			t.Errorf("unsafe path %q accepted", path)
		}
	}
	if !validChangePath("dir/with space.txt") {
		t.Fatal("valid literal path rejected")
	}
}

func TestBabysitApprovalRequiredStillPausesBeforeAnyPublish(t *testing.T) {
	base := canonicalTestPath(t, t.TempDir())
	bare := filepath.Join(base, "remote.git")
	repo := filepath.Join(base, "repo")
	runTestCommand(t, base, "git", "init", "--bare", bare)
	runTestCommand(t, base, "git", "clone", bare, repo)
	runTestCommand(t, repo, "git", "checkout", "-b", "feature")
	runTestCommand(t, repo, "git", "config", "user.name", "Factory Test")
	runTestCommand(t, repo, "git", "config", "user.email", "factory@example.test")
	if err := os.WriteFile(filepath.Join(repo, "README.md"), []byte("before\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, repo, "git", "add", "README.md")
	runTestCommand(t, repo, "git", "commit", "-m", "initial")
	runTestCommand(t, repo, "git", "push", "-u", "origin", "feature")
	head := runTestCommand(t, repo, "git", "rev-parse", "HEAD")
	jobDir := filepath.Join(base, "20260518T120000-0123456789ab")
	if err := os.MkdirAll(jobDir, 0o700); err != nil {
		t.Fatal(err)
	}
	job := &babysitJob{ID: "20260518T120000-0123456789ab", Description: "Fix docs", RepoRoot: repo, Repo: "team/repo", PR: 17, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", OriginURL: bare, HeadRepoURL: bare, BaselineHead: head, TargetBaseline: head, Status: "running", CreatedAt: time.Now().UTC()}
	if err := saveBabysitJob(jobDir, job); err != nil {
		t.Fatal(err)
	}
	worktree := filepath.Join(base, "worker")
	runTestCommand(t, repo, "git", "worktree", "add", "-b", "worker", worktree, head)
	job.Worktree, job.WorkerBranch = worktree, "worker"
	script := filepath.Join(base, "agent")
	if err := os.WriteFile(script, []byte(`#!/bin/sh
printf changed > README.md
printf 'FACTORY_PROPOSAL=Need human review\n'
printf 'FACTORY_STATUS=APPROVAL_REQUIRED\n'
`), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := processBabysitEvent(jobDir, job, Config{Command: script, Args: []string{"{stage}", "{task}", "{system_prompt}"}}, &babysitSnapshot{HeadRefOID: head}, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if job.Status != "awaiting_approval" || job.Proposal != "Need human review" {
		t.Fatalf("approval state = %q, proposal %q; want awaiting approval", job.Status, job.Proposal)
	}
	if got := runTestCommand(t, bare, "git", "rev-parse", "refs/heads/feature"); got != head {
		t.Fatalf("approval-required action changed remote: got %s, want %s", got, head)
	}
}

func TestBabysitFixedCommitsAndPushesOnlyGitDerivedChangedPaths(t *testing.T) {
	base := canonicalTestPath(t, t.TempDir())
	bare := filepath.Join(base, "remote.git")
	repo := filepath.Join(base, "repo")
	runTestCommand(t, base, "git", "init", "--bare", bare)
	runTestCommand(t, base, "git", "clone", bare, repo)
	runTestCommand(t, repo, "git", "checkout", "-b", "feature")
	runTestCommand(t, repo, "git", "config", "user.name", "Factory Test")
	runTestCommand(t, repo, "git", "config", "user.email", "factory@example.test")
	if err := os.WriteFile(filepath.Join(repo, "README.md"), []byte("before\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, repo, "git", "add", "README.md")
	runTestCommand(t, repo, "git", "commit", "-m", "initial")
	runTestCommand(t, repo, "git", "push", "-u", "origin", "feature")
	head, err := runGit(context.Background(), repo, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "state", "factory", "jobs")
	jobDir := filepath.Join(root, "20260518T120000-0123456789ab")
	if err := os.MkdirAll(jobDir, 0o700); err != nil {
		t.Fatal(err)
	}
	gh := filepath.Join(base, "gh")
	ghScript := `#!/bin/sh
case "$*" in *baseRepository*) echo 'unsupported JSON field: baseRepository' >&2; exit 2;; esac
if [ "$1" = "pr" ] && [ "$2" = "view" ]; then
  printf '%s\n' "$GH_SNAPSHOT"
  exit 0
fi
exit 1
`
	if err := os.WriteFile(gh, []byte(ghScript), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
	snapshotJSON := `{"number":17,"state":"OPEN","title":"Fix docs","url":"https://github.com/team/repo/pull/17","headRefName":"feature","headRefOid":"` + head + `","headRepository":{"nameWithOwner":"team/repo"},"baseRefName":"main","baseRefOid":"` + head + `","comments":[],"statusCheckRollup":[{"name":"ci","state":"FAILURE"}]}`
	t.Setenv("GH_SNAPSHOT", snapshotJSON)
	job := &babysitJob{ID: "20260518T120000-0123456789ab", Description: "Fix the documented typo", RepoRoot: repo, Repo: "team/repo", PR: 17, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", OriginURL: bare, HeadRepoURL: bare, BaselineHead: head, TargetBaseline: head, Status: "running", CreatedAt: time.Now().UTC()}
	snap, sig, err := readSnapshot(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	job.Snapshot = sig
	if err := saveBabysitJob(jobDir, job); err != nil {
		t.Fatal(err)
	}
	agent := filepath.Join(base, "agent")
	script := `#!/bin/sh
stage="$1"
if [ "$stage" = "monitor" ]; then
  printf 'after\n' > README.md
  printf 'FACTORY_STATUS=FIXED\n'
  exit 0
fi
exit 9
`
	script = strings.ReplaceAll(script, `\\n`, `\n`)
	if err := os.WriteFile(agent, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Command: agent, Args: []string{"{stage}", "{task}", "{system_prompt}"}, StateDir: filepath.Join(base, "state")}
	if err := processBabysitEvent(jobDir, job, cfg, snap, sig); err != nil {
		t.Fatal(err)
	}
	remoteHead, err := runGit(context.Background(), bare, "rev-parse", "refs/heads/feature")
	if err != nil {
		t.Fatal(err)
	}
	if remoteHead == head {
		log, _ := os.ReadFile(filepath.Join(jobDir, "actions.log"))
		agentLog, _ := os.ReadFile(filepath.Join(jobDir, "agent.log"))
		t.Fatalf("FIXED changes were not pushed; actions=%s agent=%s", log, agentLog)
	}
	if _, err := os.Stat(filepath.Join(jobDir, "evaluation.log")); !os.IsNotExist(err) {
		t.Fatalf("runtime evaluator transcript exists: %v", err)
	}
	message, err := runGit(context.Background(), repo, "show", "-s", "--format=%s", remoteHead)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(message, "fix: address PR feedback") {
		t.Fatalf("unexpected pushed commit %q", message)
	}
	if job.BaselineHead != remoteHead {
		t.Fatalf("job baseline %q not advanced to %q", job.BaselineHead, remoteHead)
	}
	if _, err := os.Stat(filepath.Join(repo, "README.md")); err != nil {
		t.Fatal(err)
	}
}

func TestBabysitBadOriginAndBranchBlockAutomaticCommit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		alter func(t *testing.T, repo, worktree string)
	}{
		{name: "bad target origin", alter: func(t *testing.T, repo, worktree string) {
			runTestCommand(t, repo, "git", "remote", "set-url", "origin", "https://github.com/attacker/repo.git")
		}},
		{name: "bad worker branch", alter: func(t *testing.T, repo, worktree string) {
			runTestCommand(t, worktree, "git", "checkout", "-b", "attacker")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := canonicalTestPath(t, t.TempDir())
			bare := filepath.Join(base, "remote.git")
			repo := filepath.Join(base, "repo")
			runTestCommand(t, base, "git", "init", "--bare", bare)
			runTestCommand(t, base, "git", "clone", bare, repo)
			runTestCommand(t, repo, "git", "checkout", "-b", "feature")
			runTestCommand(t, repo, "git", "config", "user.name", "Test")
			runTestCommand(t, repo, "git", "config", "user.email", "test@example.test")
			if err := os.WriteFile(filepath.Join(repo, "f"), []byte("one\\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			runTestCommand(t, repo, "git", "add", "f")
			runTestCommand(t, repo, "git", "commit", "-m", "initial")
			runTestCommand(t, repo, "git", "push", "-u", "origin", "feature")
			head, _ := runGit(context.Background(), repo, "rev-parse", "HEAD")
			worktree := filepath.Join(base, "worker")
			runTestCommand(t, repo, "git", "worktree", "add", "-b", "worker", worktree, head)
			if err := os.WriteFile(filepath.Join(worktree, "f"), []byte("two\\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(base, "jobs", "20260518T120002-0123456789ab")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			job := &babysitJob{ID: filepath.Base(dir), RepoRoot: repo, Repo: "team/repo", PR: 17, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", OriginURL: bare, HeadRepoURL: bare, BaselineHead: head, TargetBaseline: head, Worktree: worktree, WorkerBranch: "worker", Snapshot: "sig"}
			if err := saveBabysitJob(dir, job); err != nil {
				t.Fatal(err)
			}
			tc.alter(t, repo, worktree)
			if err := guardedCommitPush(dir, job, "sig", &babysitSnapshot{HeadRefOID: head}, []string{"f"}); err == nil {
				t.Fatal("unsafe commit/push accepted")
			}
			got, _ := runGit(context.Background(), bare, "rev-parse", "refs/heads/feature")
			if got != head {
				t.Fatalf("remote changed despite invalid validation: %s", got)
			}
		})
	}
}

func TestBabysitStopBeforeCommitDoesNotCreateCommit(t *testing.T) {
	base := canonicalTestPath(t, t.TempDir())
	bare := filepath.Join(base, "remote.git")
	repo := filepath.Join(base, "repo")
	runTestCommand(t, base, "git", "init", "--bare", bare)
	runTestCommand(t, base, "git", "clone", bare, repo)
	runTestCommand(t, repo, "git", "checkout", "-b", "feature")
	runTestCommand(t, repo, "git", "config", "user.name", "Test")
	runTestCommand(t, repo, "git", "config", "user.email", "test@example.test")
	if err := os.WriteFile(filepath.Join(repo, "f"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, repo, "git", "add", "f")
	runTestCommand(t, repo, "git", "commit", "-m", "initial")
	runTestCommand(t, repo, "git", "push", "-u", "origin", "feature")
	head, err := runGit(context.Background(), repo, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	worktree := filepath.Join(base, "worker")
	runTestCommand(t, repo, "git", "worktree", "add", "-b", "worker", worktree, head)
	if err := os.WriteFile(filepath.Join(worktree, "f"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "jobs")
	dir := filepath.Join(root, "20260518T120001-0123456789ab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	job := &babysitJob{ID: filepath.Base(dir), RepoRoot: repo, Repo: "team/repo", PR: 17, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", OriginURL: bare, HeadRepoURL: bare, BaselineHead: head, TargetBaseline: head, Worktree: worktree, WorkerBranch: "worker", Snapshot: "sig", StopRequested: true}
	if err := saveBabysitJob(dir, job); err != nil {
		t.Fatal(err)
	}
	if err := guardedCommitPush(dir, job, "sig", &babysitSnapshot{HeadRefOID: head}, []string{"f"}); err == nil {
		t.Fatal("stop request did not block commit")
	}
	got, err := runGit(context.Background(), worktree, "rev-parse", "HEAD")
	if err != nil || got != head {
		t.Fatalf("local commit occurred despite stop: %s %v", got, err)
	}
	remote, err := runGit(context.Background(), bare, "rev-parse", "refs/heads/feature")
	if err != nil || remote != head {
		t.Fatalf("remote changed despite stop: %s %v", remote, err)
	}
}

func TestBabysitRejectsDifferentConfiguredPushURL(t *testing.T) {
	fixture := newMonitorPushFixture(t)
	wrongRemote := filepath.Join(fixture.base, "wrong.git")
	runTestCommand(t, fixture.base, "git", "init", "--bare", wrongRemote)
	runTestCommand(t, fixture.repo, "git", "config", "remote.origin.pushurl", wrongRemote)

	err := guardedCommitPush(fixture.dir, fixture.job, fixture.signature, fixture.snapshot, []string{"f"})
	if err == nil || !strings.Contains(err.Error(), "push URL does not point") {
		t.Fatalf("different configured push URL was not rejected: %v", err)
	}
	got, err := runGit(context.Background(), wrongRemote, "rev-parse", "--verify", "refs/heads/feature")
	if err == nil {
		t.Fatalf("unsafe destination received commit %s", got)
	}
	got, err = runGit(context.Background(), fixture.bare, "rev-parse", "refs/heads/feature")
	if err != nil || got != fixture.head {
		t.Fatalf("validated remote changed: head=%s err=%v", got, err)
	}
}

func TestBabysitRejectsPushURLRewrites(t *testing.T) {
	for _, rewrite := range []string{"insteadOf", "pushInsteadOf"} {
		t.Run(rewrite, func(t *testing.T) {
			fixture := newMonitorPushFixture(t)
			pushURL := fixture.bare
			redirect := filepath.Join(fixture.base, "redirect")
			runTestCommand(t, fixture.repo, "git", "config", "remote.origin.pushurl", pushURL)
			runTestCommand(t, fixture.repo, "git", "config", "url."+redirect+"."+rewrite, fixture.bare)
			if _, err := validatedPushURL(fixture.job.Worktree, fixture.job); err == nil || !strings.Contains(err.Error(), "URL rewrite") {
				t.Fatalf("applicable %s rewrite was not rejected: %v", rewrite, err)
			}
			got, err := runGit(context.Background(), fixture.bare, "rev-parse", "refs/heads/feature")
			if err != nil || got != fixture.head {
				t.Fatalf("rewritten push changed validated remote: head=%s err=%v", got, err)
			}
		})
	}
}

func TestBabysitPushLeaseRejectsRemoteHeadRace(t *testing.T) {
	fixture := newMonitorPushFixture(t)
	competitor := filepath.Join(fixture.base, "competitor")
	runTestCommand(t, fixture.base, "git", "clone", fixture.bare, competitor)
	runTestCommand(t, competitor, "git", "checkout", "feature")
	runTestCommand(t, competitor, "git", "config", "user.name", "Concurrent Writer")
	runTestCommand(t, competitor, "git", "config", "user.email", "writer@example.test")
	if err := os.WriteFile(filepath.Join(competitor, "race"), []byte("concurrent update\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	wrapper := filepath.Join(fixture.base, "git")
	marker := filepath.Join(fixture.base, "race-pushed")
	wrapperScript := "#!/bin/sh\nfor arg do\n  if [ \"$arg\" = push ] && [ ! -e '" + marker + "' ]; then\n    touch '" + marker + "'\n    '" + gitPath + "' -C '" + competitor + "' add race\n    '" + gitPath + "' -C '" + competitor + "' commit -m 'concurrent update'\n    '" + gitPath + "' -C '" + competitor + "' push origin feature\n    break\n  fi\ndone\nexec '" + gitPath + "' \"$@\"\n"
	if err := os.WriteFile(wrapper, []byte(wrapperScript), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", fixture.base+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := guardedCommitPush(fixture.dir, fixture.job, fixture.signature, fixture.snapshot, []string{"f"}); err == nil {
		t.Fatal("push with stale expected-old head unexpectedly succeeded")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("competing update did not occur at push boundary: %v", err)
	}
	concurrentHead, err := runGit(context.Background(), fixture.bare, "rev-parse", "refs/heads/feature")
	if err != nil || concurrentHead == fixture.head {
		t.Fatalf("concurrent writer did not advance remote: head=%s err=%v", concurrentHead, err)
	}
	got, err := runGit(context.Background(), fixture.bare, "rev-parse", "refs/heads/feature")
	if err != nil || got != concurrentHead {
		t.Fatalf("stale work replaced concurrent remote head: got=%s want=%s err=%v", got, concurrentHead, err)
	}
}

type monitorPushFixture struct {
	base, bare, repo, dir string
	job                   *babysitJob
	snapshot              *babysitSnapshot
	signature, head       string
}

func newMonitorPushFixture(t *testing.T) monitorPushFixture {
	t.Helper()
	base := canonicalTestPath(t, t.TempDir())
	bare, repo := filepath.Join(base, "remote.git"), filepath.Join(base, "repo")
	runTestCommand(t, base, "git", "init", "--bare", bare)
	runTestCommand(t, base, "git", "clone", bare, repo)
	runTestCommand(t, repo, "git", "checkout", "-b", "feature")
	runTestCommand(t, repo, "git", "config", "user.name", "Factory Test")
	runTestCommand(t, repo, "git", "config", "user.email", "factory@example.test")
	if err := os.WriteFile(filepath.Join(repo, "f"), []byte("before\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, repo, "git", "add", "f")
	runTestCommand(t, repo, "git", "commit", "-m", "initial")
	runTestCommand(t, repo, "git", "push", "-u", "origin", "feature")
	head, err := runGit(context.Background(), repo, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	gh := filepath.Join(base, "gh")
	ghJSON := `{"number":17,"state":"OPEN","title":"Fix","url":"https://github.com/team/repo/pull/17","headRefName":"feature","headRefOid":"` + head + `","headRepository":{"nameWithOwner":"team/repo"},"baseRefName":"main","baseRefOid":"` + head + `","comments":[],"statusCheckRollup":[{"name":"ci","state":"FAILURE"}]}`
	ghScript := "#!/bin/sh\nif [ \"$1\" = pr ] && [ \"$2\" = view ]; then printf '%s\\n' '" + ghJSON + "'; exit 0; fi\nexit 1\n"
	if err := os.WriteFile(gh, []byte(ghScript), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
	dir := filepath.Join(base, "jobs", "20260518T120003-0123456789ab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	snapshot, signature, err := readSnapshot(context.Background(), &babysitJob{Repo: "team/repo", PR: 17, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	job := &babysitJob{ID: filepath.Base(dir), RepoRoot: repo, Repo: "team/repo", PR: 17, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", OriginURL: bare, HeadRepoURL: bare, BaselineHead: head, TargetBaseline: head, Worktree: filepath.Join(base, "worker"), WorkerBranch: "worker", Snapshot: signature}
	runTestCommand(t, repo, "git", "worktree", "add", "-b", job.WorkerBranch, job.Worktree, head)
	if err := os.WriteFile(filepath.Join(job.Worktree, "f"), []byte("agent update\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := saveBabysitJob(dir, job); err != nil {
		t.Fatal(err)
	}
	return monitorPushFixture{base: base, bare: bare, repo: repo, dir: dir, job: job, snapshot: snapshot, signature: signature, head: head}
}

func newBabysitRepo(t *testing.T) string {
	t.Helper()
	repo := filepath.Join(t.TempDir(), "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, repo, "git", "init", "-b", "main")
	runTestCommand(t, repo, "git", "config", "user.name", "Test")
	runTestCommand(t, repo, "git", "config", "user.email", "test@example.test")
	return repo
}

func runTestCommand(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

var _ = json.Valid
