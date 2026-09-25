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
	base := t.TempDir()
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

func TestBabysitAgentUsesConfiguredTimeoutAndParentCancellation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ctx        func() (context.Context, context.CancelFunc)
		timeout    string
		hangAt     string
		maxElapsed time.Duration
	}{
		{name: "configured agent timeout", ctx: func() (context.Context, context.CancelFunc) { return context.Background(), func() {} }, timeout: "100ms", hangAt: "babysit", maxElapsed: 3 * time.Second},
		{name: "configured evaluator timeout", ctx: func() (context.Context, context.CancelFunc) { return context.Background(), func() {} }, timeout: "100ms", hangAt: "evaluate", maxElapsed: 3 * time.Second},
		{name: "parent cancellation", ctx: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 100*time.Millisecond)
		}, timeout: "3s", hangAt: "babysit", maxElapsed: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
if [ "$1" = babysit ]; then
  if [ "$HANG_AT" = babysit ]; then exec sleep 30; fi
  printf changed > tracked
  printf 'FACTORY_STATUS=FIXED\n'
  exit 0
fi
exec sleep 30
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
				t.Fatal("babysit agent unexpectedly completed")
			}
			if elapsed := time.Since(started); elapsed > tc.maxElapsed {
				t.Fatalf("canceled babysit call took %s to return", elapsed)
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
	base := t.TempDir()
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
	closedResponse := `{"number":7,"state":"CLOSED","title":"Fix","url":"https://example.test/pr/7","headRefName":"feature","headRefOid":"` + head + `","headRepository":{"nameWithOwner":"team/repo"},"baseRefName":"main","baseRefOid":"` + head + `","baseRepository":{"nameWithOwner":"team/repo"},"comments":[],"statusCheckRollup":[]}`
	activeResponse := strings.Replace(closedResponse, `"state":"CLOSED"`, `"state":"OPEN"`, 1)
	activeResponse = strings.Replace(activeResponse, `"statusCheckRollup":[]`, `"statusCheckRollup":[{"state":"FAILURE"}]`, 1)
	if err := os.WriteFile(responsePath, []byte(activeResponse), 0o600); err != nil {
		t.Fatal(err)
	}
	gh := filepath.Join(base, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\n[ ! -f \"$GH_FAIL\" ] || exit 1\ncat \"$GH_RESPONSE\"\n"), 0o700); err != nil {
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
	if err := os.WriteFile(agent, []byte("#!/bin/sh\nif [ \"$1\" = babysit ]; then touch \"$AGENT_STARTED\"; exec sleep 30; fi\n"), 0o700); err != nil {
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
	base := t.TempDir()
	gh := filepath.Join(base, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\ncat \"$GH_RESPONSE\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
	response := `{"number":7,"state":"OPEN","title":"Fix","url":"https://example.test/pr/7","headRefName":"feature","headRefOid":"head123","headRepository":{"nameWithOwner":"team/repo"},"baseRefName":"main","baseRefOid":"base123","baseRepository":{"nameWithOwner":"team/repo"},"comments":[],"statusCheckRollup":[]}`
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
	if err := os.WriteFile(gh, []byte("#!/bin/sh\ncat \"$GH_RESPONSE\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	response := `{"number":7,"state":"OPEN","title":"Fix","url":"https://example.test/pr/7","headRefName":"feature","headRefOid":"abc123","headRepository":{"nameWithOwner":"team/repo"},"baseRefName":"main","baseRefOid":"base123","baseRepository":{"nameWithOwner":"team/repo"},"comments":[],"statusCheckRollup":[]}`
	file := filepath.Join(root, "response.json")
	if err := os.WriteFile(file, []byte(response), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GH_RESPONSE", file)
	job := &babysitJob{PR: 7, Repo: "team/repo", HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main"}
	first, sig, err := readSnapshot(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	second, sig2, err := readSnapshot(context.Background(), job)
	if err != nil || sig != sig2 || first.HeadRefOID != second.HeadRefOID {
		t.Fatalf("identical snapshot changed: %q %q %v", sig, sig2, err)
	}
	invalid := strings.Replace(response, `"headRefName":"feature"`, `"headRefName":"other"`, 1)
	if err := os.WriteFile(file, []byte(invalid), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readSnapshot(context.Background(), job); err == nil {
		t.Fatal("changed PR branch was accepted")
	}
}

func TestBabysitProtocolAndEvaluatorRequireExactFirstVerdict(t *testing.T) {
	for _, tc := range []struct{ out, want string }{{"tests ok\nFACTORY_STATUS=FIXED\n", "FIXED"}, {"FACTORY_STATUS=FIXED\n", "FIXED"}, {"FACTORY_STATUS=FIXED\nFACTORY_STATUS=APPROVAL_REQUIRED\n", ""}, {"FACTORY_STATUS=FIXED extra\n", ""}} {
		got, _ := agentProtocol(tc.out)
		if got != tc.want {
			t.Errorf("protocol(%q)=%q want %q", tc.out, got, tc.want)
		}
	}
	if got, _ := parseEvaluation("PASS\nFACTORY_FILES=[\"main.go\"]\n"); got != "PASS" {
		t.Fatalf("exact verdict rejected: %s", got)
	}
	for _, out := range []string{" \t\n\t\nPASS\nFACTORY_FILES=[]\n", "\n  \r\nPASS\nFACTORY_FILES=[]\n"} {
		if got, _ := parseEvaluation(out); got != "PASS" {
			t.Errorf("verdict after whitespace-only lines rejected: %q => %q", out, got)
		}
	}
	for _, out := range []string{" PASS\nFACTORY_FILES=[]\n", "prefix\nPASS\nFACTORY_FILES=[]\n", "PASS extra\nFACTORY_FILES=[]\n", " \t\nPASS \t\nFACTORY_FILES=[]\n"} {
		if got, _ := parseEvaluation(out); got == "PASS" {
			t.Errorf("non-exact verdict accepted: %q", out)
		}
	}
	if got, files := parseEvaluation("PASS\nFACTORY_FILES=[\"a.go\"]\n"); got != "PASS" || len(files) != 1 || files[0] != "a.go" {
		t.Fatalf("evaluator path protocol parsed as %q %v", got, files)
	}
}

func TestBabysitChangedPathsIncludeStagedAndUntrackedAndRejectUnsafeNames(t *testing.T) {
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
	for _, path := range []string{"../escape", "/tmp/x", ".git/config", "a/../b"} {
		if validChangePath(path) {
			t.Errorf("unsafe path %q accepted", path)
		}
	}
	if !validChangePath("dir/with space.txt") {
		t.Fatal("valid literal path rejected")
	}
}

func TestBabysitIndependentEvaluationCommitsAndPushesOnlyAuthorizedPaths(t *testing.T) {
	base := t.TempDir()
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
	snapshotJSON := `{"number":17,"state":"OPEN","title":"Fix docs","url":"https://example.test/pr/17","headRefName":"feature","headRefOid":"` + head + `","headRepository":{"nameWithOwner":"team/repo"},"baseRefName":"main","baseRefOid":"` + head + `","baseRepository":{"nameWithOwner":"team/repo"},"comments":[],"statusCheckRollup":[{"name":"ci","state":"FAILURE"}]}`
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
if [ "$stage" = "babysit" ]; then
  printf 'after\n' > README.md
  printf 'FACTORY_STATUS=FIXED\n'
  exit 0
fi
if [ "$stage" = "evaluate" ]; then
  printf '%s\\n' '[pi-web-access] Dynamic tool activation requires Pi 0.86.1 or newer; web tools remain eagerly available.' >&2
  printf 'PASS\\nFACTORY_FILES=["README.md"]\\n'
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
		evalLog, _ := os.ReadFile(filepath.Join(jobDir, "evaluation.log"))
		t.Fatalf("independently evaluated fix was not pushed; actions=%s agent=%s evaluation=%s", log, agentLog, evalLog)
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
			base := t.TempDir()
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
	base := t.TempDir()
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
