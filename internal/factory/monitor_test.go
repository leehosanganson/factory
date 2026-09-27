package factory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func setupTestMonitorWorktree(t *testing.T, repository, stateRoot string, job *monitorJob, head string) {
	t.Helper()
	store, err := NewJobStore(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	worktree, baseline, err := setupExistingPRWorktree(store, repository, job.HeadBranch, head, job.PR, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	job.Worktree, job.WorkerBranch, job.BaselineHead, job.OwnWorktree = worktree, "factory-monitor/"+job.ID, baseline, true
}

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
	root := filepath.Join(base, "factory", "detached-jobs")
	id := "20260518T120001-0123456789ab"
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	job := &monitorJob{ID: id, RepoRoot: repo, Repo: "team/repo", PR: 7, HeadRepo: "team/repo", HeadBranch: "main", BaseRepo: "team/repo", BaseBranch: "main", OriginURL: bare, BaselineHead: head, TargetBaseline: head, Status: "running", SnapshotFailures: 7}
	if err := saveMonitorJob(dir, job); err != nil {
		t.Fatal(err)
	}
	gh := filepath.Join(base, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
	oldDelay, oldLauncher := monitorRetryDelay, monitorWorkerLauncher
	monitorRetryDelay = func(int) time.Duration { return 0 }
	launched := 0
	monitorWorkerLauncher = func(gotID, gotRoot string) (int, error) {
		launched++
		if gotID != id || gotRoot != root {
			t.Errorf("launcher got %s %s", gotID, gotRoot)
		}
		return os.Getpid(), nil
	}
	t.Cleanup(func() { monitorRetryDelay, monitorWorkerLauncher = oldDelay, oldLauncher })
	if err := runMonitorWorker(id, root, Config{Command: "/bin/true", Args: []string{"{task}", "{system_prompt}"}}); err != nil {
		t.Fatal(err)
	}
	failed, err := readMonitorJob(dir)
	if err != nil || failed.Status != "recoverable_failure" || failed.SnapshotFailures != monitorSnapshotMaxRetries {
		t.Fatalf("job did not enter recoverable failure at retry cap: job=%+v err=%v", failed, err)
	}
	if err := monitorAction("reset", root, id, Config{}, nil, io.Discard); err != nil {
		t.Fatal(err)
	}
	reset, err := readMonitorJob(dir)
	if err != nil || reset.Status != "queued" || reset.SnapshotFailures != 0 || launched != 1 {
		t.Fatalf("reset semantics failed: job=%+v launches=%d err=%v", reset, launched, err)
	}
}

func TestMonitorDeadlineSurvivesRecoverableReset(t *testing.T) {
	for _, tc := range []struct {
		name        string
		expired     bool
		legacy      bool
		wantStatus  string
		wantTimeout bool
	}{
		{name: "expired deadline stops restarted worker", expired: true, wantStatus: "stopped", wantTimeout: true},
		{name: "remaining lifetime permits restarted worker", wantStatus: "closed"},
		{name: "legacy record deadline uses original start", expired: true, legacy: true, wantStatus: "stopped", wantTimeout: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newMonitorPushFixture(t)
			fixture.job.Status = "queued"
			if err := saveMonitorJob(fixture.dir, fixture.job); err != nil {
				t.Fatal(err)
			}
			gh := filepath.Join(fixture.base, "gh")
			if err := os.WriteFile(gh, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			oldDelay := monitorRetryDelay
			monitorRetryDelay = func(int) time.Duration { return 0 }
			t.Cleanup(func() { monitorRetryDelay = oldDelay })
			cfg := Config{Command: "/bin/true", Args: []string{"{task}", "{system_prompt}"}, MonitorTimeout: "1h"}
			if err := runMonitorWorker(fixture.job.ID, filepath.Dir(fixture.dir), cfg); err != nil {
				t.Fatal(err)
			}
			failed, err := readMonitorJob(fixture.dir)
			if err != nil || failed.Status != "recoverable_failure" || failed.DeadlineAt.IsZero() {
				t.Fatalf("first worker did not persist its deadline and recoverable state: job=%+v err=%v", failed, err)
			}
			deadline := time.Now().UTC().Add(time.Minute)
			if tc.expired {
				deadline = time.Now().UTC().Add(-time.Second)
			}
			store, err := NewJobStore(filepath.Dir(fixture.dir))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.UpdateJob(fixture.job.ID, func(record *JobRecord) error {
				if tc.legacy {
					record.Monitor.DeadlineAt = time.Time{}
					record.StartedAt = time.Now().UTC().Add(-2 * time.Hour)
					deadline = record.StartedAt.Add(time.Hour)
				} else {
					record.Monitor.DeadlineAt = deadline
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if !tc.expired {
				response := `{"number":17,"state":"CLOSED","title":"Fix","url":"https://github.com/team/repo/pull/17","headRefName":"feature","headRefOid":"` + fixture.head + `","headRepository":{"nameWithOwner":"team/repo"},"baseRefName":"main","baseRefOid":"` + fixture.head + `","comments":[],"statusCheckRollup":[]}`
				script := `#!/bin/sh
if [ "$1" = api ]; then echo '{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"isResolved":true,"comments":{"nodes":[],"pageInfo":{"hasNextPage":false}}}],"pageInfo":{"hasNextPage":false}}}}}}'; exit 0; fi
printf '%s\n' '` + response + `'
`
				if err := os.WriteFile(gh, []byte(script), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			workerDone := make(chan error, 1)
			startWorker := make(chan struct{})
			oldLauncher, oldAcquire := monitorWorkerLauncher, monitorAcquireWorkerLock
			startupContention := false
			monitorWorkerLauncher = func(id, root string) (int, error) {
				go func() {
					<-startWorker
					workerDone <- runMonitorWorker(id, root, cfg)
				}()
				return os.Getpid(), nil
			}
			if tc.expired {
				firstAcquire := true
				monitorAcquireWorkerLock = func(path string) (func(), error) {
					if firstAcquire {
						firstAcquire = false
						startupContention = true
						return nil, errors.New("test worker startup contention")
					}
					return acquireOwnedLock(path)
				}
			}
			t.Cleanup(func() { monitorWorkerLauncher, monitorAcquireWorkerLock = oldLauncher, oldAcquire })
			if err := monitorAction("reset", filepath.Dir(fixture.dir), fixture.job.ID, cfg, nil, io.Discard); err != nil {
				t.Fatal(err)
			}
			close(startWorker)
			select {
			case err := <-workerDone:
				if err != nil {
					t.Fatalf("restarted worker: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("restarted worker did not finish")
			}
			if tc.expired && !startupContention {
				t.Fatal("restarted worker did not encounter simulated startup lock contention")
			}
			got, err := readMonitorJob(fixture.dir)
			if err != nil || got.Status != tc.wantStatus || !got.DeadlineAt.Equal(deadline) {
				t.Fatalf("reset extended deadline or produced wrong status: status=%q deadline=%s err=%v; want %q and %s", got.Status, got.DeadlineAt, err, tc.wantStatus, deadline)
			}
			logData, err := os.ReadFile(filepath.Join(fixture.dir, "sessions", monitorSessionID, "session.log"))
			if err != nil || strings.Contains(string(logData), "Monitor lifetime timeout reached") != tc.wantTimeout {
				t.Fatalf("timeout event presence=%v err=%v; want %v; log=%s", strings.Contains(string(logData), "Monitor lifetime timeout reached"), err, tc.wantTimeout, logData)
			}
			if tc.wantTimeout {
				store, err := NewJobStore(filepath.Dir(fixture.dir))
				if err != nil {
					t.Fatal(err)
				}
				record, err := store.GetJob(fixture.job.ID)
				if err != nil || record.Status != "stopped" || record.Monitor.Status != "stopped" {
					t.Fatalf("durable job status=%q monitor status=%q err=%v; want stopped", record.Status, record.Monitor.Status, err)
				}
				session, err := store.GetSession(fixture.job.ID, monitorSessionID)
				if err != nil || session.Status != "stopped" {
					t.Fatalf("durable session status=%q err=%v; want stopped", session.Status, err)
				}
				events, err := store.SessionEvents(fixture.job.ID, monitorSessionID)
				if err != nil || len(events) == 0 || !strings.Contains(events[len(events)-1].Message, "Monitor lifetime timeout reached") {
					t.Fatalf("durable timeout event missing: events=%+v err=%v", events, err)
				}
			}
		})
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
			previousActionLimit := monitorAgentActionTimeout
			if tc.actionLimit > 0 {
				monitorAgentActionTimeout = tc.actionLimit
			}
			t.Cleanup(func() { monitorAgentActionTimeout = previousActionLimit })
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
			job := &monitorJob{ID: "20260518T120005-0123456789ab", Description: "monitor", RepoRoot: repo, Repo: "team/repo", PR: 7, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", OriginURL: bare, BaselineHead: head, TargetBaseline: head, Status: "running", Attempts: 1, CreatedAt: time.Now().UTC()}
			if err := saveMonitorJob(dir, job); err != nil {
				t.Fatal(err)
			}
			setupTestMonitorWorktree(t, repo, dir, job, head)
			ctx, cancel := tc.ctx()
			defer cancel()
			t.Setenv("HANG_AT", tc.hangAt)
			started := time.Now()
			err = processMonitorEventContext(ctx, dir, job, Config{Command: script, Args: []string{"{stage}", "{task}", "{system_prompt}"}, AgentTimeout: tc.timeout}, &monitorSnapshot{HeadRefOID: head}, strings.Repeat("a", 64))
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
	root := filepath.Join(base, "state", "factory", "detached-jobs")
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
	if err := os.WriteFile(gh, []byte("#!/bin/sh\nif [ \"$1\" = api ]; then echo '{\"data\":{\"repository\":{\"pullRequest\":{\"reviewThreads\":{\"nodes\":[{\"id\":\"resolved\",\"isResolved\":true,\"comments\":{\"nodes\":[],\"pageInfo\":{\"hasNextPage\":false}}}],\"pageInfo\":{\"hasNextPage\":false}}}}}}'; exit 0; fi\ncase \"$*\" in *baseRepository*) echo 'unsupported JSON field: baseRepository' >&2; exit 2;; esac\n[ ! -f \"$GH_FAIL\" ] || exit 1\ncat \"$GH_RESPONSE\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	failMarker := filepath.Join(base, "gh-fail")
	t.Setenv("GH_FAIL", failMarker)
	t.Setenv("GH_RESPONSE", responsePath)
	t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FACTORY_MONITOR_POLL_INTERVAL", "1s")
	agentStarted := filepath.Join(base, "agent-started")
	t.Setenv("AGENT_STARTED", agentStarted)
	agent := filepath.Join(base, "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\nif [ \"$1\" = monitor ]; then touch \"$AGENT_STARTED\"; exec sleep 30; fi\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	job := &monitorJob{ID: id, Description: "monitor", RepoRoot: repo, Repo: "team/repo", PR: 7, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", BaseSHA: head, OriginURL: bare, BaselineHead: head, TargetBaseline: head, Status: "running", CreatedAt: time.Now().UTC()}
	if err := saveMonitorJob(dir, job); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Command: agent, Args: []string{"{stage}", "{task}", "{system_prompt}"}, StateDir: filepath.Join(base, "state")}
	workerDone := make(chan error, 1)
	go func() { workerDone <- runMonitorWorker(id, root, cfg) }()
	waitForJobStatus(t, dir, "running")
	waitForFile(t, agentStarted)
	if err := monitorAction("stop", root, id, cfg, nil, io.Discard); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		actions, _ := os.ReadFile(filepath.Join(filepath.Dir(dir), "sessions", monitorSessionID, "session.log"))
		agentLog, _ := os.ReadFile(filepath.Join(dir, "agent.log"))
		t.Fatalf("worker did not stop promptly; actions=%s agent=%s", actions, agentLog)
	}
	stopped, _ := readMonitorJob(dir)
	if stopped.Status != "stopped" {
		t.Fatalf("worker status after stop = %q", stopped.Status)
	}
	if stopped.Worktree != "" {
		_, _ = runGit(context.Background(), repo, "worktree", "remove", "--force", stopped.Worktree)
		stopped.Worktree, stopped.WorkerBranch = "", ""
	}
	stopped.Status, stopped.StopRequested, stopped.SnapshotFailures = "running", false, 7
	if err := saveMonitorJob(dir, stopped); err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ClearStopRequest(id); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(failMarker, []byte("fail"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldDelay, oldLauncher := monitorRetryDelay, monitorWorkerLauncher
	monitorRetryDelay = func(int) time.Duration { return 0 }
	var nextWorker chan error
	monitorWorkerLauncher = func(gotID, gotRoot string) (int, error) {
		nextWorker = make(chan error, 1)
		go func() { nextWorker <- runMonitorWorker(gotID, gotRoot, cfg) }()
		return os.Getpid(), nil
	}
	t.Cleanup(func() { monitorRetryDelay, monitorWorkerLauncher = oldDelay, oldLauncher })
	if err := runMonitorWorker(id, root, cfg); err != nil {
		t.Fatal(err)
	}
	recoverable, err := readMonitorJob(dir)
	if err != nil || recoverable.Status != "recoverable_failure" || recoverable.SnapshotFailures != 8 {
		t.Fatalf("snapshot retry cap status=%+v err=%v", recoverable, err)
	}
	if err := os.Remove(failMarker); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(responsePath, []byte(closedResponse), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := monitorAction("reset", root, id, cfg, nil, io.Discard); err != nil {
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
	closed, err := readMonitorJob(dir)
	if err != nil || closed.Status != "closed" {
		t.Fatalf("closed PR status=%+v err=%v", closed, err)
	}
	record, err := store.GetJob(id)
	if err != nil || record.Status != closed.Status {
		t.Fatalf("closed PR job record status=%q monitor status=%q err=%v", record.Status, closed.Status, err)
	}
	session, err := store.GetSession(id, monitorSessionID)
	if err != nil || session.Status != closed.Status {
		t.Fatalf("closed PR session status=%q monitor status=%q err=%v", session.Status, closed.Status, err)
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
		job, err := readMonitorJob(dir)
		if err == nil && job.Status == status && job.Snapshot != "" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	job, _ := readMonitorJob(dir)
	t.Fatalf("job did not reach %s with a polled snapshot: %+v", status, job)
}

func TestMonitorIDsAndAtomicMetadata(t *testing.T) {
	root := t.TempDir()
	id, err := monitorID()
	if err != nil {
		t.Fatal(err)
	}
	if !monitorIDPattern.MatchString(id) {
		t.Fatalf("generated invalid id %q", id)
	}
	if _, err := monitorJobDir(root, "../escape"); err == nil {
		t.Fatal("path traversal id accepted")
	}
	dir := filepath.Join(root, id)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	job := &monitorJob{ID: id, Description: "track PR", Status: "running", CreatedAt: time.Now().UTC()}
	if err := saveMonitorJob(dir, job); err != nil {
		t.Fatal(err)
	}
	loaded, err := readMonitorJob(dir)
	if err != nil || loaded.Description != job.Description {
		t.Fatalf("metadata round trip: %#v %v", loaded, err)
	}
	store, err := NewJobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.GetJob(id)
	if err != nil || record.Monitor == nil || record.Monitor.Description != job.Description {
		t.Fatalf("canonical generic job record missing monitor metadata: %#v %v", record, err)
	}
	if _, err := store.GetSession(id, monitorSessionID); err != nil {
		t.Fatalf("canonical monitor session missing: %v", err)
	}
	sessionDir := filepath.Join(root, id, "sessions", monitorSessionID)
	if info, err := os.Stat(sessionDir); err != nil || !info.IsDir() {
		t.Fatalf("canonical monitor session directory missing: info=%v err=%v", info, err)
	}
}

func TestMonitorApprovalBindsScopeToExactSnapshotAndInvalidatesOnChange(t *testing.T) {
	base := canonicalTestPath(t, t.TempDir())
	gh := filepath.Join(base, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\ncase \"$*\" in *graphql*) echo '{\"data\":{\"repository\":{\"pullRequest\":{\"reviewThreads\":{\"nodes\":[{\"isResolved\":true,\"comments\":{\"nodes\":[],\"pageInfo\":{\"hasNextPage\":false}}}],\"pageInfo\":{\"hasNextPage\":false}}}}}}'; exit 0;; esac\ncase \"$*\" in *baseRepository*) echo 'unsupported JSON field: baseRepository' >&2; exit 2;; esac\ncat \"$GH_RESPONSE\"\n"), 0o700); err != nil {
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
	root, err := monitorRoot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	id := "20260518T120003-0123456789ab"
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	job := &monitorJob{ID: id, Repo: "team/repo", PR: 7, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", BaseSHA: "base123", BaselineHead: "head123", Status: "running", PendingSignature: "placeholder", Proposal: "Scope requested", CreatedAt: time.Now().UTC()}
	_, signature, err := readSnapshot(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	job.PendingSignature = signature
	if err := saveMonitorJob(dir, job); err != nil {
		t.Fatal(err)
	}
	if err := monitorAction("approve", root, id, cfg, strings.NewReader("y\nFix only README typo\n"), &strings.Builder{}); err != nil {
		t.Fatal(err)
	}
	approved, err := readMonitorJob(dir)
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
	approved.Status = "running"
	approved.PendingSignature = signature
	approved.Proposal = "Proposal for prior snapshot"
	if err := saveMonitorJob(dir, approved); err != nil {
		t.Fatal(err)
	}
	if err := monitorAction("approve", root, id, cfg, strings.NewReader("y\nBroader scope\n"), &strings.Builder{}); err == nil {
		t.Fatal("changed proposal snapshot was approved")
	}
	invalidated, err := readMonitorJob(dir)
	if err != nil {
		t.Fatal(err)
	}
	if invalidated.ApprovalSignature != "" || invalidated.ApprovalScope != "" || invalidated.Proposal != "" {
		t.Fatalf("stale approval persisted: %#v", invalidated)
	}
}

func TestMonitorSnapshotValidationAndSignature(t *testing.T) {
	root := t.TempDir()
	gh := filepath.Join(root, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\ncase \"$*\" in *graphql*) echo '{\"data\":{\"repository\":{\"pullRequest\":{\"reviewThreads\":{\"nodes\":[{\"isResolved\":true,\"comments\":{\"nodes\":[],\"pageInfo\":{\"hasNextPage\":false}}}],\"pageInfo\":{\"hasNextPage\":false}}}}}}'; exit 0;; esac\ncase \"$*\" in *baseRepository*) echo 'unsupported JSON field: baseRepository' >&2; exit 2;; esac\ncat \"$GH_RESPONSE\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	response := `{"number":7,"state":"OPEN","title":"Fix","url":"https://github.com/team/repo/pull/7","headRefName":"feature","headRefOid":"abc123","headRepository":{"nameWithOwner":"team/fork"},"baseRefName":"main","baseRefOid":"base123","comments":[],"statusCheckRollup":[]}`
	file := filepath.Join(root, "response.json")
	if err := os.WriteFile(file, []byte(response), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GH_RESPONSE", file)
	job := &monitorJob{PR: 7, Repo: "team/repo", HeadRepo: "team/fork", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", BaseSHA: "base123"}
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

func TestMonitorProtocolRequiresOneExactFinalStatus(t *testing.T) {
	for _, tc := range []struct{ out, want string }{{"tests ok\nFACTORY_STATUS=FIXED\n", "FIXED"}, {"FACTORY_STATUS=FIXED\n", "FIXED"}, {"FACTORY_STATUS=ERROR\n", "ERROR"}, {"FACTORY_STATUS=FIXED\nFACTORY_STATUS=APPROVAL_REQUIRED\n", ""}, {"FACTORY_STATUS=FIXED extra\n", ""}} {
		got, _ := agentProtocol(tc.out)
		if got != tc.want {
			t.Errorf("protocol(%q)=%q want %q", tc.out, got, tc.want)
		}
	}
}

func TestMonitorChangedPathsIncludeStagedUntrackedAndNoRenameEndpoints(t *testing.T) {
	repo := newMonitorRepo(t)
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

func TestMonitorApprovalRequiredStillPausesBeforeAnyPublish(t *testing.T) {
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
	jobDir := filepath.Join(base, "state", "factory", "detached-jobs", "20260518T120000-0123456789ab")
	if err := os.MkdirAll(jobDir, 0o700); err != nil {
		t.Fatal(err)
	}
	job := &monitorJob{ID: "20260518T120000-0123456789ab", Description: "Fix docs", RepoRoot: repo, Repo: "team/repo", PR: 17, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", OriginURL: bare, HeadRepoURL: bare, BaselineHead: head, TargetBaseline: head, Status: "running", CreatedAt: time.Now().UTC()}
	if err := saveMonitorJob(jobDir, job); err != nil {
		t.Fatal(err)
	}
	setupTestMonitorWorktree(t, repo, filepath.Dir(jobDir), job, head)
	script := filepath.Join(base, "agent")
	if err := os.WriteFile(script, []byte(`#!/bin/sh
printf changed > README.md
printf 'FACTORY_PROPOSAL=Need human review\n'
printf 'FACTORY_STATUS=APPROVAL_REQUIRED\n'
`), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := processMonitorEvent(jobDir, job, Config{Command: script, Args: []string{"{stage}", "{task}", "{system_prompt}"}}, &monitorSnapshot{HeadRefOID: head}, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if job.Status != "running" || job.Proposal != "Need human review" {
		t.Fatalf("approval state = %q, proposal %q; want awaiting approval", job.Status, job.Proposal)
	}
	if got := runTestCommand(t, bare, "git", "rev-parse", "refs/heads/feature"); got != head {
		t.Fatalf("approval-required action changed remote: got %s, want %s", got, head)
	}
}

func TestMonitorFixedCommitsAndPushesOnlyGitDerivedChangedPaths(t *testing.T) {
	base := canonicalTestPath(t, t.TempDir())
	bare := filepath.Join(base, "remote.git")
	repo := filepath.Join(base, "repo")
	runTestCommand(t, base, "git", "init", "--bare", bare)
	runTestCommand(t, base, "git", "clone", bare, repo)
	runTestCommand(t, repo, "git", "checkout", "-b", "main")
	runTestCommand(t, repo, "git", "config", "user.name", "Factory Test")
	runTestCommand(t, repo, "git", "config", "user.email", "factory@example.test")
	if err := os.WriteFile(filepath.Join(repo, "README.md"), []byte("before\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, repo, "git", "add", "README.md")
	runTestCommand(t, repo, "git", "commit", "-m", "initial")
	runTestCommand(t, repo, "git", "push", "-u", "origin", "main")
	runTestCommand(t, repo, "git", "checkout", "-b", "feature")
	runTestCommand(t, repo, "git", "push", "-u", "origin", "feature")
	runTestCommand(t, repo, "git", "checkout", "main")
	head, err := runGit(context.Background(), repo, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "state", "factory", "detached-jobs")
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
if [ "$1" = "api" ]; then echo '{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"isResolved":true,"comments":{"nodes":[],"pageInfo":{"hasNextPage":false}}}],"pageInfo":{"hasNextPage":false}}}}}}'; exit 0; fi
exit 1
`
	if err := os.WriteFile(gh, []byte(ghScript), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
	snapshotJSON := `{"number":17,"state":"OPEN","title":"Fix docs","url":"https://github.com/team/repo/pull/17","headRefName":"feature","headRefOid":"` + head + `","headRepository":{"nameWithOwner":"team/repo"},"baseRefName":"main","baseRefOid":"` + head + `","comments":[],"statusCheckRollup":[{"name":"ci","state":"FAILURE"}]}`
	t.Setenv("GH_SNAPSHOT", snapshotJSON)
	prWorktree := filepath.Join(base, "pr-worktree")
	runTestCommand(t, repo, "git", "worktree", "add", prWorktree, "feature")
	job := &monitorJob{ID: "20260518T120000-0123456789ab", Description: "Fix the documented typo", RepoRoot: repo, Repo: "team/repo", PR: 17, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", TargetBranch: "main", OriginURL: bare, HeadRepoURL: bare, BaselineHead: head, TargetBaseline: head, Status: "running", CreatedAt: time.Now().UTC()}
	snap, sig, err := readSnapshot(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	job.Snapshot = sig
	setupTestMonitorWorktree(t, repo, root, job, head)
	if err := saveMonitorJob(jobDir, job); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(job.Worktree, "README.md"), []byte("after\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	changed, err := changedPaths(job.Worktree)
	if err != nil {
		t.Fatal(err)
	}
	if err := guardedCommitPush(context.Background(), jobDir, job, sig, snap, changed); err != nil {
		t.Fatal(err)
	}
	remoteHead, err := runGit(context.Background(), bare, "rev-parse", "refs/heads/feature")
	if err != nil {
		t.Fatal(err)
	}
	if remoteHead == head {
		t.Fatal("FIXED changes were not pushed")
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

func TestMonitorBadOriginAndBranchBlockAutomaticCommit(t *testing.T) {
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
			dir := filepath.Join(base, "jobs", "20260518T120002-0123456789ab")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			job := &monitorJob{ID: filepath.Base(dir), RepoRoot: repo, Repo: "team/repo", PR: 17, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", OriginURL: bare, HeadRepoURL: bare, BaselineHead: head, TargetBaseline: head, Snapshot: "sig"}
			setupTestMonitorWorktree(t, repo, filepath.Dir(dir), job, head)
			worktree := job.Worktree
			if err := saveMonitorJob(dir, job); err != nil {
				t.Fatal(err)
			}
			tc.alter(t, repo, worktree)
			if err := guardedCommitPush(context.Background(), dir, job, "sig", &monitorSnapshot{HeadRefOID: head}, []string{"f"}); err == nil {
				t.Fatal("unsafe commit/push accepted")
			}
			got, _ := runGit(context.Background(), bare, "rev-parse", "refs/heads/feature")
			if got != head {
				t.Fatalf("remote changed despite invalid validation: %s", got)
			}
		})
	}
}

func TestMonitorStopBeforeCommitDoesNotCreateCommit(t *testing.T) {
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
	root := filepath.Join(base, "factory", "detached-jobs")
	dir := filepath.Join(root, "20260518T120001-0123456789ab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	job := &monitorJob{ID: filepath.Base(dir), RepoRoot: repo, Repo: "team/repo", PR: 17, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", OriginURL: bare, HeadRepoURL: bare, BaselineHead: head, TargetBaseline: head, Snapshot: "sig", StopRequested: true}
	setupTestMonitorWorktree(t, repo, filepath.Dir(dir), job, head)
	worktree := job.Worktree
	if err := os.WriteFile(filepath.Join(worktree, "f"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	job.StopRequested = true
	if err := saveMonitorJob(dir, job); err != nil {
		t.Fatal(err)
	}
	if err := guardedCommitPush(context.Background(), dir, job, "sig", &monitorSnapshot{HeadRefOID: head}, []string{"f"}); err == nil {
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

func TestMonitorRejectsDifferentConfiguredPushURL(t *testing.T) {
	fixture := newMonitorPushFixture(t)
	wrongRemote := filepath.Join(fixture.base, "wrong.git")
	runTestCommand(t, fixture.base, "git", "init", "--bare", wrongRemote)
	runTestCommand(t, fixture.repo, "git", "config", "remote.origin.pushurl", wrongRemote)

	err := guardedCommitPush(context.Background(), fixture.dir, fixture.job, fixture.signature, fixture.snapshot, []string{"f"})
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

func TestMonitorRejectsPushURLRewrites(t *testing.T) {
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

func TestMonitorPushLeaseRejectsRemoteHeadRace(t *testing.T) {
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
	if err := guardedCommitPush(context.Background(), fixture.dir, fixture.job, fixture.signature, fixture.snapshot, []string{"f"}); err == nil {
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
	job                   *monitorJob
	snapshot              *monitorSnapshot
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
	ghScript := "#!/bin/sh\nif [ \"$1\" = pr ] && [ \"$2\" = view ]; then printf '%s\\n' '" + ghJSON + "'; exit 0; fi\nif [ \"$1\" = api ]; then echo '{\"data\":{\"repository\":{\"pullRequest\":{\"reviewThreads\":{\"nodes\":[{\"isResolved\":true,\"comments\":{\"nodes\":[],\"pageInfo\":{\"hasNextPage\":false}}}],\"pageInfo\":{\"hasNextPage\":false}}}}}}'; exit 0; fi\nexit 1\n"
	if err := os.WriteFile(gh, []byte(ghScript), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
	dir := filepath.Join(base, "state", "factory", "detached-jobs", "20260518T120003-0123456789ab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	snapshot, signature, err := readSnapshot(context.Background(), &monitorJob{Repo: "team/repo", PR: 17, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	job := &monitorJob{ID: filepath.Base(dir), RepoRoot: repo, Repo: "team/repo", PR: 17, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", OriginURL: bare, HeadRepoURL: bare, BaselineHead: head, TargetBaseline: head, Snapshot: signature, OwnWorktree: false}
	setupTestMonitorWorktree(t, repo, filepath.Dir(dir), job, head)
	if err := os.WriteFile(filepath.Join(job.Worktree, "f"), []byte("agent update\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := saveMonitorJob(dir, job); err != nil {
		t.Fatal(err)
	}
	return monitorPushFixture{base: base, bare: bare, repo: repo, dir: dir, job: job, snapshot: snapshot, signature: signature, head: head}
}

func TestGuardedCommitPushCancellationAbortsSnapshotCheckpoint(t *testing.T) {
	for _, checkpoint := range []int{1, 2} {
		t.Run(fmt.Sprintf("snapshot %d", checkpoint), func(t *testing.T) {
			fixture := newMonitorPushFixture(t)
			ghPath := filepath.Join(fixture.base, "gh")
			countPath := filepath.Join(fixture.base, "gh-count")
			startedPath := filepath.Join(fixture.base, "gh-started")
			jsonData := `{"number":17,"state":"OPEN","title":"Fix","url":"https://github.com/team/repo/pull/17","headRefName":"feature","headRefOid":"` + fixture.head + `","headRepository":{"nameWithOwner":"team/repo"},"baseRefName":"main","baseRefOid":"` + fixture.head + `","comments":[],"statusCheckRollup":[{"name":"ci","state":"FAILURE"}]}`
			script := "#!/bin/sh\nn=0\n[ ! -f '" + countPath + "' ] || n=$(cat '" + countPath + "')\nn=$((n+1))\nprintf '%s' \"$n\" > '" + countPath + "'\ntouch '" + startedPath + "'\nif [ $n -eq " + strconv.Itoa(checkpoint) + " ]; then exec sleep 30; else printf '%s\\n' '" + jsonData + "'; fi\n"
			if err := os.WriteFile(ghPath, []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancelStarted := make(chan struct{})
			go func() {
				defer close(cancelStarted)
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) {
					if _, err := os.Stat(startedPath); err == nil {
						count, _ := os.ReadFile(countPath)
						if string(count) == strconv.Itoa(checkpoint) {
							cancel()
							return
						}
					}
					time.Sleep(5 * time.Millisecond)
				}
				cancel()
			}()
			err := guardedCommitPush(ctx, fixture.dir, fixture.job, fixture.signature, fixture.snapshot, []string{"f"})
			<-cancelStarted
			if err == nil || !errors.Is(err, context.Canceled) {
				t.Fatalf("guarded commit/push error = %v, want cancellation", err)
			}
			remote, err := runGit(context.Background(), fixture.bare, "rev-parse", "refs/heads/feature")
			if err != nil || remote != fixture.head {
				t.Fatalf("canceled operation changed remote: head=%s err=%v", remote, err)
			}
			count, _ := os.ReadFile(countPath)
			if string(count) != strconv.Itoa(checkpoint) {
				t.Fatalf("snapshot calls=%s, want cancellation at call %d", count, checkpoint)
			}
		})
	}
}

func TestReadReviewThreadsIncludesUnresolvedCommentDetailsAndFailsOnIncompletePages(t *testing.T) {
	base := t.TempDir()
	gh := filepath.Join(base, "gh")
	responseFile := filepath.Join(base, "response.json")
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$GH_ARGS"
case "$*" in
  *'node(id:'*) printf '%s\n' '{"data":{"node":{"comments":{"nodes":[{"body":"second comment","path":"internal/example.go","line":24,"originalLine":22}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}'; exit 0 ;;
  *'after=threads-1'*) printf '%s\n' '{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"id":"resolved-thread","isResolved":true,"comments":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}'; exit 0 ;;
esac
cat "$GH_RESPONSE"
`
	if err := os.WriteFile(gh, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GH_ARGS", filepath.Join(base, "args"))
	t.Setenv("GH_RESPONSE", responseFile)
	job := &monitorJob{BaseRepo: "team/repo", PR: 17}
	response := `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"id":"unresolved-thread","isResolved":false,"comments":{"nodes":[{"body":"Please handle this edge case","path":"internal/example.go","line":23,"originalLine":21}],"pageInfo":{"hasNextPage":true,"endCursor":"comments-1"}}}],"pageInfo":{"hasNextPage":true,"endCursor":"threads-1"}}}}}}`
	if err := os.WriteFile(responseFile, []byte(response), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readReviewThreads(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	var threads []struct {
		IsResolved bool `json:"isResolved"`
		Comments   struct {
			Nodes []struct {
				Body         string `json:"body"`
				Path         string `json:"path"`
				Line         int    `json:"line"`
				OriginalLine int    `json:"originalLine"`
			} `json:"nodes"`
		} `json:"comments"`
	}
	if err := json.Unmarshal(got, &threads); err != nil {
		t.Fatal(err)
	}
	if len(threads) != 1 || threads[0].IsResolved || len(threads[0].Comments.Nodes) != 2 {
		t.Fatalf("unresolved review threads = %s", got)
	}
	comment := threads[0].Comments.Nodes[0]
	if comment.Body != "Please handle this edge case" || comment.Path != "internal/example.go" || comment.Line != 23 || comment.OriginalLine != 21 {
		t.Fatalf("review comment details missing: %+v", comment)
	}
	if threads[0].Comments.Nodes[1].Body != "second comment" {
		t.Fatalf("comment continuation page missing: %+v", threads[0].Comments.Nodes)
	}
	args, err := os.ReadFile(filepath.Join(base, "args"))
	if err != nil || !strings.Contains(string(args), "comments(first:100") || !strings.Contains(string(args), "reviewThreads(first:100") || !strings.Contains(string(args), "after=threads-1") || !strings.Contains(string(args), "after=comments-1") {
		t.Fatalf("GraphQL pagination cursors were not requested: %s err=%v", args, err)
	}
	for _, tc := range []struct {
		name, response, want string
	}{
		{name: "thread page omits cursor", response: `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[],"pageInfo":{"hasNextPage":true}}}}}}`, want: "valid cursor"},
		{name: "comment page omits cursor", response: `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"id":"thread-id","isResolved":false,"comments":{"nodes":[],"pageInfo":{"hasNextPage":true}}}],"pageInfo":{"hasNextPage":false}}}}}}`, want: "valid cursor"},
		{name: "GitHub reports errors", response: `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[],"pageInfo":{"hasNextPage":false}}}}},"errors":[{}]}`, want: "incomplete or contains errors"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(responseFile, []byte(tc.response), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := readReviewThreads(context.Background(), job); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("readReviewThreads error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestMonitorLifetimeTimeoutStopsWithDistinctEvent(t *testing.T) {
	fixture := newMonitorPushFixture(t)
	fixture.job.Status = "queued"
	if err := saveMonitorJob(fixture.dir, fixture.job); err != nil {
		t.Fatal(err)
	}
	agent := filepath.Join(fixture.base, "agent")
	started := filepath.Join(fixture.base, "agent-started")
	t.Setenv("AGENT_STARTED", started)
	if err := os.WriteFile(agent, []byte(`#!/bin/sh
touch "$AGENT_STARTED"
exec sleep 30
`), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FACTORY_MONITOR_POLL_INTERVAL", "1s")
	cfg := Config{Command: agent, Args: []string{"{stage}", "{task}", "{system_prompt}"}, StateDir: filepath.Join(fixture.base, "state"), MonitorTimeout: "2s"}
	workerDone := make(chan error, 1)
	go func() { workerDone <- runMonitorWorker(fixture.job.ID, filepath.Dir(fixture.dir), cfg) }()
	workerFinished := false
	t.Cleanup(func() {
		if workerFinished {
			return
		}
		if err := monitorAction("stop", filepath.Dir(fixture.dir), fixture.job.ID, cfg, nil, io.Discard); err != nil {
			t.Errorf("requesting worker cleanup: %v", err)
		}
		select {
		case err := <-workerDone:
			workerFinished = true
			if err != nil {
				t.Errorf("worker cleanup: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("monitor worker did not stop during cleanup")
		}
	})
	waitForFile(t, started)
	select {
	case err := <-workerDone:
		workerFinished = true
		if err != nil {
			t.Fatalf("monitor worker: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not cancel the active sleeping agent after monitor timeout")
	}
	job, err := readMonitorJob(fixture.dir)
	if err != nil || job.Status != "stopped" {
		t.Fatalf("monitor lifetime status=%+v err=%v; want stopped", job, err)
	}
	if job.StopRequested {
		t.Fatal("monitor lifetime expiry was recorded as an explicit stop request")
	}
	logData, err := os.ReadFile(filepath.Join(fixture.dir, "sessions", monitorSessionID, "session.log"))
	if err != nil || !strings.Contains(string(logData), "Monitor lifetime timeout reached") || strings.Contains(string(logData), "Cooperative stop requested") {
		t.Fatalf("timeout event does not distinguish lifecycle expiry from user stop: log=%s err=%v", logData, err)
	}
}

func TestMonitorLifetimeTimeoutIncludesBranchLockWait(t *testing.T) {
	fixture := newMonitorPushFixture(t)
	fixture.job.Status = "queued"
	if err := saveMonitorJob(fixture.dir, fixture.job); err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(filepath.Dir(fixture.dir))
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := store.LockBranch(fixture.repo, fixture.job.HeadBranch)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()

	cfg := Config{
		Command:        "/bin/true",
		Args:           []string{"{stage}", "{task}", "{system_prompt}"},
		StateDir:       filepath.Join(fixture.base, "state"),
		MonitorTimeout: "150ms",
	}
	workerDone := make(chan error, 1)
	go func() { workerDone <- runMonitorWorker(fixture.job.ID, filepath.Dir(fixture.dir), cfg) }()
	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatalf("monitor worker: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("monitor timeout did not bound branch-lock acquisition")
	}

	job, err := readMonitorJob(fixture.dir)
	if err != nil || job.Status != "stopped" || job.StopRequested {
		t.Fatalf("branch-lock timeout state=%+v err=%v; want clean timeout stop", job, err)
	}
	logData, err := os.ReadFile(filepath.Join(fixture.dir, "sessions", monitorSessionID, "session.log"))
	if err != nil || !strings.Contains(string(logData), "Monitor lifetime timeout reached") {
		t.Fatalf("branch-lock timeout event missing: log=%s err=%v", logData, err)
	}
}

func TestMonitorLifetimeTimeoutMarksQueuedJobAfterWorkerLockWait(t *testing.T) {
	fixture := newMonitorPushFixture(t)
	fixture.job.Status, fixture.job.PID = "queued", 0
	if err := saveMonitorJob(fixture.dir, fixture.job); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Command: "/bin/true", Args: []string{"{stage}", "{task}", "{system_prompt}"}, MonitorTimeout: "150ms"}
	lockDeadline := time.Now().Add(150 * time.Millisecond)
	previousAcquire := monitorAcquireWorkerLock
	monitorAcquireWorkerLock = func(path string) (func(), error) {
		if time.Now().Before(lockDeadline) {
			return nil, errors.New("test worker lock contention")
		}
		return acquireOwnedLock(path)
	}
	t.Cleanup(func() { monitorAcquireWorkerLock = previousAcquire })
	workerDone := make(chan error, 1)
	go func() { workerDone <- runMonitorWorker(fixture.job.ID, filepath.Dir(fixture.dir), cfg) }()
	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatalf("monitor worker: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("monitor timeout did not bound worker-lock acquisition")
	}

	job, err := readMonitorJob(fixture.dir)
	if err != nil || job.Status != "stopped" || job.StopRequested {
		t.Fatalf("queued worker-lock timeout state=%+v err=%v; want lifecycle stopped", job, err)
	}
	logData, err := os.ReadFile(filepath.Join(fixture.dir, "sessions", monitorSessionID, "session.log"))
	if err != nil || !strings.Contains(string(logData), "Monitor lifetime timeout reached") {
		t.Fatalf("queued worker-lock timeout event missing: log=%s err=%v", logData, err)
	}
	store, err := NewJobStore(filepath.Dir(fixture.dir))
	if err != nil {
		t.Fatal(err)
	}
	events, err := store.SessionEvents(fixture.job.ID, monitorSessionID)
	if err != nil || len(events) == 0 || events[len(events)-1].Type != "monitor.status" || !strings.Contains(events[len(events)-1].Message, "Monitor lifetime timeout reached") {
		t.Fatalf("queued worker-lock timeout lifecycle event missing: events=%+v err=%v", events, err)
	}
}

func TestMonitorLifetimeTimeoutIncludesWorkerLockWaitWithoutChangingActiveStatus(t *testing.T) {
	fixture := newMonitorPushFixture(t)
	fixture.job.Status = "running"
	if err := saveMonitorJob(fixture.dir, fixture.job); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(fixture.dir, "worker.lock")
	unlock, err := acquireOwnedLock(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	lockReleased := false
	t.Cleanup(func() {
		if !lockReleased {
			unlock()
		}
	})

	cfg := Config{
		Command:        "/bin/true",
		Args:           []string{"{stage}", "{task}", "{system_prompt}"},
		StateDir:       filepath.Join(fixture.base, "state"),
		MonitorTimeout: "150ms",
	}
	workerDone := make(chan error, 1)
	go func() { workerDone <- runMonitorWorker(fixture.job.ID, filepath.Dir(fixture.dir), cfg) }()
	workerFinished := false
	t.Cleanup(func() {
		if workerFinished {
			return
		}
		select {
		case err := <-workerDone:
			workerFinished = true
			if err != nil {
				t.Errorf("worker cleanup: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("duplicate monitor worker did not finish during cleanup")
		}
	})
	select {
	case err := <-workerDone:
		workerFinished = true
		if err != nil {
			t.Fatalf("duplicate monitor worker: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("monitor timeout did not bound worker-lock acquisition")
	}

	job, err := readMonitorJob(fixture.dir)
	if err != nil || job.Status != "running" {
		t.Fatalf("worker-lock timeout changed active job status: status=%q err=%v; want running", job.Status, err)
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("timed-out duplicate removed the active worker lock: %v", err)
	}
	logData, err := os.ReadFile(filepath.Join(fixture.dir, "sessions", monitorSessionID, "session.log"))
	if err != nil || strings.Contains(string(logData), "Monitor lifetime timeout reached") {
		t.Fatalf("timed-out duplicate modified active worker lifecycle: log=%s err=%v", logData, err)
	}
	unlock()
	lockReleased = true
}

func TestMonitorWorkerTimeoutAtLockBoundaryUsesAuthoritativeRecord(t *testing.T) {
	for _, tc := range []struct {
		name, status, wantStatus string
		wantTimeoutEvent         bool
	}{
		{name: "queued job is stopped", status: "queued", wantStatus: "stopped", wantTimeoutEvent: true},
		{name: "terminal job remains terminal", status: "complete", wantStatus: "complete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newMonitorPushFixture(t)
			fixture.job.Status, fixture.job.PID = "queued", 0
			if err := saveMonitorJob(fixture.dir, fixture.job); err != nil {
				t.Fatal(err)
			}
			acquired := make(chan struct{})
			continueAcquire := make(chan struct{})
			acquireReleased := false
			releaseAcquire := func() {
				if !acquireReleased {
					close(continueAcquire)
					acquireReleased = true
				}
			}
			workerDone := make(chan error, 1)
			workerExited := make(chan struct{})
			cleanupWorker := func() {
				releaseAcquire()
				select {
				case <-workerExited:
				case <-time.After(3 * time.Second):
					t.Error("worker did not stop during test cleanup")
				}
			}
			previousAcquire := monitorAcquireWorkerLock
			monitorAcquireWorkerLock = func(path string) (func(), error) {
				unlock, err := acquireOwnedLock(path)
				if err != nil {
					return nil, err
				}
				close(acquired)
				<-continueAcquire
				return unlock, nil
			}
			t.Cleanup(func() { monitorAcquireWorkerLock = previousAcquire })
			cfg := Config{Command: "/bin/true", Args: []string{"{stage}", "{task}", "{system_prompt}"}, MonitorTimeout: "150ms"}
			go func() {
				defer close(workerExited)
				workerDone <- runMonitorWorker(fixture.job.ID, filepath.Dir(fixture.dir), cfg)
			}()
			t.Cleanup(cleanupWorker)
			select {
			case <-acquired:
			case <-time.After(3 * time.Second):
				t.Fatal("worker did not reach lock acquisition boundary")
			}
			time.Sleep(200 * time.Millisecond)
			stored, err := readMonitorJob(fixture.dir)
			if err != nil {
				t.Fatal(err)
			}
			stored.Status = tc.status
			if err := saveMonitorJob(fixture.dir, stored); err != nil {
				t.Fatal(err)
			}
			releaseAcquire()
			select {
			case err := <-workerDone:
				if err != nil {
					t.Fatalf("monitor worker: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("worker did not finish after acquisition boundary")
			}
			final, err := readMonitorJob(fixture.dir)
			if err != nil || final.Status != tc.wantStatus {
				t.Fatalf("status after timeout boundary = %q, err=%v; want %q", final.Status, err, tc.wantStatus)
			}
			logData, err := os.ReadFile(filepath.Join(fixture.dir, "sessions", monitorSessionID, "session.log"))
			if err != nil || strings.Contains(string(logData), "Monitor lifetime timeout reached") != tc.wantTimeoutEvent {
				t.Fatalf("timeout event presence=%v err=%v; want %v; log=%s", strings.Contains(string(logData), "Monitor lifetime timeout reached"), err, tc.wantTimeoutEvent, logData)
			}
		})
	}
}

func TestMonitorResetRelaunchRetriesWorkerLockUntilOldWorkerTeardown(t *testing.T) {
	fixture := newMonitorPushFixture(t)
	fixture.job.Status, fixture.job.PID = "recoverable_failure", 0
	if err := saveMonitorJob(fixture.dir, fixture.job); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(fixture.dir, "worker.lock")
	unlock, err := acquireOwnedLock(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	lockReleased := false
	t.Cleanup(func() {
		if !lockReleased {
			unlock()
		}
	})
	closedJSON := `{"number":17,"state":"CLOSED","title":"Fix","url":"https://github.com/team/repo/pull/17","headRefName":"feature","headRefOid":"` + fixture.head + `","headRepository":{"nameWithOwner":"team/repo"},"baseRefName":"main","baseRefOid":"` + fixture.head + `","comments":[],"statusCheckRollup":[]}`
	gh := "#!/bin/sh\nif [ \"$1\" = pr ]; then printf '%s\\n' '" + closedJSON + "'; exit 0; fi\nif [ \"$1\" = api ]; then echo '{\"data\":{\"repository\":{\"pullRequest\":{\"reviewThreads\":{\"nodes\":[{\"isResolved\":true,\"comments\":{\"nodes\":[],\"pageInfo\":{\"hasNextPage\":false}}}],\"pageInfo\":{\"hasNextPage\":false}}}}}}'; exit 0; fi\nexit 1\n"
	if err := os.WriteFile(filepath.Join(fixture.base, "gh"), []byte(gh), 0o700); err != nil {
		t.Fatal(err)
	}
	workerDone := make(chan error, 1)
	oldLauncher := monitorWorkerLauncher
	monitorWorkerLauncher = func(id, root string) (int, error) {
		go func() {
			workerDone <- runMonitorWorker(id, root, Config{Command: "/bin/true", Args: []string{"{task}", "{system_prompt}"}})
		}()
		return os.Getpid(), nil
	}
	t.Cleanup(func() { monitorWorkerLauncher = oldLauncher })
	if err := monitorAction("reset", filepath.Dir(fixture.dir), fixture.job.ID, Config{}, nil, io.Discard); err != nil {
		t.Fatalf("reset recoverable job: %v", err)
	}
	select {
	case err := <-workerDone:
		t.Fatalf("restarted queued worker abandoned lock wait without monitor_timeout: %v", err)
	case <-time.After(250 * time.Millisecond):
	}
	job, err := readMonitorJob(fixture.dir)
	if err != nil || job.Status != "queued" {
		t.Fatalf("replacement changed queued job before acquiring lock: job=%+v err=%v", job, err)
	}
	unlock()
	lockReleased = true
	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatalf("replacement worker: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("replacement worker did not claim queued job after teardown released lock")
	}
	job, err = readMonitorJob(fixture.dir)
	if err != nil || job.Status != "closed" {
		t.Fatalf("replacement did not monitor queued job: job=%+v err=%v", job, err)
	}
}

func TestMonitorWorkerReloadsTerminalJobAfterWaitingForLock(t *testing.T) {
	fixture := newMonitorPushFixture(t)
	fixture.job.Status = "queued"
	fixture.job.PID = 0
	if err := saveMonitorJob(fixture.dir, fixture.job); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(fixture.dir, "worker.lock")
	unlock, err := acquireOwnedLock(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	lockReleased := false
	t.Cleanup(func() {
		if !lockReleased {
			unlock()
		}
	})
	logPath := filepath.Join(fixture.dir, "sessions", monitorSessionID, "session.log")
	beforeLog, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	started := filepath.Join(fixture.base, "agent-started")
	t.Setenv("AGENT_STARTED", started)
	agent := filepath.Join(fixture.base, "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\\ntouch \\\"$AGENT_STARTED\\\"\\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Command: agent, Args: []string{"{stage}", "{task}", "{system_prompt}"}, MonitorTimeout: "3s"}
	workerDone := make(chan error, 1)
	go func() { workerDone <- runMonitorWorker(fixture.job.ID, filepath.Dir(fixture.dir), cfg) }()
	// Let the duplicate worker enter its bounded lock wait while the owner holds it.
	time.Sleep(100 * time.Millisecond)
	stored, err := readMonitorJob(fixture.dir)
	if err != nil {
		t.Fatal(err)
	}
	stored.Status = "complete"
	if err := saveMonitorJob(fixture.dir, stored); err != nil {
		t.Fatal(err)
	}
	unlock()
	lockReleased = true
	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatalf("duplicate worker: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("worker did not finish after lock release")
	}
	final, err := readMonitorJob(fixture.dir)
	if err != nil || final.Status != "complete" {
		t.Fatalf("terminal job was reopened: status=%q err=%v", final.Status, err)
	}
	afterLog, err := os.ReadFile(logPath)
	if err != nil || string(afterLog) != string(beforeLog) {
		t.Fatalf("duplicate worker modified monitor session log: before=%q after=%q err=%v", beforeLog, afterLog, err)
	}
	if _, err := os.Stat(started); !os.IsNotExist(err) {
		t.Fatalf("duplicate worker launched agent: stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(fixture.dir, "heartbeat")); !os.IsNotExist(err) {
		t.Fatalf("duplicate worker reopened worker lifecycle: heartbeat stat err=%v", err)
	}
}

func TestCaptureChangedStateRejectsReplacedPaths(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(string, string) error
	}{
		{
			name: "leaf replaced with symlink",
			setup: func(root, outside string) error {
				if err := os.Remove(filepath.Join(root, "file")); err != nil {
					return err
				}
				return os.Symlink(outside, filepath.Join(root, "file"))
			},
		},
		{
			name: "parent replaced with symlink",
			setup: func(root, outside string) error {
				if err := os.RemoveAll(filepath.Join(root, "dir")); err != nil {
					return err
				}
				return os.Symlink(filepath.Dir(outside), filepath.Join(root, "dir"))
			},
		},
		{
			name: "leaf replaced with regular file",
			setup: func(root, _ string) error {
				path := filepath.Join(root, "file")
				if err := os.Remove(path); err != nil {
					return err
				}
				return os.WriteFile(path, []byte("replacement inode"), 0o600)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, outside := t.TempDir(), filepath.Join(t.TempDir(), "outside")
			path := "file"
			if tc.name == "parent replaced with symlink" {
				path = filepath.Join("dir", "file")
				if err := os.Mkdir(filepath.Join(root, "dir"), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, path), []byte("inside"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(filepath.Join(root, path), []byte("inside"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(outside, []byte("outside secret"), 0o600); err != nil {
				t.Fatal(err)
			}
			previous := monitorAfterCaptureRead
			monitorAfterCaptureRead = func(string) error { return tc.setup(root, outside) }
			t.Cleanup(func() { monitorAfterCaptureRead = previous })
			if _, err := captureChangedState(root, []string{path}); err == nil {
				t.Fatal("capture accepted a path replaced during read")
			}
		})
	}
}

func TestCaptureChangedStateTreatsMissingNestedParentsAsDeletion(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join("private", "nested", "tracked.txt")
	fullPath := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fullPath, []byte("tracked content"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(root, "private")); err != nil {
		t.Fatal(err)
	}

	captured, err := captureChangedState(root, []string{path})
	if err != nil {
		t.Fatalf("capture nested tracked deletion: %v", err)
	}
	if len(captured) != 1 || captured[0].path != path || !captured[0].deleted {
		t.Fatalf("captured nested deletion = %+v; want deletion of %q", captured, path)
	}
}

func TestCaptureChangedStateRejectsContentMutationDuringRead(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "file")
	if err := os.WriteFile(path, []byte("initial"), 0o600); err != nil {
		t.Fatal(err)
	}
	previous := monitorAfterCaptureRead
	monitorAfterCaptureRead = func(string) error { return os.WriteFile(path, []byte("changed while capture"), 0o600) }
	t.Cleanup(func() { monitorAfterCaptureRead = previous })
	if _, err := captureChangedState(root, []string{"file"}); err == nil {
		t.Fatal("capture accepted file content changed during read")
	}
}

func TestMonitorSHA256NestedDirectoryDeletionUsesFormatWidthZeroOID(t *testing.T) {
	base := canonicalTestPath(t, t.TempDir())
	bare, repo := filepath.Join(base, "remote.git"), filepath.Join(base, "repo")
	runTestCommand(t, base, "git", "init", "--bare", "--object-format=sha256", bare)
	runTestCommand(t, base, "git", "clone", bare, repo)
	runTestCommand(t, repo, "git", "checkout", "-b", "feature")
	runTestCommand(t, repo, "git", "config", "user.name", "Factory Test")
	runTestCommand(t, repo, "git", "config", "user.email", "factory@example.test")
	if err := os.MkdirAll(filepath.Join(repo, "private", "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "private", "nested", "delete-me"), []byte("tracked\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, repo, "git", "add", "private/nested/delete-me")
	runTestCommand(t, repo, "git", "commit", "-m", "initial")
	runTestCommand(t, repo, "git", "push", "-u", "origin", "feature")
	head, err := runGit(context.Background(), repo, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if format, err := runGit(context.Background(), repo, "rev-parse", "--show-object-format"); err != nil || format != "sha256" {
		t.Fatalf("repository object format=%q err=%v", format, err)
	}
	gh := filepath.Join(base, "gh")
	ghJSON := `{"number":17,"state":"OPEN","title":"Fix","url":"https://github.com/team/repo/pull/17","headRefName":"feature","headRefOid":"` + head + `","headRepository":{"nameWithOwner":"team/repo"},"baseRefName":"main","baseRefOid":"` + head + `","comments":[],"statusCheckRollup":[{"name":"ci","state":"FAILURE"}]}`
	ghScript := `#!/bin/sh
if [ "$1" = pr ] && [ "$2" = view ]; then printf '%s\n' '` + ghJSON + `'; exit 0; fi
if [ "$1" = api ]; then echo '{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"isResolved":true,"comments":{"nodes":[],"pageInfo":{"hasNextPage":false}}}],"pageInfo":{"hasNextPage":false}}}}}}'; exit 0; fi
exit 1
`
	if err := os.WriteFile(gh, []byte(ghScript), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
	dir := filepath.Join(base, "state", "factory", "detached-jobs", "20260518T120010-0123456789ab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	job := &monitorJob{ID: filepath.Base(dir), RepoRoot: repo, Repo: "team/repo", PR: 17, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", OriginURL: bare, HeadRepoURL: bare, BaselineHead: head, TargetBaseline: head, Snapshot: strings.Repeat("a", 64)}
	store, err := NewJobStore(filepath.Dir(dir))
	if err != nil {
		t.Fatal(err)
	}
	job.Worktree, _, err = setupExistingPRWorktree(store, repo, job.HeadBranch, head, job.PR, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	job.WorkerBranch, job.OwnWorktree = "factory-monitor/"+job.ID, true
	if err := os.RemoveAll(filepath.Join(job.Worktree, "private")); err != nil {
		t.Fatal(err)
	}
	snapshot, signature, err := readSnapshot(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	job.Snapshot = signature
	if err := saveMonitorJob(dir, job); err != nil {
		t.Fatal(err)
	}
	if err := guardedCommitPush(context.Background(), dir, job, signature, snapshot, []string{"private/nested/delete-me"}); err != nil {
		t.Fatalf("publish SHA-256 deletion: %v", err)
	}
	remoteHead, err := runGit(context.Background(), bare, "rev-parse", "refs/heads/feature")
	if err != nil || remoteHead == head {
		t.Fatalf("SHA-256 deletion was not pushed: head=%q err=%v", remoteHead, err)
	}
	if _, err := runGit(context.Background(), bare, "cat-file", "-e", remoteHead+":private/nested/delete-me"); err == nil {
		t.Fatal("deleted path is present in SHA-256 published commit")
	}
}

func TestMonitorConcurrentEditsCannotChangePublishedTree(t *testing.T) {
	for _, boundary := range []string{"before staging", "after tree validation"} {
		t.Run(boundary, func(t *testing.T) {
			fixture := newMonitorPushFixture(t)
			t.Setenv("PATH", fixture.base+string(os.PathListSeparator)+os.Getenv("PATH"))
			previousBefore, previousAfter := monitorBeforeIndexStage, monitorAfterTreeValidate
			monitorBeforeIndexStage, monitorAfterTreeValidate = nil, nil
			t.Cleanup(func() { monitorBeforeIndexStage, monitorAfterTreeValidate = previousBefore, previousAfter })
			if boundary == "before staging" {
				monitorBeforeIndexStage = func(worktree string) error {
					if err := os.WriteFile(filepath.Join(worktree, "unrelated"), []byte("not approved\n"), 0o600); err != nil {
						return err
					}
					return os.WriteFile(filepath.Join(worktree, "f"), []byte("raced edit\n"), 0o600)
				}
				err := guardedCommitPush(context.Background(), fixture.dir, fixture.job, fixture.signature, fixture.snapshot, []string{"f"})
				if err == nil || !strings.Contains(err.Error(), "paths changed after agent completion") {
					t.Fatalf("staging-boundary race = %v; want fail-closed path mismatch", err)
				}
				local, _ := runGit(context.Background(), fixture.job.Worktree, "rev-parse", "HEAD")
				remote, _ := runGit(context.Background(), fixture.bare, "rev-parse", "refs/heads/feature")
				if local != fixture.head || remote != fixture.head {
					t.Fatalf("staging-boundary race published: local=%s remote=%s baseline=%s", local, remote, fixture.head)
				}
				return
			}
			monitorAfterTreeValidate = func(worktree, _ string) error {
				if err := os.WriteFile(filepath.Join(worktree, "f"), []byte("later edit\n"), 0o600); err != nil {
					return err
				}
				if err := os.WriteFile(filepath.Join(worktree, "unrelated"), []byte("not approved\n"), 0o600); err != nil {
					return err
				}
				return exec.Command("git", "-C", worktree, "add", "-A").Run()
			}
			if err := guardedCommitPush(context.Background(), fixture.dir, fixture.job, fixture.signature, fixture.snapshot, []string{"f"}); err != nil {
				t.Fatalf("publish captured immutable tree: %v", err)
			}
			local, err := runGit(context.Background(), fixture.job.Worktree, "rev-parse", "HEAD^{tree}")
			if err != nil {
				t.Fatal(err)
			}
			remoteHead, err := runGit(context.Background(), fixture.bare, "rev-parse", "refs/heads/feature")
			if err != nil {
				t.Fatal(err)
			}
			remoteTree, err := runGit(context.Background(), fixture.bare, "rev-parse", remoteHead+"^{tree}")
			if err != nil {
				t.Fatal(err)
			}
			if local != remoteTree {
				t.Fatalf("local and published commit trees differ: local=%s remote=%s", local, remoteTree)
			}
			committed, err := runGit(context.Background(), fixture.job.Worktree, "show", "HEAD:f")
			if err != nil || committed != "agent update" {
				t.Fatalf("commit used concurrent file edit: content=%q err=%v", committed, err)
			}
			if _, err := runGit(context.Background(), fixture.job.Worktree, "cat-file", "-e", "HEAD:unrelated"); err == nil {
				t.Fatal("unrelated concurrent file reached immutable published tree")
			}
		})
	}
}

func TestMonitorRetriesPublishFailuresThroughAttemptCap(t *testing.T) {
	base := canonicalTestPath(t, t.TempDir())
	bare, repo := filepath.Join(base, "remote.git"), filepath.Join(base, "repo")
	runTestCommand(t, base, "git", "init", "--bare", bare)
	runTestCommand(t, base, "git", "clone", bare, repo)
	runTestCommand(t, repo, "git", "checkout", "-b", "main")
	runTestCommand(t, repo, "git", "config", "user.name", "Factory Test")
	runTestCommand(t, repo, "git", "config", "user.email", "factory@example.test")
	if err := os.WriteFile(filepath.Join(repo, "tracked"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, repo, "git", "add", "tracked")
	runTestCommand(t, repo, "git", "commit", "-m", "initial")
	runTestCommand(t, repo, "git", "push", "-u", "origin", "main")
	runTestCommand(t, repo, "git", "branch", "feature")
	runTestCommand(t, repo, "git", "push", "origin", "feature")
	head, err := runGit(context.Background(), repo, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	root, id := filepath.Join(base, "state", "factory", "detached-jobs"), "20260518T120009-0123456789ab"
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	gh := filepath.Join(base, "gh")
	ghScript := `#!/bin/sh
if [ "$1" = api ]; then echo '{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"id":"resolved","isResolved":true,"comments":{"nodes":[],"pageInfo":{"hasNextPage":false}}}],"pageInfo":{"hasNextPage":false}}}}}}'; exit 0; fi
if [ "$1" = pr ] && [ "$2" = view ]; then
  printf '{"number":17,"state":"OPEN","title":"Fix","url":"https://github.com/team/repo/pull/17","headRefName":"feature","headRefOid":"%s","headRepository":{"nameWithOwner":"team/repo"},"baseRefName":"main","baseRefOid":"%s","comments":[],"statusCheckRollup":[{"state":"FAILURE"}]}\n' "$HEAD" "$HEAD"
  exit 0
fi
exit 1
`
	if err := os.WriteFile(gh, []byte(ghScript), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("HEAD", head)
	t.Setenv("FACTORY_MONITOR_POLL_INTERVAL", "1s")
	calls := filepath.Join(base, "agent-calls")
	t.Setenv("AGENT_CALLS", calls)
	agent := filepath.Join(base, "agent")
	agentScript := `#!/bin/sh
n=0
[ ! -f "$AGENT_CALLS" ] || n=$(cat "$AGENT_CALLS")
n=$((n+1))
printf '%s' "$n" > "$AGENT_CALLS"
printf 'retry %s\n' "$n" > tracked
printf 'FACTORY_STATUS=FIXED\n'
`
	if err := os.WriteFile(agent, []byte(agentScript), 0o700); err != nil {
		t.Fatal(err)
	}
	job := &monitorJob{ID: id, Description: "retry transient failure", RepoRoot: repo, Repo: "team/repo", PR: 17, HeadRepo: "team/repo", HeadBranch: "feature", BaseRepo: "team/repo", BaseBranch: "main", BaseSHA: head, OriginURL: bare, BaselineHead: head, TargetBaseline: head, TargetBranch: "main", Status: "running", CreatedAt: time.Now().UTC()}
	setupTestMonitorWorktree(t, repo, root, job, head)
	runTestCommand(t, job.Worktree, "git", "config", "remote.origin.pushurl", filepath.Join(base, "wrong-remote.git"))
	if err := saveMonitorJob(dir, job); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Command: agent, Args: []string{"{stage}", "{task}", "{system_prompt}"}, StateDir: filepath.Join(base, "state")}
	workerDone := make(chan error, 1)
	go func() { workerDone <- runMonitorWorker(id, root, cfg) }()
	workerFinished := false
	stopWorker := func() {
		t.Helper()
		_ = monitorAction("stop", root, id, cfg, nil, io.Discard)
		select {
		case err := <-workerDone:
			workerFinished = true
			if err != nil {
				t.Errorf("stopping monitor worker: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("monitor worker did not stop after cleanup request")
		}
	}
	t.Cleanup(func() {
		if !workerFinished {
			stopWorker()
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		current, readErr := readMonitorJob(dir)
		if readErr == nil && current.Attempts >= 3 && current.PendingSignature != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	capped, err := readMonitorJob(dir)
	if err != nil || capped.Attempts != 3 || capped.PendingSignature == "" {
		logData, _ := os.ReadFile(filepath.Join(dir, "sessions", monitorSessionID, "session.log"))
		t.Fatalf("failed event processing did not reach the attempt cap: job=%+v err=%v log=%s", capped, err, logData)
	}
	if err := monitorAction("stop", root, id, cfg, nil, io.Discard); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-workerDone:
		workerFinished = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("monitor worker did not stop promptly after reaching the attempt cap")
	}
	logData, err := os.ReadFile(filepath.Join(dir, "sessions", monitorSessionID, "session.log"))
	if err != nil || strings.Count(string(logData), "Event processing failed safely:") != 3 || !strings.Contains(string(logData), "push URL does not point") {
		t.Fatalf("publish failure was not recorded for each capped attempt: log=%s err=%v", logData, err)
	}
	count, countErr := os.ReadFile(calls)
	if countErr != nil || string(count) != "3" {
		t.Fatalf("publish retries invoked agent %q times, err=%v; want attempt cap of 3", count, countErr)
	}
	remoteHead, remoteErr := runGit(context.Background(), bare, "rev-parse", "refs/heads/feature")
	if remoteErr != nil || remoteHead != head {
		t.Fatalf("failed publish changed remote: head=%s err=%v", remoteHead, remoteErr)
	}
	localHead, localErr := runGit(context.Background(), job.Worktree, "rev-parse", "HEAD")
	if localErr != nil || localHead != head {
		t.Fatalf("failed publish created local commit: head=%s err=%v", localHead, localErr)
	}
	stopped, err := readMonitorJob(dir)
	if err != nil || stopped.Status != "stopped" {
		t.Fatalf("worker cleanup did not persist stopped status: job=%+v err=%v", stopped, err)
	}
}

func TestMonitorCommitPushFailureIsReturnedForBoundedRetry(t *testing.T) {
	fixture := newMonitorPushFixture(t)
	t.Setenv("PATH", fixture.base+string(os.PathListSeparator)+os.Getenv("PATH"))
	wrongRemote := filepath.Join(fixture.base, "wrong-remote.git")
	runTestCommand(t, fixture.repo, "git", "config", "remote.origin.pushurl", wrongRemote)
	agent := filepath.Join(fixture.base, "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\nprintf 'FACTORY_STATUS=FIXED\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Command: agent, Args: []string{"{stage}", "{task}", "{system_prompt}"}}
	err := processMonitorEventContext(context.Background(), fixture.dir, fixture.job, cfg, fixture.snapshot, fixture.signature)
	if err == nil || !strings.Contains(err.Error(), "push URL does not point") {
		t.Fatalf("guarded commit/push failure = %v; want retryable destination validation error", err)
	}
}

func TestMonitorInvalidAgentProtocolReturnsRetryableError(t *testing.T) {
	fixture := newMonitorPushFixture(t)
	t.Setenv("PATH", fixture.base+string(os.PathListSeparator)+os.Getenv("PATH"))
	agent := filepath.Join(fixture.base, "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\nprintf 'FACTORY_STATUS=UNKNOWN\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Command: agent, Args: []string{"{stage}", "{task}", "{system_prompt}"}}
	err := processMonitorEventContext(context.Background(), fixture.dir, fixture.job, cfg, fixture.snapshot, fixture.signature)
	if err == nil || !strings.Contains(err.Error(), "omitted valid final protocol") {
		t.Fatalf("invalid protocol error = %v; want retryable invalid-protocol error", err)
	}
}

func TestMonitorAgentSessionAppendFailureFailsAction(t *testing.T) {
	fixture := newMonitorPushFixture(t)
	if _, err := runGit(context.Background(), fixture.repo, "worktree", "remove", "--force", fixture.job.Worktree); err != nil {
		t.Fatal(err)
	}
	if _, err := runGit(context.Background(), fixture.repo, "branch", "-D", fixture.job.WorkerBranch); err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(filepath.Dir(fixture.dir))
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.GetJob(fixture.job.ID)
	if err != nil || job.Type != monitorJobType || job.Monitor == nil {
		t.Fatalf("canonical monitor job record missing: %+v err=%v", job, err)
	}
	if _, err := store.GetSession(fixture.job.ID, monitorSessionID); err != nil {
		t.Fatalf("canonical monitor session missing: %v", err)
	}
	worktreeStore, err := NewJobStore(filepath.Dir(fixture.dir))
	if err != nil {
		t.Fatal(err)
	}
	worktree, baseline, err := setupExistingPRWorktree(worktreeStore, fixture.repo, fixture.job.HeadBranch, fixture.head, fixture.job.PR, fixture.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	fixture.job.Worktree, fixture.job.WorkerBranch, fixture.job.BaselineHead, fixture.job.OwnWorktree = worktree, "factory-monitor/"+fixture.job.ID, baseline, true
	if err := saveMonitorJob(fixture.dir, fixture.job); err != nil {
		t.Fatal(err)
	}
	oldAppend := monitorSessionLogAppend
	monitorSessionLogAppend = func(*JobStore, string, []byte) error {
		return errors.New("injected session append failure")
	}
	t.Cleanup(func() { monitorSessionLogAppend = oldAppend })
	agent := filepath.Join(fixture.base, "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\nprintf 'FACTORY_STATUS=NO_ACTION\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Command: agent, Args: []string{"{stage}", "{task}", "{system_prompt}"}}
	err = processMonitorEventContext(context.Background(), fixture.dir, fixture.job, cfg, fixture.snapshot, fixture.signature)
	if err == nil || !strings.Contains(err.Error(), "append monitor agent log to session") {
		t.Fatalf("monitor action error = %v, want session append failure", err)
	}
}

func newMonitorRepo(t *testing.T) string {
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
