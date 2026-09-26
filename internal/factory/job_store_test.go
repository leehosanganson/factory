package factory

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestJobStore(t *testing.T) *JobStore {
	t.Helper()
	store, err := NewJobStore(filepath.Join(t.TempDir(), "factory", "jobs"))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func createTestJob(t *testing.T, store *JobStore, id string) {
	t.Helper()
	if err := store.CreateJob(JobRecord{ID: id, Kind: "test", Status: "queued"}); err != nil {
		t.Fatal(err)
	}
}

func TestJobStorePersistsVersionedJobSessionAndPrivateLogs(t *testing.T) {
	store := newTestJobStore(t)
	if err := store.CreateJob(JobRecord{ID: "job-one", Type: "implementation", TaskDescription: "make a useful change", TargetPath: t.TempDir(), Status: "queued"}); err != nil {
		t.Fatal(err)
	}
	job, err := store.GetJob("job-one")
	if err != nil {
		t.Fatal(err)
	}
	if job.Version != jobRecordVersion || job.Type != "implementation" || job.Kind != "implementation" || job.TaskDescription != "make a useful change" || !filepath.IsAbs(job.TargetPath) || job.Status != "queued" || job.CreatedAt.IsZero() || job.UpdatedAt.IsZero() {
		t.Fatalf("unexpected persisted job: %+v", job)
	}
	if _, err := store.UpdateJob(job.ID, func(job *JobRecord) error {
		job.Status = "running"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetJob(job.ID); err != nil || got.Status != "running" || got.StartedAt.IsZero() || !got.UpdatedAt.After(job.UpdatedAt) {
		t.Fatalf("updated job = %+v, err=%v", got, err)
	}

	session, err := store.CreateSession(job.ID, "session-one", "running")
	if err != nil {
		t.Fatal(err)
	}
	logPath, err := store.SessionLogPath(job.ID, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendSessionLog(job.ID, session.ID, []byte("agent output\n")); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(logPath)
	if err != nil || string(data) != "agent output\n" {
		t.Fatalf("session log = %q, err=%v", data, err)
	}
	if info, err := os.Stat(logPath); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("session log mode = %v, err=%v", info.Mode().Perm(), err)
	}
	if err := store.AppendSessionEvent(job.ID, session.ID, "stage", "implementation started"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdateSession(job.ID, session.ID, func(session *SessionRecord) error {
		session.Status = "complete"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	session, err = store.GetSession(job.ID, session.ID)
	if err != nil || session.Status != "complete" || session.StartedAt.IsZero() || session.EndedAt.IsZero() || !session.UpdatedAt.After(session.CreatedAt) {
		t.Fatalf("session status/history update = %+v, err=%v", session, err)
	}
	job, err = store.GetJob(job.ID)
	if err != nil || len(job.Sessions) != 1 || job.Sessions[0].ID != session.ID || job.Sessions[0].Status != "complete" || job.Sessions[0].EndedAt.IsZero() {
		t.Fatalf("job session metadata = %+v, err=%v", job.Sessions, err)
	}
	events, err := store.SessionEvents(job.ID, session.ID)
	if err != nil || len(events) != 1 || events[0].Type != "stage" || events[0].Message != "implementation started" {
		t.Fatalf("session events = %+v, err=%v", events, err)
	}
	jobs, err := store.ListJobs()
	if err != nil || len(jobs) != 1 || jobs[0].ID != job.ID {
		t.Fatalf("job history = %+v, err=%v", jobs, err)
	}
	sessions, err := store.ListSessions(job.ID)
	if err != nil || len(sessions) != 1 || sessions[0].ID != session.ID {
		t.Fatalf("session history = %+v, err=%v", sessions, err)
	}
}

func TestJobStoreRejectsInvalidIDsAndSymlinkEscapes(t *testing.T) {
	store := newTestJobStore(t)
	for _, id := range []string{"", ".", "..", "../outside", "/absolute", "a/b", "a\\b", strings.Repeat("x", 129)} {
		if err := store.CreateJob(JobRecord{ID: id}); err == nil {
			t.Errorf("CreateJob accepted invalid ID %q", id)
		}
		if _, err := store.GetJob(id); err == nil {
			t.Errorf("GetJob accepted invalid ID %q", id)
		}
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(store.Root(), "escape")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetJob("escape"); err == nil {
		t.Fatal("GetJob followed job directory symlink outside the state root")
	}
	if _, err := store.SessionLogPath("escape", "session"); err == nil {
		t.Fatal("SessionLogPath accepted a job path outside the state root")
	}

	createTestJob(t, store, "safe")
	if err := os.MkdirAll(filepath.Join(store.Root(), "safe", "sessions"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(store.Root(), "safe", "sessions", "outside")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SessionLogPath("safe", "outside"); err == nil {
		t.Fatal("SessionLogPath followed session directory symlink outside its parent")
	}
	for _, sessionID := range []string{"../escape", "/absolute", "bad/name", "bad\\name"} {
		if _, err := store.CreateSession("safe", sessionID, "running"); err == nil {
			t.Errorf("CreateSession accepted invalid session ID %q", sessionID)
		}
	}
}

func TestCreateSessionFailureLeavesNoPartialSession(t *testing.T) {
	store := newTestJobStore(t)
	createTestJob(t, store, "atomic-session")
	if _, err := store.CreateSession("atomic-session", "session", "running"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession("atomic-session", "session", "running"); err == nil {
		t.Fatal("duplicate session creation unexpectedly succeeded")
	}
	parent := filepath.Join(store.Root(), "atomic-session", "sessions")
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "session" {
		t.Fatalf("failed creation left partial session artifacts: %v", entries)
	}
	if _, err := store.GetSession("atomic-session", "session"); err != nil {
		t.Fatalf("existing session was damaged by failed duplicate creation: %v", err)
	}
}

func TestSessionEventMaximumLineIsEnforcedOnAppendAndRead(t *testing.T) {
	store := newTestJobStore(t)
	createTestJob(t, store, "bounded-events")
	session, err := store.CreateSession("bounded-events", "session", "running")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendSessionEvent("bounded-events", session.ID, "large", strings.Repeat("x", maxSessionEventLineBytes)); err == nil {
		t.Fatal("oversized event append succeeded")
	}
	if events, err := store.SessionEvents("bounded-events", session.ID); err != nil || len(events) != 0 {
		t.Fatalf("rejected event was persisted: events=%+v err=%v", events, err)
	}
	path := filepath.Join(store.Root(), "bounded-events", "sessions", session.ID, "events.jsonl")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", maxSessionEventLineBytes+1)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SessionEvents("bounded-events", session.ID); err == nil {
		t.Fatal("event reader accepted a line larger than the append limit")
	}
}

func TestJobStateRootUsesV2AndTargetLocksCanonicalizeAliases(t *testing.T) {
	state := t.TempDir()
	root, err := JobStateRoot(state)
	if err != nil || root != filepath.Join(state, "factory", "jobs", "v2") {
		t.Fatalf("JobStateRoot=%q err=%v", root, err)
	}
	store, err := NewJobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	alias := filepath.Join(t.TempDir(), "target-link")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	unlock, err := store.LockTarget(target)
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan func(), 1)
	failed := make(chan error, 1)
	go func() {
		release, err := store.LockTarget(alias)
		if err != nil {
			failed <- err
			return
		}
		acquired <- release
	}()
	select {
	case release := <-acquired:
		release()
		t.Fatal("canonical target alias acquired a second lock")
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(50 * time.Millisecond):
	}
	unlock()
	select {
	case release := <-acquired:
		release()
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("canonical target lock was not released")
	}
}

func TestTryLockTargetDoesNotWaitForLiveWorker(t *testing.T) {
	store := newTestJobStore(t)
	target := t.TempDir()
	unlock, err := store.LockTarget(target)
	if err != nil {
		t.Fatal(err)
	}
	if release, acquired, err := store.TryLockTarget(target); err != nil || acquired {
		if release != nil {
			release()
		}
		t.Fatalf("TryLockTarget while held = (%v, %v), want not acquired without error", acquired, err)
	}
	unlock()
	release, acquired, err := store.TryLockTarget(target)
	if err != nil || !acquired {
		t.Fatalf("TryLockTarget after release = (%v, %v), want acquired", acquired, err)
	}
	release()
}

func TestJobStoreRejectsUnsupportedRecordVersion(t *testing.T) {
	store := newTestJobStore(t)
	createTestJob(t, store, "versioned")
	path := filepath.Join(store.Root(), "versioned", "job.json")
	if err := os.WriteFile(path, []byte(`{"version":99,"id":"versioned","kind":"test","status":"queued"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetJob("versioned"); err == nil {
		t.Fatal("GetJob accepted an unsupported record version")
	}
}

func TestJobStoreStatusHistoryAndConcurrentAtomicUpdates(t *testing.T) {
	store := newTestJobStore(t)
	createTestJob(t, store, "counted")
	const workers, updates = 8, 8
	var wg sync.WaitGroup
	errCh := make(chan error, workers*updates)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < updates; i++ {
				_, err := store.UpdateJob("counted", func(job *JobRecord) error {
					// Status encodes a counter so each successful read-modify-write must be serialized.
					var count int
					if _, err := fmt.Sscanf(job.Status, "%d", &count); err != nil {
						count = 0
					}
					job.Status = strconv.Itoa(count + 1)
					return nil
				})
				if err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	job, err := store.GetJob("counted")
	if err != nil {
		t.Fatal(err)
	}
	var got int
	if _, err := fmt.Sscanf(job.Status, "%d", &got); err != nil || got != workers*updates {
		t.Fatalf("serialized status updates produced %q (%d), want %d; err=%v", job.Status, got, workers*updates, err)
	}

	created := job.CreatedAt
	if _, err := store.UpdateJob(job.ID, func(job *JobRecord) error {
		job.CreatedAt = time.Time{}
		return nil
	}); err == nil {
		t.Fatal("UpdateJob allowed mutation of immutable creation time")
	}
	job, err = store.GetJob("counted")
	if err != nil || !job.CreatedAt.Equal(created) {
		t.Fatalf("failed mutation changed stored record: %+v, err=%v", job, err)
	}
}

func TestJobStorePerJobLocksAndOwnerWorkerHeartbeats(t *testing.T) {
	store := newTestJobStore(t)
	createTestJob(t, store, "first")
	createTestJob(t, store, "second")
	unlock, err := store.LockJob("first")
	if err != nil {
		t.Fatal(err)
	}
	secondLock := make(chan func(), 1)
	secondLockErr := make(chan error, 1)
	go func() {
		release, lockErr := store.LockJob("first")
		if lockErr != nil {
			secondLockErr <- lockErr
			return
		}
		secondLock <- release
	}()
	select {
	case <-secondLock:
		t.Fatal("competing same-job lock did not wait for its owner")
	case err := <-secondLockErr:
		t.Fatalf("competing lock failed unexpectedly: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unlockSecond, err := store.LockJob("second")
	if err != nil {
		t.Fatalf("independent job lock blocked: %v", err)
	}
	unlockSecond()
	unlock()
	select {
	case release := <-secondLock:
		release()
	case err := <-secondLockErr:
		t.Fatalf("competing lock was not acquired after release: %v", err)
	case <-time.After(time.Second):
		t.Fatal("competing lock was not acquired after release")
	}

	if err := store.WriteOwner("first", JobOwner{PID: os.Getpid()}); err != nil {
		t.Fatal(err)
	}
	ownerBefore, err := store.ReadOwner("first")
	if err != nil {
		t.Fatal(err)
	}
	ownerAfter, err := store.HeartbeatOwner("first")
	if err != nil || ownerAfter.PID != ownerBefore.PID || ownerAfter.HeartbeatAt.Before(ownerBefore.HeartbeatAt) {
		t.Fatalf("owner heartbeat = %+v after %+v, err=%v", ownerAfter, ownerBefore, err)
	}
	if err := store.WriteWorker("first", WorkerRecord{PID: os.Getpid()}); err != nil {
		t.Fatal(err)
	}
	workerBefore, err := store.ReadWorker("first")
	if err != nil {
		t.Fatal(err)
	}
	workerAfter, err := store.HeartbeatWorker("first")
	if err != nil || workerAfter.PID != workerBefore.PID || workerAfter.HeartbeatAt.Before(workerBefore.HeartbeatAt) {
		t.Fatalf("worker heartbeat = %+v after %+v, err=%v", workerAfter, workerBefore, err)
	}
	if err := store.ClearWorker("first"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadWorker("first"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadWorker after ClearWorker err=%v, want not-exist", err)
	}
}

func TestJobStoreLockSerializesAcrossProcesses(t *testing.T) {
	if os.Getenv("FACTORY_JOB_LOCK_HELPER") == "1" {
		store, err := NewJobStore(os.Getenv("FACTORY_JOB_LOCK_ROOT"))
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(os.Stdout, "ready")
		unlock, err := store.LockJob("cross-process")
		if err != nil {
			t.Fatal(err)
		}
		unlock()
		fmt.Fprintln(os.Stdout, "acquired")
		return
	}
	store := newTestJobStore(t)
	createTestJob(t, store, "cross-process")
	unlock, err := store.LockJob("cross-process")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestJobStoreLockSerializesAcrossProcesses$")
	cmd.Env = append(os.Environ(), "FACTORY_JOB_LOCK_HELPER=1", "FACTORY_JOB_LOCK_ROOT="+store.Root())
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(stdout)
	ready, err := reader.ReadString('\n')
	if err != nil || ready != "ready\n" {
		unlock()
		_ = cmd.Process.Kill()
		t.Fatalf("child did not reach lock attempt: %q err=%v", ready, err)
	}
	acquired := make(chan string, 1)
	go func() {
		line, _ := reader.ReadString('\n')
		acquired <- line
	}()
	select {
	case line := <-acquired:
		unlock()
		_ = cmd.Wait()
		t.Fatalf("child acquired held lock early: %q", line)
	case <-time.After(50 * time.Millisecond):
	}
	unlock()
	select {
	case line := <-acquired:
		if line != "acquired\n" {
			t.Fatalf("child lock confirmation = %q", line)
		}
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("child did not acquire lock after release")
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("child lock acquisition failed: %v: %s", err, stderr.String())
	}
}

func TestJobStoreAtomicRecordReadsNeverObservePartialJSON(t *testing.T) {
	store := newTestJobStore(t)
	createTestJob(t, store, "atomic")
	var wg sync.WaitGroup
	wg.Add(2)
	readErr := make(chan error, 1)
	go func() {
		defer wg.Done()
		for i := 0; i < 80; i++ {
			if _, err := store.UpdateJob("atomic", func(job *JobRecord) error {
				if job.Status == "a" {
					job.Status = "b"
				} else {
					job.Status = "a"
				}
				return nil
			}); err != nil {
				readErr <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 80; i++ {
			job, err := store.GetJob("atomic")
			if err != nil {
				readErr <- err
				return
			}
			if job.Status != "a" && job.Status != "b" && job.Status != "queued" {
				readErr <- errors.New("observed partial atomic job record")
				return
			}
		}
	}()
	wg.Wait()
	select {
	case err := <-readErr:
		t.Fatal(err)
	default:
	}
}
