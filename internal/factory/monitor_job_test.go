package factory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMonitorJobStartsForForkHeadAndCreatesMatchingV2Session(t *testing.T) {
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
	runTestCommand(t, repo, "git", "remote", "set-url", "origin", "https://github.com/team/fork.git")
	head, err := runGit(context.Background(), repo, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	response := map[string]any{
		"number": 7, "state": "OPEN", "title": "Fix", "url": "https://github.com/team/repo/pull/7",
		"headRefName": "feature", "headRefOid": head,
		"headRepository": map[string]string{"nameWithOwner": "team/fork", "url": "https://github.com/team/fork"},
		"baseRefName":    "main", "baseRefOid": head,
		"comments": []any{}, "statusCheckRollup": []any{},
	}
	responseJSON, _ := json.Marshal(response)
	gh := filepath.Join(base, "gh")
	ghScript := "#!/bin/sh\ncase \"$*\" in *baseRepository*) echo 'unsupported JSON field: baseRepository' >&2; exit 2;; esac\nif [ \"$1\" = repo ]; then echo '{\"url\":\"https://github.com/team/fork\",\"sshUrl\":\"git@github.com:team/fork.git\"}'; else printf '%s\\n' '" + strings.ReplaceAll(string(responseJSON), "'", "'\\''") + "'; fi\n"
	if err := os.WriteFile(gh, []byte(ghScript), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
	state := filepath.Join(base, "state")
	cfg := Config{Command: "/bin/true", Args: []string{"{task}", "{system_prompt}"}, StateDir: state}
	oldLauncher := babysitWorkerLauncher
	babysitWorkerLauncher = func(string, string) (int, error) { return 1234, nil }
	t.Cleanup(func() { babysitWorkerLauncher = oldLauncher })
	var output bytes.Buffer
	if err := JobCommand([]string{"start", "monitor", "monitor", "this", "PR"}, cfg, repo, strings.NewReader(""), &output); err != nil {
		t.Fatalf("monitor start failed: %v", err)
	}
	id := strings.Fields(output.String())[2]
	store, err := NewJobStore(filepath.Join(state, "factory", "jobs", "v2"))
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.GetJob(id)
	if err != nil || job.ID != id || job.Type != monitorJobType || job.TaskDescription != "monitor this PR" || job.Status != "queued" {
		t.Fatalf("v2 monitor record = %+v, err=%v", job, err)
	}
	session, err := store.GetSession(id, monitorSessionID)
	if err != nil || session.JobID != id || session.Status != "queued" {
		t.Fatalf("monitor session = %+v, err=%v", session, err)
	}
	if err := JobCommand([]string{"start", "monitor", "duplicate"}, cfg, repo, strings.NewReader(""), &output); err == nil || !strings.Contains(err.Error(), "active babysitter already monitors") {
		t.Fatalf("second v2 monitor did not use babysit duplicate guard: %v", err)
	}
}

func TestMonitorListUsesReadableTableAndEmptyMessage(t *testing.T) {
	state := t.TempDir()
	var out bytes.Buffer
	if err := BabysitCommand([]string{"list"}, Config{StateDir: state}, t.TempDir(), strings.NewReader(""), &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	if out.String() != "No monitor jobs.\n" {
		t.Fatalf("empty monitor list output=%q", out.String())
	}
	id := "20260518T120010-0123456789ab"
	dir := filepath.Join(state, "factory", "jobs", id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := saveBabysitJob(dir, &babysitJob{ID: id, Repo: "team/repo", PR: 7, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := BabysitCommand([]string{"list"}, Config{StateDir: state}, t.TempDir(), strings.NewReader(""), &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"ID", "STATUS", "REPO", "PR", "UPDATED", id, "team/repo", "#7"} {
		if !strings.Contains(out.String(), field) {
			t.Errorf("monitor table missing %q: %q", field, out.String())
		}
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 || len(lines[1]) < 29+1+20+1+28 {
		t.Fatalf("monitor list is not a readable aligned table: %q", out.String())
	}
}

func TestMonitorGetAliasesKeepProposalsAndLogsOutOfConciseOutput(t *testing.T) {
	state := t.TempDir()
	id := "20260518T120010-0123456789ab"
	root := filepath.Join(state, "factory", "jobs")
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := &babysitJob{ID: id, Description: "monitor description", Repo: "team/repo", PR: 7, HeadBranch: "feature", Status: "awaiting_approval", Proposal: "please inspect"}
	if err := saveBabysitJob(dir, legacy); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"proposal.txt": "please inspect proposal", "actions.log": "monitor action log", "agent.log": "monitor agent output"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, command := range []string{"get", "describe"} {
		var out bytes.Buffer
		if err := BabysitCommand([]string{command, id}, Config{StateDir: state}, t.TempDir(), strings.NewReader(""), &out, io.Discard); err != nil {
			t.Fatalf("%s concise command: %v", command, err)
		}
		if !strings.Contains(out.String(), "Status: awaiting_approval") || strings.Contains(out.String(), "please inspect proposal") || strings.Contains(out.String(), "monitor action log") || strings.Contains(out.String(), "monitor agent output") {
			t.Fatalf("%s concise output leaked detailed data: %q", command, out.String())
		}
		out.Reset()
		if err := BabysitCommand([]string{command, id, "--details"}, Config{StateDir: state}, t.TempDir(), strings.NewReader(""), &out, io.Discard); err != nil {
			t.Fatalf("%s detailed command: %v", command, err)
		}
		for _, expected := range []string{"please inspect proposal", "monitor action log", "monitor agent output", "proposal.txt path:", "agent.log path:"} {
			if !strings.Contains(out.String(), expected) {
				t.Errorf("%s detailed output missing %q: %q", command, expected, out.String())
			}
		}
	}
}

func TestMonitorLegacyLifecycleAndResetSynchronizeV2Record(t *testing.T) {
	state := t.TempDir()
	legacyRoot := filepath.Join(state, "factory", "jobs")
	id := "20260518T120010-0123456789ab"
	dir := filepath.Join(legacyRoot, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	job := &babysitJob{ID: id, Description: "monitor PR", RepoRoot: t.TempDir(), Status: "running"}
	if err := saveBabysitJob(dir, job); err != nil {
		t.Fatal(err)
	}
	cfg := Config{StateDir: state}
	if err := createMonitorJobRecord(cfg, job); err != nil {
		t.Fatal(err)
	}
	if err := registerMonitorWorker(id, dir, os.Getpid()); err != nil {
		t.Fatal(err)
	}
	store, _ := NewJobStore(filepath.Join(state, "factory", "jobs", "v2"))
	workerBefore, err := store.ReadWorker(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := heartbeatMonitorWorker(id, dir); err != nil {
		t.Fatal(err)
	}
	workerAfter, err := store.ReadWorker(id)
	if err != nil || !workerAfter.HeartbeatAt.After(workerBefore.HeartbeatAt) {
		t.Fatalf("generic worker heartbeat did not advance: before=%+v after=%+v err=%v", workerBefore, workerAfter, err)
	}
	if err := storeMonitorSessionRunning(id, dir); err != nil {
		t.Fatal(err)
	}
	job.Status = "recoverable_failure"
	job.SnapshotFailures = babysitSnapshotMaxRetries
	job.LastEvent = "snapshot failed eight times"
	if err := saveBabysitJob(dir, job); err != nil {
		t.Fatal(err)
	}
	record, err := store.GetJob(id)
	if err != nil || record.Status != "recoverable_failure" {
		t.Fatalf("recoverable v2 job = %+v, err=%v", record, err)
	}
	if err := JobCommand([]string{"stop", id}, cfg, job.RepoRoot, strings.NewReader(""), io.Discard); err != nil {
		t.Fatal(err)
	}
	stopped, err := jobStopped(dir)
	if err != nil || !stopped {
		t.Fatalf("generic stop was not recognized by legacy monitor: stopped=%v err=%v", stopped, err)
	}
	if err := store.ClearStopRequest(id); err != nil {
		t.Fatal(err)
	}
	if err := babysitAction("stop", legacyRoot, id, cfg, strings.NewReader(""), io.Discard); err != nil {
		t.Fatal(err)
	}
	if !store.StopRequested(id) {
		t.Fatal("legacy stop did not write the generic cancellation request")
	}
	legacyStopped, err := readBabysitJob(dir)
	if err != nil {
		t.Fatal(err)
	}
	legacyStopped.Status = "recoverable_failure"
	legacyStopped.StopRequested = false
	if err := saveBabysitJob(dir, legacyStopped); err != nil {
		t.Fatal(err)
	}
	oldLauncher := babysitWorkerLauncher
	babysitWorkerLauncher = func(gotID, gotRoot string) (int, error) {
		if gotID != id || gotRoot != legacyRoot {
			t.Errorf("reset launcher got %q %q", gotID, gotRoot)
		}
		return 4321, nil
	}
	t.Cleanup(func() { babysitWorkerLauncher = oldLauncher })
	if err := babysitAction("reset", legacyRoot, id, cfg, strings.NewReader(""), io.Discard); err != nil {
		t.Fatal(err)
	}
	record, err = store.GetJob(id)
	if err != nil || record.Status != "queued" || store.StopRequested(id) {
		t.Fatalf("reset did not resume same v2 job: record=%+v stop=%v err=%v", record, store.StopRequested(id), err)
	}
	session, err := store.GetSession(id, monitorSessionID)
	if err != nil || session.Status != "queued" {
		t.Fatalf("reset did not resume same session: %+v err=%v", session, err)
	}
	if err := clearMonitorWorker(id, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadWorker(id); err == nil {
		t.Fatal("monitor worker registration remained after cleanup")
	}
	legacyStopped, err = readBabysitJob(dir)
	if err != nil {
		t.Fatal(err)
	}
	legacyStopped.Status = "closed"
	legacyStopped.LastEvent = "PR is closed; monitoring stopped."
	if err := saveBabysitJob(dir, legacyStopped); err != nil {
		t.Fatal(err)
	}
	record, err = store.GetJob(id)
	if err != nil || record.Status != "closed" || !isTerminalStatus(record.Status) {
		t.Fatalf("closed PR did not become terminal v2 state: %+v err=%v", record, err)
	}
	session, err = store.GetSession(id, monitorSessionID)
	if err != nil || session.Status != "closed" {
		t.Fatalf("closed PR session status=%+v err=%v", session, err)
	}
}

func TestReconcileStaleMonitorUpdatesMonitorSession(t *testing.T) {
	store := newTestJobStore(t)
	id := "20260518T120010-0123456789ab"
	createRunningMonitorForReconcile(t, store, id, time.Now().Add(-orphanJobGracePeriod-time.Second))
	if err := store.WriteWorker(id, WorkerRecord{PID: 1234}); err != nil {
		t.Fatal(err)
	}
	staleWorker(t, store, id)

	job, err := store.GetJob(id)
	if err != nil {
		t.Fatal(err)
	}
	legacyDir := filepath.Join(filepath.Dir(store.Root()), job.ID)
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := &babysitJob{ID: job.ID, RepoRoot: job.TargetPath, Status: "running", LastEvent: "worker started"}
	if err := writeBabysitJob(legacyDir, legacy); err != nil {
		t.Fatal(err)
	}
	targetUnlock, err := store.LockTarget(job.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer targetUnlock()
	reconciled, err := store.reconcileOrphan(job)
	if err != nil || !reconciled {
		t.Fatalf("reconcile stale monitor=(%v, %v), want reconciled", reconciled, err)
	}
	job, err = store.GetJob(id)
	if err != nil || job.Status != "interrupted" {
		t.Fatalf("monitor job=%+v err=%v", job, err)
	}
	session, err := store.GetSession(id, monitorSessionID)
	if err != nil || session.Status != "interrupted" {
		t.Fatalf("monitor session=%+v err=%v", session, err)
	}
	if _, err := store.GetSession(id, "workflow"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reconciliation created/updated workflow session: err=%v", err)
	}
	if _, err := store.ReadWorker(id); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale worker registration remained: err=%v", err)
	}
	legacy, err = readBabysitJob(legacyDir)
	if err != nil || legacy.Status != "interrupted" || legacy.LastEvent != "Monitor worker was stale; reconciliation marked the job interrupted." || !legacy.UpdatedAt.After(job.UpdatedAt) {
		t.Fatalf("legacy monitor job=%+v err=%v, want interrupted with reconciliation event", legacy, err)
	}
}

func TestReconcileStaleMonitorDoesNotMutateMismatchedLegacyJob(t *testing.T) {
	store := newTestJobStore(t)
	id := "20260518T120011-0123456789ab"
	createRunningMonitorForReconcile(t, store, id, time.Now().Add(-orphanJobGracePeriod-time.Second))
	if err := store.WriteWorker(id, WorkerRecord{PID: 1234}); err != nil {
		t.Fatal(err)
	}
	staleWorker(t, store, id)
	job, err := store.GetJob(id)
	if err != nil {
		t.Fatal(err)
	}
	legacyDir := filepath.Join(filepath.Dir(store.Root()), job.ID)
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := &babysitJob{
		ID: job.ID, RepoRoot: t.TempDir(), Status: "running", LastEvent: "unrelated job event",
	}
	if err := writeBabysitJob(legacyDir, legacy); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(legacyDir, "job.json"))
	if err != nil {
		t.Fatal(err)
	}

	if reconciled, err := store.reconcileOrphan(job); err == nil || reconciled {
		t.Fatalf("reconcile mismatched legacy monitor=(%v, %v), want safe error", reconciled, err)
	}
	after, err := os.ReadFile(filepath.Join(legacyDir, "job.json"))
	if err != nil || !bytes.Equal(after, before) {
		t.Fatalf("mismatched legacy record was mutated: err=%v", err)
	}
	legacy, err = readBabysitJob(legacyDir)
	if err != nil || legacy.Status != "running" || legacy.LastEvent != "unrelated job event" {
		t.Fatalf("unrelated legacy monitor=%+v err=%v", legacy, err)
	}
	job, err = store.GetJob(job.ID)
	if err != nil || job.Status != "running" {
		t.Fatalf("v2 monitor job=%+v err=%v, want unchanged running status", job, err)
	}
	session, err := store.GetSession(job.ID, monitorSessionID)
	if err != nil || session.Status != "running" {
		t.Fatalf("v2 monitor session=%+v err=%v, want unchanged running status", session, err)
	}
}

func TestReconcileFreshMonitorHeartbeatLeavesJobRunning(t *testing.T) {
	store := newTestJobStore(t)
	createRunningMonitorForReconcile(t, store, "fresh-monitor", time.Now().Add(-orphanJobGracePeriod-time.Second))
	if err := store.WriteWorker("fresh-monitor", WorkerRecord{PID: 1234}); err != nil {
		t.Fatal(err)
	}
	job, err := store.GetJob("fresh-monitor")
	if err != nil {
		t.Fatal(err)
	}
	reconciled, err := store.reconcileOrphan(job)
	if err != nil || reconciled {
		t.Fatalf("reconcile fresh monitor=(%v, %v), want not reconciled", reconciled, err)
	}
	job, err = store.GetJob("fresh-monitor")
	if err != nil || job.Status != "running" {
		t.Fatalf("fresh monitor job=%+v err=%v", job, err)
	}
	session, err := store.GetSession("fresh-monitor", monitorSessionID)
	if err != nil || session.Status != "running" {
		t.Fatalf("fresh monitor session=%+v err=%v", session, err)
	}
}

func TestMonitorHeartbeatSerializedBeforeReconciliation(t *testing.T) {
	store := newTestJobStore(t)
	createRunningMonitorForReconcile(t, store, "racing-monitor", time.Now().Add(-orphanJobGracePeriod-time.Second))
	if err := store.WriteWorker("racing-monitor", WorkerRecord{PID: 1234}); err != nil {
		t.Fatal(err)
	}
	staleWorker(t, store, "racing-monitor")
	job, err := store.GetJob("racing-monitor")
	if err != nil {
		t.Fatal(err)
	}

	unlock, err := store.LockJob("racing-monitor")
	if err != nil {
		t.Fatal(err)
	}
	reconciled := make(chan bool, 1)
	reconcileErr := make(chan error, 1)
	go func() {
		result, err := store.reconcileOrphan(job)
		reconciled <- result
		reconcileErr <- err
	}()
	// Model the heartbeat's atomic write while it owns the same per-job lock.
	worker, err := store.ReadWorker("racing-monitor")
	if err != nil {
		unlock()
		t.Fatal(err)
	}
	worker.HeartbeatAt = time.Now().UTC()
	dir, err := store.jobDir("racing-monitor", false)
	if err != nil {
		unlock()
		t.Fatal(err)
	}
	if err := writeJSONAtomic(dir, "worker.json", worker); err != nil {
		unlock()
		t.Fatal(err)
	}
	unlock()
	if err := <-reconcileErr; err != nil {
		t.Fatal(err)
	}
	if <-reconciled {
		t.Fatal("reconciliation ignored heartbeat written before it acquired the job lock")
	}
	job, err = store.GetJob("racing-monitor")
	if err != nil || job.Status != "running" {
		t.Fatalf("racing monitor job=%+v err=%v", job, err)
	}
}

func createRunningMonitorForReconcile(t *testing.T, store *JobStore, id string, createdAt time.Time) {
	t.Helper()
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TargetPath: t.TempDir(), Status: "running", CreatedAt: createdAt}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, monitorSessionID, "running"); err != nil {
		t.Fatal(err)
	}
	job, err := store.GetJob(id)
	if err != nil {
		t.Fatal(err)
	}
	job.UpdatedAt = createdAt
	dir, err := store.jobDir(id, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeJSONAtomic(dir, "job.json", job); err != nil {
		t.Fatal(err)
	}
}

func staleWorker(t *testing.T, store *JobStore, id string) {
	t.Helper()
	worker, err := store.ReadWorker(id)
	if err != nil {
		t.Fatal(err)
	}
	worker.HeartbeatAt = time.Now().Add(-orphanJobGracePeriod - time.Second)
	dir, err := store.jobDir(id, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeJSONAtomic(dir, "worker.json", worker); err != nil {
		t.Fatal(err)
	}
}

func TestMonitorTerminalStatusMapping(t *testing.T) {
	for legacy, want := range map[string]string{
		"completed": "merged", "closed": "closed", "recoverable_failure": "recoverable_failure",
		"running": "running", "starting": "queued", "stopped": "stopped",
	} {
		got := monitorRecordStatus(legacy)
		if got != want {
			t.Errorf("monitor status %q mapped to %q, want %q", legacy, got, want)
		}
	}
	for _, status := range []string{"merged", "closed"} {
		if !isTerminalStatus(monitorRecordStatus(status)) {
			t.Errorf("monitor terminal state %q was not terminal", status)
		}
	}
	if isTerminalStatus(monitorRecordStatus("recoverable_failure")) {
		t.Fatal("recoverable failure must remain resumable")
	}
}
