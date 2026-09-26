package factory

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDetachedImplementationJobRunsWorkerAndPersistsLifecycle(t *testing.T) {
	state := t.TempDir()
	target := canonicalTestPath(t, t.TempDir())
	script := filepath.Join(t.TempDir(), "fake-agent")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 0.2\ncase \"$2\" in *'Stage completed'*) echo PASS ;; *) echo agent-output ;; esac\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(t.TempDir(), "config", "factory")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"5s"}`, script, state)
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", filepath.Dir(configDir))
	t.Setenv("XDG_STATE_HOME", state)

	binary := filepath.Join(t.TempDir(), "factory")
	projectRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/factory")
	build.Dir = projectRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build factory: %v\n%s", err, output)
	}
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	targetLock, err := store.LockTarget(target)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Now()
	command := exec.Command(binary, "job", "start", "implementation", "exercise detached job")
	command.Dir = target
	var startOutput bytes.Buffer
	command.Stdout = &startOutput
	var startErrors bytes.Buffer
	command.Stderr = &startErrors
	if err := command.Run(); err != nil {
		t.Fatalf("start command: %v: %s", err, startErrors.String())
	}
	if time.Since(startedAt) > 2*time.Second {
		t.Fatalf("start waited for workflow completion: %s", time.Since(startedAt))
	}
	changedState := filepath.Join(t.TempDir(), "replacement-state")
	config = fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"5s"}`, script, changedState)
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(config), 0o600); err != nil {
		targetLock()
		t.Fatal(err)
	}
	targetLock()
	fields := strings.Fields(startOutput.String())
	if len(fields) < 4 {
		t.Fatalf("unexpected start output: %q", startOutput.String())
	}
	id := fields[3]
	startedJob, err := store.GetJob(id)
	if err != nil || isTerminalStatus(startedJob.Status) {
		t.Fatalf("parent returned only after worker completion: job=%+v err=%v", startedJob, err)
	}
	waitFor(t, 5*time.Second, func() bool {
		job, err := store.GetJob(id)
		return err == nil && isTerminalStatus(job.Status) && len(job.Sessions) == 1 && job.Sessions[0].Status == job.Status
	})
	job, err := store.GetJob(id)
	if err != nil || job.Status != "complete" || job.Type != implementationJobType || job.TargetPath != canonicalTestPath(t, target) || job.TaskDescription != "exercise detached job" {
		t.Fatalf("job record = %+v, err=%v", job, err)
	}
	if len(job.Sessions) != 1 || job.Sessions[0].Status != "complete" {
		t.Fatalf("session metadata = %+v", job.Sessions)
	}
	var cliOutput bytes.Buffer
	if err := JobCommand([]string{"list"}, Config{StateDir: state}, target, strings.NewReader(""), &cliOutput); err != nil || !strings.Contains(cliOutput.String(), id) {
		t.Fatalf("job list output=%q err=%v", cliOutput.String(), err)
	}
	cliOutput.Reset()
	if err := JobCommand([]string{"get", id}, Config{StateDir: state}, target, strings.NewReader(""), &cliOutput); err != nil || !strings.Contains(cliOutput.String(), "Status: complete") {
		t.Fatalf("job get output=%q err=%v", cliOutput.String(), err)
	}
	if strings.Contains(cliOutput.String(), "requirements completed") || strings.Contains(cliOutput.String(), "Description:") {
		t.Fatalf("concise job output leaked detailed content: %q", cliOutput.String())
	}
	cliOutput.Reset()
	if err := JobCommand([]string{"get", id, "--details"}, Config{StateDir: state}, target, strings.NewReader(""), &cliOutput); err != nil || !strings.Contains(cliOutput.String(), "Description: exercise detached job") || !strings.Contains(cliOutput.String(), "requirements completed") || !strings.Contains(cliOutput.String(), "Job log path:") {
		t.Fatalf("detailed job output=%q err=%v", cliOutput.String(), err)
	}
	events, err := store.SessionEvents(id, "workflow")
	if err != nil || len(events) < 4 {
		t.Fatalf("workflow events = %+v err=%v", events, err)
	}
	var sawStage bool
	for _, event := range events {
		if event.Type == "stage.started" && strings.Contains(event.Message, "requirements") {
			sawStage = true
		}
	}
	if !sawStage {
		t.Fatalf("events lack persisted stage transition: %+v", events)
	}
	logPath, err := store.JobLogPath(id)
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.ReadFile(logPath)
	if err != nil || !strings.Contains(string(log), "requirements completed") {
		t.Fatalf("worker log missing workflow output: %q err=%v", log, err)
	}
	cliOutput.Reset()
	if err := JobCommand([]string{"logs", id}, Config{StateDir: state}, target, strings.NewReader(""), &cliOutput); err != nil || cliOutput.String() != string(log) {
		t.Fatalf("job logs output=%q err=%v", cliOutput.String(), err)
	}
	sessionLogPath, err := store.SessionLogPath(id, "workflow")
	if err != nil {
		t.Fatal(err)
	}
	sessionLog, err := os.ReadFile(sessionLogPath)
	if err != nil || !strings.Contains(string(sessionLog), "stage.started") {
		t.Fatalf("session log lacks workflow events: %q err=%v", sessionLog, err)
	}
	cliOutput.Reset()
	if err := JobCommand([]string{"logs", id, "--session", "workflow"}, Config{StateDir: state}, target, strings.NewReader(""), &cliOutput); err != nil || cliOutput.String() != string(sessionLog) {
		t.Fatalf("session logs output=%q err=%v", cliOutput.String(), err)
	}
}

func TestJobShowAliasIsRejectedByHandler(t *testing.T) {
	var output bytes.Buffer
	if err := JobCommand([]string{"show", "missing"}, Config{StateDir: t.TempDir()}, t.TempDir(), strings.NewReader(""), &output); err == nil || !strings.Contains(err.Error(), `unknown job command "show"`) {
		t.Fatalf("job show alias error = %v, want handler rejection", err)
	}
	if output.Len() != 0 {
		t.Fatalf("rejected job alias wrote output: %q", output.String())
	}
}

func TestDetachedJobStopAndSameTargetAdmission(t *testing.T) {
	state := t.TempDir()
	target := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "existing", Type: implementationJobType, TargetPath: target, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := JobCommand([]string{"start", "implementation", "another"}, Config{StateDir: state}, target, strings.NewReader(""), &out); err == nil || !strings.Contains(err.Error(), "already targets") {
		t.Fatalf("duplicate target start error = %v", err)
	}
	if err := store.CreateJob(JobRecord{ID: "to-stop", Type: implementationJobType, TargetPath: t.TempDir(), Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if err := JobCommand([]string{"stop", "to-stop"}, Config{StateDir: state}, target, strings.NewReader(""), &out); err != nil {
		t.Fatal(err)
	}
	if !store.StopRequested("to-stop") {
		t.Fatal("stop command did not write its durable request")
	}
	for _, kind := range []string{"clean", "monitoring", "Implementation"} {
		if err := JobCommand([]string{"start", kind, "unsupported"}, Config{StateDir: state}, t.TempDir(), strings.NewReader(""), &out); err == nil || !strings.Contains(err.Error(), "unsupported job type") {
			t.Errorf("job type %q was accepted: err=%v", kind, err)
		}
	}
}

func TestDifferentTargetsCanAcquireIndependentJobLocks(t *testing.T) {
	store := newTestJobStore(t)
	first, second := t.TempDir(), t.TempDir()
	unlock, err := store.LockTarget(first)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	other, err := store.LockTarget(second)
	if err != nil {
		t.Fatalf("unrelated target lock blocked: %v", err)
	}
	other()
}

func TestDuplicateJobWorkerCannotRerunOrReplaceWorkerIdentity(t *testing.T) {
	stateDir := t.TempDir()
	target := t.TempDir()
	store, err := NewJobStore(filepath.Join(stateDir, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "single-run", Type: implementationJobType, TaskDescription: "do the work", TargetPath: target, Status: "queued"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession("single-run", "workflow", "queued"); err != nil {
		t.Fatal(err)
	}
	callsPath := filepath.Join(t.TempDir(), "calls")
	script := filepath.Join(t.TempDir(), "agent")
	body := fmt.Sprintf("#!/bin/sh\nprintf 'call\\n' >> %q\nsleep 0.2\necho PASS\n", callsPath)
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(t.TempDir(), "config", "factory")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q}`, script, stateDir)
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", filepath.Dir(configDir))
	firstDone := make(chan error, 1)
	go func() { firstDone <- RunJobWorker("single-run", store.Root()) }()
	waitFor(t, 3*time.Second, func() bool {
		job, err := store.GetJob("single-run")
		return err == nil && job.Status == "running"
	})
	worker, err := store.ReadWorker("single-run")
	if err != nil {
		t.Fatal(err)
	}
	duplicateDone := make(chan error, 1)
	go func() { duplicateDone <- RunJobWorker("single-run", store.Root()) }()
	select {
	case err := <-duplicateDone:
		t.Fatalf("duplicate worker bypassed the active target lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	workerDuringDuplicate, err := store.ReadWorker("single-run")
	if err != nil || workerDuringDuplicate.PID != worker.PID {
		t.Fatalf("duplicate invocation clobbered worker identity: before=%+v during=%+v err=%v", worker, workerDuringDuplicate, err)
	}
	if err := <-firstDone; err != nil {
		t.Fatalf("first worker failed: %v", err)
	}
	if err := <-duplicateDone; err == nil || !strings.Contains(err.Error(), "cannot be claimed") {
		t.Fatalf("duplicate worker error = %v, want already-claimed rejection", err)
	}
	if _, err := store.ReadWorker("single-run"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("duplicate invocation recreated or preserved worker metadata after completion: %v", err)
	}
	job, err := store.GetJob("single-run")
	if err != nil || job.Status != "complete" {
		t.Fatalf("duplicate changed terminal job state: %+v err=%v", job, err)
	}
	calls, err := os.ReadFile(callsPath)
	if err != nil || len(strings.Fields(string(calls))) < 1 {
		t.Fatalf("workflow did not run: calls=%q err=%v", calls, err)
	}
	callsBefore := len(strings.Fields(string(calls)))
	if err := RunJobWorker("single-run", store.Root()); err == nil {
		t.Fatal("terminal job worker invocation was accepted")
	}
	calls, err = os.ReadFile(callsPath)
	if err != nil || len(strings.Fields(string(calls))) != callsBefore {
		t.Fatalf("terminal duplicate reran workflow: calls=%q err=%v", calls, err)
	}
	if worker.PID <= 0 {
		t.Fatalf("first worker had invalid identity: %+v", worker)
	}
}

func TestRunJobWorkerRequiresCanonicalPrivateRoot(t *testing.T) {
	store := newTestJobStore(t)
	for _, root := range []string{"relative", store.Root() + string(filepath.Separator) + "."} {
		if err := RunJobWorker("missing", root); err == nil || !strings.Contains(err.Error(), "absolute") && !strings.Contains(err.Error(), "canonical") {
			t.Errorf("RunJobWorker root %q error=%v, want root validation failure", root, err)
		}
	}
	if err := os.Chmod(store.Root(), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "insecure-root-job", Type: implementationJobType, TargetPath: t.TempDir(), Status: "queued"}); err != nil {
		t.Fatal(err)
	}
	if err := RunJobWorker("insecure-root-job", store.Root()); err == nil || !strings.Contains(err.Error(), "private directory") {
		t.Fatalf("RunJobWorker accepted insecure root: %v", err)
	}
}

func TestAdmissionRecoversOrphanQueueButNeverStealsActiveWorker(t *testing.T) {
	state := t.TempDir()
	target := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "stale-queued", Type: implementationJobType, TargetPath: target, Status: "queued", CreatedAt: time.Now().Add(-orphanJobGracePeriod - time.Second)}); err != nil {
		t.Fatal(err)
	}
	makeJobStale(t, store, "stale-queued")
	var output bytes.Buffer
	if err := JobCommand([]string{"start", "implementation", "recovered target"}, Config{StateDir: state}, target, strings.NewReader(""), &output); err != nil {
		t.Fatalf("admission did not recover stale queued record: %v", err)
	}
	job, err := store.GetJob("stale-queued")
	if err != nil || job.Status != "interrupted" {
		t.Fatalf("stale queued job=%+v err=%v", job, err)
	}

	liveTarget := t.TempDir()
	if err := store.CreateJob(JobRecord{ID: "live-running", Type: implementationJobType, TargetPath: liveTarget, Status: "running", CreatedAt: time.Now().Add(-orphanJobGracePeriod - time.Second)}); err != nil {
		t.Fatal(err)
	}
	makeJobStale(t, store, "live-running")
	workerUnlock, err := store.LockTarget(liveTarget)
	if err != nil {
		t.Fatal(err)
	}
	startErr := JobCommand([]string{"start", "implementation", "must remain blocked"}, Config{StateDir: state}, liveTarget, strings.NewReader(""), &output)
	if startErr == nil || !strings.Contains(startErr.Error(), "already targets") {
		t.Fatalf("live worker admission error=%v, want existing active job", startErr)
	}
	job, err = store.GetJob("live-running")
	if err != nil || job.Status != "running" {
		t.Fatalf("admission stole live worker job: %+v err=%v", job, err)
	}
	workerUnlock()

	orphanTarget := t.TempDir()
	if err := store.CreateJob(JobRecord{ID: "orphan-running", Type: implementationJobType, TargetPath: orphanTarget, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	makeJobStale(t, store, "orphan-running")
	staleJob, err := store.GetJob("orphan-running")
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := store.reconcileOrphan(staleJob)
	if err != nil || !recovered {
		t.Fatalf("orphan running recovery=(%v, %v), want recovered", recovered, err)
	}
	job, err = store.GetJob("orphan-running")
	if err != nil || job.Status != "interrupted" {
		t.Fatalf("orphan running job=%+v err=%v", job, err)
	}
}

func TestJobListPrintsReadableJobsAlongsideReconciliationErrors(t *testing.T) {
	state := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Now().Add(-time.Minute)
	badTarget, malformedTarget, goodTarget := t.TempDir(), t.TempDir(), t.TempDir()
	for _, job := range []JobRecord{
		{ID: "bad-worker", Type: implementationJobType, TaskDescription: "worker metadata is corrupt", TargetPath: badTarget, Status: "running", CreatedAt: createdAt.Add(time.Minute)},
		{ID: "bad-worker-metadata", Type: implementationJobType, TaskDescription: "worker metadata is malformed", TargetPath: malformedTarget, Status: "running", CreatedAt: createdAt.Add(30 * time.Second)},
		{ID: "good-job", Type: implementationJobType, TaskDescription: "still available", TargetPath: goodTarget, Status: "complete", CreatedAt: createdAt},
	} {
		if err := store.CreateJob(job); err != nil {
			t.Fatal(err)
		}
	}
	workerPath := filepath.Join(store.Root(), "bad-worker", "worker.json")
	if err := os.WriteFile(workerPath, []byte(`{"pid":0}`), 0o600); err != nil {
		t.Fatal(err)
	}
	malformedWorkerPath := filepath.Join(store.Root(), "bad-worker-metadata", "worker.json")
	if err := os.WriteFile(malformedWorkerPath, []byte(`{"pid":"invalid"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	jobs, reconcileErr := store.reconcileJobs()
	if reconcileErr == nil || !strings.Contains(reconcileErr.Error(), "reconcile job bad-worker") || !strings.Contains(reconcileErr.Error(), "invalid worker record") ||
		!strings.Contains(reconcileErr.Error(), "reconcile job bad-worker-metadata") || !strings.Contains(reconcileErr.Error(), "cannot unmarshal string") {
		t.Fatalf("reconcile error = %v, want both contextual worker errors", reconcileErr)
	}
	if len(jobs) != 3 || jobs[0].ID != "bad-worker" || jobs[1].ID != "bad-worker-metadata" || jobs[2].ID != "good-job" {
		t.Fatalf("reconciled jobs = %+v, want all jobs sorted newest first", jobs)
	}

	var output bytes.Buffer
	listErr := JobCommand([]string{"list"}, Config{StateDir: state}, t.TempDir(), strings.NewReader(""), &output)
	if listErr == nil || !strings.Contains(listErr.Error(), "reconcile job bad-worker") || !strings.Contains(listErr.Error(), "reconcile job bad-worker-metadata") ||
		!strings.Contains(listErr.Error(), "invalid worker record") || !strings.Contains(listErr.Error(), "cannot unmarshal string") {
		t.Fatalf("job list error = %v, want both aggregate reconciliation errors", listErr)
	}
	got := output.String()
	badWorkerLine := "bad-worker"
	malformedWorkerLine := "bad-worker-metadata"
	goodJobLine := "good-job"
	if !strings.Contains(got, "ID") || !strings.Contains(got, "DESCRIPTION") || !strings.Contains(got, "TARGET") ||
		!strings.Contains(got, "worker metadata is corrupt") || !strings.Contains(got, "worker metadata is malformed") || !strings.Contains(got, "still available") {
		t.Fatalf("job list omitted readable table records despite reconciliation errors: %q", got)
	}
	if strings.Index(got, badWorkerLine) > strings.Index(got, malformedWorkerLine) || strings.Index(got, malformedWorkerLine) > strings.Index(got, goodJobLine) {
		t.Fatalf("job list records are not sorted newest first: %q", got)
	}
	lines := strings.Split(strings.TrimSpace(got), "\n")
	if len(lines) != 4 || len(lines[1]) < 24+1+16+1+20+1+56 {
		t.Fatalf("job list is not a human-readable aligned table: %q", got)
	}
}

func TestJobListSaysNoJobsWhenStoreIsEmpty(t *testing.T) {
	var output bytes.Buffer
	if err := JobCommand([]string{"list"}, Config{StateDir: t.TempDir()}, t.TempDir(), strings.NewReader(""), &output); err != nil {
		t.Fatalf("empty job list returned error: %v", err)
	}
	if output.String() != "No jobs.\n" {
		t.Fatalf("empty job list output = %q, want No jobs", output.String())
	}
	if err := JobCommand([]string{"get", "missing"}, Config{StateDir: t.TempDir()}, t.TempDir(), strings.NewReader(""), &output); err == nil {
		t.Fatal("get accepted a missing job")
	}
}

func TestJobListReconcilesEveryStaleJobWithoutStealingLiveTargets(t *testing.T) {
	state := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	staleQueuedTarget, staleRunningTarget, liveTarget := t.TempDir(), t.TempDir(), t.TempDir()
	for _, job := range []JobRecord{
		{ID: "list-stale-queued", Type: implementationJobType, TargetPath: staleQueuedTarget, Status: "queued"},
		{ID: "list-stale-running", Type: implementationJobType, TargetPath: staleRunningTarget, Status: "running"},
		{ID: "list-live-running", Type: implementationJobType, TargetPath: liveTarget, Status: "running"},
	} {
		if err := store.CreateJob(job); err != nil {
			t.Fatal(err)
		}
		if job.Status == "running" {
			makeJobStale(t, store, job.ID)
		}
	}
	makeJobStale(t, store, "list-stale-queued")
	liveUnlock, err := store.LockTarget(liveTarget)
	if err != nil {
		t.Fatal(err)
	}
	defer liveUnlock()

	var output bytes.Buffer
	if err := JobCommand([]string{"list"}, Config{StateDir: state}, t.TempDir(), strings.NewReader(""), &output); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{
		"list-stale-queued":  "interrupted",
		"list-stale-running": "interrupted",
		"list-live-running":  "running",
	} {
		job, err := store.GetJob(id)
		if err != nil || job.Status != want {
			t.Errorf("job %s after list = %+v err=%v, want status %s", id, job, err, want)
		}
		if !strings.Contains(output.String(), id) || !strings.Contains(output.String(), want) {
			t.Errorf("list output %q does not report %s as %s", output.String(), id, want)
		}
	}
}

func TestJobGetReconcilesOnlyRequestedStaleJob(t *testing.T) {
	state := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"show-stale", "show-unrelated"} {
		if err := store.CreateJob(JobRecord{ID: id, Type: implementationJobType, TargetPath: t.TempDir(), Status: "running"}); err != nil {
			t.Fatal(err)
		}
		makeJobStale(t, store, id)
	}
	var output bytes.Buffer
	if err := JobCommand([]string{"get", "show-stale"}, Config{StateDir: state}, t.TempDir(), strings.NewReader(""), &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Status: interrupted") {
		t.Fatalf("get output=%q, want reconciled status", output.String())
	}
	shown, err := store.GetJob("show-stale")
	if err != nil || shown.Status != "interrupted" {
		t.Fatalf("shown job=%+v err=%v", shown, err)
	}
	unrelated, err := store.GetJob("show-unrelated")
	if err != nil || unrelated.Status != "running" {
		t.Fatalf("get reconciled unrelated job: %+v err=%v", unrelated, err)
	}
}

func TestJobStopReconcilesStaleJobAndRejectsTerminalStop(t *testing.T) {
	state := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "stop-stale", Type: implementationJobType, TargetPath: t.TempDir(), Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession("stop-stale", "workflow", "running"); err != nil {
		t.Fatal(err)
	}
	makeJobStale(t, store, "stop-stale")
	var output bytes.Buffer
	err = JobCommand([]string{"stop", "stop-stale"}, Config{StateDir: state}, t.TempDir(), strings.NewReader(""), &output)
	if err == nil || !strings.Contains(err.Error(), "already interrupted") {
		t.Fatalf("stop stale job error=%v, want already interrupted", err)
	}
	job, err := store.GetJob("stop-stale")
	if err != nil || job.Status != "interrupted" || store.StopRequested("stop-stale") {
		t.Fatalf("stale stop result job=%+v stopRequested=%v err=%v", job, store.StopRequested("stop-stale"), err)
	}
	session, err := store.GetSession("stop-stale", "workflow")
	if err != nil || session.Status != "interrupted" {
		t.Fatalf("stale session=%+v err=%v", session, err)
	}
}

func TestJobStopLiveTargetOnlyRequestsCancellation(t *testing.T) {
	state := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	if err := store.CreateJob(JobRecord{ID: "stop-live", Type: implementationJobType, TargetPath: target, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	makeJobStale(t, store, "stop-live")
	liveUnlock, err := store.LockTarget(target)
	if err != nil {
		t.Fatal(err)
	}
	defer liveUnlock()
	var output bytes.Buffer
	if err := JobCommand([]string{"stop", "stop-live"}, Config{StateDir: state}, t.TempDir(), strings.NewReader(""), &output); err != nil {
		t.Fatal(err)
	}
	job, err := store.GetJob("stop-live")
	if err != nil || job.Status != "running" || !store.StopRequested("stop-live") {
		t.Fatalf("live stop result job=%+v stopRequested=%v err=%v", job, store.StopRequested("stop-live"), err)
	}
}

func TestRunJobWorkerStopRequestCancelsWorkflow(t *testing.T) {
	state := t.TempDir()
	target := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "stop-worker", Type: implementationJobType, TaskDescription: "wait", TargetPath: target, Status: "queued"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession("stop-worker", "workflow", "queued"); err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(t.TempDir(), "config", "factory")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(t.TempDir(), "state")
	config := fmt.Sprintf(`{"command":"/bin/sh","args":["-c","sleep 30; echo PASS; echo PASS >&2","{system_prompt}","{task}"],"state_dir":%q}`, stateDir)
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", filepath.Dir(configDir))
	t.Setenv("XDG_STATE_HOME", stateDir)
	workerStore, err := NewJobStore(filepath.Join(stateDir, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := workerStore.CreateJob(JobRecord{ID: "stop-worker", Type: implementationJobType, TaskDescription: "wait", TargetPath: target, Status: "queued"}); err != nil {
		t.Fatal(err)
	}
	if _, err := workerStore.CreateSession("stop-worker", "workflow", "queued"); err != nil {
		t.Fatal(err)
	}
	store = workerStore
	workerDone := make(chan error, 1)
	go func() { workerDone <- RunJobWorker("stop-worker", store.Root()) }()
	waitFor(t, 3*time.Second, func() bool {
		job, err := store.GetJob("stop-worker")
		if err == nil && job.Status == "running" {
			return true
		}
		select {
		case workerErr := <-workerDone:
			logPath, _ := store.JobLogPath("stop-worker")
			log, _ := os.ReadFile(logPath)
			t.Fatalf("worker exited before running: %v; log=%s", workerErr, log)
		default:
		}
		return false
	})
	var stopOutput bytes.Buffer
	if err := JobCommand([]string{"stop", "stop-worker"}, Config{StateDir: stateDir}, target, strings.NewReader(""), &stopOutput); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-workerDone:
		if err == nil || !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("worker error after stop = %v, want canceled workflow", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not observe stop request")
	}
	job, err := store.GetJob("stop-worker")
	if err != nil || job.Status != "stopped" {
		t.Fatalf("stopped worker job=%+v err=%v", job, err)
	}
}

func makeJobStale(t *testing.T, store *JobStore, id string) {
	t.Helper()
	job, err := store.GetJob(id)
	if err != nil {
		t.Fatal(err)
	}
	job.CreatedAt = time.Now().Add(-orphanJobGracePeriod - time.Second)
	job.UpdatedAt = job.CreatedAt
	dir, err := store.jobDir(id, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeJSONAtomic(dir, "job.json", job); err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}
