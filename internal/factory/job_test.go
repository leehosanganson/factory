package factory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDetachedImplementationJobRunsWorkerAndPersistsLifecycle(t *testing.T) {
	state := t.TempDir()
	target := initTestGitRepo(t)
	script := filepath.Join(t.TempDir(), "fake-agent")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 0.2\ncase \"$2\" in *'Stage completed'*) echo PASS ;; *) echo agent-output ;; esac\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(t.TempDir(), "config", "factory")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"5s","auto_publish":false}`, script, state)
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
	config = fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"5s","auto_publish":false}`, script, changedState)
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
	if err != nil || job.Status != "complete" || job.Type != implementationJobType || job.TargetPath != resolvedTestPath(t, target) || job.TaskDescription != "exercise detached job" {
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
	if err := JobCommand([]string{"get", id}, Config{StateDir: state}, target, strings.NewReader(""), &cliOutput); err != nil {
		t.Fatalf("job get output=%q err=%v", cliOutput.String(), err)
	}
	if lines := strings.Split(strings.TrimSuffix(cliOutput.String(), "\n"), "\n"); len(lines) != 2 || !strings.Contains(lines[0], "ID") || !strings.Contains(lines[1], id) || !strings.Contains(lines[1], "complete") {
		t.Fatalf("job get should render one table header and row: %q", cliOutput.String())
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

func TestJobDetailsExposePersistedImplementationWorktree(t *testing.T) {
	state := t.TempDir()
	target := resolvedTestPath(t, t.TempDir())
	worktree := filepath.Join(state, "factory", "detached-jobs", "job-id", "worktree")
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{
		ID: "job-id", Type: implementationJobType, Status: "complete", TargetPath: target,
		Worktree: worktree, WorkBranch: "factory-job/job-id",
	}); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := JobCommand([]string{"get", "job-id", "--details"}, Config{StateDir: state}, target, strings.NewReader(""), &output); err != nil {
		t.Fatalf("job get details: %v", err)
	}
	for _, want := range []string{"Worktree: " + worktree, "Work branch: factory-job/job-id"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("job details omitted %q: %s", want, output.String())
		}
	}

	output.Reset()
	if err := JobCommand([]string{"get", "job-id"}, Config{StateDir: state}, target, strings.NewReader(""), &output); err != nil {
		t.Fatalf("job get summary: %v", err)
	}
	if strings.Contains(output.String(), worktree) || strings.Contains(output.String(), "factory-job/job-id") {
		t.Errorf("concise job output unexpectedly includes worktree details: %s", output.String())
	}
}

func TestJobListSanitizesTerminalControlSequencesInPersistedText(t *testing.T) {
	state := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	job := JobRecord{
		ID: "terminal-control", Type: tidyJobType, Status: "complete",
		TaskDescription: "task\x1b[31mRED\x1b[0m", TargetPath: t.TempDir(),
	}
	if err := store.CreateJob(job); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := JobCommand([]string{"list"}, Config{StateDir: state}, t.TempDir(), nil, &out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	persisted, err := store.GetJob(job.ID)
	if err != nil || persisted.TaskDescription != job.TaskDescription {
		t.Fatalf("sanitizing output mutated persisted description: job=%+v err=%v", persisted, err)
	}
	if strings.Contains(text, "\x1b") || !strings.Contains(text, "taskRED") {
		t.Fatalf("job list did not neutralize persisted ANSI control text: %q", text)
	}
}

func TestJobGetDefaultUsesBoundedTableAndDetailsRemainComplete(t *testing.T) {
	state := t.TempDir()
	target := filepath.Join(string(filepath.Separator), "repo")
	worktree := filepath.Join(state, "worktree")
	description := strings.Repeat("long description ", 8)
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	job := JobRecord{
		ID: "compact-job", Type: tidyJobType, TaskDescription: description, TargetPath: target,
		Worktree: worktree, WorkBranch: "factory-job/compact-job", Status: "stopped",
	}
	if err := store.CreateJob(job); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(job.ID, "workflow", "stopped"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJobLog(job.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendSessionEvent(job.ID, "workflow", "stage.updated", "activity "+strings.Repeat("detail ", 20)); err != nil {
		t.Fatal(err)
	}
	logPath, err := store.JobLogPath(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("complete worker transcript\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := JobCommand([]string{"get", job.ID}, Config{StateDir: state}, target, strings.NewReader(""), &output); err != nil {
		t.Fatalf("job get: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "ID") || !strings.Contains(lines[0], "PUBLICATION") {
		t.Fatalf("default job get should have one compact table header and one row: %q", output.String())
	}
	row := lines[1]
	for _, want := range []string{job.Type, job.Status, target, "long descri…", "…", "stage.updat…"} {
		if !strings.Contains(row, want) {
			t.Errorf("default row omitted %q: %q", want, row)
		}
	}
	if len([]rune(row)) > 120 {
		t.Errorf("default row width = %d, want at most 120: %q", len([]rune(row)), row)
	}
	if strings.Contains(row, description) || strings.Contains(row, "complete worker transcript") {
		t.Fatalf("default row exposed unbounded details: %q", row)
	}

	output.Reset()
	if err := JobCommand([]string{"get", job.ID, "--details"}, Config{StateDir: state}, target, strings.NewReader(""), &output); err != nil {
		t.Fatalf("job get --details: %v", err)
	}
	for _, want := range []string{"Description: " + description, "Target: " + target, "Worktree: " + worktree, "Work branch: factory-job/compact-job", "Session: workflow (stopped)", "complete worker transcript"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("detailed job output omitted %q: %s", want, output.String())
		}
	}
}

func TestTruncateJobListFieldPreservesUnicodeCodePoints(t *testing.T) {
	if got, want := truncateJobListField("项目目录名称", 4), "项目目…"; got != want {
		t.Fatalf("truncated Unicode field = %q, want %q", got, want)
	}
}

func TestJobTableBoundsLongTargetPathsAndPreservesDetails(t *testing.T) {
	state := t.TempDir()
	shortPath := filepath.Join(string(filepath.Separator), "repo")
	longPath := filepath.Join(t.TempDir(), strings.Repeat("项目-segment-", 8))
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	jobs := []JobRecord{
		{ID: "short-path", Type: tidyJobType, Status: "complete", TargetPath: shortPath},
		{ID: "long-path", Type: tidyJobType, Status: "complete", TargetPath: longPath},
	}
	for _, job := range jobs {
		if err := store.CreateJob(job); err != nil {
			t.Fatal(err)
		}
	}
	persistedLongJob, err := store.GetJob("long-path")
	if err != nil {
		t.Fatal(err)
	}
	if want := resolvedTestPath(t, longPath); persistedLongJob.TargetPath != want {
		t.Fatalf("persisted long target path = %q, want canonical path %q", persistedLongJob.TargetPath, want)
	}

	var output bytes.Buffer
	if err := JobCommand([]string{"list"}, Config{StateDir: state}, shortPath, strings.NewReader(""), &output); err != nil {
		t.Fatalf("job list: %v", err)
	}
	rows := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
	if len(rows) != len(jobs)+1 {
		t.Fatalf("job list rows = %q", output.String())
	}
	if len([]rune(rows[0])) > 120 {
		t.Errorf("job list header width = %d, want at most 120: %q", len([]rune(rows[0])), rows[0])
	}
	var shortRow, longRow string
	for _, row := range rows[1:] {
		switch {
		case strings.Contains(row, "short-path"):
			shortRow = row
		case strings.Contains(row, "long-path"):
			longRow = row
		}
	}
	if shortRow == "" || !strings.Contains(shortRow, shortPath) {
		t.Fatalf("short target path changed in compact output: %q", shortRow)
	}
	if longRow == "" {
		t.Fatalf("job list omitted long-path: %q", output.String())
	}
	wantDisplay := string([]rune(filepath.Base(persistedLongJob.TargetPath))[:10]) + "…"
	if !strings.Contains(longRow, wantDisplay) || strings.Contains(longRow, persistedLongJob.TargetPath) {
		t.Fatalf("long target path was not bounded to its recognizable prefix: want %q in %q", wantDisplay, longRow)
	}
	if len([]rune(longRow)) > 120 {
		t.Errorf("job list row width = %d, want at most 120: %q", len([]rune(longRow)), longRow)
	}

	output.Reset()
	if err := JobCommand([]string{"get", "long-path", "--details"}, Config{StateDir: state}, shortPath, strings.NewReader(""), &output); err != nil {
		t.Fatalf("job get --details: %v", err)
	}
	if !strings.Contains(output.String(), "Target: "+persistedLongJob.TargetPath) {
		t.Fatalf("detailed output omitted complete target path %q: %s", persistedLongJob.TargetPath, output.String())
	}
}

func TestJobTableShowsPublicationOnlyForImplementationOutcomes(t *testing.T) {
	state := t.TempDir()
	target := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	jobs := []JobRecord{
		{ID: "outcome-1", Type: implementationJobType, Status: "complete", PublicationStatus: "published", TargetPath: target},
		{ID: "outcome-2", Type: implementationJobType, Status: "complete", PublicationStatus: "unpublished", TargetPath: target},
		{ID: "outcome-3", Type: implementationJobType, Status: "complete", PublicationStatus: "no-op", TargetPath: target},
		{ID: "outcome-4", Type: implementationJobType, Status: "running", TargetPath: target},
		{ID: "outcome-5", Type: tidyJobType, Status: "complete", PublicationStatus: "published", TargetPath: target},
	}
	for _, job := range jobs {
		if err := store.CreateJob(job); err != nil {
			t.Fatal(err)
		}
	}

	var list bytes.Buffer
	if err := JobCommand([]string{"list"}, Config{StateDir: state}, target, strings.NewReader(""), &list); err != nil {
		t.Fatalf("job list: %v", err)
	}
	listLines := strings.Split(strings.TrimSuffix(list.String(), "\n"), "\n")
	if len(listLines) != len(jobs)+1 || !strings.Contains(listLines[0], "PUBLICATION") {
		t.Fatalf("job list changed compact table shape or omitted publication column: %q", list.String())
	}
	for _, job := range jobs {
		var row string
		for _, line := range listLines[1:] {
			if strings.Contains(line, job.ID) {
				row = line
				break
			}
		}
		if row == "" {
			t.Fatalf("job list omitted %s: %q", job.ID, list.String())
		}
		if job.Type == implementationJobType && job.PublicationStatus != "" {
			if !strings.Contains(row, job.PublicationStatus) {
				t.Errorf("job list row %s omitted publication status %q: %q", job.ID, job.PublicationStatus, row)
			}
		} else if strings.Contains(row, "published") || strings.Contains(row, "unpublished") || strings.Contains(row, "no-op") {
			t.Errorf("job list row %s exposed a publication outcome: %q", job.ID, row)
		}

		var get bytes.Buffer
		if err := JobCommand([]string{"get", job.ID}, Config{StateDir: state}, target, strings.NewReader(""), &get); err != nil {
			t.Fatalf("job get %s: %v", job.ID, err)
		}
		getLines := strings.Split(strings.TrimSuffix(get.String(), "\n"), "\n")
		if len(getLines) != 2 || !strings.Contains(getLines[0], "PUBLICATION") {
			t.Fatalf("job get %s did not preserve one header and one row: %q", job.ID, get.String())
		}
		if job.Type == implementationJobType && job.PublicationStatus != "" {
			if !strings.Contains(getLines[1], job.PublicationStatus) {
				t.Errorf("job get row %s omitted publication status %q: %q", job.ID, job.PublicationStatus, getLines[1])
			}
		} else if strings.Contains(getLines[1], "published") || strings.Contains(getLines[1], "unpublished") || strings.Contains(getLines[1], "no-op") {
			t.Errorf("job get row %s exposed a publication outcome: %q", job.ID, getLines[1])
		}
	}
}

func TestDetachedTidyJobRunsNonpublishingAndIsListed(t *testing.T) {
	state := t.TempDir()
	repo := newCleanRepo(t)
	target := resolvedTestPath(t, repo.work)
	if err := os.WriteFile(filepath.Join(target, "Makefile"), []byte("fmt test vet:\n\t@true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "fake-agent")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf 'PASS\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(t.TempDir(), "config", "factory")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"5s","auto_publish":false}`, script, state)
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", filepath.Dir(configDir))
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	id, err := newJobID()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: id, Type: tidyJobType, TaskDescription: "tidy task", TargetPath: target, Status: "queued"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, "workflow", "queued"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJobLog(id); err != nil {
		t.Fatal(err)
	}
	workerDone := make(chan error, 1)
	go func() { workerDone <- RunJobWorker(id, store.Root()) }()
	select {
	case err = <-workerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("tidy worker did not finish")
	}
	if err != nil {
		t.Fatalf("tidy worker failed: %v", err)
	}
	job, err := store.GetJob(id)
	if err != nil || job.Type != tidyJobType || job.Status != "complete" || len(job.Sessions) != 1 || job.Sessions[0].ID != "workflow" {
		t.Fatalf("tidy job lifecycle = %+v err=%v", job, err)
	}
	var output bytes.Buffer
	if err := JobCommand([]string{"get", id, "--details"}, Config{StateDir: state}, target, strings.NewReader(""), &output); err != nil || !strings.Contains(output.String(), "Description: tidy task") || !strings.Contains(output.String(), "Session: workflow (complete)") {
		t.Fatalf("tidy details unavailable: output=%q err=%v", output.String(), err)
	}
	output.Reset()
	if err := JobCommand([]string{"logs", id, "--session", "workflow"}, Config{StateDir: state}, target, strings.NewReader(""), &output); err != nil || !strings.Contains(output.String(), "stage.started") {
		t.Fatalf("tidy session log unavailable: output=%q err=%v", output.String(), err)
	}
	output.Reset()
	if err := AttachJob(context.Background(), store, id, &output); err != nil {
		t.Fatalf("attach to completed tidy job: %v", err)
	}
	output.Reset()
	if err := JobCommand([]string{"list"}, Config{StateDir: state}, target, strings.NewReader(""), &output); err != nil || !strings.Contains(output.String(), id) || !strings.Contains(output.String(), tidyJobType) {
		t.Fatalf("job list omitted tidy: output=%q err=%v", output.String(), err)
	}
	logPath, err := store.SessionLogPath(id, "workflow")
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.ReadFile(logPath)
	if err != nil || !strings.Contains(string(log), "stage.started") {
		t.Fatalf("tidy workflow session unavailable: %q err=%v", log, err)
	}
}

func TestDetachedTidyJobFailureAndStopStatuses(t *testing.T) {
	for _, tc := range []struct {
		name       string
		scriptBody string
		stop       bool
		wantStatus string
	}{
		{name: "failure", scriptBody: "exit 9\n", wantStatus: "failed"},
		{name: "stop", scriptBody: "exec sleep 30\n", stop: true, wantStatus: "stopped"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := t.TempDir()
			repo := newCleanRepo(t)
			target := resolvedTestPath(t, repo.work)
			_ = os.WriteFile(filepath.Join(target, "Makefile"), []byte("fmt test vet:\n\t@true\n"), 0o600)
			script := filepath.Join(t.TempDir(), "agent")
			if err := os.WriteFile(script, []byte("#!/bin/sh\n"+tc.scriptBody), 0o700); err != nil {
				t.Fatal(err)
			}
			configDir := filepath.Join(t.TempDir(), "config", "factory")
			if err := os.MkdirAll(configDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"5s","auto_publish":false}`, script, state)), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("XDG_CONFIG_HOME", filepath.Dir(configDir))
			store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
			if err != nil {
				t.Fatal(err)
			}
			id, err := newJobID()
			if err != nil {
				t.Fatal(err)
			}
			if err := store.CreateJob(JobRecord{ID: id, Type: tidyJobType, TaskDescription: "test", TargetPath: target, Status: "queued"}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.CreateSession(id, "workflow", "queued"); err != nil {
				t.Fatal(err)
			}
			if err := store.CreateJobLog(id); err != nil {
				t.Fatal(err)
			}
			workerDone := make(chan error, 1)
			go func() { workerDone <- RunJobWorker(id, store.Root()) }()
			if tc.stop {
				waitFor(t, 3*time.Second, func() bool {
					job, err := store.GetJob(id)
					return err == nil && job.Status == "running"
				})
				if err := JobCommand([]string{"stop", id}, Config{StateDir: state}, target, strings.NewReader(""), io.Discard); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-workerDone:
			case <-time.After(8 * time.Second):
				t.Fatal("tidy job worker did not exit")
			}
			job, err := store.reconcileJob(id)
			if err != nil || job.Status != tc.wantStatus || job.Sessions[0].Status != tc.wantStatus {
				t.Fatalf("tidy lifecycle status=%+v err=%v, want %s", job, err, tc.wantStatus)
			}
		})
	}
}

func TestDetachedImplementationJobFailurePersistsWorkflowStatus(t *testing.T) {
	state := t.TempDir()
	target := t.TempDir()
	script := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 7\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(t.TempDir(), "config", "factory")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"5s","auto_publish":false}`, script, state)), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", filepath.Dir(configDir))
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	id, err := newJobID()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: id, Type: implementationJobType, TaskDescription: "failing task", TargetPath: target, Status: "queued"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, "workflow", "queued"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJobLog(id); err != nil {
		t.Fatal(err)
	}
	if err := RunJobWorker(id, store.Root()); err == nil {
		t.Fatal("failing implementation worker returned success")
	}
	job, err := store.reconcileJob(id)
	if err != nil || job.Status != "failed" || len(job.Sessions) != 1 || job.Sessions[0].Status != "failed" {
		t.Fatalf("failed implementation lifecycle=%+v err=%v", job, err)
	}
}

func TestDetachedWorkflowTypesShareTargetAdmission(t *testing.T) {
	state, target := t.TempDir(), t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "active-tidy", Type: tidyJobType, TargetPath: target, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if _, err := startImplementationJob(store, Config{}, target, "duplicate implementation"); err == nil || !strings.Contains(err.Error(), "already targets") {
		t.Fatalf("implementation admission did not conflict with tidy: %v", err)
	}
	if _, err := startTidyJob(store, Config{}, target, "duplicate tidy"); err == nil || !strings.Contains(err.Error(), "already targets") {
		t.Fatalf("tidy admission did not conflict with active tidy: %v", err)
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
	if err := JobCommand([]string{"start", "tidy", "another"}, Config{StateDir: state}, target, strings.NewReader(""), &out); err == nil || !strings.Contains(err.Error(), "already targets") {
		t.Fatalf("cross-type duplicate target start error = %v", err)
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
	config := fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"auto_publish":false}`, script, stateDir)
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

func TestRunJobWorkerRequiresResolvedPrivateRoot(t *testing.T) {
	store := newTestJobStore(t)
	for _, root := range []string{"relative", store.Root() + string(filepath.Separator) + "."} {
		if err := RunJobWorker("missing", root); err == nil || !strings.Contains(err.Error(), "absolute") && !strings.Contains(err.Error(), "resolved") {
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

func TestDetachedImplementationPublicationOutcomePersistsAndCleansOnlyOnPublish(t *testing.T) {
	for _, outcome := range []struct {
		name      string
		published bool
		noOp      bool
	}{
		{name: "published", published: true},
		{name: "unpublished"},
		{name: "no-op", noOp: true},
	} {
		t.Run(outcome.name, func(t *testing.T) {
			published := outcome.published
			stateDir := t.TempDir()
			target := initTestGitRepo(t)
			baseline := strings.TrimSpace(runPublishTestGit(t, "-C", target, "rev-parse", "HEAD"))
			var remote string
			if published {
				remote = filepath.Join(t.TempDir(), "origin.git")
				runPublishTestGit(t, "init", "--bare", remote)
				runPublishTestGit(t, "-C", target, "remote", "add", "origin", remote)
			}
			parent := filepath.Join(t.TempDir(), "jobs-worktrees")
			worktree, branch, err := createImplementationWorktreeAtParent(target, target, baseline, "test", "abcd-job-publication", parent)
			if err != nil {
				t.Fatal(err)
			}
			agent := filepath.Join(t.TempDir(), "agent")
			agentScript := "#!/bin/sh\nprintf 'PASS\\n'\n"
			if !outcome.noOp {
				agentScript += "printf 'generated\\n' > generated.txt\n"
			}
			if err := os.WriteFile(agent, []byte(agentScript), 0o700); err != nil {
				t.Fatal(err)
			}
			bin := t.TempDir()
			if err := os.WriteFile(filepath.Join(bin, "gh"), []byte("#!/bin/sh\nprintf 'https://github.com/example/repo/pull/1\\n'\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			oldPath := os.Getenv("PATH")
			t.Setenv("PATH", bin+string(os.PathListSeparator)+oldPath)
			configDir := filepath.Join(t.TempDir(), "config", "factory")
			if err := os.MkdirAll(configDir, 0o700); err != nil {
				t.Fatal(err)
			}
			config := fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"auto_publish":true}`, agent, stateDir)
			if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("XDG_CONFIG_HOME", filepath.Dir(configDir))
			t.Setenv("XDG_STATE_HOME", stateDir)
			store, err := NewJobStore(filepath.Join(stateDir, "factory", "detached-jobs"))
			if err != nil {
				t.Fatal(err)
			}
			if err := store.CreateJob(JobRecord{
				ID: "abcd-job-publication", Type: implementationJobType, TaskDescription: "publish changes",
				TargetPath: target, RepositoryPath: target, TargetBranch: publishTestTargetBranch(t, target), TargetHead: baseline,
				Worktree: worktree, WorkBranch: branch, Status: "queued",
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.CreateSession("abcd-job-publication", "workflow", "queued"); err != nil {
				t.Fatal(err)
			}
			if err := store.CreateJobLog("abcd-job-publication"); err != nil {
				t.Fatal(err)
			}
			if err := RunJobWorker("abcd-job-publication", store.Root()); err != nil {
				t.Fatalf("worker: %v", err)
			}
			job, err := store.GetJob("abcd-job-publication")
			wantPublication := "unpublished"
			if published {
				wantPublication = "published"
			} else if outcome.noOp {
				wantPublication = "no-op"
			}
			if err != nil || job.Status != "complete" || job.PublicationStatus != wantPublication || job.PublicationSummary == "" {
				t.Fatalf("job outcome = %+v, err=%v", job, err)
			}
			session, err := store.GetSession("abcd-job-publication", "workflow")
			if err != nil || session.Status != "complete" {
				t.Fatalf("workflow session = %+v, err=%v; session lifecycle must remain complete", session, err)
			}
			if published {
				if _, err := os.Stat(worktree); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("published worktree remains: %v", err)
				}
				if _, err := os.Stat(parent); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("empty worktree parent remains: %v", err)
				}
			} else if info, err := os.Stat(worktree); err != nil || !info.IsDir() {
				t.Fatalf("unpublished recovery worktree missing: %v", err)
			}
			var output bytes.Buffer
			if err := JobCommand([]string{"get", "abcd-job-publication", "--details"}, Config{StateDir: stateDir}, target, strings.NewReader(""), &output); err != nil || !strings.Contains(output.String(), "Publication: "+job.PublicationStatus) {
				t.Fatalf("job get output=%q err=%v", output.String(), err)
			}
		})
	}
}

func TestImplementationOptOutStillRequiresGitButAllowsDirtyCheckout(t *testing.T) {
	cfg := Config{StateDir: filepath.Join(t.TempDir(), "state"), Command: "/bin/true", Args: []string{"{task}"}, AutoPublish: false}
	if _, err := StartImplementationJob(cfg, t.TempDir(), "non-Git target"); err == nil || !strings.Contains(err.Error(), "Git checkout") {
		t.Fatalf("non-Git implementation with publication disabled error = %v", err)
	}
	target := initTestGitRepo(t)
	if err := os.WriteFile(filepath.Join(target, "user-change.txt"), []byte("preserve me"), 0o600); err != nil {
		t.Fatal(err)
	}
	id, err := StartImplementationJob(cfg, target, "dirty Git target")
	if err != nil {
		t.Fatalf("publication opt-out should allow a dirty Git checkout: %v", err)
	}
	job, err := NewJobStore(filepath.Join(cfg.StateDir, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	record, err := job.GetJob(id)
	if err != nil || record.Worktree == "" {
		t.Fatalf("dirty-checkout implementation job = %+v, %v", record, err)
	}
	if status := runPublishTestGit(t, "-C", target, "status", "--porcelain"); !strings.Contains(status, "user-change.txt") {
		t.Fatalf("existing user change was not retained: %s", status)
	}
}

func TestAdmissionRecoversOrphanQueueButNeverStealsActiveWorker(t *testing.T) {
	state := t.TempDir()
	target := initTestGitRepo(t)
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

	liveTarget := initTestGitRepo(t)
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
	var limitedOutput bytes.Buffer
	limitedErr := JobCommand([]string{"list", "--limit", "1"}, Config{StateDir: state}, t.TempDir(), strings.NewReader(""), &limitedOutput)
	if limitedErr == nil || !strings.Contains(limitedErr.Error(), "reconcile job bad-worker") || !strings.Contains(limitedErr.Error(), "reconcile job bad-worker-metadata") {
		t.Fatalf("limited job list error = %v, want all reconciliation errors", limitedErr)
	}
	if strings.Contains(limitedOutput.String(), "bad-worker-metadata") || !strings.Contains(limitedOutput.String(), "bad-worker") || strings.Contains(limitedOutput.String(), "good-job") {
		t.Fatalf("limited job list should truncate output only after reconciling all jobs: %q", limitedOutput.String())
	}
	got := output.String()
	badWorkerLine := "bad-worker"
	malformedWorkerLine := "bad-worker-metadata"
	goodJobLine := "good-job"
	if !strings.Contains(got, "ID") || !strings.Contains(got, "DESCRIPTION") || !strings.Contains(got, "TARGET") ||
		strings.Count(got, "worker meta…") != 2 || !strings.Contains(got, "still avail…") {
		t.Fatalf("job list omitted readable table records despite reconciliation errors: %q", got)
	}
	if strings.Index(got, badWorkerLine) > strings.Index(got, malformedWorkerLine) || strings.Index(got, malformedWorkerLine) > strings.Index(got, goodJobLine) {
		t.Fatalf("job list records are not sorted newest first: %q", got)
	}
	lines := strings.Split(strings.TrimSpace(got), "\n")
	if len(lines) != 4 || len([]rune(lines[1])) > 120 || !strings.Contains(lines[1], "implementation") || !strings.Contains(lines[1], "running") {
		t.Fatalf("job list is not a human-readable aligned table: %q", got)
	}
}

func TestJobListLimitReconcilesEveryPersistedJobBeforeLimitingOutput(t *testing.T) {
	state := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Now().Add(-6 * time.Minute)
	const count = 24
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("history-%02d", index)
		if err := store.CreateJob(JobRecord{
			ID: id, Type: tidyJobType, Status: "complete", TargetPath: t.TempDir(),
			CreatedAt: createdAt.Add(time.Duration(index) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}

	var output bytes.Buffer
	if err := JobCommand([]string{"list", "--limit", "3"}, Config{StateDir: state}, t.TempDir(), strings.NewReader(""), &output); err != nil {
		t.Fatalf("limited job list: %v", err)
	}
	jobs, err := store.ListJobs()
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != count {
		t.Fatalf("retained job count = %d, want %d", len(jobs), count)
	}
	for index, job := range jobs {
		wantID := fmt.Sprintf("history-%02d", count-index-1)
		if job.ID != wantID || job.Status != "complete" {
			t.Errorf("persisted job %d = (%s, %s), want (%s, complete)", index, job.ID, job.Status, wantID)
		}
		if index < 3 && !strings.Contains(output.String(), wantID) {
			t.Errorf("limited output omitted newest job %s: %q", wantID, output.String())
		}
		if index >= 3 && strings.Contains(output.String(), wantID) {
			t.Errorf("limited output included older job %s: %q", wantID, output.String())
		}
	}
	if lines := strings.Split(strings.TrimSpace(output.String()), "\n"); len(lines) != 4 {
		t.Fatalf("limited list printed %d lines, want header plus three jobs: %q", len(lines), output.String())
	}
}

func TestJobListLimitShowsNewestJobsAndOmittedLimitRemainsUnbounded(t *testing.T) {
	state := t.TempDir()
	target := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Now().Add(-4 * time.Minute)
	for index, id := range []string{"oldest-job", "older-job", "newer-job", "newest-job"} {
		if err := store.CreateJob(JobRecord{
			ID: id, Type: tidyJobType, Status: "complete", TargetPath: target,
			CreatedAt: createdAt.Add(time.Duration(index) * time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
	}

	var unbounded bytes.Buffer
	if err := JobCommand([]string{"list"}, Config{StateDir: state}, target, strings.NewReader(""), &unbounded); err != nil {
		t.Fatalf("unbounded job list: %v", err)
	}
	unboundedLines := strings.Split(strings.TrimSpace(unbounded.String()), "\n")
	if len(unboundedLines) != 5 {
		t.Fatalf("unbounded job list printed %d lines, want header and all four jobs: %q", len(unboundedLines), unbounded.String())
	}
	for index, id := range []string{"newest-job", "newer-job", "older-job", "oldest-job"} {
		if !strings.Contains(unboundedLines[index+1], id) {
			t.Errorf("unbounded job list row %d = %q, want %s in newest-first order", index+1, unboundedLines[index+1], id)
		}
	}

	var limited bytes.Buffer
	if err := JobCommand([]string{"list", "--limit", "2"}, Config{StateDir: state}, target, strings.NewReader(""), &limited); err != nil {
		t.Fatalf("limited job list: %v", err)
	}
	limitedLines := strings.Split(strings.TrimSpace(limited.String()), "\n")
	if len(limitedLines) != 3 {
		t.Fatalf("limited job list printed %d lines, want header and two jobs: %q", len(limitedLines), limited.String())
	}
	for index, id := range []string{"newest-job", "newer-job"} {
		if !strings.Contains(limitedLines[index+1], id) {
			t.Errorf("limited job list row %d = %q, want %s in newest-first order", index+1, limitedLines[index+1], id)
		}
	}
	for _, omitted := range []string{"older-job", "oldest-job"} {
		if strings.Contains(limited.String(), omitted) {
			t.Errorf("limited job list unexpectedly included %s: %q", omitted, limited.String())
		}
	}
}

func TestJobListFiltersBeforeApplyingLimit(t *testing.T) {
	state := t.TempDir()
	target := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Now().Add(-5 * time.Minute)
	jobs := []JobRecord{
		{ID: "matching-old", Type: implementationJobType, Status: "running", TargetPath: target, CreatedAt: createdAt},
		{ID: "wrong-status", Type: implementationJobType, Status: "complete", TargetPath: target, CreatedAt: createdAt.Add(time.Minute)},
		{ID: "wrong-type", Type: tidyJobType, Status: "running", TargetPath: target, CreatedAt: createdAt.Add(2 * time.Minute)},
		{ID: "matching-new", Type: implementationJobType, Status: "running", TargetPath: target, CreatedAt: createdAt.Add(3 * time.Minute)},
	}
	for _, job := range jobs {
		if err := store.CreateJob(job); err != nil {
			t.Fatal(err)
		}
	}

	var output bytes.Buffer
	if err := JobCommand([]string{"list", "--status", "running", "--type", "implementation", "--limit", "1"}, Config{StateDir: state}, target, strings.NewReader(""), &output); err != nil {
		t.Fatalf("filtered job list: %v", err)
	}
	if !strings.Contains(output.String(), "matching-new") || strings.Contains(output.String(), "matching-old") || strings.Contains(output.String(), "wrong-status") || strings.Contains(output.String(), "wrong-type") {
		t.Fatalf("filtered newest-one output = %q", output.String())
	}

	for _, tc := range []struct {
		name string
		args []string
		want []string
		omit []string
	}{
		{name: "status only", args: []string{"list", "--status", "running"}, want: []string{"matching-old", "wrong-type", "matching-new"}, omit: []string{"wrong-status"}},
		{name: "type only", args: []string{"list", "--type", "tidy"}, want: []string{"wrong-type"}, omit: []string{"matching-old", "wrong-status", "matching-new"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output.Reset()
			if err := JobCommand(tc.args, Config{StateDir: state}, target, strings.NewReader(""), &output); err != nil {
				t.Fatalf("filtered job list: %v", err)
			}
			for _, want := range tc.want {
				if !strings.Contains(output.String(), want) {
					t.Errorf("filtered output omitted %q: %s", want, output.String())
				}
			}
			for _, omitted := range tc.omit {
				if strings.Contains(output.String(), omitted) {
					t.Errorf("filtered output included %q: %s", omitted, output.String())
				}
			}
		})
	}
}

func TestJobListRejectsInvalidOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "missing limit", args: []string{"list", "--limit"}, want: "--limit requires a positive integer"},
		{name: "duplicate limit", args: []string{"list", "--limit", "1", "--limit", "2"}, want: "--limit may only be specified once"},
		{name: "zero limit", args: []string{"list", "--limit", "0"}, want: "--limit must be a positive integer"},
		{name: "negative limit", args: []string{"list", "--limit", "-1"}, want: "--limit must be a positive integer"},
		{name: "noninteger limit", args: []string{"list", "--limit", "1.5"}, want: "--limit must be a positive integer"},
		{name: "missing status", args: []string{"list", "--status"}, want: "--status requires one of"},
		{name: "invalid status", args: []string{"list", "--status", "unknown"}, want: "invalid --status"},
		{name: "duplicate status", args: []string{"list", "--status", "running", "--status", "failed"}, want: "--status may only be specified once"},
		{name: "missing type", args: []string{"list", "--type"}, want: "--type requires one of"},
		{name: "invalid type", args: []string{"list", "--type", "unknown"}, want: "invalid --type"},
		{name: "duplicate type", args: []string{"list", "--type", "tidy", "--type", "monitor"}, want: "--type may only be specified once"},
		{name: "unknown option", args: []string{"list", "--other"}, want: "unknown job list argument"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			err := JobCommand(tc.args, Config{StateDir: t.TempDir()}, t.TempDir(), strings.NewReader(""), &output)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("job list error = %v, want %q", err, tc.want)
			}
			if output.Len() != 0 {
				t.Fatalf("invalid job list arguments printed output: %q", output.String())
			}
		})
	}
}

func TestJobListFiltersToNoJobs(t *testing.T) {
	state := t.TempDir()
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(JobRecord{ID: "completed-tidy", Type: tidyJobType, Status: "complete", TargetPath: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := JobCommand([]string{"list", "--type", "monitor"}, Config{StateDir: state}, t.TempDir(), strings.NewReader(""), &output); err != nil {
		t.Fatalf("empty matching list returned error: %v", err)
	}
	if output.String() != "No jobs.\n" {
		t.Fatalf("empty matching output = %q, want No jobs", output.String())
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
		if !strings.Contains(output.String(), id) || !strings.Contains(output.String(), truncateJobListField(want, 18)) {
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
	if lines := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n"); len(lines) != 2 || !strings.Contains(lines[0], "STATUS") || !strings.Contains(lines[1], "interrupted") {
		t.Fatalf("get output=%q, want a compact row with reconciled status", output.String())
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
	target := initTestGitRepo(t)
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
	config := fmt.Sprintf(`{"command":"/bin/sh","args":["-c","sleep 30; echo PASS; echo PASS >&2","{system_prompt}","{task}"],"state_dir":%q,"auto_publish":false}`, stateDir)
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
