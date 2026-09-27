package factory

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMonitorJobStartsForForkHeadAndCreatesMatchingDetachedSession(t *testing.T) {
	base := t.TempDir()
	bare := filepath.Join(base, "remote.git")
	repo := filepath.Join(base, "repo")
	runTestCommand(t, base, "git", "init", "--bare", bare)
	runTestCommand(t, base, "git", "clone", bare, repo)
	runTestCommand(t, repo, "git", "checkout", "-b", "main")
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
		"number": 7, "state": "OPEN", "title": "Fix", "url": "https://github.com/team/fork/pull/7",
		"headRefName": "feature", "headRefOid": head,
		"headRepository": map[string]string{"nameWithOwner": "team/fork", "url": "https://github.com/team/fork"},
		"baseRefName":    "main", "baseRefOid": head,
		"comments": []any{}, "statusCheckRollup": []any{},
	}
	responseJSON, _ := json.Marshal(response)
	gh := filepath.Join(base, "gh")
	ghScript := "#!/bin/sh\ncase \"$*\" in *baseRepository*) echo 'unsupported JSON field: baseRepository' >&2; exit 2;; esac\nif [ \"$1\" = repo ]; then echo '{\"url\":\"https://github.com/team/fork\",\"sshUrl\":\"git@github.com:team/fork.git\"}'; elif [ \"$1\" = api ]; then echo '{\"data\":{\"repository\":{\"pullRequest\":{\"reviewThreads\":{\"nodes\":[],\"pageInfo\":{\"hasNextPage\":false}}}}}}'; else printf '%s\\n' '" + strings.ReplaceAll(string(responseJSON), "'", "'\\''") + "'; fi\n"
	if err := os.WriteFile(gh, []byte(ghScript), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
	state := filepath.Join(base, "state")
	cfg := Config{Command: "/bin/true", Args: []string{"{task}", "{system_prompt}"}, StateDir: state}
	oldLauncher := monitorWorkerLauncher
	monitorWorkerLauncher = func(string, string) (int, error) { return 1234, nil }
	t.Cleanup(func() { monitorWorkerLauncher = oldLauncher })
	var output bytes.Buffer
	if err := JobCommand([]string{"start", "monitor", "monitor", "this", "PR", "#7"}, cfg, repo, strings.NewReader(""), &output); err != nil {
		t.Fatalf("monitor start failed: %v", err)
	}
	id := strings.Fields(output.String())[2]
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.GetJob(id)
	if err != nil || job.ID != id || job.Type != monitorJobType || job.TaskDescription != "monitor this PR #7" || job.Status != "queued" {
		t.Fatalf("detached monitor record = %+v, err=%v", job, err)
	}
	if job.Monitor == nil || job.Monitor.ID != id || job.Monitor.Repo != "team/fork" {
		t.Fatalf("canonical monitor state missing from job record: %+v", job.Monitor)
	}
	session, err := store.GetSession(id, monitorSessionID)
	if err != nil || session.JobID != id || session.Status != "queued" {
		t.Fatalf("monitor session = %+v, err=%v", session, err)
	}
	if err := JobCommand([]string{"start", "monitor", "duplicate", "PR", "#7"}, cfg, repo, strings.NewReader(""), &output); err == nil || !strings.Contains(err.Error(), "active monitor already monitors") {
		t.Fatalf("second detached job monitor did not use monitor duplicate guard: %v", err)
	}
}

func TestMonitorListUsesCanonicalDetachedJobs(t *testing.T) {
	state := t.TempDir()
	cfg := Config{StateDir: state}
	var out bytes.Buffer
	if err := MonitorCommand([]string{"list"}, cfg, t.TempDir(), strings.NewReader(""), &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	if out.String() != "No monitor jobs.\n" {
		t.Fatalf("empty list=%q", out.String())
	}
	root, err := JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	id := "20260518T120010-0123456789ab"
	monitor := &monitorJob{ID: id, Repo: "team/repo", PR: 7, Status: "running"}
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TargetPath: t.TempDir(), Status: "running", Monitor: monitor}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, monitorSessionID, "running"); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := MonitorCommand([]string{"list"}, cfg, t.TempDir(), strings.NewReader(""), &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{id, "team/repo", "#7"} {
		if !strings.Contains(out.String(), field) {
			t.Errorf("list missing %q: %s", field, out.String())
		}
	}
}

func TestMonitorDetailsReadCanonicalRecordAndSession(t *testing.T) {
	state := t.TempDir()
	root, err := JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	id := "20260518T120010-0123456789ab"
	monitor := &monitorJob{ID: id, Description: "monitor description", Repo: "team/repo", PR: 7, HeadBranch: "feature", Status: "running", Proposal: "please inspect"}
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TaskDescription: monitor.Description, TargetPath: t.TempDir(), Status: "running", Monitor: monitor}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, monitorSessionID, "running"); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendSessionLog(id, monitorSessionID, []byte("monitor action log\n")); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := MonitorCommand([]string{"get", id}, Config{StateDir: state}, t.TempDir(), nil, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "please inspect") || strings.Contains(out.String(), "monitor action log") {
		t.Fatalf("concise output leaked details: %s", out.String())
	}
	out.Reset()
	if err := MonitorCommand([]string{"get", id, "--details"}, Config{StateDir: state}, t.TempDir(), nil, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"please inspect", "monitor action log", "Session log monitor path:"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("details missing %q: %s", want, out.String())
		}
	}
}

func TestMonitorHelpListsCanonicalCommandsOnly(t *testing.T) {
	var out bytes.Buffer
	if err := MonitorCommand([]string{"--help"}, Config{StateDir: t.TempDir()}, t.TempDir(), nil, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "factory monitor get") || strings.Contains(out.String(), "describe") {
		t.Fatalf("monitor help contains removed alias or omits canonical command: %q", out.String())
	}
}

func TestMonitorStatusSyncWritesCanonicalLifecycleStatusToJobAndSession(t *testing.T) {
	state := t.TempDir()
	root, err := JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	id := "20260518T120010-0123456789ab"
	target := t.TempDir()
	monitor := &monitorJob{ID: id, RepoRoot: target, Status: "queued"}
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TargetPath: target, Status: "queued", Monitor: monitor}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, monitorSessionID, "queued"); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, id)
	for _, status := range []string{"running", "recoverable_failure", "complete", "closed", "stopped", "failed", "interrupted"} {
		t.Run(status, func(t *testing.T) {
			monitor.Status = status
			if err := syncMonitorJobStatus(dir, monitor); err != nil {
				t.Fatal(err)
			}
			job, err := store.GetJob(id)
			if err != nil || job.Status != status {
				t.Fatalf("job status = %q, err=%v; want exact monitor status %q", job.Status, err, status)
			}
			session, err := store.GetSession(id, monitorSessionID)
			if err != nil || session.Status != status {
				t.Fatalf("session status = %q, err=%v; want exact monitor status %q", session.Status, err, status)
			}
		})
	}
}
