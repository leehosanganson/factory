package factory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	ghScript := "#!/bin/sh\ncase \"$*\" in *baseRepository*) echo 'unsupported JSON field: baseRepository' >&2; exit 2;; esac\nif [ \"$1\" = repo ]; then echo '{\"url\":\"https://github.com/team/fork\",\"sshUrl\":\"git@github.com:team/fork.git\"}'; elif [ \"$1\" = api ]; then echo '{\"data\":{\"repository\":{\"pullRequest\":{\"reviewThreads\":{\"nodes\":[{\"isResolved\":true,\"comments\":{\"nodes\":[],\"pageInfo\":{\"hasNextPage\":false}}}],\"pageInfo\":{\"hasNextPage\":false}}}}}}'; else printf '%s\\n' '" + strings.ReplaceAll(string(responseJSON), "'", "'\\''") + "'; fi\n"
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
	wantParent := resolvedTestPath(t, filepath.Join(filepath.Dir(repo), filepath.Base(repo)+".worktrees"))
	if job.Monitor.WorktreeParent != wantParent {
		t.Fatalf("registered monitor worktree parent = %q, want %q", job.Monitor.WorktreeParent, wantParent)
	}
	session, err := store.GetSession(id, monitorSessionID)
	if err != nil || session.JobID != id || session.Status != "queued" {
		t.Fatalf("monitor session = %+v, err=%v", session, err)
	}
	if err := JobCommand([]string{"start", "monitor", "duplicate", "PR", "#7"}, cfg, repo, strings.NewReader(""), &output); err == nil || !strings.Contains(err.Error(), "active monitor already monitors") {
		t.Fatalf("second detached job monitor did not use monitor duplicate guard: %v", err)
	}
}

func TestMonitorListBoundsRepositoryColumnButGetShowsFullValue(t *testing.T) {
	state := t.TempDir()
	root, err := JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "20260518T120010-0123456789ab"
	const repo = "a-very-long-organization-name/repository-with-a-long-name"
	monitor := &monitorJob{
		ID: id, Repo: repo, PR: 7, Status: "running",
		CreatedAt: time.Date(2026, time.May, 18, 12, 0, 10, 0, time.UTC),
	}
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TargetPath: t.TempDir(), Status: "running", Monitor: monitor}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, monitorSessionID, "running"); err != nil {
		t.Fatal(err)
	}

	cfg := Config{StateDir: state}
	var listOut bytes.Buffer
	if err := MonitorCommand([]string{"list"}, cfg, t.TempDir(), strings.NewReader(""), &listOut, io.Discard); err != nil {
		t.Fatal(err)
	}
	rows := strings.Split(strings.TrimSpace(listOut.String()), "\n")
	if len(rows) != 2 {
		t.Fatalf("monitor list output = %q, want header and one row", listOut.String())
	}
	fields := strings.Fields(rows[1])
	if len(fields) != 5 || fields[2] != "a-very-long-organization-na…" || fields[3] != "#7" {
		t.Fatalf("monitor list row = %q, want bounded repository and aligned PR column", rows[1])
	}

	var getOut bytes.Buffer
	if err := MonitorCommand([]string{"get", id}, cfg, t.TempDir(), strings.NewReader(""), &getOut, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(getOut.String(), "Repository: "+repo+"\n") {
		t.Fatalf("monitor get output = %q, want complete repository name", getOut.String())
	}
}

func TestMonitorListAlignsColumnsForUUIDAndTimestampIDs(t *testing.T) {
	state := t.TempDir()
	root, err := JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"20260518T120010-0123456789ab", "aecf5e83-3974-4357-9e5d-d34a64e58cee"}
	for _, id := range ids {
		monitor := &monitorJob{ID: id, Repo: "team/repo", PR: 7, Status: "running", CreatedAt: time.Date(2026, time.May, 18, 12, 0, 10, 0, time.UTC)}
		if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TargetPath: t.TempDir(), Status: "running", Monitor: monitor}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.CreateSession(id, monitorSessionID, "running"); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	if err := MonitorCommand([]string{"list"}, Config{StateDir: state}, t.TempDir(), nil, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("monitor list output = %q, want header and two rows", out.String())
	}
	const statusColumn = 37
	if strings.Index(lines[0], "STATUS") != statusColumn || strings.Index(lines[1], "running") != statusColumn || strings.Index(lines[2], "running") != statusColumn {
		t.Fatalf("status columns are misaligned: %q", out.String())
	}
	for _, id := range ids {
		found := false
		for _, line := range lines[1:] {
			if strings.HasPrefix(line, id) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("monitor list omitted full ID %q: %q", id, out.String())
		}
	}
}

func TestMonitorListOrdersByUpdatedAtDescending(t *testing.T) {
	state := t.TempDir()
	root, err := JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	createdOld := time.Date(2026, time.May, 18, 10, 0, 0, 0, time.UTC)
	createdNew := createdOld.Add(time.Hour)
	updatedOld := createdOld.Add(2 * time.Hour)
	updatedNew := createdOld.Add(3 * time.Hour)
	for _, tc := range []struct {
		id        string
		createdAt time.Time
		updatedAt time.Time
	}{
		{id: "older-created", createdAt: createdOld, updatedAt: updatedNew},
		{id: "newer-created", createdAt: createdNew, updatedAt: updatedOld},
	} {
		monitor := &monitorJob{ID: tc.id, Repo: tc.id, PR: 7, Status: "running", CreatedAt: tc.createdAt, UpdatedAt: tc.updatedAt}
		if err := store.CreateJob(JobRecord{ID: tc.id, Type: monitorJobType, TargetPath: t.TempDir(), Status: "running", CreatedAt: tc.createdAt, Monitor: monitor}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.UpdateJob(tc.id, func(job *JobRecord) error {
			job.Monitor.UpdatedAt = tc.updatedAt
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	var out bytes.Buffer
	if err := MonitorCommand([]string{"list"}, Config{StateDir: state}, t.TempDir(), nil, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("monitor list rows = %q, want header and two jobs", out.String())
	}
	if !strings.Contains(lines[1], "older-created") || !strings.Contains(lines[1], updatedNew.Format(time.RFC3339)) {
		t.Errorf("first monitor row = %q, want most recently updated older-created job", lines[1])
	}
	if !strings.Contains(lines[2], "newer-created") || !strings.Contains(lines[2], updatedOld.Format(time.RFC3339)) {
		t.Errorf("second monitor row = %q, want less recently updated newer-created job", lines[2])
	}
}

func TestMonitorListLimitTruncatesNewestFirstAndDefaultsToUnlimited(t *testing.T) {
	state := t.TempDir()
	cfg := Config{StateDir: state}
	root, err := JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewJobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	var empty bytes.Buffer
	if err := MonitorCommand([]string{"list"}, cfg, t.TempDir(), strings.NewReader(""), &empty, io.Discard); err != nil {
		t.Fatal(err)
	}
	if empty.String() != "No monitor jobs.\n" {
		t.Fatalf("empty list = %q", empty.String())
	}
	ids := []string{
		"20260518T120010-0123456789ab",
		"20260518T120011-1123456789ab",
		"20260518T120012-2123456789ab",
	}
	for index, id := range ids {
		monitor := &monitorJob{
			ID: id, Repo: fmt.Sprintf("team/repo-%d", index), PR: index + 7, Status: "running",
			CreatedAt: time.Date(2026, time.May, 18, 12, 0, index+10, 0, time.UTC),
		}
		if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TargetPath: t.TempDir(), Status: "running", Monitor: monitor}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.CreateSession(id, monitorSessionID, "running"); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range []struct {
		name  string
		args  []string
		count int
		order []string
	}{
		{name: "unlimited default", args: []string{"list"}, count: 3, order: []string{ids[2], ids[1], ids[0]}},
		{name: "limited newest-first", args: []string{"list", "--limit", "2"}, count: 2, order: []string{ids[2], ids[1]}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := MonitorCommand(tc.args, cfg, t.TempDir(), strings.NewReader(""), &out, io.Discard); err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(out.String()), "\n")
			if len(lines) != tc.count+1 {
				t.Fatalf("monitor list printed %d lines, want header plus %d rows: %q", len(lines), tc.count, out.String())
			}
			for i, id := range tc.order {
				if !strings.Contains(lines[i+1], id) {
					t.Errorf("row %d = %q, want newest-first ID %s", i+1, lines[i+1], id)
				}
			}
			if !strings.Contains(out.String(), "team/repo-2") || !strings.Contains(out.String(), "#9") {
				t.Errorf("monitor list omitted canonical repository or PR fields: %s", out.String())
			}
		})
	}
}

func TestMonitorListRejectsInvalidLimitOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "missing value", args: []string{"list", "--limit"}, want: "--limit requires a positive integer"},
		{name: "duplicate", args: []string{"list", "--limit", "1", "--limit", "2"}, want: "--limit may only be specified once"},
		{name: "zero", args: []string{"list", "--limit", "0"}, want: "--limit requires a positive integer"},
		{name: "negative", args: []string{"list", "--limit", "-1"}, want: "--limit requires a positive integer"},
		{name: "noninteger", args: []string{"list", "--limit", "1.5"}, want: "--limit requires a positive integer"},
		{name: "unknown option", args: []string{"list", "--other"}, want: "unknown monitor list argument"},
		{name: "unexpected positional argument", args: []string{"list", "1"}, want: "unknown monitor list argument"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := MonitorCommand(tc.args, Config{StateDir: t.TempDir()}, t.TempDir(), strings.NewReader(""), io.Discard, io.Discard)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("MonitorCommand(%v) error = %v, want containing %q", tc.args, err, tc.want)
			}
		})
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

func TestMonitorEventHistoryPersistsBoundedPhaseTransitions(t *testing.T) {
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
	monitor := &monitorJob{ID: id, Status: "running", Phase: "polling"}
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TargetPath: t.TempDir(), Status: "running", Monitor: monitor}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, monitorSessionID, "running"); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, id)
	for i := 0; i < monitorRecentEventLimit+3; i++ {
		monitor.Phase = fmt.Sprintf("phase-%d", i)
		if err := monitorEvent(dir, monitor, fmt.Sprintf("transition %d", i)); err != nil {
			t.Fatal(err)
		}
	}
	persisted, err := readMonitorJob(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(persisted.RecentEvents) != monitorRecentEventLimit {
		t.Fatalf("recent event count = %d, want bounded limit %d", len(persisted.RecentEvents), monitorRecentEventLimit)
	}
	first := persisted.RecentEvents[0]
	last := persisted.RecentEvents[len(persisted.RecentEvents)-1]
	if first.Message != "transition 3" || first.Phase != "phase-3" || last.Message != "transition 14" || last.Phase != "phase-14" {
		t.Fatalf("persisted recent phase transitions = first %+v, last %+v", first, last)
	}
}

func TestMonitorStatusExposesPhaseCheckFreshnessApprovalAndBoundedEvents(t *testing.T) {
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
	checkedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	monitor := &monitorJob{
		ID: id, Repo: "team/repo", PR: 7, HeadBranch: "feature", Status: "running",
		Phase: "agent_work", LatestCheckAt: checkedAt, LatestCheckResult: "2 passed, 1 failed, 0 pending",
		PendingSignature: "snapshot", Proposal: "Needs human judgment",
	}
	for i := 0; i < monitorRecentEventLimit+3; i++ {
		monitor.RecentEvents = append(monitor.RecentEvents, monitorTraceEvent{At: checkedAt.Add(time.Duration(i) * time.Second), Phase: "polling", Message: fmt.Sprintf("transition %d", i)})
	}
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TargetPath: t.TempDir(), Status: "running", Monitor: monitor}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, monitorSessionID, "running"); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := MonitorCommand([]string{"get", id}, Config{StateDir: state}, t.TempDir(), nil, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{"Phase: approval_pending", "Latest successful PR/check query:", checkedAt.Format(time.RFC3339), "2 passed, 1 failed, 0 pending", "Approval pending: Needs human judgment"} {
		if !strings.Contains(text, want) {
			t.Errorf("concise monitor status missing %q: %s", want, text)
		}
	}
	if strings.Contains(text, "Recent monitor events:") || strings.Contains(text, "transition ") {
		t.Fatalf("concise monitor status included the event trail: %s", text)
	}
	out.Reset()
	if err := MonitorCommand([]string{"get", id, "--details"}, Config{StateDir: state}, t.TempDir(), nil, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	text = out.String()
	for _, want := range []string{"Recent monitor events:", "transition 3", "transition 14", `"proposal": "Needs human judgment"`} {
		if !strings.Contains(text, want) {
			t.Errorf("detailed monitor status missing %q: %s", want, text)
		}
	}
}

func TestMonitorGetReturnsRecentEventWriterErrors(t *testing.T) {
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
	monitor := &monitorJob{ID: id, Status: "running", Phase: "polling", RecentEvents: []monitorTraceEvent{{Phase: "polling", Message: "query completed"}}}
	if err := store.CreateJob(JobRecord{ID: id, Type: monitorJobType, TargetPath: t.TempDir(), Status: "running", Monitor: monitor}); err != nil {
		t.Fatal(err)
	}
	writer := &failingMonitorWriter{failAfter: 4}
	if err := MonitorCommand([]string{"get", id, "--details"}, Config{StateDir: state}, t.TempDir(), nil, writer, io.Discard); err == nil {
		t.Fatal("monitor get --details swallowed recent-event output error")
	}
	if writer.writes != 5 {
		t.Fatalf("writer received %d writes before error; want failure while writing a recent event", writer.writes)
	}
}

type failingMonitorWriter struct {
	writes    int
	failAfter int
}

func (w *failingMonitorWriter) Write(data []byte) (int, error) {
	w.writes++
	if w.writes > w.failAfter {
		return 0, fmt.Errorf("output failed")
	}
	return len(data), nil
}

func TestMonitorDisplayPhasePrefersTerminalStatusToStalePhase(t *testing.T) {
	for _, tc := range []struct {
		status string
		phase  string
		want   string
	}{
		{status: "complete", phase: "stopping", want: "complete"},
		{status: "closed", phase: "polling", want: "closed"},
		{status: "stopped", phase: "agent_work", want: "stopped"},
		{status: "recoverable_failure", phase: "retrying_snapshot", want: "recoverable_failure"},
	} {
		job := &monitorJob{Status: tc.status, Phase: tc.phase}
		if got := monitorDisplayPhase(job); got != tc.want {
			t.Errorf("monitorDisplayPhase(%s, %s) = %q, want %q", tc.status, tc.phase, got, tc.want)
		}
	}
}

func TestMonitorCheckSummarySeparatesNeutralAndSkippedResults(t *testing.T) {
	raw := json.RawMessage(`[{"conclusion":"SUCCESS"},{"state":"FAILURE"},{"conclusion":"NEUTRAL"},{"conclusion":"SKIPPED"},{"state":"QUEUED"}]`)
	got := monitorCheckSummary(raw)
	want := "1 passed, 1 failed, 1 pending, 2 neutral/skipped"
	if got != want {
		t.Fatalf("monitorCheckSummary() = %q, want %q", got, want)
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
