package main

import (
	"bufio"
	"bytes"
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

	"github.com/leehosanganson/factory/internal/factory"
	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restserver"
)

func TestRESTClientCommandHelp(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"rest", "--help"}, strings.NewReader(""), &out, &bytes.Buffer{}); err != nil {
		t.Fatalf("rest help: %v", err)
	}
	for _, want := range []string{"factory rest", "submit", "get", "list", "watch", "cancel", "operations"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("REST client help missing %q: %s", want, out.String())
		}
	}
}

func TestJobJSONProcessOutput(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "factory")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build factory CLI: %v\n%s", err, output)
	}

	root := t.TempDir()
	state := filepath.Join(root, "state")
	target := filepath.Join(root, "private-host-path", "sample-repo")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_STATE_HOME", state)
	store, err := factory.NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	jobs := []factory.JobRecord{
		{ID: "json-implementation", Type: "implementation", Status: "complete", TaskDescription: "task contains PRIVATE_SECRET", TargetPath: target, Worktree: filepath.Join(root, "secret-worktree"), PublicationStatus: "published", PublicationSummary: "PRIVATE_SECRET"},
		{ID: "json-tidy", Type: "tidy", Status: "failed", TaskDescription: "other task", TargetPath: target},
	}
	for i, job := range jobs {
		job.CreatedAt = time.Date(2026, time.January, i+1, 12, 0, 0, 0, time.FixedZone("test-offset", 2*60*60))
		job.UpdatedAt = job.CreatedAt.Add(time.Hour)
		if err := store.CreateJob(job); err != nil {
			t.Fatal(err)
		}
		if _, err := store.CreateSession(job.ID, "workflow", job.Status); err != nil {
			t.Fatal(err)
		}
		if err := store.AppendSessionLog(job.ID, "workflow", []byte("PRIVATE_SECRET raw worker log /private/host/path\n")); err != nil {
			t.Fatal(err)
		}
	}

	run := func(args ...string) (string, string, error) {
		t.Helper()
		command := exec.Command(binary, args...)
		command.Dir = target
		var stdout, stderr bytes.Buffer
		command.Stdout, command.Stderr = &stdout, &stderr
		err := command.Run()
		return stdout.String(), stderr.String(), err
	}
	decode := func(output string, destination any) {
		t.Helper()
		decoder := json.NewDecoder(strings.NewReader(output))
		if err := decoder.Decode(destination); err != nil {
			t.Fatalf("decode JSON stdout %q: %v", output, err)
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			t.Fatalf("stdout contains data after one JSON value: %q (extra=%v err=%v)", output, extra, err)
		}
	}

	stdout, stderr, err := run("job", "list", "--json", "--scan-limit", "2", "--status", "complete", "--type", "implementation", "--limit", "1")
	if err != nil || stderr != "" {
		t.Fatalf("JSON filtered list: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	var listing struct {
		SchemaVersion int `json:"schema_version"`
		Jobs          []struct {
			ID     string `json:"id"`
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"jobs"`
		Scan struct {
			Scanned int  `json:"scanned"`
			HasMore bool `json:"has_more"`
		} `json:"scan"`
	}
	decode(stdout, &listing)
	if listing.SchemaVersion != 1 || len(listing.Jobs) != 1 || listing.Jobs[0].ID != "json-implementation" || listing.Jobs[0].Type != "implementation" || listing.Jobs[0].Status != "complete" || listing.Scan.Scanned != 2 || listing.Scan.HasMore {
		t.Fatalf("filtered list payload = %+v", listing)
	}
	for _, forbidden := range []string{"PRIVATE_SECRET", "private-host-path", "secret-worktree", "/private/host/path"} {
		if strings.Contains(stdout, forbidden) {
			t.Errorf("JSON list leaked %q: %s", forbidden, stdout)
		}
	}
	stdout, stderr, err = run("job", "list", "--json", "--scan-limit", "1")
	if err != nil || stderr != "" {
		t.Fatalf("bounded JSON list: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	decode(stdout, &listing)
	if listing.Scan.Scanned != 1 || !listing.Scan.HasMore {
		t.Fatalf("bounded list scan = %+v, want one scanned and more available", listing.Scan)
	}

	for _, args := range [][]string{{"job", "get", "json-implementation", "--json"}, {"job", "get", "json-implementation", "--json", "--details"}} {
		stdout, stderr, err = run(args...)
		if err != nil || stderr != "" {
			t.Fatalf("JSON get %v: err=%v stderr=%q stdout=%q", args, err, stderr, stdout)
		}
		var payload struct {
			SchemaVersion int `json:"schema_version"`
			Job           struct {
				ID                string         `json:"id"`
				Type              string         `json:"type"`
				Status            string         `json:"status"`
				CreatedAt         string         `json:"created_at"`
				UpdatedAt         string         `json:"updated_at"`
				PublicationStatus string         `json:"publication_status"`
				Details           map[string]any `json:"details"`
			} `json:"job"`
		}
		decode(stdout, &payload)
		if payload.SchemaVersion != 1 || payload.Job.ID != "json-implementation" || payload.Job.Type != "implementation" || payload.Job.Status != "complete" || payload.Job.CreatedAt == "" || payload.Job.UpdatedAt == "" || payload.Job.PublicationStatus != "published" {
			t.Fatalf("get payload = %+v", payload)
		}
		if !strings.HasSuffix(payload.Job.CreatedAt, "Z") || !strings.HasSuffix(payload.Job.UpdatedAt, "Z") {
			t.Errorf("timestamps are not UTC RFC3339: created=%q updated=%q", payload.Job.CreatedAt, payload.Job.UpdatedAt)
		}
		if (len(args) == 5) != (payload.Job.Details != nil) {
			t.Fatalf("details presence for %v = %#v", args, payload.Job.Details)
		}
		if len(args) == 5 {
			sessions, ok := payload.Job.Details["sessions"].([]any)
			if !ok || len(sessions) != 1 {
				t.Fatalf("details sessions = %#v", payload.Job.Details["sessions"])
			}
		}
		encoded, _ := json.Marshal(payload)
		for _, forbidden := range []string{"PRIVATE_SECRET", "raw worker log", "private-host-path", "secret-worktree", "/private/host/path"} {
			if strings.Contains(string(encoded), forbidden) {
				t.Errorf("JSON get leaked %q: %s", forbidden, encoded)
			}
		}
	}

	stdout, stderr, err = run("job", "list", "--json", "--status", "cancelled")
	if err != nil || stderr != "" {
		t.Fatalf("empty JSON list: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	decode(stdout, &listing)
	if listing.Jobs == nil || len(listing.Jobs) != 0 {
		t.Fatalf("empty list jobs = %#v, want an empty array", listing.Jobs)
	}
	stdout, stderr, err = run("job", "list", "--json", "--details", "--status", "complete", "--type", "implementation")
	if err != nil || stderr != "" {
		t.Fatalf("detailed JSON list: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	var detailedListing struct {
		Jobs []struct {
			Details map[string]any `json:"details"`
		} `json:"jobs"`
	}
	decode(stdout, &detailedListing)
	if len(detailedListing.Jobs) != 1 || detailedListing.Jobs[0].Details == nil {
		t.Fatalf("detailed list payload = %+v", detailedListing)
	}
	if sessions, ok := detailedListing.Jobs[0].Details["sessions"].([]any); !ok || len(sessions) != 1 {
		t.Fatalf("detailed list sessions = %#v", detailedListing.Jobs[0].Details["sessions"])
	}

	for _, args := range [][]string{{"job", "get", "missing-json-id", "--json"}, {"job", "get", "../unsafe", "--json"}} {
		stdout, stderr, err = run(args...)
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 || stdout != "" || stderr == "" {
			t.Errorf("failed JSON get %v: err=%v stdout=%q stderr=%q; want exit 1, empty stdout, stderr diagnostic", args, err, stdout, stderr)
		}
	}

	stdout, stderr, err = run("job", "list")
	if err != nil || stderr != "" || strings.Contains(stdout, "schema_version") || !strings.Contains(stdout, "json-implementation") {
		t.Fatalf("default text list changed: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	stdout, stderr, err = run("job", "get", "json-implementation")
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	if err != nil || stderr != "" || len(lines) != 2 || !strings.Contains(lines[0], "PUBLICATION") || !strings.Contains(lines[1], "published") || strings.Contains(stdout, "schema_version") {
		t.Fatalf("default text get changed: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	stdout, stderr, err = run("job", "get", "json-implementation", "--details")
	if err != nil || stderr != "" || !strings.Contains(stdout, "Description: task contains PRIVATE_SECRET") || strings.Contains(stdout, "schema_version") {
		t.Fatalf("default detailed text get changed: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	for _, args := range [][]string{{"job", "list", "--details"}, {"job", "list", "--json", "--limit", "0"}} {
		stdout, stderr, err = run(args...)
		if err == nil || stdout != "" || stderr == "" {
			t.Errorf("invalid JSON command %v: err=%v stdout=%q stderr=%q; want nonzero, empty stdout, stderr diagnostic", args, err, stdout, stderr)
		}
	}

	corruptDir := filepath.Join(store.Root(), "corrupt-json")
	if err := os.Mkdir(corruptDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(corruptDir, "job.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err = run("job", "list", "--json")
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 || stdout != "" || stderr == "" {
		t.Errorf("failed JSON list: err=%v stdout=%q stderr=%q; want exit 1, empty stdout, stderr diagnostic", err, stdout, stderr)
	}
}

func TestPrivateJobWorkerInvocationRequiresStoreRoot(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := run([]string{"__job-worker", "job-id"}, strings.NewReader(""), &out, &errOut); err == nil || !strings.Contains(err.Error(), "invalid private worker invocation") {
		t.Fatalf("worker invocation without store root error=%v", err)
	}
	if err := run([]string{"__job-worker", "job-id", "relative-root"}, strings.NewReader(""), &out, &errOut); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("worker invocation with invalid root error=%v", err)
	}
}

func TestDoctorProcessChecksSetupWithoutSideEffects(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "factory")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build factory CLI: %v\n%s", err, output)
	}

	for _, tc := range []struct {
		name          string
		config        string
		tools         []string
		want          []string
		omit          []string
		wantExitError bool
	}{
		{
			name:          "invalid config is sanitized",
			config:        `{"command":"secret-agent-path","args":["{system_prompt}","{task}"],"state_dir":"/secret/state","agent_timeout":"not-a-duration"}`,
			want:          []string{"config: invalid"},
			omit:          []string{"secret-agent-path", "/secret/state", "not-a-duration"},
			wantExitError: true,
		},
		{
			name:          "missing agent executable",
			config:        `{"command":"factory-doctor-missing-agent","args":["{system_prompt}","{task}"],"auto_publish":false}`,
			tools:         []string{"git"},
			want:          []string{"config: valid", "agent executable: missing", "git: available", "gh: skipped"},
			wantExitError: true,
		},
		{
			name:          "gh is required by default",
			tools:         []string{"pi", "git"},
			want:          []string{"config: valid", "agent executable: available", "git: available", "gh: missing"},
			wantExitError: true,
		},
		{
			name:   "doctor reports configured detached cap without config details",
			config: `{"command":"pi","args":["{system_prompt}","{task}","secret-argument"],"auto_publish":false,"detached_job_max_concurrency":2}`,
			tools:  []string{"pi", "git"},
			want:   []string{"detached job concurrency: 2"},
			omit:   []string{"secret-argument"},
		},
		{
			name:  "healthy default config reports no checks as advisory",
			tools: []string{"pi", "git", "gh"},
			want:  []string{"config: valid", "agent executable: available", "git: available", "gh: available", "detached job concurrency: unlimited (not configured)", "pipeline checks: 0 configured", "advisory", "confidence"},
			omit:  []string{"skipped"},
		},
		{
			name:   "configured checks are counted without exposing or running them",
			config: `{"command":"pi","args":["{system_prompt}","{task}"],"auto_publish":true,"pipeline_checks":[["private-check-path","private-check-argument"],["private-check-path","private-check-token"]]}`,
			tools:  []string{"pi", "git", "gh", "private-check-path"},
			want:   []string{"config: valid", "pipeline checks: 2 configured"},
			omit:   []string{"private-check-argument", "private-check-path", "private-check-token", "advisory"},
		},
		{
			name:   "gh and pipeline advisory are skipped when publication is disabled",
			config: `{"command":"pi","args":["{system_prompt}","{task}"],"auto_publish":false}`,
			tools:  []string{"pi", "git"},
			want:   []string{"config: valid", "agent executable: available", "git: available", "gh: skipped (auto_publish is false)", "pipeline checks: 0 configured"},
			omit:   []string{"advisory", "confidence", "publication risk"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			configHome := filepath.Join(root, "config")
			configDir := filepath.Join(configHome, "factory")
			fakeBin := filepath.Join(root, "bin")
			stateHome := filepath.Join(root, "state")
			target := filepath.Join(root, "target")
			for _, dir := range []string{configDir, fakeBin, target} {
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if tc.config != "" {
				if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(tc.config), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			targetFile := filepath.Join(target, "keep.txt")
			if err := os.WriteFile(targetFile, []byte("unchanged"), 0o600); err != nil {
				t.Fatal(err)
			}
			for _, tool := range tc.tools {
				if err := os.WriteFile(filepath.Join(fakeBin, tool), []byte("#!/bin/sh\nprintf invoked > \"$FACTORY_DOCTOR_MARKER\"\n"), 0o700); err != nil {
					t.Fatal(err)
				}
			}

			command := exec.Command(binary, "doctor")
			command.Dir = target
			command.Env = []string{
				"XDG_CONFIG_HOME=" + configHome,
				"XDG_STATE_HOME=" + stateHome,
				"PATH=" + fakeBin,
				"FACTORY_DOCTOR_MARKER=" + filepath.Join(root, "tool-invoked"),
			}
			output, err := command.CombinedOutput()
			if (err != nil) != tc.wantExitError {
				t.Fatalf("factory doctor error = %v, want exit-error %v; output: %s", err, tc.wantExitError, output)
			}
			for _, want := range tc.want {
				if !strings.Contains(string(output), want) {
					t.Errorf("factory doctor output missing %q: %s", want, output)
				}
			}
			for _, omitted := range tc.omit {
				if strings.Contains(string(output), omitted) {
					t.Errorf("factory doctor output leaked %q: %s", omitted, output)
				}
			}
			if _, err := os.Stat(filepath.Join(root, "tool-invoked")); !os.IsNotExist(err) {
				t.Errorf("doctor invoked a configured tool: stat error=%v", err)
			}
			if _, err := os.Stat(stateHome); !os.IsNotExist(err) {
				t.Errorf("doctor created state: stat error=%v", err)
			}
			if data, err := os.ReadFile(targetFile); err != nil || string(data) != "unchanged" {
				t.Errorf("doctor changed target file: data=%q err=%v", data, err)
			}
			if entries, err := os.ReadDir(target); err != nil || len(entries) != 1 || entries[0].Name() != "keep.txt" {
				t.Errorf("doctor changed target directory entries: entries=%v err=%v", entries, err)
			}
		})
	}
}

func TestVersionCommandReportsLinkedVersion(t *testing.T) {
	originalVersion := version
	t.Cleanup(func() { version = originalVersion })

	for _, tc := range []struct {
		name    string
		version string
	}{
		{name: "development default", version: "dev"},
		{name: "release linker value", version: "v1.2.3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			version = tc.version
			var out, errOut bytes.Buffer
			if err := run([]string{"version"}, strings.NewReader(""), &out, &errOut); err != nil {
				t.Fatal(err)
			}
			if out.String() != tc.version+"\n" {
				t.Fatalf("version output = %q, want %q", out.String(), tc.version+"\n")
			}
			if errOut.Len() != 0 {
				t.Fatalf("version wrote unexpected stderr: %q", errOut.String())
			}
		})
	}
}

func TestRootHelpAliasesAreConciseAndConsistent(t *testing.T) {
	var out, errOut bytes.Buffer
	var rootHelp string
	for _, args := range [][]string{{"-h"}, {"--help"}, {"help"}} {
		out.Reset()
		if err := run(args, strings.NewReader(""), &out, &errOut); err != nil {
			t.Fatal(err)
		}
		text := strings.Join(strings.Fields(strings.ToLower(out.String())), " ")
		for _, want := range []string{"usage: factory", "software factory workflows", "factory implement", "factory tidy", "factory monitor", "factory work", "factory job", "factory run", "factory server", "--gate", "factory version"} {
			if !strings.Contains(text, want) {
				t.Errorf("root %v help missing %q: %s", args, want, out.String())
			}
		}
		for _, omitted := range []string{"full command overview", "origin/<branch>", "dirty safe mode", "30-minute active", "optional placeholders", "approval requires exact", "isolated worktree", "factory job start implementation", "execution and safeguards", "configuration and limits", "factory pipeline", "factory clean", "alias: pipeline", "alias: clean"} {
			if strings.Contains(text, omitted) {
				t.Errorf("root %v help includes full-overview detail %q: %s", args, omitted, out.String())
			}
		}
		if rootHelp != "" && text != rootHelp {
			t.Errorf("root help aliases differ:\n%q\n%q", rootHelp, text)
		}
		rootHelp = text
	}
	out.Reset()
	printRootHelpWithOptions(&out, 80, false, false)
	if !strings.Contains(out.String(), "Usage:") || strings.Contains(out.String(), "full command overview") {
		t.Fatalf("concise root help rendering is incorrect: %s", out.String())
	}
	out.Reset()
	if err := run([]string{"implement", "help"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Implement workflow") || strings.Contains(out.String(), "Execution and safeguards") {
		t.Fatalf("workflow help should remain concise: %s", out.String())
	}
	out.Reset()
	if err := run([]string{"tidy", "help"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Tidy workflow") {
		t.Fatalf("tidy help missing canonical workflow: %s", out.String())
	}
	out.Reset()
	if err := run([]string{"work", "help"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Issue work requests", "factory work submit", "factory work list", "factory work get", "factory work issue", "factory work refresh", "factory work history", "factory work watch", "factory work respond", "factory work directions", "autonomous worker", "do not change the queue", "does not resume work"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("work help missing %q: %s", want, out.String())
		}
	}
	out.Reset()
	if err := run([]string{"job", "help"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "factory job start implementation") || !strings.Contains(out.String(), "factory job start tidy") || strings.Contains(out.String(), "full command overview") {
		t.Fatalf("command-specific help should remain concise: %s", out.String())
	}
	if err := run([]string{"job", "start", "clean", "unsupported"}, strings.NewReader(""), &out, &errOut); err == nil || !strings.Contains(err.Error(), "unsupported job type") {
		t.Fatalf("clean job type should remain unsupported: err=%v", err)
	}
	err := run([]string{"monitor"}, strings.NewReader(""), &out, &errOut)
	if err == nil || !strings.Contains(err.Error(), "usage: factory monitor <description>") || !strings.Contains(err.Error(), "reset <id>") {
		t.Fatalf("monitor should show canonical usage including reset: err=%v", err)
	}
	if err := run([]string{"babysit"}, strings.NewReader(""), &out, &errOut); err == nil || !strings.Contains(err.Error(), `unknown command "babysit"`) {
		t.Fatalf("removed public babysit command should be rejected: err=%v", err)
	}
}

func TestCommandHelpRoutesBeforeConfigAndWorkflowDispatch(t *testing.T) {
	configHome := t.TempDir()
	factoryConfig := filepath.Join(configHome, "factory")
	if err := os.MkdirAll(factoryConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(factoryConfig, "config.json"), []byte("not valid config"), 0o600); err != nil {
		t.Fatal(err)
	}
	stateHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("XDG_STATE_HOME", stateHome)

	for _, tc := range []struct {
		name string
		args []string
		want []string
		omit []string
	}{
		{name: "implement", args: []string{"implement", "--help"}, want: []string{"Implement workflow", "factory implement"}, omit: []string{"factory pipeline", "Examples:", "Ctrl-C", "interactive terminal"}},
		{name: "server help", args: []string{"server", "--help"}, want: []string{"REST API server", "factory server --config <absolute-path>", "factory server doctor --config <absolute-path>", "factory server backup --config <absolute-path> --destination <absolute-path>", "factory server bundle create --config <absolute-path> --destination <absolute-path>", "factory server bundle verify --source <absolute-path>", "factory server bundle inspect --source <absolute-path>", "factory server bundle restore --source <absolute-path> --destination <absolute-path>", "server-only JSON config", "provider reachability is not tested"}, omit: []string{"api-key-value"}},
		{name: "server doctor help", args: []string{"server", "doctor", "--help"}, want: []string{"REST server preflight", "factory server doctor --config <absolute-path>", "Local-only, read-only", "Provider reachability is not tested", "no database is opened or created"}, omit: []string{"api-key-value"}},
		{name: "tidy focused", args: []string{"tidy", "--help"}, want: []string{"Tidy workflow", "factory tidy"}, omit: []string{"factory clean", "Detached jobs", "factory job", "Monitor management", "Dirty safe mode", "make clean"}},
		{name: "job overview", args: []string{"job", "--help"}, want: []string{"Detached jobs", "Configuration: worktree_parent", "factory job list [--json] [--details]", "queued, running, complete", "Filters affect listing only", "factory job list --scan-limit 100 --status failed", "Omitted jobs remain addressable by ID", "factory job list without --scan-limit reconciles all records", "schema version 1", "read-only inspection", "{repo}"}, omit: []string{"factory run", "Monitor management"}},
		{name: "job subcommand", args: []string{"job", "start", "--help"}, want: []string{"Detached jobs", "factory job start implementation", "factory job start tidy", "factory job start monitor"}, omit: []string{"factory run", "Monitor management", "Example:"}},
		{name: "job get canonical", args: []string{"job", "get", "--help"}, want: []string{"factory job get", "--json", "--details", "safe session metadata"}, omit: []string{"factory job start", "factory run", "factory job show"}},
		{name: "job watch help", args: []string{"job", "watch", "--help"}, want: []string{"factory job watch <id>...", "Refresh selected job status and latest activity", "monitor phase", "check freshness", "recent events"}, omit: []string{"factory job logs", "factory job stop"}},
		{name: "run subcommand", args: []string{"run", "events", "--help"}, want: []string{"Gated runs", "factory run events"}, omit: []string{"factory job", "Monitor management", "Example:"}},
		{name: "run list limit", args: []string{"run", "list", "--help"}, want: []string{"factory run list [--limit <n>]", "newest positive number"}, omit: []string{"factory run get"}},
		{name: "run get canonical", args: []string{"run", "get", "--help"}, want: []string{"factory run get", "--details", "metadata"}, omit: []string{"factory run show"}},
		{name: "monitor canonical", args: []string{"monitor", "get", "--help"}, want: []string{"factory monitor get", "--details", "proposals", "latest PR check", "recent events"}, omit: []string{"factory monitor describe"}},
		{name: "monitor list", args: []string{"monitor", "list", "--help"}, want: []string{"factory monitor list [--limit <n>]", "newest positive number"}, omit: []string{"factory monitor get"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			if err := run(tc.args, strings.NewReader(""), &out, &errOut); err != nil {
				t.Fatalf("help with invalid config: %v", err)
			}
			text := strings.Join(strings.Fields(out.String()), " ")
			if errOut.Len() != 0 {
				t.Fatalf("help wrote stderr: %q", errOut.String())
			}
			for _, want := range tc.want {
				if !strings.Contains(text, want) {
					t.Errorf("help missing %q: %s", want, text)
				}
			}
			for _, omitted := range tc.omit {
				if strings.Contains(text, omitted) {
					t.Errorf("focused help unexpectedly includes %q: %s", omitted, text)
				}
			}
		})
	}
	if err := os.Remove(filepath.Join(factoryConfig, "config.json")); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"tidy", "--help"}, {"job", "list", "--help"}, {"run", "list", "--help"}, {"monitor", "list", "--help"}} {
		var out bytes.Buffer
		if err := run(args, strings.NewReader(""), &out, io.Discard); err != nil || out.Len() == 0 {
			t.Errorf("help %v with no config: output=%q err=%v", args, out.String(), err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(stateHome, "factory"))
	if err == nil && len(entries) != 0 {
		t.Fatalf("help started work or created state: %v", entries)
	}
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("inspect state after help: %v", err)
	}
}

func TestServerBundleCommandsRequireExactSyntax(t *testing.T) {
	for _, args := range [][]string{
		{"server", "bundle"},
		{"server", "bundle", "create"},
		{"server", "bundle", "create", "--config", "relative.json", "--destination", "/tmp/bundle.tar.gz"},
		{"server", "bundle", "create", "--config", "/tmp/server.json", "--destination", "/tmp/bundle.tar.gz", "extra"},
		{"server", "bundle", "verify", "--source", "relative.tar.gz"},
		{"server", "bundle", "verify", "--source", "/tmp/bundle.tar.gz", "extra"},
		{"server", "bundle", "restore", "--source", "/tmp/bundle.tar.gz", "--destination", "relative"},
		{"server", "bundle", "restore", "--source", "/tmp/bundle.tar.gz", "--destination", "/tmp/destination", "extra"},
		{"server", "bundle", "create", "--config", "/tmp/server.json", "--destination", "/tmp/bundle.tar.gz", "extra"},
	} {
		var out, errOut bytes.Buffer
		err := run(args, strings.NewReader(""), &out, &errOut)
		if err == nil || !strings.Contains(err.Error(), "usage: factory server bundle") {
			t.Errorf("run(%v) error=%v, want strict bundle syntax error", args, err)
		}
	}
}

func TestServerBackupCommandRequiresExactConfigAndDestinationSyntax(t *testing.T) {
	for _, args := range [][]string{
		{"server", "backup"},
		{"server", "backup", "--config"},
		{"server", "backup", "--config", "relative.json", "--destination", "/tmp/backup.db"},
		{"server", "backup", "--config", "/tmp/server.json", "--destination", "relative.db"},
		{"server", "backup", "--destination", "/tmp/backup.db", "--config", "/tmp/server.json"},
		{"server", "backup", "--config", "/tmp/server.json", "--destination", "/tmp/backup.db", "extra"},
	} {
		var out, errOut bytes.Buffer
		err := run(args, strings.NewReader(""), &out, &errOut)
		if err == nil || !strings.Contains(err.Error(), "usage: factory server backup --config <absolute-path> --destination <absolute-path>") {
			t.Errorf("run(%v) error=%v, want strict syntax error", args, err)
		}
	}
}

func TestServerBackupCommandCreatesSQLiteBackupWithoutLaunchingServer(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	repository := filepath.Join(dir, "repo")
	if err := os.Mkdir(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"config", "user.email", "test@example.com"}, {"config", "user.name", "Test"}} {
		command := exec.Command("git", append([]string{"-C", repository}, args...)...)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	config := restserver.DefaultConfig()
	config.Harness = restserver.HarnessConfig{Executable: "pi", Args: []string{"{task}", "{system_prompt}"}}
	config.Repositories = map[string]string{"trusted": repository}
	config.APIKeyFile = filepath.Join(dir, "api-key")
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(dir, "jobs.db")}
	configPath := filepath.Join(dir, "server.json")
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config.Persistence.Path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := restjobs.OpenSQLiteStore(config.Persistence.Path, restjobs.Config{QueueCapacity: config.Limits.QueueCapacity, MaxConcurrentJobs: config.Limits.Workers, MaxRecords: config.Limits.MaxRecords, MaxEventsPerJob: config.Limits.MaxEventsPerJob, MaxTaskBytes: config.Limits.TaskBytes, RegistryBytes: config.Limits.RegistryBytes})
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Admit("queued", restjobs.Request{Repository: "trusted", Task: "must remain queued"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		_ = store.CloseStore()
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(dir, "backup.db")
	var out, errOut bytes.Buffer
	args := []string{"server", "backup", "--config", configPath, "--destination", destination}
	if err := run(args, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatalf("run(%v): %v; stderr=%s", args, err, errOut.String())
	}
	restored, err := restjobs.OpenSQLiteStore(destination, restjobs.Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 4})
	if err != nil {
		t.Fatalf("open created backup: %v", err)
	}
	defer restored.CloseStore()
	got, err := restored.Get(job.ID)
	if err != nil || got.Status != restjobs.StatusRunning {
		t.Fatalf("backup changed running job state: (%+v, %v); want running state preserved", got, err)
	}
}

func TestServerDoctorProcessIsLocalOnlyAndReadOnly(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "factory")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build factory CLI: %v\n%s", err, output)
	}
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	repository := filepath.Join(root, "repo")
	if err := os.Mkdir(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	repository, err := filepath.EvalSymlinks(repository)
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"config", "user.email", "test@example.com"}, {"config", "user.name", "Test"}} {
		command := exec.Command("git", append([]string{"-C", repository}, args...)...)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	apiKey := filepath.Join(root, "api-key")
	providerToken := filepath.Join(root, "provider-token")
	for _, path := range []string{apiKey, providerToken} {
		if err := os.WriteFile(path, []byte("preflight-secret"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	harness := filepath.Join(root, "harness")
	if err := os.WriteFile(harness, []byte("do not execute"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := restserver.DefaultConfig()
	config.Repositories = map[string]string{"private-alias": repository}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{system_prompt}", "{task}"}}
	config.APIKeyFile = apiKey
	config.Provider = restserver.ProviderConfig{Backend: "github", TokenFile: providerToken, BaseBranch: "main", Repositories: map[string]string{"private-alias": "acme/private-repo"}}
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(root, "sqlite", "jobs.db")}
	if err := os.Mkdir(filepath.Dir(config.Persistence.Path), 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "server.json")
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(repository, "sentinel")
	if err := os.WriteFile(sentinel, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name        string
		configPath  string
		want        []string
		omit        []string
		wantFailure bool
	}{
		{name: "valid", configPath: configPath, want: []string{"config: valid", "local prerequisites: available", "provider reachability: not tested", "database, listener, and workflow: not started"}, omit: []string{root, "preflight-secret", "private-repo"}},
		{name: "missing config", configPath: filepath.Join(root, "missing-secret-config-path.json"), want: []string{"config: invalid"}, omit: []string{root, "missing-secret-config-path"}, wantFailure: true},
		{name: "malformed config", configPath: filepath.Join(root, "malformed.json"), want: []string{"config: invalid"}, omit: []string{root}, wantFailure: true},
		{name: "unreadable config target", configPath: filepath.Join(root, "config-directory"), want: []string{"config: invalid"}, omit: []string{root}, wantFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "malformed config" {
				if err := os.WriteFile(tc.configPath, []byte(`{"api_key_file":"secret-config-value"`), 0o600); err != nil {
					t.Fatal(err)
				}
				tc.omit = append(tc.omit, "secret-config-value")
			}
			if tc.name == "unreadable config target" {
				if err := os.Mkdir(tc.configPath, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			command := exec.Command(binary, "server", "doctor", "--config", tc.configPath)
			command.Env = append(os.Environ(), "XDG_CONFIG_HOME="+filepath.Join(root, "xdg-config"), "XDG_STATE_HOME="+filepath.Join(root, "xdg-state"))
			output, err := command.CombinedOutput()
			if (err != nil) != tc.wantFailure {
				t.Fatalf("doctor error = %v; output=%s", err, output)
			}
			for _, want := range tc.want {
				if !strings.Contains(string(output), want) {
					t.Errorf("output missing %q: %s", want, output)
				}
			}
			for _, omitted := range tc.omit {
				if strings.Contains(string(output), omitted) {
					t.Errorf("output leaked %q: %s", omitted, output)
				}
			}
		})
	}
	missingConfig := config
	missingConfig.APIKeyFile = filepath.Join(root, "missing-api-key-secret-path")
	missingConfig.Provider.TokenFile = filepath.Join(root, "missing-provider-token-secret-path")
	missingConfig.Persistence.Path = filepath.Join(root, "missing-sqlite-parent-secret-path", "jobs.db")
	missingConfigPath := filepath.Join(root, "missing-targets.json")
	missingData, err := json.Marshal(missingConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(missingConfigPath, missingData, 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "server", "doctor", "--config", missingConfigPath)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("preflight with missing local targets succeeded: %s", output)
	}
	for _, want := range []string{"api_key_file: unavailable or invalid", "provider.token_file: unavailable or invalid", "persistence.path: unavailable or unsafe", "provider reachability: not tested"} {
		if !strings.Contains(string(output), want) {
			t.Errorf("missing-target output missing %q: %s", want, output)
		}
	}
	for _, secretPath := range []string{missingConfig.APIKeyFile, missingConfig.Provider.TokenFile, filepath.Dir(missingConfig.Persistence.Path)} {
		if strings.Contains(string(output), secretPath) {
			t.Errorf("preflight output exposed configured path %q: %s", secretPath, output)
		}
	}
	if _, err := os.Stat(missingConfig.Persistence.Path); !os.IsNotExist(err) {
		t.Fatalf("preflight created SQLite file: stat error=%v", err)
	}
	if entries, err := os.ReadDir(filepath.Dir(config.Persistence.Path)); err != nil || len(entries) != 0 {
		t.Fatalf("preflight changed configured SQLite directory: entries=%v err=%v", entries, err)
	}
	if _, err := os.Stat(filepath.Dir(missingConfig.Persistence.Path)); !os.IsNotExist(err) {
		t.Fatalf("preflight created SQLite parent: stat error=%v", err)
	}
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "unchanged" {
		t.Fatalf("preflight changed repository sentinel: data=%q err=%v", data, err)
	}
	if err := os.Chmod(apiKey, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(providerToken, 0o644); err != nil {
		t.Fatal(err)
	}
	insecureConfig := filepath.Join(root, "insecure-targets.json")
	if err := os.WriteFile(insecureConfig, data, 0o600); err != nil {
		t.Fatal(err)
	}
	command = exec.Command(binary, "server", "doctor", "--config", insecureConfig)
	output, err = command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "api_key_file: unavailable or invalid") || !strings.Contains(string(output), "provider.token_file: unavailable or invalid") {
		t.Fatalf("preflight did not reject broadly readable secrets: err=%v output=%s", err, output)
	}
	for _, path := range []string{filepath.Join(root, "xdg-config"), filepath.Join(root, "xdg-state"), filepath.Join(root, "tool-invoked")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("preflight created or executed into %q: stat error=%v", path, err)
		}
	}
}

func TestServerCommandRequiresExactAbsoluteConfigSyntax(t *testing.T) {
	for _, args := range [][]string{
		{"server"},
		{"server", "--config"},
		{"server", "--config", "relative.json"},
		{"server", "relative.json", "--config"},
		{"server", "--config=/tmp/server.json"},
		{"server", "--config", "/tmp/server.json", "extra"},
	} {
		var out, errOut bytes.Buffer
		err := run(args, strings.NewReader(""), &out, &errOut)
		if err == nil || !strings.Contains(err.Error(), "usage: factory server --config <absolute-path>") {
			t.Errorf("run(%v) error=%v, want strict syntax error", args, err)
		}
	}
}

func TestRemovedAliasesFailBeforeHelpOrCommandDispatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "pipeline", args: []string{"pipeline", "do work"}, want: `unknown command "pipeline"`},
		{name: "pipeline help flag", args: []string{"pipeline", "--help"}, want: `unknown command "pipeline"`},
		{name: "pipeline help word", args: []string{"pipeline", "help"}, want: `unknown command "pipeline"`},
		{name: "clean", args: []string{"clean"}, want: `unknown command "clean"`},
		{name: "clean help flag", args: []string{"clean", "--help"}, want: `unknown command "clean"`},
		{name: "clean help word", args: []string{"clean", "help"}, want: `unknown command "clean"`},
		{name: "job show", args: []string{"job", "show", "job-id"}, want: `unknown job subcommand "show"`},
		{name: "job show help flag", args: []string{"job", "show", "--help"}, want: `unknown job subcommand "show"`},
		{name: "job show help word", args: []string{"job", "show", "help"}, want: `unknown job subcommand "show"`},
		{name: "run show", args: []string{"run", "show", "run-id"}, want: `unknown run subcommand "show"`},
		{name: "run show help flag", args: []string{"run", "show", "--help"}, want: `unknown run subcommand "show"`},
		{name: "run show help word", args: []string{"run", "show", "help"}, want: `unknown run subcommand "show"`},
		{name: "monitor describe", args: []string{"monitor", "describe", "job-id"}, want: `unknown monitor subcommand "describe"`},
		{name: "monitor describe help flag", args: []string{"monitor", "describe", "--help"}, want: `unknown monitor subcommand "describe"`},
		{name: "monitor describe help word", args: []string{"monitor", "describe", "help"}, want: `unknown monitor subcommand "describe"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			err := run(tc.args, strings.NewReader(""), &out, &errOut)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run(%v) error = %v, want %q", tc.args, err, tc.want)
			}
			if out.Len() != 0 {
				t.Fatalf("rejected alias printed help or output: %q", out.String())
			}
		})
	}
}

func TestCanonicalManagementCommandsStillRoute(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	for _, tc := range []struct {
		args []string
		want string
	}{
		{args: []string{"job", "list"}, want: "No jobs."},
		{args: []string{"run", "list"}, want: "No gated runs."},
		{args: []string{"monitor", "list"}, want: "No monitor jobs."},
	} {
		var out, errOut bytes.Buffer
		if err := run(tc.args, strings.NewReader(""), &out, &errOut); err != nil {
			t.Errorf("run(%v): %v", tc.args, err)
			continue
		}
		if !strings.Contains(out.String(), tc.want) {
			t.Errorf("run(%v) output = %q, want %q", tc.args, out.String(), tc.want)
		}
	}
}

func TestRootHelpFormattingWrapsAtRequestedWidth(t *testing.T) {
	var out bytes.Buffer
	for _, width := range []int{48, 24} {
		out.Reset()
		printRootHelpWithOptions(&out, width, false, false)
		text := out.String()
		if strings.Contains(text, "\033[") {
			t.Fatal("buffer help must never contain ANSI styling")
		}
		for i, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
			if helpTextWidth(line) > width {
				t.Errorf("line %d exceeds configured width (%d): %q", i+1, width, line)
			}
		}
	}
	text := out.String()
	for _, want := range []string{"Usage:", "factory implement", "factory tidy", "factory help"} {
		if !strings.Contains(text, want) {
			t.Errorf("concise help omitted %q: %s", want, text)
		}
	}
	for _, omitted := range []string{"factory implement add a small feature", "Dirty safe mode", "Execution and safeguards"} {
		if strings.Contains(text, omitted) {
			t.Errorf("concise help unexpectedly includes %q: %s", omitted, text)
		}
	}
}

func TestHelpOutputFitsNarrowWidths(t *testing.T) {
	for _, width := range []int{24, 39} {
		t.Run(fmt.Sprintf("width-%d", width), func(t *testing.T) {
			var out bytes.Buffer
			printRootHelpWithOptions(&out, width, false, false)
			for lineNumber, line := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
				if cells := helpTextWidth(line); cells > width {
					t.Errorf("line %d has %d terminal cells, exceeding width %d: %q", lineNumber+1, cells, width, line)
				}
			}
			if !strings.Contains(out.String(), "Usage:") || !strings.Contains(out.String(), "factory implement") {
				t.Errorf("width %d concise help omitted usage or workflow", width)
			}
		})
	}
}

func TestHelpTextWrappingUsesTerminalCellsForUnicode(t *testing.T) {
	commands := []helpCommand{{usage: "factory 界 command", desc: "e\u0301界 description with Unicode"}}
	for _, width := range []int{24, 39} {
		var output bytes.Buffer
		printHelpCommands(&output, commands, width)
		for lineNumber, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
			if cells := helpTextWidth(line); cells > width {
				t.Errorf("Unicode command line %d has %d cells, exceeding width %d: %q", lineNumber+1, cells, width, line)
			}
		}
	}
	for _, tc := range []struct {
		text  string
		width int
	}{
		{text: "界界界", width: 4},
		{text: "e\u0301e\u0301e\u0301", width: 2},
		{text: "界 e\u0301 界", width: 3},
	} {
		lines := wrapHelpText(tc.text, tc.width)
		if strings.Join(lines, "") == "" {
			t.Fatalf("wrapHelpText(%q, %d) returned no text", tc.text, tc.width)
		}
		for _, line := range lines {
			if cells := helpTextWidth(line); cells > tc.width {
				t.Errorf("wrapped line %q has %d cells, exceeding width %d", line, cells, tc.width)
			}
		}
	}
}

func TestHelpWidthPrefersTerminalThenColumnsThenDefault(t *testing.T) {
	calls := 0
	query := func() (int, error) {
		calls++
		return 62, nil
	}
	if got := helpWidthFromEnvironment("91", query); got != 62 || calls != 1 {
		t.Fatalf("terminal width=%d queries=%d, want 62 and one query", got, calls)
	}
	if got := helpWidthFromEnvironment("invalid", func() (int, error) { return 0, errors.New("not a tty") }); got != 80 {
		t.Fatalf("fallback width=%d, want 80", got)
	}
	if got := helpWidthFromEnvironment("91", func() (int, error) { return 0, errors.New("not a tty") }); got != 91 {
		t.Fatalf("environment fallback width=%d, want 91", got)
	}
}

func TestHelpStylingRequiresTTYColorAndWideEnoughTerminal(t *testing.T) {
	for _, tc := range []struct {
		name     string
		width    int
		terminal bool
		noColor  bool
		wantANSI bool
	}{
		{name: "wide tty", width: 80, terminal: true, wantANSI: true},
		{name: "buffer", width: 80, wantANSI: false},
		{name: "pipe", width: 80, terminal: false, wantANSI: false},
		{name: "color disabled", width: 80, terminal: true, noColor: true, wantANSI: false},
		{name: "narrow tty", width: 39, terminal: true, wantANSI: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			printRootHelpWithOptions(&out, tc.width, tc.terminal, tc.noColor)
			got := strings.Contains(out.String(), "\033[")
			if got != tc.wantANSI {
				t.Fatalf("ANSI present=%t, want %t", got, tc.wantANSI)
			}
		})
	}
}

func TestRunCommandDispatchesGatedRunControls(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	var out, errOut bytes.Buffer
	if err := run([]string{"run", "list"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatalf("run list dispatch: %v", err)
	}
	if out.String() != "No gated runs.\n" {
		t.Fatalf("run list output=%q", out.String())
	}
}

func TestJobWatchCLIUsesPlainSnapshotForNonTerminalStreams(t *testing.T) {
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	storeRoot, err := factory.JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := factory.NewJobStore(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	const id = "watch-terminal-job"
	if err := store.CreateJob(factory.JobRecord{ID: id, Type: "implementation", Status: "complete"}); err != nil {
		t.Fatal(err)
	}

	var out, errOut bytes.Buffer
	if err := run([]string{"job", "watch", id}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatalf("job watch dispatch: %v", err)
	}
	if got, want := out.String(), "Selected jobs\nJob "+id+"\n  Status: complete\n  Latest activity: No recorded activity\n  Next action: Inspect the completed result with `factory job get "+id+" --details`.\n\n"; got != want {
		t.Fatalf("non-terminal job watch output = %q, want snapshot %q", got, want)
	}
	if strings.Contains(out.String(), "\033[") {
		t.Fatalf("non-terminal job watch output contains ANSI redraw codes: %q", out.String())
	}
	if errOut.Len() != 0 {
		t.Fatalf("job watch wrote unexpected stderr: %q", errOut.String())
	}
}

func TestWorkflowApprovalCancellationWithPipeLeavesInputOpen(t *testing.T) {
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readEnd.Close()
	defer writeEnd.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stateDir := filepath.Join(t.TempDir(), "state")
	prompted := make(chan struct{})
	workflow := factory.Workflow{
		Agent: passingTestAgent{}, Config: factory.Config{StateDir: stateDir},
		In: &contextStdin{file: readEnd, ctx: ctx}, Out: promptSignalWriter{prompted: prompted},
		Workdir: t.TempDir(), Gate: true, Stages: []string{"requirements"},
	}
	result := make(chan error, 1)
	go func() { result <- workflow.RunContext(ctx, "task") }()
	select {
	case <-prompted:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("workflow did not reach approval prompt")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled workflow error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("workflow did not return promptly after approval cancellation")
	}
	entries, err := os.ReadDir(filepath.Join(stateDir, "runs"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected persisted workflow state, entries=%v err=%v", entries, err)
	}
	state, err := os.ReadFile(filepath.Join(stateDir, "runs", entries[0].Name(), "state.json"))
	if err != nil || !strings.Contains(string(state), `"status": "interrupted"`) {
		t.Fatalf("canceled approval state = %s, %v; want interrupted", state, err)
	}
	if _, err := writeEnd.Write([]byte("yes\n")); err != nil {
		t.Fatalf("canceled prompt closed stdin: %v", err)
	}
}

type passingTestAgent struct{}

func (passingTestAgent) Run(_, _, _, _, logPath string) error {
	return os.WriteFile(logPath, []byte("PASS\n"), 0o600)
}

func (a passingTestAgent) RunWithContext(_ context.Context, stage, prompt, task, workdir, logPath string) error {
	return a.Run(stage, prompt, task, workdir, logPath)
}

type promptSignalWriter struct {
	prompted chan struct{}
}

func (w promptSignalWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "Type exactly yes") {
		select {
		case <-w.prompted:
		default:
			close(w.prompted)
		}
	}
	return io.Discard.Write(p)
}

func TestForegroundContextStopsOnInterruptAndCanBeReleased(t *testing.T) {
	ctx, stop := foregroundContext()
	// Canceling the context releases signal.NotifyContext's signal resources.
	stop()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("stopped foreground context error = %v, want canceled", ctx.Err())
	}
}

func TestForegroundAgentUsesSignalContextForContextAwareRunner(t *testing.T) {
	signalCtx, cancel := context.WithCancel(context.Background())
	cancel()
	agent := foregroundAgent{ctx: signalCtx, agent: cancelAwareTestAgent{}}
	err := agent.RunWithContext(context.Background(), "review", "prompt", "task", ".", "log")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("foreground agent error = %v, want signal context cancellation", err)
	}
}

func TestForegroundAgentHonorsSuppliedContextBudget(t *testing.T) {
	started := make(chan struct{})
	suppliedCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	signalCtx := context.Background()
	agent := foregroundAgent{ctx: signalCtx, agent: waitingContextAgent{started: started}}

	err := agent.RunWithContext(suppliedCtx, "review", "prompt", "task", ".", "log")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("foreground agent error = %v, want cancellation from supplied budget", err)
	}
	select {
	case <-started:
	default:
		t.Fatal("context-aware runner was not started")
	}
	if signalCtx.Err() != nil {
		t.Fatalf("supplied budget cancellation affected outer signal context: %v", signalCtx.Err())
	}
}

type waitingContextAgent struct {
	started chan struct{}
}

func (a waitingContextAgent) Run(string, string, string, string, string) error {
	return errors.New("non-context runner called")
}

func (a waitingContextAgent) RunWithContext(ctx context.Context, _, _, _, _, _ string) error {
	close(a.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestForegroundAgentAllowsStageWithoutStdoutProtocol(t *testing.T) {
	legacy := &legacyForegroundAgent{}
	wrapped := foregroundAgent{ctx: context.Background(), agent: legacy}
	workflow := factory.Workflow{
		Agent: wrapped, Config: factory.Config{StateDir: filepath.Join(t.TempDir(), "state")},
		In: strings.NewReader(""), Out: io.Discard, Workdir: t.TempDir(), Stages: []string{"requirements"},
	}
	if err := workflow.Run("task"); err != nil {
		t.Fatalf("successful stage should not require evaluator stdout protocol: %v", err)
	}
	if legacy.calls != 1 {
		t.Fatalf("legacy agent calls = %d, want one stage invocation", legacy.calls)
	}
}

type legacyForegroundAgent struct {
	calls int
}

func (a *legacyForegroundAgent) Run(_, _, _, _, logPath string) error {
	a.calls++
	return os.WriteFile(logPath, []byte("PASS\nstderr-only protocol text\n"), 0o600)
}

func (a *legacyForegroundAgent) RunWithContext(_ context.Context, stage, prompt, task, workdir, logPath string) error {
	return a.Run(stage, prompt, task, workdir, logPath)
}

type cancelAwareTestAgent struct{}

func (cancelAwareTestAgent) Run(string, string, string, string, string) error {
	return errors.New("non-context runner called")
}

func (cancelAwareTestAgent) RunWithContext(ctx context.Context, _, _, _, _, _ string) error {
	return ctx.Err()
}

func TestGatedImplementRejectsNonInteractiveBeforeAgentOrRunState(t *testing.T) {
	stateDir := t.TempDir()
	configDir := filepath.Join(t.TempDir(), "config")
	factoryConfigDir := filepath.Join(configDir, "factory")
	if err := os.MkdirAll(factoryConfigDir, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "agent-invoked")
	agent := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\ntouch \"$AGENT_MARKER\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q}`, agent, stateDir)
	if err := os.WriteFile(filepath.Join(factoryConfigDir, "config.json"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", configDir)
	t.Setenv("AGENT_MARKER", marker)

	tests := []struct {
		name           string
		stdinTerminal  bool
		stdoutTerminal bool
	}{
		{name: "stdin is not a terminal", stdoutTerminal: true},
		{name: "stdout is not a terminal", stdinTerminal: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			readEnd, writeEnd, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { readEnd.Close(); writeEnd.Close() })
			terminalCheck := func(file *os.File) bool {
				if file == readEnd {
					return tc.stdinTerminal
				}
				if file == os.Stdout {
					return tc.stdoutTerminal
				}
				return false
			}
			err = runPipelineContextWithTerminalCheck(context.Background(), []string{"do the work"}, true, readEnd, os.Stdout, terminalCheck)
			if err == nil || !strings.Contains(err.Error(), "factory implement --gate requires an interactive terminal for approvals") {
				t.Fatalf("gated implementation error = %v", err)
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("agent invoked before terminal check: stat marker error=%v", err)
			}
			if _, err := os.Stat(filepath.Join(stateDir, "runs")); !os.IsNotExist(err) {
				t.Fatalf("gated implementation created run state before terminal check: stat error=%v", err)
			}
		})
	}
}

func TestRemovedPipelineAliasIsRejectedWhileBareInvocationStillRunsImplement(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := run([]string{"pipeline"}, strings.NewReader("task\n"), &out, &errOut); err == nil || !strings.Contains(err.Error(), `unknown command "pipeline"`) {
		t.Fatalf("pipeline alias error = %v", err)
	}

	var errs []error
	for _, args := range [][]string{{"implement"}, nil} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out, errOut bytes.Buffer
			err := run(args, strings.NewReader("task\n"), &out, &errOut)
			if err == nil || !strings.Contains(err.Error(), "interactive terminal") || !strings.Contains(err.Error(), "factory implement") {
				t.Fatalf("non-terminal invocation error = %v", err)
			}
			errs = append(errs, err)
		})
	}
	if len(errs) != 2 || errs[0].Error() != errs[1].Error() {
		t.Fatalf("bare invocation and implement should use the same workflow: errors=%v", errs)
	}
}

func TestTaskEntryPromptsWithOutputGuidanceWhenInitiallyEmpty(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("implement the result\n.\n"))
	var output bytes.Buffer
	task, err := readTask(reader, &output)
	if err != nil {
		t.Fatal(err)
	}
	if task != "implement the result" {
		t.Fatalf("task = %q, want entered task", task)
	}
	if !strings.Contains(output.String(), "Tip:") || !strings.Contains(output.String(), "how it should be verified") {
		t.Fatalf("empty-entry guidance missing: %q", output.String())
	}
}

func TestCLIParsesGateOptionAcrossSubcommands(t *testing.T) {
	for _, tc := range []struct {
		args []string
		gate bool
		task string
	}{
		{args: []string{"implement", "--gate", "do", "work"}, gate: true, task: "do work"},
		{args: []string{"--gate", "do", "work"}, gate: true, task: "do work"},
		{args: []string{"implement", "do", "work"}, gate: false, task: "do work"},
	} {
		gotGate, gotTask, err := parseGate(tc.args[1:])
		if tc.args[0] == "--gate" {
			gotGate, gotTask, err = parseGate(tc.args)
		}
		if err != nil || gotGate != tc.gate || strings.Join(gotTask, " ") != tc.task {
			t.Fatalf("CLI parse %v = (%v, %v, %v)", tc.args, gotGate, gotTask, err)
		}
	}
	if gate, task, err := parseGate([]string{"--gate"}); err != nil || !gate || len(task) != 0 {
		t.Fatalf("tidy gate parse = (%v, %v, %v)", gate, task, err)
	}
}

func TestGateParsingSupportsImplementAndBareForms(t *testing.T) {
	for _, args := range [][]string{{"--gate", "a", "task"}, {"a", "--gate", "task"}} {
		gate, task, err := parseGate(args)
		if err != nil || !gate || strings.Join(task, " ") != "a task" {
			t.Fatalf("parseGate(%v) = (%v, %v, %v)", args, gate, task, err)
		}
	}
	if _, _, err := parseGate([]string{"--gate", "--gate"}); err == nil {
		t.Fatal("duplicate --gate accepted")
	}
	for _, args := range [][]string{{"--detach", "-d"}, {"-d", "--detach"}} {
		if _, _, _, err := parseWorkflowOptions(args); err == nil {
			t.Fatalf("duplicate detach flags accepted: %v", args)
		}
	}
	for _, args := range [][]string{{"implement", "--gate", "--detach"}, {"tidy", "-d", "--gate"}} {
		gate, detach, _, err := parseWorkflowOptions(args[1:])
		if err != nil || !gate || !detach {
			t.Fatalf("options %v parse = gate=%v detach=%v err=%v", args, gate, detach, err)
		}
	}
}

func TestGateDetachConflictIsRejectedBeforeConfigLoading(t *testing.T) {
	configHome := t.TempDir()
	factoryConfig := filepath.Join(configHome, "factory")
	if err := os.MkdirAll(factoryConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(factoryConfig, "config.json"), []byte("not valid config"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", configHome)

	var output, errOutput bytes.Buffer
	err := run([]string{"--gate", "-d", "x"}, strings.NewReader(""), &output, &errOutput)
	if err == nil || !strings.Contains(err.Error(), "factory implement --gate cannot be combined with --detach") {
		t.Fatalf("gate/detach conflict = %v, want conflict validation error", err)
	}
	if strings.Contains(err.Error(), "config") || strings.Contains(err.Error(), "invalid") {
		t.Fatalf("configuration was loaded before conflict validation: %v", err)
	}
}

func TestDetachHelpAndGateConflictValidation(t *testing.T) {
	for _, command := range []string{"implement", "tidy"} {
		var output, errors bytes.Buffer
		if err := run([]string{command, "--help"}, strings.NewReader(""), &output, &errors); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(output.String(), "--detach") || !strings.Contains(output.String(), "-d") {
			t.Errorf("%s help omitted detach aliases: %q", command, output.String())
		}
	}
	for _, args := range [][]string{
		{"implement", "--gate", "--detach", "work"},
		{"implement", "-d", "--gate", "work"},
		{"--gate", "-d", "work"},
		{"tidy", "--gate", "--detach"},
		{"tidy", "--detach", "--gate"},
	} {
		var output, errors bytes.Buffer
		err := run(args, strings.NewReader(""), &output, &errors)
		if err == nil || !strings.Contains(err.Error(), "cannot be combined") {
			t.Errorf("CLI accepted conflicting flags %v: %v", args, err)
		}
	}
}

func TestImplementDefaultsToAttachedImplementationJob(t *testing.T) {
	script := filepath.Join(t.TempDir(), "fake-agent")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 0.1\nprintf 'workflow output\\n' > generated.txt\ncase \"$2\" in *'Stage completed'*) echo PASS ;; *) echo agent-output ;; esac\n"), 0o700); err != nil {
		t.Fatal(err)
	}
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
	for _, name := range []string{"implement"} {
		t.Run(name, func(t *testing.T) {
			state := t.TempDir()
			realTarget := t.TempDir()
			git := func(args ...string) {
				t.Helper()
				command := exec.Command("git", args...)
				command.Dir = realTarget
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("git %v: %v\n%s", args, err, output)
				}
			}
			git("init", "-q", "-b", "main")
			git("config", "user.name", "Factory Test")
			git("config", "user.email", "factory-test@example.invalid")
			if err := os.WriteFile(filepath.Join(realTarget, "tracked.txt"), []byte("baseline\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			git("add", "tracked.txt")
			git("commit", "-qm", "baseline")
			remote := filepath.Join(t.TempDir(), "origin.git")
			if output, err := exec.Command("git", "init", "--bare", remote).CombinedOutput(); err != nil {
				t.Fatalf("initialize origin: %v\n%s", err, output)
			}
			git("remote", "add", "origin", remote)
			fakeBin := t.TempDir()
			if err := os.WriteFile(filepath.Join(fakeBin, "gh"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(filepath.Dir(realTarget), "target-alias")
			if err := os.Symlink(realTarget, target); err != nil {
				t.Skipf("directory symlinks unavailable: %v", err)
			}
			configDir := filepath.Join(t.TempDir(), "config", "factory")
			if err := os.MkdirAll(configDir, 0o700); err != nil {
				t.Fatal(err)
			}
			config := fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"5s"}`, script, state)
			if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(binary, name, "complete", "the", "task")
			command.Dir = target
			command.Env = append(os.Environ(), "XDG_CONFIG_HOME="+filepath.Dir(configDir), "XDG_STATE_HOME="+state, "PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("%s command: %v\n%s", name, err, output)
			}
			if !strings.Contains(string(output), "Started pipeline job ") || !strings.Contains(string(output), "requirements completed") || !strings.Contains(string(output), "Published implementation PR") {
				t.Fatalf("%s did not attach to the detached worker and publish its isolated branch: %q", name, output)
			}
			storeRoot, err := factory.JobStateRoot(state)
			if err != nil {
				t.Fatal(err)
			}
			store, err := factory.NewJobStore(storeRoot)
			if err != nil {
				t.Fatal(err)
			}
			jobs, err := store.ListJobs()
			if err != nil || len(jobs) != 1 || jobs[0].Status != "complete" || jobs[0].TaskDescription != "complete the task" || !sameResolvedTestPath(t, jobs[0].TargetPath, target) || jobs[0].Worktree == "" || jobs[0].WorkBranch == "" {
				t.Fatalf("%s job records = %+v, err=%v", name, jobs, err)
			}
			if _, err := exec.Command("git", "--git-dir", remote, "show-ref", "--verify", "refs/heads/"+jobs[0].WorkBranch).CombinedOutput(); err != nil {
				t.Fatalf("%s worker did not push its isolated branch %q: %v", name, jobs[0].WorkBranch, err)
			}
		})
	}
}

func TestGatedImplementOptOutStillRejectsNonGitTarget(t *testing.T) {
	state := t.TempDir()
	configHome := t.TempDir()
	factoryConfig := filepath.Join(configHome, "factory")
	if err := os.MkdirAll(factoryConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "agent-ran")
	agent := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\nprintf called > "+marker+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(factoryConfig, "config.json"), []byte(fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"auto_publish":false}`, agent, state)), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("XDG_STATE_HOME", state)
	t.Chdir(t.TempDir())
	input, approvals, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	_ = approvals.Close()
	output, err := os.CreateTemp(t.TempDir(), "gated-output-")
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	err = runPipelineContextWithTerminalCheck(context.Background(), []string{"reject non-Git"}, true, input, output, func(*os.File) bool { return true })
	if err == nil || !strings.Contains(err.Error(), "Git checkout") {
		t.Fatalf("gated non-Git implementation error = %v", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("agent ran before gated Git prerequisite validation (stat error %v)", err)
	}
}

func TestGatedImplementUsesAutomaticForegroundPublisherByDefault(t *testing.T) {
	target := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = target
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
		return string(output)
	}
	git("init", "-q", "-b", "main")
	git("config", "user.name", "Factory Test")
	git("config", "user.email", "factory-test@example.invalid")
	if err := os.WriteFile(filepath.Join(target, "tracked.txt"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", "tracked.txt")
	git("commit", "-qm", "baseline")
	baseline := strings.TrimSpace(git("rev-parse", "HEAD"))
	remote := filepath.Join(t.TempDir(), "origin.git")
	if output, err := exec.Command("git", "init", "--bare", remote).CombinedOutput(); err != nil {
		t.Fatalf("initialize origin: %v\n%s", err, output)
	}
	git("remote", "add", "origin", remote)
	t.Chdir(target)

	state := t.TempDir()
	configHome := t.TempDir()
	factoryConfig := filepath.Join(configHome, "factory")
	if err := os.MkdirAll(factoryConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	agent := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\nprintf 'PASS\\n'\nprintf 'workflow output\\n' > generated.txt\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(factoryConfig, "config.json"), []byte(fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"5s"}`, agent, state)), 0o600); err != nil {
		t.Fatal(err)
	}
	fakeBin := t.TempDir()
	if err := os.WriteFile(filepath.Join(fakeBin, "gh"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("XDG_STATE_HOME", state)
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	input, approvals, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	if _, err := approvals.WriteString(strings.Repeat("yes\n", 4)); err != nil {
		t.Fatal(err)
	}
	if err := approvals.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := os.CreateTemp(t.TempDir(), "foreground-output-")
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	terminalCheck := func(*os.File) bool { return true }
	if err := runPipelineContextWithTerminalCheck(context.Background(), []string{"publish the task"}, true, input, output, terminalCheck); err != nil {
		t.Fatalf("gated implementation: %v", err)
	}
	if _, err := output.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	outputText, err := io.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(outputText), "Published implementation PR") {
		t.Fatalf("gated implementation did not use foreground publisher: %s", outputText)
	}
	if head := strings.TrimSpace(git("rev-parse", "HEAD")); head != baseline {
		t.Fatalf("foreground publication changed invoking checkout HEAD: %s", head)
	}
	if status := strings.TrimSpace(git("status", "--porcelain")); status != "" {
		t.Fatalf("foreground publication changed invoking checkout: %s", status)
	}
	refs := strings.TrimSpace(runTestGit(t, "--git-dir", remote, "for-each-ref", "--format=%(refname:short)"))
	if !strings.HasPrefix(refs, "factory-implement-") {
		t.Fatalf("foreground publisher did not push an isolated task branch: %s", refs)
	}
	storeRoot, err := factory.JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := factory.NewJobStore(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if jobs, err := store.ListJobs(); err != nil || len(jobs) != 0 {
		t.Fatalf("gated invocation created detached jobs: %+v, %v", jobs, err)
	}
}

func runTestGit(t *testing.T, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return string(output)
}

func TestDetachedImplementCLIPublishedPRPersistsAcrossProcesses(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	configHome := filepath.Join(root, "config")
	configDir := filepath.Join(configHome, "factory")
	for _, dir := range []string{state, configDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	agent := filepath.Join(root, "fake-pi")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\ncase \"$*\" in *'failed published handoff'*) printf 'PASS\\nFAILED_PI_TRANSCRIPT_MARKER\\n' ;; *) printf 'PASS\\nPI_WORKFLOW_TRANSCRIPT_MARKER\\n' ;; esac\nprintf 'generated\\n' > generated.txt\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"30s","auto_publish":true}`, agent, state)), 0o600); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(root, "target")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main", target}, {"-C", target, "config", "user.name", "Factory Test"}, {"-C", target, "config", "user.email", "factory-test@example.invalid"}} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(target, "tracked.txt"), []byte("baseline\\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("git", "-C", target, "add", "tracked.txt").CombinedOutput(); err != nil {
		t.Fatalf("git add: %v\n%s", err, output)
	}
	if output, err := exec.Command("git", "-C", target, "commit", "-qm", "baseline").CombinedOutput(); err != nil {
		t.Fatalf("git commit: %v\n%s", err, output)
	}
	remote := filepath.Join(root, "origin.git")
	if output, err := exec.Command("git", "init", "--bare", remote).CombinedOutput(); err != nil {
		t.Fatalf("init local bare remote: %v\n%s", err, output)
	}
	if output, err := exec.Command("git", "-C", target, "remote", "add", "origin", remote).CombinedOutput(); err != nil {
		t.Fatalf("add local origin: %v\n%s", err, output)
	}
	target, err := filepath.EvalSymlinks(target)
	if err != nil {
		t.Fatal(err)
	}
	initialHead := strings.TrimSpace(runTestGit(t, "-C", target, "rev-parse", "HEAD"))
	initialStatus := runTestGit(t, "-C", target, "status", "--porcelain")

	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	gitCalls := filepath.Join(state, "git-calls")
	fakeBin := filepath.Join(root, "bin")
	if err := os.MkdirAll(fakeBin, 0o700); err != nil {
		t.Fatal(err)
	}
	gitWrapper := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$*\" >> %q\nexec %q \"$@\"\n", gitCalls, gitPath)
	if err := os.WriteFile(filepath.Join(fakeBin, "git"), []byte(gitWrapper), 0o700); err != nil {
		t.Fatal(err)
	}
	ghCalls := filepath.Join(state, "gh-calls")
	ghStub := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$*\" > %q\nprintf 'https://github.com/example/repo/pull/149\\n'\n", ghCalls)
	if err := os.WriteFile(filepath.Join(fakeBin, "gh"), []byte(ghStub), 0o700); err != nil {
		t.Fatal(err)
	}

	binary := filepath.Join(root, "factory")
	projectRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/factory")
	build.Dir = projectRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build factory: %v\n%s", err, output)
	}
	env := make([]string, 0, len(os.Environ())+5)
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		switch key {
		case "GH_TOKEN", "GITHUB_TOKEN", "GH_ENTERPRISE_TOKEN", "GITHUB_ENTERPRISE_TOKEN", "GH_CONFIG_DIR", "GH_HOST":
			continue
		}
		env = append(env, item)
	}
	env = append(env, "XDG_CONFIG_HOME="+configHome, "XDG_STATE_HOME="+state, "GH_CONFIG_DIR="+filepath.Join(root, "empty-gh-config"), "PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	doctor := exec.Command(binary, "doctor")
	doctor.Dir, doctor.Env = target, env
	doctorOutput, err := doctor.CombinedOutput()
	if err != nil {
		t.Fatalf("documented factory doctor command: %v\n%s", err, doctorOutput)
	}
	for _, want := range []string{"config: valid", "agent executable: available", "git: available", "gh: available", "pipeline checks: 0 configured"} {
		if !strings.Contains(string(doctorOutput), want) {
			t.Errorf("factory doctor output omitted %q: %s", want, doctorOutput)
		}
	}
	command := exec.Command(binary, "implement", "--detach", "persist published handoff")
	command.Dir = target
	command.Env = env
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("start detached implementation: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "Started implementation job") {
		t.Fatalf("detached start output = %q", output)
	}
	storeRoot, err := factory.JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := factory.NewJobStore(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	var job factory.JobRecord
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		jobs, err := store.ListJobs()
		if err != nil {
			t.Fatal(err)
		}
		if len(jobs) == 1 && jobs[0].TaskDescription == "persist published handoff" {
			job = jobs[0]
			if job.Status == "complete" {
				if _, err := store.ReadWorker(job.ID); errors.Is(err, os.ErrNotExist) {
					break
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if job.ID == "" || job.Status != "complete" {
		t.Fatalf("detached job did not complete: %+v", job)
	}
	job, err = store.GetJob(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	const wantPRURL = "https://github.com/example/repo/pull/149"
	if job.PublicationStatus != "published" || !strings.Contains(job.PublicationSummary, wantPRURL) {
		t.Fatalf("persisted publication outcome = status %q summary %q", job.PublicationStatus, job.PublicationSummary)
	}
	if _, err := os.Stat(job.Worktree); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("published worktree was not cleaned up: %v", err)
	}
	if got := strings.TrimSpace(runTestGit(t, "--git-dir", remote, "for-each-ref", "--format=%(refname:short)")); got != job.WorkBranch || got == "main" {
		t.Fatalf("local bare remote branch = %q, want isolated job branch %q", got, job.WorkBranch)
	}
	if calls, err := os.ReadFile(gitCalls); err != nil || !strings.Contains(string(calls), "push --porcelain") {
		t.Fatalf("local git wrapper did not observe branch push: calls=%q err=%v", calls, err)
	}
	if calls, err := os.ReadFile(ghCalls); err != nil || !strings.HasPrefix(string(calls), "pr create ") {
		t.Fatalf("fake gh did not observe PR creation: calls=%q err=%v", calls, err)
	}

	inspect := exec.Command(binary, "job", "get", job.ID, "--details")
	inspect.Dir = target
	inspect.Env = env
	details, err := inspect.CombinedOutput()
	if err != nil {
		t.Fatalf("fresh-process job details: %v\n%s", err, details)
	}
	for _, want := range []string{"Status: complete", "Publication: published", wantPRURL} {
		if !strings.Contains(string(details), want) {
			t.Errorf("fresh-process details omitted %q: %s", want, details)
		}
	}
	if got := strings.TrimSpace(runTestGit(t, "-C", target, "rev-parse", "HEAD")); got != initialHead {
		t.Errorf("invoking checkout HEAD changed: got %s want %s", got, initialHead)
	}
	if got := runTestGit(t, "-C", target, "status", "--porcelain"); got != initialStatus {
		t.Errorf("invoking checkout status changed: got %q want %q", got, initialStatus)
	}

	failedGHCalls := filepath.Join(state, "failed-gh-calls")
	failedGH := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$*\" > %q\necho unavailable >&2\nexit 1\n", failedGHCalls)
	if err := os.WriteFile(filepath.Join(fakeBin, "gh"), []byte(failedGH), 0o700); err != nil {
		t.Fatal(err)
	}
	failedCommand := exec.Command(binary, "implement", "-d", "failed published handoff")
	failedCommand.Dir = target
	failedCommand.Env = env
	if output, err := failedCommand.CombinedOutput(); err != nil || !strings.Contains(string(output), "Started implementation job") {
		t.Fatalf("start detached publication-failure job: %v\n%s", err, output)
	}
	var failedJob factory.JobRecord
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		jobs, err := store.ListJobs()
		if err != nil {
			t.Fatal(err)
		}
		for _, candidate := range jobs {
			if candidate.TaskDescription == "failed published handoff" {
				failedJob = candidate
			}
		}
		if failedJob.ID != "" && failedJob.Status == "complete" {
			if _, err := store.ReadWorker(failedJob.ID); errors.Is(err, os.ErrNotExist) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if failedJob.ID == "" || failedJob.Status != "complete" || failedJob.PublicationStatus != "unpublished" {
		t.Fatalf("publication failure job outcome = %+v", failedJob)
	}
	failedJob, err = store.GetJob(failedJob.ID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(failedJob.PublicationSummary, wantPRURL) {
		t.Fatalf("failed publication persisted a false PR URL: %q", failedJob.PublicationSummary)
	}
	if info, err := os.Stat(failedJob.Worktree); err != nil || !info.IsDir() {
		t.Fatalf("failed publication recovery worktree was not retained at %q: %v", failedJob.Worktree, err)
	}
	if artifact, err := os.ReadFile(filepath.Join(failedJob.Worktree, "generated.txt")); err != nil || string(artifact) != "generated\n" {
		t.Errorf("failed publication recovery artifact = %q, err=%v", artifact, err)
	}
	inspectFailed := exec.Command(binary, "job", "get", failedJob.ID, "--details")
	inspectFailed.Dir = target
	inspectFailed.Env = env
	failedDetails, err := inspectFailed.CombinedOutput()
	if err != nil {
		t.Fatalf("fresh-process failed job details: %v\n%s", err, failedDetails)
	}
	if !strings.Contains(string(failedDetails), "Publication: unpublished") || strings.Contains(string(failedDetails), wantPRURL) {
		t.Errorf("failed publication details claim a PR handoff: %s", failedDetails)
	}
	failedLogs := exec.Command(binary, "job", "logs", failedJob.ID, "--session", "workflow")
	failedLogs.Dir, failedLogs.Env = target, env
	failedLogOutput, err := failedLogs.CombinedOutput()
	if err != nil || !strings.Contains(string(failedLogOutput), "stage.completed stage=implement") {
		t.Errorf("fresh-process workflow logs omitted completed implementation event: err=%v output=%s", err, failedLogOutput)
	}
	stageLogs, err := filepath.Glob(filepath.Join(state, "runs", "*", "02-implement.log"))
	if err != nil || len(stageLogs) != 2 {
		t.Fatalf("implementation stage transcripts = %v, err=%v; want one per completed job", stageLogs, err)
	}
	foundFailedTranscript := false
	for _, path := range stageLogs {
		transcript, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("read implementation transcript %q: %v", path, readErr)
		}
		if strings.Contains(string(transcript), "PI_WORKFLOW_TRANSCRIPT_MARKER") {
			foundFailedTranscript = true
		}
	}
	if !foundFailedTranscript {
		t.Errorf("fake Pi transcript marker was not retained in implementation transcripts: %v", stageLogs)
	}
	foundFailureTranscript := false
	for _, path := range stageLogs {
		transcript, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("read implementation transcript %q: %v", path, readErr)
		}
		if strings.Contains(string(transcript), "FAILED_PI_TRANSCRIPT_MARKER") {
			foundFailureTranscript = true
		}
	}
	if !foundFailureTranscript {
		t.Errorf("unpublished fake Pi transcript marker was not retained: %v", stageLogs)
	}
	if calls, err := os.ReadFile(failedGHCalls); err != nil || !strings.Contains(string(calls), "pr create ") {
		t.Errorf("failed publication did not use local gh tripwire: calls=%q err=%v", calls, err)
	}
	if got := strings.TrimSpace(runTestGit(t, "-C", target, "rev-parse", "HEAD")); got != initialHead {
		t.Errorf("failed publication changed invoking checkout HEAD: got %s want %s", got, initialHead)
	}
	if got := runTestGit(t, "-C", target, "status", "--porcelain"); got != initialStatus {
		t.Errorf("failed publication changed invoking checkout status: got %q want %q", got, initialStatus)
	}
}

func TestDetachedImplementCLIStopWorksAcrossProcesses(t *testing.T) {
	root, err := os.MkdirTemp("", "factory detached worker test-")
	if err != nil {
		t.Fatal(err)
	}
	removeRoot := true
	t.Cleanup(func() {
		if !removeRoot {
			t.Errorf("preserving detached worker test directory because worker shutdown could not be verified: %s", root)
			return
		}
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove detached worker test directory: %v", err)
		}
	})

	state := filepath.Join(root, "state")
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	releaseWorker := filepath.Join(state, "release-worker")
	workerStarted := filepath.Join(state, "worker-started")
	ghCalled := filepath.Join(state, "gh-called")
	configHome := filepath.Join(root, "config")
	factoryConfig := filepath.Join(configHome, "factory")
	if err := os.MkdirAll(factoryConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "agent")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ncase \"$1\" in *'Propose a concise slug'*) printf 'test-detached\\n'; exit 0 ;; esac\n: > \"$FACTORY_TEST_STARTED\"\nwhile [ ! -f \"$FACTORY_TEST_RELEASE\" ]; do sleep 0.02; done\nprintf 'PASS\\nfake-agent-output\\n'\nprintf 'generated\\n' > generated.txt\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(factoryConfig, "config.json"), []byte(fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"30s","auto_publish":true}`, script, state)), 0o600); err != nil {
		t.Fatal(err)
	}
	fakeBin := filepath.Join(root, "bin")
	if err := os.MkdirAll(fakeBin, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fakeBin, "gh"), []byte("#!/bin/sh\n: > \"$FACTORY_TEST_GH_CALLED\"\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "factory")
	projectRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/factory")
	build.Dir = projectRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build factory: %v\n%s", err, output)
	}
	target := filepath.Join(root, "target")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main", target}, {"-C", target, "config", "user.name", "Factory Test"}, {"-C", target, "config", "user.email", "factory-test@example.invalid"}} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(target, "tracked.txt"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("git", "-C", target, "add", "tracked.txt").CombinedOutput(); err != nil {
		t.Fatalf("git add: %v\n%s", err, output)
	}
	if output, err := exec.Command("git", "-C", target, "commit", "-qm", "baseline").CombinedOutput(); err != nil {
		t.Fatalf("git commit: %v\n%s", err, output)
	}
	initialHead := strings.TrimSpace(runTestGit(t, "-C", target, "rev-parse", "HEAD"))
	target, err = filepath.EvalSymlinks(target)
	if err != nil {
		t.Fatal(err)
	}
	storeRoot, err := factory.JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := factory.NewJobStore(storeRoot)
	if err != nil {
		t.Fatal(err)
	}

	command := exec.Command(binary, "implement", "-d", "test", "detached")
	command.Dir = target
	command.Env = append(os.Environ(), "XDG_CONFIG_HOME="+configHome, "XDG_STATE_HOME="+state, "FACTORY_TEST_RELEASE="+releaseWorker, "FACTORY_TEST_STARTED="+workerStarted, "FACTORY_TEST_GH_CALLED="+ghCalled, "PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	var commandOutput bytes.Buffer
	command.Stdout, command.Stderr = &commandOutput, &commandOutput
	var commandDone chan error
	commandExited := false
	findTestJob := func() (factory.JobRecord, bool, error) {
		jobs, err := store.ListJobs()
		if err != nil {
			return factory.JobRecord{}, false, err
		}
		var found factory.JobRecord
		for _, job := range jobs {
			if job.TaskDescription == "test detached" && sameResolvedTestPath(t, job.TargetPath, target) {
				if found.ID != "" {
					return factory.JobRecord{}, false, fmt.Errorf("multiple detached test jobs found: %s and %s", found.ID, job.ID)
				}
				found = job
			}
		}
		if found.ID == "" {
			return factory.JobRecord{}, false, nil
		}
		return found, true, nil
	}
	findJob := func() (factory.JobRecord, bool, error) {
		jobs, err := store.ListJobs()
		if err != nil {
			return factory.JobRecord{}, false, err
		}
		if len(jobs) == 0 {
			return factory.JobRecord{}, false, nil
		}
		if len(jobs) != 1 {
			return factory.JobRecord{}, false, fmt.Errorf("found %d detached jobs, want exactly one", len(jobs))
		}
		job := jobs[0]
		if job.TaskDescription != "test detached" || !sameResolvedTestPath(t, job.TargetPath, target) || job.Type != "implementation" {
			return factory.JobRecord{}, false, fmt.Errorf("unexpected detached job: %+v", job)
		}
		return job, true, nil
	}
	waitForShutdown := func(timeout time.Duration, requiredStatus string) error {
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			job, found, err := findTestJob()
			if err != nil {
				return fmt.Errorf("find detached job: %w", err)
			}
			statusMatches := found && job.Status != "queued" && job.Status != "running" && (requiredStatus == "" || job.Status == requiredStatus)
			if statusMatches {
				if _, err := store.ReadWorker(job.ID); errors.Is(err, os.ErrNotExist) {
					unlock, acquired, err := store.TryLockTarget(target)
					if err != nil {
						return fmt.Errorf("check target lock after shutdown: %w", err)
					}
					if acquired {
						unlock()
						return nil
					}
				} else if err != nil {
					return fmt.Errorf("read worker record after shutdown: %w", err)
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		return fmt.Errorf("detached job did not reach status %q, clear its worker record, and release its target lock within %s", requiredStatus, timeout)
	}
	t.Cleanup(func() {
		if commandDone == nil {
			if removeRoot {
				if err := os.RemoveAll(root); err != nil {
					t.Errorf("remove detached worker test directory: %v", err)
				}
			}
			return
		}
		if err := os.WriteFile(releaseWorker, nil, 0o600); err != nil {
			t.Errorf("release detached worker during cleanup: %v", err)
		}
		if commandDone != nil && !commandExited {
			select {
			case <-commandDone:
				commandExited = true
			case <-time.After(5 * time.Second):
				killErr := command.Process.Kill()
				select {
				case <-commandDone:
					commandExited = true
					if killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
						t.Errorf("kill detached CLI after cleanup timeout: %v", killErr)
					}
				case <-time.After(5 * time.Second):
					if killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
						t.Errorf("kill detached CLI after cleanup timeout: %v", killErr)
					}
					t.Errorf("detached CLI remains unreaped after kill; preserving temporary directory %s", root)
					return
				}
			}
		}
		if initialShutdownErr := waitForShutdown(10*time.Second, ""); initialShutdownErr != nil {
			job, found, findErr := findTestJob()
			if findErr != nil {
				t.Errorf("could not locate detached job after shutdown timeout; preserving temporary directory %s: %v", root, findErr)
				return
			}
			if !found {
				t.Errorf("could not locate detached job after shutdown timeout; preserving temporary directory %s", root)
				return
			}
			currentJob, err := store.GetJob(job.ID)
			if err != nil {
				t.Errorf("could not refresh detached job after shutdown timeout; preserving temporary directory %s: %v", root, err)
				return
			}
			if currentJob.Status == "queued" || currentJob.Status == "running" {
				if err := store.RequestStop(currentJob.ID); err != nil {
					t.Errorf("request cooperative cancellation of detached job; preserving temporary directory %s: %v", root, err)
					return
				}
				if err := waitForShutdown(10*time.Second, ""); err != nil {
					t.Errorf("could not verify worker shutdown after cooperative cancellation; preserving temporary directory %s: %v (initial wait: %v)", root, err, initialShutdownErr)
					return
				}
				if completedJob, found, err := findTestJob(); err == nil && found && completedJob.Status == "complete" && store.StopRequested(completedJob.ID) {
					t.Logf("detached worker completed as a stop request raced with its final status update")
				}
			} else {
				if currentJob.Status == "complete" && store.StopRequested(currentJob.ID) {
					t.Logf("detached worker has a stop marker after reaching complete status")
				}
				if err := waitForShutdown(10*time.Second, currentJob.Status); err != nil {
					t.Errorf("could not verify detached worker shutdown in status %q; preserving temporary directory %s: %v (initial wait: %v)", currentJob.Status, root, err, initialShutdownErr)
					return
				}
			}
		}
		jobs, err := store.ListJobs()
		if err != nil {
			t.Errorf("could not verify detached jobs before cleanup; preserving temporary directory %s: %v", root, err)
			return
		}
		if len(jobs) != 1 || jobs[0].Type != "implementation" || jobs[0].TaskDescription != "test detached" || !sameResolvedTestPath(t, jobs[0].TargetPath, target) {
			t.Errorf("unexpected detached jobs before cleanup; preserving temporary directory %s: got %+v, want exactly one implementation job for task %q at target %q", root, jobs, "test detached", target)
			return
		}
		removeRoot = true
	})

	if err := command.Start(); err != nil {
		t.Fatalf("start detached implement: %v", err)
	}
	removeRoot = false
	commandDone = make(chan error, 1)
	go func() { commandDone <- command.Wait() }()
	select {
	case err := <-commandDone:
		commandExited = true
		if err != nil {
			t.Fatalf("detached implement: %v\n%s", err, commandOutput.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("detached CLI did not return within 10s; cleanup will terminate it if needed; temporary directory %s", root)
	}
	if !strings.Contains(commandOutput.String(), "Started implementation job") || strings.Contains(commandOutput.String(), "requirements completed") {
		t.Fatalf("detached command returned unexpected output: %q", commandOutput.String())
	}
	markerDeadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(workerStarted); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("check fake worker start marker: %v", err)
		}
		if time.Now().After(markerDeadline) {
			t.Fatal("fake worker did not reach its gate within 10s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := os.Stat(releaseWorker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worker gate was unexpectedly released before CLI returned: stat err=%v", err)
	}
	job, found, err := findJob()
	if err != nil || !found || job.Type != "implementation" {
		t.Fatalf("detached CLI job state=%+v found=%t err=%v", job, found, err)
	}
	if job.Status != "running" {
		t.Fatalf("detached CLI returned after fake agent started with job status %q, want running", job.Status)
	}
	unlock, acquired, err := store.TryLockTarget(target)
	if err != nil {
		t.Fatalf("check active target lock: %v", err)
	}
	if acquired {
		unlock()
		t.Fatal("worker gate was reached without the worker holding its target lock")
	}
	runCLI := func(args ...string) string {
		t.Helper()
		inspect := exec.Command(binary, args...)
		inspect.Dir = target
		inspect.Env = command.Env
		output, err := inspect.CombinedOutput()
		if err != nil {
			t.Fatalf("factory %v: %v\n%s", args, err, output)
		}
		return string(output)
	}
	stopOutput := runCLI("job", "stop", job.ID)
	if !strings.Contains(stopOutput, "Stop requested for job "+job.ID) {
		t.Fatalf("separate-process stop output = %q", stopOutput)
	}
	if !store.StopRequested(job.ID) {
		t.Fatal("separate-process stop command did not persist a stop request")
	}
	if err := os.WriteFile(releaseWorker, nil, 0o600); err != nil {
		t.Fatalf("release detached worker after stop request: %v", err)
	}
	if err := waitForShutdown(10*time.Second, "stopped"); err != nil {
		t.Fatal(err)
	}
	job, found, err = findJob()
	if err != nil || !found {
		t.Fatalf("stopped detached job state=%+v found=%t err=%v", job, found, err)
	}
	details := runCLI("job", "get", job.ID, "--details")
	for _, want := range []string{
		"Status: stopped",
		"Worktree: " + job.Worktree,
		"Work branch: " + job.WorkBranch,
	} {
		if !strings.Contains(details, want) {
			t.Errorf("separate-process job details omitted %q: %s", want, details)
		}
	}
	if info, err := os.Stat(job.Worktree); err != nil || !info.IsDir() {
		t.Errorf("stopped implementation worktree was not retained at %q: %v", job.Worktree, err)
	}
	workflowLogPath, err := store.SessionLogPath(job.ID, "workflow")
	if err != nil {
		t.Fatalf("locate retained workflow log: %v", err)
	}
	if info, err := os.Stat(workflowLogPath); err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
		t.Errorf("stopped job workflow log was not retained at %q: %v", workflowLogPath, err)
	}
	logs := runCLI("job", "logs", job.ID, "--session", "workflow")
	if strings.TrimSpace(logs) == "" {
		t.Error("separate-process workflow log was empty after stop")
	}
	if _, err := os.Stat(ghCalled); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("PR publication unexpectedly invoked fake gh: stat err=%v", err)
	}
	if head := strings.TrimSpace(runTestGit(t, "-C", target, "rev-parse", "HEAD")); head != initialHead {
		t.Errorf("stopped job changed target checkout HEAD: got %s, want %s", head, initialHead)
	}
	if status := strings.TrimSpace(runTestGit(t, "-C", target, "status", "--porcelain")); status != "" {
		t.Errorf("stopped job changed target checkout: %s", status)
	}
}

func TestFreshProcessAttachFollowsRunningDetachedImplementation(t *testing.T) {
	root, err := os.MkdirTemp("", "factory detached attach test-")
	if err != nil {
		t.Fatal(err)
	}
	preserveRoot := false
	var attachCommand *exec.Cmd
	var attachDone chan error
	var attachExited bool
	var workerStarted, attachStarted, workerLogPathFile, releaseWorker, ghInvoked string
	var state, target string
	var store *factory.JobStore
	var jobID string
	var workerPID int
	var attachOutput *os.File
	t.Cleanup(func() {
		if releaseWorker != "" {
			if err := os.WriteFile(releaseWorker, nil, 0o600); err != nil {
				t.Errorf("release detached worker during cleanup: %v", err)
				preserveRoot = true
			}
		}
		if attachDone != nil && !attachExited {
			select {
			case <-attachDone:
				attachExited = true
			case <-time.After(15 * time.Second):
				if attachCommand != nil && attachCommand.Process != nil {
					if err := attachCommand.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
						t.Errorf("kill attached CLI during cleanup: %v", err)
					}
				}
				select {
				case <-attachDone:
					attachExited = true
				case <-time.After(5 * time.Second):
					t.Errorf("attached CLI remains unreaped; preserving temporary directory %s", root)
					preserveRoot = true
				}
			}
		}
		if attachOutput != nil {
			if err := attachOutput.Close(); err != nil {
				t.Errorf("close attach output: %v", err)
			}
		}
		if store != nil && jobID != "" {
			shutdownErr := waitFailedImplementTestShutdown(store, target, jobID, 15*time.Second)
			if shutdownErr != nil {
				job, err := store.GetJob(jobID)
				if err == nil && (job.Status == "queued" || job.Status == "running") {
					if err := store.RequestStop(jobID); err != nil {
						shutdownErr = errors.Join(shutdownErr, fmt.Errorf("request cooperative worker stop: %w", err))
					} else {
						shutdownErr = waitFailedImplementTestShutdown(store, target, jobID, 15*time.Second)
					}
				} else if err != nil {
					shutdownErr = errors.Join(shutdownErr, fmt.Errorf("read job before cooperative stop: %w", err))
				}
			}
			if shutdownErr != nil {
				t.Errorf("could not verify detached worker shutdown; preserving temporary directory %s: %v", root, shutdownErr)
				preserveRoot = true
			} else if workerPID > 0 {
				if err := waitForTestProcessExit(workerPID, 10*time.Second); err != nil {
					t.Errorf("worker PID %d did not exit; preserving temporary directory %s: %v", workerPID, root, err)
					preserveRoot = true
				}
			}
		}
		if preserveRoot {
			t.Errorf("preserving detached attach test diagnostics: %s (job %s, started marker %s, release marker %s, provider tripwire %s)", root, jobID, workerStarted, releaseWorker, ghInvoked)
			return
		}
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove detached attach test directory: %v", err)
		}
	})

	state = filepath.Join(root, "state")
	configHome := filepath.Join(root, "config")
	configDir := filepath.Join(configHome, "factory")
	fakeBin := filepath.Join(root, "bin")
	for _, dir := range []string{state, configDir, fakeBin} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	releaseWorker = filepath.Join(state, "release-implement")
	workerStarted = filepath.Join(state, "implement-started")
	attachStarted = filepath.Join(state, "attach-started")
	workerLogPathFile = filepath.Join(state, "worker-log-path")
	ghInvoked = filepath.Join(state, "gh-invoked")
	agent := filepath.Join(root, "fake-agent")
	script := `#!/bin/sh
case "$1" in
  implement) printf 'ATTACH_STAGE_LOG_ONLY_MARKER\n'; : > "$FACTORY_TEST_STARTED"; while [ ! -f "$FACTORY_TEST_WORKER_LOG_PATH_FILE" ]; do sleep 0.02; done; worker_log=$(cat "$FACTORY_TEST_WORKER_LOG_PATH_FILE"); printf 'ATTACH_PREEXISTING_WORKER_EVENT\n' >> "$worker_log"; while [ ! -f "$FACTORY_TEST_ATTACH_STARTED" ]; do sleep 0.02; done; printf 'ATTACH_WORKER_LOG_EVENT\n' >> "$worker_log"; while [ ! -f "$FACTORY_TEST_RELEASE" ]; do sleep 0.02; done; printf 'ATTACH_COMPLETION_MARKER\nPASS\n' ;;
  *) printf 'PASS\n' ;;
esac
`
	if err := os.WriteFile(agent, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`{"command":%q,"args":["{stage}","{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"30s","auto_publish":false}`, agent, state)
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fakeBin, "gh"), []byte(`#!/bin/sh
printf invoked > "$FACTORY_TEST_GH_INVOKED"
exit 1
`), 0o700); err != nil {
		t.Fatal(err)
	}

	target = filepath.Join(root, "target")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main", target}, {"-C", target, "config", "user.name", "Factory Test"}, {"-C", target, "config", "user.email", "factory-test@example.invalid"}} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(target, "tracked.txt"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("git", "-C", target, "add", "tracked.txt").CombinedOutput(); err != nil {
		t.Fatalf("git add: %v\n%s", err, output)
	}
	if output, err := exec.Command("git", "-C", target, "commit", "-qm", "baseline").CombinedOutput(); err != nil {
		t.Fatalf("git commit: %v\n%s", err, output)
	}
	initialHead := strings.TrimSpace(runTestGit(t, "-C", target, "rev-parse", "HEAD"))
	initialStatus := runTestGit(t, "-C", target, "status", "--porcelain")
	target, err = filepath.EvalSymlinks(target)
	if err != nil {
		t.Fatal(err)
	}

	binary := filepath.Join(root, "factory")
	projectRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/factory")
	build.Dir = projectRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build factory: %v\n%s", err, output)
	}
	env := append(os.Environ(),
		"XDG_CONFIG_HOME="+configHome,
		"XDG_STATE_HOME="+state,
		"FACTORY_TEST_STARTED="+workerStarted,
		"FACTORY_TEST_ATTACH_STARTED="+attachStarted,
		"FACTORY_TEST_WORKER_LOG_PATH_FILE="+workerLogPathFile,
		"FACTORY_TEST_RELEASE="+releaseWorker,
		"FACTORY_TEST_GH_INVOKED="+ghInvoked,
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	storeRoot, err := factory.JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err = factory.NewJobStore(storeRoot)
	if err != nil {
		t.Fatal(err)
	}

	start := exec.Command(binary, "implement", "--detach", "attach", "process", "test")
	start.Dir, start.Env = target, env
	startOutput, err := start.CombinedOutput()
	if err != nil || !strings.Contains(string(startOutput), "Started implementation job") {
		t.Fatalf("start detached implementation: %v\n%s", err, startOutput)
	}
	preserveRoot = true
	fields := strings.Fields(string(startOutput))
	if len(fields) < 4 {
		t.Fatalf("unexpected detached start output: %q", startOutput)
	}
	jobID = strings.TrimSuffix(fields[3], ".")
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(workerStarted); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("check implementation start marker: %v", err)
		}
		if time.Now().After(deadline) {
			job, getErr := store.GetJob(jobID)
			workerLog, logErr := os.ReadFile(filepath.Join(storeRoot, jobID, "worker.log"))
			t.Fatalf("fake implementation agent did not reach its gate: job=%+v jobErr=%v workerLog=%q logErr=%v", job, getErr, workerLog, logErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	job, err := store.GetJob(jobID)
	if err != nil || job.Status != "running" {
		t.Fatalf("detached job at implementation gate = %+v, err=%v", job, err)
	}
	worker, err := store.ReadWorker(jobID)
	if err != nil || worker.PID <= 0 {
		t.Fatalf("read detached worker record: %+v, err=%v", worker, err)
	}
	workerPID = worker.PID

	jobLogPath, err := store.JobLogPath(jobID)
	if err != nil {
		t.Fatalf("locate detached worker log: %v", err)
	}
	if err := os.WriteFile(workerLogPathFile, []byte(jobLogPath), 0o600); err != nil {
		t.Fatalf("provide fake agent worker log path: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		jobLog, err := os.ReadFile(jobLogPath)
		if err != nil {
			t.Fatalf("read pre-attach worker log: %v", err)
		}
		if strings.Contains(string(jobLog), "ATTACH_PREEXISTING_WORKER_EVENT\n") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("already-running fake worker did not append its pre-attach event: %q", jobLog)
		}
		time.Sleep(20 * time.Millisecond)
	}

	attachPath := filepath.Join(root, "attach-output.log")
	attachOutput, err = os.Create(attachPath)
	if err != nil {
		t.Fatal(err)
	}
	attachCommand = exec.Command(binary, "job", "attach", jobID)
	attachCommand.Dir, attachCommand.Env = target, env
	attachCommand.Stdout, attachCommand.Stderr = attachOutput, attachOutput
	if err := attachCommand.Start(); err != nil {
		t.Fatalf("start fresh-process attach: %v", err)
	}
	attachDone = make(chan error, 1)
	go func() { attachDone <- attachCommand.Wait() }()

	deadline = time.Now().Add(10 * time.Second)
	for {
		output, err := os.ReadFile(attachPath)
		if err != nil {
			t.Fatalf("read pre-existing attach output: %v", err)
		}
		if strings.Contains(string(output), "ATTACH_PREEXISTING_WORKER_EVENT\n") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fresh attach did not follow the already-running worker event: attach=%q", output)
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case err := <-attachDone:
		attachExited = true
		t.Fatalf("fresh attach exited while the detached implementation was still gated: %v", err)
	default:
	}
	if err := os.WriteFile(attachStarted, nil, 0o600); err != nil {
		t.Fatalf("signal fresh-process attach is following worker output: %v", err)
	}

	deadline = time.Now().Add(10 * time.Second)
	for {
		output, err := os.ReadFile(attachPath)
		if err != nil {
			t.Fatalf("read incremental attach output: %v", err)
		}
		if strings.Contains(string(output), "ATTACH_WORKER_LOG_EVENT\n") {
			if strings.Contains(string(output), "implement completed") {
				t.Fatalf("attach observed implementation completion before the fake worker was released: %q", output)
			}
			break
		}
		if time.Now().After(deadline) {
			jobLog, logErr := os.ReadFile(jobLogPath)
			t.Fatalf("fresh attach did not receive the exact worker-log event before release: attach=%q workerLog=%q logErr=%v", output, jobLog, logErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case err := <-attachDone:
		attachExited = true
		t.Fatalf("fresh attach exited while the detached implementation was still gated: %v", err)
	default:
	}
	if _, err := os.Stat(releaseWorker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worker was released before incremental output was observed: %v", err)
	}
	if err := os.WriteFile(releaseWorker, nil, 0o600); err != nil {
		t.Fatalf("release fake implementation agent: %v", err)
	}
	select {
	case err := <-attachDone:
		attachExited = true
		if err != nil {
			t.Fatalf("fresh-process attach: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("fresh-process attach did not exit after implementation completion")
	}
	if err := attachOutput.Close(); err != nil {
		t.Fatalf("close attach output: %v", err)
	}
	attachOutput = nil
	transcript, err := os.ReadFile(attachPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(transcript), "ATTACH_WORKER_LOG_EVENT\n") || !strings.Contains(string(transcript), "implement completed") {
		t.Fatalf("attach transcript did not follow the worker-log event through completion: %s", transcript)
	}
	implementationLogs, err := filepath.Glob(filepath.Join(state, "runs", "*", "02-implement.log"))
	if err != nil || len(implementationLogs) != 1 {
		t.Fatalf("implementation stage logs = %v, err=%v; want exactly one", implementationLogs, err)
	}
	implementationLog, err := os.ReadFile(implementationLogs[0])
	if err != nil || !strings.Contains(string(implementationLog), "ATTACH_STAGE_LOG_ONLY_MARKER") || !strings.Contains(string(implementationLog), "ATTACH_COMPLETION_MARKER") {
		t.Fatalf("fake agent stage-log markers were not recorded in its stage log: %q err=%v", implementationLog, err)
	}
	if err := waitFailedImplementTestShutdown(store, target, jobID, 15*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := waitForTestProcessExit(workerPID, 10*time.Second); err != nil {
		t.Fatalf("detached worker PID %d did not exit: %v", workerPID, err)
	}

	inspect := exec.Command(binary, "job", "get", jobID, "--details")
	inspect.Dir, inspect.Env = target, env
	details, err := inspect.CombinedOutput()
	if err != nil {
		t.Fatalf("fresh-process job get --details: %v\n%s", err, details)
	}
	for _, want := range []string{"Status: complete", "Description: attach process test", "Job log path:"} {
		if !strings.Contains(string(details), want) {
			t.Errorf("fresh-process details omitted %q: %s", want, details)
		}
	}
	if _, err := os.Stat(ghInvoked); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("provider tripwire was invoked despite publication being disabled: %v", err)
	}
	if head := strings.TrimSpace(runTestGit(t, "-C", target, "rev-parse", "HEAD")); head != initialHead {
		t.Errorf("detached implementation changed target checkout HEAD: got %s, want %s", head, initialHead)
	}
	if status := runTestGit(t, "-C", target, "status", "--porcelain"); status != initialStatus {
		t.Errorf("detached implementation changed target checkout status: got %q, want %q", status, initialStatus)
	}
	preserveRoot = false
}

func TestDetachedImplementCrashIsInspectableAndNotReplayed(t *testing.T) {
	root, err := os.MkdirTemp("", "factory detached crash test-")
	if err != nil {
		t.Fatal(err)
	}
	preserveRoot := true
	t.Cleanup(func() {
		if preserveRoot {
			t.Errorf("preserving detached crash test evidence: %s", root)
			return
		}
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove detached crash test directory: %v", err)
		}
	})

	state := filepath.Join(root, "state")
	configHome := filepath.Join(root, "config")
	configDir := filepath.Join(configHome, "factory")
	fakeBin := filepath.Join(root, "bin")
	for _, dir := range []string{state, configDir, fakeBin} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	gate := filepath.Join(state, "release-agent")
	abort := filepath.Join(state, "abort-agent")
	started := filepath.Join(state, "agent-started")
	agentExited := filepath.Join(state, "agent-exited")
	agentPID := filepath.Join(state, "agent-pid")
	invocations := filepath.Join(state, "implement-invocations")
	ghInvoked := filepath.Join(state, "gh-invoked")
	agent := filepath.Join(root, "agent")
	script := "#!/bin/sh\ncase \"$1\" in\n  slug) printf 'crash-recovery\\n'; exit 0 ;;\n  implement) printf 'called\\n' >> \"$FACTORY_TEST_INVOCATIONS\"; printf '%s\\n' \"$$\" > \"$FACTORY_TEST_AGENT_PID\"; printf 'crash artifact\\n' > generated.txt; : > \"$FACTORY_TEST_STARTED\"; while [ ! -f \"$FACTORY_TEST_GATE\" ]; do if [ -f \"$FACTORY_TEST_ABORT\" ]; then exit 23; fi; sleep 0.02; done; printf 'PASS\\n'; : > \"$FACTORY_TEST_AGENT_EXITED\" ;;\n  *) printf 'PASS\\n' ;;\nesac\n"
	if err := os.WriteFile(agent, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`{"command":%q,"args":["{stage}","{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"2m","auto_publish":true}`, agent, state)
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fakeBin, "gh"), []byte("#!/bin/sh\nprintf invoked > \"$FACTORY_TEST_GH_INVOKED\"\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(root, "target")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main", target}, {"-C", target, "config", "user.name", "Factory Test"}, {"-C", target, "config", "user.email", "factory-test@example.invalid"}} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(target, "tracked.txt"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("git", "-C", target, "add", "tracked.txt").CombinedOutput(); err != nil {
		t.Fatalf("git add: %v\n%s", err, output)
	}
	if output, err := exec.Command("git", "-C", target, "commit", "-qm", "baseline").CombinedOutput(); err != nil {
		t.Fatalf("git commit: %v\n%s", err, output)
	}
	initialHead := strings.TrimSpace(runTestGit(t, "-C", target, "rev-parse", "HEAD"))
	initialStatus := runTestGit(t, "-C", target, "status", "--porcelain")
	target, err = filepath.EvalSymlinks(target)
	if err != nil {
		t.Fatal(err)
	}

	binary := filepath.Join(root, "factory")
	projectRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/factory")
	build.Dir = projectRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build factory: %v\n%s", err, output)
	}
	env := append(os.Environ(),
		"XDG_CONFIG_HOME="+configHome,
		"XDG_STATE_HOME="+state,
		"FACTORY_TEST_GATE="+gate,
		"FACTORY_TEST_ABORT="+abort,
		"FACTORY_TEST_STARTED="+started,
		"FACTORY_TEST_AGENT_EXITED="+agentExited,
		"FACTORY_TEST_AGENT_PID="+agentPID,
		"FACTORY_TEST_INVOCATIONS="+invocations,
		"FACTORY_TEST_GH_INVOKED="+ghInvoked,
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	storeRoot, err := factory.JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := factory.NewJobStore(storeRoot)
	if err != nil {
		t.Fatal(err)
	}

	start := exec.Command(binary, "implement", "--detach", "test", "crash", "recovery")
	start.Dir, start.Env = target, env
	output, err := start.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "Started implementation job") {
		t.Fatalf("start detached implementation: %v\n%s", err, output)
	}
	var job factory.JobRecord
	var worker factory.WorkerRecord
	shutdownVerified := false
	defer func() {
		if shutdownVerified {
			return
		}
		_ = os.WriteFile(abort, nil, 0o600)
		_ = os.WriteFile(gate, nil, 0o600)
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if job.ID == "" {
				jobs, listErr := store.ListJobs()
				if listErr == nil {
					for _, candidate := range jobs {
						if candidate.TaskDescription == "test crash recovery" {
							job = candidate
							break
						}
					}
				}
			}
			if job.ID != "" {
				current, getErr := store.GetJob(job.ID)
				if getErr == nil && current.Status != "queued" && current.Status != "running" {
					_, workerErr := store.ReadWorker(job.ID)
					unlock, acquired, lockErr := store.TryLockTarget(target)
					if acquired {
						unlock()
					}
					processExited := worker.PID == 0 || waitForTestProcessExit(worker.PID, 100*time.Millisecond) == nil
					if errors.Is(workerErr, os.ErrNotExist) && lockErr == nil && acquired && processExited {
						shutdownVerified = true
						preserveRoot = false
						return
					}
				}
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Errorf("worker shutdown could not be verified; preserving test evidence: %s", root)
	}()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		jobs, err := store.ListJobs()
		if err != nil {
			t.Fatal(err)
		}
		for _, candidate := range jobs {
			if candidate.TaskDescription == "test crash recovery" {
				job = candidate
			}
		}
		if job.ID != "" && job.Status == "running" {
			if _, err := os.Stat(started); err == nil {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if job.ID == "" || job.Status != "running" || job.Worktree == "" || job.WorkBranch == "" {
		t.Fatalf("detached job did not start with its recovery worktree: %+v", job)
	}
	worker, err = store.ReadWorker(job.ID)
	if err != nil || worker.ID != job.ID || worker.PID <= 0 || time.Since(worker.HeartbeatAt) > time.Minute {
		t.Fatalf("live worker record = %+v, err=%v", worker, err)
	}
	if err := waitForTestProcessLive(worker.PID, 5*time.Second); err != nil {
		t.Fatalf("worker record PID %d is not live: %v", worker.PID, err)
	}
	workerProcess, err := os.FindProcess(worker.PID)
	if err != nil {
		t.Fatalf("find worker process %d: %v", worker.PID, err)
	}
	if err := workerProcess.Kill(); err != nil {
		t.Fatalf("kill only worker PID %d: %v", worker.PID, err)
	}
	if err := waitForTestProcessExit(worker.PID, 10*time.Second); err != nil {
		t.Fatalf("observe killed worker termination: %v", err)
	}
	// Let the already-started fake agent exit normally; do not kill the test CLI
	// harness or any process group as part of simulating the worker crash.
	worker, err = store.ReadWorker(job.ID)
	if err != nil || worker.PID <= 0 || time.Since(worker.HeartbeatAt) > time.Minute {
		t.Fatalf("worker record after process exit = %+v, err=%v", worker, err)
	}
	if err := os.WriteFile(gate, nil, 0o600); err != nil {
		t.Fatalf("release fake agent after worker termination: %v", err)
	}
	if err := waitForTestFile(t, agentExited, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	pidData, err := os.ReadFile(agentPID)
	if err != nil {
		t.Fatalf("read fake agent PID: %v", err)
	}
	fakeAgentPID, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
	if err != nil {
		t.Fatalf("parse fake agent PID %q: %v", pidData, err)
	}
	if err := waitForTestProcessExit(fakeAgentPID, 5*time.Second); err != nil {
		t.Fatalf("fake agent process was not reaped: %v", err)
	}
	if got, err := os.ReadFile(invocations); err != nil || strings.Count(string(got), "called\n") != 1 {
		t.Fatalf("implementation invocation marker after crash = %q, err=%v; want exactly one invocation", got, err)
	}

	lockDeadline := time.Now().Add(10 * time.Second)
	for {
		unlock, acquired, err := store.TryLockTarget(target)
		if err != nil {
			t.Fatalf("poll target lock release: %v", err)
		}
		if acquired {
			unlock()
			break
		}
		if time.Now().After(lockDeadline) {
			t.Fatal("worker termination did not release target lock")
		}
		time.Sleep(20 * time.Millisecond)
	}
	job, err = store.GetJob(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != "running" {
		t.Fatalf("crashed job before orphan grace period = %q, want running", job.Status)
	}
	workflow, err := store.GetSession(job.ID, "workflow")
	if err != nil || workflow.Status != "running" {
		t.Fatalf("workflow session before orphan grace period = %+v, err=%v", workflow, err)
	}
	preGraceInspect := exec.Command(binary, "job", "get", job.ID, "--details")
	preGraceInspect.Dir, preGraceInspect.Env = target, env
	preGraceDetails, err := preGraceInspect.CombinedOutput()
	if err != nil || !strings.Contains(string(preGraceDetails), "Status: running") {
		t.Fatalf("pre-grace job get details = %v\n%s; want running", err, preGraceDetails)
	}
	job, err = store.GetJob(job.ID)
	if err != nil || job.Status != "running" {
		t.Fatalf("job get reconciled before stale activity grace period: job=%+v err=%v", job, err)
	}
	lastActivity := job.UpdatedAt
	if worker.HeartbeatAt.After(lastActivity) {
		lastActivity = worker.HeartbeatAt
	}
	graceDeadline := lastActivity.Add(30*time.Second + 100*time.Millisecond)
	if delay := time.Until(graceDeadline); delay > 0 {
		time.Sleep(delay)
	}

	inspect := exec.Command(binary, "job", "get", job.ID, "--details")
	inspect.Dir, inspect.Env = target, env
	details, err := inspect.CombinedOutput()
	if err != nil {
		t.Fatalf("post-crash job get --details: %v\n%s", err, details)
	}
	job, err = store.GetJob(job.ID)
	if err != nil || job.Status != "interrupted" {
		t.Fatalf("reconciled crash job = %+v, err=%v; want interrupted", job, err)
	}
	workflow, err = store.GetSession(job.ID, "workflow")
	if err != nil || workflow.Status != "interrupted" {
		t.Fatalf("reconciled workflow session = %+v, err=%v; want interrupted", workflow, err)
	}
	if !strings.Contains(string(details), "Status: interrupted") || !strings.Contains(string(details), "Session: workflow (interrupted)") {
		t.Fatalf("job details did not consistently report interrupted lifecycle: %s", details)
	}
	if _, err := store.ReadWorker(job.ID); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("worker record after recovery = %v, want removed", err)
	}
	if got, err := os.ReadFile(invocations); err != nil || strings.Count(string(got), "called\n") != 1 {
		t.Errorf("implementation was replayed after recovery: invocation marker=%q err=%v", got, err)
	}
	if info, err := os.Stat(job.Worktree); err != nil || !info.IsDir() {
		t.Errorf("crash recovery worktree not retained at %q: %v", job.Worktree, err)
	}
	if _, err := exec.Command("git", "-C", target, "show-ref", "--verify", "refs/heads/"+job.WorkBranch).CombinedOutput(); err != nil {
		t.Errorf("crash recovery branch %q not retained: %v", job.WorkBranch, err)
	}
	if got, err := os.ReadFile(filepath.Join(job.Worktree, "generated.txt")); err != nil || string(got) != "crash artifact\n" {
		t.Errorf("crash artifact not retained: %q err=%v", got, err)
	}
	workflowLog, err := store.SessionLogPath(job.ID, "workflow")
	if err != nil {
		t.Fatalf("locate workflow log: %v", err)
	}
	if info, err := os.Stat(workflowLog); err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
		t.Errorf("crash workflow log not retained at %q: %v", workflowLog, err)
	}
	logs := exec.Command(binary, "job", "logs", job.ID, "--session", "workflow")
	logs.Dir, logs.Env = target, env
	logOutput, err := logs.CombinedOutput()
	if err != nil || strings.TrimSpace(string(logOutput)) == "" {
		t.Errorf("crash workflow logs not inspectable: err=%v output=%s", err, logOutput)
	}
	if _, err := os.Stat(ghInvoked); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("crashed job invoked fake gh: %v", err)
	}
	if got := strings.TrimSpace(runTestGit(t, "-C", target, "rev-parse", "HEAD")); got != initialHead {
		t.Errorf("crashed job changed target HEAD: got %s, want %s", got, initialHead)
	}
	if got := runTestGit(t, "-C", target, "status", "--porcelain"); got != initialStatus {
		t.Errorf("crashed job changed target worktree: got %q, want %q", got, initialStatus)
	}
	if err := waitForTestProcessExit(worker.PID, 10*time.Second); err != nil {
		t.Fatalf("worker did not remain terminated: %v", err)
	}
	shutdownVerified = true
	preserveRoot = false
}

func waitForTestFile(t *testing.T, path string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat %s: %w", path, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("file %s did not appear within %s", path, timeout)
}

func waitForTestProcessLive(pid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		output, err := exec.Command("ps", "-o", "stat=", "-p", fmt.Sprint(pid)).CombinedOutput()
		if err == nil {
			state := strings.TrimSpace(string(output))
			if state != "" && !strings.HasPrefix(state, "Z") {
				return nil
			}
		} else if len(bytes.TrimSpace(output)) != 0 {
			return fmt.Errorf("inspect PID %d: %w: %s", pid, err, output)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("PID %d was not observed live within %s", pid, timeout)
}

func waitForTestProcessExit(pid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		output, err := exec.Command("ps", "-o", "stat=", "-p", fmt.Sprint(pid)).CombinedOutput()
		if err != nil {
			if len(bytes.TrimSpace(output)) == 0 {
				return nil
			}
			return fmt.Errorf("inspect PID %d: %w: %s", pid, err, output)
		}
		state := strings.TrimSpace(string(output))
		if state == "" || strings.HasPrefix(state, "Z") {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("PID %d remained live after %s", pid, timeout)
}

func TestDetachedImplementFailureIsInspectableAndRetainsRecoveryWorktree(t *testing.T) {
	root, err := os.MkdirTemp("", "factory detached failure test-")
	if err != nil {
		t.Fatal(err)
	}
	removeRoot := true
	t.Cleanup(func() {
		if !removeRoot {
			t.Errorf("preserving detached failure test directory because worker shutdown could not be verified: %s", root)
			return
		}
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove detached failure test directory: %v", err)
		}
	})

	state := filepath.Join(root, "state")
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	releaseWorker := filepath.Join(state, "release-worker")
	workerStarted := filepath.Join(state, "worker-started")
	ghInvoked := filepath.Join(state, "gh-invoked")
	configHome := filepath.Join(root, "config")
	factoryConfig := filepath.Join(configHome, "factory")
	if err := os.MkdirAll(factoryConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "agent")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ncase \"$1\" in\n  slug) printf 'failure-test\\n'; exit 0 ;;\n  requirements) printf 'PASS\\n'; exit 0 ;;\n  implement) : > \"$FACTORY_TEST_STARTED\"; while [ ! -f \"$FACTORY_TEST_RELEASE\" ]; do sleep 0.02; done; printf 'simulated implementation failure\\n' >&2; printf 'retained recovery artifact\\n' > generated.txt; exit 23 ;;\nesac\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(factoryConfig, "config.json"), []byte(fmt.Sprintf(`{"command":%q,"args":["{stage}","{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"30s","auto_publish":true}`, script, state)), 0o600); err != nil {
		t.Fatal(err)
	}
	fakeBin := filepath.Join(root, "bin")
	if err := os.MkdirAll(fakeBin, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fakeBin, "gh"), []byte("#!/bin/sh\nprintf invoked > \"$FACTORY_TEST_GH_INVOKED\"\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "factory")
	projectRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/factory")
	build.Dir = projectRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build factory: %v\n%s", err, output)
	}

	target := filepath.Join(root, "target")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main", target}, {"-C", target, "config", "user.name", "Factory Test"}, {"-C", target, "config", "user.email", "factory-test@example.invalid"}} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(target, "tracked.txt"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("git", "-C", target, "add", "tracked.txt").CombinedOutput(); err != nil {
		t.Fatalf("git add: %v\n%s", err, output)
	}
	if output, err := exec.Command("git", "-C", target, "commit", "-qm", "baseline").CombinedOutput(); err != nil {
		t.Fatalf("git commit: %v\n%s", err, output)
	}
	initialHead := strings.TrimSpace(runTestGit(t, "-C", target, "rev-parse", "HEAD"))
	remote := filepath.Join(root, "origin.git")
	if output, err := exec.Command("git", "init", "--bare", remote).CombinedOutput(); err != nil {
		t.Fatalf("initialize origin: %v\n%s", err, output)
	}
	if output, err := exec.Command("git", "-C", target, "remote", "add", "origin", remote).CombinedOutput(); err != nil {
		t.Fatalf("git remote add: %v\n%s", err, output)
	}
	target, err = filepath.EvalSymlinks(target)
	if err != nil {
		t.Fatal(err)
	}
	storeRoot, err := factory.JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := factory.NewJobStore(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(),
		"XDG_CONFIG_HOME="+configHome,
		"XDG_STATE_HOME="+state,
		"FACTORY_TEST_RELEASE="+releaseWorker,
		"FACTORY_TEST_STARTED="+workerStarted,
		"FACTORY_TEST_GH_INVOKED="+ghInvoked,
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	command := exec.Command(binary, "implement", "--detach", "test", "detached", "failure")
	command.Dir = target
	command.Env = env
	var commandOutput bytes.Buffer
	command.Stdout, command.Stderr = &commandOutput, &commandOutput
	if err := command.Start(); err != nil {
		t.Fatalf("start detached implement: %v", err)
	}
	removeRoot = false
	commandDone := make(chan error, 1)
	go func() { commandDone <- command.Wait() }()
	commandExited := false
	defer func() {
		_ = os.WriteFile(releaseWorker, nil, 0o600)
		if !commandExited {
			select {
			case <-commandDone:
				commandExited = true
			case <-time.After(5 * time.Second):
				if err := command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
					t.Errorf("kill detached CLI during cleanup: %v", err)
				}
				select {
				case <-commandDone:
					commandExited = true
				case <-time.After(5 * time.Second):
					t.Errorf("detached CLI remains unreaped; preserving temporary directory %s", root)
					return
				}
			}
		}
		job, found, err := findFailedImplementTestJob(t, store, target)
		if err != nil || !found {
			t.Errorf("could not locate detached job to verify worker shutdown; preserving temporary directory %s: job=%+v found=%t err=%v", root, job, found, err)
			return
		}
		if job.Status == "queued" || job.Status == "running" {
			if err := store.RequestStop(job.ID); err != nil {
				t.Errorf("request detached worker cancellation; preserving temporary directory %s: %v", root, err)
				return
			}
		}
		if err := waitFailedImplementTestShutdown(store, target, job.ID, 10*time.Second); err != nil {
			t.Errorf("worker shutdown could not be verified; preserving temporary directory %s: %v", root, err)
			return
		}
		removeRoot = true
	}()
	select {
	case err := <-commandDone:
		commandExited = true
		if err != nil {
			t.Fatalf("detached implement: %v\n%s", err, commandOutput.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("detached CLI did not return within 10s: %s", commandOutput.String())
	}
	if !strings.Contains(commandOutput.String(), "Started implementation job") {
		t.Fatalf("detached command did not report job start: %q", commandOutput.String())
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(workerStarted); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("check fake worker start marker: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("fake worker did not reach its gate after detached CLI exited")
		}
		time.Sleep(20 * time.Millisecond)
	}
	job, found, err := findFailedImplementTestJob(t, store, target)
	if err != nil || !found {
		t.Fatalf("find detached failure job: job=%+v found=%t err=%v", job, found, err)
	}
	if job.Status != "running" {
		t.Fatalf("job status at fake worker gate = %q, want running", job.Status)
	}
	if err := os.WriteFile(releaseWorker, nil, 0o600); err != nil {
		t.Fatalf("release fake worker: %v", err)
	}
	if err := waitFailedImplementTestShutdown(store, target, job.ID, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	job, err = store.GetJob(job.ID)
	if err != nil || job.Status != "failed" {
		t.Fatalf("terminal job = %+v, err=%v; want failed", job, err)
	}

	inspect := exec.Command(binary, "job", "get", job.ID, "--details")
	inspect.Dir, inspect.Env = target, env
	details, err := inspect.CombinedOutput()
	if err != nil {
		t.Fatalf("factory job get --details: %v\n%s", err, details)
	}
	for _, want := range []string{"Status: failed", "Worktree: " + job.Worktree, "Work branch: " + job.WorkBranch, "Job log path:"} {
		if !strings.Contains(string(details), want) {
			t.Errorf("separate-process job details omitted %q: %s", want, details)
		}
	}
	if job.Worktree == "" || job.WorkBranch == "" {
		t.Fatalf("failed job did not retain worktree/branch metadata: %+v", job)
	}
	if info, err := os.Stat(job.Worktree); err != nil || !info.IsDir() {
		t.Errorf("failed job worktree was not retained at %q: %v", job.Worktree, err)
	}
	if _, err := exec.Command("git", "-C", target, "show-ref", "--verify", "refs/heads/"+job.WorkBranch).CombinedOutput(); err != nil {
		t.Errorf("failed job branch %q was not retained: %v", job.WorkBranch, err)
	}
	if got := strings.TrimSpace(runTestGit(t, "-C", target, "rev-parse", "HEAD")); got != initialHead {
		t.Errorf("failed implementation changed target HEAD: got %s want %s", got, initialHead)
	}
	if status := strings.TrimSpace(runTestGit(t, "-C", target, "status", "--porcelain")); status != "" {
		t.Errorf("failed implementation changed target files: %s", status)
	}
	if got, err := os.ReadFile(filepath.Join(job.Worktree, "generated.txt")); err != nil || string(got) != "retained recovery artifact\n" {
		t.Errorf("failed implementation artifact not retained: %q err=%v", got, err)
	}
	if job.PublicationStatus == "published" || strings.Contains(string(details), "Publication: published") || strings.Contains(string(details), "Published implementation PR") {
		t.Errorf("failed job claimed successful publication: job=%+v details=%s", job, details)
	}
	if _, err := os.Stat(ghInvoked); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("fake gh was invoked for failed workflow: stat err=%v", err)
	}
	if refs := strings.TrimSpace(runTestGit(t, "--git-dir", remote, "for-each-ref", "--format=%(refname:short)")); refs != "" {
		t.Errorf("failed workflow published remote refs: %s", refs)
	}
	logs := exec.Command(binary, "job", "logs", job.ID, "--session", "workflow")
	logs.Dir, logs.Env = target, env
	logOutput, err := logs.CombinedOutput()
	if err != nil || !strings.Contains(string(logOutput), "stage.failed") || !strings.Contains(string(logOutput), "stage=implement") {
		t.Errorf("failed workflow logs are not inspectable: err=%v output=%s", err, logOutput)
	}
	if _, err := store.ReadWorker(job.ID); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("terminal failed worker record was not cleared: %v", err)
	}
	unlock, acquired, err := store.TryLockTarget(target)
	if err != nil {
		t.Fatalf("check target lock after failed worker: %v", err)
	}
	if !acquired {
		t.Fatal("terminal failed worker did not release target lock")
	}
	unlock()
}

func findFailedImplementTestJob(t *testing.T, store *factory.JobStore, target string) (factory.JobRecord, bool, error) {
	t.Helper()
	jobs, err := store.ListJobs()
	if err != nil {
		return factory.JobRecord{}, false, err
	}
	var found factory.JobRecord
	for _, job := range jobs {
		if job.TaskDescription != "test detached failure" || !sameResolvedTestPath(t, job.TargetPath, target) {
			continue
		}
		if found.ID != "" {
			return factory.JobRecord{}, false, fmt.Errorf("multiple detached failure jobs found: %s and %s", found.ID, job.ID)
		}
		found = job
	}
	return found, found.ID != "", nil
}

func waitFailedImplementTestShutdown(store *factory.JobStore, target, id string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := store.GetJob(id)
		if err != nil {
			return fmt.Errorf("read job during worker shutdown: %w", err)
		}
		if job.Status != "queued" && job.Status != "running" {
			if _, err := store.ReadWorker(id); errors.Is(err, os.ErrNotExist) {
				unlock, acquired, err := store.TryLockTarget(target)
				if err != nil {
					return fmt.Errorf("check target lock after worker shutdown: %w", err)
				}
				if acquired {
					unlock()
					return nil
				}
			} else if err != nil {
				return fmt.Errorf("read worker record after shutdown: %w", err)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("detached worker %s did not reach terminal status and clear its worker record/target lock within %s", id, timeout)
}

func TestDetachedTidyCLIUsesDefaultDescriptionAndNeverPublishes(t *testing.T) {
	state := t.TempDir()
	configHome := t.TempDir()
	factoryConfig := filepath.Join(configHome, "factory")
	if err := os.MkdirAll(factoryConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	agent := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\nprintf 'PASS\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(factoryConfig, "config.json"), []byte(fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"agent_timeout":"5s"}`, agent, state)), 0o600); err != nil {
		t.Fatal(err)
	}
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

	target := t.TempDir()
	git := func(args ...string) []byte {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = target
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
		return output
	}
	git("init", "-q")
	git("config", "user.name", "Factory Test")
	git("config", "user.email", "factory-test@example.invalid")
	if err := os.WriteFile(filepath.Join(target, "tracked.txt"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", "tracked.txt")
	git("commit", "-qm", "baseline")
	initialHead := strings.TrimSpace(string(git("rev-parse", "HEAD")))
	if err := os.WriteFile(filepath.Join(target, "preexisting.txt"), []byte("keep me\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fakeBin := t.TempDir()
	if err := os.WriteFile(filepath.Join(fakeBin, "make"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(binary, "tidy", "--detach")
	command.Dir = target
	command.Env = append(os.Environ(), "XDG_CONFIG_HOME="+configHome, "XDG_STATE_HOME="+state, "PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("detached tidy CLI: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "Started tidy job ") || !strings.Contains(string(output), "remain unpublished") {
		t.Fatalf("CLI did not report tidy dispatch and nonpublishing mode: %q", output)
	}
	storeRoot, err := factory.JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := factory.NewJobStore(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	var jobs []factory.JobRecord
	waitUntil := time.Now().Add(10 * time.Second)
	for time.Now().Before(waitUntil) {
		jobs, err = store.ListJobs()
		if err == nil && len(jobs) == 1 && jobs[0].Status == "complete" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil || len(jobs) != 1 || jobs[0].Type != "tidy" || jobs[0].Status != "complete" || jobs[0].TaskDescription != "Review, fix, document, and verify the target repository without publishing changes." {
		t.Fatalf("detached tidy job record = %+v err=%v", jobs, err)
	}
	if got := strings.TrimSpace(string(git("rev-parse", "HEAD"))); got != initialHead {
		t.Fatalf("detached tidy published a commit: HEAD=%s want=%s", got, initialHead)
	}
	if got, err := os.ReadFile(filepath.Join(target, "preexisting.txt")); err != nil || string(got) != "keep me\n" {
		t.Fatalf("detached tidy did not preserve existing work: %q err=%v", got, err)
	}
	if status := strings.TrimSpace(string(git("status", "--porcelain"))); status == "" {
		t.Fatal("detached tidy unexpectedly cleaned or committed the pre-existing change")
	}
}

func TestTidyRejectsTaskArgumentsUnlessDetached(t *testing.T) {
	var out, errOut bytes.Buffer
	err := run([]string{"tidy", "unexpected"}, strings.NewReader(""), &out, &errOut)
	if err == nil || !strings.Contains(err.Error(), "factory tidy accepts only --gate") {
		t.Fatalf("tidy argument error = %v", err)
	}
}

func TestPipelineArgumentsBecomeTaskWithoutPrompt(t *testing.T) {
	var out bytes.Buffer
	var gotTask string
	var gotInput io.Reader
	run := func(task string, input io.Reader) error {
		gotTask = task
		gotInput = input
		return nil
	}
	if err := runPipelineTask([]string{"implement", "a", "small", "feature"}, strings.NewReader("unused input"), &out, run); err != nil {
		t.Fatal(err)
	}
	if gotTask != "implement a small feature" {
		t.Fatalf("workflow task = %q, want joined arguments", gotTask)
	}
	if out.Len() != 0 {
		t.Fatalf("argument-based task unexpectedly prompted: %q", out.String())
	}
	if got, err := io.ReadAll(gotInput); err != nil || string(got) != "unused input" {
		t.Fatalf("workflow input = %q, %v; argument-based execution must preserve stdin for approval gates", got, err)
	}
}

func TestMonitorListAndGetPersistedMetadata(t *testing.T) {
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	id := "20260518T120000-0123456789ab"
	jobRoot, err := factory.JobStateRoot(state)
	if err != nil {
		t.Fatal(err)
	}
	store, err := factory.NewJobStore(jobRoot)
	if err != nil {
		t.Fatal(err)
	}
	var job factory.JobRecord
	metadata := `{"id":"` + id + `","type":"monitor","task_description":"watch task","status":"running","monitor":{"id":"` + id + `","description":"watch task","repo":"owner/repo","pr":12,"head_branch":"feature","status":"running","created_at":"2026-05-18T12:00:00Z","updated_at":"2026-05-18T12:00:00Z"}}`
	if err := json.Unmarshal([]byte(metadata), &job); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJob(job); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(id, "monitor", "running"); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendSessionLog(id, "monitor", []byte("[now] worker started\n")); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if err := run([]string{"monitor", "list"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	monitorList := out.String()
	if !strings.Contains(monitorList, id) || !strings.Contains(monitorList, "owner/repo") {
		t.Fatalf("list omitted persisted job: %s", monitorList)
	}
	out.Reset()
	if err := run([]string{"monitor", "get", id, "--details"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "watch task") || !strings.Contains(out.String(), "worker started") {
		t.Fatalf("get omitted metadata/log: %s", out.String())
	}
	if err := run([]string{"monitor", "get", "../escape"}, strings.NewReader(""), &out, &errOut); err == nil {
		t.Fatal("unsafe job id accepted")
	}
}
