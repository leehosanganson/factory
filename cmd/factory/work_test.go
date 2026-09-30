package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkSubmitListGetPersistsOnlyQueuedRequests(t *testing.T) {
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	args := []string{"work", "submit", "--tracker", "github", "--issue", "owner/repo#42", "--code-host", "github", "--repository", "owner/repo"}
	var out, errOut bytes.Buffer
	if err := run(args, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	submitted := out.String()
	for _, want := range []string{"Dedup key: work-", "State: queued", "Tracker: github", "Issue: owner/repo#42", "Code host: github", "Repository: owner/repo", "Only queued", "no issue was fetched", "no PR was created", "engineering has not started", "no provider or worker"} {
		if !strings.Contains(submitted, want) {
			t.Errorf("submit output missing %q: %s", want, submitted)
		}
	}
	keyLine := strings.Split(strings.SplitN(submitted, "\n", 2)[0], ": ")
	if len(keyLine) != 2 {
		t.Fatalf("invalid dedup key output %q", submitted)
	}
	key := keyLine[1]

	out.Reset()
	if err := run([]string{"work", "list"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Dedup key: "+key) || !strings.Contains(out.String(), "State: queued") || strings.Contains(out.String(), "Only queued") {
		t.Fatalf("list should show persisted identity/state without submission notice: %s", out.String())
	}

	out.Reset()
	if err := run([]string{"work", "get", key}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Dedup key: "+key) || !strings.Contains(out.String(), "Issue: owner/repo#42") || !strings.Contains(out.String(), "State: queued") {
		t.Fatalf("get should show persisted identity/state: %s", out.String())
	}

	queueRoot := filepath.Join(state, "factory", "work-requests")
	info, err := os.Stat(queueRoot)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Errorf("work request subtree permissions = %o, want private", info.Mode().Perm())
	}
}

func TestWorkSubmitIdempotencyAndExplicitKeyConflict(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	base := []string{"work", "submit", "--tracker", "github", "--issue", "42", "--code-host", "github", "--repository", "owner/repo"}
	var out, errOut bytes.Buffer
	if err := run(base, nil, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	key := strings.Fields(strings.SplitN(out.String(), "\n", 2)[0])[2]

	out.Reset()
	if err := run(base, nil, &out, &errOut); err != nil {
		t.Fatalf("identical submission should be idempotent: %v", err)
	}
	if !strings.Contains(out.String(), "Dedup key: "+key) || !strings.Contains(out.String(), "State: queued") {
		t.Fatalf("repeat submission should return original queued request: %s", out.String())
	}

	changedIdentity := append([]string(nil), base...)
	changedIdentity[5] = "43"
	out.Reset()
	if err := run(changedIdentity, nil, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	changedKey := strings.Fields(strings.SplitN(out.String(), "\n", 2)[0])[2]
	if changedKey == key {
		t.Fatalf("different issue identity reused deterministic key %q", key)
	}

	conflict := append([]string(nil), base...)
	conflict[len(conflict)-1] = "other/repo"
	conflict = append(conflict, "--dedup-key", key)
	if err := run(conflict, nil, &out, &errOut); err == nil || !strings.Contains(err.Error(), "conflicts with existing payload") {
		t.Fatalf("explicit reused key with changed identity should conflict, got %v", err)
	}

	separate := append([]string(nil), base...)
	separate = append(separate, "--dedup-key", "intentional-rerun")
	if err := run(separate, nil, &out, &errOut); err != nil {
		t.Fatalf("explicit distinct key should allow intentional separate work: %v", err)
	}
	if !strings.Contains(out.String(), "Dedup key: intentional-rerun") {
		t.Fatalf("separate request key missing: %s", out.String())
	}
}

func TestWorkCommandsRejectInvalidArgumentsProvidersAndMissingRequests(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"missing submit values", []string{"work", "submit", "--tracker", "github"}, "usage: factory work submit"},
		{"empty explicit key", []string{"work", "submit", "--tracker", "github", "--issue", "42", "--code-host", "github", "--repository", "owner/repo", "--dedup-key", ""}, "invalid work request deduplication key"},
		{"unknown submit option", []string{"work", "submit", "--tracker", "github", "--issue", "42", "--code-host", "github", "--repository", "owner/repo", "--extra", "x"}, "usage: factory work submit"},
		{"invalid tracker", []string{"work", "submit", "--tracker", "GitHub", "--issue", "42", "--code-host", "github", "--repository", "owner/repo"}, "invalid work request tracker provider"},
		{"invalid code host", []string{"work", "submit", "--tracker", "github", "--issue", "42", "--code-host", "github.com", "--repository", "owner/repo"}, "invalid work request code host provider"},
		{"missing get key", []string{"work", "get"}, "usage: factory work get"},
		{"missing request", []string{"work", "get", "unknown-key"}, "not found"},
		{"extra list argument", []string{"work", "list", "extra"}, "usage: factory work list"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			err := run(tc.args, nil, &out, &errOut)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run(%v) error = %v, want containing %q", tc.args, err, tc.want)
			}
		})
	}
}

func TestWorkQueueUsesConfiguredStateAndDefaultStateIsolation(t *testing.T) {
	stateA, stateB := t.TempDir(), t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_STATE_HOME", stateA)
	var out, errOut bytes.Buffer
	args := []string{"work", "submit", "--tracker", "github", "--issue", "42", "--code-host", "github", "--repository", "owner/repo"}
	if err := run(args, nil, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	key := strings.Fields(strings.SplitN(out.String(), "\n", 2)[0])[2]

	out.Reset()
	t.Setenv("XDG_STATE_HOME", stateB)
	if err := run([]string{"work", "get", key}, nil, &out, &errOut); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("request in another XDG state home must be isolated, got %v", err)
	}

	configDir := filepath.Join(t.TempDir(), "configured-state")
	configPath := filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "factory", "config.json")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"command":"pi","args":["{system_prompt}","{task}"],"state_dir":"`+configDir+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	t.Setenv("XDG_STATE_HOME", stateA)
	if err := run([]string{"work", "list"}, nil, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "No work requests queued") {
		t.Fatalf("configured state_dir should isolate queue from XDG state: %s", out.String())
	}
}
