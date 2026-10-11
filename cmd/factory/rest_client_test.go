package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/leehosanganson/factory/internal/restapi"
	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restserver"
)

func TestRESTClientProcessSubmitsInspectsListsAndCancels(t *testing.T) {
	manager, err := restjobs.NewManager(restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	const token = "isolated-test-token"
	tokenPath := filepath.Join(t.TempDir(), "server-token")
	if err := os.WriteFile(tokenPath, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := restserver.LoadAPIKey(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := restapi.New(manager, key, restapi.Config{
		MaxRequestBodyBytes: 2 << 20,
		MaxTaskBytes:        256 << 10,
		RepositoryAliases:   map[string]struct{}{"widget": {}},
		Ready:               func() bool { return true },
		ReadyError:          func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	root := t.TempDir()
	binary := filepath.Join(root, "factory")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	credentialPath := filepath.Join(root, "client-token")
	if err := os.WriteFile(credentialPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "rest-client.json")
	config := `{"base_url":"` + server.URL + `","token_file":"client-token"}`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) (string, string, error) {
		t.Helper()
		command := exec.Command(binary, args...)
		var stdout, stderr bytes.Buffer
		command.Stdout, command.Stderr = &stdout, &stderr
		err := command.Run()
		return stdout.String(), stderr.String(), err
	}
	stdout, stderr, err := run("rest", "submit", "--config", configPath, "--repository", "widget", "--idempotency-key", "integration-key", "--json", "private task text")
	if err != nil || stderr != "" {
		t.Fatalf("submit: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	var submission struct {
		SchemaVersion int `json:"schema_version"`
		Submission    struct {
			Job struct {
				ID     string `json:"id"`
				Status string `json:"status"`
			} `json:"job"`
		} `json:"submission"`
	}
	if err := json.Unmarshal([]byte(stdout), &submission); err != nil {
		t.Fatalf("decode submit JSON %q: %v", stdout, err)
	}
	id := submission.Submission.Job.ID
	if submission.SchemaVersion != 1 || id == "" || submission.Submission.Job.Status != "queued" {
		t.Fatalf("submission = %+v", submission)
	}
	if strings.Contains(stdout, "private task text") || strings.Contains(stdout, token) {
		t.Fatalf("JSON output disclosed task/token: %q", stdout)
	}
	for _, args := range [][]string{{"get", "--config", configPath, "--json", id}, {"list", "--config", configPath, "--json"}} {
		stdout, stderr, err = run(append([]string{"rest"}, args...)...)
		if err != nil || stderr != "" || !json.Valid([]byte(stdout)) {
			t.Fatalf("inspect %v: err=%v stderr=%q stdout=%q", args, err, stderr, stdout)
		}
	}
	stdout, stderr, err = run("rest", "cancel", "--config", configPath, "--json", id)
	if err != nil || stderr != "" || !json.Valid([]byte(stdout)) {
		t.Fatalf("cancel: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	if !strings.Contains(stdout, "canceled") {
		t.Fatalf("cancel response = %q", stdout)
	}
	stdout, stderr, err = run("rest", "watch", "--config", configPath, "--json", id)
	if err != nil || stderr != "" || !json.Valid([]byte(stdout)) {
		t.Fatalf("watch terminal job: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
	if strings.Contains(stdout, "private task text") || strings.Contains(stdout, token) {
		t.Fatalf("watch JSON disclosed task/token: %q", stdout)
	}
	if err := os.WriteFile(credentialPath, []byte("wrong-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err = run("rest", "get", "--config", configPath, id)
	if err == nil || !strings.Contains(stderr, "401") || !strings.Contains(stderr, "token configuration") || strings.Contains(stderr, "wrong-token") || stdout != "" {
		t.Fatalf("sanitized auth failure: err=%v stderr=%q stdout=%q", err, stderr, stdout)
	}
}
