package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restserver"
)

func TestRecoveryBundleProcessCreateMoveVerifyRestoreInspect(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	stateHome := filepath.Join(root, "state")
	serverRoot := filepath.Join(stateHome, "factory", "rest-server")
	resultsRoot := filepath.Join(serverRoot, bundleAliasDir("private-alias"), "results")
	for _, dir := range []string{stateHome, filepath.Join(stateHome, "factory"), serverRoot, filepath.Join(serverRoot, bundleAliasDir("private-alias")), resultsRoot} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"config", "user.email", "synthetic@example.invalid"}, {"config", "user.name", "Synthetic"}} {
		cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	dbPath := filepath.Join(serverRoot, "jobs.db")
	store, err := restjobs.OpenSQLiteStore(dbPath, restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 2, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	failed, _, err := store.Admit("failure-key", restjobs.Request{Repository: "private-alias", Task: "private-secret-task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(failed.ID, restjobs.StatusFailed); err != nil {
		t.Fatal(err)
	}
	interrupted, _, err := store.Admit("interrupted-key", restjobs.Request{Repository: "private-alias", Task: "interrupted-task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	for _, job := range []restjobs.Snapshot{failed, interrupted} {
		jobRoot := filepath.Join(resultsRoot, job.ID)
		for _, name := range []string{"worktree", "state", "output"} {
			if err := os.MkdirAll(filepath.Join(jobRoot, name), 0o700); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(jobRoot, "output", "recovery-artifact"), []byte("synthetic retained artifact"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	config := restserver.DefaultConfig()
	config.Repositories = map[string]string{"private-alias": repo}
	config.Harness = restserver.HarnessConfig{Executable: "unused-harness", Args: []string{"{task}", "{system_prompt}"}}
	config.APIKeyFile = filepath.Join(root, "api-key")
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: dbPath}
	if err := os.WriteFile(config.APIKeyFile, []byte("never-included-api-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "server.json")
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "factory")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v: %s", err, output)
	}
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(binary, args...)
		cmd.Env = append(os.Environ(), "XDG_STATE_HOME="+stateHome)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("factory %v: %v: %s", args, err, output)
		}
		return string(output)
	}
	bundleDir := filepath.Join(root, "bundle-source")
	if err := os.Mkdir(bundleDir, 0o700); err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(bundleDir, "recovery.tar.gz")
	run("server", "bundle", "create", "--config", configPath, "--destination", bundle)
	movedDir := filepath.Join(root, "moved")
	if err := os.Mkdir(movedDir, 0o700); err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(movedDir, "moved-bundle.tar.gz")
	if err := os.Rename(bundle, moved); err != nil {
		t.Fatal(err)
	}
	archiveFile, err := os.Open(moved)
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := gzip.NewReader(archiveFile)
	if err != nil {
		_ = archiveFile.Close()
		t.Fatal(err)
	}
	archive := tar.NewReader(compressed)
	var manifest []byte
	for {
		header, err := archive.Next()
		if err != nil {
			if err != io.EOF {
				t.Fatalf("read recovery archive: %v", err)
			}
			break
		}
		if header.Name == "manifest.json" {
			manifest, err = io.ReadAll(archive)
			if err != nil {
				t.Fatalf("read recovery manifest: %v", err)
			}
			break
		}
	}
	_ = compressed.Close()
	_ = archiveFile.Close()
	if len(manifest) == 0 {
		t.Fatal("recovery archive has no manifest")
	}
	for _, forbidden := range []string{"private-secret-task", "interrupted-task", "never-included-api-key", repo, root} {
		if strings.Contains(string(manifest), forbidden) {
			t.Errorf("recovery manifest exposed %q: %s", forbidden, manifest)
		}
	}
	if output := run("server", "bundle", "verify", "--source", moved); !strings.Contains(output, "2 job records") {
		t.Fatalf("verify output = %q", output)
	}
	inspect := run("server", "bundle", "inspect", "--source", moved)
	for _, want := range []string{failed.ID, interrupted.ID, "failed", "running", "workspace=true"} {
		if !strings.Contains(inspect, want) {
			t.Errorf("inspect output missing %q: %s", want, inspect)
		}
	}
	for _, forbidden := range []string{"private-secret-task", "interrupted-task", "never-included-api-key", root, repo} {
		if strings.Contains(inspect, forbidden) {
			t.Errorf("inspect exposed %q: %s", forbidden, inspect)
		}
	}
	restoreParent := filepath.Join(root, "restores")
	if err := os.Mkdir(restoreParent, 0o700); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(restoreParent, "new-server")
	run("server", "bundle", "restore", "--source", moved, "--destination", restored)
	restoredStore, err := restjobs.OpenSQLiteStore(filepath.Join(restored, "factory", "rest-server", "jobs.db"), restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 2, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatalf("open restored store: %v", err)
	}
	defer restoredStore.CloseStore()
	for id, status := range map[string]restjobs.Status{failed.ID: restjobs.StatusFailed, interrupted.ID: restjobs.StatusRunning} {
		got, err := restoredStore.Get(id)
		if err != nil || got.Status != status {
			t.Errorf("restored job %s=%+v err=%v want=%s", id, got, err, status)
		}
		history, err := restoredStore.History(id)
		if err != nil || len(history.Events) < 2 {
			t.Errorf("restored history %s=%+v err=%v", id, history, err)
		}
		artifact := filepath.Join(restored, "factory", "rest-server", bundleAliasDir("private-alias"), "results", id, "output", "recovery-artifact")
		if data, err := os.ReadFile(artifact); err != nil || string(data) != "synthetic retained artifact" {
			t.Errorf("restored artifact %s=%q err=%v", id, data, err)
		}
	}
	if err := restjobs.RestoreRecoveryBundle(moved, restored); err == nil {
		t.Fatal("restore overwrote existing destination")
	}
	badBundle := filepath.Join(movedDir, "bad.tar.gz")
	if err := os.WriteFile(badBundle, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := restjobs.RestoreRecoveryBundle(badBundle, filepath.Join(restoreParent, "corrupt-restore")); err == nil {
		t.Fatal("restore accepted corrupt bundle")
	}
	if _, err := os.Stat(filepath.Join(restoreParent, "corrupt-restore")); !os.IsNotExist(err) {
		t.Fatalf("failed restore left published destination: %v", err)
	}
	if err := os.Chmod(filepath.Join(restoreParent), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := restjobs.RestoreRecoveryBundle(moved, filepath.Join(restoreParent, "unsafe-parent-restore")); err == nil {
		t.Fatal("restore accepted non-private parent")
	}
	if err := os.Chmod(filepath.Join(restoreParent), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "escape")); !os.IsNotExist(err) {
		t.Fatalf("unexpected external write: %v", err)
	}
}

func bundleAliasDir(alias string) string {
	digest := sha256.Sum256([]byte(alias))
	return hex.EncodeToString(digest[:])
}
