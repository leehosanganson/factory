package restjobs

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRecoveryBundleCreateVerifyRestore(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(root, "jobs.db")
	store, err := OpenSQLiteStore(database, Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Admit("synthetic-key", Request{Repository: "private-alias", Task: "synthetic task text must not enter manifest"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(job.ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}

	resultsRoot := filepath.Join(root, "state", "factory", "rest-server")
	aliasDigest := sha256.Sum256([]byte("private-alias"))
	aliasDir := hex.EncodeToString(aliasDigest[:])
	jobRoot := filepath.Join(resultsRoot, aliasDir, "results", job.ID)
	for _, path := range []string{filepath.Join(root, "state"), filepath.Join(root, "state", "factory"), resultsRoot, filepath.Join(resultsRoot, aliasDir), filepath.Join(resultsRoot, aliasDir, "results"), jobRoot} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"worktree", "state", "output"} {
		if err := os.MkdirAll(filepath.Join(jobRoot, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(jobRoot, "output", "artifact.txt"), []byte("retained synthetic artifact"), 0o600); err != nil {
		t.Fatal(err)
	}
	bundlePath := filepath.Join(root, "recovery.tar.gz")
	if err := CreateRecoveryBundle(context.Background(), database, resultsRoot, map[string]string{"private-alias": aliasDir}, bundlePath); err != nil {
		t.Fatalf("create recovery bundle: %v", err)
	}
	if err := VerifyRecoveryBundle(bundlePath); err != nil {
		t.Fatalf("verify recovery bundle: %v", err)
	}
	if err := RestoreRecoveryBundle(bundlePath, filepath.Join(root, "restored")); err != nil {
		t.Fatalf("restore recovery bundle: %v", err)
	}
	restored, err := OpenSQLiteStore(filepath.Join(root, "restored", "factory", "rest-server", "jobs.db"), Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatalf("open restored database: %v", err)
	}
	defer restored.CloseStore()
	got, err := restored.Get(job.ID)
	if err != nil || got.Status != StatusFailed {
		t.Fatalf("restored job = %+v, %v; want failed synthetic job", got, err)
	}
	artifact := filepath.Join(root, "restored", "factory", "rest-server", aliasDir, "results", job.ID, "output", "artifact.txt")
	if data, err := os.ReadFile(artifact); err != nil || string(data) != "retained synthetic artifact" {
		t.Fatalf("restored artifact = %q, %v", data, err)
	}
}

func TestRecoveryBundleAcceptsCanonicalPrivateDirectoriesThroughAliasedAncestors(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(filepath.Dir(root), filepath.Base(root)+"-alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(root, "jobs.db")
	store, err := OpenSQLiteStore(database, Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	results := filepath.Join(root, "results")
	if err := os.Mkdir(results, 0o700); err != nil {
		t.Fatal(err)
	}
	bundleDir := filepath.Join(root, "bundles")
	if err := os.Mkdir(bundleDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := CreateRecoveryBundle(context.Background(), filepath.Join(alias, "jobs.db"), filepath.Join(alias, "results"), map[string]string{"a": recoveryAliasDirectory("a")}, filepath.Join(alias, "results", "bundle.tar.gz")); err == nil {
		t.Fatal("bundle creation accepted a destination under an aliased results root")
	}
	if err := CreateRecoveryBundle(context.Background(), filepath.Join(alias, "jobs.db"), filepath.Join(root, "results"), map[string]string{"a": recoveryAliasDirectory("a")}, filepath.Join(alias, "results", "bundle.tar.gz")); err == nil {
		t.Fatal("bundle creation accepted a destination overlapping results through an aliased ancestor")
	}
	if err := CreateRecoveryBundle(context.Background(), filepath.Join(alias, "jobs.db"), filepath.Join(alias, "results"), map[string]string{"a": recoveryAliasDirectory("a")}, filepath.Join(alias, "bundles", "bundle.tar.gz")); err != nil {
		t.Fatalf("create bundle through aliased ancestors: %v", err)
	}
	if err := VerifyRecoveryBundle(filepath.Join(root, "bundles", "bundle.tar.gz")); err != nil {
		t.Fatalf("verify bundle created through aliased ancestor: %v", err)
	}
}

func TestRecoveryBundleRejectsSymlinkedResultsRoot(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(root, "jobs.db")
	store, err := OpenSQLiteStore(database, Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "results")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	results := filepath.Join(root, "results-link")
	if err := os.Symlink(target, results); err != nil {
		t.Fatal(err)
	}
	if err := CreateRecoveryBundle(context.Background(), database, results, map[string]string{"a": recoveryAliasDirectory("a")}, filepath.Join(root, "bundle.tar.gz")); err == nil {
		t.Fatal("bundle creation accepted a symlinked results root")
	}
}

func TestRecoveryBundleRejectsActiveOwnerPermissionsAndSourceOverlap(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(root, "jobs.db")
	store, err := OpenSQLiteServerStore(database, Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	results := filepath.Join(root, "results")
	if err := os.Mkdir(results, 0o700); err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(root, "bundle.tar.gz")
	if err := CreateRecoveryBundle(context.Background(), database, results, map[string]string{"a": recoveryAliasDirectory("a")}, bundle); err == nil {
		t.Fatal("bundle creation accepted active server ownership")
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	if err := CreateRecoveryBundle(context.Background(), database, results, map[string]string{"a": recoveryAliasDirectory("a")}, database); err == nil {
		t.Fatal("bundle creation accepted source database destination")
	}
	if err := CreateRecoveryBundle(context.Background(), database, results, map[string]string{"a": recoveryAliasDirectory("a")}, filepath.Join(results, "inside.tar.gz")); err == nil {
		t.Fatal("bundle creation accepted destination inside workspace source root")
	}

	outside := filepath.Join(root, "private")
	if err := os.Mkdir(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := CreateRecoveryBundle(context.Background(), database, results, map[string]string{"a": recoveryAliasDirectory("a")}, filepath.Join(outside, "bundle.tar.gz")); err != nil {
		t.Fatalf("create empty bundle: %v", err)
	}
	if err := os.Chmod(filepath.Join(outside, "bundle.tar.gz"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := VerifyRecoveryBundle(filepath.Join(outside, "bundle.tar.gz")); err == nil {
		t.Fatal("verify accepted group-readable bundle")
	}
}

func TestRecoveryBundleRejectsSQLiteCorruptionAndDoesNotPublishPartialBundle(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(root, "jobs.db")
	store, err := OpenSQLiteStore(database, Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Admit("failure-key", Request{Repository: "broken", Task: "synthetic"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	results := filepath.Join(root, "results")
	alias := recoveryAliasDirectory("broken")
	jobRoot := filepath.Join(results, alias, "results", job.ID)
	for _, path := range []string{results, filepath.Join(results, alias), filepath.Join(results, alias, "results"), jobRoot} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(root, filepath.Join(jobRoot, "unsafe-link")); err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(root, "partial.tar.gz")
	if err := CreateRecoveryBundle(context.Background(), database, results, map[string]string{"broken": alias}, bundle); err == nil {
		t.Fatal("bundle accepted symlink in retained workspace")
	}
	if _, err := os.Lstat(bundle); !os.IsNotExist(err) {
		t.Fatalf("failed creation published partial bundle: %v", err)
	}

	corrupt := filepath.Join(root, "corrupt-sqlite.tar.gz")
	file, err := os.OpenFile(corrupt, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(file)
	archive := tar.NewWriter(gz)
	manifest := []byte(`{"version":1,"database":"factory/rest-server/jobs.db","jobs":[]}`)
	for _, item := range []struct {
		name    string
		payload []byte
	}{{"manifest.json", manifest}, {"factory/rest-server/jobs.db", []byte("not sqlite")}} {
		name, payload := item.name, item.payload
		if err := archive.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(payload)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := archive.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := VerifyRecoveryBundle(corrupt); err == nil {
		t.Fatal("verify accepted corrupt SQLite payload")
	}
}

func TestRecoveryBundleFailureNeverRemovesConcurrentDestination(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(root, "jobs.db")
	store, err := OpenSQLiteStore(database, Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	results := filepath.Join(root, "results")
	if err := os.Mkdir(results, 0o700); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "raced.tar.gz")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan error, 1)
	go func() {
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			matches, _ := filepath.Glob(filepath.Join(root, ".factory-recovery-*.partial"))
			if len(matches) != 0 {
				if err := os.WriteFile(destination, []byte("created by another process"), 0o600); err != nil {
					ready <- err
					return
				}
				cancel()
				ready <- nil
				return
			}
			select {
			case <-ctx.Done():
				ready <- ctx.Err()
				return
			case <-deadline.C:
				ready <- errors.New("timed out waiting for temporary recovery archive")
				return
			case <-ticker.C:
			}
		}
	}()
	if err := CreateRecoveryBundle(ctx, database, results, map[string]string{"a": recoveryAliasDirectory("a")}, destination); err == nil {
		t.Fatal("creation unexpectedly succeeded after cancellation")
	}
	if err := <-ready; err != nil {
		t.Fatalf("concurrent destination setup: %v", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil || string(got) != "created by another process" {
		t.Fatalf("failed creation changed concurrent destination: %q, %v", got, err)
	}
}

func TestRecoveryBundleRemovesPartialArchiveAfterSizeLimitFailure(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(root, "jobs.db")
	store, err := OpenSQLiteStore(database, Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Admit("large-artifact", Request{Repository: "large", Task: "synthetic"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	results := filepath.Join(root, "results")
	alias := recoveryAliasDirectory("large")
	jobRoot := filepath.Join(results, alias, "results", job.ID)
	for _, path := range []string{results, filepath.Join(results, alias), filepath.Join(results, alias, "results"), jobRoot} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(jobRoot, "a-small"), []byte("written before limit"), 0o600); err != nil {
		t.Fatal(err)
	}
	large := filepath.Join(jobRoot, "z-over-limit")
	file, err := os.OpenFile(large, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxRecoveryBundleBytes + 1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	destination := filepath.Join(root, "partial.tar.gz")
	if err := CreateRecoveryBundle(context.Background(), database, results, map[string]string{"large": alias}, destination); err == nil {
		t.Fatal("creation accepted artifact exceeding the recovery bundle byte limit")
	}
	if _, err := os.Lstat(destination); !os.IsNotExist(err) {
		t.Fatalf("failed creation published partial bundle: %v", err)
	}
	for _, pattern := range []string{filepath.Join(root, ".factory-recovery-*.partial"), filepath.Join(root, ".factory-recovery-*.partial.db*"), filepath.Join(root, "partial.tar.gz.partial")} {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		if len(matches) != 0 {
			t.Errorf("failed creation left temporary archive files matching %q: %v", pattern, matches)
		}
	}
}

func TestRecoveryBundleRejectsMalformedAndUnsafeRestore(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(root, "bad.tar.gz")
	if err := os.WriteFile(bad, []byte("not a bundle"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyRecoveryBundle(bad); err == nil {
		t.Fatal("verify accepted malformed archive")
	}
	traversal := filepath.Join(root, "traversal.tar.gz")
	file, err := os.OpenFile(traversal, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	zip := gzip.NewWriter(file)
	archive := tar.NewWriter(zip)
	payload := []byte("escape")
	if err := archive.WriteHeader(&tar.Header{Name: "../escape", Mode: 0o600, Size: int64(len(payload)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := archive.Write(payload); err != nil {
		t.Fatal(err)
	}
	_ = archive.Close()
	_ = zip.Close()
	_ = file.Close()
	if err := VerifyRecoveryBundle(traversal); err == nil {
		t.Fatal("verify accepted path traversal")
	}
}
