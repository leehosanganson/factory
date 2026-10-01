package factory

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestOAuthStateStoreCreatesDigestOnlyStateAndSurvivesRestart(t *testing.T) {
	root := filepath.Join(t.TempDir(), "oauth")
	clock := &oauthStateTestClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	store, err := newOAuthStateStore(root, clock.Time)
	if err != nil {
		t.Fatal(err)
	}
	token, err := store.Create(context.Background(), "principal_1")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) != 32 || strings.Contains(token, "=") {
		t.Fatalf("Create returned invalid opaque token %q (%d bytes): %v", token, len(decoded), err)
	}
	digest := sha256.Sum256([]byte(token))
	recordPath := store.recordPath(hex.EncodeToString(digest[:]))
	data, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), token) {
		t.Fatal("persisted OAuth state contains raw token")
	}
	for path, want := range map[string]os.FileMode{root: 0o700, filepath.Join(root, "oauth-state.lock"): 0o600, recordPath: 0o600} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("mode(%s) = %o; want %o", filepath.Base(path), got, want)
		}
	}
	var record oauthStateRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.Digest != hex.EncodeToString(digest[:]) || record.Principal != "principal_1" || !record.ExpiresAt.Equal(clock.Time().Add(oauthStateTTL)) {
		t.Fatalf("persisted record = %+v; unexpected digest/principal/expiry", record)
	}

	reopened, err := newOAuthStateStore(root, clock.Time)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := reopened.Consume(context.Background(), token)
	if err != nil || principal != "principal_1" {
		t.Fatalf("Consume after restart = %q, %v", principal, err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "state-") {
			t.Fatalf("consumed state left a record or tombstone: %s", entry.Name())
		}
	}
	restarted, err := newOAuthStateStore(root, clock.Time)
	if err != nil {
		t.Fatal(err)
	}
	if principal, err := restarted.Consume(context.Background(), token); err == nil || principal != "" {
		t.Fatalf("replayed token after restart = %q, %v; want fail closed", principal, err)
	}
}

func TestOAuthStateStoreCreateRemovesExpiredRecords(t *testing.T) {
	clock := &oauthStateTestClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	store := newOAuthStateTestStore(t, filepath.Join(t.TempDir(), "oauth"), clock.Time)
	expired, err := store.Create(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(oauthStateTTL)
	current, err := store.Create(context.Background(), "bob")
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(expired))
	if _, err := os.Lstat(store.recordPath(hex.EncodeToString(digest[:]))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired state record remains after create: %v", err)
	}
	if principal, err := store.Consume(context.Background(), current); err != nil || principal != "bob" {
		t.Fatalf("current state consume = %q, %v", principal, err)
	}
}

func TestOAuthStateStoreDeleteFailureAfterConsumeFailsClosed(t *testing.T) {
	store := newOAuthStateTestStore(t, filepath.Join(t.TempDir(), "oauth"), time.Now)
	token, err := store.Create(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	previousRemove := oauthStateRemove
	oauthStateRemove = func(path string) error {
		if err := previousRemove(path); err != nil {
			return err
		}
		return errors.New("injected uncertain delete failure")
	}
	t.Cleanup(func() { oauthStateRemove = previousRemove })

	if principal, err := store.Consume(context.Background(), token); err == nil || principal != "" {
		t.Fatalf("Consume after uncertain delete = %q, %v; want failure without principal", principal, err)
	}
	digest := sha256.Sum256([]byte(token))
	if _, err := os.Lstat(store.recordPath(hex.EncodeToString(digest[:]))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("record after uncertain delete: stat err = %v, want record absent", err)
	}
	restarted := newOAuthStateTestStore(t, store.root, time.Now)
	if principal, err := restarted.Consume(context.Background(), token); err == nil || principal != "" {
		t.Fatalf("replay after uncertain delete = %q, %v; want fail closed", principal, err)
	}
}

func TestOAuthStateStoreSyncFailureAfterConsumeFailsClosed(t *testing.T) {
	store := newOAuthStateTestStore(t, filepath.Join(t.TempDir(), "oauth"), time.Now)
	token, err := store.Create(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	previousSync := oauthStateSyncDirectory
	oauthStateSyncDirectory = func(string) error { return errors.New("injected directory sync failure") }
	t.Cleanup(func() { oauthStateSyncDirectory = previousSync })

	if principal, err := store.Consume(context.Background(), token); err == nil || principal != "" {
		t.Fatalf("Consume after uncertain directory sync = %q, %v; want failure without principal", principal, err)
	}
	digest := sha256.Sum256([]byte(token))
	if _, err := os.Lstat(store.recordPath(hex.EncodeToString(digest[:]))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("record after sync failure: stat err = %v, want record absent", err)
	}
	restarted := newOAuthStateTestStore(t, store.root, time.Now)
	if principal, err := restarted.Consume(context.Background(), token); err == nil || principal != "" {
		t.Fatalf("replay after uncertain directory sync = %q, %v; want fail closed", principal, err)
	}
}

func TestOAuthStateStoreLockWaitHonorsContextCancellation(t *testing.T) {
	store := newOAuthStateTestStore(t, filepath.Join(t.TempDir(), "oauth"), time.Now)
	token, err := store.Create(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(store.root, "oauth-state.lock")
	oauthStateLockMu.Lock()
	localLock := oauthStateLocks[lockPath]
	oauthStateLockMu.Unlock()
	if localLock == nil {
		t.Fatal("state store local lock was not registered")
	}
	localLock.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(25*time.Millisecond, cancel)
	if principal, err := store.Consume(ctx, token); !errors.Is(err, context.Canceled) || principal != "" {
		t.Fatalf("Consume while local lock held = %q, %v; want cancellation without principal", principal, err)
	}
	cancel()
	localLock.Unlock()

	lockFile, err := os.OpenFile(lockPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lockFile.Close()
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)

	ctx, cancel = context.WithCancel(context.Background())
	time.AfterFunc(25*time.Millisecond, cancel)
	if principal, err := store.Consume(ctx, token); !errors.Is(err, context.Canceled) || principal != "" {
		t.Fatalf("Consume while file lock held = %q, %v; want cancellation without principal", principal, err)
	}
	cancel()
	ctx, cancel = context.WithCancel(context.Background())
	time.AfterFunc(25*time.Millisecond, cancel)
	if token, err := store.Create(ctx, "bob"); !errors.Is(err, context.Canceled) || token != "" {
		t.Fatalf("Create while file lock held = %q, %v; want cancellation without token", token, err)
	}
	cancel()
}

func TestOAuthStateStoreExpiresAtExactBoundary(t *testing.T) {
	for _, tc := range []struct {
		name      string
		advance   time.Duration
		wantValid bool
	}{
		{name: "just before expiry", advance: oauthStateTTL - time.Nanosecond, wantValid: true},
		{name: "at expiry", advance: oauthStateTTL},
		{name: "after expiry", advance: oauthStateTTL + time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &oauthStateTestClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
			store := newOAuthStateTestStore(t, filepath.Join(t.TempDir(), "oauth"), clock.Time)
			token, err := store.Create(context.Background(), "alice")
			if err != nil {
				t.Fatal(err)
			}
			clock.Advance(tc.advance)
			principal, err := store.Consume(context.Background(), token)
			if tc.wantValid && (err != nil || principal != "alice") {
				t.Fatalf("Consume before expiry = %q, %v", principal, err)
			}
			if !tc.wantValid && (err == nil || principal != "") {
				t.Fatalf("Consume at/after expiry = %q, %v; want fail closed", principal, err)
			}
		})
	}
}

func TestOAuthStateStoreUnknownExpiredReusedAndInvalidTokensFailClosed(t *testing.T) {
	clock := &oauthStateTestClock{now: time.Now().UTC()}
	store := newOAuthStateTestStore(t, filepath.Join(t.TempDir(), "oauth"), clock.Time)
	token, err := store.Create(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	unknown := base64.RawURLEncoding.EncodeToString([]byte("01234567890123456789012345678901"))
	for _, input := range []string{unknown, "", "not base64", base64.RawURLEncoding.EncodeToString([]byte("short"))} {
		if principal, err := store.Consume(context.Background(), input); err == nil || principal != "" {
			t.Errorf("Consume(%q) = %q, %v; want failure without principal", input, principal, err)
		}
	}
	principal, err := store.Consume(context.Background(), token)
	if err != nil || principal != "alice" {
		t.Fatalf("first consume = %q, %v", principal, err)
	}
	if principal, err := store.Consume(context.Background(), token); err == nil || principal != "" {
		t.Fatalf("replay = %q, %v; want failure without principal", principal, err)
	}

	expired, err := store.Create(context.Background(), "bob")
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(oauthStateTTL)
	if principal, err := store.Consume(context.Background(), expired); err == nil || principal != "" {
		t.Fatalf("expired token = %q, %v; want failure without principal", principal, err)
	}
}

func TestOAuthStateStoreIndependentInstancesConsumeOnlyOnce(t *testing.T) {
	root := filepath.Join(t.TempDir(), "oauth")
	store := newOAuthStateTestStore(t, root, time.Now)
	token, err := store.Create(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	stores := make([]*OAuthStateStore, 8)
	for i := range stores {
		stores[i] = newOAuthStateTestStore(t, root, time.Now)
	}
	var successes atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, candidate := range stores {
		wg.Add(1)
		go func(candidate *OAuthStateStore) {
			defer wg.Done()
			<-start
			if principal, err := candidate.Consume(context.Background(), token); err == nil && principal == "alice" {
				successes.Add(1)
			}
		}(candidate)
	}
	close(start)
	wg.Wait()
	if got := successes.Load(); got != 1 {
		t.Fatalf("successful consumes = %d, want exactly one", got)
	}
}

func TestOAuthStateStoreConsumeIsSingleUseAcrossProcesses(t *testing.T) {
	if root := os.Getenv("FACTORY_OAUTH_STATE_HELPER_ROOT"); root != "" {
		ready := os.Getenv("FACTORY_OAUTH_STATE_HELPER_READY")
		gate := os.Getenv("FACTORY_OAUTH_STATE_HELPER_GATE")
		if err := os.WriteFile(ready, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, err := os.Stat(gate); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for cross-process start gate")
			}
			time.Sleep(time.Millisecond)
		}
		store, err := NewOAuthStateStore(root)
		if err != nil {
			fmt.Printf("STORE_ERROR=%v\n", err)
			return
		}
		principal, err := store.Consume(context.Background(), os.Getenv("FACTORY_OAUTH_STATE_HELPER_TOKEN"))
		fmt.Printf("CONSUME_RESULT=%s|%v\n", principal, err)
		return
	}
	root := filepath.Join(t.TempDir(), "oauth")
	store := newOAuthStateTestStore(t, root, time.Now)
	token, err := store.Create(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	// Process starts are concurrent; the shared OS lock must serialize the durable consume.
	commands := make([]*exec.Cmd, 2)
	readyPaths := make([]string, len(commands))
	gate := filepath.Join(t.TempDir(), "release")
	for i := range commands {
		readyPaths[i] = filepath.Join(t.TempDir(), fmt.Sprintf("ready-%d", i))
		commands[i] = exec.Command(os.Args[0], "-test.run=^TestOAuthStateStoreConsumeIsSingleUseAcrossProcesses$")
		commands[i].Env = append(os.Environ(), "FACTORY_OAUTH_STATE_HELPER_ROOT="+root, "FACTORY_OAUTH_STATE_HELPER_TOKEN="+token,
			"FACTORY_OAUTH_STATE_HELPER_READY="+readyPaths[i], "FACTORY_OAUTH_STATE_HELPER_GATE="+gate)
	}
	var outputs [2][]byte
	var wg sync.WaitGroup
	for i, command := range commands {
		wg.Add(1)
		go func(i int, command *exec.Cmd) {
			defer wg.Done()
			outputs[i], _ = command.CombinedOutput()
		}(i, command)
	}
	for _, ready := range readyPaths {
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, err := os.Stat(ready); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for child process readiness")
			}
			time.Sleep(time.Millisecond)
		}
	}
	if err := os.WriteFile(gate, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	var successes, rejected int
	for i, output := range outputs {
		text := string(output)
		if !strings.Contains(text, "PASS") {
			t.Errorf("child %d did not complete test: %s", i, output)
		}
		if strings.Contains(text, "STORE_ERROR=") {
			t.Errorf("child %d failed before reaching Consume: %s", i, output)
		}
		if strings.Contains(text, "CONSUME_RESULT=alice|<nil>") {
			successes++
		}
		if strings.Contains(text, "CONSUME_RESULT=|") {
			rejected++
		}
	}
	if successes != 1 || rejected != 1 {
		t.Fatalf("process consume outcomes = successes %d, rejected %d; want one each; output=%q", successes, rejected, outputs)
	}
}

func TestOAuthStateStoreRejectsCorruptAndUnsafePaths(t *testing.T) {
	t.Run("corrupt JSON", func(t *testing.T) {
		store := newOAuthStateTestStore(t, filepath.Join(t.TempDir(), "oauth"), time.Now)
		token, err := store.Create(context.Background(), "alice")
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256([]byte(token))
		if err := os.WriteFile(store.recordPath(hex.EncodeToString(digest[:])), []byte(`{"version":`), 0o600); err != nil {
			t.Fatal(err)
		}
		if principal, err := store.Consume(context.Background(), token); err == nil || principal != "" {
			t.Fatalf("corrupt record consume = %q, %v; want fail closed", principal, err)
		}
	})
	t.Run("symlink record", func(t *testing.T) {
		store := newOAuthStateTestStore(t, filepath.Join(t.TempDir(), "oauth"), time.Now)
		token, err := store.Create(context.Background(), "alice")
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256([]byte(token))
		path := store.recordPath(hex.EncodeToString(digest[:]))
		outside := filepath.Join(t.TempDir(), "outside")
		if err := os.Rename(path, outside); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, path); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if principal, err := store.Consume(context.Background(), token); err == nil || principal != "" {
			t.Fatalf("symlink consume = %q, %v; want fail closed", principal, err)
		}
	})
	t.Run("special record", func(t *testing.T) {
		store := newOAuthStateTestStore(t, filepath.Join(t.TempDir(), "oauth"), time.Now)
		token, err := store.Create(context.Background(), "alice")
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256([]byte(token))
		path := store.recordPath(hex.EncodeToString(digest[:]))
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Mkfifo(path, 0o600); err != nil {
			t.Skipf("FIFO unavailable: %v", err)
		}
		if principal, err := store.Consume(context.Background(), token); err == nil || principal != "" {
			t.Fatalf("special file consume = %q, %v; want fail closed", principal, err)
		}
	})
	t.Run("unavailable record permissions", func(t *testing.T) {
		store := newOAuthStateTestStore(t, filepath.Join(t.TempDir(), "oauth"), time.Now)
		token, err := store.Create(context.Background(), "alice")
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256([]byte(token))
		path := store.recordPath(hex.EncodeToString(digest[:]))
		if err := os.Chmod(path, 0o200); err != nil {
			t.Fatal(err)
		}
		if principal, err := store.Consume(context.Background(), token); err == nil || principal != "" {
			t.Fatalf("unreadable record consume = %q, %v; want fail closed", principal, err)
		}
	})
	t.Run("lock symlink", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "oauth")
		store := newOAuthStateTestStore(t, root, time.Now)
		outside := filepath.Join(t.TempDir(), "lock")
		if err := os.WriteFile(outside, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(root, "oauth-state.lock")); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if token, err := store.Create(context.Background(), "alice"); err == nil || token != "" {
			t.Fatalf("Create through lock symlink = %q, %v; want failure", token, err)
		}
	})
	t.Run("lock special file", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "oauth")
		store := newOAuthStateTestStore(t, root, time.Now)
		path := filepath.Join(root, "oauth-state.lock")
		if err := syscall.Mkfifo(path, 0o600); err != nil {
			t.Skipf("FIFO unavailable: %v", err)
		}
		if token, err := store.Create(context.Background(), "alice"); err == nil || token != "" {
			t.Fatalf("Create through special lock file = %q, %v; want failure", token, err)
		}
	})
	t.Run("root symlink", func(t *testing.T) {
		base := t.TempDir()
		outside := t.TempDir()
		link := filepath.Join(base, "link")
		if err := os.Symlink(outside, link); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if _, err := NewOAuthStateStore(link); err == nil {
			t.Fatal("store accepted a symlink root")
		}
	})
	t.Run("clock before creation", func(t *testing.T) {
		clock := &oauthStateTestClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
		store := newOAuthStateTestStore(t, filepath.Join(t.TempDir(), "oauth"), clock.Time)
		token, err := store.Create(context.Background(), "alice")
		if err != nil {
			t.Fatal(err)
		}
		clock.Advance(-time.Nanosecond)
		if principal, err := store.Consume(context.Background(), token); err == nil || principal != "" {
			t.Fatalf("Consume before creation = %q, %v; want fail closed", principal, err)
		}
	})
	t.Run("malformed timestamps", func(t *testing.T) {
		store := newOAuthStateTestStore(t, filepath.Join(t.TempDir(), "oauth"), time.Now)
		token, err := store.Create(context.Background(), "alice")
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256([]byte(token))
		path := store.recordPath(hex.EncodeToString(digest[:]))
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var record map[string]any
		if err := json.Unmarshal(data, &record); err != nil {
			t.Fatal(err)
		}
		record["expires_at"] = "2099-01-01T00:00:00Z"
		data, _ = json.Marshal(record)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if principal, err := store.Consume(context.Background(), token); err == nil || principal != "" {
			t.Fatalf("invalid TTL record = %q, %v; want fail closed", principal, err)
		}
	})
}

func TestOAuthStateStoreRejectsInvalidInputsAndCanceledContext(t *testing.T) {
	if _, err := NewOAuthStateStore("relative"); err == nil {
		t.Fatal("relative root was accepted")
	}
	store := newOAuthStateTestStore(t, filepath.Join(t.TempDir(), "oauth"), time.Now)
	for _, id := range []string{"", "../alice", "has space"} {
		if token, err := store.Create(context.Background(), id); err == nil || token != "" {
			t.Errorf("Create(%q) = %q, %v; want rejection", id, token, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if token, err := store.Create(ctx, "alice"); !errors.Is(err, context.Canceled) || token != "" {
		t.Fatalf("Create canceled = %q, %v", token, err)
	}
	if principal, err := store.Consume(ctx, strings.Repeat("a", 43)); !errors.Is(err, context.Canceled) || principal != "" {
		t.Fatalf("Consume canceled = %q, %v", principal, err)
	}

	token, err := store.Create(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	store.root = filepath.Join(store.root, "missing-parent", "state")
	if principal, err := store.Consume(context.Background(), token); err == nil || principal != "" {
		t.Fatalf("Consume without available persistence = %q, %v; want failure without principal", principal, err)
	}
}

type oauthStateTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *oauthStateTestClock) Time() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *oauthStateTestClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

func newOAuthStateTestStore(t *testing.T, root string, now func() time.Time) *OAuthStateStore {
	t.Helper()
	store, err := newOAuthStateStore(root, now)
	if err != nil {
		t.Fatal(err)
	}
	return store
}
