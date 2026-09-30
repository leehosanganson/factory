package factory

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLocalWorkQueueEnqueueIsIdempotentAndRejectsPayloadConflicts(t *testing.T) {
	queue := newTestWorkQueue(t)
	request := testWorkRequest("issue:42")
	if err := queue.Enqueue(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := queue.Enqueue(context.Background(), request); err != nil {
		t.Fatalf("repeat enqueue: %v", err)
	}
	changed := request
	changed.Repository = "other/repo"
	if err := queue.Enqueue(context.Background(), changed); !errors.Is(err, ErrWorkRequestConflict) {
		t.Fatalf("conflicting enqueue error = %v, want ErrWorkRequestConflict", err)
	}
	item, err := queue.Get(context.Background(), request.DeduplicationKey)
	if err != nil {
		t.Fatal(err)
	}
	if item.Request != request || item.State != "queued" {
		t.Fatalf("persisted item = %+v, want original queued request", item)
	}
}

func TestLocalWorkQueuePersistsAndListsAcrossReopen(t *testing.T) {
	root := filepath.Join(t.TempDir(), "queue")
	first, err := NewLocalWorkQueue(root)
	if err != nil {
		t.Fatal(err)
	}
	request := testWorkRequest("issue:7")
	if err := first.Enqueue(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := NewLocalWorkQueue(root)
	if err != nil {
		t.Fatal(err)
	}
	items, err := second.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Request != request || items[0].State != "queued" {
		t.Fatalf("reopened queue list = %+v, want original queued request", items)
	}
}

func TestLocalWorkQueueOnlyAllowsOneConcurrentClaim(t *testing.T) {
	root := filepath.Join(t.TempDir(), "queue")
	first, err := NewLocalWorkQueue(root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewLocalWorkQueue(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Enqueue(context.Background(), testWorkRequest("issue:concurrent")); err != nil {
		t.Fatal(err)
	}
	queues := []*LocalWorkQueue{first, second}
	start := make(chan struct{})
	claims := make(chan WorkClaim, len(queues))
	errs := make(chan error, len(queues))
	var wg sync.WaitGroup
	for _, queue := range queues {
		wg.Add(1)
		go func(queue *LocalWorkQueue) {
			defer wg.Done()
			<-start
			claim, err := queue.Claim(context.Background())
			if err != nil {
				errs <- err
				return
			}
			claims <- claim
		}(queue)
	}
	close(start)
	wg.Wait()
	close(claims)
	close(errs)
	claimCount, emptyCount := 0, 0
	for claim := range claims {
		claimCount++
		_ = claim
	}
	for err := range errs {
		if errors.Is(err, ErrNoWorkAvailable) {
			emptyCount++
		} else {
			t.Errorf("Claim error = %v", err)
		}
	}
	if claimCount != 1 || emptyCount != 1 {
		t.Fatalf("claims=%d no-work=%d, want one exclusive claim and one no-work result", claimCount, emptyCount)
	}
}

func TestLocalWorkQueueRecoversAbandonedClaimAndFencesOldToken(t *testing.T) {
	root := filepath.Join(t.TempDir(), "queue")
	first, err := NewLocalWorkQueue(root)
	if err != nil {
		t.Fatal(err)
	}
	request := testWorkRequest("issue:recover")
	if err := first.Enqueue(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	stale, err := first.Claim(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := NewLocalWorkQueue(root)
	if err != nil {
		t.Fatal(err)
	}
	current, err := second.Claim(context.Background())
	if err != nil {
		t.Fatalf("recover abandoned claim: %v", err)
	}
	if current.Generation <= stale.Generation {
		t.Fatalf("recovered generation %d did not fence old generation %d", current.Generation, stale.Generation)
	}
	if err := second.Acknowledge(context.Background(), stale); !errors.Is(err, ErrStaleWorkClaim) {
		t.Fatalf("stale acknowledgement error = %v, want ErrStaleWorkClaim", err)
	}
	if err := second.Release(context.Background(), stale); !errors.Is(err, ErrStaleWorkClaim) {
		t.Fatalf("stale release error = %v, want ErrStaleWorkClaim", err)
	}
	if err := second.Acknowledge(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	item, err := second.Get(context.Background(), request.DeduplicationKey)
	if err != nil {
		t.Fatal(err)
	}
	if item.State != "acknowledged" {
		t.Fatalf("acknowledged state = %q", item.State)
	}
}

func TestLocalWorkQueueRecoversClaimAfterProcessDeath(t *testing.T) {
	if os.Getenv("FACTORY_WORK_QUEUE_CRASH_ROOT") != "" {
		queue, err := NewLocalWorkQueue(os.Getenv("FACTORY_WORK_QUEUE_CRASH_ROOT"))
		if err != nil {
			t.Fatal(err)
		}
		claim, err := queue.Claim(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(os.Getenv("FACTORY_WORK_QUEUE_CRASH_READY"), []byte("claimed"), 0o600); err != nil {
			_ = claim
			t.Fatal(err)
		}
		select {}
	}

	root := filepath.Join(t.TempDir(), "queue")
	queue, err := NewLocalWorkQueue(root)
	if err != nil {
		t.Fatal(err)
	}
	request := testWorkRequest("issue:process-death")
	if err := queue.Enqueue(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(t.TempDir(), "claimed")
	cmd := exec.Command(os.Args[0], "-test.run=^TestLocalWorkQueueRecoversClaimAfterProcessDeath$")
	cmd.Env = append(os.Environ(), "FACTORY_WORK_QUEUE_CRASH_ROOT="+root, "FACTORY_WORK_QUEUE_CRASH_READY="+ready)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	waitForWorkQueueFile(t, ready)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("killed claimant process unexpectedly exited successfully")
	}
	reopened, err := NewLocalWorkQueue(root)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := reopened.Claim(context.Background())
	if err != nil {
		t.Fatalf("claim after process death: %v", err)
	}
	item, err := reopened.Get(context.Background(), request.DeduplicationKey)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Generation != item.ClaimGeneration || claim.Generation < 2 {
		t.Fatalf("recovered generation=%d record generation=%d, want generation at least 2", claim.Generation, item.ClaimGeneration)
	}
}

func TestLocalWorkQueueReleaseMakesRequestClaimableAgain(t *testing.T) {
	queue := newTestWorkQueue(t)
	request := testWorkRequest("issue:release")
	if err := queue.Enqueue(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	first, err := queue.Claim(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Release(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second, err := queue.Claim(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second.Generation <= first.Generation {
		t.Fatalf("released claim generation %d did not advance beyond %d", second.Generation, first.Generation)
	}
	if err := queue.Acknowledge(context.Background(), first); !errors.Is(err, ErrStaleWorkClaim) {
		t.Fatalf("acknowledging released claim error = %v, want ErrStaleWorkClaim", err)
	}
	if err := queue.Release(context.Background(), first); !errors.Is(err, ErrStaleWorkClaim) {
		t.Fatalf("releasing released claim error = %v, want ErrStaleWorkClaim", err)
	}
	if err := queue.Release(context.Background(), second); err != nil {
		t.Fatal(err)
	}
}

func TestLocalWorkQueueRejectsMalformedRecordsAndSymlinks(t *testing.T) {
	t.Run("malformed json", func(t *testing.T) {
		queue := newTestWorkQueue(t)
		request := testWorkRequest("issue:malformed")
		if err := queue.Enqueue(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		path := queue.recordPath(request.DeduplicationKey)
		if err := os.WriteFile(path, []byte(`{"version":1,`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := queue.Get(context.Background(), request.DeduplicationKey); err == nil || !strings.Contains(err.Error(), "invalid work queue record") {
			t.Fatalf("malformed record error = %v, want rejection", err)
		}
	})
	t.Run("record symlink", func(t *testing.T) {
		queue := newTestWorkQueue(t)
		request := testWorkRequest("issue:symlink")
		if err := queue.Enqueue(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		path := queue.recordPath(request.DeduplicationKey)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		outside := filepath.Join(t.TempDir(), "outside.json")
		if err := os.WriteFile(outside, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, path); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if _, err := queue.Get(context.Background(), request.DeduplicationKey); err == nil {
			t.Fatal("Get followed a symlinked queue record")
		}
	})
	t.Run("directory symlink", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "queue")
		if err := os.MkdirAll(root, 0o700); err != nil {
			t.Fatal(err)
		}
		outside := t.TempDir()
		if err := os.Symlink(outside, filepath.Join(root, "records")); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if _, err := NewLocalWorkQueue(root); err == nil {
			t.Fatal("queue accepted a symlinked records directory")
		}
	})
}

func TestLocalWorkQueueRejectsInvalidRequestsAndIDs(t *testing.T) {
	queue := newTestWorkQueue(t)
	invalid := testWorkRequest("../bad")
	if err := queue.Enqueue(context.Background(), invalid); err == nil {
		t.Fatal("Enqueue accepted an unsafe deduplication ID")
	}
	if _, err := queue.Get(context.Background(), "../bad"); err == nil {
		t.Fatal("Get accepted an unsafe deduplication ID")
	}
	withSecret := testWorkRequest("issue:secret")
	withSecret.Repository = "repo\nsecret"
	if err := queue.Enqueue(context.Background(), withSecret); err == nil {
		t.Fatal("Enqueue accepted a control character in a request identity")
	}
}

func waitForWorkQueueFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for queue claim marker %s", path)
}

func newTestWorkQueue(t *testing.T) *LocalWorkQueue {
	t.Helper()
	queue, err := NewLocalWorkQueue(filepath.Join(t.TempDir(), "queue"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = queue.Close() })
	return queue
}

func testWorkRequest(key string) WorkRequest {
	return WorkRequest{
		TrackerProvider:  "github",
		IssueID:          "42",
		CodeHostProvider: "github",
		Repository:       "owner/repo",
		DeduplicationKey: key,
	}
}
