package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
)

func TestRemoteWorkerPiSlots(t *testing.T) {
	for _, slots := range []int{1, 2} {
		t.Run(fmt.Sprintf("%d_slots", slots), func(t *testing.T) {
			testRemoteWorkerPiSlots(t, slots, false)
		})
	}
	t.Run("fatal_slot_drains_sibling", func(t *testing.T) {
		testRemoteWorkerPiSlots(t, 2, true)
	})
}

func testRemoteWorkerPiSlots(t *testing.T, slots int, rejectFirstFinish bool) {
	t.Helper()
	const workerID = "pi-slot-worker"
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	initE2ERepository(t, root)
	state, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_STATE_HOME", state)
	t.Setenv("PI_CODING_AGENT_DIR", state)
	harness := filepath.Join(state, "pi")
	if err := os.WriteFile(harness, []byte(`#!/bin/sh
IFS= read -r task
printf '%s\n' "$$" > "$PI_CODING_AGENT_DIR/$task.tmp"
mv "$PI_CODING_AGENT_DIR/$task.tmp" "$PI_CODING_AGENT_DIR/$task.pid"
while [ ! -f "$PI_CODING_AGENT_DIR/$task.release" ]; do sleep .02; done
printf 'reply for %s\n' "$task"
`), 0o700); err != nil {
		t.Fatal(err)
	}
	secretFile := filepath.Join(state, "worker-secret")
	if err := os.WriteFile(secretFile, []byte(strings.Repeat("s", 32)), 0o600); err != nil {
		t.Fatal(err)
	}

	firstFinished := make(chan struct{})
	siblingHeartbeat := make(chan struct{}, 1)
	siblingFinished := make(chan struct{})
	refill := make(chan struct{}, 1)
	var mu sync.Mutex
	var bootID string
	claims := make(map[string]int)
	var claimRetried, finished bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/v1/worker/claims" {
			var req struct {
				ClaimID        string `json:"claim_id"`
				WorkerID       string `json:"worker_id"`
				BootID         string `json:"boot_id"`
				ThreadSessions bool   `json:"thread_sessions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ClaimID == "" || req.WorkerID != workerID || req.BootID == "" {
				t.Errorf("invalid claim request: %+v, %v", req, err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if !req.ThreadSessions {
				t.Error("Pi Worker did not request thread sessions")
			}
			if bootID == "" {
				bootID = req.BootID
			}
			if req.BootID != bootID {
				t.Error("slots did not share the Worker boot ID")
			}
			n, exists := claims[req.ClaimID]
			if !exists {
				n = len(claims) + 1
				claims[req.ClaimID] = n
			}
			if n == 1 && !exists {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			if n > slots {
				if rejectFirstFinish {
					t.Error("claimed another job after a fatal slot error")
				} else if !finished {
					t.Error("claimed another job before a slot finished")
				}
				select {
				case refill <- struct{}{}:
				default:
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if n == 1 {
				claimRetried = exists
			}
			id := fmt.Sprintf("00000000-0000-4000-8000-%012d", n)
			threadID := fmt.Sprintf("00000000-0000-4000-9000-%012d", n)
			_, _ = fmt.Fprintf(w, `{"job":{"id":%q,"thread_id":%q,"thread_position":1,"request":{"repository":"widget","task":%q}},"attempt":{"job_id":%q,"generation":1,"owner_token":%q,"worker_id":%q,"boot_id":%q}}`, id, threadID, fmt.Sprintf("job-%d", n), id, fmt.Sprintf("%064d", n), workerID, bootID)
			return
		}
		var n int
		var action string
		if _, err := fmt.Sscanf(r.URL.Path, "/v1/worker/attempts/00000000-0000-4000-8000-%d/%s", &n, &action); err != nil || n < 1 || n > slots {
			t.Errorf("unexpected attempt request %s", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var payload remoteWorkerFinish
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.Generation != 1 || payload.OwnerToken != fmt.Sprintf("%064d", n) || payload.WorkerID != workerID || payload.BootID != bootID {
			t.Errorf("attempt ownership for job %d: %+v, %v", n, payload, err)
		}
		switch action {
		case "heartbeat":
			if rejectFirstFinish && finished && n == 2 {
				select {
				case siblingHeartbeat <- struct{}{}:
				default:
				}
			}
			_, _ = fmt.Fprint(w, `{"cancellation_requested":false}`)
		case "finish":
			if rejectFirstFinish && n == 2 && finished && payload.Status == restjobs.StatusSucceeded && payload.Evidence.Reply == "reply for job-2" {
				close(siblingFinished)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if n != 1 || finished || payload.Status != restjobs.StatusSucceeded || payload.Evidence.Reply != "reply for job-1" {
				t.Errorf("unexpected finish for job %d: %+v", n, payload)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			finished = true
			close(firstFinished)
			if rejectFirstFinish {
				w.WriteHeader(http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected attempt action %s", action)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	config := restserver.RemoteWorkerConfig{
		ControllerBaseURL: server.URL, SharedSecretFile: secretFile, WorkerID: workerID,
		Repositories:    map[string]string{"widget": root},
		HarnessProfiles: map[string]restserver.RemoteHarnessConfig{"local": {Adapter: "pi", Executable: harness, Repositories: []string{"widget"}}},
		Limits:          restserver.RemoteWorkerLimits{JobTimeout: "1m", HarnessOutputBytes: 4096},
	}
	if slots == 2 {
		config.MaxConcurrentJobs = 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	done := make(chan struct{})
	var workerErr error
	go func() {
		workerErr = RunRemoteWorker(ctx, config)
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("Worker did not join its slots on shutdown")
		}
	}()
	wait := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-done:
			t.Fatalf("Worker exited before %s: %v", what, workerErr)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
		}
	}
	pids := make([]int, slots)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for i := range pids {
		for {
			data, err := os.ReadFile(filepath.Join(state, fmt.Sprintf("job-%d.pid", i+1)))
			if err == nil {
				if _, err := fmt.Sscan(string(data), &pids[i]); err != nil {
					t.Fatal(err)
				}
				break
			}
			if !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			select {
			case <-ticker.C:
			case <-done:
				t.Fatalf("Worker exited before Pi job %d started: %v", i+1, workerErr)
			case <-ctx.Done():
				t.Fatalf("Pi job %d did not start while earlier jobs were blocked", i+1)
			}
		}
	}
	mu.Lock()
	gotClaims, retried := len(claims), claimRetried
	mu.Unlock()
	if gotClaims != slots || !retried {
		t.Fatalf("active slots got %d claim IDs and retried=%t, want %d and true", gotClaims, retried, slots)
	}
	if lock, err := lockPiWorker(); err == nil {
		lock.Close()
		t.Fatal("a second Worker could lock active Pi state")
	}
	if err := os.WriteFile(filepath.Join(state, "job-1.release"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	wait(firstFinished, "first job finish")
	if rejectFirstFinish {
		wait(siblingHeartbeat, "healthy sibling heartbeat after rejected finish")
		if err := os.WriteFile(filepath.Join(state, "job-2.release"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
			if workerErr == nil || !strings.Contains(workerErr.Error(), "finish was rejected") {
				t.Fatalf("Worker did not return the original slot failure: %v", workerErr)
			}
		case <-ctx.Done():
			t.Fatal("Worker did not exit after the healthy job finished")
		}
		select {
		case <-siblingFinished:
		default:
			t.Fatal("Worker exited without finishing the healthy sibling")
		}
		return
	}
	wait(refill, "freed slot to claim again")
	cancel()
	select {
	case <-done:
		if workerErr != nil {
			t.Fatalf("Worker shutdown: %v", workerErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Worker did not stop")
	}
	for _, pid := range pids {
		if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Errorf("Pi child %d did not stop before Worker returned: %v", pid, err)
		}
	}
	lock, err := lockPiWorker()
	if err != nil {
		t.Fatalf("Worker did not release the lifetime lock: %v", err)
	}
	lock.Close()
}
