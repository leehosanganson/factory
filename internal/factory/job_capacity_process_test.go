package factory

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDetachedJobCapacityIsAtomicAcrossCLIProcessesAndReleasedAtTerminal(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	configDir := filepath.Join(root, "config", "factory")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	agentCalls := filepath.Join(root, "agent-calls")
	releaseAgent := filepath.Join(root, "release-agent")
	agent := filepath.Join(root, "fake-agent")
	agentScript := fmt.Sprintf("#!/bin/sh\nprintf 'call\\n' >> %q\nwhile [ ! -f %q ]; do sleep 0.02; done\necho PASS\n", agentCalls, releaseAgent)
	if err := os.WriteFile(agent, []byte(agentScript), 0o700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`{"command":%q,"args":["{system_prompt}","{task}"],"state_dir":%q,"auto_publish":false,"agent_timeout":"30s","detached_job_max_concurrency":1}`, agent, state)
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", filepath.Dir(configDir))
	t.Setenv("XDG_STATE_HOME", filepath.Join(root, "xdg-state"))

	firstTarget := capacityTestTarget(t)
	secondTarget := capacityTestTarget(t)
	secondTargetAlias := filepath.Join(root, "second-target-alias")
	if err := os.Symlink(secondTarget, secondTargetAlias); err != nil {
		t.Fatal(err)
	}
	secondTarget = secondTargetAlias
	binary := buildCapacityTestCLI(t)

	start := func(target, description string) ([]byte, error) {
		command := exec.Command(binary, "job", "start", "tidy", description)
		command.Dir = target
		return command.CombinedOutput()
	}
	var wg sync.WaitGroup
	outputs := make([][]byte, 2)
	errors := make([]error, 2)
	for i, target := range []string{firstTarget, secondTarget} {
		wg.Add(1)
		go func(i int, target string) {
			defer wg.Done()
			outputs[i], errors[i] = start(target, fmt.Sprintf("capacity race %d", i))
		}(i, target)
	}
	wg.Wait()
	succeeded, rejected := 0, 0
	for i := range outputs {
		if errors[i] == nil {
			succeeded++
			continue
		}
		rejected++
		if !strings.Contains(string(outputs[i]), "detached job concurrency limit reached") || !strings.Contains(string(outputs[i]), "factory job list") {
			t.Fatalf("capacity rejection was not actionable: err=%v output=%s", errors[i], outputs[i])
		}
	}
	if succeeded != 1 || rejected != 1 {
		t.Fatalf("concurrent starts: succeeded=%d rejected=%d outputs=%q errors=%v; want exactly one of each", succeeded, rejected, outputs, errors)
	}
	waitFor(t, 5*time.Second, func() bool {
		_, err := os.Stat(agentCalls)
		return err == nil
	})
	store, err := NewJobStore(filepath.Join(state, "factory", "detached-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := store.ListJobs()
	if err != nil || len(jobs) != 1 {
		t.Fatalf("records after rejected admission = %d, %v; want only the admitted job", len(jobs), err)
	}
	calls, err := os.ReadFile(agentCalls)
	if err != nil || len(strings.Fields(string(calls))) != 1 {
		t.Fatalf("agent calls after rejected admission = %q, %v; want only the admitted worker", calls, err)
	}

	if err := os.WriteFile(releaseAgent, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 15*time.Second, func() bool {
		job, err := store.GetJob(jobs[0].ID)
		return err == nil && job.Status == "complete"
	})
	secondTargetResolved := resolvedTestPath(t, secondTarget)
	if output, err := start(secondTarget, "capacity after terminal release"); err != nil {
		t.Fatalf("start after previous job became terminal: %v: %s", err, output)
	}
	waitFor(t, 15*time.Second, func() bool {
		jobs, err := store.ListJobs()
		if err != nil || len(jobs) != 2 {
			return false
		}
		for _, job := range jobs {
			if job.TargetPath == secondTargetResolved {
				return job.Status == "complete"
			}
		}
		return false
	})

	jobs, err = store.ListJobs()
	if err != nil {
		t.Fatal(err)
	}
	var firstJob, secondJob JobRecord
	for _, job := range jobs {
		if job.TaskDescription == "capacity after terminal release" {
			secondJob = job
		} else {
			firstJob = job
		}
	}
	if firstJob.ID == "" || secondJob.ID == "" {
		t.Fatalf("completed initial and follow-up jobs not found: %+v", jobs)
	}
	waitForDetachedJobWorkerCleanup(t, store, firstJob)
	waitForDetachedJobWorkerCleanup(t, store, secondJob)
}

func waitForDetachedJobWorkerCleanup(t *testing.T, store *JobStore, job JobRecord) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	workerAbsent := false
	lockAvailable := false
	for time.Now().Before(deadline) {
		_, err := store.ReadWorker(job.ID)
		switch {
		case err == nil:
			workerAbsent = false
		case errors.Is(err, os.ErrNotExist):
			workerAbsent = true
		default:
			t.Fatalf("read worker record for job %s (target %q): %v", job.ID, job.TargetPath, err)
		}

		unlock, acquired, err := store.TryLockTarget(job.TargetPath)
		if err != nil {
			t.Fatalf("try target lock for job %s (target %q): %v", job.ID, job.TargetPath, err)
		}
		lockAvailable = acquired
		if acquired {
			unlock()
		}
		if workerAbsent && lockAvailable {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("worker cleanup timed out for job %s (target %q): worker_absent=%t target_lock_available=%t", job.ID, job.TargetPath, workerAbsent, lockAvailable)
}

func capacityTestTarget(t *testing.T) string {
	t.Helper()
	repo := newCleanRepo(t)
	if err := os.WriteFile(filepath.Join(repo.work, "Makefile"), []byte("fmt test vet:\n\t@true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitClean(t, repo.work, "add", "Makefile")
	gitClean(t, repo.work, "commit", "-m", "add checks")
	gitClean(t, repo.work, "push")
	return repo.work
}

func buildCapacityTestCLI(t *testing.T) string {
	t.Helper()
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
	return binary
}
