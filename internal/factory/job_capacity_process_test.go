package factory

import (
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
	if output, err := start(secondTarget, "capacity after terminal release"); err != nil {
		t.Fatalf("start after previous job became terminal: %v: %s", err, output)
	}
	waitFor(t, 15*time.Second, func() bool {
		jobs, err := store.ListJobs()
		if err != nil || len(jobs) != 2 {
			return false
		}
		for _, job := range jobs {
			if job.TargetPath == secondTarget {
				return job.Status == "complete"
			}
		}
		return false
	})
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
