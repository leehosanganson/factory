package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/leehosanganson/factory/internal/restapi"
	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restserver"
	"github.com/leehosanganson/factory/internal/restworker"
)

const watchPrivateTask = "WATCH_PRIVATE_TASK /private/watch/task"

func TestRESTWatchProcessFallsBackToHistoryWithoutDuplicateEvents(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process semantics")
	}

	fixture := newRESTWatchProcessFixture(t, func(w http.ResponseWriter, r *http.Request, handler http.Handler) {
		if strings.HasSuffix(r.URL.Path, "/events") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprint(w, `{"error":{"code":"PRIVATE_STREAM_ERROR","message":"WATCH_PRIVATE_TASK /PRIVATE_PROVIDER_TOKEN /PRIVATE_WORKSPACE"}}`)
			return
		}
		handler.ServeHTTP(w, r)
	})
	defer fixture.close()

	process := fixture.startWatch(t, "--poll-interval", "10ms", fixture.job.ID)
	process.waitForLine(t, " running")
	if got := fixture.managerGet(t).Status; got != restjobs.StatusRunning {
		t.Fatalf("watch baseline job state=%s, want nonterminal running", got)
	}
	if err := fixture.manager.AddEvent(fixture.job.ID, "cancel_requested", "PRIVATE_EVENT /PRIVATE_WORKSPACE PRIVATE_PROVIDER_TOKEN"); err != nil {
		t.Fatal(err)
	}
	process.waitForLine(t, "cancel_requested")
	fixture.executor.release()
	if err := process.wait(t); err != nil {
		t.Fatalf("watch process: %v; stderr=%q; stdout=%q", err, process.stderr.String(), process.output())
	}
	if got := fixture.managerGet(t).Status; got != restjobs.StatusSucceeded {
		t.Fatalf("job after history fallback=%s, want terminal succeeded", got)
	}

	output := process.output()
	for _, event := range []string{"queued", "running", "cancel_requested", "succeeded"} {
		if count := countWatchEvents(output, event); count != 1 {
			t.Errorf("displayed %q event %d times, want exactly once; output=%q", event, count, output)
		}
	}
	if strings.Count(output, "Job "+fixture.job.ID+": succeeded") != 1 {
		t.Errorf("terminal status missing or repeated: %q", output)
	}
	fixture.assertTerminalHistory(t, []string{"queued", "running", "cancel_requested", "succeeded"})
	fixture.assertNoPrivateData(t, output+process.stderr.String())
}

func TestRESTWatchProcessReconnectsWithoutDuplicatingEventsAtTerminalTransition(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process semantics")
	}

	firstStream := make(chan struct{})
	reconnected := make(chan struct{})
	var streams atomic.Int32
	var fixture *restWatchProcessFixture
	fixture = newRESTWatchProcessFixture(t, func(w http.ResponseWriter, r *http.Request, handler http.Handler) {
		if strings.HasSuffix(r.URL.Path, "/events") {
			switch streams.Add(1) {
			case 1:
				close(firstStream)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_, _ = fmt.Fprint(w, `{"error":{"code":"cursor_expired","message":"private stream details"}}`)
				return
			case 2:
				if got := r.Header.Get("Last-Event-ID"); got != "" {
					t.Errorf("expired cursor was sent on complete-history reconnect: %q", got)
				}
				fixture.executor.release()
				waitRESTWatchStatus(t, fixture.manager, fixture.job.ID, restjobs.StatusSucceeded)
				close(reconnected)
				return
			}
		}
		handler.ServeHTTP(w, r)
	})
	defer fixture.close()

	process := fixture.startWatch(t, "--poll-interval", "10ms", fixture.job.ID)
	select {
	case <-firstStream:
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not open its first event stream")
	}
	select {
	case <-reconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not reconnect after status/history reconciliation")
	}
	if got := fixture.managerGet(t).Status; got != restjobs.StatusSucceeded {
		t.Fatalf("job during reconnect=%s, want terminal succeeded", got)
	}
	if err := process.wait(t); err != nil {
		t.Fatalf("watch process: %v; stderr=%q", err, process.stderr.String())
	}

	output := process.output()
	for _, event := range []string{"queued", "running", "succeeded"} {
		if count := countWatchEvents(output, event); count != 1 {
			t.Errorf("reconnect displayed %q event %d times, want exactly once; output=%q", event, count, output)
		}
	}
	if strings.Count(output, "Job "+fixture.job.ID+": succeeded") != 1 {
		t.Errorf("reconnect did not display terminal status once: %q", output)
	}
	fixture.assertTerminalHistory(t, []string{"queued", "running", "succeeded"})
	fixture.assertNoPrivateData(t, process.output()+process.stderr.String())
}

func TestRESTWatchProcessCancellationLeavesJobRunning(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process semantics")
	}
	fixture := newRESTWatchProcessFixture(t, nil)
	defer fixture.close()

	process := fixture.startWatch(t, "--poll-interval", "10ms", fixture.job.ID)
	process.waitForLine(t, " running")
	if got := fixture.managerGet(t).Status; got != restjobs.StatusRunning {
		t.Fatalf("job before observer cancellation=%s, want running", got)
	}
	if err := process.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := process.wait(t); err == nil {
		t.Fatal("watch process exited successfully after observer cancellation")
	}
	if got := fixture.managerGet(t).Status; got != restjobs.StatusRunning {
		t.Fatalf("observer cancellation changed job status to %s", got)
	}
	fixture.assertNoPrivateData(t, process.output()+process.stderr.String())
	fixture.executor.release()
	waitRESTWatchStatus(t, fixture.manager, fixture.job.ID, restjobs.StatusSucceeded)
	fixture.assertTerminalHistory(t, []string{"queued", "running", "succeeded"})
}

func TestRESTWatchProcessSanitizesAuthReadinessAndStreamErrors(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process semantics")
	}

	t.Run("authentication", func(t *testing.T) {
		fixture := newRESTWatchProcessFixture(t, nil)
		defer fixture.close()
		if err := os.WriteFile(fixture.clientTokenPath, []byte("WRONG_PRIVATE_TOKEN\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		process := fixture.startWatch(t, "--json", fixture.job.ID)
		if err := process.wait(t); err == nil {
			t.Fatal("watch accepted incorrect authentication")
		}
		stderr := process.stderr.String()
		if !strings.Contains(stderr, "401") || !strings.Contains(stderr, "token configuration") {
			t.Errorf("authentication error lacks safe diagnostic: %q", stderr)
		}
		fixture.assertNoPrivateData(t, stderr+process.output())
		fixture.executor.release()
	})

	t.Run("readiness", func(t *testing.T) {
		fixture := newRESTWatchProcessFixture(t, nil)
		defer fixture.close()
		fixture.ready.Store(false)
		process := fixture.startWatch(t, "--json", fixture.job.ID)
		if err := process.wait(t); err == nil {
			t.Fatal("watch succeeded while server was not ready")
		}
		stderr := process.stderr.String()
		if !strings.Contains(stderr, "503") || !strings.Contains(stderr, "not ready") {
			t.Errorf("readiness error lacks safe diagnostic: %q", stderr)
		}
		fixture.assertNoPrivateData(t, stderr+process.output())
		fixture.executor.release()
	})

	t.Run("stream", func(t *testing.T) {
		fixture := newRESTWatchProcessFixture(t, func(w http.ResponseWriter, r *http.Request, handler http.Handler) {
			if strings.HasSuffix(r.URL.Path, "/events") {
				w.WriteHeader(http.StatusBadGateway)
				_, _ = fmt.Fprintf(w, "private stream detail %s", watchPrivateTask)
				return
			}
			if strings.HasPrefix(r.URL.Path, "/v1/jobs/") && !strings.HasSuffix(r.URL.Path, "/history") {
				w.WriteHeader(http.StatusBadGateway)
				_, _ = fmt.Fprintf(w, "private status detail %s", watchPrivateTask)
				return
			}
			handler.ServeHTTP(w, r)
		})
		defer fixture.close()
		process := fixture.startWatch(t, "--json", fixture.job.ID)
		if err := process.wait(t); err == nil {
			t.Fatal("watch succeeded after server stream/status failure")
		}
		stderr := process.stderr.String()
		if !strings.Contains(stderr, "502") || strings.Contains(stderr, "private status detail") {
			t.Errorf("stream failure diagnostic was not sanitized: %q", stderr)
		}
		fixture.assertNoPrivateData(t, stderr+process.output())
		fixture.executor.release()
	})
}

type restWatchExecutor struct {
	started     chan struct{}
	releaseCh   chan struct{}
	releaseOnce sync.Once
	calls       atomic.Int32
}

func (e *restWatchExecutor) Execute(ctx context.Context, _ restjobs.Snapshot) error {
	e.calls.Add(1)
	close(e.started)
	select {
	case <-e.releaseCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *restWatchExecutor) release() { e.releaseOnce.Do(func() { close(e.releaseCh) }) }

type restWatchProcessFixture struct {
	t               *testing.T
	manager         *restjobs.LocalManager
	job             restjobs.Snapshot
	executor        *restWatchExecutor
	coordinator     *restworker.Coordinator
	server          *httptest.Server
	ready           atomic.Bool
	root            string
	privatePath     string
	repoPath        string
	repoMarker      string
	secret          string
	clientTokenPath string
	configPath      string
	binary          string
	process         *restWatchProcess
}

func newRESTWatchProcessFixture(t *testing.T, wrap func(http.ResponseWriter, *http.Request, http.Handler)) *restWatchProcessFixture {
	t.Helper()
	manager, err := restjobs.NewManager(restjobs.Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 4, MaxEventsPerJob: 8})
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := manager.Admit("private-idempotency-key", restjobs.Request{Repository: "widget", Task: watchPrivateTask})
	if err != nil {
		manager.Close()
		t.Fatal(err)
	}
	t.Logf("synthetic REST watch job ID: %s", job.ID)
	fixture := &restWatchProcessFixture{
		t: t, manager: manager, job: job,
		executor: &restWatchExecutor{started: make(chan struct{}), releaseCh: make(chan struct{})},
		root:     t.TempDir(), privatePath: filepath.Join(t.TempDir(), "PRIVATE_WORKSPACE"), secret: "PRIVATE_PROVIDER_TOKEN",
	}
	fixture.repoPath = filepath.Join(fixture.root, "synthetic-widget")
	if err := os.Mkdir(fixture.repoPath, 0o700); err != nil {
		manager.Close()
		t.Fatal(err)
	}
	fixture.repoMarker = filepath.Join(fixture.repoPath, "baseline.txt")
	if err := os.WriteFile(fixture.repoMarker, []byte("isolated synthetic repository baseline\n"), 0o600); err != nil {
		manager.Close()
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"config", "user.email", "watch-test@example.invalid"}, {"config", "user.name", "REST watch test"}, {"add", "baseline.txt"}, {"commit", "-q", "-m", "baseline"}} {
		if output, err := exec.Command("git", append([]string{"-C", fixture.repoPath}, args...)...).CombinedOutput(); err != nil {
			manager.Close()
			t.Fatalf("initialize synthetic repository with git %v: %v: %s", args, err, output)
		}
	}
	fixture.ready.Store(true)
	fixture.coordinator, err = restworker.New(manager, fixture.executor, restworker.CoordinatorConfig{Workers: 1})
	if err != nil {
		manager.Close()
		t.Fatal(err)
	}
	select {
	case <-fixture.executor.started:
	case <-time.After(5 * time.Second):
		fixture.close()
		t.Fatal("fake executor did not claim synthetic job")
	}

	serverTokenPath := filepath.Join(fixture.root, "server-token")
	if err := os.WriteFile(serverTokenPath, []byte(fixture.secret), 0o600); err != nil {
		fixture.close()
		t.Fatal(err)
	}
	key, err := restserver.LoadAPIKey(serverTokenPath)
	if err != nil {
		fixture.close()
		t.Fatal(err)
	}
	handler, err := restapi.New(manager, key, restapi.Config{
		MaxRequestBodyBytes: 2 << 20,
		MaxTaskBytes:        256 << 10,
		RepositoryAliases:   map[string]struct{}{"widget": {}},
		Ready:               fixture.ready.Load,
		ReadyError:          func(context.Context) error { return nil },
	})
	if err != nil {
		fixture.close()
		t.Fatal(err)
	}
	if wrap == nil {
		fixture.server = httptest.NewServer(handler)
	} else {
		fixture.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { wrap(w, r, handler) }))
	}
	fixture.clientTokenPath = filepath.Join(fixture.root, "client-token")
	if err := os.WriteFile(fixture.clientTokenPath, []byte(fixture.secret+"\n"), 0o600); err != nil {
		fixture.close()
		t.Fatal(err)
	}
	fixture.configPath = filepath.Join(fixture.root, "rest-client.json")
	config := fmt.Sprintf(`{"base_url":%q,"token_file":"client-token"}`, fixture.server.URL)
	if err := os.WriteFile(fixture.configPath, []byte(config), 0o600); err != nil {
		fixture.close()
		t.Fatal(err)
	}
	fixture.binary = filepath.Join(fixture.root, "factory")
	build := exec.Command("go", "build", "-o", fixture.binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		fixture.close()
		t.Fatalf("build first-party factory CLI: %v\n%s", err, output)
	}
	return fixture
}

func (f *restWatchProcessFixture) close() {
	if f.process != nil && !f.process.waited && f.process.cmd.Process != nil {
		_ = f.process.cmd.Process.Kill()
		<-f.process.done
		for range f.process.lines {
		}
	}
	if f.server != nil {
		f.server.Close()
	}
	if f.executor != nil {
		f.executor.release()
	}
	if f.coordinator != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := f.coordinator.Shutdown(ctx); err != nil {
			f.t.Errorf("stop fake REST worker: %v", err)
		}
	} else if f.manager != nil {
		f.manager.Close()
	}
}

func (f *restWatchProcessFixture) startWatch(t *testing.T, args ...string) *restWatchProcess {
	t.Helper()
	commandArgs := append([]string{"rest", "watch", "--config", f.configPath}, args...)
	cmd := exec.Command(f.binary, commandArgs...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	process := &restWatchProcess{cmd: cmd, lines: make(chan string, 128), stderr: &bytes.Buffer{}}
	cmd.Stderr = process.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		defer close(process.lines)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			process.lines <- scanner.Text()
		}
	}()
	process.done = make(chan error, 1)
	go func() { process.done <- cmd.Wait() }()
	f.process = process
	return process
}

type restWatchProcess struct {
	cmd    *exec.Cmd
	lines  chan string
	done   chan error
	stderr *bytes.Buffer
	seen   []string
	waited bool
}

func (p *restWatchProcess) waitForLine(t *testing.T, want string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case line, ok := <-p.lines:
			if !ok {
				t.Fatalf("watch exited before output %q; stdout=%q stderr=%q", want, p.output(), p.stderr.String())
			}
			p.seen = append(p.seen, line)
			if strings.Contains(line, want) {
				return
			}
		case <-timer.C:
			t.Fatalf("watch did not emit %q; stdout=%q", want, p.output())
		}
	}
}

func (p *restWatchProcess) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-p.done:
		p.waited = true
		for line := range p.lines {
			p.seen = append(p.seen, line)
		}
		return err
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		p.waited = true
		for line := range p.lines {
			p.seen = append(p.seen, line)
		}
		t.Fatalf("watch process did not exit; stdout=%q stderr=%q", p.output(), p.stderr.String())
		return errors.New("unreachable")
	}
}

func (p *restWatchProcess) output() string { return strings.Join(p.seen, "\n") }

func (f *restWatchProcessFixture) managerGet(t *testing.T) restjobs.Snapshot {
	t.Helper()
	job, err := f.manager.Get(f.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func (f *restWatchProcessFixture) assertTerminalHistory(t *testing.T, want []string) {
	t.Helper()
	history, err := f.manager.History(f.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertWatchHistory(t, history.Events, want)
	if calls := f.executor.calls.Load(); calls != 1 {
		t.Errorf("fake executor ran %d times, want one synthetic execution", calls)
	}
	if got, err := os.ReadFile(f.repoMarker); err != nil || string(got) != "isolated synthetic repository baseline\n" {
		t.Errorf("watch changed synthetic repository marker: data=%q err=%v", got, err)
	}
	if output, err := exec.Command("git", "-C", f.repoPath, "status", "--porcelain").CombinedOutput(); err != nil || len(output) != 0 {
		t.Errorf("watch changed synthetic repository state: status=%q err=%v", output, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	page, err := f.manager.ListJobs(ctx, 0, 0, 10)
	if err != nil || len(page.Jobs) != 1 || page.Jobs[0].ID != f.job.ID {
		t.Errorf("watch changed isolated job registry: jobs=%+v err=%v", page.Jobs, err)
	}
}

func (f *restWatchProcessFixture) assertNoPrivateData(t *testing.T, output string) {
	t.Helper()
	for _, private := range []string{watchPrivateTask, "PRIVATE_PROVIDER_TOKEN", "WRONG_PRIVATE_TOKEN", f.privatePath, f.root, f.repoPath, f.clientTokenPath, "/PRIVATE_WORKSPACE", "PRIVATE_EVENT", "PRIVATE_STREAM_ERROR", "private stream detail", "private status detail"} {
		if private != "" && strings.Contains(output, private) {
			t.Errorf("watch output disclosed private value %q: %q", private, output)
		}
	}
}

func assertWatchHistory(t *testing.T, events []restjobs.Event, want []string) {
	t.Helper()
	got := make([]string, len(events))
	for i, event := range events {
		got[i] = event.Type
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("watch history=%v, want %v", got, want)
	}
}

func countWatchEvents(output, event string) int {
	count := 0
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, "  ") && strings.HasSuffix(line, " "+event) {
			count++
		}
	}
	return count
}

func waitRESTWatchStatus(t *testing.T, manager *restjobs.LocalManager, id string, want restjobs.Status) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, err := manager.Get(id)
		if err == nil && job.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	job, err := manager.Get(id)
	t.Fatalf("job status=%s err=%v, want %s", job.Status, err, want)
}
