package restworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/leehosanganson/factory/internal/restjobs"
)

func newTestManager(t *testing.T, capacity, managerWorkers int) *restjobs.LocalManager {
	t.Helper()
	manager, err := restjobs.NewManager(restjobs.Config{
		QueueCapacity: capacity, MaxConcurrentJobs: managerWorkers,
		MaxRecords: capacity, MaxEventsPerJob: 16,
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	return manager
}

func admit(t *testing.T, manager *restjobs.LocalManager, key string) restjobs.Snapshot {
	t.Helper()
	job, _, err := manager.Admit(key, restjobs.Request{Repository: "repo", Task: key})
	if err != nil {
		t.Fatalf("Admit(%q) error = %v", key, err)
	}
	return job
}

type executorFunc func(context.Context, restjobs.Snapshot) error

func (f executorFunc) Execute(ctx context.Context, job restjobs.Snapshot) error {
	return f(ctx, job)
}

type requiredResultExecutor struct{ outcome *restjobs.ProviderOutcome }

func (requiredResultExecutor) Execute(context.Context, restjobs.Snapshot) error { return nil }
func (e requiredResultExecutor) ExecuteWithResult(context.Context, restjobs.Snapshot) (*restjobs.ProviderOutcome, error) {
	return e.outcome, nil
}
func (requiredResultExecutor) RequiresProviderOutcome() bool { return true }

func TestWorkerRequiresProviderOutcomeBeforeSuccess(t *testing.T) {
	for _, outcome := range []*restjobs.ProviderOutcome{nil, {Provider: "github", Repository: "acme/widget", Number: 7, URL: "https://github.com/acme/widget/pull/7", Branch: "factory/job/01234567-89ab-4cde-8fab-0123456789ab", Commit: "abc123", State: "open"}} {
		manager := newTestManager(t, 1, 1)
		job := admit(t, manager, "provider-result")
		coordinator, err := New(manager, requiredResultExecutor{outcome: outcome}, CoordinatorConfig{Workers: 1})
		if err != nil {
			t.Fatal(err)
		}
		want := restjobs.StatusSucceeded
		if outcome == nil {
			want = restjobs.StatusFailed
		}
		result := waitForStatus(t, manager, job.ID, want)
		if outcome != nil && (result.Provider == nil || *result.Provider != *outcome) {
			t.Fatalf("provider outcome=%+v", result.Provider)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := coordinator.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
}

func waitForStatus(t *testing.T, manager *restjobs.LocalManager, id string, want restjobs.Status) restjobs.Snapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, err := manager.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status == want {
			return job
		}
		time.Sleep(time.Millisecond)
	}
	job, _ := manager.Get(id)
	t.Fatalf("job status = %q, want %q", job.Status, want)
	return restjobs.Snapshot{}
}

func TestWorkerCountBoundsConcurrentExecutions(t *testing.T) {
	manager := newTestManager(t, 8, 4)
	for i := range 6 {
		admit(t, manager, fmt.Sprintf("job-%d", i))
	}
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	var mu sync.Mutex
	active, maximum := 0, 0
	executor := executorFunc(func(ctx context.Context, _ restjobs.Snapshot) error {
		mu.Lock()
		active++
		if active > maximum {
			maximum = active
		}
		mu.Unlock()
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		mu.Lock()
		active--
		mu.Unlock()
		return nil
	})
	coordinator, err := New(manager, executor, CoordinatorConfig{Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	<-started
	select {
	case <-started:
		t.Fatal("more than configured worker count started execution")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if maximum > 2 {
		t.Fatalf("maximum concurrent executions = %d, want <= 2", maximum)
	}
}

func TestManagerConcurrencyLimitConstrainsCoordinator(t *testing.T) {
	manager := newTestManager(t, 3, 1)
	for i := range 3 {
		admit(t, manager, fmt.Sprintf("job-%d", i))
	}
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	coordinator, err := New(manager, executorFunc(func(ctx context.Context, _ restjobs.Snapshot) error {
		started <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}), CoordinatorConfig{Workers: 3})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	select {
	case <-started:
		t.Fatal("manager allowed more than one concurrent claim")
	case <-time.After(30 * time.Millisecond):
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	close(release)
}

func TestSingleWorkerExecutesFIFO(t *testing.T) {
	manager := newTestManager(t, 3, 1)
	var jobs []restjobs.Snapshot
	for _, key := range []string{"first", "second", "third"} {
		jobs = append(jobs, admit(t, manager, key))
	}
	var mu sync.Mutex
	var order []string
	coordinator, err := New(manager, executorFunc(func(_ context.Context, job restjobs.Snapshot) error {
		mu.Lock()
		order = append(order, job.Request.Task)
		mu.Unlock()
		return nil
	}), CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range jobs {
		waitForStatus(t, manager, job.ID, restjobs.StatusSucceeded)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	want := []string{"first", "second", "third"}
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("execution order = %v, want %v", order, want)
	}
	history, err := manager.History(jobs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	var eventTypes []string
	for _, event := range history.Events {
		eventTypes = append(eventTypes, event.Type)
	}
	if fmt.Sprint(eventTypes) != fmt.Sprint([]string{"queued", "running", "succeeded"}) {
		t.Fatalf("lifecycle events = %v, want queued/running/succeeded", eventTypes)
	}
}

func TestExecutorReceivesClaimedSnapshotIncludingOpaqueJobID(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	admitted := admit(t, manager, "task carried with job")
	executed := make(chan restjobs.Snapshot, 1)
	coordinator, err := New(manager, executorFunc(func(_ context.Context, job restjobs.Snapshot) error {
		executed <- job
		return nil
	}), CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	got := <-executed
	waitForStatus(t, manager, admitted.ID, restjobs.StatusSucceeded)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got.ID != admitted.ID {
		t.Fatalf("executor job ID = %q, want claimed job ID %q", got.ID, admitted.ID)
	}
	if got.Request.Repository != admitted.Request.Repository || got.Request.Task != admitted.Request.Task {
		t.Fatalf("executor request = %+v, want admitted request %+v", got.Request, admitted.Request)
	}
	if got.Status != restjobs.StatusRunning {
		t.Fatalf("executor snapshot status = %q, want running", got.Status)
	}
}

func TestExecutorErrorFinishesFailedWithoutLeakingError(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	job := admit(t, manager, "failing")
	coordinator, err := New(manager, executorFunc(func(context.Context, restjobs.Snapshot) error {
		return errors.New("sensitive executor output")
	}), CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, job.ID, restjobs.StatusFailed)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	checkNoSensitiveEvent(t, manager, job.ID)
}

func TestExecutorPanicFinishesFailedAndWorkerContinues(t *testing.T) {
	manager := newTestManager(t, 2, 1)
	panicked := admit(t, manager, "panic")
	after := admit(t, manager, "after-panic")
	coordinator, err := New(manager, executorFunc(func(_ context.Context, job restjobs.Snapshot) error {
		if job.Request.Task == "panic" {
			panic("private panic details")
		}
		return nil
	}), CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, panicked.ID, restjobs.StatusFailed)
	waitForStatus(t, manager, after.ID, restjobs.StatusSucceeded)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	checkNoSensitiveEvent(t, manager, panicked.ID)
}

func TestRequestCancelDoesNotFabricateCanceledAfterProviderAttempt(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	job := admit(t, manager, "provider-side-effect")
	started := make(chan struct{})
	executor := requiredResultExecutorFunc(func(ctx context.Context, job restjobs.Snapshot) (*restjobs.ProviderOutcome, error) {
		if err := manager.AddEvent(job.ID, "provider_attempt", "Persisted provider write identity"); err != nil {
			return nil, err
		}
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	coordinator, err := New(manager, executor, CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := coordinator.RequestCancel(job.ID); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		finished <- coordinator.Shutdown(ctx)
	}()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	got, err := manager.Get(job.ID)
	if err != nil || got.Status != restjobs.StatusFailed || got.Provider != nil {
		t.Fatalf("canceled provider side effect outcome=%+v err=%v, want failed without fabricated provider outcome", got, err)
	}
}

type requiredResultExecutorFunc func(context.Context, restjobs.Snapshot) (*restjobs.ProviderOutcome, error)

func (f requiredResultExecutorFunc) Execute(ctx context.Context, job restjobs.Snapshot) error {
	_, err := f(ctx, job)
	return err
}
func (f requiredResultExecutorFunc) ExecuteWithResult(ctx context.Context, job restjobs.Snapshot) (*restjobs.ProviderOutcome, error) {
	return f(ctx, job)
}
func (requiredResultExecutorFunc) RequiresProviderOutcome() bool { return true }

func TestRequestCancelCancelsOnlySelectedRunningExecutor(t *testing.T) {
	manager := newTestManager(t, 2, 2)
	first := admit(t, manager, "first")
	second := admit(t, manager, "second")
	started := make(chan string, 2)
	canceled := make(chan string, 2)
	release := make(chan struct{})
	coordinator, err := New(manager, executorFunc(func(ctx context.Context, job restjobs.Snapshot) error {
		started <- job.ID
		<-ctx.Done()
		canceled <- job.ID
		<-release
		return ctx.Err()
	}), CoordinatorConfig{Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	startedIDs := map[string]bool{<-started: true, <-started: true}
	if !startedIDs[first.ID] || !startedIDs[second.ID] {
		t.Fatalf("started jobs=%v, want both jobs", startedIDs)
	}
	requested, err := coordinator.RequestCancel(first.ID)
	if err != nil || requested.Status != restjobs.StatusRunning || !requested.CancellationRequested {
		t.Fatalf("RequestCancel()=(%+v,%v), want running pending cancellation", requested, err)
	}
	select {
	case id := <-canceled:
		if id != first.ID {
			t.Fatalf("canceled executor job=%q, want selected job %q", id, first.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("selected executor did not receive cancellation")
	}
	if got, _ := manager.Get(second.ID); got.Status != restjobs.StatusRunning || got.CancellationRequested {
		t.Fatalf("unselected job changed after cancellation: %+v", got)
	}
	close(release)
	waitForStatus(t, manager, first.ID, restjobs.StatusCanceled)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownCancelsQueuedAndActiveJobs(t *testing.T) {
	manager := newTestManager(t, 3, 1)
	active := admit(t, manager, "active")
	queued := admit(t, manager, "queued")
	started := make(chan struct{})
	coordinator, err := New(manager, executorFunc(func(ctx context.Context, _ restjobs.Snapshot) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}), CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got, _ := manager.Get(active.ID); got.Status != restjobs.StatusCanceled {
		t.Fatalf("active status = %q, want canceled", got.Status)
	}
	if got, _ := manager.Get(queued.ID); got.Status != restjobs.StatusCanceled {
		t.Fatalf("queued status = %q, want canceled", got.Status)
	}
}

func TestShutdownWaitContextCanTimeOutThenWaitAgain(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	job := admit(t, manager, "blocked")
	started := make(chan struct{})
	release := make(chan struct{})
	coordinator, err := New(manager, executorFunc(func(context.Context, restjobs.Snapshot) error {
		close(started)
		<-release
		return nil
	}), CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := coordinator.Shutdown(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown(short) error = %v, want deadline exceeded", err)
	}
	close(release)
	long, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := coordinator.Shutdown(long); err != nil {
		t.Fatalf("Shutdown(long) error = %v", err)
	}
	if got, _ := manager.Get(job.ID); got.Status != restjobs.StatusCanceled {
		t.Fatalf("active status after shutdown = %q, want canceled", got.Status)
	}
}

func TestShutdownDeadlineDoesNotWaitForeverForNonCooperativeExecutor(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	admit(t, manager, "ignores-cancel")
	started := make(chan struct{})
	release := make(chan struct{})
	coordinator, err := New(manager, executorFunc(func(context.Context, restjobs.Snapshot) error {
		close(started)
		<-release
		return nil
	}), CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := coordinator.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want deadline exceeded", err)
	}
	close(release)
	cleanup, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := coordinator.Shutdown(cleanup); err != nil {
		t.Fatalf("cleanup Shutdown() error = %v", err)
	}
}

func TestWaitClaimHonorsContextWithoutPolling(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := manager.WaitClaim(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitClaim() error = %v, want deadline exceeded", err)
	}
}

func TestNewRejectsMissingDependenciesAndWorkers(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	validExecutor := executorFunc(func(context.Context, restjobs.Snapshot) error { return nil })
	for _, tc := range []struct {
		manager  restjobs.Manager
		executor Executor
		workers  int
	}{
		{nil, validExecutor, 1}, {manager, nil, 1}, {manager, validExecutor, 0},
	} {
		if _, err := New(tc.manager, tc.executor, CoordinatorConfig{Workers: tc.workers}); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("New(%+v) error = %v, want ErrInvalidConfig", tc, err)
		}
	}
}

func checkNoSensitiveEvent(t *testing.T, manager *restjobs.LocalManager, id string) {
	t.Helper()
	history, err := manager.History(id)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range history.Events {
		if event.Message == "sensitive executor output" || event.Message == "private panic details" {
			t.Fatalf("sensitive execution detail leaked in event: %+v", event)
		}
	}
}
