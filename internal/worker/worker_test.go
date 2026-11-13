package restworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leehosanganson/factory/internal/jobs"
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

func (f executorFunc) Execute(ctx context.Context, job restjobs.Snapshot, _ restjobs.AttemptHandle) ExecutionResult {
	return ExecutionResult{Err: f(ctx, job)}
}
func (executorFunc) Complete(context.Context, restjobs.Snapshot, restjobs.AttemptHandle, ExecutionResult) error {
	return nil
}

type attemptExecutorFunc func(context.Context, restjobs.Snapshot, restjobs.AttemptHandle) ExecutionResult

func (f attemptExecutorFunc) Execute(ctx context.Context, job restjobs.Snapshot, attempt restjobs.AttemptHandle) ExecutionResult {
	return f(ctx, job, attempt)
}
func (attemptExecutorFunc) Complete(context.Context, restjobs.Snapshot, restjobs.AttemptHandle, ExecutionResult) error {
	return nil
}

type lifecycleOnlyStore struct{ manager *restjobs.LocalManager }

func (s lifecycleOnlyStore) WaitClaimAttempt(ctx context.Context) (restjobs.AttemptClaim, error) {
	return s.manager.WaitClaimAttempt(ctx)
}
func (s lifecycleOnlyStore) HeartbeatAttempt(attempt restjobs.AttemptHandle) error {
	return s.manager.HeartbeatAttempt(attempt)
}
func (s lifecycleOnlyStore) Get(id string) (restjobs.Snapshot, error) { return s.manager.Get(id) }
func (s lifecycleOnlyStore) RecordProviderOutcomeForAttempt(attempt restjobs.AttemptHandle, outcome restjobs.ProviderOutcome) error {
	return s.manager.RecordProviderOutcomeForAttempt(attempt, outcome)
}
func (s lifecycleOnlyStore) FinishAttempt(attempt restjobs.AttemptHandle, status restjobs.Status) error {
	return s.manager.FinishAttempt(attempt, status)
}
func (s lifecycleOnlyStore) Cancel(id string) (restjobs.Snapshot, error) { return s.manager.Cancel(id) }
func (s lifecycleOnlyStore) Close()                                      { s.manager.Close() }

type claimGateManager struct {
	restjobs.Manager
	claimed chan struct{}
	release chan struct{}
}

func (m *claimGateManager) WaitClaimAttempt(ctx context.Context) (restjobs.AttemptClaim, error) {
	claim, err := m.Manager.WaitClaimAttempt(ctx)
	if err != nil {
		return claim, err
	}
	close(m.claimed)
	select {
	case <-m.release:
		return claim, nil
	case <-ctx.Done():
		return restjobs.AttemptClaim{}, ctx.Err()
	}
}

type attemptAwareExecutor struct {
	manager *restjobs.LocalManager
	attempt restjobs.AttemptHandle
}

func (e *attemptAwareExecutor) Execute(ctx context.Context, job restjobs.Snapshot, attempt restjobs.AttemptHandle) ExecutionResult {
	e.attempt = attempt
	if attempt.JobID != job.ID || attempt.Generation <= 0 || len(attempt.OwnerToken) != 64 {
		return ExecutionResult{Err: errors.New("invalid worker attempt handle")}
	}
	return ExecutionResult{Err: e.manager.AddEventForAttempt(attempt, "worker_test", "fenced execution")}
}
func (*attemptAwareExecutor) Complete(context.Context, restjobs.Snapshot, restjobs.AttemptHandle, ExecutionResult) error {
	return nil
}

func TestCoordinatorPassesClaimHandleToAttemptAwareExecutor(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	job := admit(t, manager, "attempt-aware")
	executor := &attemptAwareExecutor{manager: manager}
	coordinator, err := New(lifecycleOnlyStore{manager: manager}, executor, CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := coordinator.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	}()
	result := waitForStatus(t, manager, job.ID, restjobs.StatusSucceeded)
	if executor.attempt.JobID != job.ID || result.Status != restjobs.StatusSucceeded {
		t.Fatalf("execution attempt=%+v result=%+v", executor.attempt, result)
	}
	history, err := manager.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range history.Events {
		found = found || event.Type == "worker_test"
	}
	if !found {
		t.Fatalf("attempt-aware worker event missing: %+v", history.Events)
	}
}

func TestWorkerDoesNotCompleteWhenProviderOutcomePersistenceFails(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	job := admit(t, manager, "provider-result-store-failure")
	outcome := &restjobs.ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 7, URL: "https://github.com/acme/widget/pull/7", Branch: "factory/job/01234567-89ab-4cde-8fab-0123456789ab", Commit: "abc123", State: "open"}
	var completions atomic.Int32
	executor := completionProbeExecutor{
		Executor: attemptExecutorFunc(func(context.Context, restjobs.Snapshot, restjobs.AttemptHandle) ExecutionResult {
			return ExecutionResult{ProviderOutcome: outcome, ProviderOutcomeRequired: true}
		}),
		complete: func(context.Context, restjobs.Snapshot, ExecutionResult) error {
			completions.Add(1)
			return nil
		},
	}
	coordinator, err := New(providerOutcomeFailingStore{workerLifecycleStore: manager}, executor, CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	failed := waitForStatus(t, manager, job.ID, restjobs.StatusFailed)
	if failed.Provider != nil || completions.Load() != 0 {
		t.Fatalf("failed provider persistence reached completion or exposed outcome: job=%+v completions=%d", failed, completions.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

type providerOutcomeFailingStore struct{ workerLifecycleStore }

func (providerOutcomeFailingStore) RecordProviderOutcomeForAttempt(restjobs.AttemptHandle, restjobs.ProviderOutcome) error {
	return errors.New("private persistence detail")
}

func TestWorkerRejectsUnexpectedProviderOutcomeBeforePersistence(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	job := admit(t, manager, "unexpected-provider-result")
	outcome := &restjobs.ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 7, URL: "https://github.com/acme/widget/pull/7", Branch: "factory/job/01234567-89ab-4cde-8fab-0123456789ab", Commit: "abc123", State: "open"}
	var completions atomic.Int32
	executor := completionProbeExecutor{
		Executor: attemptExecutorFunc(func(context.Context, restjobs.Snapshot, restjobs.AttemptHandle) ExecutionResult {
			return ExecutionResult{ProviderOutcome: outcome, ProviderOutcomeRequired: false}
		}),
		complete: func(context.Context, restjobs.Snapshot, ExecutionResult) error {
			completions.Add(1)
			return nil
		},
	}
	coordinator, err := New(manager, executor, CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	failed := waitForStatus(t, manager, job.ID, restjobs.StatusFailed)
	if failed.Provider != nil || completions.Load() != 0 {
		t.Fatalf("unexpected provider result persisted or reached completion: job=%+v completions=%d", failed, completions.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerPersistsProviderOutcomeBeforeCompletionAndSuccess(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	job := admit(t, manager, "attempt-provider-result")
	outcome := &restjobs.ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 7, URL: "https://github.com/acme/widget/pull/7", Branch: "factory/job/01234567-89ab-4cde-8fab-0123456789ab", Commit: "abc123", State: "open"}
	var gotAttempt restjobs.AttemptHandle
	executor := attemptExecutorFunc(func(_ context.Context, _ restjobs.Snapshot, attempt restjobs.AttemptHandle) ExecutionResult {
		gotAttempt = attempt
		return ExecutionResult{ProviderOutcome: outcome, ProviderOutcomeRequired: true}
	})
	completed := atomic.Bool{}
	executorWithCompletion := completionProbeExecutor{Executor: executor, complete: func(_ context.Context, job restjobs.Snapshot, result ExecutionResult) error {
		stored, err := manager.Get(job.ID)
		if err != nil || stored.Status != restjobs.StatusRunning || stored.Provider == nil || *stored.Provider != *outcome || result.ProviderOutcome == nil || !result.ProviderOutcomeRequired {
			return fmt.Errorf("provider result was not persisted before completion: job=%+v err=%v", stored, err)
		}
		completed.Store(true)
		return nil
	}}
	coordinator, err := New(manager, executorWithCompletion, CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	result := waitForStatus(t, manager, job.ID, restjobs.StatusSucceeded)
	if gotAttempt.JobID != job.ID || gotAttempt.Generation <= 0 || result.Provider == nil || *result.Provider != *outcome || !completed.Load() {
		t.Fatalf("attempt=%+v result=%+v completed=%t", gotAttempt, result, completed.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func waitForStatus(t *testing.T, manager restjobs.Store, id string, want restjobs.Status) restjobs.Snapshot {
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
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestStopClaimsLetsRunningExecutionFinishAndDoesNotClaimQueuedJobs(t *testing.T) {
	manager := newTestManager(t, 3, 1)
	first := admit(t, manager, "claim-stop-running")
	second := admit(t, manager, "claim-stop-queued")
	started := make(chan struct{})
	release := make(chan struct{})
	coordinator, err := New(manager, executorFunc(func(ctx context.Context, job restjobs.Snapshot) error {
		if job.ID == first.ID {
			close(started)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		t.Errorf("executor started queued job %s after claim stop", job.ID)
		return nil
	}), CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	coordinator.StopClaims()
	close(release)
	waitForStatus(t, manager, first.ID, restjobs.StatusSucceeded)
	secondStatus, err := manager.Get(second.ID)
	if err != nil || secondStatus.Status != restjobs.StatusQueued {
		t.Fatalf("queued job after StopClaims=(%+v,%v), want queued", secondStatus, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
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

func TestRequestCancelAndExecutorPanicRemainFailedAfterPossibleProviderWrite(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	job := admit(t, manager, "cancel-and-panic-after-provider-dispatch")
	started := make(chan struct{})
	release := make(chan struct{})
	executor := attemptExecutorFunc(func(_ context.Context, _ restjobs.Snapshot, attempt restjobs.AttemptHandle) ExecutionResult {
		if err := manager.AddEventForAttempt(attempt, "provider_attempt", "Provider write may have been dispatched"); err != nil {
			return ExecutionResult{Err: err}
		}
		close(started)
		<-release
		panic("private panic details")
	})
	coordinator, err := New(manager, executor, CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := coordinator.RequestCancel(job.ID); err != nil {
		t.Fatal(err)
	}
	close(release)
	failed := waitForStatus(t, manager, job.ID, restjobs.StatusFailed)
	if failed.Provider != nil || failed.Status == restjobs.StatusCanceled {
		t.Fatalf("cancel+panic fabricated a provider outcome or canceled terminal status: %+v", failed)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	checkNoSensitiveEvent(t, manager, job.ID)
}

func TestRequestCancelDoesNotFabricateCanceledAfterProviderAttempt(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	job := admit(t, manager, "provider-side-effect")
	started := make(chan struct{})
	executor := attemptExecutorFunc(func(ctx context.Context, job restjobs.Snapshot, attempt restjobs.AttemptHandle) ExecutionResult {
		if err := manager.AddEventForAttempt(attempt, "provider_attempt", "Persisted provider write identity"); err != nil {
			return ExecutionResult{Err: err}
		}
		close(started)
		<-ctx.Done()
		return ExecutionResult{ProviderWriteMayHaveDispatched: true, Err: ctx.Err()}
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

type completionProbeExecutor struct {
	Executor
	complete func(context.Context, restjobs.Snapshot, ExecutionResult) error
}

func (e completionProbeExecutor) Complete(ctx context.Context, job restjobs.Snapshot, _ restjobs.AttemptHandle, result ExecutionResult) error {
	return e.complete(ctx, job, result)
}

func TestCancelDuringCompletionAfterProviderOutcomeRemainsFailed(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	job := admit(t, manager, "cancel-during-provider-completion")
	outcome := &restjobs.ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 7, URL: "https://github.com/acme/widget/pull/7", Branch: "factory/job/01234567-89ab-4cde-8fab-0123456789ab", Commit: "abc123", State: "open"}
	completionStarted := make(chan struct{})
	executor := completionProbeExecutor{
		Executor: attemptExecutorFunc(func(context.Context, restjobs.Snapshot, restjobs.AttemptHandle) ExecutionResult {
			return ExecutionResult{ProviderOutcome: outcome, ProviderOutcomeRequired: true}
		}),
		complete: func(ctx context.Context, job restjobs.Snapshot, _ ExecutionResult) error {
			stored, err := manager.Get(job.ID)
			if err != nil || stored.Provider == nil || *stored.Provider != *outcome {
				return fmt.Errorf("provider outcome was not persisted before completion: job=%+v err=%v", stored, err)
			}
			close(completionStarted)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	coordinator, err := New(manager, executor, CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-completionStarted
	if _, err := coordinator.RequestCancel(job.ID); err != nil {
		t.Fatal(err)
	}
	failed := waitForStatus(t, manager, job.ID, restjobs.StatusFailed)
	if failed.Provider == nil || *failed.Provider != *outcome || failed.Status == restjobs.StatusSucceeded || failed.Status == restjobs.StatusCanceled {
		t.Fatalf("canceled completion lost confirmed provider outcome or misclassified job: %+v", failed)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRequestCancelBeforeProviderDispatchRemainsCanceled(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	job := admit(t, manager, "cancel-before-provider-dispatch")
	started := make(chan struct{})
	executor := attemptExecutorFunc(func(ctx context.Context, _ restjobs.Snapshot, _ restjobs.AttemptHandle) ExecutionResult {
		close(started)
		<-ctx.Done()
		return ExecutionResult{Err: ctx.Err()}
	})
	coordinator, err := New(manager, executor, CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := coordinator.RequestCancel(job.ID); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, job.ID, restjobs.StatusCanceled)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRequestCancelCancelsOnlySelectedRunningExecutor(t *testing.T) {
	manager := newTestManager(t, 2, 2)
	first := admit(t, manager, "first")
	second := admit(t, manager, "second")
	started := make(chan string, 2)
	canceled := make(chan string, 2)
	release := make(chan struct{})
	coordinator, err := New(manager, executorFunc(func(ctx context.Context, job restjobs.Snapshot) error {
		started <- job.ID
		if job.ID == first.ID {
			<-ctx.Done()
			canceled <- job.ID
			<-release
			return ctx.Err()
		}
		<-release
		return nil
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
	waitForStatus(t, manager, second.ID, restjobs.StatusSucceeded)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownLetsRunningJobFinishAndCancelsQueuedJob(t *testing.T) {
	manager := newTestManager(t, 3, 1)
	active := admit(t, manager, "active")
	queued := admit(t, manager, "queued")
	started := make(chan struct{})
	release := make(chan struct{})
	coordinator, err := New(manager, executorFunc(func(ctx context.Context, _ restjobs.Snapshot) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}), CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownDone <- coordinator.Shutdown(ctx)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		if _, _, err := manager.Admit("after-close", restjobs.Request{Repository: "repo", Task: "after-close"}); errors.Is(err, restjobs.ErrManagerClosed) {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("manager did not close admission: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	if got, _ := manager.Get(active.ID); got.Status != restjobs.StatusRunning {
		t.Fatalf("active status during drain = %q, want running", got.Status)
	}
	if got, _ := manager.Get(queued.ID); got.Status != restjobs.StatusCanceled {
		t.Fatalf("queued status = %q, want canceled", got.Status)
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown finished before running job: %v", err)
	default:
	}
	close(release)
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got, _ := manager.Get(active.ID); got.Status != restjobs.StatusSucceeded {
		t.Fatalf("active status = %q, want succeeded", got.Status)
	}
}

func TestShutdownTimeoutCancelsRunningJobAndLaterWaitFinishes(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	job := admit(t, manager, "blocked")
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	coordinator, err := New(manager, executorFunc(func(ctx context.Context, _ restjobs.Snapshot) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return ctx.Err()
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
	<-canceled
	if got, _ := manager.Get(job.ID); got.Status != restjobs.StatusRunning {
		t.Fatalf("job status while canceled executor is finishing = %q, want running", got.Status)
	}
	long, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	waitDone := make(chan error, 1)
	go func() { waitDone <- coordinator.Shutdown(long) }()
	select {
	case err := <-waitDone:
		t.Fatalf("later shutdown returned before worker finished: %v", err)
	default:
	}
	close(release)
	if err := <-waitDone; err != nil {
		t.Fatalf("Shutdown(long) error = %v", err)
	}
	if got, _ := manager.Get(job.ID); got.Status != restjobs.StatusCanceled {
		t.Fatalf("active status after shutdown = %q, want canceled", got.Status)
	}
}

func TestShutdownWaitsForClaimToStartTransition(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	job := admit(t, manager, "claim-start-boundary")
	atBoundary := make(chan struct{})
	releaseBoundary := make(chan struct{})
	executorStarted := make(chan struct{})
	releaseExecutor := make(chan struct{})
	coordinator, err := New(manager, executorFunc(func(context.Context, restjobs.Snapshot) error {
		close(executorStarted)
		<-releaseExecutor
		return nil
	}), CoordinatorConfig{Workers: 1, beforeStart: func() {
		close(atBoundary)
		<-releaseBoundary
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-atBoundary

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownDone <- coordinator.Shutdown(ctx)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		if _, _, err := manager.Admit("after-close", restjobs.Request{Repository: "repo", Task: "after-close"}); errors.Is(err, restjobs.ErrManagerClosed) {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("manager did not close admission: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned while claim-to-start transition was in flight: %v", err)
	default:
	}
	close(releaseBoundary)
	select {
	case <-executorStarted:
	case <-time.After(time.Second):
		t.Fatal("executor did not start for a claim reserved before shutdown")
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned before in-flight executor finished: %v", err)
	default:
	}
	close(releaseExecutor)
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got, _ := manager.Get(job.ID); got.Status != restjobs.StatusSucceeded {
		t.Fatalf("job status = %q, want succeeded", got.Status)
	}
}

func TestShutdownCancelsClaimBeforeSynchronizedStartBoundary(t *testing.T) {
	base := newTestManager(t, 1, 1)
	job := admit(t, base, "claimed-not-started")
	manager := &claimGateManager{Manager: base, claimed: make(chan struct{}), release: make(chan struct{})}
	var executorCalls atomic.Int32
	coordinator, err := New(manager, executorFunc(func(context.Context, restjobs.Snapshot) error {
		executorCalls.Add(1)
		return nil
	}), CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-manager.claimed
	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownDone <- coordinator.Shutdown(ctx)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		if _, _, err := base.Admit("after-close", restjobs.Request{Repository: "repo", Task: "after-close"}); errors.Is(err, restjobs.ErrManagerClosed) {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("manager did not close admission: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	close(manager.release)
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if executorCalls.Load() != 0 {
		t.Fatalf("executor calls = %d, want 0 for claim not yet started", executorCalls.Load())
	}
	if got, _ := base.Get(job.ID); got.Status != restjobs.StatusCanceled {
		t.Fatalf("claimed job status = %q, want canceled", got.Status)
	}
}

func TestConcurrentShutdownCallsWaitForSameWorkers(t *testing.T) {
	manager := newTestManager(t, 1, 1)
	admit(t, manager, "concurrent-shutdown")
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
	results := make(chan error, 2)
	for range 2 {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			results <- coordinator.Shutdown(ctx)
		}()
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, _, err := manager.Admit("after-close", restjobs.Request{Repository: "repo", Task: "after-close"}); errors.Is(err, restjobs.ErrManagerClosed) {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("manager did not close admission: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-results:
		t.Fatalf("concurrent shutdown returned before worker finished: %v", err)
	default:
	}
	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("concurrent Shutdown() error = %v", err)
		}
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
