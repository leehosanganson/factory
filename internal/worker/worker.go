// Package restworker coordinates bounded, attempt-fenced execution of jobs
// admitted by restjobs. It owns no admission, lease, or persistence policy.
package restworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/leehosanganson/factory/internal/jobs"
)

var ErrInvalidConfig = errors.New("invalid restworker configuration")

const attemptHeartbeatInterval = 5 * time.Second

// ExecutionResult reports side effects and errors from one attempt.
type ExecutionResult struct {
	ProviderOutcome                *restjobs.ProviderOutcome
	ProviderOutcomeRequired        bool
	ProviderWriteMayHaveDispatched bool
	Err                            error
}

// Executor runs one claimed attempt and completes it after Coordinator persists
// any provider outcome. Implementations should honor ctx cancellation.
type Executor interface {
	Execute(context.Context, restjobs.Snapshot, restjobs.AttemptHandle) ExecutionResult
	Complete(context.Context, restjobs.Snapshot, restjobs.AttemptHandle, ExecutionResult) error
}

// CoordinatorConfig selects the maximum number of executor goroutines. The
// store independently enforces its own MaxConcurrentJobs bound.
type CoordinatorConfig struct {
	Workers  int
	WorkerID string
	BootID   string

	beforeStart func()
}

// workerLifecycleStore is the attempt-fenced job lifecycle used by Coordinator.
// Admission, inspection beyond Get, recovery, and persistence policy remain
// owned by the store; optional attempt identity recording is a separate
// capability.
type workerLifecycleStore interface {
	WaitClaimAttempt(context.Context) (restjobs.AttemptClaim, error)
	HeartbeatAttempt(restjobs.AttemptHandle) error
	Get(string) (restjobs.Snapshot, error)
	RecordProviderOutcomeForAttempt(restjobs.AttemptHandle, restjobs.ProviderOutcome) error
	FinishAttempt(restjobs.AttemptHandle, restjobs.Status) error
	Cancel(string) (restjobs.Snapshot, error)
	Close()
}

// Coordinator dispatches attempt-fenced job claims to a fixed set of workers.
type Coordinator struct {
	store    workerLifecycleStore
	executor Executor
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}

	mu            sync.Mutex
	stopped       bool
	starting      int
	startsDone    chan struct{}
	startsDoneSet bool
	beforeStart   func()
	stop          sync.Once
	wg            sync.WaitGroup
	jobsMu        sync.Mutex
	jobs          map[string]context.CancelFunc
	workerID      string
	bootID        string
}

// New starts the configured fixed set of worker goroutines.
func validCoordinatorIdentity(value string) bool {
	if value == "" {
		return true
	}
	return restjobs.WorkerIdentityValid(value)
}

func defaultCoordinatorIdentity(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func New(store workerLifecycleStore, executor Executor, config CoordinatorConfig) (*Coordinator, error) {
	if store == nil || executor == nil || config.Workers < 1 || !validCoordinatorIdentity(config.WorkerID) || !validCoordinatorIdentity(config.BootID) {
		return nil, fmt.Errorf("%w: store, executor, and positive worker count are required", ErrInvalidConfig)
	}
	ctx, cancel := context.WithCancel(context.Background())
	workerID, bootID := config.WorkerID, config.BootID
	if workerID == "" {
		workerID = "worker-local"
	}
	if bootID == "" {
		bootID = "boot-local"
	}
	coordinator := &Coordinator{
		store: store, executor: executor, ctx: ctx, cancel: cancel,
		done: make(chan struct{}), startsDone: make(chan struct{}), jobs: make(map[string]context.CancelFunc), workerID: workerID, bootID: bootID,
		beforeStart: config.beforeStart,
	}
	coordinator.wg.Add(config.Workers)
	for i := 0; i < config.Workers; i++ {
		go coordinator.worker()
	}
	go func() {
		coordinator.wg.Wait()
		close(coordinator.done)
	}()
	return coordinator, nil
}

// Shutdown stops admission and claims, cancels queued work, and lets running
// executions finish until ctx expires. On deadline it requests cooperative
// cancellation. Calling it again can wait for workers that did not finish
// before an earlier caller's deadline.
func (c *Coordinator) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("shutdown context must not be nil")
	}
	c.stop.Do(func() {
		c.mu.Lock()
		c.stopped = true
		c.closeStartsDoneIfReady()
		c.mu.Unlock()
		c.store.Close()
	})
	select {
	case <-c.startsDone:
	case <-ctx.Done():
		c.cancel()
		return ctx.Err()
	}
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		c.cancel()
		return ctx.Err()
	}
}

func (c *Coordinator) RequestCancel(id string) (restjobs.Snapshot, error) {
	c.jobsMu.Lock()
	defer c.jobsMu.Unlock()
	snapshot, err := c.store.Cancel(id)
	if err != nil {
		return restjobs.Snapshot{}, err
	}
	if snapshot.Status == restjobs.StatusRunning {
		if cancel := c.jobs[id]; cancel != nil {
			cancel()
		}
	}
	return snapshot, nil
}

func (c *Coordinator) worker() {
	defer c.wg.Done()
	for {
		c.mu.Lock()
		stopped := c.stopped
		c.mu.Unlock()
		if stopped {
			return
		}
		claim, err := c.store.WaitClaimAttempt(c.ctx)
		if err != nil {
			if errors.Is(err, restjobs.ErrManagerClosed) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			// An unexpected store error is terminal for this worker. Retrying
			// could create an unbounded error loop against a broken store.
			return
		}
		hasClaimIdentity := claim.Attempt.WorkerID != "" && claim.Attempt.BootID != ""
		claim.Attempt.WorkerID = defaultCoordinatorIdentity(claim.Attempt.WorkerID, c.workerID)
		claim.Attempt.BootID = defaultCoordinatorIdentity(claim.Attempt.BootID, c.bootID)
		if !hasClaimIdentity {
			if identityStore, ok := c.store.(restjobs.AttemptIdentityStore); ok {
				_ = identityStore.SetAttemptIdentity(claim.Attempt)
			}
		}
		if !c.claimExecutionStart() {
			// Shutdown won the synchronized claim-to-start boundary. The executor
			// has not received this claim, so ending it here is safe.
			_ = c.store.FinishAttempt(claim.Attempt, restjobs.StatusCanceled)
			return
		}
		if c.beforeStart != nil {
			c.beforeStart()
		}
		c.execute(claim.Job, claim.Attempt, c.markExecutionStarted)
	}
}

// claimExecutionStart reserves a claimed job for transition to executor start.
// Shutdown waits for reserved transitions, but never holds mu across executor code.
func (c *Coordinator) claimExecutionStart() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return false
	}
	c.starting++
	return true
}

func (c *Coordinator) markExecutionStarted() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.starting--
	c.closeStartsDoneIfReady()
}

func (c *Coordinator) closeStartsDoneIfReady() {
	if c.stopped && c.starting == 0 && !c.startsDoneSet {
		close(c.startsDone)
		c.startsDoneSet = true
	}
}

func (c *Coordinator) execute(job restjobs.Snapshot, attempt restjobs.AttemptHandle, markStarted func()) {
	jobCtx, cancel := context.WithCancel(c.ctx)
	heartbeatDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(attemptHeartbeatInterval)
		defer ticker.Stop()
		defer close(heartbeatDone)
		for {
			select {
			case <-jobCtx.Done():
				return
			case <-ticker.C:
				if err := c.store.HeartbeatAttempt(attempt); err != nil {
					cancel()
					return
				}
				if current, err := c.store.Get(job.ID); err != nil || current.CancellationRequested {
					cancel()
					return
				}
			}
		}
	}()
	c.jobsMu.Lock()
	c.jobs[job.ID] = cancel
	current, err := c.store.Get(job.ID)
	if job.CancellationRequested || (err == nil && current.CancellationRequested) {
		cancel()
	}
	c.jobsMu.Unlock()
	defer func() {
		c.jobsMu.Lock()
		delete(c.jobs, job.ID)
		c.jobsMu.Unlock()
		cancel()
		<-heartbeatDone
	}()
	status := restjobs.StatusFailed
	var result ExecutionResult
	panicOccurred := false
	var startOnce sync.Once
	startExecutor := func() bool {
		started := false
		startOnce.Do(func() {
			markStarted()
			started = jobCtx.Err() == nil
		})
		return started
	}
	defer startExecutor()
	func() {
		defer func() {
			if recover() != nil {
				panicOccurred = true
				status = restjobs.StatusFailed
			}
		}()
		if !startExecutor() {
			return
		}
		result = c.executor.Execute(jobCtx, job, attempt)
		if result.Err != nil {
			return
		}
		if result.ProviderOutcomeRequired && result.ProviderOutcome == nil {
			return
		}
		if result.ProviderOutcome != nil && !result.ProviderOutcomeRequired {
			return
		}
		if result.ProviderOutcome != nil {
			if c.store.RecordProviderOutcomeForAttempt(attempt, *result.ProviderOutcome) != nil {
				return
			}
			job.Provider = result.ProviderOutcome
		}
		if c.executor.Complete(jobCtx, job, attempt, result) != nil {
			return
		}
		status = restjobs.StatusSucceeded
	}()
	c.jobsMu.Lock()
	defer c.jobsMu.Unlock()
	if jobCtx.Err() != nil {
		current, err := c.store.Get(job.ID)
		requested := err == nil && current.CancellationRequested
		if (requested || c.ctx.Err() != nil) && !panicOccurred && result.ProviderOutcome == nil {
			if result.ProviderWriteMayHaveDispatched {
				status = restjobs.StatusFailed
			} else {
				status = restjobs.StatusCanceled
			}
		}
	}
	// Finish is the sole terminal transition. Executor errors and panic values
	// are intentionally excluded from history to avoid leaking raw output.
	_ = c.store.FinishAttempt(attempt, status)
}
