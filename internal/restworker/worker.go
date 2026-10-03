// Package restworker coordinates bounded execution of jobs admitted by
// restjobs. It owns no admission or persistence policy.
package restworker

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/leehosanganson/factory/internal/restjobs"
)

var ErrInvalidConfig = errors.New("invalid restworker configuration")

// Executor runs one claimed job. Implementations should honor ctx cancellation.
type Executor interface {
	Execute(ctx context.Context, job restjobs.Snapshot) error
}
type ResultExecutor interface {
	ExecuteWithResult(ctx context.Context, job restjobs.Snapshot) (*restjobs.ProviderOutcome, error)
}

type ProviderOutcomeExecutor interface{ RequiresProviderOutcome() bool }
type ResultCompletionExecutor interface {
	CompleteResult(context.Context, restjobs.Snapshot) error
}

// CoordinatorConfig selects the maximum number of executor goroutines. The
// manager independently enforces its own MaxConcurrentJobs bound.
type CoordinatorConfig struct {
	Workers int
}

// Coordinator dispatches manager claims to a fixed set of workers.
type Coordinator struct {
	manager  restjobs.Manager
	executor Executor
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}

	mu      sync.Mutex
	stopped bool
	stop    sync.Once
	wg      sync.WaitGroup
}

// New starts the configured fixed set of worker goroutines.
func New(manager restjobs.Manager, executor Executor, config CoordinatorConfig) (*Coordinator, error) {
	if manager == nil || executor == nil || config.Workers < 1 {
		return nil, fmt.Errorf("%w: manager, executor, and positive worker count are required", ErrInvalidConfig)
	}
	ctx, cancel := context.WithCancel(context.Background())
	coordinator := &Coordinator{
		manager: manager, executor: executor, ctx: ctx, cancel: cancel,
		done: make(chan struct{}),
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

// Shutdown stops claims, cancels queued and active work, and waits for all
// workers until ctx expires. Calling it again can wait for workers that did
// not finish before an earlier caller's deadline.
func (c *Coordinator) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("shutdown context must not be nil")
	}
	c.stop.Do(func() {
		c.mu.Lock()
		c.stopped = true
		c.mu.Unlock()
		c.manager.Close()
		c.cancel()
	})
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
		job, err := c.manager.WaitClaim(c.ctx)
		if err != nil {
			if errors.Is(err, restjobs.ErrManagerClosed) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			// An unexpected manager error is terminal for this worker. Retrying
			// could create an unbounded error loop against a broken manager.
			return
		}
		c.mu.Lock()
		stopped = c.stopped
		c.mu.Unlock()
		if stopped {
			_ = c.manager.Finish(job.ID, restjobs.StatusCanceled)
			return
		}
		c.execute(job)
	}
}

func (c *Coordinator) execute(job restjobs.Snapshot) {
	status := restjobs.StatusFailed
	func() {
		defer func() {
			if recover() != nil {
				status = restjobs.StatusFailed
			}
		}()
		if required, ok := c.executor.(ProviderOutcomeExecutor); ok && required.RequiresProviderOutcome() {
			executor, resultCapable := c.executor.(ResultExecutor)
			if !resultCapable {
				return
			}
			outcome, err := executor.ExecuteWithResult(c.ctx, job)
			if err != nil {
				if c.ctx.Err() != nil {
					status = restjobs.StatusCanceled
				}
				return
			}
			if outcome == nil {
				return
			}
			if c.manager.RecordProviderOutcome(job.ID, *outcome) != nil {
				return
			}
			job.Provider = outcome
			if completion, ok := c.executor.(ResultCompletionExecutor); ok && completion.CompleteResult(c.ctx, job) != nil {
				return
			}
			status = restjobs.StatusSucceeded
			return
		}
		if executor, ok := c.executor.(ResultExecutor); ok {
			outcome, err := executor.ExecuteWithResult(c.ctx, job)
			if err == nil && outcome != nil {
				if c.manager.RecordProviderOutcome(job.ID, *outcome) == nil {
					status = restjobs.StatusSucceeded
				}
			} else if err == nil {
				status = restjobs.StatusSucceeded
			} else if c.ctx.Err() != nil {
				status = restjobs.StatusCanceled
			}
			return
		}
		if err := c.executor.Execute(c.ctx, job); err == nil {
			status = restjobs.StatusSucceeded
		} else if c.ctx.Err() != nil {
			status = restjobs.StatusCanceled
		}
	}()
	if c.ctx.Err() != nil {
		status = restjobs.StatusCanceled
	}
	// Finish is the sole terminal transition. Executor errors and panic values
	// are intentionally excluded from history to avoid leaking raw output.
	_ = c.manager.Finish(job.ID, status)
}
