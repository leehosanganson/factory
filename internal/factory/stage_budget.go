package factory

import (
	"context"
	"errors"
	"sync"
	"time"
)

const defaultStageExecutionBudget = 30 * time.Minute

var errStageExecutionBudgetExhausted = errors.New("stage agent execution budget exhausted")

type stageExecutionBudget struct {
	mu        sync.Mutex
	remaining time.Duration
	active    map[uint64]context.CancelFunc
	nextID    uint64
	lastCheck time.Time
	timer     *time.Timer
	exhausted bool
}

func newStageExecutionBudget(limit time.Duration) *stageExecutionBudget {
	if limit <= 0 {
		limit = defaultStageExecutionBudget
	}
	if limit > defaultStageExecutionBudget {
		limit = defaultStageExecutionBudget
	}
	return &stageExecutionBudget{remaining: limit, active: make(map[uint64]context.CancelFunc)}
}

func (b *stageExecutionBudget) invoke(parent context.Context, run func(context.Context) error) (result error) {
	ctx, id, cancel, err := b.begin(parent)
	if err != nil {
		return err
	}
	defer func() {
		b.end(id, cancel)
		if b.isExhausted() {
			result = errStageExecutionBudgetExhausted
		}
	}()
	return run(ctx)
}

func (b *stageExecutionBudget) begin(parent context.Context) (context.Context, uint64, context.CancelFunc, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.updateLocked(time.Now())
	if b.exhausted || b.remaining <= 0 {
		return nil, 0, nil, errStageExecutionBudgetExhausted
	}
	ctx, cancel := context.WithCancel(parent)
	b.nextID++
	id := b.nextID
	b.active[id] = cancel
	b.scheduleLocked(time.Now())
	return ctx, id, cancel, nil
}

func (b *stageExecutionBudget) end(id uint64, cancel context.CancelFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.updateLocked(time.Now())
	delete(b.active, id)
	cancel()
	if b.remaining <= 0 {
		b.exhaustLocked()
	} else {
		b.scheduleLocked(time.Now())
	}
}

func (b *stageExecutionBudget) isExhausted() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.updateLocked(time.Now())
	return b.exhausted
}

func (b *stageExecutionBudget) updateLocked(now time.Time) {
	if !b.lastCheck.IsZero() && len(b.active) > 0 {
		b.remaining -= now.Sub(b.lastCheck)
		if b.remaining <= 0 {
			b.exhaustLocked()
		}
	}
	b.lastCheck = now
}

func (b *stageExecutionBudget) scheduleLocked(now time.Time) {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	if b.exhausted || len(b.active) == 0 {
		b.lastCheck = now
		return
	}
	b.lastCheck = now
	wait := b.remaining
	if wait <= 0 {
		b.exhaustLocked()
		return
	}
	b.timer = time.AfterFunc(wait, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.updateLocked(time.Now())
		if !b.exhausted {
			b.scheduleLocked(time.Now())
		}
	})
}

func (b *stageExecutionBudget) exhaustLocked() {
	b.exhausted = true
	b.remaining = 0
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	for _, cancel := range b.active {
		cancel()
	}
}

type budgetedAgent struct {
	agent   Agent
	budget  *stageExecutionBudget
	timeout time.Duration
}

func (a budgetedAgent) Run(stage, prompt, task, workdir, logPath string) error {
	return errors.New("budgeted agent requires context-aware execution")
}

func (a budgetedAgent) RunWithContext(ctx context.Context, stage, prompt, task, workdir, logPath string) error {
	invocationCtx, cancel := a.invocationContext(ctx)
	defer cancel()
	return a.budget.invoke(invocationCtx, func(ctx context.Context) error {
		return runAgentWithContext(ctx, a.agent, stage, prompt, task, workdir, logPath)
	})
}

func (a budgetedAgent) RunWithOutputContext(ctx context.Context, stage, prompt, task, workdir, logPath string) (string, error) {
	contextual, ok := a.agent.(outputContextAgent)
	if !ok {
		return "", errors.New("agent does not provide stdout protocol output")
	}
	invocationCtx, cancel := a.invocationContext(ctx)
	defer cancel()
	var output string
	err := a.budget.invoke(invocationCtx, func(ctx context.Context) error {
		var err error
		output, err = contextual.RunWithOutputContext(ctx, stage, prompt, task, workdir, logPath)
		return err
	})
	return output, err
}

func (a budgetedAgent) invocationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if a.timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, a.timeout)
}
