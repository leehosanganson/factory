package factory

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStageExecutionBudgetAccumulatesOnlyActiveAgentTime(t *testing.T) {
	budget := newStageExecutionBudget(120 * time.Millisecond)
	for _, duration := range []time.Duration{45 * time.Millisecond, 45 * time.Millisecond} {
		if err := budget.invoke(context.Background(), func(context.Context) error {
			time.Sleep(duration)
			return nil
		}); err != nil {
			t.Fatalf("agent invocation failed before cumulative budget: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	called := false
	err := budget.invoke(context.Background(), func(ctx context.Context) error {
		called = true
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, errStageExecutionBudgetExhausted) || !called {
		t.Fatalf("third invocation error/called = %v/%v, want active invocation canceled at remaining budget", err, called)
	}
}

func TestStageExecutionBudgetParallelCallsShareActiveUnion(t *testing.T) {
	budget := newStageExecutionBudget(100 * time.Millisecond)
	started := make(chan struct{}, 2)
	finish := make(chan struct{})
	var workers sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			results <- budget.invoke(context.Background(), func(ctx context.Context) error {
				started <- struct{}{}
				select {
				case <-finish:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		}()
	}
	<-started
	<-started
	startedAt := time.Now()
	for !budget.isExhausted() {
		time.Sleep(time.Millisecond)
	}
	if elapsed := time.Since(startedAt); elapsed < 80*time.Millisecond || elapsed > 250*time.Millisecond {
		t.Fatalf("shared parallel budget exhausted after %s, want approximately one 100ms active interval", elapsed)
	}
	close(finish)
	workers.Wait()
	close(results)
	for err := range results {
		if !errors.Is(err, errStageExecutionBudgetExhausted) {
			t.Errorf("parallel invocation error = %v, want shared budget exhaustion", err)
		}
	}
}

func TestBudgetedAgentPreservesConfiguredPerInvocationTimeout(t *testing.T) {
	budget := newStageExecutionBudget(time.Second)
	agent := budgetedAgent{agent: contextWaitingAgent{}, budget: budget, timeout: 40 * time.Millisecond}
	started := time.Now()
	err := agent.RunWithContext(context.Background(), "requirements", "", "", t.TempDir(), filepath.Join(t.TempDir(), "agent.log"))
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errStageExecutionBudgetExhausted) {
		t.Fatalf("invocation error = %v, want configured invocation timeout independent of stage budget", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("configured invocation timeout took %s", elapsed)
	}
}

func TestStageExecutionBudgetCancelsInvocationAtLimit(t *testing.T) {
	budget := newStageExecutionBudget(40 * time.Millisecond)
	started := time.Now()
	err := budget.invoke(context.Background(), func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, errStageExecutionBudgetExhausted) {
		t.Fatalf("invocation error = %v, want cumulative budget exhaustion", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("budget cancellation took %s, want prompt cancellation", elapsed)
	}
	if !budget.isExhausted() {
		t.Fatal("budget did not remain exhausted after canceling active invocation")
	}
}

func TestStageExecutionBudgetClampsLimitAndExcludesInactivePeriods(t *testing.T) {
	budget := newStageExecutionBudget(2 * defaultStageExecutionBudget)
	if budget.remaining != defaultStageExecutionBudget {
		t.Fatalf("budget = %s, want fixed maximum %s", budget.remaining, defaultStageExecutionBudget)
	}
	budget = newStageExecutionBudget(80 * time.Millisecond)
	if err := budget.invoke(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if budget.isExhausted() || budget.remaining <= 0 {
		t.Fatalf("inactive interval consumed execution budget: remaining=%s exhausted=%v", budget.remaining, budget.exhausted)
	}
}
