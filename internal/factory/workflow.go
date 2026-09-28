package factory

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

var workflowStages = []string{"requirements", "implement", "review", "document"}

const evaluatorOutputLimit = 8 * 1024

var pipelineCheckOpenFile = func(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
}
var pipelineCheckCloseFile = func(file *os.File) error { return file.Close() }

// ContextLineReader reads one line and can stop waiting when its context is canceled.
type ContextLineReader interface {
	ReadLineContext(context.Context) (string, error)
}

// WorkflowEvent describes one observable workflow lifecycle transition.
type WorkflowEvent struct {
	RunID      string
	Type       string
	Stage      string
	Message    string
	Command    []string
	Transcript string
	Outcome    string
	ExitCode   *int
	StartedAt  time.Time
	EndedAt    time.Time
}

type workflowEventRecord struct {
	Version    int        `json:"version"`
	Timestamp  time.Time  `json:"timestamp"`
	RunID      string     `json:"runID"`
	Type       string     `json:"type"`
	Stage      string     `json:"stage,omitempty"`
	Message    string     `json:"message,omitempty"`
	Command    []string   `json:"command,omitempty"`
	Transcript string     `json:"transcript,omitempty"`
	Outcome    string     `json:"outcome,omitempty"`
	ExitCode   *int       `json:"exit_code,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	EndedAt    *time.Time `json:"ended_at,omitempty"`
}

// WorkflowObserver receives durable-session lifecycle events. Implementations
// should return an error when an event could not be recorded.
type WorkflowObserver interface {
	ObserveWorkflowEvent(WorkflowEvent) error
}

// WorkflowObserverFunc adapts a function to WorkflowObserver.
type WorkflowObserverFunc func(WorkflowEvent) error

func (f WorkflowObserverFunc) ObserveWorkflowEvent(event WorkflowEvent) error { return f(event) }

// Workflow coordinates fresh agent processes and human approval gates.
type Workflow struct {
	Agent            Agent
	Config           Config
	In               io.Reader
	Out              io.Writer
	Workdir          string
	Terminal         bool
	Gate             bool
	RequireComplete  bool
	Stages           []string
	FinalApproval    string
	Observer         WorkflowObserver
	ProcessObserver  func(string, int, bool)
	DeferCompletion  bool
	Managed          bool
	RunCreated       func(runDir, runID string)
	stageBudgetLimit time.Duration
	statusInterval   time.Duration
	statusCall       func(context.Context, string, string, string, string) (string, error)
}

// Run starts a persisted run and executes the complete human-gated workflow.
func (w Workflow) Run(task string) error {
	return w.RunContext(context.Background(), task)
}

// RunContext starts a persisted run and executes the workflow until completion or cancellation.
func (w Workflow) RunContext(ctx context.Context, task string) error {
	if w.Managed && !w.Gate {
		return fmt.Errorf("managed run controls are reserved for gated foreground workflows")
	}
	if err := validatePipelineChecks(w.Config.PipelineChecks); err != nil {
		return err
	}
	target, err := resolvedPath(w.Workdir)
	if err != nil {
		return fmt.Errorf("resolve target directory: %w", err)
	}
	w.Workdir = target
	root, err := StateRoot(w.Config.StateDir)
	if err != nil {
		return err
	}
	root, err = resolvedPath(root)
	if err != nil {
		return fmt.Errorf("resolve state directory: %w", err)
	}
	if isWithin(target, root) {
		return fmt.Errorf("state directory %s must be outside target directory %s", root, target)
	}
	runDir, state, err := createRun(root, w.Workdir, task)
	if err != nil {
		return err
	}
	if w.RunCreated != nil {
		w.RunCreated(runDir, state.ID)
	}
	state.Managed = w.Managed
	if err := writeState(runDir, state); err != nil {
		return err
	}
	var managed *managedRun
	if w.Managed {
		managed, err = startManagedRun(ctx, runDir)
		if err != nil {
			state.Status = "failed"
			stateErr := writeState(runDir, state)
			eventErr := persistWorkflowEvent(runDir, WorkflowEvent{
				RunID: state.ID, Type: "workflow.transition", Message: "failed", Outcome: "failure",
			})
			return errors.Join(err, stateErr, eventErr)
		}
		defer managed.close()
		ctx = managed.ctx
	}
	fmt.Fprintf(w.Out, "Run: %s\n", safeProgressPath(runDir, progressLogLineLimit-5))
	observe := func(event WorkflowEvent) error {
		if err := persistWorkflowEvent(runDir, event); err != nil {
			return err
		}
		if w.Observer != nil {
			return w.Observer.ObserveWorkflowEvent(event)
		}
		return nil
	}
	transition := func(status string) error {
		state.Status = status
		writeErr := writeState(runDir, state)
		eventErr := observe(WorkflowEvent{RunID: state.ID, Type: "workflow.transition", Stage: state.Stage, Message: status})
		if writeErr != nil {
			return writeErr
		}
		return eventErr
	}
	initialEvent := WorkflowEvent{RunID: state.ID, Type: "workflow.transition", Message: state.Status}
	if err := observe(initialEvent); err != nil {
		state.Status = "failed"
		writeErr := writeState(runDir, state)
		failedEvent := initialEvent
		failedEvent.Message = state.Status
		eventErr := persistWorkflowEvent(runDir, failedEvent)
		return errors.Join(err, writeErr, eventErr)
	}
	fail := func(err error) error {
		status := "failed"
		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			status = "interrupted"
		}
		if w.Managed && managedStopRequested(runDir) {
			status = "stopped"
		}
		if transitionErr := transition(status); transitionErr != nil {
			return errors.Join(err, transitionErr)
		}
		return err
	}
	var reader io.Reader = bufio.NewReader(w.In)
	if _, ok := w.In.(ContextLineReader); ok {
		reader = w.In
	}
	stages := w.Stages
	if stages == nil {
		stages = workflowStages
	}
	for _, stage := range stages {
		if ctx.Err() != nil {
			return fail(fmt.Errorf("workflow interrupted: %w", ctx.Err()))
		}
		state.Stage = stage
		state.Stages = append(state.Stages, StageRecord{
			Name: stage, Status: "running", StartedAt: time.Now().UTC(),
		})
		stageRecord := &state.Stages[len(state.Stages)-1]
		if err := writeState(runDir, state); err != nil {
			return fail(err)
		}
		passed, err := w.runStage(ctx, reader, runDir, task, stage, state, observe)
		if err != nil {
			stageRecord.Status = "failed"
			if (ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && !errors.Is(err, errStageExecutionBudgetExhausted) {
				stageRecord.Status = "interrupted"
			}
			stageRecord.EndedAt = time.Now().UTC()
			if writeErr := writeState(runDir, state); writeErr != nil {
				err = errors.Join(err, writeErr)
			}
			if (ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && !errors.Is(err, errStageExecutionBudgetExhausted) {
				return fail(fmt.Errorf("workflow interrupted: %w", err))
			}
			return fail(err)
		}
		if err := ctx.Err(); err != nil {
			if stageRecord.Status != "failed" {
				stageRecord.Status = "interrupted"
				stageRecord.EndedAt = time.Now().UTC()
			}
			if writeErr := writeState(runDir, state); writeErr != nil {
				err = errors.Join(err, writeErr)
			}
			return fail(fmt.Errorf("workflow interrupted: %w", err))
		}
		if !passed {
			stageRecord.Status = "stopped"
			stageRecord.EndedAt = time.Now().UTC()
			if err := writeState(runDir, state); err != nil {
				return fail(err)
			}
			if err := transition("stopped"); err != nil {
				return err
			}
			if w.RequireComplete {
				return fmt.Errorf("workflow stopped before completion")
			}
			return nil
		}
		stageRecord.Status = "passed"
		stageRecord.EndedAt = time.Now().UTC()
		if err := writeState(runDir, state); err != nil {
			return fail(err)
		}
	}
	if ctx.Err() != nil {
		return fail(fmt.Errorf("workflow interrupted: %w", ctx.Err()))
	}
	if !w.DeferCompletion {
		if err := runPipelineChecks(ctx, w.Config.PipelineChecks, w.Workdir, runDir, state, observe); err != nil {
			return fail(err)
		}
	}
	if w.DeferCompletion {
		return nil
	}
	if err := transition("complete"); err != nil {
		return fail(err)
	}
	return nil
}

func runPipelineChecks(ctx context.Context, checks [][]string, workdir, runDir string, state *State, observe func(WorkflowEvent) error) error {
	for i, args := range checks {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("pipeline check interrupted: %w", err)
		}
		started := time.Now().UTC()
		logName := fmt.Sprintf("pipeline-check-%02d.log", i+1)
		logPath := filepath.Join(runDir, logName)
		startedEvent := WorkflowEvent{
			RunID: state.ID, Type: "check.started", Stage: args[0],
			Command: append([]string(nil), args...), Transcript: logName, StartedAt: started,
		}
		if err := observe(startedEvent); err != nil {
			return fmt.Errorf("persist pipeline check start: %w", err)
		}
		log, err := pipelineCheckOpenFile(logPath)
		if err != nil {
			ended := time.Now().UTC()
			exitCode := -1
			state.Checks = append(state.Checks, CheckResult{
				Command: append([]string(nil), args...), Log: logName, ExitCode: exitCode,
				StartedAt: started, EndedAt: ended,
			})
			stateErr := writeState(runDir, state)
			completedEvent := WorkflowEvent{
				RunID: state.ID, Type: "check.completed", Stage: args[0],
				Command: append([]string(nil), args...), Transcript: logName, Outcome: "failure",
				ExitCode: &exitCode, StartedAt: started, EndedAt: ended,
			}
			eventErr := observe(completedEvent)
			return errors.Join(fmt.Errorf("create pipeline check log %s: %w", logName, err), stateErr, eventErr)
		}
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		configureProcessCancellation(cmd)
		cmd.Dir = workdir
		cmd.Stdout = log
		cmd.Stderr = log
		runErr := cmd.Run()
		closeErr := pipelineCheckCloseFile(log)
		ended := time.Now().UTC()
		exitCode := 0
		if runErr != nil {
			exitCode = -1
			var exitErr *exec.ExitError
			if errors.As(runErr, &exitErr) {
				exitCode = exitErr.ExitCode()
			}
		}
		if closeErr != nil && runErr == nil {
			exitCode = -1
		}
		state.Checks = append(state.Checks, CheckResult{
			Command: append([]string(nil), args...), Log: logName, ExitCode: exitCode,
			StartedAt: started, EndedAt: ended,
		})
		if writeErr := writeState(runDir, state); writeErr != nil {
			return errors.Join(runErr, closeErr, fmt.Errorf("persist pipeline check result: %w", writeErr))
		}
		outcome := "success"
		if runErr != nil || closeErr != nil {
			outcome = "failure"
			if closeErr == nil && (ctx.Err() != nil || errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded)) {
				outcome = "canceled"
			}
		}
		completedEvent := WorkflowEvent{
			RunID: state.ID, Type: "check.completed", Stage: args[0],
			Command: append([]string(nil), args...), Transcript: logName, Outcome: outcome,
			ExitCode: &exitCode, StartedAt: started, EndedAt: ended,
		}
		if err := observe(completedEvent); err != nil {
			return errors.Join(runErr, closeErr, fmt.Errorf("persist pipeline check completion: %w", err))
		}
		if closeErr != nil {
			return fmt.Errorf("close pipeline check transcript %s: %w", logPath, closeErr)
		}
		if runErr != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("pipeline check %q interrupted (transcript: %s): %w", args[0], logPath, ctx.Err())
			}
			return fmt.Errorf("pipeline check %q failed (exit code %d; transcript: %s): %w", args[0], exitCode, logPath, runErr)
		}
	}
	return nil
}

func optionalTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

func persistWorkflowEvent(runDir string, event WorkflowEvent) error {
	record := workflowEventRecord{
		Version: 1, Timestamp: time.Now().UTC(), RunID: event.RunID,
		Type: event.Type, Stage: event.Stage, Message: event.Message,
		Command: event.Command, Transcript: event.Transcript, Outcome: event.Outcome,
		ExitCode: event.ExitCode, StartedAt: optionalTime(event.StartedAt), EndedAt: optionalTime(event.EndedAt),
	}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal workflow event: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(runDir, "workflow-events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open workflow event log: %w", err)
	}
	_, writeErr := file.Write(append(data, '\n'))
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("write workflow event: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close workflow event log: %w", closeErr)
	}
	return nil
}

func (w Workflow) runStage(ctx context.Context, reader io.Reader, runDir, task, stage string, state *State, observe func(WorkflowEvent) error) (bool, error) {
	budget := newStageExecutionBudget(w.stageBudgetLimit)
	agentTimeout, err := w.Config.agentTimeout()
	if err != nil {
		return false, err
	}
	stageAgent := budgetedAgent{agent: w.Agent, budget: budget, timeout: agentTimeout}
	prompt, err := LoadPrompt(w.Config.PromptDir, stage)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if budget.isExhausted() {
		return false, fmt.Errorf("%s exceeded its cumulative agent execution budget: %w", stage, errStageExecutionBudgetExhausted)
	}
	stageLog := filepath.Join(runDir, fmt.Sprintf("%02d-%s.log", len(state.Stages), stage))
	stageTask := task
	stageWorkdir := w.Workdir
	if stage == "requirements" {
		stageTask = fmt.Sprintf("Target repository: %s\n\nOriginal task:\n%s", w.Workdir, task)
		stageWorkdir = runDir
	}
	if err := observe(WorkflowEvent{RunID: filepath.Base(runDir), Type: "stage.started", Stage: stage, Message: stageLog}); err != nil {
		return false, err
	}
	progress := startWorkflowProgress(w.Out, w.Terminal, stage, stageLog, runDir)
	runErr := runWithProgress(progress, func() error {
		statusWorkflow := w
		statusWorkflow.ProcessObserver = w.ProcessObserver
		return runWithSecondaryStatus(ctx, statusWorkflow, progress, stage, stageTask, stageWorkdir, stageLog, observe, func(primaryCtx context.Context) error {
			if stage == "implement" {
				parallelWorkflow := w
				parallelWorkflow.Agent = stageAgent
				used, err := parallelWorkflow.runParallelImplementation(primaryCtx, stageTask, stageLog, runDir, state, observe)
				if used || err != nil {
					return err
				}
			}
			return runAgentWithContext(primaryCtx, stageAgent, stage, prompt, stageTask, stageWorkdir, stageLog)
		})
	})
	if runErr != nil {
		if err := observe(WorkflowEvent{RunID: filepath.Base(runDir), Type: "stage.failed", Stage: stage, Message: boundedOutput(runErr.Error()+"\n"+stageLog, evaluatorOutputLimit)}); err != nil {
			return false, err
		}
	} else if err := observe(WorkflowEvent{RunID: filepath.Base(runDir), Type: "stage.completed", Stage: stage, Message: stageLog}); err != nil {
		return false, err
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if runErr != nil {
		if errors.Is(runErr, errStageExecutionBudgetExhausted) {
			return false, fmt.Errorf("%s exceeded its cumulative agent execution budget: %w", stage, runErr)
		}
		stageOutput := readLog(stageLog)
		fmt.Fprintf(w.Out, "%s\n", boundedOutput(stageOutput, evaluatorOutputLimit))
		fmt.Fprintf(w.Out, "Agent failed: %s\n", boundedOutput(runErr.Error(), evaluatorOutputLimit))
		return false, fmt.Errorf("%s agent failed: %s", stage, boundedOutput(runErr.Error(), evaluatorOutputLimit))
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if w.Gate {
		next := "the next stage"
		if stage == "document" {
			next = "complete the workflow"
			if w.FinalApproval != "" {
				next = w.FinalApproval
			}
		}
		approved, err := askApproval(ctx, reader, w.Out, fmt.Sprintf("Agent completed %s. Approve to %s? Type exactly yes: ", stage, next))
		if err != nil {
			return false, err
		}
		if !approved {
			return false, nil
		}
	}
	return true, nil
}

func runWithProgress(progress *stageProgress, run func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			progress.finish(errors.New("invocation panicked"))
			panic(recovered)
		}
		progress.finish(err)
	}()
	return run()
}

type contextAgent interface {
	RunWithContext(context.Context, string, string, string, string, string) error
}

type outputContextAgent interface {
	RunWithOutputContext(context.Context, string, string, string, string, string) (string, error)
}

func runAgentWithOutputContext(ctx context.Context, agent Agent, stage, prompt, task, workdir, logPath string) (string, error) {
	if contextual, ok := agent.(outputContextAgent); ok {
		return contextual.RunWithOutputContext(ctx, stage, prompt, task, workdir, logPath)
	}
	return "", fmt.Errorf("agent does not provide stdout protocol output")
}

func runAgentWithContext(ctx context.Context, agent Agent, stage, prompt, task, workdir, logPath string) error {
	if contextual, ok := agent.(contextAgent); ok {
		return contextual.RunWithContext(ctx, stage, prompt, task, workdir, logPath)
	}
	return fmt.Errorf("agent does not support context-aware execution")
}

func askApproval(ctx context.Context, reader io.Reader, out io.Writer, question string) (bool, error) {
	fmt.Fprint(out, question)
	answer, err := readPromptLine(ctx, reader)
	if err != nil && err != io.EOF {
		return false, err
	}
	return answer == "yes\n" || answer == "yes\r\n", nil
}

func readPromptLine(ctx context.Context, reader io.Reader) (string, error) {
	if contextual, ok := reader.(ContextLineReader); ok {
		return contextual.ReadLineContext(ctx)
	}
	buffered, ok := reader.(*bufio.Reader)
	if !ok {
		buffered = bufio.NewReader(reader)
	}
	return buffered.ReadString('\n')
}

func boundedOutput(output string, limit int) string {
	clean := terminalSafeText(output)
	if limit <= 0 {
		return ""
	}
	if len(clean) <= limit {
		return clean
	}
	const marker = "\n… output truncated …"
	if limit <= len(marker) {
		markerRunes := []rune(marker)
		for len(string(markerRunes)) > limit {
			markerRunes = markerRunes[:len(markerRunes)-1]
		}
		return string(markerRunes)
	}
	prefix := clean[:limit-len(marker)]
	for !utf8.ValidString(prefix) {
		prefix = prefix[:len(prefix)-1]
	}
	return prefix + marker
}

func terminalSafeText(text string) string {
	var clean strings.Builder
	for i := 0; i < len(text); {
		if text[i] == '\x1b' {
			i = skipProgressEscape(text, i)
			continue
		}
		r, size := utf8.DecodeRuneInString(text[i:])
		if r == 0x9b || r == 0x9d || r == 0x9c || r == 0x90 || r == 0x98 || r == 0x9e || r == 0x9f {
			i = skipProgressC1(text, i, r, size)
			continue
		}
		i += size
		if r == '\n' || r == '\t' || !unicode.IsControl(r) {
			clean.WriteRune(r)
		}
	}
	return clean.String()
}
