package factory

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"
)

var workflowStages = []string{"requirements", "implement", "review", "document"}

const evaluatorOutputLimit = 8 * 1024

// ContextLineReader reads one line and can stop waiting when its context is canceled.
type ContextLineReader interface {
	ReadLineContext(context.Context) (string, error)
}

// Workflow coordinates fresh agent processes and human approval gates.
type Workflow struct {
	Agent           Agent
	Config          Config
	In              io.Reader
	Out             io.Writer
	Workdir         string
	Terminal        bool
	Gate            bool
	RequireComplete bool
	Stages          []string
	FinalApproval   string
}

// Run starts a persisted run and executes the complete human-gated workflow.
func (w Workflow) Run(task string) error {
	return w.RunContext(context.Background(), task)
}

// RunContext starts a persisted run and executes the workflow until completion or cancellation.
func (w Workflow) RunContext(ctx context.Context, task string) error {
	target, err := canonicalPath(w.Workdir)
	if err != nil {
		return fmt.Errorf("resolve target directory: %w", err)
	}
	w.Workdir = target
	root, err := StateRoot(w.Config.StateDir)
	if err != nil {
		return err
	}
	root, err = canonicalPath(root)
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
	fmt.Fprintf(w.Out, "Run: %s\n", safeProgressPath(runDir, progressLogLineLimit-5))
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
			state.Status = "interrupted"
			_ = writeState(runDir, state)
			return fmt.Errorf("workflow interrupted: %w", ctx.Err())
		}
		state.Stage = stage
		if err := writeState(runDir, state); err != nil {
			state.Status = "failed"
			_ = writeState(runDir, state)
			return err
		}
		passed, err := w.runStage(ctx, reader, runDir, task, stage)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				state.Status = "interrupted"
				_ = writeState(runDir, state)
				return fmt.Errorf("workflow interrupted: %w", err)
			}
			state.Status = "failed"
			_ = writeState(runDir, state)
			return err
		}
		if err := ctx.Err(); err != nil {
			state.Status = "interrupted"
			_ = writeState(runDir, state)
			return fmt.Errorf("workflow interrupted: %w", err)
		}
		if !passed {
			state.Status = "stopped"
			_ = writeState(runDir, state)
			if w.RequireComplete {
				return fmt.Errorf("workflow stopped before completion")
			}
			return nil
		}
	}
	if ctx.Err() != nil {
		state.Status = "interrupted"
		_ = writeState(runDir, state)
		return fmt.Errorf("workflow interrupted: %w", ctx.Err())
	}
	state.Status = "complete"
	if err := writeState(runDir, state); err != nil {
		state.Status = "failed"
		_ = writeState(runDir, state)
		return err
	}
	return nil
}

func (w Workflow) runStage(ctx context.Context, reader io.Reader, runDir, task, stage string) (bool, error) {
	var retryFeedback string
	for attempt := 1; attempt <= 4; attempt++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		prompt, err := LoadPrompt(w.Config.PromptDir, stage)
		if err != nil {
			return false, err
		}
		stageLog := filepath.Join(runDir, fmt.Sprintf("%02d-%s.log", attempt, stage))
		stageTask := task
		stageWorkdir := w.Workdir
		if stage == "requirements" {
			stageTask = fmt.Sprintf("Target repository: %s\n\nOriginal task:\n%s", w.Workdir, task)
			stageWorkdir = runDir
		}
		if retryFeedback != "" {
			stageTask += "\n\nFeedback from the previous attempt:\n" + retryFeedback
		}
		progress := startProgress(w.Out, w.Terminal, stage, attempt, stageLog)
		runErr := runWithProgress(progress, func() error {
			return runAgentWithContext(ctx, w.Agent, stage, prompt, stageTask, stageWorkdir, stageLog)
		})
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if runErr != nil {
			stageOutput := readLog(stageLog)
			fmt.Fprintf(w.Out, "%s\n", boundedOutput(stageOutput, evaluatorOutputLimit))
			fmt.Fprintf(w.Out, "Agent failed: %s\n", boundedOutput(runErr.Error(), evaluatorOutputLimit))
			if attempt == 4 {
				return false, fmt.Errorf("%s agent failed after attempt %d/4: %s", stage, attempt, boundedOutput(runErr.Error(), evaluatorOutputLimit))
			}
			if w.Gate {
				retry, err := askRetry(ctx, reader, w.Out, stage, attempt)
				if err != nil {
					return false, err
				}
				if !retry {
					return false, nil
				}
			}
			fmt.Fprintf(w.Out, "Retrying %s: agent invocation failed (%s); starting attempt %d/4 (maximum 4 attempts).\n", stage, boundedOutput(runErr.Error(), evaluatorOutputLimit), attempt+1)
			retryFeedback = fmt.Sprintf("Previous stage attempt failed.\nStage output/error log:\n%s\nAgent error: %s", boundedOutput(stageOutput, evaluatorOutputLimit), boundedOutput(runErr.Error(), evaluatorOutputLimit))
			continue
		}

		evaluatorTask := fmt.Sprintf("Original task:\n%s\n\nStage completed: %s\nStage output log: %s\n\nThe stage agent already exited successfully; otherwise this evaluator would not run. The workflow state remains status=running during evaluation by design, until evaluator acceptance is recorded. The evaluator process being active is expected. Do not count status=running or this evaluator being active alone as failure or incompleteness. Verify the requested work and stage output on substance against the original task and workflow requirements; keep those content and quality checks strict. Do not perform the stage. Report findings, then put PASS as the first non-empty stdout line only if the stage succeeded and materially satisfies its requirements; otherwise put FAIL first.", task, stage, stageLog)
		evaluatorPrompt, err := LoadPrompt(w.Config.PromptDir, "evaluate")
		if err != nil {
			return false, err
		}
		evaluatorLog := filepath.Join(runDir, fmt.Sprintf("%02d-evaluate-%s.log", attempt, stage))
		progress = startProgress(w.Out, w.Terminal, "evaluate "+stage, attempt, evaluatorLog)
		var protocolOutput []byte
		evalErr := runWithProgress(progress, func() error {
			var err error
			protocolOutput, err = runEvaluatorWithContext(ctx, w.Agent, evaluatorPrompt, evaluatorTask, w.Workdir, evaluatorLog)
			return err
		})
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		output := readLog(evaluatorLog)
		passed := protocolOutput != nil && evaluatorPassed(string(protocolOutput))
		if evalErr == nil && passed {
			fmt.Fprintf(w.Out, "Evaluation: PASS — %s; log: %s\n", stage, safeProgressPath(evaluatorLog, evaluatorOutputLimit-64))
		} else {
			if evalErr != nil {
				fmt.Fprintf(w.Out, "Evaluation: ERROR — %s: %s\n", stage, boundedOutput(evalErr.Error(), evaluatorOutputLimit))
			} else {
				fmt.Fprintf(w.Out, "Evaluation: FAIL — %s; first non-empty stdout line was not exactly PASS.\n", stage)
			}
			fmt.Fprintf(w.Out, "Evaluator findings/output:\n%s\nEvaluator log: %s\n", boundedOutput(output, evaluatorOutputLimit), safeProgressPath(evaluatorLog, evaluatorOutputLimit-20))
		}
		if evalErr != nil || !passed {
			if attempt == 4 {
				if evalErr != nil {
					return false, fmt.Errorf("%s evaluator failed after attempt %d/4: %s", stage, attempt, boundedOutput(evalErr.Error(), evaluatorOutputLimit))
				}
				return false, fmt.Errorf("%s evaluator rejected the stage after attempt %d/4", stage, attempt)
			}
			if w.Gate {
				retry, err := askRetry(ctx, reader, w.Out, stage, attempt)
				if err != nil {
					return false, err
				}
				if !retry {
					return false, nil
				}
			}
			reason := "evaluator rejected the stage"
			if evalErr != nil {
				reason = fmt.Sprintf("evaluator invocation failed (%s)", boundedOutput(evalErr.Error(), evaluatorOutputLimit))
			}
			fmt.Fprintf(w.Out, "Retrying %s: %s; starting attempt %d/4 (maximum 4 attempts).\n", stage, reason, attempt+1)
			retryFeedback = fmt.Sprintf("Evaluator rejected the previous attempt.\nStage output log:\n%s\nEvaluator output/findings:\n%s", boundedOutput(readLog(stageLog), evaluatorOutputLimit), boundedOutput(output, evaluatorOutputLimit))
			if evalErr != nil {
				retryFeedback += fmt.Sprintf("\nEvaluator error: %s", boundedOutput(evalErr.Error(), evaluatorOutputLimit))
			}
			continue
		}
		if w.Gate {
			next := "the next stage"
			if stage == "document" {
				next = "complete the workflow"
				if w.FinalApproval != "" {
					next = w.FinalApproval
				}
			}
			approved, err := askApproval(ctx, reader, w.Out, fmt.Sprintf("Evaluator passed for %s. Approve to %s? Type exactly yes: ", stage, next))
			if err != nil {
				return false, err
			}
			if !approved {
				return false, nil
			}
		}
		return true, nil
	}
	return false, nil
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

func runEvaluatorWithContext(ctx context.Context, agent Agent, prompt, task, workdir, logPath string) ([]byte, error) {
	if contextual, ok := agent.(outputContextAgent); ok {
		output, err := contextual.RunWithOutputContext(ctx, "evaluate", prompt, task, workdir, logPath)
		return []byte(output), err
	}
	return nil, fmt.Errorf("evaluator agent does not provide stdout protocol output")
}

func runAgentWithContext(ctx context.Context, agent Agent, stage, prompt, task, workdir, logPath string) error {
	if contextual, ok := agent.(contextAgent); ok {
		return contextual.RunWithContext(ctx, stage, prompt, task, workdir, logPath)
	}
	return fmt.Errorf("agent does not support context-aware execution")
}

func askRetry(ctx context.Context, reader io.Reader, out io.Writer, stage string, attempt int) (bool, error) {
	fmt.Fprintf(out, "%s attempt %d/4 did not pass. Retry this stage? Type exactly yes to retry; any other response stops the workflow: ", stage, attempt)
	answer, err := readPromptLine(ctx, reader)
	if err != nil && err != io.EOF {
		return false, err
	}
	return answer == "yes\n" || answer == "yes\r\n", nil
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

func evaluatorPassed(output string) bool {
	for _, line := range strings.Split(output, "\n") {
		if strings.TrimSpace(line) != "" {
			return line == "PASS"
		}
	}
	return false
}
