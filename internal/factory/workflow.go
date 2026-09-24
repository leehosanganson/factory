package factory

import (
	"bufio"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

var workflowStages = []string{"requirements", "implement", "review", "document"}

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
	fmt.Fprintf(w.Out, "Run: %s\n", runDir)
	reader := bufio.NewReader(w.In)
	stages := w.Stages
	if stages == nil {
		stages = workflowStages
	}
	for _, stage := range stages {
		state.Stage = stage
		if err := writeState(runDir, state); err != nil {
			return err
		}
		passed, err := w.runStage(reader, runDir, task, stage)
		if err != nil {
			state.Status = "stopped"
			_ = writeState(runDir, state)
			return err
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
	state.Status = "complete"
	return writeState(runDir, state)
}

func (w Workflow) runStage(reader *bufio.Reader, runDir, task, stage string) (bool, error) {
	var retryFeedback string
	for attempt := 1; attempt <= 4; attempt++ {
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
		runErr := w.Agent.Run(stage, prompt, stageTask, stageWorkdir, stageLog)
		progress.finish(runErr)
		if runErr != nil {
			stageOutput := readLog(stageLog)
			copyOutput(w.Out, stageOutput)
			fmt.Fprintf(w.Out, "Agent failed: %v\n", runErr)
			if attempt == 4 || (w.Gate && !askRetry(reader, w.Out, stage, attempt)) {
				return false, nil
			}
			retryFeedback = fmt.Sprintf("Previous stage attempt failed.\nStage output/error log:\n%s\nAgent error: %v", stageOutput, runErr)
			continue
		}

		evaluatorTask := fmt.Sprintf("Original task:\n%s\n\nStage completed: %s\nStage output log: %s\n\nInspect the target repository and stage output log. Verify the stage against the original task and workflow requirements. Do not perform the stage. Report findings, then put PASS as the first non-empty output line only if the stage succeeded and materially satisfies its requirements; otherwise put FAIL first.", task, stage, stageLog)
		evaluatorPrompt, err := LoadPrompt(w.Config.PromptDir, "evaluate")
		if err != nil {
			return false, err
		}
		evaluatorLog := filepath.Join(runDir, fmt.Sprintf("%02d-evaluate-%s.log", attempt, stage))
		progress = startProgress(w.Out, w.Terminal, "evaluate "+stage, attempt, evaluatorLog)
		evalErr := w.Agent.Run("evaluate", evaluatorPrompt, evaluatorTask, w.Workdir, evaluatorLog)
		progress.finish(evalErr)
		output := readLog(evaluatorLog)
		copyOutput(w.Out, output)
		passed := evaluatorPassed(output)
		if evalErr != nil {
			fmt.Fprintf(w.Out, "Evaluator for %s failed: %v\n", stage, evalErr)
		} else if !passed {
			fmt.Fprintf(w.Out, "Evaluator rejected %s: the first non-empty output line was not exactly PASS.\n", stage)
		}
		if evalErr != nil || !passed {
			if attempt == 4 || (w.Gate && !askRetry(reader, w.Out, stage, attempt)) {
				return false, nil
			}
			retryFeedback = fmt.Sprintf("Evaluator rejected the previous attempt.\nStage output log:\n%s\nEvaluator output/findings:\n%s", readLog(stageLog), output)
			if evalErr != nil {
				retryFeedback += fmt.Sprintf("\nEvaluator error: %v", evalErr)
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
			if !askApproval(reader, w.Out, fmt.Sprintf("Evaluator passed for %s. Approve to %s? Type exactly yes: ", stage, next)) {
				return false, nil
			}
		}
		return true, nil
	}
	return false, nil
}

func askRetry(reader *bufio.Reader, out io.Writer, stage string, attempt int) bool {
	fmt.Fprintf(out, "%s attempt %d/4 did not pass. Retry this stage? Type exactly yes to retry; any other response stops the workflow: ", stage, attempt)
	answer, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return false
	}
	return answer == "yes\n" || answer == "yes\r\n"
}

func askApproval(reader *bufio.Reader, out io.Writer, question string) bool {
	fmt.Fprint(out, question)
	answer, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return false
	}
	return answer == "yes\n" || answer == "yes\r\n"
}

func evaluatorPassed(output string) bool {
	for _, line := range strings.Split(output, "\n") {
		if strings.TrimSpace(line) != "" {
			return line == "PASS"
		}
	}
	return false
}
