package factory

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/leehosanganson/factory/internal/workflow"
)

// Compatibility aliases keep CLI-local job and monitor code on their existing package
// while shared execution moves to internal/workflow.
type (
	Config                       = workflow.Config
	ParallelImplementationConfig = workflow.ParallelImplementationConfig
	Workflow                     = workflow.Workflow
	WorkflowEvent                = workflow.WorkflowEvent
	WorkflowObserver             = workflow.WorkflowObserver
	WorkflowObserverFunc         = workflow.WorkflowObserverFunc
	WorkflowExecutionHook        = workflow.WorkflowExecutionHook
	PipelineCheckRunner          = workflow.PipelineCheckRunner
	ProcessPipelineCheckRunner   = workflow.ProcessPipelineCheckRunner
	Agent                        = workflow.Agent
	Runner                       = workflow.Runner
	State                        = workflow.State
	StageRecord                  = workflow.StageRecord
	CheckResult                  = workflow.CheckResult
	SubtaskRecord                = workflow.SubtaskRecord
	WorkflowEventRecord          = workflow.WorkflowEventRecord
	ContextLineReader            = workflow.ContextLineReader
	BoundedOutputWriter          = workflow.BoundedOutputWriter
)

const (
	maxDetachedJobConcurrency = 64
	maxParallelSubtasks       = workflow.MaxParallelSubtasks
	evaluatorOutputLimit      = workflow.EvaluatorOutputLimit
	outputTruncatedMarker     = workflow.OutputTruncatedMarker
	progressLogLineLimit      = workflow.ProgressLogLineLimit
)

func DefaultConfig() Config                  { return workflow.DefaultConfig() }
func ConfigPath() (string, error)            { return workflow.ConfigPath() }
func LoadConfig(path string) (Config, error) { return workflow.LoadConfig(path) }

var pipelineCheckOpenFile = func(path string) (*os.File, error) { return workflow.PipelineCheckOpenFile(path) }
var pipelineCheckCloseFile = func(file *os.File) error { return workflow.PipelineCheckCloseFile(file) }

func NewBoundedOutputWriter(ctx context.Context, dst io.Writer, limit int) *BoundedOutputWriter {
	return workflow.NewBoundedOutputWriter(ctx, dst, limit)
}
func StateRoot(override string) (string, error) { return workflow.StateRoot(override) }
func newUUIDv4() (string, error)                { return workflow.NewUUIDv4() }
func sanitizeProgressLine(line string) string   { return workflow.SanitizeProgressLine(line) }
func sanitizeHumanDirection(text string) string { return workflow.SanitizeHumanDirection(text) }

type workflowEventRecord = workflow.WorkflowEventRecord

func resolvedPath(path string) (string, error) { return workflow.ResolvedPath(path) }
func isWithin(parent, path string) bool        { return workflow.IsWithin(parent, path) }
func createRun(root, workdir, task string) (string, *State, error) {
	return workflow.CreateRun(root, workdir, task)
}
func writeState(dir string, state *State) error { return workflow.WriteState(dir, state) }
func persistWorkflowEvent(runDir string, event WorkflowEvent) error {
	return workflow.PersistWorkflowEvent(runDir, event)
}
func sanitizeSecondaryStatus(status string) string { return workflow.SanitizeSecondaryStatus(status) }
func terminalSafeText(text string) string          { return workflow.TerminalSafeText(text) }
func validStage(stage string) bool                 { return workflow.ValidStage(stage) }

type processPipelineCheckRunner = workflow.ProcessPipelineCheckRunner

func runPipelineChecks(ctx context.Context, checks [][]string, workdir, runDir string, state *State, observe func(WorkflowEvent) error, runner PipelineCheckRunner) error {
	return workflow.RunPipelineChecks(ctx, checks, workdir, runDir, state, observe, runner)
}

func boundedOutput(output string, limit int) string { return workflow.BoundedOutput(output, limit) }
func newStageExecutionBudget(limit time.Duration) *workflow.StageExecutionBudget {
	return workflow.NewStageExecutionBudget(limit)
}

var errStageExecutionBudgetExhausted = workflow.ErrStageExecutionBudgetExhausted

const defaultStageExecutionBudget = 30 * time.Minute
const maxSubtaskPlanBytes = 64 * 1024
const stdoutProtocolCaptureLimit = maxSubtaskPlanBytes
const secondaryStatusLimit = 1024

func askApproval(ctx context.Context, reader io.Reader, out io.Writer, question string) (bool, error) {
	return workflow.AskApproval(ctx, reader, out, question)
}
func runAgentWithContext(ctx context.Context, agent Agent, stage, prompt, task, workdir, logPath string) error {
	return workflow.RunAgentWithContext(ctx, agent, stage, prompt, task, workdir, logPath)
}
func runAgentWithOutputContext(ctx context.Context, agent Agent, stage, prompt, task, workdir, logPath string) (string, error) {
	return workflow.RunAgentWithOutputContext(ctx, agent, stage, prompt, task, workdir, logPath)
}
func LoadPrompt(promptDir, stage string) (string, error) {
	return workflow.LoadPrompt(promptDir, stage)
}
func RunAutomaticImplementation(ctx context.Context, cfg Config, target, task string, in io.Reader, out io.Writer, terminal, gate bool) error {
	return runAutomaticImplementation(ctx, cfg, target, task, in, out, terminal, gate)
}
