package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/leehosanganson/factory/internal/factory"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "factory:", err)
		os.Exit(1)
	}
}

func run(args []string, in io.Reader, out, errOut io.Writer) error {
	if len(args) > 0 {
		if len(args) > 1 && args[0] != "babysit" && args[0] != "pipeline" && args[0] != "clean" && args[0] != "--gate" {
			return fmt.Errorf("%s does not accept extra arguments", args[0])
		}
		switch args[0] {
		case "help", "-h", "--help":
			printHelp(out)
			return nil
		case "babysit":
			cfg, err := factory.LoadConfig("")
			if err != nil {
				return err
			}
			workdir, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("get current directory: %w", err)
			}
			return factory.BabysitCommand(args[1:], cfg, workdir, in, out, errOut)
		case "pipeline":
			gate, task, err := parseGate(args[1:])
			if err != nil {
				return err
			}
			ctx, stop := foregroundContext()
			defer stop()
			return runPipelineContext(ctx, task, gate, in, out)
		case "clean":
			gate, task, err := parseGate(args[1:])
			if err != nil {
				return err
			}
			if len(task) != 0 {
				return fmt.Errorf("factory clean accepts only --gate")
			}
			ctx, stop := foregroundContext()
			defer stop()
			return runCleanContext(ctx, gate, in, out, errOut)
		default:
			if args[0] == "--gate" {
				gate, task, err := parseGate(args)
				if err != nil {
					return err
				}
				ctx, stop := foregroundContext()
				defer stop()
				return runPipelineContext(ctx, task, gate, in, out)
			}
			return fmt.Errorf("unknown command %q (try factory help)", args[0])
		}
	}
	ctx, stop := foregroundContext()
	defer stop()
	return runPipelineContext(ctx, nil, false, in, out)
}

func foregroundContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func parseGate(args []string) (bool, []string, error) {
	gate := false
	task := make([]string, 0, len(args))
	for _, arg := range args {
		if arg == "--gate" {
			if gate {
				return false, nil, fmt.Errorf("--gate may only be specified once")
			}
			gate = true
			continue
		}
		task = append(task, arg)
	}
	return gate, task, nil
}

func runPipeline(args []string, gate bool, in io.Reader, out io.Writer) error {
	ctx, stop := foregroundContext()
	defer stop()
	return runPipelineContext(ctx, args, gate, in, out)
}

func runPipelineContext(ctx context.Context, args []string, gate bool, in io.Reader, out io.Writer) error {
	if file, ok := in.(*os.File); ok {
		in = &contextStdin{file: file, ctx: ctx}
	}
	return runPipelineTask(args, in, out, func(task string, workflowIn io.Reader) error {
		cfg, err := factory.LoadConfig("")
		if err != nil {
			return err
		}
		workdir, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("get target repository directory: %w", err)
		}
		stdout, outputIsFile := out.(*os.File)
		stdin, inputIsFile := inputFile(in)
		terminal := inputIsFile && outputIsFile && isTerminal(stdin) && isTerminal(stdout)
		workflow := factory.Workflow{Agent: factory.Runner{Config: cfg}, Config: cfg, In: workflowIn, Out: out, Workdir: filepath.Clean(workdir), Terminal: terminal, Gate: gate}
		return workflow.RunContext(ctx, task)
	})
}

func runClean(gate bool, in io.Reader, out, errOut io.Writer) error {
	ctx, stop := foregroundContext()
	defer stop()
	return runCleanContext(ctx, gate, in, out, errOut)
}

func runCleanContext(ctx context.Context, gate bool, in io.Reader, out, errOut io.Writer) error {
	if file, ok := in.(*os.File); ok {
		in = &contextStdin{file: file, ctx: ctx}
	}
	cfg, err := factory.LoadConfig("")
	if err != nil {
		return err
	}
	workdir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get target repository directory: %w", err)
	}
	stdout, outputIsFile := out.(*os.File)
	stdin, inputIsFile := inputFile(in)
	terminal := inputIsFile && outputIsFile && isTerminal(stdin) && isTerminal(stdout)
	if gate && !terminal {
		return fmt.Errorf("factory clean --gate requires an interactive terminal for approvals")
	}
	return (factory.CleanWorkflow{Agent: foregroundAgent{ctx: ctx, agent: factory.Runner{Config: cfg}}, Config: cfg, In: in, Out: out, Workdir: filepath.Clean(workdir), Terminal: terminal, Gate: gate}).RunContext(ctx, "")
}

type foregroundAgent struct {
	ctx   context.Context
	agent factory.Agent
}

func (a foregroundAgent) Run(stage, prompt, task, workdir, logPath string) error {
	return a.RunWithContext(a.ctx, stage, prompt, task, workdir, logPath)
}

func (a foregroundAgent) RunWithContext(_ context.Context, stage, prompt, task, workdir, logPath string) error {
	ctx := a.ctx
	if contextual, ok := a.agent.(interface {
		RunWithContext(context.Context, string, string, string, string, string) error
	}); ok {
		return contextual.RunWithContext(ctx, stage, prompt, task, workdir, logPath)
	}
	return fmt.Errorf("agent does not support context-aware execution")
}

func (a foregroundAgent) RunWithOutputContext(ctx context.Context, stage, prompt, task, workdir, logPath string) (string, error) {
	if contextual, ok := a.agent.(interface {
		RunWithOutputContext(context.Context, string, string, string, string, string) (string, error)
	}); ok {
		return contextual.RunWithOutputContext(ctx, stage, prompt, task, workdir, logPath)
	}
	return "", fmt.Errorf("evaluator agent does not provide stdout protocol output")
}

func runPipelineTask(args []string, in io.Reader, out io.Writer, run func(string, io.Reader) error) error {
	var task string
	workflowIn := in
	if len(args) > 0 {
		task = strings.Join(args, " ")
	} else {
		stdin, inputIsFile := inputFile(in)
		stdout, outputIsFile := out.(*os.File)
		if !inputIsFile || !outputIsFile || !isTerminal(stdin) || !isTerminal(stdout) {
			return fmt.Errorf("an interactive terminal is required; run factory pipeline from a terminal")
		}
		fmt.Fprintln(out, "Factory task (enter one line per paragraph; a line containing only . ends input):")
		if stdin, ok := in.(*contextStdin); ok {
			var err error
			task, err = readTaskContext(stdin, out)
			if err != nil {
				return err
			}
			workflowIn = stdin
		} else {
			reader := bufio.NewReader(in)
			var err error
			task, err = readTask(reader, out)
			if err != nil {
				return err
			}
			workflowIn = reader
		}
	}
	if strings.TrimSpace(task) == "" {
		return fmt.Errorf("task must not be empty")
	}
	return run(task, workflowIn)
}

func inputFile(in io.Reader) (*os.File, bool) {
	if file, ok := in.(*os.File); ok {
		return file, true
	}
	if stdin, ok := in.(*contextStdin); ok {
		return stdin.file, true
	}
	return nil, false
}

func readTaskContext(reader *contextStdin, out io.Writer) (string, error) {
	fmt.Fprintln(out, "Tip: describe the outcome you want and how it should be verified.")
	var lines []string
	for {
		line, err := reader.ReadLineContext(reader.ctx)
		trimmed := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if trimmed == "." {
			return strings.Join(lines, "\n"), nil
		}
		if line != "" {
			lines = append(lines, trimmed)
		}
		if err != nil {
			if err == io.EOF {
				return strings.Join(lines, "\n"), nil
			}
			return "", err
		}
	}
}

func readTask(reader *bufio.Reader, out io.Writer) (string, error) {
	fmt.Fprintln(out, "Tip: describe the outcome you want and how it should be verified.")
	var lines []string
	for {
		line, err := reader.ReadString('\n')
		if strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r") == "." {
			return strings.Join(lines, "\n"), nil
		}
		if line != "" {
			trimmed := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
			lines = append(lines, trimmed)
		}
		if err != nil {
			if err == io.EOF {
				return strings.Join(lines, "\n"), nil
			}
			return "", err
		}
	}
}

func printHelp(out io.Writer) {
	fmt.Fprintln(out, `factory - software factory workflows

Usage:
  factory pipeline [--gate] [description...]  Run the task workflow (approvals default off)
  factory clean [--gate]                     Review/fix/document and verify; pristine runs publish, dirty runs do not
  factory [--gate] [description...]           Alias for factory pipeline
  factory babysit   Start or manage detached PR babysitting
  factory help      Show this help
  factory -h        Show this help
  factory babysit <description>  Start detached PR monitoring
  factory babysit list           List active and recent jobs
  factory babysit describe <id>  Show job details, proposals, and actions
  factory babysit approve <id>   Approve a proposal with an exact scope
  factory babysit reject <id>    Reject a pending proposal
  factory babysit stop <id>      Request a stop and cancel active processes
  factory babysit reset <id>     Restart a recoverable-failure job

Pipeline successful-stage approvals and retry prompts are skipped by default. Use --gate
for human approval; evaluator PASS checks and four-attempt limits always apply. With no
arguments, pipeline task entry still requires an interactive terminal.
Clean requires a non-detached branch. Pristine mode applies to a clean worktree; it reviews, fixes,
documents, verifies, then commits generated changes and pushes existing local commits
and verified cleanup changes. It uses the configured upstream when available. Fallback
destination: origin/<branch>, only if that remote branch does not already exist. Upstream-ahead
and diverged branches are refused. A synced no-op creates no empty commit.

With staged, unstaged, or untracked changes, clean warns that agents and formatters may
affect them, then reviews, fixes, documents, and runs the same checks in dirty safe mode.
It does not stage, commit, or push any files in dirty mode, and does not require an
upstream or origin. Preserve or back up important local changes first. Neither mode is
a sandbox. This is separate from `+"`make clean`"+`, which removes local build artifacts.

Configuration: ${XDG_CONFIG_HOME:-~/.config}/factory/config.json
Foreground run state: ${XDG_STATE_HOME:-~/.local/state}/factory/runs
Babysit job state and logs: ${XDG_STATE_HOME:-~/.local/state}/factory/jobs
Babysit polling interval: FACTORY_BABYSIT_POLL_INTERVAL (default 30s)
Agent/evaluator timeout: config agent_timeout (default 60m; pipeline, clean, babysit)
GitHub snapshot timeout: 2m; clean verification commands are not covered; failure cap: 8 retries
Agent argument placeholders: {system_prompt}, {task}, {workdir}, {stage}

Default agent: pi -p --no-session --append-system-prompt {system_prompt} {task}`)
}
