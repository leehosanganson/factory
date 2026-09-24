package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

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
			return runPipeline(task, gate, in, out)
		case "clean":
			gate, task, err := parseGate(args[1:])
			if err != nil {
				return err
			}
			if len(task) != 0 {
				return fmt.Errorf("factory clean accepts only --gate")
			}
			return runClean(gate, in, out, errOut)
		default:
			if args[0] == "--gate" {
				gate, task, err := parseGate(args)
				if err != nil {
					return err
				}
				return runPipeline(task, gate, in, out)
			}
			return fmt.Errorf("unknown command %q (try factory help)", args[0])
		}
	}
	return runPipeline(nil, false, in, out)
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
		stdin, inputIsFile := in.(*os.File)
		terminal := inputIsFile && outputIsFile && isTerminal(stdin) && isTerminal(stdout)
		workflow := factory.Workflow{Agent: factory.Runner{Config: cfg}, Config: cfg, In: workflowIn, Out: out, Workdir: filepath.Clean(workdir), Terminal: terminal, Gate: gate}
		return workflow.Run(task)
	})
}

func runClean(gate bool, in io.Reader, out, errOut io.Writer) error {
	cfg, err := factory.LoadConfig("")
	if err != nil {
		return err
	}
	workdir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get target repository directory: %w", err)
	}
	stdout, outputIsFile := out.(*os.File)
	stdin, inputIsFile := in.(*os.File)
	terminal := inputIsFile && outputIsFile && isTerminal(stdin) && isTerminal(stdout)
	if gate && !terminal {
		return fmt.Errorf("factory clean --gate requires an interactive terminal for approvals")
	}
	return (factory.CleanWorkflow{Agent: factory.Runner{Config: cfg}, Config: cfg, In: in, Out: out, Workdir: filepath.Clean(workdir), Terminal: terminal, Gate: gate}).Run("")
}

func runPipelineTask(args []string, in io.Reader, out io.Writer, run func(string, io.Reader) error) error {
	var task string
	workflowIn := in
	if len(args) > 0 {
		task = strings.Join(args, " ")
	} else {
		stdin, inputIsFile := in.(*os.File)
		stdout, outputIsFile := out.(*os.File)
		if !inputIsFile || !outputIsFile || !isTerminal(stdin) || !isTerminal(stdout) {
			return fmt.Errorf("an interactive terminal is required; run factory pipeline from a terminal")
		}
		fmt.Fprintln(out, "Factory task (enter one line per paragraph; a line containing only . ends input):")
		reader := bufio.NewReader(in)
		var err error
		task, err = readTask(reader, out)
		if err != nil {
			return err
		}
		workflowIn = reader
	}
	if strings.TrimSpace(task) == "" {
		return fmt.Errorf("task must not be empty")
	}
	return run(task, workflowIn)
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
  factory clean [--gate]                     Review, fix, document, verify, commit, and push
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
Clean requires a pristine worktree on a non-detached branch whose configured upstream
is an ancestor of local HEAD; upstream-ahead and diverged branches are refused. After
review and checks, it pushes existing local commits and any verified cleanup commit.
A synced no-op creates no empty commit. It never changes upstream or force-pushes. This is
separate from `+"`make clean`"+`, which removes local build artifacts.

Configuration: ${XDG_CONFIG_HOME:-~/.config}/factory/config.json
Foreground run state: ${XDG_STATE_HOME:-~/.local/state}/factory/runs
Babysit job state and logs: ${XDG_STATE_HOME:-~/.local/state}/factory/jobs
Babysit polling interval: FACTORY_BABYSIT_POLL_INTERVAL (default 30s)
Babysit agent/evaluator timeout: 5m; GitHub snapshot failure cap: 8 retries
Agent argument placeholders: {system_prompt}, {task}, {workdir}, {stage}

Default agent: pi -p --no-session --append-system-prompt {system_prompt} {task}`)
}
