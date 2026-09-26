package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"unicode"

	"github.com/leehosanganson/factory/internal/factory"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "factory:", err)
		os.Exit(1)
	}
}

func run(args []string, in io.Reader, out, errOut io.Writer) error {
	if len(args) > 0 {
		if args[0] == "__job-worker" {
			if len(args) != 3 {
				return fmt.Errorf("invalid private worker invocation")
			}
			return factory.RunJobWorker(args[1], args[2])
		}
		if len(args) > 1 && args[0] != "babysit" && args[0] != "job" && args[0] != "run" && args[0] != "pipeline" && args[0] != "implement" && args[0] != "clean" && args[0] != "tidy" && args[0] != "monitor" && args[0] != "--gate" {
			return fmt.Errorf("%s does not accept extra arguments", args[0])
		}
		switch args[0] {
		case "version":
			fmt.Fprintln(out, version)
			return nil
		case "help", "-h", "--help":
			printHelp(out)
			return nil
		case "run":
			cfg, err := factory.LoadConfig("")
			if err != nil {
				return err
			}
			ctx, stop := foregroundContext()
			defer stop()
			return factory.RunCommand(ctx, args[1:], cfg, out)
		case "job":
			cfg, err := factory.LoadConfig("")
			if err != nil {
				return err
			}
			workdir, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("get target repository directory: %w", err)
			}
			ctx, stop := foregroundContext()
			defer stop()
			return factory.JobCommandContext(ctx, args[1:], cfg, workdir, in, out)
		case "babysit", "monitor":
			cfg, err := factory.LoadConfig("")
			if err != nil {
				return err
			}
			workdir, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("get current directory: %w", err)
			}
			return factory.BabysitCommand(args[1:], cfg, workdir, in, out, errOut)
		case "pipeline", "implement":
			gate, task, err := parseGate(args[1:])
			if err != nil {
				return err
			}
			ctx, stop := foregroundContext()
			defer stop()
			return runPipelineContext(ctx, task, gate, in, out)
		case "clean", "tidy":
			gate, task, err := parseGate(args[1:])
			if err != nil {
				return err
			}
			if len(task) != 0 {
				return fmt.Errorf("factory tidy accepts only --gate")
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
	return runPipelineContextWithTerminalCheck(ctx, args, gate, in, out, isTerminal)
}

func runPipelineContextWithTerminalCheck(ctx context.Context, args []string, gate bool, in io.Reader, out io.Writer, terminalCheck func(*os.File) bool) error {
	if file, ok := in.(*os.File); ok {
		in = &contextStdin{file: file, ctx: ctx}
	}
	stdout, outputIsFile := out.(*os.File)
	stdin, inputIsFile := inputFile(in)
	terminal := inputIsFile && outputIsFile && terminalCheck(stdin) && terminalCheck(stdout)
	if gate && !terminal {
		return fmt.Errorf("factory implement --gate requires an interactive terminal for approvals")
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
		if gate {
			workflow := factory.Workflow{Agent: factory.Runner{Config: cfg}, Config: cfg, In: workflowIn, Out: out, Workdir: filepath.Clean(workdir), Terminal: terminal, Gate: true, Managed: true}
			return workflow.RunContext(ctx, task)
		}
		id, err := factory.StartImplementationJob(cfg, workdir, task)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "Started pipeline job %s. Attach is active; Ctrl-C detaches without stopping the job.\n", id)
		storeRoot, err := factory.JobStateRoot(cfg.StateDir)
		if err != nil {
			return err
		}
		store, err := factory.NewJobStore(storeRoot)
		if err != nil {
			return err
		}
		return factory.AttachJob(ctx, store, id, out)
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

func (a foregroundAgent) RunWithContext(ctx context.Context, stage, prompt, task, workdir, logPath string) error {
	mergedCtx, cleanup := a.mergedContext(ctx)
	defer cleanup()
	if contextual, ok := a.agent.(interface {
		RunWithContext(context.Context, string, string, string, string, string) error
	}); ok {
		return contextual.RunWithContext(mergedCtx, stage, prompt, task, workdir, logPath)
	}
	return fmt.Errorf("agent does not support context-aware execution")
}

func (a foregroundAgent) mergedContext(ctx context.Context) (context.Context, func()) {
	mergedCtx, cancel := context.WithCancel(a.ctx)
	stop := context.AfterFunc(ctx, cancel)
	return mergedCtx, func() {
		stop()
		cancel()
	}
}

func (a foregroundAgent) RunWithOutputContext(ctx context.Context, stage, prompt, task, workdir, logPath string) (string, error) {
	mergedCtx, cleanup := a.mergedContext(ctx)
	defer cleanup()
	if contextual, ok := a.agent.(interface {
		RunWithOutputContext(context.Context, string, string, string, string, string) (string, error)
	}); ok {
		return contextual.RunWithOutputContext(mergedCtx, stage, prompt, task, workdir, logPath)
	}
	return "", fmt.Errorf("agent does not provide stdout protocol output")
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
			return fmt.Errorf("an interactive terminal is required; run factory implement from a terminal")
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
	_, noColor := os.LookupEnv("NO_COLOR")
	printHelpWithOptions(out, detectHelpWidth(out), helpOutputIsTerminal(out), noColor)
}

func detectHelpWidth(out io.Writer) int {
	file, ok := out.(*os.File)
	if !ok || !isTerminal(file) {
		return 80
	}
	return helpWidthFromEnvironment(os.Getenv("COLUMNS"), func() (int, error) {
		return terminalWidth(file)
	})
}

func helpWidthFromEnvironment(columns string, query func() (int, error)) int {
	if width, err := query(); err == nil && width > 0 {
		return width
	}
	if width, err := strconv.Atoi(columns); err == nil && width > 0 {
		return width
	}
	return 80
}

func helpOutputIsTerminal(out io.Writer) bool {
	file, ok := out.(*os.File)
	return ok && isTerminal(file)
}

type helpCommand struct {
	usage string
	desc  string
}

type helpSection struct {
	title      string
	commands   []helpCommand
	paragraphs []string
}

func printHelpWithOptions(out io.Writer, width int, terminal, noColor bool) {
	if width <= 0 {
		width = 80
	}
	color := terminal && !noColor && width >= 40
	style := func(text, code string) string {
		if !color {
			return text
		}
		return "\033[" + code + "m" + text + "\033[0m"
	}
	sections := []helpSection{
		{title: "Workflows", commands: []helpCommand{
			{"factory version", "Print the build version (defaults to dev)."},
			{"factory implement [--gate] [description...]", "Start an implementation job and attach to its output (alias: factory pipeline)."},
			{"factory tidy [--gate]", "Review, fix, document, and verify (alias: factory clean)."},
			{"factory monitor <description>", "Start detached monitoring for a routine fix on an open PR (alias: factory babysit)."},
			{"factory [--gate] [description...]", "Interactive alias for factory implement."},
			{"factory help, -h, --help", "Show this help."},
		}, paragraphs: []string{
			"Legacy command forms: factory pipeline -> factory implement; factory clean -> factory tidy; factory babysit -> factory monitor.",
			"Build version is set by the release tag at link time; development builds report dev.",
		}},

		{title: "Detached jobs", commands: []helpCommand{
			{"factory job start implementation <description>", "Start a detached implementation job."},
			{"factory job start monitor <description>", "Start a detached PR monitor."},
			{"factory job list", "List detached jobs."},
			{"factory job show <id>", "Show job details."},
			{"factory job logs <id> [--session <id>] [--follow]", "Read or follow worker/session logs."},
			{"factory job attach <id>", "Follow worker output; Ctrl-C detaches."},
			{"factory job stop <id>", "Request cooperative cancellation."},
		}},
		{title: "Gated runs", commands: []helpCommand{
			{"factory run list", "List gated foreground runs."},
			{"factory run show <id>", "Show status and liveness."},
			{"factory run events <id> [--follow]", "Read or follow workflow events."},
			{"factory run stop <id>", "Request cooperative cancellation."},
		}},
		{title: "Monitor management", commands: []helpCommand{
			{"factory monitor list", "List active and recent jobs."},
			{"factory monitor describe <id>", "Show job details, proposals, and actions."},
			{"factory monitor approve <id>", "Approve a proposal with an exact, non-empty scope."},
			{"factory monitor reject <id>", "Reject a pending proposal."},
			{"factory monitor stop <id>", "Request a stop and cancel active agent processes."},
			{"factory monitor reset <id>", "Restart a recoverable-failure job after its worker exits."},
		}, paragraphs: []string{
			"The same management commands remain available as factory babysit. Approval requires exact lowercase y and scoped task text; it is invalidated if the PR/check snapshot changes. After three automatic actions against an unchanged snapshot, the monitor pauses for approval.",
			"Monitoring runs in an isolated worktree and stops when the PR closes or merges. Before publishing, Factory revalidates the checkout, branch, safe paths, and live PR/check snapshot. It may push a guarded routine fix, but does not merge the PR.",
		}},
		{title: "Examples", paragraphs: []string{
			"factory implement add a small feature",
			"factory implement --gate add a small feature",
			"factory job start implementation update the parser",
			"factory monitor address a failing check on this PR",
			"factory monitor list",
		}},
		{title: "Execution and safeguards", paragraphs: []string{
			"Implement starts a detached job and attaches to it by default. Ctrl-C detaches without stopping the worker; use factory job stop to request cancellation. --gate keeps the workflow in the foreground for approvals. With no description, task entry requires an interactive terminal.",
			"Each agent stage runs once; invocation errors fail the workflow. Successful agent exit is not an independent correctness evaluation. Pipeline/clean stages and monitor actions have a 30-minute active agent-execution budget. The configured agent_timeout (default 60m) is a per-process limit; budgets pause during approvals and other non-agent work. There is no overall job deadline.",
			"Monitor is for narrow, routine fixes on an existing open PR—not general autonomous engineering, high-impact decisions, or a security sandbox. Stop requests are cooperative; the active agent process is canceled, but the detached worker is not forcibly killed. Inspect changes and logs.",
		}},
		{title: "Tidy safety", paragraphs: []string{
			"Pristine mode requires a non-detached branch and an interactive terminal with exact lowercase yes before publication. It uses a configured upstream when available; otherwise it targets origin/<branch> only if that remote branch does not already exist. Upstream-ahead or diverged branches are refused; a synced no-op creates no commit.",
			"Dirty safe mode warns that agents and formatters may affect existing work. It does not stage, commit, or push files and does not require an upstream or origin. Back up important local changes first. Neither mode is a sandbox. This differs from `make clean`, which removes local build artifacts.",
		}},
		{title: "Configuration and limits", paragraphs: []string{
			"Config: ${XDG_CONFIG_HOME:-~/.config}/factory/config.json",
			"Run state: ${XDG_STATE_HOME:-~/.local/state}/factory/runs; detached jobs: ${XDG_STATE_HOME:-~/.local/state}/factory/jobs/v2",
			"Monitor polling: FACTORY_BABYSIT_POLL_INTERVAL (default 30s). GitHub snapshot timeout: 2m. Eight consecutive snapshot failures move the job to recoverable failure; reset clears the failure count but does not fix the underlying cause. Clean verification commands are outside the agent timeout/budget.",
			"Default agent: pi -p --no-session --append-system-prompt {system_prompt} {task}",
			"Optional placeholders: {workdir} and {stage}.",
		}},
	}

	for _, line := range wrapHelpText("factory - software factory workflows", width) {
		fmt.Fprintln(out, style(line, "1;36"))
	}
	fmt.Fprintln(out)
	for _, section := range sections {
		for _, line := range wrapHelpText(section.title, width) {
			fmt.Fprintln(out, style(line, "1;34"))
		}
		if len(section.commands) > 0 {
			printHelpCommands(out, section.commands, width)
		}
		for _, paragraph := range section.paragraphs {
			for _, line := range wrapHelpText(paragraph, width) {
				fmt.Fprintln(out, line)
			}
		}
		fmt.Fprintln(out)
	}
}

func printHelpCommands(out io.Writer, commands []helpCommand, width int) {
	if width < 40 {
		for _, command := range commands {
			for _, line := range wrapHelpText(command.usage, max(1, width-2)) {
				fmt.Fprintf(out, "  %s\n", line)
			}
			for _, line := range wrapHelpText(command.desc, max(1, width-4)) {
				fmt.Fprintf(out, "    %s\n", line)
			}
		}
		return
	}

	maxUsage := 0
	for _, command := range commands {
		maxUsage = max(maxUsage, helpTextWidth(command.usage))
	}
	usageWidth := min(maxUsage, max(12, width/2))
	gap := 2
	descWidth := max(1, width-usageWidth-gap-2)
	for _, command := range commands {
		usageLines := wrapHelpText(command.usage, usageWidth)
		descLines := wrapHelpText(command.desc, descWidth)
		if helpTextWidth(command.usage) > usageWidth {
			for _, usage := range usageLines {
				fmt.Fprintf(out, "  %s\n", usage)
			}
			for _, desc := range descLines {
				fmt.Fprintf(out, "    %s\n", desc)
			}
			continue
		}
		for i := 0; i < max(len(usageLines), len(descLines)); i++ {
			usage, desc := "", ""
			if i < len(usageLines) {
				usage = usageLines[i]
			}
			if i < len(descLines) {
				desc = descLines[i]
			}
			fmt.Fprintf(out, "  %s%s  %s\n", usage, strings.Repeat(" ", usageWidth-helpTextWidth(usage)), desc)
		}
	}
}

func wrapHelpText(text string, width int) []string {
	if width < 1 {
		width = 1
	}
	words := strings.Fields(text)
	if len(words) == 0 {
		return []string{""}
	}
	var lines []string
	line := ""
	for _, word := range words {
		for helpTextWidth(word) > width {
			chunk, rest := splitHelpWord(word, width)
			if line != "" {
				lines = append(lines, line)
				line = ""
			}
			lines = append(lines, chunk)
			word = rest
		}
		if line == "" {
			line = word
			continue
		}
		if helpTextWidth(line)+1+helpTextWidth(word) <= width {
			line += " " + word
			continue
		}
		lines = append(lines, line)
		line = word
	}
	if line != "" {
		lines = append(lines, line)
	}
	return lines
}

func splitHelpWord(word string, width int) (string, string) {
	type cluster struct {
		text  string
		width int
	}
	var clusters []cluster
	for _, r := range word {
		if (unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Me, r)) && len(clusters) > 0 {
			clusters[len(clusters)-1].text += string(r)
			continue
		}
		clusters = append(clusters, cluster{text: string(r), width: helpRuneWidth(r)})
	}
	var chunk strings.Builder
	used, end := 0, 0
	for _, item := range clusters {
		if used+item.width > width && end > 0 {
			return word[:end], word[end:]
		}
		chunk.WriteString(item.text)
		used += item.width
		end += len(item.text)
	}
	return chunk.String(), ""
}

func helpTextWidth(text string) int {
	width := 0
	for _, r := range text {
		width += helpRuneWidth(r)
	}
	return width
}

func helpRuneWidth(r rune) int {
	if unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Me, r) || unicode.Is(unicode.Cf, r) {
		return 0
	}
	if isWideHelpRune(r) {
		return 2
	}
	return 1
}

func isWideHelpRune(r rune) bool {
	return r >= 0x1100 && (r <= 0x115f || r == 0x2329 || r == 0x232a ||
		(r >= 0x2e80 && r <= 0xa4cf && r != 0x303f) ||
		(r >= 0xac00 && r <= 0xd7a3) || (r >= 0xf900 && r <= 0xfaff) ||
		(r >= 0xfe10 && r <= 0xfe19) || (r >= 0xfe30 && r <= 0xfe6f) ||
		(r >= 0xff00 && r <= 0xff60) || (r >= 0xffe0 && r <= 0xffe6) ||
		(r >= 0x1f300 && r <= 0x1faff) || (r >= 0x20000 && r <= 0x3fffd))
}
