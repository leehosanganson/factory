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
		if err := rejectRemovedAlias(args); err != nil {
			return err
		}
		if commandHelpRequested(args) {
			printCommandHelp(out, args)
			return nil
		}
		if args[0] == "__job-worker" {
			if len(args) != 3 {
				return fmt.Errorf("invalid private worker invocation")
			}
			return factory.RunJobWorker(args[1], args[2])
		}
		if args[0] == "__monitor-worker" {
			cfg, err := factory.LoadConfig("")
			if err != nil {
				return err
			}
			return factory.MonitorCommand(append([]string{"--worker"}, args[1:]...), cfg, "", in, out, errOut)
		}
		if len(args) > 1 && args[0] != "job" && args[0] != "run" && args[0] != "implement" && args[0] != "tidy" && args[0] != "monitor" && args[0] != "--gate" {
			return fmt.Errorf("%s does not accept extra arguments", args[0])
		}
		switch args[0] {
		case "version":
			fmt.Fprintln(out, version)
			return nil
		case "help":
			printRootHelp(out)
			return nil
		case "-h", "--help":
			printRootHelp(out)
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
			stdin, inputIsFile := inputFile(in)
			stdout, outputIsFile := out.(*os.File)
			watchTerminal := inputIsFile && outputIsFile && isTerminal(stdin) && isTerminal(stdout) && strings.TrimSpace(os.Getenv("TERM")) != "dumb"
			return factory.JobCommandContextWithTerminal(ctx, args[1:], cfg, workdir, in, out, watchTerminal)
		case "monitor":
			cfg, err := factory.LoadConfig("")
			if err != nil {
				return err
			}
			workdir, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("get current directory: %w", err)
			}
			return factory.MonitorCommand(args[1:], cfg, workdir, in, out, errOut)
		case "implement":
			gate, detached, task, err := parseWorkflowOptions(args[1:])
			if err != nil {
				return err
			}
			if gate && detached {
				return fmt.Errorf("factory implement --gate cannot be combined with --detach")
			}
			if detached {
				return startDetachedImplementation(strings.Join(task, " "), out)
			}
			ctx, stop := foregroundContext()
			defer stop()
			return runPipelineContext(ctx, task, gate, in, out)
		case "tidy":
			gate, detached, task, err := parseWorkflowOptions(args[1:])
			if err != nil {
				return err
			}
			if gate && detached {
				return fmt.Errorf("factory tidy --gate cannot be combined with --detach")
			}
			if detached {
				description := strings.TrimSpace(strings.Join(task, " "))
				if description == "" {
					description = "Review, fix, document, and verify the target repository without publishing changes."
				}
				return startDetachedTidy(description, out)
			}
			if len(task) != 0 {
				return fmt.Errorf("factory tidy accepts only --gate; use --detach with a description for a detached job")
			}
			ctx, stop := foregroundContext()
			defer stop()
			return runCleanContext(ctx, gate, in, out, errOut)
		default:
			if args[0] == "--gate" {
				gate, detached, task, err := parseWorkflowOptions(args)
				if err != nil {
					return err
				}
				if gate && detached {
					return fmt.Errorf("factory implement --gate cannot be combined with --detach")
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

func rejectRemovedAlias(args []string) error {
	if len(args) == 0 {
		return nil
	}
	switch args[0] {
	case "pipeline", "clean":
		return fmt.Errorf("unknown command %q (try factory help)", args[0])
	}
	if len(args) > 1 {
		if args[0] == "job" || args[0] == "run" {
			if args[1] == "show" {
				return fmt.Errorf("unknown %s subcommand %q (try factory %s help)", args[0], args[1], args[0])
			}
		}
		if args[0] == "monitor" && args[1] == "describe" {
			return fmt.Errorf("unknown monitor subcommand %q (try factory monitor help)", args[1])
		}
	}
	return nil
}

func commandHelpRequested(args []string) bool {
	if len(args) < 2 {
		return false
	}
	switch args[0] {
	case "implement", "tidy", "job", "run", "monitor":
		for _, arg := range args[1:] {
			if arg == "-h" || arg == "--help" || arg == "help" && len(args) == 2 {
				return true
			}
		}
	}
	return false
}

func printCommandHelp(out io.Writer, args []string) {
	command, subcommand := args[0], ""
	if len(args) > 1 {
		subcommand = args[1]
	}
	canonical := command

	var title string
	var commands []helpCommand
	var paragraphs []string
	switch canonical {
	case "implement":
		title = "Implement workflow"
		commands = []helpCommand{
			{"factory implement [-d|--detach] [--gate] [description...]", "Start an implementation workflow; --gate runs in the foreground."},
		}
	case "tidy":
		title = "Tidy workflow"
		commands = []helpCommand{
			{"factory tidy [-d|--detach] [--gate] [description...]", "Review, fix, document, and verify; detached mode returns immediately."},
		}
	case "job":
		title = "Detached jobs"
		commands, paragraphs = jobHelp(subcommand)
	case "run":
		title = "Gated runs"
		commands, paragraphs = runHelp(subcommand)
	case "monitor":
		title = "Monitor management"
		commands, paragraphs = monitorHelp(subcommand)
	}
	width := detectHelpWidth(out)
	_, noColor := os.LookupEnv("NO_COLOR")
	color := helpOutputIsTerminal(out) && !noColor && width >= 40
	if width <= 0 {
		width = 80
	}
	style := func(text string) string {
		if color {
			return "\033[1;34m" + text + "\033[0m"
		}
		return text
	}
	fmt.Fprintln(out, style("factory - "+title))
	if len(commands) > 0 {
		printHelpCommands(out, commands, width)
	}
	for _, paragraph := range paragraphs {
		for _, line := range wrapHelpText(paragraph, width) {
			fmt.Fprintln(out, line)
		}
	}
}

func jobHelp(subcommand string) ([]helpCommand, []string) {
	all := []helpCommand{
		{"factory job start implementation <description>", "Start an implementation job."},
		{"factory job start tidy <description>", "Start a nonpublishing tidy job."},
		{"factory job start monitor <description>", "Start a PR monitor."},
		{"factory job list [--limit <n>]", "List detached jobs, optionally limited to the newest positive number of jobs."},
		{"factory job get <id> [--details]", "Show job status; --details includes metadata."},
		{"factory job logs <id> [--session <id>] [--follow]", "Read or follow logs."},
		{"factory job attach <id>", "Follow worker output."},
		{"factory job stop <id>", "Request cancellation."},
		{"factory job watch <id>...", "Refresh selected job status and latest activity, with monitor phase, check freshness, and recent events."},
		{"Configuration: worktree_parent", "Parent path template for implementation and monitor worktrees; {repo} is the primary checkout name."},
	}
	return selectCommandHelp(all, subcommand, "")
}

func runHelp(subcommand string) ([]helpCommand, []string) {
	all := []helpCommand{
		{"factory run list", "List gated runs."},
		{"factory run get <id> [--details]", "Show run status; --details includes metadata."},
		{"factory run events <id> [--follow]", "Read or follow events."},
		{"factory run stop <id>", "Request cancellation."},
	}
	return selectCommandHelp(all, subcommand, "")
}

func monitorHelp(subcommand string) ([]helpCommand, []string) {
	all := []helpCommand{
		{"factory monitor <description>", "Start monitoring an open PR."},
		{"factory monitor list [--limit <n>]", "List monitor jobs, optionally limited to the newest positive number of jobs."},
		{"factory monitor get <id> [--details]", "Show concise phase, latest PR check, and pending approval; --details includes proposals, recent events, and diagnostics."},
		{"factory monitor approve <id>", "Approve a proposal with a scope."},
		{"factory monitor reject <id>", "Reject a pending proposal."},
		{"factory monitor stop <id>", "Stop monitoring."},
		{"factory monitor reset <id>", "Reset a recoverable failure."},
	}
	return selectCommandHelp(all, subcommand, "")
}

func selectCommandHelp(all []helpCommand, subcommand, example string) ([]helpCommand, []string) {
	if subcommand == "" || subcommand == "--help" || subcommand == "-h" || subcommand == "help" {
		if example == "" {
			return all, nil
		}
		return all, []string{example}
	}
	name := subcommand
	var selected []helpCommand
	for _, command := range all {
		fields := strings.Fields(command.usage)
		if len(fields) >= 3 && fields[2] == name {
			selected = append(selected, command)
		}
	}
	if len(selected) == 0 {
		if example == "" {
			return all, nil
		}
		return all, []string{example}
	}
	return selected, nil
}

func foregroundContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func parseGate(args []string) (bool, []string, error) {
	gate, _, task, err := parseWorkflowOptions(args)
	return gate, task, err
}

func parseWorkflowOptions(args []string) (bool, bool, []string, error) {
	gate, detached := false, false
	task := make([]string, 0, len(args))
	for _, arg := range args {
		switch arg {
		case "--gate":
			if gate {
				return false, false, nil, fmt.Errorf("--gate may only be specified once")
			}
			gate = true
		case "--detach", "-d":
			if detached {
				return false, false, nil, fmt.Errorf("--detach may only be specified once")
			}
			detached = true
		default:
			task = append(task, arg)
		}
	}
	return gate, detached, task, nil
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
		if gate && cfg.AutoPublish {
			return factory.RunAutomaticImplementation(ctx, cfg, filepath.Clean(workdir), task, workflowIn, out, terminal, true)
		}
		if gate {
			if err := factory.ValidateImplementationCheckout(filepath.Clean(workdir)); err != nil {
				return err
			}
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

func startDetachedImplementation(task string, out io.Writer) error {
	if strings.TrimSpace(task) == "" {
		return fmt.Errorf("detached implementation requires a description")
	}
	cfg, err := factory.LoadConfig("")
	if err != nil {
		return err
	}
	workdir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get target repository directory: %w", err)
	}
	id, err := factory.StartImplementationJob(cfg, workdir, task)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "Started implementation job %s. Use factory job attach %s to follow progress.\n", id, id)
	return nil
}

func startDetachedTidy(task string, out io.Writer) error {
	cfg, err := factory.LoadConfig("")
	if err != nil {
		return err
	}
	workdir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get target repository directory: %w", err)
	}
	id, err := factory.StartTidyJob(cfg, workdir, task)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "Started tidy job %s. Changes will remain unpublished; use factory job attach %s to follow progress.\n", id, id)
	return nil
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
		return fmt.Errorf("factory tidy --gate requires an interactive terminal for approvals")
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

func printRootHelp(out io.Writer) {
	_, noColor := os.LookupEnv("NO_COLOR")
	printRootHelpWithOptions(out, detectHelpWidth(out), helpOutputIsTerminal(out), noColor)
}

func printRootHelpWithOptions(out io.Writer, width int, terminal, noColor bool) {
	if width <= 0 {
		width = 80
	}
	color := terminal && !noColor && width >= 40
	style := func(text string) string {
		if color {
			return "\033[1;36m" + text + "\033[0m"
		}
		return text
	}
	for _, line := range wrapHelpText("factory - software factory workflows", width) {
		fmt.Fprintln(out, style(line))
	}
	fmt.Fprintln(out)
	for _, line := range wrapHelpText("Usage: factory [--gate] [description...]", width) {
		fmt.Fprintln(out, line)
	}
	for _, line := range wrapHelpText("       factory <command> [options]", width) {
		fmt.Fprintln(out, line)
	}
	fmt.Fprintln(out)
	commands := []helpCommand{
		{"factory implement [-d|--detach] [--gate] [description...]", "Start an implementation workflow."},
		{"factory tidy [-d|--detach] [--gate] [description...]", "Review, fix, document, and verify."},
		{"factory monitor <description>", "Monitor an open pull request."},
		{"factory job <command>", "Manage detached jobs."},
		{"factory run <command>", "Manage gated runs."},
		{"factory version", "Print the build version."},
		{"factory help, -h, --help", "Show this concise help."},
	}
	printHelpCommands(out, commands, width)
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
