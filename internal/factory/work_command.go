package factory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// WorkCommand manages locally queued issue work requests. Issue snapshots are
// fetched only by explicit issue, refresh, and foreground watch subcommands; no command starts engineering.
func WorkCommand(ctx context.Context, args []string, cfg Config, out io.Writer) error {
	return WorkCommandWithIssueTracker(ctx, args, cfg, out, NewGitHubIssueTracker())
}

// WorkCommandWithIssueTracker makes the read-only issue lookup dependency explicit.
func WorkCommandWithIssueTracker(ctx context.Context, args []string, cfg Config, out io.Writer, tracker IssueTracker) (result error) {
	if len(args) == 0 {
		return fmt.Errorf("usage: factory work submit --tracker <provider> --issue <issue-ref> --code-host <provider> --repository <repo> [--dedup-key <key>] | list | get <dedup-key> | issue <dedup-key> | refresh <dedup-key> | history <dedup-key> | watch <dedup-key> [--interval <duration>] | respond <dedup-key> --version <issue-version> --instruction <text> | directions <dedup-key>")
	}
	if args[0] == "submit" {
		request, hasDeduplicationKey, err := parseWorkSubmit(args[1:])
		if err != nil {
			return err
		}
		if !hasDeduplicationKey {
			request.DeduplicationKey = defaultWorkDeduplicationKey(request)
		}
		if err := validateWorkRequest(request); err != nil {
			return err
		}
		queue, err := openWorkQueue(cfg.StateDir)
		if err != nil {
			return err
		}
		defer func() {
			if err := queue.Close(); result == nil && err != nil {
				result = fmt.Errorf("close work request queue: %w", err)
			}
		}()
		if err := queue.Enqueue(ctx, request); err != nil {
			return fmt.Errorf("submit work request: %w", err)
		}
		item, err := queue.Get(ctx, request.DeduplicationKey)
		if err != nil {
			return fmt.Errorf("read submitted work request: %w", err)
		}
		printWorkItem(out, item)
		fmt.Fprintln(out, "Only queued: no issue was fetched, no PR was created, and engineering has not started because no provider or worker is configured.")
		return nil
	}

	queue, err := openWorkQueue(cfg.StateDir)
	if err != nil {
		return err
	}
	defer func() {
		if err := queue.Close(); result == nil && err != nil {
			result = fmt.Errorf("close work request queue: %w", err)
		}
	}()
	switch args[0] {
	case "list":
		if len(args) != 1 {
			return fmt.Errorf("usage: factory work list")
		}
		items, err := queue.List(ctx)
		if err != nil {
			return fmt.Errorf("list work requests: %w", err)
		}
		if len(items) == 0 {
			fmt.Fprintln(out, "No work requests queued.")
			return nil
		}
		for _, item := range items {
			printWorkItem(out, item)
		}
		return nil
	case "respond":
		key, version, instruction, err := parseWorkRespond(args[1:])
		if err != nil {
			return err
		}
		item, err := queue.Get(ctx, key)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("work request %q not found", key)
			}
			return fmt.Errorf("get work request: %w", err)
		}
		store, err := openIssueObservationStore(cfg.StateDir)
		if err != nil {
			return err
		}
		direction, recorded, err := store.RecordDirection(ctx, item.Request.DeduplicationKey, version, instruction)
		if err != nil {
			return fmt.Errorf("record human direction: %w", err)
		}
		if recorded {
			fmt.Fprintln(out, "Direction: recorded")
		} else {
			fmt.Fprintln(out, "Direction: already recorded")
		}
		fmt.Fprintf(out, "Issue version: %s\nInstruction: %s\n", sanitizeProgressLine(direction.IssueVersion), sanitizeHumanDirection(direction.Instruction))
		fmt.Fprintln(out, "No lifecycle, queue, or engineering action was taken.")
		return nil
	case "directions":
		if len(args) != 2 || strings.HasPrefix(args[1], "-") {
			return fmt.Errorf("usage: factory work directions <dedup-key>")
		}
		item, err := queue.Get(ctx, args[1])
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("work request %q not found", args[1])
			}
			return fmt.Errorf("get work request: %w", err)
		}
		store, err := openIssueObservationStore(cfg.StateDir)
		if err != nil {
			return err
		}
		directions, err := store.ListDirections(ctx, item.Request.DeduplicationKey)
		if err != nil {
			return fmt.Errorf("list human directions: %w", err)
		}
		if len(directions) == 0 {
			fmt.Fprintln(out, "No human directions recorded.")
			return nil
		}
		for _, direction := range directions {
			fmt.Fprintf(out, "Issue version: %s\nRecorded: %s\nInstruction: %s\n", sanitizeProgressLine(direction.IssueVersion), direction.RecordedAt.UTC().Format(time.RFC3339Nano), sanitizeHumanDirection(direction.Instruction))
		}
		return nil
	case "get", "issue", "refresh", "history", "watch":
		key := ""
		interval := defaultWorkWatchInterval
		if args[0] == "watch" {
			var err error
			key, interval, err = parseWorkWatch(args[1:])
			if err != nil {
				return err
			}
		} else if len(args) == 2 && !strings.HasPrefix(args[1], "-") {
			key = args[1]
		} else {
			return fmt.Errorf("usage: factory work %s <dedup-key>", args[0])
		}
		item, err := queue.Get(ctx, key)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("work request %q not found", key)
			}
			return fmt.Errorf("get work request: %w", err)
		}
		printWorkItem(out, item)
		if args[0] == "get" {
			return nil
		}
		if args[0] != "history" && item.Request.TrackerProvider != "github" {
			return fmt.Errorf("cannot %s issue for tracker %q: only github is supported", args[0], item.Request.TrackerProvider)
		}
		if args[0] == "history" {
			store, err := openIssueObservationStore(cfg.StateDir)
			if err != nil {
				return err
			}
			observations, err := store.List(ctx, item.Request.DeduplicationKey)
			if err != nil {
				return fmt.Errorf("list issue history: %w", err)
			}
			if len(observations) == 0 {
				fmt.Fprintln(out, "No issue observations recorded.")
				return nil
			}
			for _, observation := range observations {
				printIssueSnapshot(out, observation.Snapshot)
			}
			state, err := store.Reconcile(ctx, item.Request.DeduplicationKey)
			if err != nil {
				return fmt.Errorf("reconcile issue lifecycle: %w", err)
			}
			fmt.Fprintf(out, "Lifecycle: %s\n", state.Status)
			return nil
		}
		if args[0] == "watch" {
			store, err := openIssueObservationStore(cfg.StateDir)
			if err != nil {
				return err
			}
			return watchIssue(ctx, item, tracker, store, interval, out, waitWorkWatchInterval)
		}
		operation := args[0]
		if operation == "issue" {
			operation = "inspect"
		}
		if tracker == nil {
			return fmt.Errorf("%s issue: issue tracker is unavailable", operation)
		}
		snapshot, err := tracker.GetIssue(ctx, item.Request.Repository, item.Request.IssueID)
		if err != nil {
			return fmt.Errorf("%s issue: %w", operation, err)
		}
		printIssueSnapshot(out, snapshot)
		if args[0] == "refresh" {
			store, err := openIssueObservationStore(cfg.StateDir)
			if err != nil {
				return err
			}
			recorded, err := store.Record(ctx, item.Request.DeduplicationKey, snapshot)
			if err != nil {
				return fmt.Errorf("record issue observation: %w", err)
			}
			if recorded {
				fmt.Fprintln(out, "Observation: newly recorded")
			} else {
				fmt.Fprintln(out, "Observation: duplicate")
			}
			state, err := store.Reconcile(ctx, item.Request.DeduplicationKey)
			if err != nil {
				return fmt.Errorf("reconcile issue lifecycle: %w", err)
			}
			fmt.Fprintf(out, "Lifecycle: %s\n", state.Status)
		}
		return nil
	default:
		return fmt.Errorf("unknown work subcommand %q (try factory work help)", args[0])
	}
}

func parseWorkRespond(args []string) (string, string, string, error) {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return "", "", "", fmt.Errorf("usage: factory work respond <dedup-key> --version <issue-version> --instruction <text>")
	}
	key := args[0]
	values := make(map[string]string, 2)
	for i := 1; i < len(args); i++ {
		flag := args[i]
		if (flag != "--version" && flag != "--instruction") || i+1 >= len(args) {
			return "", "", "", fmt.Errorf("usage: factory work respond <dedup-key> --version <issue-version> --instruction <text>")
		}
		if _, exists := values[flag]; exists {
			return "", "", "", fmt.Errorf("option %s may only be specified once", flag)
		}
		i++
		values[flag] = args[i]
	}
	version, versionOK := values["--version"]
	instruction, instructionOK := values["--instruction"]
	if !versionOK || !instructionOK {
		return "", "", "", fmt.Errorf("usage: factory work respond <dedup-key> --version <issue-version> --instruction <text>")
	}
	return key, version, instruction, nil
}

func parseWorkSubmit(args []string) (WorkRequest, bool, error) {
	values := make(map[string]string, 5)
	allowed := map[string]bool{"--tracker": true, "--issue": true, "--code-host": true, "--repository": true, "--dedup-key": true}
	for i := 0; i < len(args); i++ {
		flag := args[i]
		if !allowed[flag] || i+1 >= len(args) || strings.HasPrefix(args[i+1], "--") {
			return WorkRequest{}, false, fmt.Errorf("usage: factory work submit --tracker <provider> --issue <issue-ref> --code-host <provider> --repository <repo> [--dedup-key <key>]")
		}
		if _, exists := values[flag]; exists {
			return WorkRequest{}, false, fmt.Errorf("option %s may only be specified once", flag)
		}
		i++
		values[flag] = args[i]
	}
	for _, flag := range []string{"--tracker", "--issue", "--code-host", "--repository"} {
		if _, ok := values[flag]; !ok {
			return WorkRequest{}, false, fmt.Errorf("usage: factory work submit --tracker <provider> --issue <issue-ref> --code-host <provider> --repository <repo> [--dedup-key <key>]")
		}
	}
	_, hasDeduplicationKey := values["--dedup-key"]
	return WorkRequest{
		TrackerProvider: values["--tracker"], IssueID: values["--issue"],
		CodeHostProvider: values["--code-host"], Repository: values["--repository"],
		DeduplicationKey: values["--dedup-key"],
	}, hasDeduplicationKey, nil
}

func defaultWorkDeduplicationKey(request WorkRequest) string {
	identity := strings.Join([]string{request.TrackerProvider, request.IssueID, request.CodeHostProvider, request.Repository}, "\x00")
	sum := sha256.Sum256([]byte(identity))
	return "work-" + hex.EncodeToString(sum[:])
}

func openWorkQueue(stateDir string) (*LocalWorkQueue, error) {
	base := stateDir
	if base == "" {
		base = os.Getenv("XDG_STATE_HOME")
		if base == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return nil, fmt.Errorf("find home directory: %w", err)
			}
			base = filepath.Join(home, ".local", "state")
		}
	}
	if !filepath.IsAbs(base) {
		return nil, fmt.Errorf("state directory must be absolute")
	}
	queue, err := NewLocalWorkQueue(filepath.Join(base, "factory", "work-requests"))
	if err != nil {
		return nil, fmt.Errorf("open work request queue: %w", err)
	}
	return queue, nil
}

func openIssueObservationStore(stateDir string) (*LocalIssueObservationStore, error) {
	base := stateDir
	if base == "" {
		base = os.Getenv("XDG_STATE_HOME")
		if base == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return nil, fmt.Errorf("find home directory: %w", err)
			}
			base = filepath.Join(home, ".local", "state")
		}
	}
	if !filepath.IsAbs(base) {
		return nil, fmt.Errorf("state directory must be absolute")
	}
	store, err := NewLocalIssueObservationStore(filepath.Join(base, "factory", "work-observations"))
	if err != nil {
		return nil, fmt.Errorf("open issue observation store: %w", err)
	}
	return store, nil
}

func printWorkItem(out io.Writer, item WorkItem) {
	request := item.Request
	fmt.Fprintf(out, "Dedup key: %s\nState: %s\nTracker: %s\nIssue: %s\nCode host: %s\nRepository: %s\n",
		request.DeduplicationKey, item.State, request.TrackerProvider, request.IssueID, request.CodeHostProvider, request.Repository)
}

func printIssueSnapshot(out io.Writer, snapshot IssueSnapshot) {
	fmt.Fprintf(out, "Issue snapshot:\nTitle: %s\nState: %s\nUpdated: %s\nVersion: %s\nURL: %s\n",
		sanitizeProgressLine(snapshot.Title), snapshot.State, snapshot.UpdatedAt.UTC().Format(time.RFC3339Nano), snapshot.Version, snapshot.URL)
}
