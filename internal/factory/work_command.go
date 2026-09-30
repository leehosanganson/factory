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
)

// WorkCommand manages locally queued issue work requests. It does not fetch
// issues or start engineering work.
func WorkCommand(ctx context.Context, args []string, cfg Config, out io.Writer) (result error) {
	if len(args) == 0 {
		return fmt.Errorf("usage: factory work submit --tracker <provider> --issue <issue-ref> --code-host <provider> --repository <repo> [--dedup-key <key>] | list | get <dedup-key>")
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
	case "get":
		if len(args) != 2 || strings.HasPrefix(args[1], "-") {
			return fmt.Errorf("usage: factory work get <dedup-key>")
		}
		item, err := queue.Get(ctx, args[1])
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("work request %q not found", args[1])
			}
			return fmt.Errorf("get work request: %w", err)
		}
		printWorkItem(out, item)
		return nil
	default:
		return fmt.Errorf("unknown work subcommand %q (try factory work help)", args[0])
	}
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

func printWorkItem(out io.Writer, item WorkItem) {
	request := item.Request
	fmt.Fprintf(out, "Dedup key: %s\nState: %s\nTracker: %s\nIssue: %s\nCode host: %s\nRepository: %s\n",
		request.DeduplicationKey, item.State, request.TrackerProvider, request.IssueID, request.CodeHostProvider, request.Repository)
}
