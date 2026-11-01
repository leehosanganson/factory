package poller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"

	"github.com/leehosanganson/factory/internal/rest/jobs"
)

func parseOperatorListLimit(value string) (int, error) {
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 || limit > 100 {
		return 0, errors.New("issue-poller list limit must be between 1 and 100")
	}
	return limit, nil
}

func listOperatorActions(ctx context.Context, out io.Writer, args []string, store interface {
	ListIssuePollerOperatorActions(context.Context, int) ([]restjobs.IssuePollerOperatorAction, error)
}) error {
	limit := 100
	if len(args) == 5 {
		var err error
		limit, err = parseOperatorListLimit(args[4])
		if err != nil {
			return err
		}
	}
	actions, err := store.ListIssuePollerOperatorActions(ctx, limit)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	encoder.SetEscapeHTML(true)
	return encoder.Encode(actions)
}

// OperatorCommand lists or resolves bounded safe issue-poller ledger outcomes.
func OperatorCommand(ctx context.Context, args []string, out io.Writer) error {
	if ctx == nil || out == nil || len(args) < 3 || args[1] != "--config" {
		return errors.New("usage: factory issue-poller list --config <absolute-path> [--limit <1..100>] | resolve --config <absolute-path> --source <owner/repo> --issue <number> --version <sha256>")
	}
	if !filepath.IsAbs(args[2]) || (args[0] == "list" && (len(args) != 3 && len(args) != 5 || len(args) == 5 && args[3] != "--limit")) || (args[0] == "resolve" && (len(args) != 9 || args[3] != "--source" || args[5] != "--issue" || args[7] != "--version")) || (args[0] != "list" && args[0] != "resolve") {
		return errors.New("invalid issue-poller operator options")
	}
	var err error
	var source, version string
	var issueNumber int
	if args[0] == "resolve" {
		source, version = args[4], args[8]
		issueNumber, err = strconv.Atoi(args[6])
		if err != nil || issueNumber < 1 || len(version) != 64 {
			return errors.New("issue-poller resolution identity is invalid")
		}
	}
	config, err := LoadConfig(args[2])
	if err != nil {
		return err
	}
	if args[0] == "resolve" {
		if _, allowed := config.Sources[source]; !allowed {
			return errors.New("issue-poller source is not allowlisted")
		}
	}
	storeConfig := restjobs.Config{QueueCapacity: config.QueueCapacity, MaxConcurrentJobs: config.Workers, MaxRecords: config.MaxRecords, MaxEventsPerJob: config.MaxEvents, MaxTaskBytes: config.TaskBytes, RegistryBytes: config.RegistryBytes}
	store, err := restjobs.OpenPostgresServerStore(config.PostgresDSN, storeConfig)
	if err != nil {
		return errors.New("open canonical PostgreSQL job store")
	}
	defer store.CloseStore()
	switch args[0] {
	case "list":
		return listOperatorActions(ctx, out, args, store)
	case "resolve":
		if err := store.ResolveIssuePollerOperatorAction(ctx, source, issueNumber, version); err != nil {
			return err
		}
		fmt.Fprintln(out, "issue-poller operator action resolved")
		return nil
	default:
		return errors.New("issue-poller subcommand must be list or resolve")
	}
}
