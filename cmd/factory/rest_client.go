package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/leehosanganson/factory/internal/restclient"
	"github.com/leehosanganson/factory/internal/restjobs"
)

func restClientHelp() []helpCommand {
	return []helpCommand{
		{"factory rest submit --repository <alias> [--idempotency-key <key>] [--json] <task>", "Submit work to the configured REST server."},
		{"factory rest get [--json] <job-id>", "Inspect a remote job."},
		{"factory rest list [--limit <1..100>] [--after <cursor> --snapshot <sequence>] [--json]", "List retained remote jobs."},
		{"factory rest history [--json] <job-id>", "Inspect bounded remote job history."},
		{"factory rest watch [--poll-interval <duration>] [--json] <job-id>", "Follow one job; poll status/history safely."},
		{"factory rest cancel [--json] <job-id>", "Request cooperative cancellation of one remote job."},
		{"factory rest operations [--json]", "Show an authenticated aggregate operations snapshot."},
		{"factory rest disposition --confirm [--json] <failed|canceled> <job-id>", "Explicitly disposition an eligible interrupted SQLite job."},
		{"factory rest reconcile [--json] <job-id>", "Confirm an eligible SQLite provider outcome using read-only reconciliation."},
	}
}

func runRESTClient(args []string, out io.Writer) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		printHelpCommands(out, restClientHelp(), 100)
		return nil
	}
	command := args[0]
	flags := flag.NewFlagSet("factory rest "+command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "REST client config")
	jsonOutput := flags.Bool("json", false, "emit stable JSON")
	repository := flags.String("repository", "", "configured repository alias")
	key := flags.String("idempotency-key", "", "stable idempotency key")
	confirmDisposition := flags.Bool("confirm", false, "confirm the explicit failed or canceled disposition")
	limit := flags.Int("limit", 0, "page size")
	after := flags.Uint64("after", 0, "job-list continuation cursor")
	snapshot := flags.Uint64("snapshot", 0, "job-list snapshot ceiling")
	pollInterval := flags.Duration("poll-interval", time.Second, "watch polling interval")
	if err := flags.Parse(args[1:]); err != nil {
		return fmt.Errorf("invalid REST client options (try factory rest help)")
	}
	if *pollInterval <= 0 || (*limit != 0 && (*limit < 1 || *limit > 100)) {
		return errors.New("invalid REST client options")
	}
	config, err := restclient.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	client := restclient.New(config)
	ctx, stop := foregroundContext()
	defer stop()
	switch command {
	case "submit":
		if *repository == "" || len(flags.Args()) == 0 {
			return errors.New("usage: factory rest submit --repository <alias> [--idempotency-key <key>] [--json] <task>")
		}
		result, err := client.Submit(ctx, *repository, strings.Join(flags.Args(), " "), *key)
		if err != nil {
			return err
		}
		if *jsonOutput {
			return json.NewEncoder(out).Encode(struct {
				SchemaVersion int `json:"schema_version"`
				Submission    struct {
					Job      safeRESTJob `json:"job"`
					Replayed bool        `json:"replayed"`
					Links    any         `json:"links"`
				} `json:"submission"`
			}{1, safeSubmission(result)})
		}
		fmt.Fprintf(out, "Submitted remote job %s (%s).\n", result.Job.ID, result.Job.Status)
		return nil
	case "get":
		if flags.NArg() != 1 {
			return errors.New("usage: factory rest get [--json] <job-id>")
		}
		result, err := client.Get(ctx, flags.Arg(0))
		if err != nil {
			return err
		}
		return writeRESTResult(out, *jsonOutput, "job", result, fmt.Sprintf("Job %s: %s", result.ID, result.Status))
	case "list":
		if flags.NArg() != 0 || ((*after == 0) != (*snapshot == 0)) {
			return errors.New("usage: factory rest list [--limit <1..100>] [--after <cursor> --snapshot <sequence>] [--json]")
		}
		result, err := client.List(ctx, *limit, *after, *snapshot)
		if err != nil {
			return err
		}
		return writeRESTResult(out, *jsonOutput, "page", result, fmt.Sprintf("%d retained remote job(s).", len(result.Jobs)))
	case "history":
		if flags.NArg() != 1 {
			return errors.New("usage: factory rest history [--json] <job-id>")
		}
		result, err := client.History(ctx, flags.Arg(0))
		if err != nil {
			return err
		}
		return writeRESTResult(out, *jsonOutput, "history", result, fmt.Sprintf("%d retained event(s) for %s.", len(result.Events), result.JobID))
	case "cancel":
		if flags.NArg() != 1 {
			return errors.New("usage: factory rest cancel [--json] <job-id>")
		}
		result, err := client.Cancel(ctx, flags.Arg(0))
		if err != nil {
			return err
		}
		return writeRESTResult(out, *jsonOutput, "job", result, fmt.Sprintf("Cancellation requested for %s; cancellation is cooperative (%s).", result.ID, result.Status))
	case "operations":
		if flags.NArg() != 0 {
			return errors.New("usage: factory rest operations [--json]")
		}
		result, err := client.Operations(ctx)
		if err != nil {
			return err
		}
		return writeRESTResult(out, *jsonOutput, "operations", result, fmt.Sprintf("Operations snapshot: %d retained job(s), %d/%d records, queue %d/%d (saturated: %t), queued %d, running %d, succeeded %d, failed %d, canceled %d, recovery needed %d.", result.RetainedRecords, result.RetainedRecords, result.RecordLimit, result.Queued, result.QueueCapacity, result.QueueSaturated, result.Queued, result.Running, result.Succeeded, result.Failed, result.Canceled, result.RecoveryNeeded))
	case "disposition":
		if !*confirmDisposition || flags.NArg() != 2 || (flags.Arg(0) != string(restjobs.InterruptedDispositionFailed) && flags.Arg(0) != string(restjobs.InterruptedDispositionCanceled)) {
			return errors.New("usage: factory rest disposition --confirm [--json] <failed|canceled> <job-id>")
		}
		result, err := client.Disposition(ctx, flags.Arg(1), restjobs.InterruptedDisposition(flags.Arg(0)))
		if err != nil {
			return err
		}
		return writeRESTResult(out, *jsonOutput, "job", result, fmt.Sprintf("Remote job %s explicitly dispositioned as %s.", result.ID, result.Status))
	case "reconcile":
		if flags.NArg() != 1 || *confirmDisposition {
			return errors.New("usage: factory rest reconcile [--json] <job-id>")
		}
		result, err := client.Reconcile(ctx, flags.Arg(0))
		if err != nil {
			return err
		}
		return writeRESTResult(out, *jsonOutput, "job", result, fmt.Sprintf("Remote job %s reconciled from read-only provider confirmation (%s).", result.ID, result.Status))
	case "watch":
		if flags.NArg() != 1 {
			return errors.New("usage: factory rest watch [--poll-interval <duration>] [--json] <job-id>")
		}
		return watchRESTJob(ctx, client, flags.Arg(0), *pollInterval, *jsonOutput, out)
	default:
		return fmt.Errorf("unknown REST client command %q (try factory rest help)", command)
	}
}

type safeRESTJob struct {
	ID                    string          `json:"id"`
	Status                restjobs.Status `json:"status"`
	CancellationRequested bool            `json:"cancellation_requested,omitempty"`
	CreatedAt             time.Time       `json:"created_at"`
	UpdatedAt             time.Time       `json:"updated_at"`
}

type safeRESTHistory struct {
	JobID     string           `json:"job_id"`
	Events    []restjobs.Event `json:"events"`
	Truncated bool             `json:"truncated"`
}

func safeJob(job restjobs.Snapshot) safeRESTJob {
	return safeRESTJob{job.ID, job.Status, job.CancellationRequested, job.CreatedAt, job.UpdatedAt}
}

func safeSubmission(result restclient.Submission) struct {
	Job      safeRESTJob `json:"job"`
	Replayed bool        `json:"replayed"`
	Links    any         `json:"links"`
} {
	return struct {
		Job      safeRESTJob `json:"job"`
		Replayed bool        `json:"replayed"`
		Links    any         `json:"links"`
	}{safeJob(result.Job), result.Replayed, result.Links}
}

func safeHistory(history restjobs.History) safeRESTHistory {
	events := make([]restjobs.Event, 0, len(history.Events))
	for _, event := range history.Events {
		event.Message = ""
		events = append(events, event)
	}
	return safeRESTHistory{history.JobID, events, history.Truncated}
}

func safePage(page restjobs.JobPage) any {
	jobs := make([]safeRESTJob, 0, len(page.Jobs))
	for _, summary := range page.Jobs {
		jobs = append(jobs, safeRESTJob{ID: summary.ID, Status: summary.Status, CreatedAt: summary.CreatedAt, UpdatedAt: summary.UpdatedAt})
	}
	return struct {
		Jobs             []safeRESTJob `json:"jobs"`
		SnapshotSequence uint64        `json:"snapshot_sequence"`
		NextSequence     uint64        `json:"next_sequence,omitempty"`
		HasMore          bool          `json:"has_more"`
	}{jobs, page.SnapshotSequence, page.NextSequence, page.HasMore}
}

func writeRESTResult(out io.Writer, asJSON bool, name string, value any, text string) error {
	if !asJSON {
		fmt.Fprintln(out, text)
		return nil
	}
	safeValue := value
	switch result := value.(type) {
	case restjobs.Snapshot:
		safeValue = safeJob(result)
	case restjobs.JobPage:
		safeValue = safePage(result)
	case restjobs.History:
		safeValue = safeHistory(result)
	}
	return json.NewEncoder(out).Encode(struct {
		SchemaVersion int `json:"schema_version"`
		Result        any `json:"result"`
	}{1, map[string]any{name: safeValue}})
}

func watchRESTJob(ctx context.Context, client *restclient.Client, id string, interval time.Duration, asJSON bool, out io.Writer) error {
	seen := make(map[string]struct{})
	var cursor uint64
	polling := false
	for {
		expiredCursor := false
		if !polling {
			var streamErr error
			expiredCursor = false
			cursor, expiredCursor, streamErr = client.FollowEvents(ctx, id, cursor, func(event restclient.Event) {
				if !asJSON {
					fmt.Fprintf(out, "  %s %s\n", event.At, event.Type)
				}
			})
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if streamErr != nil && !expiredCursor {
				polling = true
			}
		}
		job, err := client.Get(ctx, id)
		if err != nil {
			return err
		}
		history, err := client.History(ctx, id)
		if err != nil {
			return err
		}
		if asJSON && job.Status.Terminal() {
			return json.NewEncoder(out).Encode(struct {
				SchemaVersion int             `json:"schema_version"`
				Job           safeRESTJob     `json:"job"`
				History       safeRESTHistory `json:"history"`
			}{1, safeJob(job), safeHistory(history)})
		}
		if !asJSON {
			fmt.Fprintf(out, "Job %s: %s\n", job.ID, job.Status)
			for _, event := range history.Events {
				key := fmt.Sprintf("%d:%d:%s", event.At.UnixNano(), event.Sequence, event.Type)
				if _, ok := seen[key]; !ok {
					fmt.Fprintf(out, "  %s %s\n", event.At.Format(time.RFC3339), event.Type)
					seen[key] = struct{}{}
				}
			}
		}
		if job.Status.Terminal() {
			return nil
		}
		if expiredCursor && !polling {
			// Status/history above reconcile the lost cursor. Retry the event route
			// without Last-Event-ID when the retained history is a complete baseline.
			if history.Truncated {
				polling = true
			} else {
				cursor = 0
				continue
			}
		}
		if !polling {
			continue
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
