package factory

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"
)

const defaultWorkWatchInterval = time.Minute

type workWatchWait func(context.Context, time.Duration) error

func parseWorkWatch(args []string) (string, time.Duration, error) {
	usage := "usage: factory work watch <dedup-key> [--interval <duration>]"
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return "", 0, fmt.Errorf("%s", usage)
	}
	key, interval := args[0], defaultWorkWatchInterval
	if len(args) == 1 {
		return key, interval, nil
	}
	if len(args) != 3 || args[1] != "--interval" {
		return "", 0, fmt.Errorf("%s", usage)
	}
	parsed, err := time.ParseDuration(args[2])
	if err != nil || parsed <= 0 {
		return "", 0, fmt.Errorf("invalid watch interval %q: expected a positive duration", args[2])
	}
	return key, parsed, nil
}

func watchIssue(ctx context.Context, item WorkItem, tracker IssueTracker, store IssueObservationStore, interval time.Duration, out io.Writer, wait workWatchWait) error {
	if tracker == nil {
		return fmt.Errorf("watch issue: issue tracker is unavailable")
	}
	if store == nil {
		return fmt.Errorf("watch issue: issue observation store is unavailable")
	}
	if interval <= 0 {
		return fmt.Errorf("watch interval must be positive")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		snapshot, err := tracker.GetIssue(ctx, item.Request.Repository, item.Request.IssueID)
		if err != nil {
			return fmt.Errorf("watch issue: %w", err)
		}
		recorded, err := store.Record(ctx, item.Request.DeduplicationKey, snapshot)
		if err != nil {
			return fmt.Errorf("record issue observation: %w", err)
		}
		printIssueSnapshot(out, snapshot)
		if recorded {
			fmt.Fprintln(out, "Observation: newly recorded")
		} else {
			fmt.Fprintln(out, "Observation: duplicate")
		}
		if snapshot.State == "closed" {
			return nil
		}
		if err := wait(ctx, interval); err != nil {
			return err
		}
	}
}

func waitWorkWatchInterval(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
