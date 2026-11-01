package restjobs

import (
	"context"
	"os"
	"testing"
	"time"
)

func pollerIntPointer(value int) *int { return &value }

func TestIssuePollerLabelOutcomeHasRealBoundedRetryCap(t *testing.T) {
	for _, test := range []struct {
		attempts int
		want     string
	}{
		{maxIssuePollerOutcomeAttempts - 2, "label_removal_failed"},
		{maxIssuePollerOutcomeAttempts - 1, "operator_action_required"},
		{maxIssuePollerOutcomeAttempts, "operator_action_required"},
	} {
		outcome, reason, err := issuePollerLabelOutcome(test.attempts, "label_removal_failed")
		if err != nil || outcome != test.want {
			t.Errorf("attempts=%d outcome=(%q,%q,%v), want %q", test.attempts, outcome, reason, err, test.want)
		}
		if test.want == "operator_action_required" && reason != "label_removal_failed" {
			t.Errorf("terminal reason=%q, want bounded class", reason)
		}
	}
	if outcome, _, err := issuePollerLabelOutcome(0, "label_removed"); err != nil || outcome != "label_removed" {
		t.Fatalf("success outcome=(%q,%v)", outcome, err)
	}
}

func TestIssuePollerLedgerRetentionFailsClosedWhenAllRecordsUnresolved(t *testing.T) {
	if got, err := issuePollerRetentionDeleteCount(11, 0, 10); err == nil || got != 0 {
		t.Fatalf("retention plan=(%d,%v), want failure rather than deleting unresolved outcomes", got, err)
	}
	if got, err := issuePollerRetentionDeleteCount(11, 2, 10); err != nil || got != 2 {
		t.Fatalf("retention plan for completed records=(%d,%v), want 2", got, err)
	}
}

func TestIssuePollerPostgresGuardAndAdmissionIntegration(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL issue-poller integration tests")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	db := scopedPostgresDB(t, admin, dsn)
	store, err := NewPostgresStore(db, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.CloseStore() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	schema := currentTestSchema(t, db)
	secondDB := postgresDBForSchema(t, dsn, schema)
	secondStore, err := NewPostgresStore(secondDB, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondStore.CloseStore() })
	first, err := store.AcquireIssuePollerGuard(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := secondStore.AcquireIssuePollerGuard(ctx); err == nil {
		t.Fatal("second database-wide poller lock acquired")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := secondStore.AcquireIssuePollerGuard(ctx)
	if err != nil {
		t.Fatalf("lock unavailable after release: %v", err)
	}
	_ = second.Close()
	page, err := store.IssuePollerCursor(ctx, "source/widget")
	if err != nil || page != 1 {
		t.Fatalf("initial cursor=(%d,%v)", page, err)
	}
	if err := store.AdvanceIssuePollerCursor(ctx, "source/widget", page, 4); err != nil {
		t.Fatal(err)
	}
	if err := secondStore.CloseStore(); err != nil {
		t.Fatal(err)
	}
	restartedDB := postgresDBForSchema(t, dsn, schema)
	restartedStore, err := NewPostgresStore(restartedDB, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restartedStore.CloseStore() })
	page, err = restartedStore.IssuePollerCursor(ctx, "source/widget")
	if err != nil || page != 4 {
		t.Fatalf("cursor did not survive poller restart: page=%d err=%v", page, err)
	}
	if err := store.AdvanceIssuePollerCursor(ctx, "source/widget", 1, 2); err == nil {
		t.Fatal("stale cursor update advanced past newer durable scan state")
	}
	request := Request{Repository: "widget", Task: "safe task", Issue: pollerIntPointer(91), IssueProvenance: &IssueProvenance{Tracker: "github", SourceRepository: "source/widget", IssueNumber: 91, Version: stringsOf('a', 64), URL: "https://github.com/source/widget/issues/91", Title: "Issue"}}
	job, replay, err := store.AdmitIssuePoller(ctx, "poller-test-key-91", "github-issue-poller", request, "trigger")
	if err != nil || replay || job.Status != StatusQueued {
		t.Fatalf("admission=(%+v,%t,%v)", job, replay, err)
	}
	pending, err := store.ListIssuePollerPending(ctx)
	if err != nil || len(pending) != 1 || pending[0].JobID != job.ID || pending[0].Label != "trigger" {
		t.Fatalf("pending=(%+v,%v)", pending, err)
	}
	jobAgain, replay, err := store.AdmitIssuePoller(ctx, "poller-test-key-91", "github-issue-poller", request, "trigger")
	if err != nil || !replay || jobAgain.ID != job.ID {
		t.Fatalf("replay=(%+v,%t,%v)", jobAgain, replay, err)
	}
	if err := store.FinishIssuePollerLabel(ctx, "source/widget", 91, stringsOf('a', 64), "label_removed"); err != nil {
		t.Fatal(err)
	}
	pending, err = store.ListIssuePollerPending(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatalf("completed outcome remains pending: (%+v,%v)", pending, err)
	}

	quarantineVersion := stringsOf('b', 64)
	if err := store.RecordIssuePollerQuarantine(ctx, "source/widget", 92, quarantineVersion, "task_too_large"); err != nil {
		t.Fatal(err)
	}
	if outcome, err := store.IssuePollerOutcome(ctx, "source/widget", 92, quarantineVersion); err != nil || outcome != "quarantined" {
		t.Fatalf("quarantine replay state=(%q,%v)", outcome, err)
	}
	replayDB := postgresDBForSchema(t, dsn, schema)
	replayStore, err := NewPostgresStore(replayDB, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replayStore.CloseStore() })
	if outcome, err := replayStore.IssuePollerOutcome(ctx, "source/widget", 92, quarantineVersion); err != nil || outcome != "quarantined" {
		t.Fatalf("quarantine did not survive store reopen: outcome=%q err=%v", outcome, err)
	}
	quarantineActions, err := store.ListIssuePollerOperatorActions(ctx, 10)
	if err != nil || len(quarantineActions) != 1 || quarantineActions[0].ReasonClass != "task_too_large" || quarantineActions[0].Version != quarantineVersion {
		t.Fatalf("bounded quarantine listing=(%+v,%v)", quarantineActions, err)
	}

	retryVersion := stringsOf('d', 64)
	retryRequest := Request{Repository: "widget", Task: "bounded label retries", Issue: pollerIntPointer(94), IssueProvenance: &IssueProvenance{Tracker: "github", SourceRepository: "source/widget", IssueNumber: 94, Version: retryVersion, URL: "https://github.com/source/widget/issues/94", Title: "Issue"}}
	if _, _, err := store.AdmitIssuePoller(ctx, "poller-test-key-94", "github-issue-poller", retryRequest, "trigger"); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < maxIssuePollerOutcomeAttempts; attempt++ {
		if err := store.FinishIssuePollerLabel(ctx, "source/widget", 94, retryVersion, "label_removal_failed"); err != nil {
			t.Fatalf("label failure attempt %d: %v", attempt+1, err)
		}
	}
	if err := store.FinishIssuePollerLabel(ctx, "source/widget", 94, retryVersion, "label_removal_failed"); err == nil {
		t.Fatal("label operation remained retryable after cap")
	}
	pending, err = store.ListIssuePollerPending(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatalf("exhausted retry remains pending: (%+v,%v)", pending, err)
	}
	actions, err := store.ListIssuePollerOperatorActions(ctx, 10)
	if err != nil || len(actions) != 2 || actions[0].Outcome != "quarantined" && actions[1].Outcome != "quarantined" {
		t.Fatalf("operator actions after retry cap=(%+v,%v)", actions, err)
	}
	if err := store.ResolveIssuePollerOperatorAction(ctx, "source/widget", 94, retryVersion); err != nil {
		t.Fatal(err)
	}
	if err := store.ResolveIssuePollerOperatorAction(ctx, "source/widget", 92, quarantineVersion); err != nil {
		t.Fatal(err)
	}
	if actions, err := store.ListIssuePollerOperatorActions(ctx, 10); err != nil || len(actions) != 0 {
		t.Fatalf("explicitly resolved actions remain listed=(%+v,%v)", actions, err)
	}

	newRequest := Request{Repository: "widget", Task: "capacity after terminal outcome", Issue: pollerIntPointer(93), IssueProvenance: &IssueProvenance{Tracker: "github", SourceRepository: "source/widget", IssueNumber: 93, Version: stringsOf('c', 64), URL: "https://github.com/source/widget/issues/93", Title: "Issue"}}
	if _, _, err := store.AdmitIssuePoller(ctx, "poller-test-key-93", "github-issue-poller", newRequest, "trigger"); err != nil {
		t.Fatalf("terminal outcome blocked future admission: %v", err)
	}
}
