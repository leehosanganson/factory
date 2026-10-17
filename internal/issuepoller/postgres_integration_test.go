package issuepoller

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	factorycore "github.com/leehosanganson/factory/internal/factory"
	"github.com/leehosanganson/factory/internal/restjobs"
)

func TestPostgresProvenanceConflictQuarantinesAndAdvancesScan(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL issue-poller integration tests")
	}
	adminConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*adminConfig)
	t.Cleanup(func() { _ = admin.Close() })
	schema := fmt.Sprintf("issue_poller_conflict_%d", time.Now().UnixNano())
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})
	storeConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	storeConfig.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*storeConfig)
	db.SetMaxOpenConns(1)
	store, err := restjobs.NewPostgresStore(db, restjobs.Config{
		QueueCapacity: 8, MaxConcurrentJobs: 2, MaxRecords: 8, MaxEventsPerJob: 8,
		MaxTaskBytes: 256 << 10, RegistryBytes: 256 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.CloseStore() })

	const version = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	provenance := &restjobs.IssueProvenance{
		Tracker: "github", SourceRepository: "source/repo", IssueNumber: 42,
		Version: version, URL: "https://github.com/source/repo/issues/42", Title: "Existing issue",
	}
	ownerRequest := restjobs.Request{Repository: "destination", Task: "existing owner's separate work", Issue: issueNumberPointer(42), IssueProvenance: provenance}
	ownerJob, _, err := store.AdmitAs("owner-key", "another-principal", ownerRequest)
	if err != nil {
		t.Fatal(err)
	}

	const privateBody = "ISSUE_BODY_MUST_NOT_BE_PERSISTED_8f2d"
	snapshot := factorycore.IssueSnapshot{
		Repository: "source/repo", Number: 42, Title: "Candidate", Body: privateBody,
		State: "open", URL: provenance.URL, Version: version,
	}
	provider := &fakeProvider{
		pages:       map[int]CandidatePage{1: {Issues: []factorycore.IssueSnapshot{snapshot}, HasNext: true}},
		issues:      map[int]factorycore.IssueSnapshot{42: snapshot},
		issueLabels: map[int][]string{42: {"ready"}},
	}
	config := Config{
		Sources: map[string]string{"source/repo": "destination"}, Trigger: "ready", BatchSize: 10,
		QueueCapacity: 8, Workers: 2, MaxRecords: 8, MaxEvents: 8, TaskBytes: 256 << 10, RegistryBytes: 256 << 20,
	}
	if err := (Poller{Config: config, Provider: provider, Store: store}).PollOnce(context.Background()); err != nil {
		t.Fatalf("poll conflicting provenance: %v", err)
	}

	page, err := store.IssuePollerCursor(context.Background(), "source/repo")
	if err != nil || page != 2 {
		t.Fatalf("scan cursor=(%d,%v), want next page 2", page, err)
	}
	if provider.removals != 0 || !containsExact(provider.issueLabels[42], "ready") {
		t.Fatalf("conflicting issue trigger was modified: removals=%d labels=%v", provider.removals, provider.issueLabels[42])
	}
	gotOwner, err := store.GetForPrincipal(ownerJob.ID, "another-principal")
	if err != nil || gotOwner.ID != ownerJob.ID || gotOwner.Request.Task != ownerRequest.Task {
		t.Fatalf("pre-existing owner job changed: job=%+v err=%v", gotOwner, err)
	}
	actions, err := store.ListIssuePollerOperatorActions(context.Background(), 10)
	if err != nil || len(actions) != 1 {
		t.Fatalf("operator actions=(%+v,%v), want one unresolved quarantine", actions, err)
	}
	action := actions[0]
	if action.SourceRepository != "source/repo" || action.IssueNumber != 42 || action.Version != version || action.Outcome != "quarantined" || action.ReasonClass != "provenance_conflict" {
		t.Fatalf("operator action lacks bounded conflict identity: %+v", action)
	}
	if strings.Contains(fmt.Sprintf("%+v", action), privateBody) {
		t.Fatal("operator action exposed issue text")
	}
	var persistedIssueText int
	if err := db.QueryRow(`SELECT COUNT(*) FROM factory_jobs WHERE request_json::text LIKE '%' || $1 || '%'`, privateBody).Scan(&persistedIssueText); err != nil {
		t.Fatal(err)
	}
	if persistedIssueText != 0 {
		t.Fatal("conflicting candidate issue body was persisted")
	}
	provider.pages[2] = CandidatePage{HasNext: true}
	if err := (Poller{Config: config, Provider: provider, Store: store}).PollOnce(context.Background()); err != nil {
		t.Fatalf("scan after quarantined page: %v", err)
	}
	page, err = store.IssuePollerCursor(context.Background(), "source/repo")
	if err != nil || page != 3 {
		t.Fatalf("subsequent scan cursor=(%d,%v), want page 3", page, err)
	}
	if err := store.ResolveIssuePollerOperatorAction(context.Background(), "source/repo", 42, version); err != nil {
		t.Fatalf("operator could not explicitly resolve quarantine: %v", err)
	}
	if unresolved, err := store.ListIssuePollerOperatorActions(context.Background(), 10); err != nil || len(unresolved) != 0 {
		t.Fatalf("resolved quarantine remains actionable: (%+v,%v)", unresolved, err)
	}
}

func issueNumberPointer(value int) *int { return &value }
