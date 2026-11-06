package restjobs

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPostgresIntakeOwnerRegistrationAndTransferFencesAdapters(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL intake-owner integration tests")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	store, err := NewPostgresStore(scopedPostgresDB(t, admin, dsn), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	ctx := context.Background()
	owner, err := store.RegisterIntakeOwner(ctx, "acme/issues", "Ready", IntakeOwnerWebhook)
	if err != nil || owner.Generation != 1 {
		t.Fatalf("registration=(%+v,%v)", owner, err)
	}
	retry, err := store.RegisterIntakeOwner(ctx, "acme/issues", "Ready", IntakeOwnerWebhook)
	if err != nil || retry != owner {
		t.Fatalf("idempotent registration=(%+v,%v)", retry, err)
	}
	if _, err := store.RegisterIntakeOwner(ctx, "acme/issues", "Ready", IntakeOwnerPoller); err == nil {
		t.Fatal("conflicting owner registration succeeded")
	}
	transferred, err := store.TransferIntakeOwner(ctx, "acme/issues", "Ready", IntakeOwnerPoller, "webhook stopped; delivery effects reconciled")
	if err != nil || transferred.Generation != 2 || transferred.Kind != IntakeOwnerPoller {
		t.Fatalf("transfer=(%+v,%v)", transferred, err)
	}
	if _, _, err := store.AdmitGitHubIssueWebhook(ctx, "stale", strings.Repeat("a", 64), "issue-owner", Request{Repository: "widget", Task: "stale"}, owner); err == nil {
		t.Fatal("webhook admission with prior generation succeeded")
	}
	if _, err := store.RegisterIntakeOwner(ctx, "acme/issues", "Ready", IntakeOwnerWebhook); err == nil {
		t.Fatal("old adapter reacquired ownership")
	}
	audit, err := store.ListIntakeOwnerAudit(ctx, "acme/issues", "Ready")
	if err != nil || len(audit) != 2 || audit[1].PreviousKind != IntakeOwnerWebhook || audit[1].Attestation == "" {
		t.Fatalf("audit=(%+v,%v)", audit, err)
	}
}

func TestSQLiteGitHubWebhookAdmissionFailsClosedWithoutPartialRows(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "jobs.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	body := sha256.Sum256([]byte("signed delivery body"))
	_, replay, err := store.AdmitGitHubIssueWebhook(context.Background(), "delivery-42", fmt.Sprintf("%x", body), "issue-owner", Request{Repository: "widget", Task: "bounded issue task"}, IntakeOwner{})
	if !errors.Is(err, ErrGitHubWebhookUnavailable) || replay {
		t.Fatalf("SQLite webhook admission=(replayed %t, %v), want fail-closed unavailable", replay, err)
	}
	for _, table := range []string{"factory_github_issue_webhook_deliveries", "factory_threads", "factory_thread_jobs", "factory_thread_idempotency", "factory_jobs"} {
		var count int
		if err := store.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("rejected webhook left %d rows in %s (err=%v)", count, table, err)
		}
	}
}

func TestPostgresGitHubWebhookAdmissionIsAtomicAndDeliveryIdempotent(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL 18 webhook integration tests")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	var version int
	if err := admin.QueryRow(`SHOW server_version_num`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version < 180000 || version >= 190000 {
		t.Fatalf("PostgreSQL version %d is outside the supported integration test version", version)
	}
	firstDB := scopedPostgresDB(t, admin, dsn)
	schema := currentTestSchema(t, firstDB)
	secondDB := postgresDBForSchema(t, dsn, schema)
	first, err := NewPostgresStore(firstDB, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewPostgresStore(secondDB, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.CloseStore(); _ = second.CloseStore() })
	body := sha256.Sum256([]byte("postgres delivery"))
	request := Request{Repository: "widget", Task: "postgres issue work"}
	owner, err := first.RegisterIntakeOwner(context.Background(), "acme/issues", "Ready", IntakeOwnerWebhook)
	if err != nil {
		t.Fatal(err)
	}
	webhookOwner := owner
	initial, replay, err := first.AdmitGitHubIssueWebhook(context.Background(), "pg-delivery", fmt.Sprintf("%x", body), "issue-owner", request, webhookOwner)
	if err != nil || replay {
		t.Fatalf("initial admission=(%+v,%t,%v)", initial, replay, err)
	}
	again, replay, err := second.AdmitGitHubIssueWebhook(context.Background(), "pg-delivery", fmt.Sprintf("%x", body), "issue-owner", request, webhookOwner)
	if err != nil || !replay || again.Thread.ID != initial.Thread.ID || again.Jobs[0].ID != initial.Jobs[0].ID {
		t.Fatalf("cross-connection same-delivery retry=(%+v,%t,%v)", again, replay, err)
	}
	changed := sha256.Sum256([]byte("different PostgreSQL payload"))
	if _, _, err := second.AdmitGitHubIssueWebhook(context.Background(), "pg-delivery", fmt.Sprintf("%x", changed), "issue-owner", request, webhookOwner); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed PostgreSQL body error=%v", err)
	}
	transferredOwner, err := first.TransferIntakeOwner(context.Background(), "acme/issues", "Ready", IntakeOwnerPoller, "webhook stopped; external effects reconciled")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := second.AdmitGitHubIssueWebhook(context.Background(), "stale-delivery", fmt.Sprintf("%x", changed), "issue-owner", request, webhookOwner); err == nil {
		t.Fatal("webhook admission with the prior generation succeeded")
	}
	if transferredOwner.Generation != webhookOwner.Generation+1 {
		t.Fatalf("transfer generation=%d, want %d", transferredOwner.Generation, webhookOwner.Generation+1)
	}
	var jobs, threads, deliveries int
	for query, target := range map[string]*int{
		`SELECT COUNT(*) FROM factory_jobs`:                            &jobs,
		`SELECT COUNT(*) FROM factory_threads`:                         &threads,
		`SELECT COUNT(*) FROM factory_github_issue_webhook_deliveries`: &deliveries,
	} {
		if err := firstDB.QueryRow(query).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	if jobs != 1 || threads != 1 || deliveries != 1 {
		t.Fatalf("PostgreSQL delivery identity not atomic: jobs=%d threads=%d deliveries=%d", jobs, threads, deliveries)
	}
	boundedConfig := testConfig()
	boundedConfig.QueueCapacity = 1
	boundedConfig.MaxConcurrentJobs = 1
	boundedConfig.MaxRecords = 1
	bounded, err := NewPostgresStore(scopedPostgresDB(t, admin, dsn), boundedConfig)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bounded.RegisterIntakeOwner(context.Background(), "acme/issues", "Ready", IntakeOwnerWebhook); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bounded.CloseStore() })
	if _, _, err := bounded.Admit("fill-capacity", Request{Repository: "widget", Task: "fills capacity"}); err != nil {
		t.Fatal(err)
	}
	other := sha256.Sum256([]byte("must roll back"))
	if _, _, err := bounded.AdmitGitHubIssueWebhook(context.Background(), "pg-rollback", fmt.Sprintf("%x", other), "issue-owner", Request{Repository: "widget", Task: "over capacity"}, webhookOwner); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("PostgreSQL rollback admission error=%v", err)
	}
	var rollbackDeliveries, rollbackThreads, rollbackJobs int
	for query, target := range map[string]*int{
		`SELECT COUNT(*) FROM factory_github_issue_webhook_deliveries`: &rollbackDeliveries,
		`SELECT COUNT(*) FROM factory_threads`:                         &rollbackThreads,
		`SELECT COUNT(*) FROM factory_jobs`:                            &rollbackJobs,
	} {
		if err := bounded.db.QueryRow(query).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	if rollbackDeliveries != 0 || rollbackThreads != 0 || rollbackJobs != 1 {
		t.Fatalf("PostgreSQL failed admission left partial state: deliveries=%d threads=%d jobs=%d", rollbackDeliveries, rollbackThreads, rollbackJobs)
	}
}
