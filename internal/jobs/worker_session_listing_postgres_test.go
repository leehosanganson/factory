package restjobs

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"
)

func TestPostgresWorkerSessionListingUsesNewestFirstBoundedKeyset(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL worker session listing integration")
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
	ctx := context.Background()
	id, err := newJobID()
	if err != nil {
		t.Fatal(err)
	}
	principal := "listing-worker-" + id
	first, _, err := store.RegisterWorkerSession(ctx, principal+"-first", "boot-first")
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := store.RegisterWorkerSession(ctx, principal+"-second", "boot-second")
	if err != nil {
		t.Fatal(err)
	}
	third, _, err := store.RegisterWorkerSession(ctx, principal+"-third", "boot-third")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2025, 1, 2, 3, 4, 5, 120_000_000, time.UTC)
	for _, item := range []struct {
		id string
		at time.Time
	}{
		{first.ID, base}, {second.ID, base}, {third.ID, base.Add(2 * time.Microsecond)},
	} {
		if _, err := db.ExecContext(ctx, `UPDATE factory_worker_sessions SET created_at=$1 WHERE id=$2`, item.at, item.id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.RetireWorkerSession(ctx, third.PrincipalID, third.BootID, third.ID); err != nil {
		t.Fatal(err)
	}
	page, err := store.ListWorkerSessions(ctx, nil, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Sessions) != 2 || !page.HasMore || page.Sessions[0].ID != third.ID || !page.Sessions[1].CreatedAt.Equal(base) {
		t.Fatalf("PostgreSQL first page=%+v; want newest retired session then a tied timestamp", page)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, principal+"-later", "boot-later"); err != nil {
		t.Fatal(err)
	}
	continued, err := store.ListWorkerSessions(ctx, page.NextCreatedAt, page.NextID, 2)
	if err != nil {
		t.Fatal(err)
	}
	remainingID := first.ID
	if page.Sessions[1].ID == first.ID {
		remainingID = second.ID
	}
	if len(continued.Sessions) != 1 || continued.Sessions[0].ID != remainingID || continued.HasMore {
		t.Fatalf("PostgreSQL continuation=%+v; want remaining tied session %q only", continued, remainingID)
	}
	var heartbeat sql.NullTime
	if err := db.QueryRowContext(ctx, `SELECT heartbeat_at FROM factory_worker_sessions WHERE id=$1`, third.ID).Scan(&heartbeat); err != nil || heartbeat.Valid {
		t.Fatalf("PostgreSQL null heartbeat=%v err=%v; want null", heartbeat, err)
	}
}
