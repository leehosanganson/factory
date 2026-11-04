package restjobs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSQLiteWorkerMappingLookupReflectsCommittedState(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "worker-mapping-lookup.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	ctx := context.Background()
	fingerprint := strings.Repeat("b", 64)
	if _, err := store.ReconcileWorkerMappings(ctx, map[string]string{fingerprint: "worker-before"}); err != nil {
		t.Fatal(err)
	}
	if principal, found, err := store.LookupWorkerMapping(ctx, fingerprint); err != nil || !found || principal != "worker-before" {
		t.Fatalf("initial lookup=(%q,%v,%v)", principal, found, err)
	}
	var auditRows int
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_mapping_audit`).Scan(&auditRows); err != nil || auditRows != 1 {
		t.Fatalf("authentication lookup changed mapping audit rows=%d err=%v; want only registration event", auditRows, err)
	}
	if _, err := store.ReconcileWorkerMappings(ctx, map[string]string{fingerprint: "worker-after"}); err != nil {
		t.Fatal(err)
	}
	if principal, found, err := store.LookupWorkerMapping(ctx, fingerprint); err != nil || !found || principal != "worker-after" {
		t.Fatalf("changed lookup=(%q,%v,%v)", principal, found, err)
	}
	if _, err := store.ReconcileWorkerMappings(ctx, map[string]string{}); err != nil {
		t.Fatal(err)
	}
	if principal, found, err := store.LookupWorkerMapping(ctx, fingerprint); err != nil || found || principal != "" {
		t.Fatalf("revoked lookup=(%q,%v,%v), want absent", principal, found, err)
	}
	if _, _, err := store.LookupWorkerMapping(nil, fingerprint); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("nil context lookup error=%v, want invalid input", err)
	}
}

func TestSQLiteWorkerMappingAuditFailureRollsBackRegistryState(t *testing.T) {
	dbPath := filepath.Join(privateSQLiteDir(t), "worker-mapping-audit-failure.db")
	store, err := OpenSQLiteStore(dbPath, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	ctx := context.Background()
	fingerprint := strings.Repeat("d", 64)
	initial, err := store.ReconcileWorkerMappings(ctx, map[string]string{fingerprint: "worker-before"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER reject_worker_mapping_audit BEFORE INSERT ON factory_worker_mapping_audit BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReconcileWorkerMappings(ctx, map[string]string{fingerprint: "worker-after"}); err == nil {
		t.Fatal("mapping reconciliation succeeded when its audit write failed")
	}
	current, err := store.ReconcileWorkerMappings(ctx, map[string]string{fingerprint: "worker-before"})
	if err != nil {
		t.Fatalf("previous mapping was not retained after failed audit: %v", err)
	}
	if current.Generation != initial.Generation || current.Mappings[fingerprint] != "worker-before" {
		t.Fatalf("state after failed audit=(%+v), want prior state %+v", current, initial)
	}
	var auditRows int
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_mapping_audit WHERE fingerprint=?`, fingerprint).Scan(&auditRows); err != nil || auditRows != 1 {
		t.Fatalf("audit rows after failed change=%d, err=%v; want only initial accepted event", auditRows, err)
	}
}

func TestPostgresWorkerMappingLookupUsesCurrentCommittedRegistry(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL worker mapping integration tests")
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
	fingerprint := strings.Repeat("a", 64)
	if _, err := store.ReconcileWorkerMappings(ctx, map[string]string{fingerprint: "worker-before"}); err != nil {
		t.Fatal(err)
	}
	principal, found, err := store.LookupWorkerMapping(ctx, fingerprint)
	if err != nil || !found || principal != "worker-before" {
		t.Fatalf("initial lookup=(%q,%v,%v)", principal, found, err)
	}
	if _, err := store.ReconcileWorkerMappings(ctx, map[string]string{fingerprint: "worker-after"}); err != nil {
		t.Fatal(err)
	}
	principal, found, err = store.LookupWorkerMapping(ctx, fingerprint)
	if err != nil || !found || principal != "worker-after" {
		t.Fatalf("changed lookup=(%q,%v,%v)", principal, found, err)
	}
	if _, err := store.ReconcileWorkerMappings(ctx, map[string]string{}); err != nil {
		t.Fatal(err)
	}
	if principal, found, err := store.LookupWorkerMapping(ctx, fingerprint); err != nil || found || principal != "" {
		t.Fatalf("revoked lookup=(%q,%v,%v), want absent", principal, found, err)
	}
}

func TestPostgresWorkerMappingReconciliationAndAuditFailureAtomicity(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL worker mapping integration tests")
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
	first, second := strings.Repeat("e", 64), strings.Repeat("f", 64)
	accepted, err := store.ReconcileWorkerMappings(ctx, map[string]string{first: "worker-one", second: "worker-two"})
	if err != nil || accepted.Generation != 1 || len(accepted.Mappings) != 2 {
		t.Fatalf("accepted state=(%+v,%v)", accepted, err)
	}
	changed, err := store.ReconcileWorkerMappings(ctx, map[string]string{first: "worker-renamed"})
	if err != nil || changed.Generation != 2 || len(changed.Mappings) != 1 || changed.Mappings[first] != "worker-renamed" {
		t.Fatalf("changed/removed state=(%+v,%v)", changed, err)
	}
	schema := currentTestSchema(t, db)
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	db = postgresDBForSchema(t, dsn, schema)
	store, err = NewPostgresStore(db, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.CloseStore() })
	if _, err := store.ReconcileWorkerMappings(ctx, map[string]string{first: "worker-renamed", second: "worker-two"}); err == nil {
		t.Fatal("removed fingerprint was re-added after reopening PostgreSQL store")
	}
	if _, err := db.Exec(`CREATE FUNCTION reject_worker_mapping_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit unavailable'; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER reject_worker_mapping_audit BEFORE INSERT ON factory_worker_mapping_audit FOR EACH ROW EXECUTE FUNCTION reject_worker_mapping_audit()`); err != nil {
		t.Fatal(err)
	}
	third := strings.Repeat("1", 64)
	if _, err := store.ReconcileWorkerMappings(ctx, map[string]string{first: "worker-final", third: "worker-three"}); err == nil {
		t.Fatal("mapping reconciliation succeeded when its audit write failed")
	}
	var generation int
	var principal string
	if err := db.QueryRow(`SELECT generation FROM factory_worker_mapping_state WHERE singleton=1`).Scan(&generation); err != nil || generation != 2 {
		t.Fatalf("generation after rejected audit=%d, err=%v; want 2", generation, err)
	}
	if err := db.QueryRow(`SELECT principal_id FROM factory_worker_mappings WHERE fingerprint=$1`, first).Scan(&principal); err != nil || principal != "worker-renamed" {
		t.Fatalf("mapping after rejected audit=%q, err=%v; want worker-renamed", principal, err)
	}
	var thirdMappings int
	if err := db.QueryRow(`SELECT count(*) FROM factory_worker_mappings WHERE fingerprint=$1`, third).Scan(&thirdMappings); err != nil || thirdMappings != 0 {
		t.Fatalf("new mapping rows after rejected audit=%d, err=%v; want 0", thirdMappings, err)
	}
	var removedRevoked bool
	if err := db.QueryRow(`SELECT revoked FROM factory_worker_mappings WHERE fingerprint=$1`, second).Scan(&removedRevoked); err != nil || !removedRevoked {
		t.Fatalf("removed mapping tombstone=(%v,%v), want revoked", removedRevoked, err)
	}
	var auditRows int
	if err := db.QueryRow(`SELECT count(*) FROM factory_worker_mapping_audit`).Scan(&auditRows); err != nil || auditRows != 4 {
		t.Fatalf("audit rows after rejected reconciliation=%d, err=%v; want initial accept x2, one change, and one removal", auditRows, err)
	}
}

func TestSQLiteWorkerMappingsPersistChangesAndPermanentRevocation(t *testing.T) {
	dbPath := filepath.Join(privateSQLiteDir(t), "worker-mappings.db")
	store, err := OpenSQLiteStore(dbPath, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	fingerprint := strings.Repeat("a", 64)
	accepted, err := store.ReconcileWorkerMappings(ctx, map[string]string{fingerprint: "worker-one"})
	if err != nil || accepted.Generation != 1 || accepted.Mappings[fingerprint] != "worker-one" {
		t.Fatalf("accepted mappings=(%+v,%v)", accepted, err)
	}
	unchanged, err := store.ReconcileWorkerMappings(ctx, map[string]string{fingerprint: "worker-one"})
	if err != nil || unchanged.Generation != accepted.Generation {
		t.Fatalf("unchanged mappings=(%+v,%v), want stable generation", unchanged, err)
	}
	changed, err := store.ReconcileWorkerMappings(ctx, map[string]string{fingerprint: "worker-two"})
	if err != nil || changed.Generation != 2 || changed.Mappings[fingerprint] != "worker-two" {
		t.Fatalf("changed mappings=(%+v,%v)", changed, err)
	}
	removed, err := store.ReconcileWorkerMappings(ctx, map[string]string{})
	if err != nil || removed.Generation != 3 || len(removed.Mappings) != 0 {
		t.Fatalf("removed mappings=(%+v,%v)", removed, err)
	}
	if _, err := store.ReconcileWorkerMappings(ctx, map[string]string{fingerprint: "worker-one"}); err == nil {
		t.Fatal("revoked fingerprint was re-added")
	}
	if _, err := store.db.Exec(`UPDATE factory_worker_mapping_audit SET event='tampered' WHERE fingerprint=?`, fingerprint); err == nil {
		t.Fatal("worker mapping audit accepted an update")
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLiteStore(dbPath, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.CloseStore()
	if _, err := reopened.ReconcileWorkerMappings(ctx, map[string]string{fingerprint: "worker-one"}); err == nil {
		t.Fatal("revoked fingerprint was re-added after reopening the durable store")
	}
	var revoked, audit int
	if err := reopened.db.QueryRow(`SELECT revoked FROM factory_worker_mappings WHERE fingerprint=?`, fingerprint).Scan(&revoked); err != nil || revoked != 1 {
		t.Fatalf("tombstone=(%d,%v), want permanent revocation", revoked, err)
	}
	if err := reopened.db.QueryRow(`SELECT count(*) FROM factory_worker_mapping_audit WHERE fingerprint=?`, fingerprint).Scan(&audit); err != nil || audit != 3 {
		t.Fatalf("audit rows=%d, err=%v; want accepted, changed, revoked", audit, err)
	}
	if _, err := reopened.ReconcileWorkerMappings(nil, map[string]string{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("nil context error=%v", err)
	}
}
