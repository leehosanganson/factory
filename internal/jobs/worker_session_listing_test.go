package restjobs

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteWorkerSessionListingUsesNewestFirstBoundedKeysetAndSafeProjection(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "worker-session-list.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	old, _, err := store.RegisterWorkerSession(ctx, "worker-list-old", "boot-old")
	if err != nil {
		t.Fatal(err)
	}
	newest, _, err := store.RegisterWorkerSession(ctx, "worker-list-new", "boot-new")
	if err != nil {
		t.Fatal(err)
	}
	oldTime := time.Date(2025, 1, 2, 3, 4, 5, 120_000_000, time.UTC)
	newTime := oldTime.Add(time.Nanosecond)
	if _, err := store.db.ExecContext(ctx, `UPDATE factory_worker_sessions SET created_at=?,heartbeat_at=? WHERE id=?`, oldTime.Format(time.RFC3339Nano), oldTime.Format(time.RFC3339Nano), old.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE factory_worker_sessions SET created_at=?,heartbeat_at=NULL WHERE id=?`, newTime.Format(time.RFC3339Nano), newest.ID); err != nil {
		t.Fatal(err)
	}
	retired, _, err := store.RegisterWorkerSession(ctx, "worker-list-retired", "boot-retired")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RetireWorkerSession(ctx, retired.PrincipalID, retired.BootID, retired.ID); err != nil {
		t.Fatal(err)
	}
	retiredTime := oldTime.Add(2 * time.Nanosecond)
	if _, err := store.db.ExecContext(ctx, `UPDATE factory_worker_sessions SET created_at=?,retired_at=? WHERE id=?`, retiredTime.Format(time.RFC3339Nano), retiredTime.Add(time.Second).Format(time.RFC3339Nano), retired.ID); err != nil {
		t.Fatal(err)
	}

	page, err := store.ListWorkerSessions(ctx, nil, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Sessions) != 2 || !page.HasMore || page.Sessions[0].ID != retired.ID || page.Sessions[1].ID != newest.ID {
		t.Fatalf("first page = %+v; want retired and active newest-first", page)
	}
	if page.Sessions[0].RetiredAt == nil || page.Sessions[0].HeartbeatAt != nil || page.Sessions[1].RetiredAt != nil || page.Sessions[1].HeartbeatAt != nil {
		t.Fatalf("nullable timestamps not retained in projection: %+v", page.Sessions)
	}
	if page.Sessions[0].PrincipalID != retired.PrincipalID || page.Sessions[0].Status != "retired" {
		t.Fatalf("session identity/status projection = %+v", page.Sessions[0])
	}
	encoded, err := json.Marshal(page.Sessions[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"boot_id", "capability", "capability_hash", "capability_expires_at"} {
		if containsJSONField(string(encoded), forbidden) {
			t.Fatalf("safe session projection exposes %q: %s", forbidden, encoded)
		}
	}
	if _, _, err := store.RegisterWorkerSession(ctx, "worker-list-arrived-later", "boot-later"); err != nil {
		t.Fatal(err)
	}
	second, err := store.ListWorkerSessions(ctx, page.NextCreatedAt, page.NextID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Sessions) != 1 || second.Sessions[0].ID != old.ID || second.HasMore {
		t.Fatalf("continuation page = %+v; want oldest active session", second)
	}
}

func canceledWorkerSessionListContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func containsJSONField(encoded, field string) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(encoded), &fields) != nil {
		return true
	}
	_, exists := fields[field]
	return exists
}

func TestWorkerSessionPageValidation(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "worker-session-list-validation.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, test := range []struct {
		name   string
		ctx    context.Context
		before *time.Time
		id     string
		limit  int
	}{
		{name: "nil context", before: nil, limit: 1},
		{name: "empty cursor id", ctx: context.Background(), before: func() *time.Time { value := time.Now(); return &value }(), limit: 1},
		{name: "invalid limit", ctx: context.Background(), limit: MaxWorkerSessionListLimit + 1},
		{name: "canceled context", ctx: canceledWorkerSessionListContext(t), limit: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := store.ListWorkerSessions(test.ctx, test.before, test.id, test.limit); err == nil {
				t.Fatal("invalid page request succeeded")
			}
		})
	}
}
