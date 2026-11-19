package restapi

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
)

func TestSQLiteNotificationPaginationIsOrderedAndOwnerScoped(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	databasePath := filepath.Join(dir, "notification-pagination.db")
	store, err := restjobs.OpenSQLiteStore(databasePath, restjobs.Config{
		QueueCapacity: 8, MaxConcurrentJobs: 2, MaxRecords: 16, MaxEventsPerJob: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()

	handler := newPrincipalTestHandler(t, store,
		restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "bob", Role: "submitter", Enabled: true},
	)

	// Create real Thread rows first so seeded notifications satisfy the schema's
	// foreign-key constraint. Explicit notification IDs make pagination order
	// deterministic while the owner rows alternate in ascending ID order.
	seeds := []struct {
		id    int64
		owner string
	}{
		{id: 10, owner: "alice"},
		{id: 20, owner: "bob"},
		{id: 30, owner: "alice"},
		{id: 40, owner: "bob"},
		{id: 50, owner: "alice"},
	}
	threadIDs := make(map[int64]string, len(seeds))
	for _, seed := range seeds {
		created, _, err := store.AdmitThreadAs(
			fmt.Sprintf("notification-pagination-%d", seed.id), seed.owner,
			restjobs.Request{Repository: "widget", Task: "pagination fixture"},
		)
		if err != nil {
			t.Fatalf("create %s fixture Thread for notification %d: %v", seed.owner, seed.id, err)
		}
		threadIDs[seed.id] = created.Thread.ID
	}

	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open notification fixture database: %v", err)
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin notification fixture transaction: %v", err)
	}
	defer tx.Rollback()
	for _, seed := range seeds {
		if _, err := tx.Exec(`UPDATE factory_threads SET done_cycle=1 WHERE id=?`, threadIDs[seed.id]); err != nil {
			t.Fatalf("set Done cycle for Thread %s: %v", threadIDs[seed.id], err)
		}
		if _, err := tx.Exec(`INSERT INTO factory_thread_notifications(id,thread_id,done_cycle,owner,status,created_at) VALUES(?,?,1,?,'done','2026-01-01T00:00:00Z')`, seed.id, threadIDs[seed.id], seed.owner); err != nil {
			t.Fatalf("insert notification %d: %v", seed.id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit notification fixtures: %v", err)
	}

	requestPage := func(principal, query string) restjobs.NotificationPage {
		t.Helper()
		response := principalRequest(handler, http.MethodGet, "/v1/notifications"+query, "", namedPrincipalToken(principal))
		if response.Code != http.StatusOK {
			t.Fatalf("%s notification page status=%d body=%s", principal, response.Code, response.Body.String())
		}
		var page restjobs.NotificationPage
		if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
			t.Fatalf("decode %s notification page: %v; body=%s", principal, err, response.Body.String())
		}
		return page
	}

	assertPage := func(name string, page restjobs.NotificationPage, expectedIDs []int64, expectedThreads []string, hasMore bool, nextCursor int64) {
		t.Helper()
		if page.HasMore != hasMore || page.NextCursor != nextCursor {
			t.Fatalf("%s page metadata=(has_more %t,next_cursor %d); want (%t,%d)", name, page.HasMore, page.NextCursor, hasMore, nextCursor)
		}
		if len(page.Notifications) != len(expectedIDs) {
			t.Fatalf("%s notification count=%d; want %d: %+v", name, len(page.Notifications), len(expectedIDs), page.Notifications)
		}
		for i, notification := range page.Notifications {
			if notification.ID != expectedIDs[i] || notification.ThreadID != expectedThreads[i] {
				t.Errorf("%s notification[%d]=(ID %d, ThreadID %q); want (ID %d, ThreadID %q)", name, i, notification.ID, notification.ThreadID, expectedIDs[i], expectedThreads[i])
			}
			if notification.Status != restjobs.ThreadStatusDone {
				t.Errorf("%s notification[%d] status=%q; want done", name, i, notification.Status)
			}
		}
	}

	aliceFirst := requestPage("alice", "?limit=2")
	assertPage("Alice first", aliceFirst, []int64{50, 30}, []string{threadIDs[50], threadIDs[30]}, true, 30)

	aliceSecond := requestPage("alice", "?limit=2&before=30")
	assertPage("Alice second", aliceSecond, []int64{10}, []string{threadIDs[10]}, false, 0)
	aliceIDs := make(map[int64]struct{}, len(aliceFirst.Notifications)+len(aliceSecond.Notifications))
	for _, page := range []restjobs.NotificationPage{aliceFirst, aliceSecond} {
		for _, notification := range page.Notifications {
			if _, duplicate := aliceIDs[notification.ID]; duplicate {
				t.Errorf("Alice pagination returned duplicate notification ID %d", notification.ID)
			}
			aliceIDs[notification.ID] = struct{}{}
		}
	}
	if len(aliceIDs) != 3 {
		t.Errorf("Alice pagination returned %d unique notifications; want 3", len(aliceIDs))
	}

	bobPage := requestPage("bob", "?limit=2")
	assertPage("Bob", bobPage, []int64{40, 20}, []string{threadIDs[40], threadIDs[20]}, false, 0)
}
