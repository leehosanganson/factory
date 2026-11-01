package restjobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func finishThreadJobs(t *testing.T, manager Store, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if _, err := manager.ClaimNext(); err != nil {
			t.Fatal(err)
		}
		if err := manager.Finish(id, StatusSucceeded); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMemoryThreadNotificationsTrackDoneCyclesAndOwnerInbox(t *testing.T) {
	manager := newTestManagerForNotifications(t)
	created, _, err := manager.AdmitThreadAs("thread", "alice", Request{Repository: "widget", Task: "private task text"})
	if err != nil {
		t.Fatal(err)
	}
	if page, err := manager.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10); err != nil || len(page.Notifications) != 0 {
		t.Fatalf("queued inbox=(%+v,%v), want empty", page, err)
	}
	finishThreadJobs(t, manager, created.Jobs[0].ID)
	first, err := manager.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10)
	if err != nil || len(first.Notifications) != 1 || first.Notifications[0].ThreadID != created.Thread.ID || first.Notifications[0].Status != ThreadStatusDone {
		t.Fatalf("first Done inbox=(%+v,%v)", first, err)
	}
	for range 3 {
		if _, err := manager.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10); err != nil {
			t.Fatal(err)
		}
	}
	if foreign, err := manager.ListNotificationsForPrincipal(context.Background(), "bob", 0, 10); err != nil || len(foreign.Notifications) != 0 {
		t.Fatalf("foreign inbox=(%+v,%v), want empty", foreign, err)
	}
	added, member, _, err := manager.AdmitThreadJobAs(created.Thread.ID, "follow", "alice", Request{Repository: "widget", Task: "follow up"})
	if err != nil || added.Thread.Status != ThreadStatusInProgress {
		t.Fatalf("append after Done=(%+v,%v)", added, err)
	}
	finishThreadJobs(t, manager, member.ID)
	second, err := manager.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10)
	if err != nil || len(second.Notifications) != 2 || second.Notifications[0].ID <= second.Notifications[1].ID {
		t.Fatalf("second Done cycle=(%+v,%v)", second, err)
	}
	if second.Notifications[0].ThreadID != created.Thread.ID || second.Notifications[1].ThreadID != created.Thread.ID || second.Notifications[0].CreatedAt.Equal(second.Notifications[1].CreatedAt) {
		t.Fatalf("Done cycle identities/timestamps=%+v, want separate transitions", second.Notifications)
	}
	encoded, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if secret := string(encoded); strings.Contains(secret, "follow up") || strings.Contains(secret, "private task text") {
		t.Fatalf("notification exposed request text: %s", secret)
	}
	job, err := manager.Get(member.ID)
	if err != nil || job.Status != StatusSucceeded {
		t.Fatalf("notification mutated Job: %+v, %v", job, err)
	}
}

func newTestManagerForNotifications(t *testing.T) *LocalManager {
	t.Helper()
	manager, err := NewManager(Config{QueueCapacity: 8, MaxConcurrentJobs: 2, MaxRecords: 8})
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func TestMemoryQueuedCancellationAndCloseNotifyOnlyWhenAggregateBecomesDone(t *testing.T) {
	manager := newTestManagerForNotifications(t)
	thread, _, err := manager.AdmitThreadAs("cancel-thread", "alice", Request{Repository: "widget", Task: "first"})
	if err != nil {
		t.Fatal(err)
	}
	_, member, _, err := manager.AdmitThreadJobAs(thread.Thread.ID, "member", "alice", Request{Repository: "widget", Task: "second"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Cancel(thread.Jobs[0].ID); err != nil {
		t.Fatal(err)
	}
	page, err := manager.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10)
	if err != nil || len(page.Notifications) != 0 {
		t.Fatalf("partial queued cancellation inbox=(%+v,%v), want empty", page, err)
	}
	if _, err := manager.Cancel(member.ID); err != nil {
		t.Fatal(err)
	}
	page, err = manager.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10)
	if err != nil || len(page.Notifications) != 1 {
		t.Fatalf("last queued cancellation inbox=(%+v,%v), want one", page, err)
	}
	if got, err := manager.Get(member.ID); err != nil || got.Status != StatusCanceled {
		t.Fatalf("cancel changed unrelated Job incorrectly=(%+v,%v)", got, err)
	}

	closing, _, err := manager.AdmitThreadAs("close-thread", "alice", Request{Repository: "widget", Task: "queued before close"})
	if err != nil {
		t.Fatal(err)
	}
	manager.Close()
	page, err = manager.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10)
	if err != nil || len(page.Notifications) != 2 {
		t.Fatalf("manager close inbox=(%+v,%v), want one additional Done", page, err)
	}
	if got, err := manager.Get(closing.Jobs[0].ID); err != nil || got.Status != StatusCanceled {
		t.Fatalf("close changed Job status=(%+v,%v)", got, err)
	}
}

func TestSQLiteInterruptedDispositionNotifiesOnlyFinalThreadMember(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "notifications-recovery.db")
	config := Config{QueueCapacity: 4, MaxConcurrentJobs: 2, MaxRecords: 8}
	store, err := OpenSQLiteStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	thread, _, err := store.AdmitThreadAs("thread", "alice", Request{Repository: "widget", Task: "first"})
	if err != nil {
		t.Fatal(err)
	}
	_, second, _, err := store.AdmitThreadJobAs(thread.Thread.ID, "member", "alice", Request{Repository: "widget", Task: "second"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE factory_jobs SET status='running' WHERE id IN (?,?)`, thread.Jobs[0].ID, second.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenSQLiteStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	if err := store.ResolveInterrupted(thread.Jobs[0].ID, InterruptedDispositionFailed); err != nil {
		t.Fatal(err)
	}
	page, err := store.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10)
	if err != nil || len(page.Notifications) != 0 {
		t.Fatalf("partial recovery inbox=(%+v,%v), want empty", page, err)
	}
	if err := store.ResolveInterrupted(second.ID, InterruptedDispositionCanceled); err != nil {
		t.Fatal(err)
	}
	page, err = store.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10)
	if err != nil || len(page.Notifications) != 1 {
		t.Fatalf("recovery completion inbox=(%+v,%v), want one", page, err)
	}
}

func TestSQLiteThreadNotificationsAtomicDoneCyclesAndRestart(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "notifications.db")
	config := Config{QueueCapacity: 8, MaxConcurrentJobs: 2, MaxRecords: 8}
	store, err := OpenSQLiteStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	created, _, err := store.AdmitThreadAs("thread", "alice", Request{Repository: "widget", Task: "must not be exposed"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(created.Jobs[0].ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	read, err := store.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10)
	if err != nil || len(read.Notifications) != 1 {
		t.Fatalf("terminal failure inbox=(%+v,%v)", read, err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenSQLiteStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	read, err = store.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10)
	if err != nil || len(read.Notifications) != 1 {
		t.Fatalf("restarted inbox=(%+v,%v), want one", read, err)
	}
	if err := store.Finish(created.Jobs[0].ID, StatusCanceled); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("repeat finish=%v, want invalid transition", err)
	}
	read, err = store.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10)
	if err != nil || len(read.Notifications) != 1 {
		t.Fatalf("repeated finish inbox=(%+v,%v), want one", read, err)
	}
}

func TestSQLiteThreadNotificationPaginationAndRetentionAreBounded(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "notifications-bounded.db")
	store, err := OpenSQLiteStore(path, Config{QueueCapacity: 1000, MaxConcurrentJobs: 2, MaxRecords: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	thread, _, err := store.AdmitThreadAs("thread", "alice", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxRecordsLimit; i++ {
		if i > 0 {
			_, job, _, err := store.AdmitThreadJobAs(thread.Thread.ID, fmt.Sprintf("member-%d", i), "alice", Request{Repository: "widget", Task: "follow-up"})
			if err != nil {
				t.Fatal(err)
			}
			thread.Jobs = append(thread.Jobs, job)
		}
		if _, err := store.ClaimNext(); err != nil {
			t.Fatal(err)
		}
		if err := store.Finish(thread.Jobs[i].ID, StatusSucceeded); err != nil {
			t.Fatal(err)
		}
	}
	page, err := store.ListNotificationsForPrincipal(context.Background(), "alice", 0, 1)
	if err != nil || len(page.Notifications) != 1 || !page.HasMore || page.NextCursor == 0 {
		t.Fatalf("first page=(%+v,%v)", page, err)
	}
	older, err := store.ListNotificationsForPrincipal(context.Background(), "alice", page.NextCursor, 1)
	if err != nil || len(older.Notifications) != 1 || older.Notifications[0].ID >= page.Notifications[0].ID {
		t.Fatalf("older page=(%+v,%v)", older, err)
	}
	var retained int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM factory_thread_notifications`).Scan(&retained); err != nil || retained != maxRecordsLimit {
		t.Fatalf("retained notifications=%d err=%v want cap %d", retained, err, maxRecordsLimit)
	}
}

func TestSQLiteThreadNotificationRollbackAndRetry(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "notifications-rollback.db")
	store, err := OpenSQLiteStore(path, Config{QueueCapacity: 4, MaxConcurrentJobs: 2, MaxRecords: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	created, _, err := store.AdmitThreadAs("thread", "alice", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER fail_thread_notification BEFORE INSERT ON factory_thread_notifications BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(created.Jobs[0].ID, StatusSucceeded); err == nil {
		t.Fatal("Finish succeeded with notification insertion failing")
	}
	job, err := store.Get(created.Jobs[0].ID)
	if err != nil || job.Status != StatusRunning {
		t.Fatalf("rollback status=(%+v,%v), want running", job, err)
	}
	if _, err := store.db.Exec(`DROP TRIGGER fail_thread_notification`); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(created.Jobs[0].ID, StatusSucceeded); err != nil {
		t.Fatalf("retry after rollback: %v", err)
	}
	page, err := store.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10)
	if err != nil || len(page.Notifications) != 1 {
		t.Fatalf("retry inbox=(%+v,%v), want exactly one", page, err)
	}
}

func TestSQLiteConcurrentFinalMemberCompletionsEmitOneNotification(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "notifications-concurrent.db")
	config := Config{QueueCapacity: 8, MaxConcurrentJobs: 4, MaxRecords: 8}
	first, err := OpenSQLiteStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenSQLiteStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseStore()
	defer second.CloseStore()
	created, _, err := first.AdmitThreadAs("thread", "alice", Request{Repository: "widget", Task: "first"})
	if err != nil {
		t.Fatal(err)
	}
	_, secondJob, _, err := first.AdmitThreadJobAs(created.Thread.ID, "member", "alice", Request{Repository: "widget", Task: "second"})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := first.ClaimNext(); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, entry := range []struct {
		store *SQLiteStore
		id    string
	}{{first, created.Jobs[0].ID}, {second, secondJob.ID}} {
		wg.Add(1)
		go func(s *SQLiteStore, id string) { defer wg.Done(); errs <- s.Finish(id, StatusFailed) }(entry.store, entry.id)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil && !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("finish race: %v", err)
		}
	}
	page, err := first.ListNotificationsForPrincipal(context.Background(), "alice", 0, 10)
	if err != nil || len(page.Notifications) != 1 {
		t.Fatalf("concurrent completion inbox=(%+v,%v), want one", page, err)
	}
}
