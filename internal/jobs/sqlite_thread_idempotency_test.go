package restjobs

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSQLiteThreadMemberChangedPayloadConflictIsAtomic(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "thread-member-idempotency.db"), Config{
		QueueCapacity:     4,
		MaxConcurrentJobs: 2,
		MaxRecords:        8,
		MaxEventsPerJob:   8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()

	created, replayed, err := store.AdmitThreadAs("thread-key", "alice", Request{Repository: "widget", Task: "initial task"})
	if err != nil || replayed {
		t.Fatalf("create Thread=(%+v,%t,%v), want new Thread", created, replayed, err)
	}
	created, member, replayed, err := store.AdmitThreadJobAs(created.Thread.ID, "member-key", "alice", Request{Repository: "widget", Task: "member task"})
	if err != nil || replayed {
		t.Fatalf("create Thread member=(%+v,%+v,%t,%v), want new member", created, member, replayed, err)
	}
	if len(created.Jobs) != 2 || created.Jobs[1].ID != member.ID || len(created.Thread.JobIDs) != 2 || created.Thread.JobIDs[1] != member.ID {
		t.Fatalf("created Thread membership=(%+v,%+v), want initial Job followed by member %s", created.Thread.JobIDs, created.Jobs, member.ID)
	}

	type rowCounts struct {
		threads, jobs, allJobs, memberships, threadKeys, memberKeys, events, allEvents, allThreadKeys int
	}
	readRowCounts := func() rowCounts {
		t.Helper()
		var counts rowCounts
		queries := []struct {
			dest  *int
			query string
			args  []any
		}{
			{&counts.threads, `SELECT COUNT(*) FROM factory_threads WHERE id=?`, []any{created.Thread.ID}},
			{&counts.jobs, `SELECT COUNT(*) FROM factory_jobs j JOIN factory_thread_jobs m ON m.job_id=j.id WHERE m.thread_id=?`, []any{created.Thread.ID}},
			{&counts.allJobs, `SELECT COUNT(*) FROM factory_jobs`, nil},
			{&counts.memberships, `SELECT COUNT(*) FROM factory_thread_jobs WHERE thread_id=?`, []any{created.Thread.ID}},
			{&counts.threadKeys, `SELECT COUNT(*) FROM factory_thread_idempotency WHERE thread_id=? AND principal_id=?`, []any{created.Thread.ID, "alice"}},
			{&counts.memberKeys, `SELECT COUNT(*) FROM factory_thread_job_idempotency WHERE thread_id=? AND principal_id=?`, []any{created.Thread.ID, "alice"}},
			{&counts.events, `SELECT COUNT(*) FROM factory_job_events e JOIN factory_thread_jobs m ON m.job_id=e.job_id WHERE m.thread_id=?`, []any{created.Thread.ID}},
			{&counts.allEvents, `SELECT COUNT(*) FROM factory_job_events`, nil},
			{&counts.allThreadKeys, `SELECT COUNT(*) FROM factory_thread_job_idempotency`, nil},
		}
		for _, item := range queries {
			if err := store.db.QueryRow(item.query, item.args...).Scan(item.dest); err != nil {
				t.Fatalf("read scoped row count: %v", err)
			}
		}
		return counts
	}
	type jobState struct {
		jobs      []JobSummary
		histories map[string]History
	}
	readState := func() (ThreadSnapshot, map[string]History, jobState) {
		t.Helper()
		snapshot, err := store.GetThread(created.Thread.ID)
		if err != nil {
			t.Fatalf("read Thread snapshot: %v", err)
		}
		history := make(map[string]History, len(snapshot.Jobs))
		for _, job := range snapshot.Jobs {
			history[job.ID], err = store.History(job.ID)
			if err != nil {
				t.Fatalf("read history for Job %s: %v", job.ID, err)
			}
		}
		page, err := store.ListJobs(t.Context(), 0, 0, MaxJobListLimit)
		if err != nil {
			t.Fatalf("list all Jobs: %v", err)
		}
		allHistory := make(map[string]History, len(page.Jobs))
		for _, job := range page.Jobs {
			allHistory[job.ID], err = store.History(job.ID)
			if err != nil {
				t.Fatalf("read history for listed Job %s: %v", job.ID, err)
			}
		}
		return snapshot, history, jobState{jobs: page.Jobs, histories: allHistory}
	}

	beforeSnapshot, beforeHistory, beforeAllJobs := readState()
	beforeCounts := readRowCounts()
	if beforeCounts != (rowCounts{threads: 1, jobs: 2, allJobs: 2, memberships: 2, threadKeys: 1, memberKeys: 1, events: 2, allEvents: 2, allThreadKeys: 1}) {
		t.Fatalf("initial row counts=%+v, want one Thread and both idempotency keys plus two Jobs, memberships, and events", beforeCounts)
	}

	_, _, replayed, err = store.AdmitThreadJobAs(created.Thread.ID, "member-key", "alice", Request{Repository: "widget", Task: "changed member task"})
	if replayed {
		t.Fatal("changed-payload member admission was replayed, want a conflict")
	}
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed-payload member admission error=%v, want ErrIdempotencyConflict", err)
	}

	afterSnapshot, afterHistory, afterAllJobs := readState()
	if !reflect.DeepEqual(afterSnapshot, beforeSnapshot) {
		t.Fatalf("Thread/job snapshot changed after rejected admission:\nbefore=%+v\nafter=%+v", beforeSnapshot, afterSnapshot)
	}
	if !reflect.DeepEqual(afterHistory, beforeHistory) {
		t.Fatalf("Thread job history changed after rejected admission:\nbefore=%+v\nafter=%+v", beforeHistory, afterHistory)
	}
	if !reflect.DeepEqual(afterAllJobs, beforeAllJobs) {
		t.Fatalf("global Job list or history changed after rejected admission:\nbefore=%+v\nafter=%+v", beforeAllJobs, afterAllJobs)
	}
	if afterCounts := readRowCounts(); afterCounts != beforeCounts {
		t.Fatalf("scoped row counts changed after rejected admission: before=%+v after=%+v", beforeCounts, afterCounts)
	}
}
