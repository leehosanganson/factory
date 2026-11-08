package restjobs

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteScheduleOccurrenceAdmissionRetainsPendingAndSkipsLaterOccurrences(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "schedule-occurrences.db")
	config := Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128}
	store, err := OpenSQLiteServerStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			_ = store.CloseStore()
		}
	}()
	schedule, _, err := store.CreateSchedule("alice", "create-key", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "scheduled task"})
	if err != nil {
		t.Fatal(err)
	}
	first := schedule.NextOccurrence()
	if first.IsZero() {
		t.Fatal("created schedule has no next occurrence")
	}
	for _, key := range []string{"fill-one", "fill-two"} {
		if _, _, err := store.Admit(key, Request{Repository: "widget", Task: key}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DispatchScheduleOccurrences(context.Background(), first, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.DispatchScheduleOccurrences(context.Background(), first.Add(2*time.Minute), 1); err != nil {
		t.Fatal(err)
	}
	pending, err := store.GetSchedule(schedule.ID)
	if err != nil || pending.PendingOccurrence() == nil || !pending.PendingOccurrence().Equal(first) {
		t.Fatalf("pending occurrence=(%+v,%v), want %s", pending.PendingOccurrence(), err, first)
	}
	if pending.SkippedOccurrences() == nil || pending.SkippedOccurrences().Revision != schedule.Revision || pending.SkippedOccurrences().Count != 2 {
		t.Fatalf("skipped summary=%+v, want two later occurrences", pending.SkippedOccurrences())
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenSQLiteServerStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	pending, err = store.GetSchedule(schedule.ID)
	if err != nil || pending.PendingOccurrence() == nil || !pending.PendingOccurrence().Equal(first) {
		t.Fatalf("reopened pending occurrence=(%+v,%v)", pending.PendingOccurrence(), err)
	}
	for _, key := range []string{"refill-one", "refill-two"} {
		if _, _, err := store.Admit(key, Request{Repository: "widget", Task: key}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DispatchScheduleOccurrences(context.Background(), first.Add(3*time.Minute), 1); err != nil {
		t.Fatal(err)
	}
	pending, err = store.GetSchedule(schedule.ID)
	if err != nil || pending.SkippedOccurrences() == nil || pending.SkippedOccurrences().Revision != schedule.Revision || pending.SkippedOccurrences().Count != 3 {
		t.Fatalf("continued pending skip summary=%+v err=%v", pending.SkippedOccurrences(), err)
	}
	for i := 0; i < 4; i++ {
		claim, err := store.ClaimNextAttempt()
		if errors.Is(err, ErrNoQueuedJobs) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := store.FinishAttempt(claim.Attempt, StatusSucceeded); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := store.Admit("free-capacity", Request{Repository: "widget", Task: "free capacity"}); err != nil {
		t.Fatal(err)
	}
	if err := store.DispatchScheduleOccurrences(context.Background(), first.Add(2*time.Minute), 1); err != nil {
		t.Fatal(err)
	}
	page, err := store.ListJobs(context.Background(), 0, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	var scheduledJobs int
	for i := range page.Jobs {
		job, getErr := store.Get(page.Jobs[i].ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if job.Request.Task == "scheduled task" {
			scheduledJobs++
			if job.Submitter != "alice" || job.Status != StatusQueued || job.Request.Repository != "widget" {
				t.Fatalf("scheduled job=%+v, want normal standalone queued Job owned by alice", job)
			}
		}
	}
	if scheduledJobs != 1 {
		t.Fatalf("scheduled Jobs=%d, want exactly one", scheduledJobs)
	}
	if err := store.DispatchScheduleOccurrences(context.Background(), first.Add(2*time.Minute), 1); err != nil {
		t.Fatal(err)
	}
	page, err = store.ListJobs(context.Background(), 0, 0, 10)
	if err != nil || len(page.Jobs) != 6 {
		t.Fatalf("jobs after retry=(%d,%v), want six records including refills and one scheduled Job", len(page.Jobs), err)
	}
	updated, err := store.GetSchedule(schedule.ID)
	if err != nil || updated.PendingOccurrence() != nil {
		t.Fatalf("pending state after successful retry=(%+v,%v)", updated.PendingOccurrence(), err)
	}
}

func TestSQLiteScheduleRunsIgnorePublicKeysInReservedNamespace(t *testing.T) {
	store, err := OpenSQLiteServerStore(filepath.Join(privateSQLiteDir(t), "schedule-runs-reserved-key.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	schedule, _, err := store.CreateSchedule("alice", "create", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "scheduled work"})
	if err != nil {
		t.Fatal(err)
	}
	occurrence := schedule.NextOccurrence()
	if _, _, err := store.AdmitAs(scheduleOccurrenceKey(schedule.ID, schedule.Revision, occurrence), "alice", Request{Repository: "widget", Task: "spoofed standalone work"}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("standalone reserved-key admission error=%v, want ErrInvalidInput", err)
	}
	// Simulate a historical or forged row inserted before namespace reservation
	// was enforced. A matching key must not establish schedule provenance.
	forgedJob, _, err := store.Admit("forged-unreserved-key", Request{Repository: "widget", Task: "forged scheduled work"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE factory_job_idempotency SET key=? WHERE principal_id=? AND job_id=?`, scheduleOccurrenceKey(schedule.ID, schedule.Revision, occurrence.Add(-time.Minute)), "alice", forgedJob.ID); err != nil {
		t.Fatal(err)
	}
	page, err := store.ListScheduleRuns(schedule.ID, "alice", false, 0, 10)
	if err != nil || len(page.Runs) != 0 {
		t.Fatalf("runs before dispatcher admission=(%+v,%v), want none despite matching key", page, err)
	}
	if err := store.DispatchScheduleOccurrences(context.Background(), occurrence, 1); err != nil {
		t.Fatalf("dispatch scheduled occurrence after rejected spoof: %v", err)
	}
	page, err = store.ListScheduleRuns(schedule.ID, "alice", false, 0, 10)
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("schedule runs=(%+v,%v), want one dispatcher-admitted run", page, err)
	}
	job, err := store.Get(page.Runs[0].JobID)
	if err != nil || job.Request.Task != "scheduled work" || job.ID == forgedJob.ID {
		t.Fatalf("dispatcher run Job=(%+v,%v), want only dispatcher-admitted request", job, err)
	}
	if _, err := store.db.Exec(`DELETE FROM factory_schedules WHERE id=?`, schedule.ID); err == nil {
		t.Fatal("schedule deletion succeeded while run provenance references it; want default restrict")
	}
	if _, err := store.db.Exec(`DELETE FROM factory_job_events WHERE job_id=?`, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`DELETE FROM factory_job_idempotency WHERE job_id=?`, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`DELETE FROM factory_jobs WHERE id=?`, job.ID); err != nil {
		t.Fatal(err)
	}
	page, err = store.ListScheduleRuns(schedule.ID, "alice", false, 0, 10)
	if err != nil || len(page.Runs) != 0 {
		t.Fatalf("runs after canonical Job deletion=(%+v,%v), want empty after provenance cascade", page, err)
	}
}

func TestSQLiteScheduleDispatchLimitProcessesDifferentDueSchedulesOnce(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "schedule-batch.db"), Config{QueueCapacity: 4, MaxConcurrentJobs: 2, MaxRecords: 16, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	var first time.Time
	for i := range 3 {
		schedule, _, err := store.CreateSchedule("alice", string(rune('a'+i)), ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: string(rune('a' + i))})
		if err != nil {
			t.Fatal(err)
		}
		if first.IsZero() || schedule.NextOccurrence().Before(first) {
			first = schedule.NextOccurrence()
		}
	}
	if err := store.DispatchScheduleOccurrences(context.Background(), first, 3); err != nil {
		t.Fatal(err)
	}
	page, err := store.ListJobs(context.Background(), 0, 0, 10)
	if err != nil || len(page.Jobs) != 3 {
		t.Fatalf("jobs=(%d,%v), want three distinct schedule Jobs", len(page.Jobs), err)
	}
}

func TestCountScheduleOccurrencesHandlesLongDowntimeWithoutMinuteByMinuteWalk(t *testing.T) {
	first, last, count, err := countScheduleOccurrences("* * * * *", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if count != 13675680 || first.IsZero() || !last.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("long-gap occurrence range=(%s,%s,%d)", first, last, count)
	}
}

func TestSQLiteScheduleDispatchFirstAllowsMutationWithCurrentUserRevision(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "schedule-dispatch-first.db"), Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	schedule, _, err := store.CreateSchedule("alice", "dispatch-first", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "old task"})
	if err != nil {
		t.Fatal(err)
	}
	oldOccurrence := schedule.NextOccurrence()
	if err := store.DispatchScheduleOccurrences(context.Background(), oldOccurrence, 1); err != nil {
		t.Fatal(err)
	}
	edited, err := store.MutateSchedule(schedule.ID, "alice", schedule.Revision, ScheduleEdit, &ScheduleInput{Cron: "0 9 * * *", Repository: "widget", Task: "new task"})
	if err != nil {
		t.Fatalf("mutation after dispatch with unchanged user revision: %v", err)
	}
	if edited.Revision != schedule.Revision+1 || edited.Task != "new task" {
		t.Fatalf("edited schedule=%+v; want one user revision increment", edited)
	}
	page, err := store.ListJobs(context.Background(), 0, 0, 10)
	if err != nil || len(page.Jobs) != 1 {
		t.Fatalf("committed old occurrence jobs=(%+v,%v); want one immutable old-revision Job", page.Jobs, err)
	}
	job, err := store.Get(page.Jobs[0].ID)
	if err != nil || job.Request.Task != "old task" {
		t.Fatalf("committed occurrence=(%+v,%v); want old task", job, err)
	}
}

func TestSQLiteScheduleRegistryFullRetainsPendingOccurrence(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "schedule-registry-full.db"), Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 4, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	for i := range 4 {
		job, _, err := store.Admit("fill-"+string(rune('a'+i)), Request{Repository: "widget", Task: "fills retained registry"})
		if err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimNextAttempt()
		if err != nil || claim.Job.ID != job.ID {
			t.Fatalf("claim=%+v err=%v", claim, err)
		}
		if err := store.FinishAttempt(claim.Attempt, StatusSucceeded); err != nil {
			t.Fatal(err)
		}
	}
	schedule, _, err := store.CreateSchedule("alice", "registry-full", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "scheduled task"})
	if err != nil {
		t.Fatal(err)
	}
	occurrence := schedule.NextOccurrence()
	if err := store.DispatchScheduleOccurrences(context.Background(), occurrence, 1); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetSchedule(schedule.ID)
	if err != nil || got.PendingOccurrence() == nil || !got.PendingOccurrence().Equal(occurrence) {
		t.Fatalf("pending occurrence=(%+v,%v), want retained occurrence %s", got.PendingOccurrence(), err, occurrence)
	}
	page, err := store.ListJobs(context.Background(), 0, 0, 10)
	if err != nil || len(page.Jobs) != 4 {
		t.Fatalf("jobs=(%+v,%v), want only four retained capacity-filling Jobs", page.Jobs, err)
	}
}

func TestSQLiteScheduleMutationFirstExcludesOldOccurrence(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "schedule-revision-race.db"), Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	schedule, _, err := store.CreateSchedule("alice", "revision-race", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "old task"})
	if err != nil {
		t.Fatal(err)
	}
	oldOccurrence := schedule.NextOccurrence()
	edited, err := store.MutateSchedule(schedule.ID, "alice", schedule.Revision, ScheduleEdit, &ScheduleInput{Cron: "0 9 * * *", Repository: "widget", Task: "new task"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DispatchScheduleOccurrences(context.Background(), oldOccurrence.Add(time.Minute), 1); err != nil {
		t.Fatal(err)
	}
	page, err := store.ListJobs(context.Background(), 0, 0, 10)
	if err != nil || len(page.Jobs) != 0 {
		t.Fatalf("old-revision Jobs=%d err=%v, want none", len(page.Jobs), err)
	}
	current, err := store.GetSchedule(schedule.ID)
	if err != nil || current.Revision != edited.Revision || !current.NextOccurrence().Equal(edited.NextOccurrence()) {
		t.Fatalf("current revision=(%+v,%v)", current, err)
	}
}

func TestSQLiteScheduleCreatesNextOccurrenceStrictlyAfterCreateTime(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "schedule-next.db"), Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 4, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	created, _, err := store.CreateSchedule("alice", "create-next", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if !created.NextOccurrence().After(created.CreatedAt) || !created.NextOccurrence().Equal(created.CreatedAt.UTC().Truncate(time.Minute).Add(time.Minute)) {
		t.Fatalf("first occurrence=%s, created at=%s", created.NextOccurrence(), created.CreatedAt)
	}
}

func TestSQLiteScheduleHistoryOperationalOwnerLookupErrorIsNotNotFound(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "schedule-history-owner-error.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	schedule, _, err := store.CreateSchedule("alice", "history-owner-error", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`ALTER TABLE factory_schedules RENAME TO factory_schedules_original`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`CREATE VIEW factory_schedules AS SELECT owner FROM missing_schedule_table`); err != nil {
		t.Fatal(err)
	}

	_, err = store.ListScheduleHistory(schedule.ID, "admin", true, 0, MaxScheduleHistoryLimit)
	if err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("history error=%v; want operational error distinct from ErrNotFound", err)
	}
}

func TestSQLiteScheduleAuditIsAtomicAndAppendOnly(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "schedule-audit-store.db")
	store, err := OpenSQLiteStore(path, Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	created, replayed, err := store.CreateSchedule("alice", "audit-key", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "task"})
	if err != nil || replayed {
		t.Fatalf("create=(%+v,%t,%v)", created, replayed, err)
	}
	if _, replayed, err := store.CreateSchedule("alice", "audit-key", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "task"}); err != nil || !replayed {
		t.Fatalf("replay=(%t,%v)", replayed, err)
	}
	if _, err := store.MutateSchedule(created.ID, "alice", 1, SchedulePause, nil); err != nil {
		t.Fatal(err)
	}
	page, err := store.ListScheduleHistory(created.ID, "alice", false, 0, MaxScheduleHistoryLimit)
	if err != nil || len(page.Events) != 2 || page.Events[0].Action != ScheduleCreated || page.Events[1].Action != SchedulePauseAction || page.HasMore {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	if _, err := store.db.Exec(`UPDATE factory_schedule_audit SET actor='changed' WHERE schedule_id=?`, created.ID); err == nil {
		t.Fatal("audit update unexpectedly succeeded")
	}
	if _, err := store.db.Exec(`DELETE FROM factory_schedule_audit WHERE schedule_id=?`, created.ID); err == nil {
		t.Fatal("audit delete unexpectedly succeeded")
	}
}

func TestSQLiteScheduleDefinitionsPersistRevisionAndTombstone(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "schedules.db")
	config := Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128}
	store, err := OpenSQLiteStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	input := ScheduleInput{Cron: "0 9 * * 1-5", Repository: "widget", Task: "run task"}
	created, replayed, err := store.CreateSchedule("alice", "create-key", input)
	if err != nil || replayed {
		t.Fatalf("create=(%+v,%t,%v)", created, replayed, err)
	}
	if repeat, replayed, err := store.CreateSchedule("alice", "create-key", input); err != nil || !replayed || repeat.ID != created.ID {
		t.Fatalf("idempotent create=(%+v,%t,%v)", repeat, replayed, err)
	}
	if _, _, err := store.CreateSchedule("bob", "create-key", input); err != nil {
		t.Fatalf("principal-scoped key conflict: %v", err)
	}
	if _, _, err := store.CreateSchedule("alice", "create-key", ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: "different"}); !errors.Is(err, ErrScheduleIDConflict) {
		t.Fatalf("key reuse error=%v, want conflict", err)
	}
	paused, err := store.MutateSchedule(created.ID, "alice", 1, SchedulePause, nil)
	if err != nil || paused.Status != SchedulePaused || paused.Revision != 2 {
		t.Fatalf("pause=(%+v,%v)", paused, err)
	}
	if _, err := store.MutateSchedule(created.ID, "alice", 1, ScheduleResume, nil); !errors.Is(err, ErrScheduleConflict) {
		t.Fatalf("stale update error=%v", err)
	}
	if _, err := store.MutateSchedule("missing", "alice", 2, ScheduleResume, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown mutation error=%v", err)
	}
	if _, err := store.MutateSchedule(created.ID, "bob", 2, ScheduleResume, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign mutation error=%v", err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenSQLiteStore(path, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	persisted, err := store.GetSchedule(created.ID)
	if err != nil || persisted.Status != SchedulePaused || persisted.Revision != 2 {
		t.Fatalf("reopened schedule=(%+v,%v)", persisted, err)
	}
	resumed, err := store.MutateSchedule(created.ID, "alice", 2, ScheduleResume, nil)
	if err != nil || resumed.Status != ScheduleActive || resumed.Revision != 3 {
		t.Fatalf("resume=(%+v,%v)", resumed, err)
	}
	removed, err := store.MutateSchedule(created.ID, "alice", 3, ScheduleRemove, nil)
	if err != nil || removed.Status != ScheduleRemoved || removed.Revision != 4 {
		t.Fatalf("remove=(%+v,%v)", removed, err)
	}
	if _, err := store.GetSchedule(created.ID); err != nil {
		t.Fatalf("removed tombstone was not retained: %v", err)
	}
	if _, err := store.MutateSchedule(created.ID, "alice", 4, ScheduleResume, nil); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("removed schedule mutation error=%v", err)
	}
	list, err := store.ListSchedules("alice")
	if err != nil || len(list) != 1 || list[0].Status != ScheduleRemoved {
		t.Fatalf("schedule listing=(%+v,%v)", list, err)
	}
}
