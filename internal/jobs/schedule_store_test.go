package restjobs

import (
	"errors"
	"path/filepath"
	"testing"
)

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
