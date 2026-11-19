package restjobs

import (
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestSQLiteListAllSchedulesIncludesAllOwnersAndStatusesInStableOrder(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "all-schedules.db"), Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()

	empty, err := store.ListAllSchedules()
	if err != nil {
		t.Fatal(err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("empty listing=%#v, want a nonnil empty slice", empty)
	}

	base := time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC)
	seeds := []struct {
		owner, key, task string
		createdAt        time.Time
		status           ScheduleStatus
	}{
		{"alice", "alice-paused", "alice paused task", base, SchedulePaused},
		{"bob", "bob-active-tied", "bob active task", base.Add(time.Hour), ScheduleActive},
		{"alice", "alice-removed-tied", "alice removed task", base.Add(time.Hour), ScheduleRemoved},
		{"bob", "bob-active-newest", "bob newest task", base.Add(2 * time.Hour), ScheduleActive},
	}

	expected := make([]Schedule, 0, len(seeds))
	for _, seed := range seeds {
		created, replayed, err := store.CreateSchedule(seed.owner, seed.key, ScheduleInput{Cron: "* * * * *", Repository: "widget", Task: seed.task})
		if err != nil || replayed {
			t.Fatalf("create schedule for %s=(%+v,%t,%v)", seed.owner, created, replayed, err)
		}
		schedule := created
		switch seed.status {
		case SchedulePaused:
			schedule, err = store.MutateSchedule(created.ID, seed.owner, created.Revision, SchedulePause, nil)
		case ScheduleRemoved:
			schedule, err = store.MutateSchedule(created.ID, seed.owner, created.Revision, ScheduleRemove, nil)
		}
		if err != nil {
			t.Fatalf("set %s schedule status: %v", seed.status, err)
		}
		stamp := seed.createdAt.Format(time.RFC3339Nano)
		if _, err := store.db.Exec(`UPDATE factory_schedules SET created_at=? WHERE id=?`, stamp, created.ID); err != nil {
			t.Fatalf("set created_at for %s: %v", created.ID, err)
		}
		schedule.CreatedAt = seed.createdAt
		expected = append(expected, schedule)
	}
	sort.Slice(expected, func(i, j int) bool {
		if expected[i].CreatedAt.Equal(expected[j].CreatedAt) {
			return expected[i].ID < expected[j].ID
		}
		return expected[i].CreatedAt.Before(expected[j].CreatedAt)
	})

	got, err := store.ListAllSchedules()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(expected) {
		t.Fatalf("listed %d schedules, want %d: %+v", len(got), len(expected), got)
	}
	for i := range expected {
		want, actual := expected[i], got[i]
		if actual.ID != want.ID || actual.Owner != want.Owner || actual.Cron != want.Cron || actual.Repository != want.Repository || actual.Task != want.Task || actual.Status != want.Status || actual.Revision != want.Revision || !actual.CreatedAt.Equal(want.CreatedAt) || !actual.UpdatedAt.Equal(want.UpdatedAt) {
			t.Errorf("schedule[%d]=%+v, want ID=%q owner=%q cron=%q repository=%q task=%q status=%q revision=%d created_at=%s updated_at=%s", i, actual, want.ID, want.Owner, want.Cron, want.Repository, want.Task, want.Status, want.Revision, want.CreatedAt, want.UpdatedAt)
		}
	}

	repeated, err := store.ListAllSchedules()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, repeated) {
		t.Fatalf("repeated listing changed: first=%+v repeated=%+v", got, repeated)
	}

	for _, owner := range []string{"alice", "bob"} {
		ownerSchedules, err := store.ListSchedules(owner)
		if err != nil {
			t.Fatalf("list schedules for %s: %v", owner, err)
		}
		var wantIDs []string
		for _, schedule := range expected {
			if schedule.Owner == owner {
				wantIDs = append(wantIDs, schedule.ID)
			}
		}
		var gotIDs []string
		for _, schedule := range ownerSchedules {
			if schedule.Owner != owner {
				t.Errorf("ListSchedules(%q) returned foreign schedule %+v", owner, schedule)
			}
			gotIDs = append(gotIDs, schedule.ID)
		}
		if !reflect.DeepEqual(gotIDs, wantIDs) {
			t.Errorf("ListSchedules(%q) IDs=%v, want owner-only IDs %v", owner, gotIDs, wantIDs)
		}
	}
}
