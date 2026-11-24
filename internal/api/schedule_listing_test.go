package restapi

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
)

func TestScheduleListingScopesOwnersAndRetainsRemovedSchedules(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := restjobs.OpenSQLiteStore(filepath.Join(dir, "schedule-listing.db"), restjobs.Config{
		QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 8, MaxEventsPerJob: 8, MaxTaskBytes: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()

	handler := newPrincipalTestHandler(t, store,
		restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "bob", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true},
	)

	schedules := make([]restjobs.Schedule, 0, 3)
	for _, seed := range []struct {
		owner string
		key   string
		task  string
	}{
		{owner: "alice", key: "alice-first", task: "alice first private task"},
		{owner: "alice", key: "alice-second", task: "alice second private task"},
		{owner: "bob", key: "bob-only", task: "bob private task"},
	} {
		schedule, replayed, err := store.CreateSchedule(seed.owner, seed.key, restjobs.ScheduleInput{
			Cron: "* * * * *", Repository: "widget", Task: seed.task,
		})
		if err != nil || replayed {
			t.Fatalf("create schedule for %s: schedule=%+v replayed=%t err=%v", seed.owner, schedule, replayed, err)
		}
		schedules = append(schedules, schedule)
	}

	removed := schedules[1]
	removed, err = store.MutateSchedule(removed.ID, removed.Owner, removed.Revision, restjobs.ScheduleRemove, nil)
	if err != nil {
		t.Fatalf("remove Alice schedule: %v", err)
	}
	if removed.Status != restjobs.ScheduleRemoved {
		t.Fatalf("removed schedule status=%q, want %q", removed.Status, restjobs.ScheduleRemoved)
	}
	schedules[1] = removed

	// List order is CreatedAt then ID. Derive the expected order from persisted
	// values so equal creation timestamps remain deterministic in this test.
	sort.Slice(schedules, func(i, j int) bool {
		if schedules[i].CreatedAt.Equal(schedules[j].CreatedAt) {
			return schedules[i].ID < schedules[j].ID
		}
		return schedules[i].CreatedAt.Before(schedules[j].CreatedAt)
	})

	assertListing := func(principal string, expected []restjobs.Schedule) {
		t.Helper()
		response := principalRequest(handler, http.MethodGet, "/v1/schedules", "", namedPrincipalToken(principal))
		if response.Code != http.StatusOK {
			t.Fatalf("%s schedule list status=%d body=%s", principal, response.Code, response.Body.String())
		}
		var body struct {
			Schedules []map[string]json.RawMessage `json:"schedules"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode %s schedule list: %v; body=%s", principal, err, response.Body.String())
		}
		for i, fields := range body.Schedules {
			var task string
			if err := json.Unmarshal(fields["task"], &task); err != nil {
				t.Errorf("decode %s schedule[%d] task: %v", principal, i, err)
				continue
			}
			if principal == "admin" {
				continue
			}
			for _, schedule := range schedules {
				if schedule.Owner != principal && task == schedule.Task {
					t.Errorf("%s schedule listing leaked another owner's task sentinel %q", principal, task)
				}
			}
		}
		if len(body.Schedules) != len(expected) {
			t.Errorf("%s schedule count=%d; want %d: %+v", principal, len(body.Schedules), len(expected), body.Schedules)
		}
		allowedFields := map[string]bool{
			"id": true, "owner": true, "cron": true, "repository": true, "task": true,
			"status": true, "revision": true, "created_at": true, "updated_at": true,
			"pending_occurrence_at": true, "skipped_occurrences": true,
		}
		requiredFields := []string{"id", "owner", "cron", "repository", "task", "status", "revision", "created_at", "updated_at"}
		for i, fields := range body.Schedules {
			if i >= len(expected) {
				break
			}
			want := expected[i]
			for name := range fields {
				if !allowedFields[name] {
					t.Errorf("%s schedule[%d] contains unexpected field %q", principal, i, name)
				}
			}
			for _, name := range requiredFields {
				if _, ok := fields[name]; !ok {
					t.Errorf("%s schedule[%d] omits required field %q", principal, i, name)
				}
			}
			encoded, err := json.Marshal(fields)
			if err != nil {
				t.Errorf("encode %s schedule[%d]: %v", principal, i, err)
				continue
			}
			var got restjobs.Schedule
			if err := json.Unmarshal(encoded, &got); err != nil {
				t.Errorf("decode %s schedule[%d]: %v", principal, i, err)
				continue
			}
			if got.ID != want.ID || got.Owner != want.Owner || got.Status != want.Status || !got.CreatedAt.Equal(want.CreatedAt) {
				t.Errorf("%s schedule[%d]=%+v; want ID=%s owner=%s status=%s created_at=%s", principal, i, got, want.ID, want.Owner, want.Status, want.CreatedAt)
			}
			var task string
			if err := json.Unmarshal(fields["task"], &task); err != nil {
				t.Errorf("decode %s schedule[%d] task: %v", principal, i, err)
				continue
			}
			if principal == "admin" || want.Owner == principal {
				if task != want.Task {
					t.Errorf("%s schedule[%d] task=%q; want %q", principal, i, task, want.Task)
				}
			}
		}
	}

	assertListing("admin", schedules)
	aliceSchedules := make([]restjobs.Schedule, 0, 2)
	bobSchedules := make([]restjobs.Schedule, 0, 1)
	for _, schedule := range schedules {
		switch schedule.Owner {
		case "alice":
			aliceSchedules = append(aliceSchedules, schedule)
		case "bob":
			bobSchedules = append(bobSchedules, schedule)
		}
	}
	assertListing("alice", aliceSchedules)
	assertListing("bob", bobSchedules)
}
