package restjobs

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteDispatchDueScheduleOccurrencesCapsBatchAndDoesNotDuplicate(t *testing.T) {
	const scheduleCount = 40
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "schedule-due-dispatch.db"), Config{
		QueueCapacity: 64, MaxConcurrentJobs: 1, MaxRecords: 64, MaxEventsPerJob: 8, MaxTaskBytes: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()

	fixedOccurrence := time.Date(2024, time.January, 2, 3, 4, 0, 0, time.UTC)
	for i := range scheduleCount {
		schedule, _, err := store.CreateSchedule("alice", fmt.Sprintf("due-%02d", i), ScheduleInput{
			Cron: "* * * * *", Repository: "widget", Task: fmt.Sprintf("scheduled task %02d", i),
		})
		if err != nil {
			t.Fatalf("create schedule %d: %v", i, err)
		}
		if _, err := store.db.Exec(`UPDATE factory_schedules SET created_at=?,next_occurrence_at=? WHERE id=?`,
			fixedOccurrence.Format(time.RFC3339Nano), fixedOccurrence.Format(time.RFC3339Nano), schedule.ID); err != nil {
			t.Fatalf("freeze schedule %d: %v", i, err)
		}
	}

	assertState := func(want int) (map[string]struct{}, map[string]string) {
		t.Helper()
		jobsPage, err := store.ListJobs(context.Background(), 0, 0, MaxJobListLimit)
		if err != nil {
			t.Fatalf("list Jobs: %v", err)
		}
		if jobsPage.HasMore || len(jobsPage.Jobs) != want {
			t.Fatalf("Jobs count=%d has_more=%v, want %d", len(jobsPage.Jobs), jobsPage.HasMore, want)
		}
		jobs := make(map[string]struct{}, want)
		for _, job := range jobsPage.Jobs {
			if job.Status != StatusQueued {
				t.Errorf("Job %s status=%s, want queued without a worker", job.ID, job.Status)
			}
			if _, duplicate := jobs[job.ID]; duplicate {
				t.Errorf("duplicate Job %s in listing", job.ID)
			}
			jobs[job.ID] = struct{}{}
		}

		schedules, err := store.ListAllSchedules()
		if err != nil {
			t.Fatalf("list schedules: %v", err)
		}
		if len(schedules) != scheduleCount {
			t.Fatalf("schedule count=%d, want %d", len(schedules), scheduleCount)
		}
		runsBySchedule := make(map[string]string, want)
		jobsFromRuns := make(map[string]struct{}, want)
		for _, schedule := range schedules {
			page, err := store.ListScheduleRuns(schedule.ID, "alice", false, 0, MaxScheduleHistoryLimit)
			if err != nil {
				t.Fatalf("list runs for schedule %s: %v", schedule.ID, err)
			}
			if len(page.Runs) > 1 {
				t.Errorf("schedule %s has %d runs, want at most one", schedule.ID, len(page.Runs))
			}
			for _, run := range page.Runs {
				if !run.OccurrenceAt.Equal(fixedOccurrence) {
					t.Errorf("schedule %s occurrence=%s, want %s", schedule.ID, run.OccurrenceAt, fixedOccurrence)
				}
				if _, duplicate := jobsFromRuns[run.JobID]; duplicate {
					t.Errorf("Job %s is linked to multiple schedule runs", run.JobID)
				}
				jobsFromRuns[run.JobID] = struct{}{}
				runsBySchedule[schedule.ID] = run.JobID
				if _, exists := jobs[run.JobID]; !exists {
					t.Errorf("schedule %s run references unlisted Job %s", schedule.ID, run.JobID)
				}
			}
		}
		if len(jobsFromRuns) != want {
			t.Errorf("schedule run count=%d, want %d", len(jobsFromRuns), want)
		}
		if len(jobs) != len(jobsFromRuns) {
			t.Errorf("Jobs=%d, jobs referenced by runs=%d; want one-to-one mapping", len(jobs), len(jobsFromRuns))
		}
		return jobs, runsBySchedule
	}

	if err := store.DispatchDueScheduleOccurrences(context.Background(), fixedOccurrence, 100); err != nil {
		t.Fatalf("first due dispatch: %v", err)
	}
	assertState(32)

	if err := store.DispatchDueScheduleOccurrences(context.Background(), fixedOccurrence, 32); err != nil {
		t.Fatalf("second due dispatch: %v", err)
	}
	jobsAfterSecond, runsAfterSecond := assertState(scheduleCount)

	if err := store.DispatchDueScheduleOccurrences(context.Background(), fixedOccurrence, 32); err != nil {
		t.Fatalf("third due dispatch: %v", err)
	}
	jobsAfterThird, runsAfterThird := assertState(scheduleCount)
	if len(jobsAfterThird) != len(jobsAfterSecond) || len(runsAfterThird) != len(runsAfterSecond) {
		t.Fatalf("third dispatch changed counts: Jobs %d->%d, schedules with runs %d->%d", len(jobsAfterSecond), len(jobsAfterThird), len(runsAfterSecond), len(runsAfterThird))
	}
	for jobID := range jobsAfterSecond {
		if _, exists := jobsAfterThird[jobID]; !exists {
			t.Errorf("third dispatch replaced or removed Job %s", jobID)
		}
	}
	for scheduleID, jobID := range runsAfterSecond {
		if got := runsAfterThird[scheduleID]; got != jobID {
			t.Errorf("third dispatch changed schedule %s run Job from %s to %s", scheduleID, jobID, got)
		}
	}
}
