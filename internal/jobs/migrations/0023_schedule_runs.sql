CREATE TABLE factory_schedule_runs (
    schedule_id TEXT NOT NULL REFERENCES factory_schedules(id),
    occurrence_at TEXT NOT NULL,
    schedule_revision INTEGER NOT NULL CHECK (schedule_revision > 0),
    job_id TEXT NOT NULL UNIQUE REFERENCES factory_jobs(id) ON DELETE CASCADE,
    PRIMARY KEY (schedule_id, occurrence_at)
);
