CREATE TABLE factory_schedule_runs (
    schedule_id text NOT NULL REFERENCES factory_schedules(id),
    occurrence_at timestamptz NOT NULL,
    schedule_revision bigint NOT NULL CHECK (schedule_revision > 0),
    job_id text NOT NULL UNIQUE REFERENCES factory_jobs(id) ON DELETE CASCADE,
    PRIMARY KEY (schedule_id, occurrence_at)
);
