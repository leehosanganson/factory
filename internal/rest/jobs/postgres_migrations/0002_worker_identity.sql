ALTER TABLE factory_rest_worker_ownership
    ADD COLUMN worker_id text NOT NULL DEFAULT 'worker-local',
    ADD COLUMN boot_id text NOT NULL DEFAULT 'boot-local';

CREATE TABLE factory_job_progress (
    job_id text PRIMARY KEY NOT NULL REFERENCES factory_jobs(id),
    stage text NOT NULL,
    state text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE factory_rest_attempt_history (
    job_id text NOT NULL REFERENCES factory_jobs(id),
    generation bigint NOT NULL CHECK (generation > 0),
    worker_id text NOT NULL CHECK (length(worker_id) BETWEEN 1 AND 128),
    boot_id text NOT NULL CHECK (length(boot_id) BETWEEN 1 AND 128),
    claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    status text CHECK (status IN ('succeeded','failed','canceled')),
    PRIMARY KEY (job_id, generation),
    CHECK ((finished_at IS NULL AND status IS NULL) OR (finished_at IS NOT NULL AND status IS NOT NULL))
);

CREATE INDEX factory_rest_attempt_history_job_claimed ON factory_rest_attempt_history(job_id, generation DESC);
