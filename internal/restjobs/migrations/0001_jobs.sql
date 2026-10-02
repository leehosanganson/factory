CREATE TABLE factory_jobs (
    id TEXT PRIMARY KEY NOT NULL,
    request_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued','running','succeeded','failed','canceled')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    queue_sequence INTEGER NOT NULL UNIQUE,
    history_truncated INTEGER NOT NULL DEFAULT 0 CHECK (history_truncated IN (0,1))
);

CREATE INDEX factory_jobs_status_queue_idx ON factory_jobs(status, queue_sequence);

CREATE TABLE factory_job_idempotency (
    key TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL REFERENCES factory_jobs(id),
    request_json TEXT NOT NULL
);

CREATE TABLE factory_job_events (
    job_id TEXT NOT NULL REFERENCES factory_jobs(id),
    sequence INTEGER NOT NULL,
    at TEXT NOT NULL,
    type TEXT NOT NULL,
    message TEXT NOT NULL,
    PRIMARY KEY (job_id, sequence)
);

CREATE TABLE factory_job_side_effects (
    job_id TEXT NOT NULL REFERENCES factory_jobs(id),
    kind TEXT NOT NULL,
    external_id TEXT NOT NULL,
    state TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job_id, kind, external_id)
);

CREATE TABLE factory_worker_ownership (
    job_id TEXT PRIMARY KEY NOT NULL REFERENCES factory_jobs(id),
    owner_token TEXT NOT NULL,
    generation INTEGER NOT NULL,
    acquired_at TEXT NOT NULL,
    lease_until TEXT NOT NULL
);

CREATE INDEX factory_job_events_at_idx ON factory_job_events(job_id, sequence);
CREATE INDEX factory_job_side_effects_state_idx ON factory_job_side_effects(state, updated_at);
