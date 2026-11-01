CREATE TABLE factory_threads (
    id TEXT PRIMARY KEY NOT NULL,
    submitter TEXT NOT NULL,
    created_at TEXT NOT NULL,
    initial_job_id TEXT NOT NULL UNIQUE REFERENCES factory_jobs(id) ON DELETE CASCADE
);

CREATE TABLE factory_thread_jobs (
    thread_id TEXT NOT NULL REFERENCES factory_threads(id) ON DELETE CASCADE,
    job_id TEXT NOT NULL UNIQUE REFERENCES factory_jobs(id) ON DELETE CASCADE,
    position INTEGER NOT NULL CHECK (position > 0),
    created_at TEXT NOT NULL,
    PRIMARY KEY (thread_id, position)
);

INSERT INTO factory_thread_jobs(thread_id, job_id, position, created_at)
SELECT id, initial_job_id, 1, created_at FROM factory_threads;

CREATE TABLE factory_thread_idempotency (
    principal_id TEXT NOT NULL,
    key TEXT NOT NULL,
    thread_id TEXT NOT NULL REFERENCES factory_threads(id) ON DELETE CASCADE,
    request_json TEXT NOT NULL,
    PRIMARY KEY (principal_id, key)
);

CREATE INDEX factory_thread_idempotency_thread_idx ON factory_thread_idempotency(thread_id);
