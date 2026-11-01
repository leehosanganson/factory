CREATE TABLE factory_threads (
    id text PRIMARY KEY NOT NULL,
    submitter text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    initial_job_id text NOT NULL UNIQUE REFERENCES factory_jobs(id) ON DELETE CASCADE
);

CREATE TABLE factory_thread_jobs (
    thread_id text NOT NULL REFERENCES factory_threads(id) ON DELETE CASCADE,
    job_id text NOT NULL UNIQUE REFERENCES factory_jobs(id) ON DELETE CASCADE,
    position bigint NOT NULL CHECK (position > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (thread_id, position)
);

INSERT INTO factory_thread_jobs(thread_id, job_id, position, created_at)
SELECT id, initial_job_id, 1, created_at FROM factory_threads;

CREATE TABLE factory_thread_idempotency (
    principal_id text NOT NULL,
    key text NOT NULL,
    thread_id text NOT NULL REFERENCES factory_threads(id) ON DELETE CASCADE,
    request_json text NOT NULL,
    PRIMARY KEY (principal_id, key)
);
CREATE INDEX factory_thread_idempotency_thread_idx ON factory_thread_idempotency(thread_id);
