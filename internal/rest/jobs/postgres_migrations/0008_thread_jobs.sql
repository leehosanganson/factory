CREATE TABLE factory_thread_job_idempotency (
    principal_id text NOT NULL,
    thread_id text NOT NULL REFERENCES factory_threads(id) ON DELETE CASCADE,
    key text NOT NULL,
    job_id text NOT NULL REFERENCES factory_jobs(id) ON DELETE CASCADE,
    request_json text NOT NULL,
    PRIMARY KEY (principal_id, thread_id, key)
);
