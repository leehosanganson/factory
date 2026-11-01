CREATE TABLE factory_thread_job_idempotency (
    principal_id TEXT NOT NULL,
    thread_id TEXT NOT NULL REFERENCES factory_threads(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES factory_jobs(id) ON DELETE CASCADE,
    request_json TEXT NOT NULL,
    PRIMARY KEY (principal_id, thread_id, key)
);
