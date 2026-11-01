CREATE TABLE factory_job_provider_attempts (
    job_id TEXT PRIMARY KEY NOT NULL REFERENCES factory_jobs(id),
    attempt_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
