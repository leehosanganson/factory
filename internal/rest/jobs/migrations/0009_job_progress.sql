CREATE TABLE factory_job_progress (
    job_id TEXT PRIMARY KEY NOT NULL REFERENCES factory_jobs(id),
    stage TEXT NOT NULL,
    state TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
