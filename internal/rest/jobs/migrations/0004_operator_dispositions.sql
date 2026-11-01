CREATE TABLE factory_job_operator_dispositions (
    job_id TEXT PRIMARY KEY NOT NULL REFERENCES factory_jobs(id),
    disposition TEXT NOT NULL CHECK (disposition IN ('failed','canceled')),
    updated_at TEXT NOT NULL
);
