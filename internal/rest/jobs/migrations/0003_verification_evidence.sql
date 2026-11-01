CREATE TABLE factory_job_verification_evidence (
    job_id TEXT PRIMARY KEY NOT NULL REFERENCES factory_jobs(id),
    evidence_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
