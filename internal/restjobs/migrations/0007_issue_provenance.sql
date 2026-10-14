CREATE TABLE factory_job_issue_provenance (
    tracker TEXT NOT NULL,
    source_repository TEXT NOT NULL,
    issue_number INTEGER NOT NULL CHECK (issue_number > 0),
    version TEXT NOT NULL,
    job_id TEXT NOT NULL UNIQUE REFERENCES factory_jobs(id) ON DELETE CASCADE,
    PRIMARY KEY (tracker, source_repository, issue_number, version)
);
