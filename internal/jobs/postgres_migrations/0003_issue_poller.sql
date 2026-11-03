CREATE TABLE factory_issue_poller_outcomes (
    tracker text NOT NULL CHECK (tracker = 'github'),
    source_repository text NOT NULL,
    issue_number integer NOT NULL CHECK (issue_number > 0),
    version text NOT NULL CHECK (length(version) = 64),
    job_id text NOT NULL REFERENCES factory_jobs(id),
    label text NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('pending_label_removal','label_removed','version_changed','label_removal_failed')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tracker, source_repository, issue_number, version)
);
CREATE INDEX factory_issue_poller_pending_idx ON factory_issue_poller_outcomes(updated_at)
    WHERE outcome = 'pending_label_removal';
