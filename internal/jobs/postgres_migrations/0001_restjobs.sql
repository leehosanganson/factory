CREATE SEQUENCE factory_rest_job_sequence AS bigint;

CREATE TABLE factory_jobs (
    id text PRIMARY KEY NOT NULL,
    request_json text NOT NULL,
    status text NOT NULL CHECK (status IN ('queued','running','succeeded','failed','canceled')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    queue_sequence bigint NOT NULL UNIQUE,
    history_truncated boolean NOT NULL DEFAULT false,
    cancellation_requested boolean NOT NULL DEFAULT false,
    event_sequence bigint NOT NULL DEFAULT 0 CHECK (event_sequence >= 0),
    submitter text NOT NULL DEFAULT 'legacy-shared'
);
CREATE INDEX factory_jobs_status_queue_idx ON factory_jobs(status, queue_sequence);

CREATE TABLE factory_job_idempotency (
    principal_id text NOT NULL,
    key text NOT NULL,
    job_id text NOT NULL REFERENCES factory_jobs(id),
    request_json text NOT NULL,
    PRIMARY KEY (principal_id, key)
);
CREATE INDEX factory_job_idempotency_job_idx ON factory_job_idempotency(job_id);

CREATE TABLE factory_job_issue_provenance (
    tracker text NOT NULL,
    source_repository text NOT NULL,
    issue_number integer NOT NULL CHECK (issue_number > 0),
    version text NOT NULL,
    job_id text NOT NULL UNIQUE REFERENCES factory_jobs(id) ON DELETE CASCADE,
    PRIMARY KEY (tracker, source_repository, issue_number, version)
);

CREATE TABLE factory_job_events (
    job_id text NOT NULL REFERENCES factory_jobs(id),
    sequence bigint NOT NULL,
    at timestamptz NOT NULL DEFAULT clock_timestamp(),
    type text NOT NULL,
    message text NOT NULL,
    PRIMARY KEY (job_id, sequence)
);

CREATE TABLE factory_rest_worker_ownership (
    job_id text PRIMARY KEY NOT NULL REFERENCES factory_jobs(id),
    generation bigint NOT NULL CHECK (generation > 0),
    owner_token text NOT NULL CHECK (length(owner_token) = 64),
    lease_until timestamptz NOT NULL
);

CREATE TABLE factory_job_provider_attempts (
    job_id text PRIMARY KEY NOT NULL REFERENCES factory_jobs(id),
    attempt_json text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE factory_job_provider_outcomes (
    job_id text PRIMARY KEY NOT NULL REFERENCES factory_jobs(id),
    provider_json text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE factory_job_verification_evidence (
    job_id text PRIMARY KEY NOT NULL REFERENCES factory_jobs(id),
    evidence_json text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE factory_job_operator_dispositions (
    job_id text PRIMARY KEY NOT NULL REFERENCES factory_jobs(id),
    disposition text NOT NULL CHECK (disposition IN ('failed','canceled')),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
