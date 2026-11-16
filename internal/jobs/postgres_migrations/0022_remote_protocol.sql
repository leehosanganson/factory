CREATE TABLE factory_rest_remote_claims (
    claim_id text PRIMARY KEY NOT NULL CHECK (length(claim_id) BETWEEN 1 AND 128),
    job_id text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    worker_id text NOT NULL CHECK (length(worker_id) BETWEEN 1 AND 128),
    boot_id text NOT NULL CHECK (length(boot_id) BETWEEN 1 AND 128),
    owner_token text NOT NULL CHECK (length(owner_token) = 64),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (job_id, generation),
    FOREIGN KEY (job_id, generation) REFERENCES factory_rest_attempt_history(job_id, generation)
);

CREATE TABLE factory_rest_remote_results (
    job_id text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    worker_id text NOT NULL CHECK (length(worker_id) BETWEEN 1 AND 128),
    boot_id text NOT NULL CHECK (length(boot_id) BETWEEN 1 AND 128),
    owner_token text NOT NULL CHECK (length(owner_token) = 64),
    status text NOT NULL CHECK (status IN ('succeeded','failed','canceled')),
    evidence_json text NOT NULL,
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (job_id, generation),
    FOREIGN KEY (job_id, generation) REFERENCES factory_rest_attempt_history(job_id, generation)
);
