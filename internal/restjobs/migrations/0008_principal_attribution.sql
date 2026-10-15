ALTER TABLE factory_jobs ADD COLUMN submitter TEXT NOT NULL DEFAULT 'legacy-shared';

CREATE TABLE factory_job_idempotency_principal (
    principal_id TEXT NOT NULL,
    key TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES factory_jobs(id),
    request_json TEXT NOT NULL,
    PRIMARY KEY (principal_id, key)
);

INSERT INTO factory_job_idempotency_principal(principal_id, key, job_id, request_json)
SELECT j.submitter, i.key, i.job_id, i.request_json
FROM factory_job_idempotency i
JOIN factory_jobs j ON j.id=i.job_id;

DROP TABLE factory_job_idempotency;
ALTER TABLE factory_job_idempotency_principal RENAME TO factory_job_idempotency;
