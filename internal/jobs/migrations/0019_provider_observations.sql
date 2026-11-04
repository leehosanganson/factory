CREATE TABLE factory_job_provider_observations (
    job_id TEXT PRIMARY KEY NOT NULL REFERENCES factory_jobs(id),
    observation_json TEXT NOT NULL
);
