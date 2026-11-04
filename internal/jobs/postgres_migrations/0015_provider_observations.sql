CREATE TABLE factory_job_provider_observations (
    job_id text PRIMARY KEY NOT NULL REFERENCES factory_jobs(id),
    observation_json text NOT NULL
);
