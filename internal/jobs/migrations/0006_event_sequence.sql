ALTER TABLE factory_jobs ADD COLUMN event_sequence INTEGER NOT NULL DEFAULT 0 CHECK (event_sequence >= 0);

UPDATE factory_jobs
SET event_sequence = COALESCE((SELECT MAX(sequence) FROM factory_job_events WHERE job_id = factory_jobs.id), 0);
