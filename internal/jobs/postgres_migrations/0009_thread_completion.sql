ALTER TABLE factory_threads ADD COLUMN completed_at timestamptz;
ALTER TABLE factory_threads ADD COLUMN completed_by text;
