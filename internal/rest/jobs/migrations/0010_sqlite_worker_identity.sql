ALTER TABLE factory_worker_ownership ADD COLUMN worker_id TEXT NOT NULL DEFAULT 'worker-local';
ALTER TABLE factory_worker_ownership ADD COLUMN boot_id TEXT NOT NULL DEFAULT 'boot-local';
