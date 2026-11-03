ALTER TABLE factory_worker_sessions ADD COLUMN capability_hash text;
ALTER TABLE factory_worker_sessions ADD COLUMN capability_expires_at timestamptz;
ALTER TABLE factory_worker_session_audit DROP CONSTRAINT factory_worker_session_audit_event_check;
ALTER TABLE factory_worker_session_audit ADD CONSTRAINT factory_worker_session_audit_event_check
    CHECK (event IN ('registered', 'retired', 'capability_issued', 'capability_rotated'));
ALTER TABLE factory_worker_session_audit ADD COLUMN capability_hash text;
ALTER TABLE factory_worker_session_audit ADD COLUMN capability_expires_at timestamptz;
