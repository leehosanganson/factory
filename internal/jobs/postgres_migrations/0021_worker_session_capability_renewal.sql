ALTER TABLE factory_worker_session_audit DROP CONSTRAINT factory_worker_session_audit_event_check;
ALTER TABLE factory_worker_session_audit ADD CONSTRAINT factory_worker_session_audit_event_check
    CHECK (event IN ('registered', 'retired', 'capability_issued', 'capability_rotated', 'capability_renewed'));
