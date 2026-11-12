ALTER TABLE factory_worker_session_audit RENAME TO factory_worker_session_audit_old;
CREATE TABLE factory_worker_session_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES factory_worker_sessions(id),
    principal_id TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    event TEXT NOT NULL CHECK (event IN ('registered', 'retired', 'capability_issued', 'capability_rotated', 'capability_renewed')),
    created_at TEXT NOT NULL,
    capability_hash TEXT,
    capability_expires_at TEXT
);
INSERT INTO factory_worker_session_audit(id,session_id,principal_id,boot_id,event,created_at,capability_hash,capability_expires_at)
    SELECT id,session_id,principal_id,boot_id,event,created_at,capability_hash,capability_expires_at FROM factory_worker_session_audit_old;
DROP TABLE factory_worker_session_audit_old;
CREATE TRIGGER factory_worker_session_audit_no_update
BEFORE UPDATE ON factory_worker_session_audit BEGIN
    SELECT RAISE(ABORT, 'worker session audit is append-only');
END;
CREATE TRIGGER factory_worker_session_audit_no_delete
BEFORE DELETE ON factory_worker_session_audit BEGIN
    SELECT RAISE(ABORT, 'worker session audit is append-only');
END;
