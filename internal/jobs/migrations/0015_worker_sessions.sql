CREATE TABLE factory_worker_sessions (
    id TEXT PRIMARY KEY NOT NULL,
    principal_id TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'retired')),
    created_at TEXT NOT NULL,
    retired_at TEXT
);
CREATE UNIQUE INDEX factory_worker_sessions_one_active_principal
    ON factory_worker_sessions(principal_id) WHERE status = 'active';
CREATE UNIQUE INDEX factory_worker_sessions_principal_boot
    ON factory_worker_sessions(principal_id, boot_id);

CREATE TABLE factory_worker_session_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES factory_worker_sessions(id),
    principal_id TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    event TEXT NOT NULL CHECK (event IN ('registered', 'retired')),
    created_at TEXT NOT NULL
);

CREATE TRIGGER factory_worker_session_audit_no_update
BEFORE UPDATE ON factory_worker_session_audit BEGIN
    SELECT RAISE(ABORT, 'worker session audit is append-only');
END;
CREATE TRIGGER factory_worker_session_audit_no_delete
BEFORE DELETE ON factory_worker_session_audit BEGIN
    SELECT RAISE(ABORT, 'worker session audit is append-only');
END;
