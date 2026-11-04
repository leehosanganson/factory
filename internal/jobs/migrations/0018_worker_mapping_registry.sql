CREATE TABLE factory_worker_mapping_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    generation INTEGER NOT NULL CHECK (generation >= 0)
);
INSERT INTO factory_worker_mapping_state(singleton, generation) VALUES (1, 0);
CREATE TABLE factory_worker_mappings (
    fingerprint TEXT PRIMARY KEY NOT NULL,
    principal_id TEXT,
    revoked INTEGER NOT NULL CHECK (revoked IN (0, 1)),
    CHECK ((revoked = 1 AND principal_id IS NULL) OR (revoked = 0 AND principal_id IS NOT NULL))
);
CREATE TABLE factory_worker_mapping_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL,
    principal_id TEXT,
    event TEXT NOT NULL CHECK (event IN ('accepted', 'changed', 'revoked')),
    generation INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TRIGGER factory_worker_mapping_audit_no_update
BEFORE UPDATE ON factory_worker_mapping_audit BEGIN
    SELECT RAISE(ABORT, 'worker mapping audit is append-only');
END;
CREATE TRIGGER factory_worker_mapping_audit_no_delete
BEFORE DELETE ON factory_worker_mapping_audit BEGIN
    SELECT RAISE(ABORT, 'worker mapping audit is append-only');
END;
