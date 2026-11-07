CREATE TABLE factory_schedule_audit (
    schedule_id TEXT NOT NULL REFERENCES factory_schedules(id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    creator TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('create','update','pause','resume','remove')),
    occurred_at TEXT NOT NULL,
    PRIMARY KEY (schedule_id, revision)
);
CREATE TRIGGER factory_schedule_audit_no_update
BEFORE UPDATE ON factory_schedule_audit
BEGIN
    SELECT RAISE(ABORT, 'schedule audit is append-only');
END;
CREATE TRIGGER factory_schedule_audit_no_delete
BEFORE DELETE ON factory_schedule_audit
BEGIN
    SELECT RAISE(ABORT, 'schedule audit is append-only');
END;
