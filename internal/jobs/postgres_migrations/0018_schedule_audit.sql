CREATE TABLE factory_schedule_audit (
    schedule_id text NOT NULL REFERENCES factory_schedules(id),
    revision bigint NOT NULL CHECK (revision > 0),
    creator text NOT NULL,
    actor text NOT NULL,
    action text NOT NULL CHECK (action IN ('create','update','pause','resume','remove')),
    occurred_at timestamptz NOT NULL,
    PRIMARY KEY (schedule_id, revision)
);
CREATE FUNCTION factory_schedule_audit_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'schedule audit is append-only';
END;
$$;
CREATE TRIGGER factory_schedule_audit_no_update
BEFORE UPDATE OR DELETE ON factory_schedule_audit
FOR EACH ROW EXECUTE FUNCTION factory_schedule_audit_append_only();
