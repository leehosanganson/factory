CREATE TABLE factory_worker_sessions (
    id text PRIMARY KEY NOT NULL,
    principal_id text NOT NULL,
    boot_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('active', 'retired')),
    created_at timestamptz NOT NULL,
    retired_at timestamptz
);
CREATE UNIQUE INDEX factory_worker_sessions_one_active_principal
    ON factory_worker_sessions(principal_id) WHERE status = 'active';
CREATE UNIQUE INDEX factory_worker_sessions_principal_boot
    ON factory_worker_sessions(principal_id, boot_id);

CREATE TABLE factory_worker_session_audit (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id text NOT NULL REFERENCES factory_worker_sessions(id),
    principal_id text NOT NULL,
    boot_id text NOT NULL,
    event text NOT NULL CHECK (event IN ('registered', 'retired')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION factory_worker_session_audit_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'worker session audit is append-only';
END;
$$;
CREATE TRIGGER factory_worker_session_audit_no_update
    BEFORE UPDATE OR DELETE ON factory_worker_session_audit
    FOR EACH ROW EXECUTE FUNCTION factory_worker_session_audit_immutable();
