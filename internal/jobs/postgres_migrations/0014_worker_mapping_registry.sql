CREATE TABLE factory_worker_mapping_state (
    singleton integer PRIMARY KEY CHECK (singleton = 1),
    generation bigint NOT NULL CHECK (generation >= 0)
);
INSERT INTO factory_worker_mapping_state(singleton, generation) VALUES (1, 0);
CREATE TABLE factory_worker_mappings (
    fingerprint text PRIMARY KEY NOT NULL,
    principal_id text,
    revoked boolean NOT NULL,
    CHECK ((revoked AND principal_id IS NULL) OR (NOT revoked AND principal_id IS NOT NULL))
);
CREATE TABLE factory_worker_mapping_audit (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fingerprint text NOT NULL,
    principal_id text,
    event text NOT NULL CHECK (event IN ('accepted', 'changed', 'revoked')),
    generation bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE FUNCTION factory_worker_mapping_audit_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'worker mapping audit is append-only';
END;
$$;
CREATE TRIGGER factory_worker_mapping_audit_no_update
    BEFORE UPDATE OR DELETE ON factory_worker_mapping_audit
    FOR EACH ROW EXECUTE FUNCTION factory_worker_mapping_audit_immutable();
