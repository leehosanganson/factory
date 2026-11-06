CREATE TABLE factory_intake_owners (
    source_repository text NOT NULL,
    trigger_label text NOT NULL,
    owner_kind text NOT NULL CHECK (owner_kind IN ('webhook','poller')),
    generation bigint NOT NULL CHECK (generation > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_repository, trigger_label)
);

CREATE TABLE factory_intake_owner_audit (
    audit_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_repository text NOT NULL,
    trigger_label text NOT NULL,
    owner_kind text NOT NULL CHECK (owner_kind IN ('webhook','poller')),
    generation bigint NOT NULL CHECK (generation > 0),
    previous_kind text NOT NULL DEFAULT '' CHECK (previous_kind IN ('','webhook','poller')),
    operation text NOT NULL CHECK (operation IN ('registered','transferred')),
    attestation text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (source_repository, trigger_label, generation),
    CHECK ((operation='registered' AND generation=1 AND previous_kind='') OR (operation='transferred' AND generation>1 AND previous_kind<>'' AND previous_kind<>owner_kind AND attestation<>''))
);

CREATE FUNCTION factory_reject_intake_owner_audit_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'intake-owner audit is append-only';
END;
$$;
CREATE TRIGGER factory_intake_owner_audit_append_only
    BEFORE UPDATE OR DELETE ON factory_intake_owner_audit
    FOR EACH ROW EXECUTE FUNCTION factory_reject_intake_owner_audit_mutation();
