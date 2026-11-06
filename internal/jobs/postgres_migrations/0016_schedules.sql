CREATE TABLE factory_schedules (
    id text PRIMARY KEY,
    owner text NOT NULL,
    cron text NOT NULL,
    repository text NOT NULL,
    task text NOT NULL,
    status text NOT NULL CHECK (status IN ('active','paused','removed')),
    revision bigint NOT NULL CHECK (revision > 0),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);
CREATE INDEX factory_schedules_owner_status ON factory_schedules(owner,status,id);
CREATE TABLE factory_schedule_idempotency (
    owner text NOT NULL,
    idempotency_key text NOT NULL,
    schedule_id text NOT NULL,
    request_json text NOT NULL,
    PRIMARY KEY (owner,idempotency_key)
);
