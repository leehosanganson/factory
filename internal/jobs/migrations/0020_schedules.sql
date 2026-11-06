CREATE TABLE factory_schedules (
    id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    cron TEXT NOT NULL,
    repository TEXT NOT NULL,
    task TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active','paused','removed')),
    revision INTEGER NOT NULL CHECK (revision > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX factory_schedules_owner_status ON factory_schedules(owner,status,id);
CREATE TABLE factory_schedule_idempotency (
    owner TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    schedule_id TEXT NOT NULL REFERENCES factory_schedules(id),
    request_json TEXT NOT NULL,
    PRIMARY KEY (owner,idempotency_key)
);
