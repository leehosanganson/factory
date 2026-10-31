CREATE TABLE factory_github_issue_webhook_deliveries (
    delivery_id TEXT PRIMARY KEY NOT NULL,
    body_sha256 TEXT NOT NULL CHECK (length(body_sha256) = 64),
    thread_id TEXT NOT NULL UNIQUE REFERENCES factory_threads(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL
);
