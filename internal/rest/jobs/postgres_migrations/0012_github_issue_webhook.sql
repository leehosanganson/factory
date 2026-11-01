CREATE TABLE factory_github_issue_webhook_deliveries (
    delivery_id text PRIMARY KEY NOT NULL,
    body_sha256 text NOT NULL CHECK (length(body_sha256) = 64),
    thread_id text NOT NULL UNIQUE REFERENCES factory_threads(id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
