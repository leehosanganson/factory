ALTER TABLE factory_threads ADD COLUMN done_cycle INTEGER NOT NULL DEFAULT 0;

CREATE TABLE factory_thread_notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id TEXT NOT NULL REFERENCES factory_threads(id) ON DELETE CASCADE,
    done_cycle INTEGER NOT NULL,
    owner TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status = 'done'),
    created_at TEXT NOT NULL,
    UNIQUE(thread_id, done_cycle)
);
CREATE INDEX factory_thread_notifications_owner_id ON factory_thread_notifications(owner, id DESC);
