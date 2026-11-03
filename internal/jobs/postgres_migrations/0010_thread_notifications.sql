ALTER TABLE factory_threads ADD COLUMN done_cycle bigint NOT NULL DEFAULT 0;

CREATE TABLE factory_thread_notifications (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    thread_id text NOT NULL REFERENCES factory_threads(id) ON DELETE CASCADE,
    done_cycle bigint NOT NULL,
    owner text NOT NULL,
    status text NOT NULL CHECK (status = 'done'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(thread_id, done_cycle)
);
CREATE INDEX factory_thread_notifications_owner_id ON factory_thread_notifications(owner, id DESC);
