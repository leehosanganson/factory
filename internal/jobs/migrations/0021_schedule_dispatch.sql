ALTER TABLE factory_schedules ADD COLUMN next_occurrence_at TEXT;
ALTER TABLE factory_schedules ADD COLUMN pending_occurrence_at TEXT;
ALTER TABLE factory_schedules ADD COLUMN skipped_revision INTEGER NOT NULL DEFAULT 1 CHECK (skipped_revision > 0);
ALTER TABLE factory_schedules ADD COLUMN skipped_first_at TEXT;
ALTER TABLE factory_schedules ADD COLUMN skipped_last_at TEXT;
ALTER TABLE factory_schedules ADD COLUMN skipped_count INTEGER NOT NULL DEFAULT 0 CHECK (skipped_count >= 0);
