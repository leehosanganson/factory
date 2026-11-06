ALTER TABLE factory_schedules ADD COLUMN next_occurrence_at timestamptz;
ALTER TABLE factory_schedules ADD COLUMN pending_occurrence_at timestamptz;
ALTER TABLE factory_schedules ADD COLUMN skipped_revision bigint NOT NULL DEFAULT 1 CHECK (skipped_revision > 0);
ALTER TABLE factory_schedules ADD COLUMN skipped_first_at timestamptz;
ALTER TABLE factory_schedules ADD COLUMN skipped_last_at timestamptz;
ALTER TABLE factory_schedules ADD COLUMN skipped_count bigint NOT NULL DEFAULT 0 CHECK (skipped_count >= 0);
