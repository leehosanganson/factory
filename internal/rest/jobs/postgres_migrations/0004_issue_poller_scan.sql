UPDATE factory_issue_poller_outcomes SET outcome='pending_label_removal'
WHERE outcome='version_changed';

ALTER TABLE factory_issue_poller_outcomes DROP CONSTRAINT factory_issue_poller_outcomes_outcome_check;
ALTER TABLE factory_issue_poller_outcomes ADD CONSTRAINT factory_issue_poller_outcomes_outcome_check
    CHECK (outcome IN ('pending_label_removal','label_removed','label_removal_failed'));

CREATE TABLE factory_issue_poller_scan_state (
    source_repository text PRIMARY KEY,
    page integer NOT NULL DEFAULT 1 CHECK (page > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
