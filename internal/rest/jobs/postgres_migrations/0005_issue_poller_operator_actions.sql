ALTER TABLE factory_issue_poller_outcomes ALTER COLUMN job_id DROP NOT NULL;
ALTER TABLE factory_issue_poller_outcomes ADD COLUMN reason_class text NOT NULL DEFAULT '';

ALTER TABLE factory_issue_poller_outcomes DROP CONSTRAINT factory_issue_poller_outcomes_outcome_check;
ALTER TABLE factory_issue_poller_outcomes ADD CONSTRAINT factory_issue_poller_outcomes_outcome_check
    CHECK (outcome IN ('pending_label_removal','label_removed','label_removal_failed','quarantined','operator_action_required','operator_action_resolved'));
ALTER TABLE factory_issue_poller_outcomes ADD CONSTRAINT factory_issue_poller_outcomes_reason_class_check
    CHECK (reason_class IN ('','task_too_large','title_too_large','label_removal_failed'));
