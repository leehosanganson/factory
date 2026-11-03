ALTER TABLE factory_issue_poller_outcomes DROP CONSTRAINT factory_issue_poller_outcomes_reason_class_check;
ALTER TABLE factory_issue_poller_outcomes ADD CONSTRAINT factory_issue_poller_outcomes_reason_class_check
    CHECK (reason_class IN ('','task_too_large','title_too_large','label_removal_failed','provenance_conflict'));
