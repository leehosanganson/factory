# Monitor list ID alignment

A local `factory monitor list --limit 5` run made a formatting defect directly
visible: UUID-based legacy IDs extended beyond the 29-character ID column,
shifting later columns relative to timestamp-prefixed IDs. A seeded mixed-ID test
reproduced the same column mismatch before the width correction and passes with
the 36-character column. Other recent monitor-list output checks also pass.
