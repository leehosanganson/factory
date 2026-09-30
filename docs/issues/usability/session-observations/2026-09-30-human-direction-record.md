# Session observation: version-pinned work directions

- **Type:** Implementation session observation (not a user-reported finding).
- **Observed:** The existing `factory work` help and documentation described intake, issue inspection, refresh/history, and watch, but had no explicit local command for recording or inspecting human direction while lifecycle was `waiting_for_human`.
- **Evidence:** Before implementation, `factory work help` listed submit/list/get/issue/refresh/history/watch only. The implemented observation store already had hashed immutable snapshots, persisted reconciled lifecycle state, private atomic writes, and a shared file lock, providing a local persistence boundary for a direction record.
- **Change in this task:** Added `respond` tied to the exact latest open issue version and `directions` for local inspection. Neither command resumes or approves engineering work.
- **Status:** The requested bounded capability is implemented in this worktree; no separate product issue is proposed by this session note.
