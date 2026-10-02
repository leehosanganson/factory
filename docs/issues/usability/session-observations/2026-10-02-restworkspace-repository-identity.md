# REST workspace repository identity session observation

- PR #77 was merged before this follow-up was prepared. Its source branch was deleted; the follow-up was based on freshly fetched `origin/main` in a separate worktree rather than pushing to that branch.
- The repository-identity fix and tests were transferred from the preserved dirty `rest-workspace-manager` worktree. Its manager implementation and test changes and other untracked files were left untouched.
- This session records a focused safety follow-up: reject a replaced configured checkout before result creation and success marking, and retain uncertain worktree results when checkout identity changes during `git worktree add`. The replacement-checkout tests restore the original checkout before completing.

These are task-session observations, not user-reported issues.
