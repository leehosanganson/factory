# Avoid stale worktree hints after implementation publication

- **Finding:** A completed implementation job with a persisted publication
  outcome may still be told to inspect its worktree diff.
- **Evidence:** `jobWatchNextAction` chooses the worktree-diff hint whenever a
  completed implementation has a non-empty `Worktree`, without checking its
  publication outcome. Successful publication removes the implementation
  worktree in `cleanupPublishedImplementationWorktree`. The current local job
  store had no published implementation record, so this was confirmed by source
  inspection rather than an interactive reproduction.
- **Desired outcome:** Make completed-job next-action hints reflect whether the
  implementation was published and whether its worktree is still available.
- **Acceptance criteria:** A published implementation is not directed to inspect
  a removed worktree; unpublished jobs with an available worktree retain the
  review-diff hint; task remains to inspect/review output and does not imply
  publication proves correctness.
- **Status:** Implemented in `internal/factory/job_watch.go`. Completed
  implementations recommend the recorded diff only when the publication is not
  marked `published` and the worktree directory is available; otherwise the
  hint directs users to inspect job details. Tests cover published,
  unpublished/no-op, absent outcome, and missing worktrees. Publication is not
  presented as proof of correctness.
