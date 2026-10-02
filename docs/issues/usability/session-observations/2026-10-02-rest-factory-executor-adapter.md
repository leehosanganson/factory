# REST Factory executor adapter session observation

- The executor changes are isolated on a new worktree based on freshly fetched `origin/main`; the original checkout's roadmap edit and existing observation files were left untouched.
- PRs #79, #80, and #81 were open when first polled, each targeted `main`, and had distinct workflow, worker, and workspace seams. They subsequently merged before this adapter was ready; the adapter worktree incorporated their exact head SHAs.
- The shared bounded output seam was necessary to prevent Factory's normal private per-stage and per-check transcript files from retaining additional unbounded subprocess output. REST workflows disable those extra transcripts and share one bounded capture.
- Verification for this task is being performed in the adapter worktree. No REST command/server wiring or publication behavior is included.

These are task-session observations, not user-reported issues.
