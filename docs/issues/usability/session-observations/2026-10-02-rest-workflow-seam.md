# REST workflow seam session observation

- Started implementation in a dedicated worktree from `origin/main`; pre-existing roadmap and observation-file edits in the original checkout remained untouched.
- Workflow stage and pipeline-check execution are now injectable independently, and the same bounded capture can be supplied to both paths.
- Focused tests exercise truncation, continued child-output draining, explicit child environments, cancellation, and unchanged publication state. No Factory CLI usability finding was verified during this implementation task.
