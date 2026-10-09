# Session observations — parallel subtask case-collision pass

- The ordinary Factory detached implementation workflow started and completed its requirements stage, but the implement-stage Pi log remained empty and the stage reported no useful progress before stopping. The scoped isolated worktree was unchanged after stop.
- Direct focused edits supported the test-first path: the new case-only plan validation test failed with a nil error before implementation, then passed after adding rejection. The filesystem-alias test confirmed this runner uses a case-sensitive temporary filesystem and skipped with that evidence.
- Improvement idea: surface useful implement-stage startup/provider diagnostics promptly when an agent produces no output, rather than showing a generic waiting/progress line for the full stage duration.
