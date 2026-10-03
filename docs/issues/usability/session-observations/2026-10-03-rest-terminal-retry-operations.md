# Session observation: REST terminal retry operations

- **Scope:** Clarified the REST operations guide for #89 on a worktree aligned to `origin/main` at `2edf349`. The prior text mentioned same-identity provider reconciliation and that its result is not written into the job, but did not explain that terminal failed jobs cannot be retried through the API or give operator decision steps.
- **Change:** Documented checking retained job/history/workspace and the live provider state, avoiding a new idempotency key or duplicate PR, stopping on ambiguity, and the manual same-identity recovery boundary. Explicitly stated that the Factory record stays failed without provider outcome and there is no retry endpoint or persistence path for the manual result.
- **Verification:** Documentation links and `git diff --check` checked; only documentation changed.
