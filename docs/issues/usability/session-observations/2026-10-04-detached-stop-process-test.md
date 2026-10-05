# Detached stop across processes

- Extended the deterministic detached implementation CLI process test to issue `factory job stop <id>` from a second process using the same binary, config, state directory, and environment while the fake agent was blocked.
- The stop command reported success and persisted the request. Releasing the fake agent let its cooperative cancellation complete; the durable record reached `stopped`, and the worker record and target lock were cleared.
- The test also verified the target checkout HEAD and working tree remained unchanged, the implementation worktree and workflow log were retained, and the fake `gh` provider was never invoked.
- Focused process-level test passed. No product behavior changes were needed.
