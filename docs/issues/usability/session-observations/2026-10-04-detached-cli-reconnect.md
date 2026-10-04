# Detached CLI reconnect validation

- User-requested issue #139 supplied a reproducible process-boundary gap: the detached implementation test waited on persisted store state but did not inspect the finished job through fresh CLI processes.
- Extending that test verified `factory job get <id> --details` and `factory job logs <id> --session workflow` with the same binary and isolated config/state after the worker exited.
- Workflow session logs contain persisted lifecycle events and stage transcript paths; fake-agent output is in the referenced retained stage transcript rather than inlined in the workflow event log. The test now verifies that route without changing the CLI contract.
- A local fake `gh` executable that always fails exercises the unpublished recovery path without live provider writes; the test confirms terminal status, publication details, worktree/branch metadata, retained worktree, and readable fake-agent output.
- The process-level inspection sequence worked with existing commands, so no implementation or feature documentation change was needed.
