# Detached jobs

`factory job` manages detached implementation, tidy, and monitor jobs. Jobs persist outside the target repository under `${XDG_STATE_HOME:-~/.local/state}/factory/detached-jobs`, or the configured state directory. `factory job start` supports these types only; foreground/gated `factory run` records are separate and do not appear in the detached job list.

```sh
factory job start implementation <description>
factory job start tidy <description>
factory job start monitor <description>
factory job list
factory job get <id> [--details]
factory job logs <id> [--session workflow] [--follow]
factory job attach <id>
factory job stop <id>
```

A start returns after launching a worker. Implementation and tidy workers serialize against other Factory jobs for the same canonical target. Attach follows worker output; Ctrl-C detaches the observer without stopping the worker. `stop` records a cooperative cancellation request, which the worker checks; it is not a promise of immediate forced termination. Logs and workflow events are retained. Job list/get include latest activity and best-effort status-call and direct Pi subprocess counts.

Detached tidy never commits or pushes. The monitor job uses the PR-specific monitoring engine and its duplicate guard; see [PR monitor](monitor.md). Current detached types are not a generic arbitrary command runner, and jobs cannot be chained together.
