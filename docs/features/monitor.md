# Detached PR monitor

`factory monitor <description>` monitors the open pull request associated with the current checkout's branch. It requires a clean Git checkout on a branch, a matching `origin`, a local head matching the validated PR head, and GitHub CLI access. Factory rejects a second active monitor for the same PR.

A monitor runs in a detached worker and isolated Git worktree, polling the PR and its checks. It can respond to failed checks or changed PR/check snapshots; monitoring ends when the PR is merged or closed. After bounded automatic actions against an unchanged snapshot, it pauses for explicit approval. Agent `ERROR` or invalid protocol responses and guarded commit/push failures count as failed attempts against the same snapshot and retry only up to the existing three-attempt cap, after which explicit approval is required. Manage it with:

```sh
factory monitor list
factory monitor get <id> [--details]
factory monitor approve <id>
factory monitor reject <id>
factory monitor stop <id>
factory monitor reset <id>
```

An agent's `FIXED` response does not itself authorize publication. Before committing/pushing, Factory derives changed paths from Git and revalidates the checkout, worktree, branch/baseline, safe paths, and live PR/check snapshot. It stages only Git-derived changed paths and does not force-push. There is no independent correctness evaluator. A stop request is cooperative; a commit made in the isolated worktree immediately before stop may remain unpushed.

The optional JSON `monitor_timeout` config sets a positive whole-lifecycle limit for each detached monitor, starting after configuration validation and before worker setup or PR branch reservation. Factory persists the absolute deadline on the first worker start, so recoverable-failure resets and worker restarts retain the original deadline rather than receiving a fresh lifetime. It includes worker/branch reservation waits, polling, retry waits, approval waits, and agent work; invalid configuration is rejected before the deadline starts. When omitted or empty, the monitor runs indefinitely unless stopped or the PR closes/merges. On expiry, it ends cleanly with status `stopped` and records a distinct “Monitor lifetime timeout reached” event; this is separate from an explicit stop request. This lifecycle timeout is independent of the 2-minute GitHub snapshot query timeout and the 30-minute maximum active agent action budget (also capped by `agent_timeout`).

`factory job start monitor <description>` uses the same monitor start path. Monitor is for bounded maintenance of an existing PR, not general autonomous implementation, arbitrary `run`, or job chaining. See [detached jobs](jobs.md) and the [operating skill](../../.agents/skills/factory/SKILL.md).
