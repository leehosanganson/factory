# Detached PR monitor

`factory monitor <description>` monitors the open pull request associated with the current checkout's branch. It requires a clean Git checkout on a branch, a matching `origin`, a local head matching the validated PR head, and GitHub CLI access. Factory rejects a second active monitor for the same PR.

A monitor runs in a detached worker and isolated Git worktree, polling the PR and its checks. It can respond to failed checks or changed PR/check snapshots; monitoring ends when the PR is merged or closed. After bounded automatic actions against an unchanged snapshot, it pauses for explicit approval. Manage it with:

```sh
factory monitor list
factory monitor get <id> [--details]
factory monitor approve <id>
factory monitor reject <id>
factory monitor stop <id>
factory monitor reset <id>
```

An agent's `FIXED` response does not itself authorize publication. Before committing/pushing, Factory derives changed paths from Git and revalidates the checkout, worktree, branch/baseline, safe paths, and live PR/check snapshot. It stages only Git-derived changed paths and does not force-push. There is no independent correctness evaluator. A stop request is cooperative; a commit made in the isolated worktree immediately before stop may remain unpushed.

`factory job start monitor <description>` uses the same monitor start path. Monitor is for bounded maintenance of an existing PR, not general autonomous implementation, arbitrary `run`, or job chaining. See [detached jobs](jobs.md) and the [operating skill](../../.agents/skills/factory/SKILL.md).
