---
name: factory
description: >-
  Use when working in the Factory repository. Follow the intended REST job-service
  MVP: durable job lifecycle, optional SQL persistence, and required provider PR
  creation/update. Keep current CLI/server behavior distinct from target direction.
version: 1.1.0
author: Anson Lee
license: MIT
metadata:
  hermes:
    tags: [factory, rest-api, jobs, persistence, pull-requests]
    related_skills: [documentation]
---

# Factory operating guidance

## Product direction

Factory's intended MVP is one RESTful service for agent-driven repository jobs:

**request → durable admission → bounded execution → verification → provider PR create/update → inspectable outcome**

Creating or updating a pull request through the configured code-repository provider is required for an implementation job to succeed. Factory does not merge, release, or deploy. A successful agent process is not an independent correctness verdict; preserve verification evidence and limitations.

The REST server currently uses an in-memory job backend. Memory mode is simple and useful for development/tests, but process restart loses jobs, history, idempotency, and active execution state. Optional SQL persistence is recommended when restart durability is needed, but is not implemented yet. Never describe memory mode as durable across restarts or imply SQL/provider PR operations already exist. The target has one request/job lifecycle; issue polling is not a separate MVP. If issue intake is added later, it should submit into the same lifecycle.

A local process, agent, Nix shell, or container is not a security sandbox. Keep operator-configured repositories, credentials, harness, and resource limits separate from caller-controlled request data.

## When to use Factory

For substantive engineering work in this repository—features, behavioral fixes, multi-file changes, or work requiring requirements, implementation, review, and documentation—use `factory implement <description>`. Add `--gate` when explicit approval between stages is wanted and an interactive terminal is available.

Do not start an implementation workflow for questions, research, review-only requests, or small isolated edits. Do not use `factory monitor` as a general implementation workflow; it is for bounded routine maintenance on an existing open PR. Use `factory tidy` only for a repository-wide review/fix/document/verify pass. Before launching a workflow, determine whether this task is already running inside Factory; continue there rather than recursively starting another worker.

## Current CLI workflows

- **Implement:** requirements → implementation → review → documentation. By default it starts a detached job and attaches; `--detach` returns immediately, while `--gate` runs in the foreground with stage approvals. Each stage runs once; process success is not a correctness verdict.
- **Tidy:** repository-wide review/fix/document plus configured checks. Foreground pristine mode can publish only after safeguards and exact confirmation; dirty mode warns and never stages/commits/pushes. Detached tidy is nonpublishing.
- **Monitor:** bounded maintenance on an existing open PR. It may publish guarded fixes after path and live-snapshot validation, but there is no independent correctness evaluator. Ambiguous/high-impact work requires human direction; it cannot merge.

Current CLI implementation publication is enabled by default: successful stages/checks may lead to a task-branch commit, push, and PR creation. `auto_publish: false` disables it. A no-op creates no PR; failures may leave recovery artifacts. This is current CLI behavior, distinct from the REST target where provider PR create/update is required for successful implementation-job completion.

Inspect logs, diffs, and checks. Keep the user in control of scope and consequential choices. Factory and its agent tools are not sandboxed.

### Detached job commands

```text
factory job start implementation <description>
factory job start tidy <description>
factory job start monitor <description>
factory job list [--limit <n>]
factory job get <id> [--details]
factory job logs <id> [--session workflow] [--follow]
factory job attach <id>
factory job stop <id>
```

Ctrl-C while attached detaches the observer but leaves the worker running. `stop` requests cooperative cancellation. Foreground gated `factory run` records are managed separately from detached jobs.

## REST server target

The implemented `factory server` accepts authenticated bounded requests, executes configured workflows in isolated workspaces, and exposes status/history using a process-local in-memory registry. It does not persist jobs across restarts or create/update PRs through a configurable provider.

The target MVP must:

- keep in-memory persistence as the simple development/test backend and clearly disclose its volatility;
- provide optional SQL persistence, recommended when restart durability is needed; if configured SQL is unavailable, fail closed rather than silently falling back to memory;
- preserve idempotent admission, bounded history/resources, worker ownership, cancellation, and inspectable recovery outcomes across restarts when using durable storage;
- create or update a PR through the configured code-repository provider as a required success condition, recording confirmed PR identity/URL;
- reconcile live provider state before retrying uncertain branch/PR writes; never blindly create duplicates;
- keep repository-provider and any future issue-tracker interfaces separate;
- retain human authority over ambiguous scope, merge, release, and deployment.

Follow the [REST job contract](../../../docs/roadmap/rest-api-contract.md) for intended semantics and the [implemented REST server docs](../../../docs/features/rest-server.md) for current behavior. Do not copy intermediate design notes or stale milestones into product claims.

## Documentation and verification

Keep implemented behavior, target MVP requirements, and later ideas distinct. Update the README and canonical feature/contract docs when behavior changes; avoid duplicating detailed requirements across design notes. For docs-only work, check local links and `git diff --check`. For code changes, run focused tests and required Make targets, then inspect the final diff.

## Common pitfalls

- Calling memory-backed REST jobs durable without noting restart loss.
- Treating optional/recommended SQL as already implemented.
- Reporting a REST job successful before provider PR create/update is confirmed and recorded.
- Blindly retrying a timed-out provider write without reconciliation.
- Treating PR creation as merge approval or a correctness verdict.
- Treating tests, an agent response, process, worktree, Nix shell, or container as a security/correctness guarantee.
- Maintaining separate REST-task and autonomous issue-to-PR MVPs instead of one job lifecycle.

## Verification checklist

- [ ] Distinguish current memory-backed REST behavior from target SQL/provider-PR requirements.
- [ ] State that target implementation jobs require provider PR create/update; merge/release/deploy remain excluded.
- [ ] Do not overstate persistence, restart recovery, or uncertain-write behavior.
- [ ] Preserve user scope control and the documented trust boundary.
- [ ] Run relevant checks or report the exact blocker.
- [ ] Review the final diff and links for stale or contradictory claims.

## Tidy and monitor references

See [tidy feature docs](../../../docs/features/tidy.md) for publication safeguards. Monitor validates changed paths and the live PR/check snapshot before publishing; “guarded” does not mean independently verified. Inspect its diff and check evidence. Stop is cooperative, and a local commit may remain if stop arrives before push.
