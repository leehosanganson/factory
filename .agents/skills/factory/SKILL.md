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

The current REST server supports volatile memory persistence (the default) and operator-configured SQLite through strict server JSON; configured SQLite fails closed if unavailable. After readiness, the runtime logs the selected persistence backend; it never logs the SQLite path or secrets. Memory records are lost on restart. With SQLite, queued jobs resume after restart, while interrupted running jobs require operator investigation and are not replayed. Configured GitHub PR publication is required for job success. An explicit authenticated reconciliation action applies only to eligible failed SQLite jobs with a persisted provider attempt and retained workspace; it performs read-only provider confirmation without running the harness or writing to the provider. Keep these shipped behaviors distinct from remaining target requirements. The target has one request/job lifecycle; issue polling is not a separate MVP. If issue intake is added later, it should submit into the same lifecycle.

A local process, agent, Nix shell, or container is not a security sandbox. Keep operator-configured repositories, credentials, harness, and resource limits separate from caller-controlled request data.

## When to use Factory

For substantive engineering work in this repository—features, behavioral fixes, multi-file changes, or work requiring requirements, implementation, review, and documentation—use `factory implement <description>`. Add `--gate` when explicit approval between stages is wanted and an interactive terminal is available.

Do not start an implementation workflow for questions, research, review-only requests, or small isolated edits. Do not use `factory monitor` as a general implementation workflow; it is for bounded routine maintenance on an existing open PR. Use `factory tidy` only for a repository-wide review/fix/document/verify pass. Before launching a workflow, determine whether this task is already running inside Factory; continue there rather than recursively starting another worker.

## Current CLI workflows

- **Implement:** requirements → implementation → review → documentation. By default it starts a detached job and attaches; `--detach` returns immediately, while `--gate` runs in the foreground with stage approvals. Each stage runs once; process success is not a correctness verdict.
- **Tidy:** repository-wide review/fix/document plus configured checks. Foreground pristine mode can publish only after safeguards and exact confirmation; dirty mode warns and never stages/commits/pushes. Detached tidy is nonpublishing.
- **Monitor:** bounded maintenance on an existing open PR. It may publish guarded fixes after path and live-snapshot validation, but there is no independent correctness evaluator. Ambiguous/high-impact work requires human direction; it cannot merge.

Current CLI implementation publication is enabled by default: successful stages/checks may lead to a task-branch commit, push, and PR creation. `auto_publish: false` disables it. A no-op creates no PR; failures may leave recovery artifacts. This is current CLI behavior, distinct from the REST server, where configured GitHub PR create/update is required for successful job completion.

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

The implemented `factory server` accepts authenticated bounded requests, executes configured workflows in isolated workspaces, and exposes status/history using operator-selected memory (default) or SQLite persistence and an optional configured GitHub PR provider. Memory state is lost on restart. With SQLite, queued jobs resume; interrupted running jobs require operator investigation and are not replayed. When GitHub is configured, PR create/update is required for success. An explicit authenticated reconciliation action is available only for eligible failed SQLite jobs with a persisted provider attempt and retained workspace; it confirms provider state read-only without harness replay or provider writes. After readiness, the runtime logs the selected persistence backend; it never logs the SQLite path or secrets.

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

- Calling memory-backed REST jobs durable without noting restart loss, or implying the runtime logs the SQLite path or secrets.
- Treating SQLite as available without operator configuration, or implying configured SQLite silently falls back to memory.
- Reporting a GitHub-configured REST job successful before PR create/update is confirmed and recorded.
- Assuming interrupted running SQLite jobs automatically resume or are eligible for failed-job reconciliation.
- Treating failed-job reconciliation as a harness replay or provider write; it is read-only provider confirmation for eligible persisted attempts with retained workspaces.
- Blindly retrying a timed-out provider write without reconciliation.
- Treating PR creation as merge approval or a correctness verdict.
- Treating tests, an agent response, process, worktree, Nix shell, or container as a security/correctness guarantee.
- Maintaining separate REST-task and autonomous issue-to-PR MVPs instead of one job lifecycle.

## Verification checklist

- [ ] Distinguish current selectable memory/SQLite REST behavior and optional GitHub publication from remaining target requirements; state that after readiness the runtime logs the selected persistence backend without logging the SQLite path or secrets.
- [ ] State that configured GitHub PR create/update is required for current REST job success, while target provider requirements remain explicit; merge/release/deploy remain excluded.
- [ ] Describe restart recovery precisely: queued SQLite jobs resume, interrupted running jobs require operator investigation and are not replayed; explicit reconciliation is read-only and restricted to eligible failed SQLite jobs with persisted provider attempts and retained workspaces.
- [ ] Do not overstate persistence, restart recovery, or uncertain-write behavior.
- [ ] Preserve user scope control and the documented trust boundary.
- [ ] Run relevant checks or report the exact blocker.
- [ ] Review the final diff and links for stale or contradictory claims.

## Tidy and monitor references

See [tidy feature docs](../../../docs/features/tidy.md) for publication safeguards. Monitor validates changed paths and the live PR/check snapshot before publishing; “guarded” does not mean independently verified. Inspect its diff and check evidence. Stop is cooperative, and a local commit may remain if stop arrives before push.
