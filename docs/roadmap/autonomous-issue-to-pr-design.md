# Autonomous issue-to-PR engineering system

## Status and scope

This document records an agreed product direction and an ordered design/implementation plan. Most of the lifecycle described here remains future work; this is not a release commitment. Today Factory is a Go CLI with the workflows described in the [implemented feature index](../features/README.md), plus a local CLI for durably queuing and inspecting provider-neutral issue work requests. `factory work issue <dedup-key>` fetches and displays one ephemeral current GitHub snapshot; `factory work refresh/history` explicitly persists and lists versioned observations; `factory work watch` performs opt-in foreground polling until closure. Watch does not start an autonomous worker or engineering. The CLI does not create/update pull requests; an internal read-only GitHub pull request snapshot adapter exists, but no worker or PR writes are available.

The intended system takes a bounded engineering issue, works through changes in a repository, and maintains a pull request for human review. The system continuously observes the source issue while work is active, incorporates relevant changes, and stops for human direction when the requested scope is ambiguous or exceeds policy. It can prepare and update a PR, but people retain authority over merge, release, and deployment.

### Explicit MVP boundaries

- **Issue tracker:** GitHub Issues.
- **Code host and PR:** GitHub repositories and GitHub pull requests.
- **Provider boundaries:** tracker and code-host integrations are separate provider-neutral interfaces, even though GitHub supplies both MVP implementations. Do not make the issue tracker and repository host a single inseparable integration.
- **Intake:** a CLI submission path and a built-in durable, single-host local queue. Both create the same request type and enter the same worker path; neither has a separate execution implementation.
- **Future hosting shape:** keep core request processing and worker responsibilities suitable for later hosting in a server. This is a structural constraint, not permission to build server mode in the MVP.
- **Out of MVP:** a Factory server, provider events/webhooks, an external or distributed broker, and Azure DevOps integration. Azure DevOps is roadmap-only and must cover **both Azure Boards as tracker and Azure Repos pull requests as code host**; it is not an MVP adapter or partial MVP milestone.

## Product behavior

### Submission and durable request lifecycle

A request identifies the source issue and its tracker, the target repository and code-host provider, and the context needed to create a bounded engineering job. The precise user-facing fields and defaults remain to be designed, but both CLI intake and local-queue admission must validate and persist the same canonical request before acknowledging acceptance.

The durable record should distinguish at least the request, its lifecycle state, attempts, issue observations, plan/reconciliation history, workspace and branch references, PR identity, pending human decisions, and final outcome. Persist transitions and enough evidence to explain what happened after restart. A queue item must not disappear merely because a process exits or the host reboots. The local queue is single-host and locally durable; it is not a distributed coordination service.

A request moves through explicit states such as accepted/queued, active, waiting for human direction, reconciling, verifying, and terminal. Exact names and transition rules are an implementation decision, but accepted work must not be silently dropped, duplicated, or reported complete before its durable result is recorded. Cancellation, retry, and recovery behavior must be explicit and bounded.

### Issue monitoring and reconciliation

Once accepted, the worker continuously refreshes and monitors the issue until it closes. Monitoring continues while work is paused for human direction so further issue changes are not missed; an explicit cancellation or other human-directed termination may stop the request. The MVP may choose a polling mechanism and interval during implementation; it must not depend on provider events, which are outside MVP scope. Record observation time and a stable issue version/fingerprint where available so the worker can identify changed source material and explain its decisions.

Issue changes that affect requirements, constraints, priority, or acceptance criteria are reconciled into the working plan and implementation. The system then re-verifies the resulting change and updates the **same pull request**, retaining its identity and history rather than opening a replacement for each reconciliation. If a change is ambiguous, materially expands scope, or crosses configured policy, stop autonomous work and request human direction. Do not silently reinterpret the issue or proceed outside the accepted policy.

If the issue closes, stop engineering activity and report the issue closure and current work/PR state. Preserve the worktree and any existing PR for human inspection. Do not close, merge, or delete the PR or discard the worktree as a consequence of issue closure. Closure is a stop condition, not permission to publish a different outcome.

### Human authority

Humans decide ambiguous or over-policy work and retain merge, release, and deployment authority. The system may create or update its PR according to policy, but it must not merge it, release artifacts, deploy software, or represent PR creation as approval of the change. Verification results and limitations must be visible to reviewers; automation is not an independent correctness verdict.

## Architecture boundaries

Use provider-neutral contracts for the two distinct domains:

- **Issue tracker:** fetch an issue and its current state/content, and expose enough versioning or timestamps to detect meaningful updates and closure.
- **Code host:** resolve repository identity and perform the scoped branch/PR operations required to create and update the one associated PR.

GitHub Issues and GitHub PR/repository adapters implement these contracts for MVP. Do not let GitHub-specific types or response formats become the worker's canonical request or lifecycle model. Azure DevOps remains a future integration spanning both Azure Boards and Azure Repos PRs. Supporting Boards without its corresponding Repos PR workflow (or vice versa) is not the agreed Azure roadmap outcome.

Keep intake, durable lifecycle coordination, and execution responsibilities separable. CLI submission and local queue admission normalize into the same request type and call the same admission/worker path. Keep dependencies explicit so later server hosting or provider-event intake can be added without moving lifecycle rules into a CLI command or provider callback. The MVP nevertheless implements only the CLI and local single-host queue: no server, webhooks/provider events, or external distributed broker.

## Reliability and operational design requirements

### Idempotency and side effects

Submission needs a stable idempotency identity derived from the intended source request (with an explicit way to distinguish an intentional re-run). Repeated CLI submission or queue recovery for the same identity must not create duplicate active work, branches, or PRs. Provider operations should be safe to repeat where possible, keyed to the durable request and stored PR/branch identity.

Persist intent before external side effects and record confirmed outcomes afterward. Treat timeouts, disconnects, and process crashes around GitHub writes as **uncertain**, not as proof that the operation failed. On recovery, inspect the live provider state and reconcile it against the durable record before retrying; never blindly create another branch or PR because an acknowledgement was lost. Surface cases that cannot be resolved safely for human action. Define bounded retry/backoff and avoid retry loops that can repeatedly mutate provider state.

### Ownership and recovery

For the MVP's single-host queue, durable claims/leases or equivalent ownership fencing must prevent two local workers from processing the same request concurrently, including after a restart or stale worker. A process restart is not by itself evidence that replaying a partially completed operation is safe. Recover only from recorded checkpoints with known replay semantics; reconcile uncertain GitHub, Git, and workspace side effects first.

Define what happens when ownership expires while a worker may still be running, how a stale worker is prevented from recording authoritative completion, and how an operator can identify and resolve a stuck or uncertain request. Retain the worktree on issue closure and on any outcome where deleting it would destroy evidence or unfinished work. Worktree cleanup for other terminal outcomes requires an explicit policy and must not compromise diagnosis or recovery.

The local queue's persistence format, atomicity, backup/migration behavior, retention, and corruption handling must be specified before implementation. These choices should keep the worker contract hostable by a future server without claiming that the local store already provides multi-host safety.

### Credentials and trust

Use least-privilege GitHub credentials, scoped to the repositories and issue/PR actions required by policy. Keep credentials out of request records, prompts, logs, worktrees, and artifacts; provide them only to the operations that need them and support safe rotation/revocation. Define how credentials are configured for CLI submission versus background worker execution, and fail closed when authorization is insufficient. Never infer that a local process, agent, or repository is sandboxed merely because Factory orchestrates it.

Issue text, comments, repository contents, PR feedback, and generated agent output are untrusted inputs. They do not override user instructions, policy, or credential boundaries. Define policy enforcement points for repository allowlists, permitted paths/actions, resource/time bounds, and scope escalation. If the policy decision is unclear or cannot be evaluated, pause for human direction rather than widening authority.

### Observability and operator control

Provide a durable, inspectable history for submission, queue admission, ownership changes, issue observations, plan revisions, agent actions, verification, provider reads/writes, retries, pauses, and final reports. Correlate logs and external records using stable request, attempt, branch, and PR identities. Distinguish observed facts, agent conclusions, and unresolved uncertainty. Redact secrets and avoid logging raw credentials or sensitive payloads unnecessarily.

An operator must be able to determine whether a request is queued, active, waiting, making progress, blocked on an uncertain side effect, or terminal, and why. Human-direction requests must include the changed issue evidence, proposed interpretation/scope, relevant policy boundary, and what will remain untouched while paused. Define bounded stop/cancel behavior and document which in-flight operations may still complete after a stop request.

## Ordered implementation plan

These slices are dependency ordered; they are not dates or release commitments. Each is a design/implementation gate, not a claim that the capability exists.

### Slice 1 — contracts, policy, and lifecycle model

Define the canonical request and durable state transitions shared by CLI and worker. Specify tracker and code-host interfaces independently, the GitHub MVP operations each needs, issue-change reconciliation inputs, idempotency identity, pause conditions, terminal outcomes, and provider side-effect classification. Define credential handling, repository/policy boundaries, observability fields, and the single-host ownership/recovery model before implementing autonomous writes.

**Acceptance criteria:** written contracts demonstrate that both intake paths submit the same request type; GitHub tracker and code-host responsibilities are separate; Azure is excluded from MVP; closure and human-pause behavior are unambiguous; and each external side effect has a repeat, reconcile, or human-escalation rule.

### Slice 2 — durable single-host admission and recovery

Implement the local durable queue and shared admission/worker path, initially exercising lifecycle transitions without enabling unbounded provider mutations. Add idempotent admission, exclusive ownership/fencing, durable checkpoints, bounded retry/cancellation, and restart recovery. Define operator-visible state and corruption/stuck-work handling.

**Progress:** The queue foundation provides a provider-neutral `WorkRequest`/`WorkQueue` contract and a local atomic-JSON queue with idempotent admission, explicit payload conflicts, exclusive process-bound claims, and generation fencing. A follow-on CLI intake slice adds `factory work submit/list/get`, deterministic default request identity, explicit re-run keys, and private local persistence under `factory/work-requests`. Submission is only queue admission; no issue is fetched and no worker starts. Provider adapters, a worker, broader request lifecycle/checkpoints, and restart recovery for workflow side effects are not implemented.

**Acceptance criteria:** CLI and local queue converge on the same canonical request type; duplicate submissions do not create duplicate active requests; restart tests cover every durable transition and interrupted side effect boundary; competing workers cannot authoritatively complete the same request; uncertain work is reconciled or paused, not blindly replayed. The current CLI intake does not yet establish the worker path or restart recovery for workflow side effects.

### Slice 3 — GitHub issue intake and continuous observation

Implement the GitHub Issues tracker adapter, CLI submission, issue refresh, durable issue observations, and detection of issue updates and closure. Keep the queue as the durable handoff into the worker. Do not add webhooks, Azure, server mode, or a distributed broker.

**Progress:** The provider-neutral CLI can queue and inspect tracker/code-host references. `factory work issue <dedup-key>` reads the queued request, rejects missing requests and non-GitHub trackers before network access, fetches one snapshot through the provider-neutral `IssueTracker`, and prints issue title, state, update time, version, and URL with the queued identity. This command remains ephemeral and does not persist the snapshot. The explicit `factory work refresh <dedup-key>` performs one fetch and stores an immutable observation keyed by request deduplication key and snapshot `Version`; repeats of the same version are idempotent and distinct versions are retained. `factory work history <dedup-key>` displays the saved title, state, update time, version, and URL. Records live in the private local `<state_dir>/factory/work-observations` store and contain no credentials. These reads do not alter the queue. `factory work watch <dedup-key> [--interval <duration>]` immediately reads and persists an observation, then polls in the foreground at a one-minute default interval until it observes closure; it reports duplicate versions and can be stopped by context cancellation. It is not an autonomous worker and performs no PR engineering or GitHub writes. The read-only `GitHubIssueTracker` uses the authenticated `gh` CLI; pull requests are rejected as non-issues. No worker or reconciliation exists. A separate read-only GitHub code-host PR snapshot adapter exists, but it is not connected to a worker and cannot write branches or PRs.

**Acceptance criteria:** accepted work survives process restart; issue state continues to refresh while active; changed issue evidence is durably recorded and delivered to reconciliation; closure stops further engineering activity and reports status without closing/merging the PR or deleting the worktree.

### Slice 4 — bounded implementation and same-PR maintenance

Integrate the worker with a repository workspace and GitHub code-host adapter. Create or locate one associated branch/PR, implement within approved scope, verify changes, and update that same PR after a relevant issue reconciliation. Pause before acting on ambiguous or over-policy changes. Reconcile uncertain provider outcomes before retrying writes.

**Progress:** A provider-neutral `CodeRepository` contract and read-only `GitHubCodeRepository` PR snapshot adapter now exist. `GetPullRequest(repository, number)` reads authenticated GitHub PR metadata through `gh api --method GET`, validates the requested repository/number against the response identity and URL, and returns PR content/state, branch names, commit IDs, update time, and a version fingerprint. Authentication remains managed by `gh`; Factory stores no credentials. The adapter has no write operations and is not connected to a worker or CLI. Branch creation, PR creation/update, workspace execution, polling, and issue reconciliation remain unimplemented.

**Acceptance criteria:** end-to-end tests prove that a changed issue leads to plan/implementation reconciliation, re-verification, and an update to the same PR; ambiguous and over-policy cases pause for human direction; lost acknowledgements do not create duplicate branches/PRs; verification evidence and limitations are available to the human reviewer; merge, release, and deployment remain unavailable to the worker.

### Slice 5 — operational hardening and MVP validation

Exercise long-running operation, process/host restart, credential failure/rotation, provider throttling/outages, queue recovery, stale ownership, cancellation, issue closure, and uncertain GitHub writes. Confirm retention and operator recovery behavior. Validate that the core worker has no CLI-only assumption that would block future hosting, while keeping server and event-triggered intake out of this slice.

**Acceptance criteria:** documented recovery tests show no lost accepted requests, duplicate active execution, blind replay of uncertain side effects, or unauthorized merge/release/deploy; operators can inspect and resolve blocked work; credentials are redacted and least-privileged; all agreed MVP lifecycle scenarios have automated coverage and a clear human-facing report.

### Later roadmap — additional providers and hosting

After the GitHub MVP's provider contracts and lifecycle behavior are validated, a future Azure DevOps integration may implement **both Azure Boards issue tracking and Azure Repos pull requests** against the separate interfaces. Future work may also add server hosting and provider-event intake, using the same request and worker path. An external distributed broker is a separate future architecture decision, not an assumed prerequisite or part of the MVP. Any such expansion requires its own design and explicit scope approval; this document sets no release dates.
