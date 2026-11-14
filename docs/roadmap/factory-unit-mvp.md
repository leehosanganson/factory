# Factory Worker MVP: target vision and architecture

## Status and product boundary

This document defines the target Factory Worker MVP. It is a product direction, not a description of shipped behavior or a delivery-date commitment. The target is a Controller, shared PostgreSQL state, and separate Worker clients, with a required browser workbench for users and operators. It is not a high-availability or general fleet-management promise.

The target job lifecycle is:

**request → Thread/Job admission → assignment → bounded Worker execution → inspectable result and notification**

A Job succeeds when its configured harness execution returns a successful result under Factory's bounded execution rules. Publishing a branch or pull request through a configured provider is optional; publication must not be a success prerequisite for this product direction. Users inspect the result and any available evidence in the browser. Factory does not merge, release, or deploy.

A process, worktree, container, or harness is not a security sandbox. Factory makes no sandbox or hostile-code containment guarantee. Operators choose trusted repositories and harnesses and control the authority available to their Workers. Execution must remain bounded and cooperatively cancellable. Recovery is conservative: uncertain or interrupted work is retained for inspection, not automatically replayed or reassigned merely because a Worker disappears or a heartbeat is stale.

## Target components and ownership

```text
Users and operators
        │ browser
        ▼
Controller service ───── PostgreSQL
  users and access          durable Threads, Jobs, schedules,
  Thread/Job API             assignments, notifications, results
  scheduling
  assignment and outcomes
        │ authenticated Worker requests
        ▼
Separate Worker clients
  local configured harnesses
  choose one harness per assigned Job
```

The initial deployment shape is Docker Compose with a Controller, PostgreSQL, and one or more separately started Worker clients. The Controller is the authority for users, Threads and Jobs, schedule definitions and firing, notification state, Worker assignment, and recorded results. Workers receive assigned Jobs from the Controller, execute them locally, and report bounded status and results back. PostgreSQL provides shared durable state; it does not by itself promise Controller or Worker high availability, failover, or fleet coordination.

For the MVP, a Worker authenticates requests to the Controller with a basic shared secret configured by the operator. Protect that secret and the Controller network according to the deployment's trust needs. Do not expand this MVP into worker mutual TLS (mTLS), per-worker cryptographic identity, credential issuance/rotation, or a session/capability subsystem. These are deferred identity and session concerns, not requirements for the chosen initial protocol.

Each Worker has one or more harnesses configured locally by its operator. For each assigned Job, the Worker chooses one of its configured harnesses using its local policy and reports which bounded result it produced. A request, user, or schedule cannot select a harness, executable, repository path, credential, or host authority. Harness selection is an execution detail owned by the Worker; multi-harness coordination within a single Job is not part of this MVP.

The Controller schedules recurring definitions into Jobs and assigns eligible Jobs to Workers. Scheduling and assignment are distinct: a schedule creates work; it does not select or configure a harness. A Thread groups related user work and Jobs, but does not replace the canonical Job execution lifecycle. Scheduled Job runs remain standalone unless a later product decision explicitly changes that relationship.

## Threads, requests, notifications, and schedules

A Thread is the user's continuing work context, not an operating-system thread. A user can create a Thread with a bounded request, inspect its Jobs and outcomes, and add work while it remains open. The Controller owns Thread membership and derived status. When all member Jobs reach an outcome, the Controller makes that outcome available to the user and creates a notification. Notifications are part of the product's user-facing workflow; external email, chat, or push delivery is not required by this MVP.

The required browser UI must let users create and inspect Threads, submit or append Job requests, and review each Job's status, result, and outcome. It must also let operators manage recurring schedules and inspect their admitted runs. Schedule management belongs to the Controller and UI, not a local CLI. Schedules create ordinary Jobs through the same assignment, execution, and result path. Preserve recurring schedules, Threads, and notifications as core product concepts.

The MVP does not require an issue poller or issue webhook. Existing issue intake mechanisms are current implementation features, not target architecture prerequisites; if issue intake is revisited later, it should submit into the same Thread/Job lifecycle rather than create another execution system. See the current [issue poller](../features/issue-poller.md) and [GitHub issue webhook](../features/github-issue-webhook.md) docs for their shipped behavior; both are deferred from this target MVP.

## Current implementation is different

Factory already ships a REST job service, persistent schedules, direct-request Threads and owner notifications, a focused browser dashboard, and separate PostgreSQL worker processes. These provide useful foundations but are not this target architecture:

- The current separate `factory worker` connects directly to PostgreSQL and claims Jobs and writes lifecycle state through the shared Store. It does not receive assignments or report results through a Controller worker-job API.
- The current Controller session listener handles Worker registration and advisory heartbeat only. It does not carry Job claims, assignments, or results. See [REST worker process](../features/rest-worker.md) for shipped behavior and the [worker identity and credential contract](worker-identity-credential-contract.md) for the current session boundary and deferred protocol design.
- Current Docker Compose defaults to a Controller and PostgreSQL with **no Worker service**, so admitted Jobs remain queued unless the optional worker profile is started. See [local Compose API control plane](../features/compose-local-control-plane.md).
- The current embedded dashboard is operator-focused. It supports bounded Job operations, direct Thread actions, schedules, and read-only Worker-session inspection; it is not yet the required user-facing Thread request/response/outcome workbench. See [embedded REST operator dashboard](../features/rest-web-ui.md).
- Current implementation documentation describes shipped behavior. Consult [implemented features](../features/README.md), the [REST job API and lifecycle contract](rest-api-contract.md), and [REST server operations](../features/rest-server-operations.md) for current semantics. In particular, current REST jobs with a configured provider require PR publication for success. That is not the target success rule stated above.

The target Controller-to-Worker protocol replaces direct shared-PostgreSQL Worker ownership operations. Do not infer that target assignment model from the existing separate process command, session heartbeat, or Compose profile.

## Safety and operational limits

The Controller and Workers run configured repositories and harnesses with the authority of their local accounts. Network access, filesystem access, and other process permissions depend on deployment configuration; Factory does not contain or isolate hostile behavior. Keep job inputs bounded, apply configured execution time and output limits, and support cooperative per-Job cancellation. A cancellation request is not proof that every subprocess has stopped or that external effects were undone.

Persist enough lifecycle and result information for users to inspect completed work and operators to investigate failures. If a Worker disconnects, a result is ambiguous, or execution is interrupted, preserve the record and artifacts needed for review. Do not automatically rerun an attempt or assign the same uncertain work to another Worker without a safe, explicit recovery rule. Heartbeat or session expiry alone is not proof of process stop.

This topology is a practical Compose MVP, not an HA design. It does not promise redundant Controllers, automated database failover, zero downtime, an always-available Worker fleet, remote provisioning, or a fleet coordinator. Operators remain responsible for backups, recovery procedures, access controls, and deployment-specific availability.

## Scope summary

**In scope:** Compose-based Controller plus PostgreSQL and separate Workers; Controller-owned users, Threads, Jobs, recurring schedules, notifications, assignment and results; shared-secret Worker access; locally configured multiple harness choices with one selected per assigned Job; required browser flows for Thread/Job requests, responses and outcomes, and schedule management; bounded execution, cancellation, and conservative recovery.

**Deferred or excluded:** issue poller and webhook; local CLI as the user-facing workflow; Worker mTLS and identity/session subsystem; caller-selected harnesses; provider publication as a success requirement; remote host or harness provisioning; sandbox guarantees; automatic unsafe replay/reassignment; HA and fleet-coordination promises; merge, release, and deployment.

Current schedule routes, dispatch rules, and operating limits are described in [REST server](../features/rest-server.md) and [REST server operations](../features/rest-server-operations.md). The [Thread model contract](thread-model.md) documents the current Thread implementation and separates it from the broader target. Those references do not override the target decisions in this document.
