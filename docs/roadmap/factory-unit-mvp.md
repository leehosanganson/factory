# Factory Worker MVP architecture

## Status

This document records the target MVP architecture and delivery sequence; it is not a claim that these capabilities have shipped. The current REST service already has direct-request Threads, membership, derived status, owner inbox notifications, explicit completion, a separate issue poller, and one optional GitHub `issues.labeled` webhook. These are foundations, not the target controller-managed Thread system. The REST service does not yet provide controller-owned assignment, registered Factory Workers, or Worker placement. The split Controller and Worker images and separately launched `factory worker` processes are process roles, not registered Workers in a fleet. Current behavior is documented in [implemented features](../features/README.md).

## Product boundary

Factory accepts bounded work under operator-controlled configuration and exposes an inspectable lifecycle. Each Worker uses its operator-configured default harness/profile; request content cannot select a harness, arbitrary executables, repositories, host access, or credentials. Users/operators are responsible for repository, model, and harness threat risk in their deployment. Factory does not provide or guarantee sandboxing, security isolation, or hostile-code containment.

Factory preserves job correctness and lifecycle invariants such as authenticated ownership, no unsafe reassignment/replay, scoped data/artifact handling, and conservative provider-write recovery. Those controls do not constitute a security boundary. Factory does not merge, release, or deploy.

## Components and ownership

```text
User / Webhook / Provider event
                │
                ▼
   Horizontally scaled controller replicas
   (one logical control plane)
       ├── Thread lifecycle and user notification
       ├── simple Job-to-Worker scheduler
       └── Auto or named-Worker routing
                │
                ▼
             Worker
       (owns assigned Job attempt)
                │
       local coordination adapter
       (simple first; replaceable)
                │
          one harness per job
       (initial MVP/pilot behavior)
                │
       harness-managed agents
       (topology depends on harness)
                ↔ optional communication; Kitsume may be used

Scheduled Task definition ──> standalone scheduled Job runs
                              (not part of a Thread)
```

### Controllers

Users interact with the controller layer to submit work and track Thread/Job outcomes. The MVP routing choices are **Auto** (the Controller selects an eligible Worker) or a **named Worker**. Agent routing is not part of this MVP. Auto and named-Worker selection do not let a request choose a harness, executable, repository, or credential.

The initial horizontal scaling model is interchangeable replicas of one logical control plane using shared durable state; independently scaled controller microservices are not an initial requirement. Controllers own Thread state, Job admission and inspection, user-facing status/notification, and assignment decisions. A manually pinned Job remains queued for that Worker if it is unavailable or full; it does not silently fall back. The exact unavailable-Worker behavior and any explicit reassignment action must be settled before implementation. A later context-aware coordinator is not part of this MVP.

### Threads and Jobs

A **Thread** is a user-facing work context, not an operating-system thread. A direct user request starts a Thread. Accepted events from multiple operator-configured webhook sources also create Threads through the shared authenticated ingress contract. Stable source event identity deduplicates retries; arbitrary user transforms are not supported. The authentication and event-envelope details remain open in #301. Jobs induced by a Thread belong to that Thread; a Thread may have one or more Jobs. See the [Thread model contract](thread-model.md) for ingress identity/deduplication, stable membership, current API scope, concurrent status updates, and user completion semantics.

A **Scheduled Task** is a schedule definition. Each scheduled firing creates a standalone scheduled Job that is not attached to a Thread. Scheduled work uses the same controller admission, assignment, execution, and outcome lifecycle; its distinct membership rule does not create a second execution queue. The current REST API already persists schedules and dispatches occurrences with strict five-field UTC cron. Submitters manage their own schedules; named operators can create schedules attributed to their own principal identity and manage schedules across owners; the legacy shared operator credential retains list/get/mutate access but cannot create schedules. Audit history is metadata-only and starts after upgrade for schedules that existed before the audit migration because historical actors cannot be reconstructed. No schedule UI exists; the target operator workbench requires operators to manage all schedules, including those created by submitters, while preserving creator attribution.

Thread status summarizes its Jobs and does not directly mutate their lifecycle:

- **Need Attention** if any Job is waiting for human input, recovery, or an unresolved operator decision. In the current Job model, a `running` Job classified by its durable Store as recovery-needed is included; a live running Job is not.
- Otherwise **In Progress** if any Job is queued or live-running.
- **Done** when every Thread Job is terminal (`succeeded`, `failed`, or `canceled`) and none needs input or recovery. The first Thread slice counts terminal failure/cancellation as complete.

The controller uses a deterministic classifier/reducer over explicit Job states/events, rather than an unconstrained model decision. When a Thread becomes Done, the controller notifies the user. Done does not close the Thread: the user explicitly marks it completed at the controller, which closes it. If the user adds a Job before closing the Thread, its status is recomputed and can return to In Progress or Need Attention. Scheduled Jobs and Tasks do not acquire Thread membership through status aggregation.

The target generic webhook is one shared authenticated inbound contract for multiple configured sources. It maps accepted events into canonical Thread/Job admission, deduplicates by stable source event identity, and does not allow arbitrary user transforms. Its authentication and event-envelope details remain open in [#301](https://github.com/leehosanganson/factory/issues/301). It must not silently append unrelated source events to an existing user Thread. The shipped GitHub `issues.labeled` webhook is one narrower adapter, not this general contract.

### Worker registration and execution

A **Worker** is a registered Factory execution service, distinct from a Controller, an individual process, a Job, or a harness. Each Job attempt has one assigned Worker at a time; a Worker may own multiple concurrent attempts within operator-configured capacity limits. Authenticated Worker identity and durable attempt ownership are required; a missed heartbeat, lease expiry, or restart alone does not authorize reassignment or replay. Worker registration records identity and operator-approved configuration. It does not provision a host, install Factory, or install a harness.

Each Worker exposes a **local coordination adapter**. The first implementation is simple and deterministic. It cannot change a Job's assigned Worker, select an unconfigured repository or harness, obtain credentials outside operator configuration, or override bounded job policy. A local adapter does not schedule across the fleet; more capable coordination is outside this MVP.

Each Worker uses its operator-configured default harness/profile for a Job. A request cannot choose among profiles. The initial MVP runs one harness execution context per Job. Any internal agent topology belongs to that harness and is not exposed as a Factory routing target. Multi-harness execution for one Job is outside this MVP and requires a separate design and acceptance criteria.

The term **mailbox** is informal shorthand for the Worker-to-local-harness interface. It is not a separate messaging protocol or a required MVP component. Kitsume and agent-to-agent communication are not required and do not own Factory Threads, Jobs, assignments, attempts, or outcomes.

Provider configuration may be optional, as it is in current deployments. When configured, provider PR creation/update and durable outcome recording are required for a Job to succeed; PR publication is a human-review handoff, not a correctness verdict or merge authorization.

## Pilot shape

The provisional first pilot uses one controller service (which may have horizontally scalable replicas), PostgreSQL, and at least two independently registered Workers on one host. It runs two independent small test-backed Jobs concurrently, one harness per Job, assigned to distinct Workers in an operator-approved fixture repository. The starting limits in issue #299 are validation targets, not capacity or enforcement guarantees until implemented and measured.

This is a one-host development/validation topology, not a production HA, fleet coordinator, remote placement, or sandbox claim. Users/operators accept the consequences of running their configured repositories and harnesses with the Worker account's available authority. Kubernetes remains a later deployment option, not a substitute for job ownership or recovery semantics.

## MVP delivery sequence and gates

The target MVP is Auto or named-Worker routing, operator-configured default harness per Worker, user-facing operator/developer workbench, recurring schedules, and authenticated multi-source webhook admission into the canonical Thread/Job lifecycle. This sequence is a dependency order, not a release commitment:

1. Complete the Worker identity, durable attempt ownership/fencing, cancellation/drain, conservative recovery, data-integrity, and provider-write safety gates (#248–#261). These correctness controls do not provide sandboxing.
2. Complete the target Thread lifecycle and shared authenticated multi-source webhook contract (#301). Preserve the already shipped Thread API and GitHub `issues.labeled` adapter (#306) as narrower existing behavior. Before generic webhook implementation, settle its authentication and event envelope; map configured sources into canonical Thread/Job admission, dedupe by stable source-event identity, and do not allow arbitrary transforms. Schedules create standalone Jobs.
3. Register/configure Workers and implement durable Auto or named-Worker assignment, capacity, and the operator-configured default harness/profile (#297). No Agent routing or remote host/harness provisioning.
4. Align recurring schedule management, dispatch, and authorization with the operator-only workbench. Persistent five-field UTC cron, dispatch, and operator-wide management of existing schedules already exist in the REST API; named operators can also create schedules attributed to themselves, and there is no schedule UI (#332, #333, #339). The legacy shared operator credential cannot create schedules. Operators must be able to manage all schedules while preserving creator attribution. The target UI must not duplicate backend schedule ownership or semantics.
5. Deliver the operator/developer workbench for Threads, Jobs, Workers, schedules, and configured webhook sources/events after the required APIs are available (#358). It may register/configure Worker identities but does not install software or provision compute.
6. Run the measured one-host pilot with two concurrent single-harness Jobs (#299).

Before implementation, settle and document the webhook authentication and event-envelope contract, Worker registration trust/capacity model, and behavior for unavailable pinned Workers. Do not choose provider-specific webhook formats or silently infer fallback behavior in this roadmap. Durable role-addressed handoffs (#298), multi-harness execution, richer coordinators, and Kubernetes are outside this MVP and need separate acceptance criteria.

Every stage remains subject to user scope, human approval for ambiguous work, and the current implementation facts. Successful harness execution or PR creation is not an independent correctness verdict.
