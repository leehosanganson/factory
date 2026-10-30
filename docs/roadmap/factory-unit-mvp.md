# Factory-unit MVP architecture

## Status

This document records the North Star target architecture and sequencing. Its Thread ingress, membership, status, notification, and user-completion details are specified in [the Thread model contract](thread-model.md). These capabilities are not claims about the shipped REST server and are not implementation or deployment authorization. Current behavior remains documented in [implemented features](../features/README.md). In particular, the current service has no horizontally scalable controller replicas, controller-level Thread model, simple multi-unit scheduler, or registered Factory-unit execution API.

## Product boundary

Factory accepts bounded work under operator-controlled configuration and exposes an inspectable lifecycle. Users/operators are responsible for repository, model, and harness threat risk in their deployment. Factory does not provide or guarantee sandboxing, security isolation, or hostile-code containment. Operator-configured repository aliases, enabled harness profiles, credentials, and bounded job policy remain authoritative; request content cannot select arbitrary executables, host access, or credentials.

Factory preserves job correctness and lifecycle invariants such as authenticated ownership, no unsafe reassignment/replay, scoped data/artifact handling, and conservative provider-write recovery. Those controls do not constitute a security boundary. Factory does not merge, release, or deploy.

## Components and ownership

```text
User / Webhook / Provider event
                │
                ▼
   Horizontally scaled controller replicas
   (one logical control plane)
       ├── Thread lifecycle and user notification
       ├── simple job-to-unit scheduler
       └── user pin or scheduler-selected unit
                │
                ▼
          Factory unit / worker
       (owns assigned job attempt)
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

Users interact with the controller layer to submit work, choose whether to assign it or let the scheduler choose, and track Thread/Job outcomes. The initial horizontal scaling model is interchangeable replicas of one logical control plane using shared durable state; independently scaled controller microservices are not an initial requirement.

Controllers own Thread state, Job admission and inspection, user-facing status/notification, and assignment decisions. The first scheduler is simple: it honors a user-selected named Factory unit or selects an eligible unit according to configured capacity/availability. A manually pinned Job remains queued for that unit when it is unavailable or full. The controller does not silently move it; reassignment requires an explicit user or authorized controller action.

The control-plane scheduler remains simple for the initial architecture. A future context-aware coordinator/orchestrator is an optional replacement behind the Factory-unit local coordination adapter, where it can coordinate local execution only; it cannot change unit placement.

### Threads and Jobs

A **Thread** is a user-facing work context, not an operating-system thread. A direct user request starts a Thread. Webhook and Git/provider or project-management events (for example GitHub Issues or Linear) also enter through adapters and create Threads. Jobs induced by a Thread belong to that Thread; a Thread may have one or more Jobs. See the [Thread model contract](thread-model.md) for ingress identity/deduplication, stable membership, the approved first-slice API boundary, concurrent status updates, and user completion semantics.

A **Scheduled Task** is a schedule definition. Each scheduled firing creates a standalone scheduled Job that is not attached to a Thread. Scheduled work uses the same controller admission, assignment, execution, and outcome lifecycle; its distinct membership rule does not create a second execution queue.

Thread status summarizes its Jobs and does not directly mutate their lifecycle:

- **Need Attention** if any Job is waiting for human input, recovery, or an unresolved operator decision. In the current Job model, a `running` Job classified by its durable Store as recovery-needed is included; a live running Job is not.
- Otherwise **In Progress** if any Job is queued or live-running.
- **Done** when every Thread Job is terminal (`succeeded`, `failed`, or `canceled`) and none needs input or recovery. The first Thread slice counts terminal failure/cancellation as complete.

The controller uses a deterministic classifier/reducer over explicit Job states/events, rather than an unconstrained model decision. When a Thread becomes Done, the controller notifies the user. Done does not close the Thread: the user explicitly marks it completed at the controller, which closes it. If the user adds a Job before closing the Thread, its status is recomputed and can return to In Progress or Need Attention. Scheduled Jobs and Tasks do not acquire Thread membership through status aggregation.

Webhook/provider adapters must preserve source identity and deduplicate/reconcile event delivery before admission. Exact identity, repeated-event, and Thread reuse rules are specified in [#301](https://github.com/leehosanganson/factory/issues/301); they must not silently append unrelated source events to an existing user Thread.

### Factory units and execution

A **Factory unit** is a registered execution service, distinct from a controller, a worker process, a Job, a harness, or an agent role. Each Job attempt has one assigned Factory unit at a time; a unit may own multiple concurrent attempts within separately configured capacity limits. Authenticated unit identity and durable attempt ownership are required; a missed heartbeat, lease expiry, or unit restart alone does not authorize reassignment or replay.

Each unit exposes a **local coordination adapter**. The first implementation can be simple and deterministic. Its interface may later select a context-aware coordinator/orchestrator for work local to that unit. It cannot change a Job's assigned unit, select an unconfigured repository or harness, obtain credentials outside operator configuration, or override bounded job policy. A unit-local coordinator does not schedule across the fleet.

The initial Factory-unit MVP runs **one harness execution context per Job**. A harness controls its own internal agent topology; Factory does not assume one agent per harness. Later, an explicit adapter capability may coordinate multiple harnesses for one Job, but those harnesses remain on the same Factory unit that owns the Job. That later capability requires defined subtask ownership, separate workspaces/artifacts, concurrency bounds, and trusted integration semantics before it is enabled.

Agent-to-agent communication is optional and may be harness-native or use a future bounded protocol. Kitsume may provide optional memory/communication integration; it is not required by the MVP and is not the authority for Factory Threads, Jobs, assignments, attempts, or outcomes.

Provider configuration may be optional, as it is in current deployments. When configured, provider PR creation/update and durable outcome recording are required for a Job to succeed; PR publication is a human-review handoff, not a correctness verdict or merge authorization.

## Pilot shape

The provisional first pilot uses one controller service (which may have horizontally scalable replicas), PostgreSQL, and at least two independently registered Factory units on one host. It runs two independent small test-backed Jobs concurrently, one harness per Job, assigned to distinct units in an operator-approved fixture repository. The starting limits in issue #299 are validation targets, not capacity or enforcement guarantees until implemented and measured.

This is a one-host development/validation topology, not a production HA, fleet coordinator, remote placement, or sandbox claim. Users/operators accept the consequences of running their configured repositories and harnesses with the unit account's available authority. Kubernetes remains a later deployment option, not a substitute for job ownership or recovery semantics.

## Delivery sequence and gates

1. Implement authenticated Factory-unit identity, durable attempt ownership/fencing, and conservative recovery/cancellation/drain (#248, #259, #260). Current runtime behavior and the isolated PostgreSQL protocol remain distinct from those future guarantees.
2. Implement attempt/job-scoped data integrity and safe serialized provider operations (#261), including stale-output rejection and ambiguous-write handling. These protect lifecycle and publication correctness; they do not provide sandboxing.
3. Implement the controller-level Thread lifecycle, ingress distinctions, Job membership, status reducer/notification, and explicit user completion action (#301).
4. Register Factory units and add the simple scheduler plus local coordination adapter; start with named-unit pinning or eligible-unit selection and one harness per Job (#297).
5. Add durable role-addressed handoffs only if needed by multi-agent coordination; keep the protocol optional and separate from Thread status, ordinary job lifecycle events, and Kitsume (#298).
6. Run the measured one-host multi-container pilot with two concurrent single-harness Jobs (#299).
7. Consider same-unit multi-harness orchestration, more capable context-aware coordinators, additional ingress adapters, and Kubernetes only as separate later capabilities with their own acceptance criteria.

Every stage remains subject to user scope, human approval for ambiguous work, and the current implementation facts. Successful harness execution or PR creation is not an independent correctness verdict.
