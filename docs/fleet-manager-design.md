# Fleet manager design

## Purpose and status

This document is an architecture brainstorm for a future Factory fleet manager. It is a proposal, not a finalized specification or a description of current functionality. Today Factory is a Go CLI with detached local jobs; it is not a server or fleet manager. The roadmap keeps Factory as the execution runtime and places fleet coordination in `factory-control`.

The core goal is to coordinate independently scaled Factory instances, including instances on remote hosts, without moving container lifecycle management or workflow execution into the fleet manager. Durable job state and safe worker ownership are prerequisites, not capabilities this proposal assumes already exist.

## Proposed topology and boundaries

- **Kubernetes (or another deployment platform)** creates, runs, restarts, scales, and terminates Factory containers. It owns container lifecycle, not correctness or job recovery.
- **Factory instances** execute jobs and own workflow semantics: stages, agent sessions, recoverable errors, cancellation, and workflow-level retry policy. Each instance can serve multiple jobs subject to its configured capacity. Instances scale independently.
- **Fleet manager (`factory-control`)** accepts or discovers job requests, coordinates job assignment, observes instance health and capacity, and provides fleet-wide job control and discovery. It does not start, stop, or manage containers and does not duplicate Factory's workflow logic.
- **Durable shared state** stores job definitions, authoritative job/attempt state, events, cancellation requests, and ownership metadata independently of any manager or Factory process. Logs and artifacts need durable references and retention policy; whether their contents live in the same store is open.

A possible high-level path is:

```text
Client / scheduler -> Fleet manager -> durable job record
                                      ^              |
                                      |              v
Remote or local Factory instances <--- claim / report
                    |
                    +-- execute workflow and persist progress

Kubernetes / deployment platform independently manages Factory containers.
```

The diagram is logical, not a required network topology. The manager should be replaceable or restartable without losing job truth; workers should not depend on in-memory manager state to establish authority.

## Remote connectivity and trust boundaries

Prefer a Factory-initiated, outbound authenticated connection to the control plane, using polling/claiming or a long-lived connection. This supports workers behind NAT or restrictive firewalls and avoids requiring inbound access to every host. A mutually authenticated API where the manager initiates worker requests is an alternative when networks permit it, but increases inbound exposure and operational setup. The protocol should authenticate both ends, authorize each worker's claims and reports, protect transport, and support credential rotation and revocation. The exact transport and identity provider are open decisions.

Treat job descriptions, repository credentials, logs, and artifacts as sensitive. Remote hosts may have different trust and network boundaries from the control plane and shared state. Workers should receive only the job data and credentials they need; a worker identity must not grant broad fleet or store access. Do not treat container isolation as a security boundary for configured agents or their tools.

## Registration, health, capacity, and drain

An instance registers a stable identity and reports protocol version, supported capabilities/configuration, available capacity, and current assignments. It periodically heartbeats with health and capacity; stale heartbeats are evidence that a worker is unreachable, not proof that its process is dead or its side effects stopped. Registration and heartbeat expiry thresholds, identity persistence across container replacement, and capability matching need specification.

Capacity should be explicit and independently configurable per instance, including concurrent jobs and any relevant resource limits. The manager assigns only work compatible with reported capabilities and available capacity. A drain request prevents new assignments and lets existing work finish or reach a safe handoff point; it must not equate drain with abrupt termination. Kubernetes can then perform its ordinary lifecycle action after the drain policy permits it.

## Job authority and execution lifecycle

The durable store is the source of truth for job and attempt state. The manager coordinates requests and assignment, while a Factory worker owns workflow transitions within its authorized attempt. A rough lifecycle is:

1. A request is validated and durably recorded as queued, with an idempotency key or equivalent deduplication rule for repeated submissions.
2. A compatible worker claims the job and receives a time-bounded ownership lease with a monotonically increasing fencing token or generation.
3. The worker records progress and events durably as it executes. Updates are accepted only from the current attempt/token; stale workers cannot commit state after ownership changes.
4. Factory handles recoverable workflow failures according to its own bounded recovery policy and records the outcome. It reports terminal completion, cancellation, or a failure requiring coordination.
5. The manager reconciles persisted state, worker reports, lease expiry, and health signals, and decides whether to leave a job terminal, reassign it, or require intervention.

A lease does not stop a paused or partitioned old process. Fencing must be checked by the durable state-writing boundary and, where possible, by side-effecting integrations. There must never be two authorized owners for the same attempt. A replacement attempt is not permission to run an uncertain in-flight operation concurrently with the old worker.

## Failure handling and safe recovery

Distinguish failures Factory can recover from while it is running from loss of the Factory process itself:

- **Recoverable workflow/job failure:** Factory retains responsibility for bounded recovery, stage semantics, and recording progress/outcome. Fleet management should not add an independent blanket retry loop or reinterpret a recoverable failure as container loss.
- **Abrupt instance loss:** A process or container may disappear (for example, OOM, host failure, or network partition) before it can report an outcome. The manager detects missing heartbeats or expired ownership, then reconciles the durable job record and last committed progress. Kubernetes may restart a container, but that alone does not establish whether a job is safe to resume.
- **Safe reassignment:** A new Factory may resume or retry only when the job's durable checkpoint and operation semantics establish that it is safe, and the old attempt is fenced from further authoritative writes. Recovery should prefer a safe checkpoint or explicit idempotent operation boundary over replaying an entire job blindly.
- **Uncertain side effects:** If the previous attempt may have changed an external system and there is no reliable idempotency key, query/reconciliation mechanism, or safe checkpoint, do not automatically retry that work. Mark it for reconciliation or a human decision. Report the uncertainty rather than presenting it as a normal retry.

Recovery therefore requires Factory-level durable progress and well-defined idempotency/checkpoint contracts before fleet reassignment is enabled. Kubernetes health and restart policies do not provide these guarantees.

## Fleet manager durability and operations

The manager itself may restart or run redundantly. Its transient scheduling decisions should be reconstructible from durable records; concurrent manager replicas need a defined coordination mechanism so they cannot issue conflicting ownership. Job claims and fencing, rather than a manager's in-memory view, should be authoritative. Manager failover behavior, store availability requirements, and consistency expectations remain open.

Expose fleet and job status, assignment/lease transitions, failure classifications, drain progress, and recovery decisions through structured logs, events, and metrics. Operators need ways to inspect stale workers and uncertain jobs, request drain, and make or record recovery decisions. Health/readiness should distinguish a live manager from its ability to reach durable state. Define retention and access controls for job data, event history, logs, credentials, and audit records.

## Possible phased path

1. **Single-worker foundation:** Define durable job/attempt state, checkpoints, worker ownership, fencing, and recoverability classifications for Factory. Prove restart/recovery semantics before fleet orchestration.
2. **One server and store:** Run a containerized Factory server with durable state, authenticated worker/control APIs, capacity limits, and job inspection; verify process restart and interruption behavior.
3. **Small fleet:** Add manager-coordinated claims, registration, heartbeats, drain, and safe reassignment. Test abrupt loss, stale workers, manager restart, duplicate requests, and uncertain side effects.
4. **Remote hosts and operations:** Validate outbound authenticated connectivity, identity/credential rotation, network partitions, independent scaling, observability, and operator reconciliation procedures.

These phases are sequencing ideas, not release commitments.

## Tradeoffs and open questions

- Outbound pull/connection is friendlier to remote networks; manager-initiated APIs can offer faster dispatch but require reachable workers and more inbound security controls.
- A shared transactional store simplifies authoritative claims and fencing, but creates an availability and latency dependency. The storage technology and required consistency are unspecified.
- Job-level leases are simpler; finer-grained stage/session ownership may improve utilization but adds recovery complexity. Initial ownership granularity is open.
- The durable checkpoint format, safe-resume boundaries, external side-effect/idempotency contract, and criteria for automatic versus operator-approved recovery must be defined with Factory workflow semantics.
- Decide how long leases and heartbeats may be stale before declaring an attempt eligible for reconciliation; network partitions make fixed timeouts imperfect evidence.
- Decide how workers are identified across replacement/redeployment, how capabilities are negotiated, and how mixed protocol versions are upgraded safely.
- Define whether logs/artifacts are written directly to shared durable storage or through workers/manager, and their retention, size, privacy, and audit requirements.
- Clarify whether manager replicas share a scheduler, partition queues, or use another coordination model, and what behavior is acceptable during store or control-plane outages.
