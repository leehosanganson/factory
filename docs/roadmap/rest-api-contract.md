# REST Job and lifecycle contract

## Status and authority

This document defines the **target REST contract** for Factory's Controller/Worker product. It is normative for target behavior, not a description of the current server or a delivery-date promise. The [Factory Worker MVP](factory-unit-mvp.md) defines the product scope and architecture; this document defines the Job lifecycle and its safety requirements.

Factory's current API and OpenAPI specification describe the **shipped routes**. Use the [implemented feature index](../features/README.md), [REST server guide](../features/rest-server.md), and [REST server operations guide](../features/rest-server-operations.md) for current route, configuration, persistence, and recovery behavior. Do not infer target routes or protocol details from those materials.

The target execution path is:

**request → durable admission → Controller assignment → bounded Worker execution → inspectable outcome and notification**

A Job succeeds when its selected harness reports a successful result under Factory's bounded execution rules. Provider publication, including branch or pull request publication, is optional and is not a success condition. Factory does not merge, release, or deploy.

## Components and authority

The target service has a Controller, PostgreSQL, separate Worker clients, and a required browser UI. PostgreSQL is the durable store shared by the Controller. Workers contact the Controller; they do not use shared PostgreSQL job ownership as the target Worker interface.

The Controller is authoritative for user access, Threads, Jobs, recurring schedule definitions and firings, Worker assignment, cancellation requests, notifications, and persisted outcomes. A Worker accepts assigned work from the Controller, runs it locally, and reports bounded progress and results. The initial Worker authentication mechanism is an operator-configured shared secret. Do not expand this MVP contract to require mutual TLS (mTLS), per-Worker identity, registration/session/capability management, or a credential lifecycle.

Each Worker has locally configured harness choices and selects **one** harness for each assigned Job. The caller, Thread, schedule, and Job request do not select a Worker, harness, executable, repository path, credential, or host authority. Multi-harness collaboration within one Job and remote Worker or harness provisioning are outside this contract.

The required browser UI supports the user-facing Thread/Job workflow and operator management of recurring schedules. It must present inspectable Job status and outcome, and notifications. This contract does not prescribe UI layout or additional schedule-management details.

## Users, Threads, and Jobs

The Controller enforces access and ownership for user work. Users can create and inspect their own Threads and the Jobs and outcomes in them. One user's access must not expose another user's private request or result. Operators have the authority needed to manage the service and recurring schedules; operator access does not make a Thread a shared execution queue.

A **Job** is the single canonical unit of admission, assignment, execution, cancellation, and outcome. Thread membership does not create a second Job lifecycle or queue. A **Thread** groups related user work and Jobs and presents their derived status; it is not an operating-system thread or an execution attempt. Thread completion is explicit and does not itself change member Job outcomes. A completion notification is an in-product notification, not a promise of email, chat, or push delivery.

A recurring schedule creates ordinary, standalone Jobs through the same canonical lifecycle. Scheduled Jobs remain outside Threads unless a future product decision changes this rule. A schedule is a source of Job admission, not a separate execution system and not a way to choose a Worker or harness. The Controller owns schedule definitions and firing; operators manage them in the required browser UI. This contract requires recurring schedules but does not define schedule screens, recurrence syntax, missed-run policy, or route details.

When all Jobs in a Thread reach outcomes, the Controller makes the resulting status and outcomes available to its owner and creates an in-product notification. Notifications are inspectable state in the browser workflow, not an external delivery integration. A Thread's aggregate status is derived from its Jobs; explicitly completing a Thread is a separate user action and does not rewrite, cancel, or otherwise change those Jobs. This keeps the grouping and notification model separate from Job execution.

## Admission and request boundaries

The Controller validates and records a Job before it makes the Job eligible for assignment. Admission is idempotent: a retry of the same admission identity and same normalized request returns or identifies the original Job, rather than creating duplicate work. Reuse of that identity with different request content is rejected. Do not promise exactly-once execution or external side effects from idempotent admission.

Requests contain bounded user work, not host instructions. The Controller accepts only configured repository and execution choices. Callers cannot supply arbitrary filesystem paths, executables, shell commands, credentials, or host authority. External text and repository content remain untrusted input; they cannot override operator policy or credential boundaries. The Controller owns each admitted Job and its association with the requesting user's Thread. A user can inspect their own work and outcome, but cannot use an identifier or retry to gain access to another user's private work. Service operators manage service-level configuration and schedules; the API's exact role names, authentication fields, route shapes, and error codes are outside this target contract.

The service applies configured bounds to request size, task/input size, retained history, concurrent work, execution time, and captured output. Limits must apply across the components that own the relevant work, and overload must not silently bypass them. Persist enough bounded status, event, result, and limitation information for a user to inspect the outcome and an operator to investigate an interrupted or failed Job. Do not expose secrets, host paths, or unbounded process output as ordinary API history. Results should make clear whether the harness reported success, failure, cancellation, or unresolved interruption; they must not imply independent correctness verification when none was performed. Bounded progress may aid inspection but does not replace the final outcome.

## Execution, cancellation, and recovery

Each Job follows one lifecycle: the Controller admits it, makes it eligible for assignment, assigns it to a Worker, records execution progress, and persists its outcome. A successful harness result, failure, cancellation, or unresolved interruption is an outcome of that same Job, not a new kind of work item or separate queue. The Controller assigns admitted Jobs to separately running Workers. Workers execute with locally configured harnesses under the operator's local account authority and report their result to the Controller. The Controller records the outcome for inspection. This contract does not specify an HTTP transport, Worker message schema, lease fields, heartbeat protocol, or assignment/reassignment algorithm.

Cancellation is cooperative. The Controller can request cancellation of a queued or running Job, but a request is not proof that a process has stopped, a child process has exited, or an external side effect has been undone. A running Job must not be reported as canceled or complete until its execution has reached a safely recorded outcome. Preserve uncertainty rather than claiming success based only on a Worker disconnect or cancellation request.

An interrupted or uncertain Job remains persisted and inspectable for operator investigation. Do not automatically replay it or assign it to another Worker merely because a Worker is disconnected, unavailable, or has a stale heartbeat. A missing heartbeat does not prove the original process stopped. This contract does not define an automatic recovery, lease-expiry, replay, or reassignment rule; any such rule requires a separate safe product decision.

Persist enough information to distinguish a reported successful harness result from failure, cancellation, and unresolved interruption, and to retain the evidence and limitations needed for inspection. PostgreSQL provides durable shared state, not automatic replay, process checkpointing, high availability, failover, or a guarantee that all external effects can be reconstructed after a failure.

## Trust and non-goals

Factory does not provide a sandbox or security-isolation guarantee. A process, worktree, container, or harness can act with the authority available to its local account. Operators are responsible for trusted repositories and harnesses, credentials, network exposure, host permissions, and deployment-specific controls. Bounds and cooperative cancellation are resource and lifecycle controls, not containment of hostile code.

The target is a practical Controller/Worker deployment, not a fleet manager or high-availability platform. It makes no promise of redundant Controllers, database failover, automated Worker replacement, zero downtime, or safe exactly-once side effects. It does not merge, release, or deploy changes.

## Current implementation boundary

The repository ships parts of the target alongside older execution modes. The following describes current behavior, not additional target requirements:

- Current server persistence can use memory, SQLite, or PostgreSQL. Memory is volatile; configured SQL fails closed when unavailable. PostgreSQL supports an API control plane with in-process workers disabled. All-in-one server workers remain available, and there is no fleet coordinator.
- The standalone HTTP Worker polls the PostgreSQL-backed Controller's opt-in, shared-secret Worker API for assignments and reports heartbeats and outcomes without a database connection. The [default Compose stack](../features/compose-local-control-plane.md) uses this path. The legacy standalone Worker instead claims Jobs directly through PostgreSQL; its Controller sessions and advisory heartbeats do not assign Jobs.
- The [browser UI](../features/rest-web-ui.md) ships a separate owner workbench for creating Threads with Jobs, sending follow-ups, inspecting owned outcomes, and reading completion notices. Operators can create, edit, pause, resume, and remove recurring schedules and inspect recent runs. See the feature guide for current limits and access rules.
- In-process server workers and legacy direct-PostgreSQL Workers require confirmed pull request creation or update for success when a GitHub publication provider is configured. The standalone HTTP Worker does not publish pull requests. The target success rule does not require publication.
- The issue poller, GitHub issue webhook, and local CLI are current capabilities or workflows, but are deferred from the target MVP as user-facing intake paths.

For shipped routes and implementation details, use [REST server](../features/rest-server.md), [REST server operations](../features/rest-server-operations.md), [REST worker process](../features/rest-worker.md), and the [implemented feature index](../features/README.md). The [OpenAPI specification](../openapi/rest-api-v1.json) describes the current API, not an unimplemented target route set. Detailed current recovery and provider behavior is covered by the [Worker recovery and cancellation contract](worker-recovery-cancellation-contract.md) and [attempt/workspace/provider-write contract](attempt-workspace-provider-write-contract.md); neither changes this target contract.
