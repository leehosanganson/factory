# Factory roadmap

## Direction and status

Factory's long-term direction is to manage a durable feedback loop that turns intent into deployed, improved software:

**intent → requirements → implementation → review → documentation → CI/CD → deploy → feedback → intent**

The loop should retain context, decisions, evidence, and outcomes at each transition. Feedback from checks, deployments, users, and operations should refine requirements and future work rather than being discarded at job completion. Human direction remains essential for ambiguity, risk, and approval; automation should not be presented as an independent correctness verdict.

## REST server MVP foundation

The agreed direction is a containerized, single-host REST service with durable local state, a per-principal bearer credential linked to a GitHub App user grant, and principal-scoped work/history. Repository access must be available to both the user and App installation; merge, release, and deployment remain human-only. This server is not implemented. See the [proposed API/security contract](rest-api-contract.md) for route schemas and security boundaries, and the [credential-custody design](credential-custody-design.md) for OAuth, encrypted grant storage, refresh, and rotation gates.

Today Factory is a Go CLI with sequential implementation stages, a repository-wide tidy workflow, detached PR monitoring, detached jobs, and opt-in parallel implementation within an eligible implementation stage. It does not currently provide a server, fleet coordinator, issue scheduler, deployment engine, generic arbitrary-command `run`, reusable workflow/task catalog, or chained jobs. The [feature index](../features/README.md) describes implemented behavior.

This is a directional roadmap, not a commitment to specific features, release dates, or ordering. The capabilities below are aspirations and require design, validation, and explicit product decisions before implementation.

## Reusable tasks and composed workflows

A future system may support reusable, versioned task definitions and workflows rather than hard-coding each use case. Reusable units could describe inputs, prerequisites, outputs, safety limits, approvals, verification, and provenance. Composition should preserve clear ownership and auditability: a combined workflow must show which task ran, what evidence it produced, and why the next action was permitted.

The existing `implement`, `tidy`, and `monitor` workflows are distinct current CLI behavior. A future `run` concept might execute a user-defined workflow, and multiple jobs and job chains might pass explicit results or artifacts between work. These are not current generic runner or chaining capabilities. Design must define dependency handling, cancellation propagation, retries, resource limits, and how partial outcomes are represented before treating composition as safe.

## Full feedback lifecycle

The intended lifecycle spans requirements through deployment and back to new intent:

1. Capture user intent and source evidence.
2. Refine and approve requirements, constraints, and acceptance criteria.
3. Implement bounded changes, sequentially or through validated independent work.
4. Review changes and verify them with appropriate tests and policy checks.
5. Update documentation based on verified implementation.
6. Run CI/CD and deployment through explicitly integrated systems.
7. Observe deployment and operational/user feedback.
8. Feed evidence and unresolved issues into the next intent/requirements cycle.

Factory's present implementation workflow covers requirements, implementation, review, and documentation; tidy covers review/fix/document and local checks; monitor handles bounded existing-PR maintenance. CI/release automation lives in repository GitHub workflows. End-to-end deployment and feedback orchestration are future direction, not present Factory behavior.

The feedback loop should distinguish observations from inferred causes, preserve links to source evidence, and make retries and human decisions visible. It should also allow a workflow to stop safely when evidence is missing or scope changes.

## REST server MVP contract

The agreed REST MVP is a containerized, single-host service with persistent local state, per-principal bearer authentication, and a linked GitHub App user grant. Remote work and history are scoped to the submitting principal; repository access requires both that user's access and the App installation. The worker may prepare/update a PR but cannot merge, release, or deploy. The server is not implemented; the separate REST design worktree contains the detailed MVP plan, while the tracked [proposed API/security contract](rest-api-contract.md) and [credential-custody design](credential-custody-design.md) record reviewable schemas, security boundaries, and implementation gates.

## Durable Factory servers and horizontal scaling

A future Factory server could accept workflow requests and run multiple independent jobs with configured concurrency and resource limits. Horizontal scaling requires durable job definitions, stage state, events, logs/artifact references, cancellation, worker ownership, and safe recovery independent of one process. Sequential stages within a job should remain distinct from parallel jobs or task waves.

Before enabling reassignment across instances, prove restart recovery and define durable checkpoints, leases/fencing, idempotency, and handling of uncertain external side effects. A lease expiring cannot prove a paused worker has stopped making changes. Safe resume should require a validated checkpoint or operator reconciliation rather than blind replay.

Factory instances could run on local infrastructure or remote hosts and scale independently. A shared durable coordination boundary would be needed for job discovery, admission, ownership, and recovery; local state alone cannot coordinate a fleet. Containerization may be one deployment option but is not sufficient isolation or a substitute for external lifecycle management.

See the separate [fleet manager design note](fleet-manager-design.md) for an exploratory topology, authority boundaries, Nix environment considerations, trust assumptions, and open decisions. It is a design proposal, not an implementation specification or commitment.

## Fleet coordination

A future `factory-control` service could register Factory servers, track health/capacity/capabilities, schedule work, drain instances, coordinate configuration/upgrades, and expose fleet-wide job discovery and control. Execution should remain in Factory workers; the coordinator should not duplicate workflow stage logic or create a competing retry loop.

Any fleet design depends on durable shared job state and safe worker ownership first. Worker identity, authenticated outbound communication, store consistency, recovery rules, and treatment of uncertain side effects need explicit answers before production use.

## Possible future integrations

- **Autonomous issue-to-PR engineering:** the [issue-to-PR design](autonomous-issue-to-pr-design.md) records the agreed GitHub Issues + GitHub PR MVP, separate provider-neutral tracker/code-host interfaces, durable local queue and shared worker path, continuous issue reconciliation, safety requirements, and ordered implementation gates. A read-only GitHub Issues snapshot adapter implements the tracker contract; explicit `factory work refresh/history` commands record and display durable observations, and `factory work watch` offers opt-in foreground polling with a durable baseline / `waiting_for_human` / terminal `stopped` lifecycle until closure. Polling and recording continue while waiting for a human; lifecycle recovery derives state from immutable version history after a crash. This watch is not an autonomous worker and does not reconcile requirements or restart engineering. `factory work respond` can persist version-pinned human direction for an exactly reconciled `waiting_for_human` request, and `factory work directions` inspects it; these commands do not approve, resume, or initiate any work. A separate read-only GitHub PR snapshot adapter implements the code-host contract, but is not connected to a worker or CLI; branch/PR writes, worker lifecycle, and autonomous continuous reconciliation remain unimplemented. Azure DevOps (both Azure Boards and Azure Repos PRs), server hosting, provider events, and an external distributed broker are outside MVP scope. The full system remains future direction, not implemented behavior or a dated commitment.
- **CI/CD and deployment:** integrations could carry verified artifacts and approvals across CI and deployment systems, then record deployment outcomes. Factory does not currently deploy software.
- **Web application:** a UI could build on stable APIs for instances, workflows, jobs, stages, logs, approvals, provenance, and outcomes. It should not introduce a separate execution or persistence authority.

## Suggested sequencing and feedback

1. Validate durable server-side job state, worker ownership, and recovery on a single instance.
2. Demonstrate multiple concurrent jobs on that instance with explicit resource bounds and traceable stage outcomes.
3. Add container or other deployment packaging and validate restart behavior without equating process restart with safe replay.
4. Introduce shared coordination and test horizontal scaling, partitions, stale workers, and reconciliation.
5. Add fleet control only after worker contracts and ownership are proven.
6. Explore reusable task definitions, job composition/chains, CI/CD/deployment feedback, and a web interface against the same durable APIs.

Issue-to-PR automation has a separate proposed sequence in the [autonomous issue-to-PR design](autonomous-issue-to-pr-design.md). Its GitHub MVP is based on a durable single-host queue and does not depend on server or fleet milestones above.

At each step, use observed user and operational feedback to revisit intent, document evidence and friction, and adjust the direction. These stages are a planning aid, not a schedule or promise; sequence and scope may change as evidence and product priorities evolve.
