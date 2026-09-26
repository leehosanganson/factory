# Factory roadmap

## Product direction

Factory is the durable workflow runtime. It owns sequential, multi-stage job execution and recovery, while supporting multiple independent agent sessions concurrently. Pi subagents complement Factory for focused, bounded work within or alongside a Factory job.

## 1. Containerized Factory server

Package Factory as a container and add a server mode that accepts jobs and manages agent sessions.

Key capabilities:

- Persist job definitions, stage state, events, logs, and cancellation requests independently of a server process.
- Run multiple sessions per instance with configurable concurrency and resource limits.
- Support horizontal scaling through shared durable storage and coordinated worker claims or leases, rather than instance-local state.
- Expose health and readiness, metrics, structured logs, and an authenticated API.
- Preserve current workflow semantics: sequential stages per job, bounded active agent time, and no overall job deadline.

**Milestone gate:** A job must survive worker restart and be safely recovered or classified when an instance disappears before fleet-wide orchestration is added.

## 2. `factory-control` fleet manager

Build a control plane that manages multiple Factory servers.

Responsibilities:

- Register instances and track health, capacity, configuration, and supported capabilities.
- Route or schedule work, avoid overloading instances, and drain instances for maintenance.
- Coordinate upgrades and configuration changes.
- Provide a single API for fleet-wide job discovery and control.

Execution remains in Factory instances; `factory-control` coordinates the fleet rather than duplicating workflow logic.

## 3. Scheduled issue-driven jobs

Add scheduled or event-driven sources that create Factory jobs from GitHub and Azure DevOps issues.

Initial scope:

- Filter by project or repository, labels or tags, and other configured criteria.
- Turn qualifying issues into durable job requests with source links and captured metadata.
- Deduplicate repeated events, handle provider rate limits, and record why an issue was accepted or skipped.
- Start with polling if operationally simplest; add webhooks once delivery, authentication, retries, and deduplication are robust.

## 4. Web application

Build the WebApp on Factory and `factory-control` APIs rather than giving it separate execution or persistence logic.

Core views and actions:

- Fleet health, capacity, and configuration.
- Jobs, stages, sessions, events, logs, and outcomes.
- Start, inspect, attach to, and stop jobs; expose human approval actions where workflows require them.
- Show issue provenance and the status of scheduled or provider-triggered work.

## Suggested rollout

1. Establish the server's persistence, worker ownership, recovery, and API contracts.
2. Validate one containerized instance running multiple concurrent jobs.
3. Add shared coordination and test horizontal scaling and instance failure.
4. Introduce `factory-control` for fleet operations.
5. Add GitHub and Azure DevOps issue ingestion and scheduling.
6. Deliver the WebApp over the stable control APIs.

The main architectural prerequisite is durable, shared job state and safe worker ownership. Fleet management, issue scheduling, and the WebApp all depend on that foundation.

## Status

This is a directional roadmap, not a commitment to specific release dates or a claim that these capabilities are currently implemented.
