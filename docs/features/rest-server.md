# REST job server (current behavior)

`factory server --config <absolute-path>` starts Factory's authenticated REST job service. It accepts bounded requests for operator-configured repositories, persists jobs using memory (default), SQLite, or PostgreSQL, executes the configured workflow, and exposes job status/history. Use the [strict configuration guide](rest-server-config.md) for the schema and [operations guide](rest-server-operations.md) for API behavior, persistence, recovery, capacity, and backup.

## Current execution topology

The all-in-one server starts local workers by default. Factory also supports two standalone modes: `factory worker --config <absolute-path>` connects directly to PostgreSQL, while `factory worker remote --config <absolute-path>` uses the separately enabled shared-secret HTTP Worker API and has no database connection. The remote HTTP API is a shipped Worker pull protocol, not a fleet coordinator or a claim that every part of the target Controller/Worker product is complete. The legacy direct-PostgreSQL mode remains supported. See [REST worker process](rest-worker.md), [server configuration](rest-server-config.md), and the [target Factory Worker MVP](../roadmap/factory-unit-mvp.md).

The [local Compose sample](compose-local-control-plane.md) starts PostgreSQL and the API by default, with no Worker unless its optional profile is selected. It is a development setup, not the target user-facing product deployment.

## Job outcome and recovery

Jobs use idempotent admission and bounded status/history. Cancellation is cooperative. With SQLite or PostgreSQL, queued jobs may resume after restart; interrupted running jobs are retained for investigation and are not automatically replayed. Memory state is lost on restart. When a GitHub provider is configured, the current service requires a confirmed PR create/update for success. This differs from the target product decision, where a successful configured harness result is sufficient and publication is optional.

## Optional current capabilities

The server also supports persistent recurring schedules, direct-request Threads, an in-product owner notification inbox, and an optional operator dashboard. Current schedules use five-field UTC cron and revision-guarded mutations; see the [configuration guide](rest-server-config.md) for current limits and [operations guide](rest-server-operations.md) for audit, access, and run-inspection behavior. Schedule firings create standalone Jobs, outside Threads. The dashboard is operator-focused and is not yet the target user-facing Thread/Job workbench. Optional GitHub issue poller and webhook adapters admit work into the same lifecycle; they are deferred from the target MVP. See [implemented features](README.md) and the individual integration guides.

## Trust boundary

The server executes configured harnesses and repository content with the service account's local authority. A process or container is not a security sandbox. Operators control repository roots, credentials, network access, and resource bounds. API requests cannot select arbitrary executables, shell commands, or filesystem paths. Factory does not merge, release, or deploy.

## Source and API reference

The current API is specified in the [OpenAPI specification](../openapi/rest-api-v1.json). Compatible API changes update the spec and info.version; incompatible API changes publish a new versioned document. For source navigation and process-level behavior, see the [operations guide](rest-server-operations.md). Current implementation must not be confused with the target architecture in the [roadmap](../roadmap/roadmap.md).
