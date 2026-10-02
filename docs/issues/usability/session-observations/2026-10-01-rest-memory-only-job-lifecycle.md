# Superseded REST persistence decision

- **Status:** Historical decision record; superseded by the unified REST job-service direction in the [roadmap](../../../roadmap/roadmap.md) and [REST job contract](../../../roadmap/rest-api-contract.md).
- **Earlier direction:** Keep accepted jobs, status, idempotency data, and history in process memory, accepting loss on restart; stop admission and cancel active jobs on graceful shutdown. This describes an intermediate decision, not the current persistence target.
- **Current direction:** Memory remains the simple volatile starter backend for local development/tests. Optional SQL persistence is recommended when restart durability is needed; the target contract must define durable admission, lifecycle/history, ownership, recovery, and fail-closed behavior. A configured durable backend must not silently fall back to memory.
- **Other retained decision:** Clients submit bounded task jobs over HTTP and Factory owns scheduling/execution. Credentials, shell commands, and executable selection remain operator-controlled, not caller-supplied. Issue context does not imply separate issue-polling automation.
- **Historical implementation note:** The current local REST runtime uses memory-backed job state. SQL persistence and provider PR create/update remain target-MVP gaps. Earlier PostgreSQL/migration experiments mentioned in the original note were provisional and did not establish the current schema or backend contract.

Use the canonical roadmap and contract for current guidance; preserve this file only as provenance for the earlier memory-only direction.
