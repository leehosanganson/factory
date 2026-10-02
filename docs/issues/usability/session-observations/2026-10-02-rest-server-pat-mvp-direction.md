# Superseded REST server direction record

- **Status:** Historical decision record; superseded by the unified REST job-service direction in the [roadmap](../../../roadmap/roadmap.md) and [REST job contract](../../../roadmap/rest-api-contract.md).
- **Earlier direction:** A single-host REST service with a shared bearer key and shared GitHub PAT, PostgreSQL job metadata, persistent workspaces, and guarded branch/PR publication. OAuth custody was deferred. These notes record an intermediate decision and do not define the current MVP.
- **Current direction:** One REST job lifecycle, with memory as the simple volatile starter backend and optional/recommended SQL for restart durability. Creating/updating a PR through the configured code-repository provider is required for an implementation job to succeed. Provider/credential selection and durable recovery semantics must be designed against that contract.
- **Retained caution:** Process and agent memory cannot be recovered; timeouts or interruptions around external writes require reconciliation rather than blind replay. A local process/container is not a security sandbox.
- **Historical scope:** No server runtime, SQL state, PAT support, or provider PR behavior was implemented by the documentation change originally recorded here.

Use the canonical roadmap and contract for current product guidance; preserve this note only as historical provenance.