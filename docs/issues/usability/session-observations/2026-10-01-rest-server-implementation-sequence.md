# Superseded REST implementation sequence

- **Status:** Historical session observation; the described process-local/no-PostgreSQL direction was superseded by the unified REST job-service direction. See the [roadmap](../../../roadmap/roadmap.md) and [REST job contract](../../../roadmap/rest-api-contract.md).
- **Earlier sequence:** Server configuration, in-memory job manager, HTTP routes, isolated workspaces, workflow integration, shared-PAT PR publication, then local Compose/E2E. The note recorded volatile jobs, no PostgreSQL, guarded publication, and no merge/release/deploy.
- **Current direction:** Keep memory as a simple volatile starter. Optional SQL persistence is recommended for restart durability. Creating/updating a PR through the configured code-repository provider is required for a target-MVP implementation job to succeed. Provider choice, credentials, durable recovery, and uncertain-write reconciliation must follow the current canonical contract.
- **Historical status:** The earlier note reported an intermediate decision and implementation state; it is not a current feature status or delivery plan.

Preserve this file only as decision history. Do not use it to guide current implementation.