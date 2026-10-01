# REST API wire-contract documentation session

- **Scope:** Specified a proposed HTTP wire contract in the roadmap from the user-approved decisions. No HTTP implementation is present or claimed.
- **Source inspected:** The merged Slice 1 configuration and Slice 2 `internal/restjobs` manager on `origin/main`, their merged PRs #67 and #68, plus open repository-root-validation PR #69. Slice 2 defines the job status/event vocabulary, status DTO fields, bounded event shape, positive issue validation, idempotency/capacity errors, and history truncation. Slice 1 defaults the listener to `127.0.0.1:8080`.
- **Contract choices documented:** Shared bearer-key trust across holders, loopback default, unauthenticated minimal health/readiness probes, submitted task visibility in job status, exact methods/paths, representative JSON DTOs, and stable generic error envelope/status mapping. Local subprocesses are not a sandbox; PAT, push, and PR operations remain deferred pending restart-safe reconciliation.
- **Friction:** None observed during this focused docs change. This is a session observation, not a user-reported finding.
