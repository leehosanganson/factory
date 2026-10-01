# REST API and security contract design

- **Type:** Self-improvement design session observation; not a user-reported finding.
- Verified PR #43 is merged; no merge action was taken here.
- Recorded `docs/issues/usability/define-rest-api-security-contract.md` before drafting the proposed API contract.
- Added `docs/roadmap/rest-api-contract.md` to define candidate request/response schemas, principal scoping, idempotency, bounded history, error handling, and OAuth-state behavior. Scope remains documentation-only; no route, OAuth, remote-admission, or worker code was added.
- User decisions for this contract: accept a GitHub issue with optional supplementary instruction text (not arbitrary task execution); use the minimum permission split as a candidate pending API verification; cap history at 100 events with a truncation indicator.
- External GitHub Docs were fetched directly for PR creation/update, Git ref creation, contents writes, and user-token permission intersection. The configured search service was unavailable, so the issue-read permission was not independently verified. Permission statements are explicitly provisional and require endpoint-by-endpoint verification before App registration/implementation.
- Extended the contract to separate source-issue and target-repository authorization, define atomic idempotency/ownership admission, specify state TTL/one-time consumption and failure recovery, and describe external-key AES-GCM custody/rotation and explicit relink/revoke behavior. These remain design constraints, not implemented behavior.
- No implementation is authorized by this planning artifact alone.
