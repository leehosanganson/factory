# REST API and security contract design

- **Type:** Self-improvement design session observation; not a user-reported finding.
- Verified PR #43 is merged; no merge action was taken here.
- Recorded `docs/issues/usability/define-rest-api-security-contract.md` before drafting the proposed API contract.
- Added `docs/roadmap/rest-api-contract.md` to define candidate request/response schemas, principal scoping, idempotency, bounded history, error handling, and OAuth-state behavior. Scope remains documentation-only; no route, OAuth, remote-admission, or worker code was added.
- User decisions for this contract: accept a GitHub issue with optional supplementary instruction text (not arbitrary task execution); use the minimum permission split as a candidate pending API verification; cap history at 100 events with a truncation indicator.
- External GitHub Docs were fetched directly for PR creation/update, Git ref creation, contents writes, and user-token permission intersection. The configured search service was unavailable, so the issue-read permission was not independently verified. Permission statements are explicitly provisional and require endpoint-by-endpoint verification before App registration/implementation.
- Next contract review should verify source-issue authorization separate from target-repository authorization, durability atomicity for request/ownership/idempotency, and the callback response/state details. No implementation is authorized by this planning artifact alone.
