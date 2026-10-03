# REST operator reconciliation session observation

- **Scope:** Implemented the explicit authenticated action for reconciling an uncertain provider result on an eligible failed SQLite REST job.
- **Evidence:** Focused REST packages passed; the process-level SQLite/fake-provider E2E passed repeatedly, including restart without replay, rejection paths, idempotent success, and stable provider/harness operation counts.
- **What worked:** The preexisting process-level uncertainty test already provided the lost-response, durable-workspace, and restart fixture. Extending it through the real HTTP route exercised the product behavior end-to-end without live credentials or GitHub.
- **Friction:** The previous fake provider's `Publish` retry could prove lookup behavior but could not prove the server persisted operator reconciliation or kept the job result inspectable. The test now drives the authenticated endpoint and records the harness invocation count.
- **Boundary:** This observation records repository work, not a user-reported issue. The E2E uses a deterministic fake provider and does not establish live GitHub behavior or automatic startup recovery.
