# Session observation: durable work queue MVP

- **Type:** implementation session observation; not a user-reported usability finding.
- **Scope exercised:** local durable queue contracts, enqueue, listing, claiming, recovery, and fencing.
- **Evidence:** the implementation was tested directly through Go tests; no CLI surface exists for this queue slice, and no Factory usability audit was performed.
- **What worked:** existing `JobStore` patterns supplied reusable conventions for atomic JSON replacement, directory checks, and OS-backed file locks. A held per-request OS lock let another queue instance distinguish a live claimant from an abandoned claim without relying on an age timeout; generations provide a checkable stale-token boundary.
- **Limitations:** the queue is intentionally internal and has no provider adapters, worker, CLI/server intake, or operator-facing recovery interface in this slice. Its single-host lock semantics do not claim multi-host safety.
