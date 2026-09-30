# Session observation: explicit work issue refresh and history

- **Type:** implementation session observation; not a user-reported usability finding.
- **Scope:** one-shot GitHub issue refresh, durable versioned observation history, and history display for queued work requests.
- **What worked:** the existing queued-request lookup and injected `IssueTracker` provide the one-read flow; immutable version-keyed records make repeated refresh of the same snapshot idempotent while retaining distinct versions. The queue remains an independent record and is not changed by refresh/history.
- **Verification evidence:** targeted store and command tests exercise reopen, duplicate and changed versions, permissions, symlink/corrupt-record rejection, preflight failures, fetch failure, history, and unchanged queue state. No live authenticated GitHub request was made.
- **Limitations:** refresh is explicitly invoked and performs one fetch; no polling, background worker, issue write, or PR operation is added.
