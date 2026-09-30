# Session observation: queued GitHub issue inspection

- **Type:** implementation session observation; not a user-reported usability finding.
- **Scope:** explicit one-shot inspection of the GitHub issue referenced by an existing queued work request.
- **What worked:** the durable request supplies the repository and issue reference, while an injected `IssueTracker` keeps the network read testable. Missing requests and unsupported trackers are rejected before any tracker call. The command displays the queued identity and a single snapshot without changing the stored item. Terminal control and multiline content in the untrusted issue title is sanitized before display.
- **Limitations:** this is not issue polling, durable observation history, a worker, or an issue/PR write path. No authenticated live GitHub request was made during implementation tests.
