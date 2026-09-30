# Session observation: foreground issue watch

- **Type:** implementation session observation; not a user-reported usability finding.
- **Scope:** `factory work watch` foreground polling for queued GitHub issue requests.
- **What worked:** the existing injected issue tracker and observation-store contracts support immediate snapshot reads and version-deduplicated persistence without adding queue claim behavior. A wait seam makes update, closure, duplicate, and cancellation paths testable without wall-clock delays.
- **Verification evidence:** focused unit tests cover open updates followed by closure, duplicate versions, cancellation, provider/store errors, duration parsing, and missing/unsupported request preflight. CLI integration exercises polling through a fake `gh` executable.
- **Limitations:** the command is opt-in foreground polling only. No autonomous worker, PR engineering, or GitHub writes are included.
