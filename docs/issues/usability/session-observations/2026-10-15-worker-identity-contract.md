# Session observations: worker identity contract

- Continued from merged PR #269 and followed the documented dependency order: #258 is closed, so drafted the #259 worker identity/credential contract; #260/#261 remain dependent and open.
- Current PostgreSQL ownership code accepts worker/boot IDs as labels and uses a per-attempt opaque owner token for fencing. Documented that distinction and proposed future mTLS from trusted supervisors, while clearly marking authentication, remote workers, Store/runtime integration, and production support as not implemented.
- The contract covers principal mapping, boot/session binding, least privilege, issuance/rotation/revocation, failure behavior, harness secret exclusion, audit, tests, and dependencies. Independent review found no P1/P2 findings.
- `git diff --check` and local-link existence passed; commit attribution, Ubuntu, macOS, and PostgreSQL 18 integration CI passed for PR #270. No runtime changes. CI ran for this docs-only contract because it shared repository workflow jobs; no friction observed.
