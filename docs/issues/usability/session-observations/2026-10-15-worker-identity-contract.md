# Session observations: worker identity contract

- Continued from merged PR #269 and followed the documented dependency order: #258 is closed, so drafted the #259 worker identity/credential contract; #260/#261 remain dependent and open.
- Current PostgreSQL ownership code accepts worker/boot IDs as labels and uses a per-attempt opaque owner token for fencing. Documented that distinction and proposed future mTLS from trusted supervisors, while clearly marking authentication, remote workers, Store/runtime integration, and production support as not implemented.
- The contract covers principal mapping, boot/session binding, least privilege, issuance/rotation/revocation, failure behavior, harness secret exclusion, audit, tests, and dependencies. Independent review found no P1/P2 findings.
- Local docs checks (`git diff --check`, local-link existence) remain to run before PR. No runtime changes or usability friction observed.
