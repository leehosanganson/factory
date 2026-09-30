# Session observation: read-only GitHub issue tracker

- **Type:** implementation session observation; not a user-reported usability finding.
- **Scope:** provider-neutral issue snapshot contract and read-only GitHub CLI-backed implementation.
- **What worked:** isolating tracker reads from the existing queue and PR publication/monitor paths kept `factory work submit/list/get` behavior unchanged. An injectable command boundary made open/closed state, updated snapshot versions, invalid references, API errors, and PR detection testable without GitHub credentials.
- **Evidence:** focused tracker tests and the repository test, vet, build, and diff checks were run for this slice.
- **Limitations:** the adapter fetches a snapshot only; it does not poll, persist observations, start a worker, or perform issue/PR writes. No interactive Factory CLI usability audit was performed.
