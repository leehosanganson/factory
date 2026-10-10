# Session observation: REST job listing (#221)

- **Type:** Implementation session; not a user-reported finding.
- **Evidence:** On refreshed `main` at `b408358`, route/OpenAPI inspection confirmed job IDs could not be rediscovered by collection listing. Implemented and verified a bounded authenticated listing against memory and SQLite contract tests and a REST server process test.
- **What worked:** Shared store-contract tests exercised both persistence backends with the same ordering, snapshot cursor, and payload omission expectations.
- **Friction observed:** Process E2E setup required a private SQLite parent directory and existing runtime fixture helpers; initial fixture assumptions did not match these constraints and needed correction before the focused process test passed.
- **Idea:** Continue reusing established process fixtures and helper APIs when adding REST-process coverage to reduce test setup mismatch.
- **Verification:** Focused restjobs/restapi/runtime tests and `make test`, `make vet`, `make build` passed on the final code state before this note; `git diff --check` passed.
