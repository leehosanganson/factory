# Issue #135 workflow secret scope

- **Scope:** Removed job-level live-test environment inheritance; configured the three App credentials only on the mint-token step and explicit non-secret sandbox identity values only on mint/live/cleanup steps. Minting remains a single step; its existing target loader validates before network access.
- **Verification:** `actionlint` passed for the workflow; focused live-provider tests, `make test`, `make vet`, `make build`, and `git diff --check` passed. No live GitHub calls were made.
- **Friction:** `make help` is not implemented and the initial Nix invocation required enabling `nix-command`/`flakes`; rerunning with those features enabled provided actionlint. No product behavior was exercised.
