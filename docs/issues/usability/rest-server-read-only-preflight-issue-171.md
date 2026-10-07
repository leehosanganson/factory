# Read-only REST server preflight — issue #171

- **Date:** 2026-10-07
- **Status:** Closed; implemented in [PR #172](https://github.com/leehosanganson/factory/pull/172), merged to `main` as `36709196134d757c6a12c9559702d040d4b84617`.
- **Report (user-reported):** [Issue #171](https://github.com/leehosanganson/factory/issues/171) noted that operators had no read-only command to validate the separate REST server configuration and its local requirements before starting the service. The existing `factory doctor` checks the regular CLI configuration, not server-only JSON.
- **Verified need and impact:** Server configuration errors and unavailable local prerequisites otherwise surfaced during startup. A local-only preflight lets operators inspect setup without launching service resources; it is not a provider reachability or security check.
- **Desired outcome:** Provide a bounded, read-only preflight for strict REST server configuration and local prerequisites, with actionable diagnostics that do not expose configured paths or values.
- **Acceptance criteria:**
  1. Provide `factory server doctor --config <absolute-path>` using strict server-config validation.
  2. Check local API-key/provider-token files, SQLite path metadata, configured Git repository roots, and harness executable availability without provider network calls.
  3. Do not open or create a database, start a listener, run a workflow, or change config, state, or repository contents.
  4. Redact configured paths and secret values; document scope and non-goals; cover behavior with process tests.
- **Outcome and evidence:** Main implements the command and documents its local-only scope in the [REST server guide](../../features/rest-server.md) and [configuration reference](../../features/rest-server-config.md). It checks strict config and local prerequisites; diagnostics name fields/categories only. Provider reachability is not tested, and no database is opened/created, listener started, or workflow run. The process test `TestServerDoctorProcessIsLocalOnlyAndReadOnly` passed locally on 2026-10-07. PR #172 records `make test`, `make build`, `make vet`, and `git diff --check`; its Ubuntu and macOS CI checks both passed. No live provider validation was performed.
