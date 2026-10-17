# Session observation: REST API smoke validation

- **Verified:** `factory server doctor` accepted a fresh temporary strict config containing a protected shared API key, enabled operator verifier, SQLite state, exact temporary Git checkout, fake agent, and one configured check.
- **Verified:** a real `factory server` process with that config returned successful liveness/readiness responses; authenticated submission was admitted and job detail, history, and listing exposed the terminal result. The configured check was recorded passed, history moved through queued/running/succeeded, and the startup log named SQLite without printing its path or credentials.
- **Test boundary:** the agent was a synthetic executable that exited successfully; no Pi coding-agent stage or GitHub provider was configured. This validates local REST API lifecycle wiring only, not coding quality or PR publication. Temporary repository, credentials, config, state, and harness were removed after the run.
- **Tooling:** initial inspection found the installed CLI binary but no standalone `factory` on `PATH`; the local build at `./bin/factory` worked for preflight and server invocation.
- **Review:** an independent read-only evaluator found no actionable documentation accuracy or scope issues. `git diff --check` passed.
