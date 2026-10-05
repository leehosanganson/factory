# Detached published PR handoff

- Added a detached implementation CLI process test that publishes its generated branch only to a temporary local bare Git remote; a local `gh` stub returns a deterministic PR URL. A fresh `factory job get <id> --details` process displayed the persisted `complete` status, `published` state, and exact URL.
- Added a second detached scenario where the fake `gh` fails. The durable record and fresh details output report `unpublished` and do not contain a PR URL. The failure output includes recovery instructions, so the stub was kept free of URL text to avoid confusing echoed subprocess diagnostics with a confirmed PR.
- Both paths leave invoking checkout HEAD/status unchanged. Successful publication removes the implementation worktree while preserving the pushed task branch in the local bare remote. Provider credential environment variables are removed from the tested processes; no live provider is called.
- Focused process test, `umask 022 make test`, `make vet`, `make build`, and `git diff --check` passed. No production behavior change was needed; existing persistence and details rendering satisfy the tested handoff.
