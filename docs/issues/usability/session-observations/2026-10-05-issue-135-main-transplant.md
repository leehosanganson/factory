# Issue #135 main-branch transplant

- **What worked:** The two existing commits transplanted in order onto the verified `origin/main` base. Upstream GitHub provider tests and the live workflow's response-loss transport test were preserved when resolving the overlapping test-file conflict.
- **Verification:** Credential-free focused tests, `make test` with `umask 0000`, `make vet`, `make build`, and `git diff --check` passed. Both environment-gated live workflow tests skipped with all `FACTORY_LIVE_GITHUB_*` variables unset. No provider access or live validation was performed.
- **Friction:** The default shell umask (`0007`) caused an unrelated existing filesystem-permission test to fail because a requested `0755` directory was created as `0750`; rerunning the suite with a neutral umask passed. The Makefile has no `help` target.
