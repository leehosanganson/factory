# Issue #135 GitHub App live-test token scope

- **What worked:** The production GitHub publisher already supports an injectable HTTP transport and read-only PR reconciliation after an ambiguous create response. Focused local tests exercise one forwarded create, discarded successful response, actual response identity checks, and refusal of a second POST.
- **Security review:** The first workflow draft minted credentials in the live and cleanup jobs separately. Review moved token minting into one dedicated step and kept the private key out of both Go test processes. Both tests now use the pre-minted masked installation token through `LoadProviderConfig`.
- **Verification:** Focused tests, complete `make test`, `make vet`, `make build`, YAML parsing, and staged diff checks passed. No live calls were made because #134 sandbox provisioning is incomplete.
- **Friction:** `make help` is not defined, `uv` Python failed to launch in this NixOS environment, and no YAML parser was exposed on PATH. YAML syntax was checked using an installed Nix Python and PyYAML package instead.
