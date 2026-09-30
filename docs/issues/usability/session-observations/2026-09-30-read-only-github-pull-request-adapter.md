# Session observation: read-only GitHub pull request adapter

- **Type:** implementation session observation; not a user-reported usability finding.
- **Scope:** provider-neutral code-host/PR snapshot contract and read-only GitHub CLI-backed implementation.
- **What worked:** reusing the repository validation and `gh api --method GET` approach kept this adapter independent from `IssueTracker` while allowing local tests to inject provider responses and verify that invalid references make no command call.
- **Evidence:** adapter tests check PR field mapping for open and closed states, version changes, invalid references, API/JSON failures, mismatched PR identity and URL, and the exact GET-only command arguments.
- **Limitations:** this is an internal read-only snapshot adapter. It is not wired to a CLI command or worker and does not poll, create branches, or create/update PRs. No interactive Factory CLI usability audit was performed.
