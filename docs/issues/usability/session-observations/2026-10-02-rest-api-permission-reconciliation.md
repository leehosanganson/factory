# REST API permission evidence reconciliation

- **Type:** Documentation follow-up observation for the REST MVP design; not a user-reported finding.
- **What worked:** The credential-custody evidence now verifies the issue GET permission from GitHub's fine-grained permission map and endpoint docs, alongside Contents: write and Pull requests read/write requirements. Comparing that evidence with the REST API contract exposed an outdated paragraph that still said issue-read was unverified.
- **Scope status:** Updated only the REST contract's candidate permission summary and source references. Kept the set provisional and delegated user-token HTTPS Git transport explicitly unverified. No final App permission set, API behavior, or runtime code was selected or implemented.
- **Friction:** The design contract and evidence notes had been updated at different times, so the authoritative contract still contained a stale claim after the supporting evidence was confirmed. A focused status review after evidence changes can catch that inconsistency.
