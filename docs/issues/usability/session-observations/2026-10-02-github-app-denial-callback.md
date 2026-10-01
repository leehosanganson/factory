# GitHub App web-flow denial callback evidence boundary

- **Type:** Evidence-based OAuth documentation clarification; not a user-reported finding.
- **Evidence:** GitHub's GitHub App web application flow describes the callback redirect when authorization is accepted, but does not specify a callback request or error parameters for denial. The same page documents `access_denied` only for Device Flow, which must not be projected onto web flow.
- **Scope status:** Clarified this evidence boundary and preserved denial/provider-error handling as an open design and validation gate. No denial behavior or runtime logic was selected or implemented; OAuth App documentation was not used to fill the gap.
- **Reference:** [Generating a user access token for a GitHub App](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-a-user-access-token-for-a-github-app).
- **Verification:** Fetched the official GitHub App-specific flow documentation, checked local links, and ran `git diff --check`.
