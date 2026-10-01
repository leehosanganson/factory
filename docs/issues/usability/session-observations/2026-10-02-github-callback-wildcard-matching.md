# GitHub App callback wildcard-matching gate

- **Type:** Evidence-based OAuth deployment-security clarification; not a user-reported finding.
- **Evidence:** GitHub's GitHub App callback URL documentation says wildcard matching may allow subdomains and subdirectory paths, warns that this can expose authorization codes to attacker-controlled callback locations, recommends disabling wildcard matching when unnecessary, and notes that some callback registrations created before 2026-08-03 may have it enabled for compatibility.
- **Scope status:** Recorded exact callback matching (wildcard disabled) as a deployment requirement in the credential-custody design and OAuth flow. No PKCE, denial/error callback semantics, authorization-code lifetime, or runtime behavior was decided or implemented.
- **Reference:** [GitHub App user authorization callback URL](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/about-the-user-authorization-callback-url).
- **Verification:** Fetched the official GitHub App-specific callback documentation, checked local links, and ran `git diff --check`.
