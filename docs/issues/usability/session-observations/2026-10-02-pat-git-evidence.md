# PAT HTTPS Git evidence for REST MVP

- **Type:** Evidence-based clarification; not a user-reported finding.
- **Evidence:** GitHub's PAT guidance says PATs may be used in place of passwords for HTTPS Git operations. Its REST docs list fine-grained PAT support with `Contents: write` for creating a Git reference and `Pull requests: write` for creating/updating a PR.
- **Scope status:** Clarified these facts without claiming the REST permission tables establish all Git transport behavior. The exact repository workflow (clone/fetch, push, branch/PR creation/update) and least combined permission set still require an integration test. No PAT use or server behavior was implemented.
- **References:** [Managing personal access tokens](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens); [Git references](https://docs.github.com/en/rest/git/refs#create-a-reference); [Pull requests](https://docs.github.com/en/rest/pulls/pulls#create-a-pull-request).
- **Verification:** Fetched official GitHub documentation for PAT HTTPS Git operations and fine-grained PAT endpoint support; `git diff --check` passed.
