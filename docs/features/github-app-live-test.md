# Manual GitHub App live-provider test

This is an **opt-in validation harness**, not a production credential feature. It uses the existing `internal/restprovider.GitHubPublisher` against one disposable GitHub repository. Default CI does not invoke it, and there is no push, pull-request, or scheduled trigger. Live execution remains unverified until the dedicated installation and sandbox repository are provisioned through issue #134 and the manual workflow succeeds.

## Provisioning gate

Do not run before issue #134 is complete. An operator must create/install a dedicated GitHub App against exactly one disposable repository named with the `-factory-live-sandbox` suffix, and configure the repository environment `github-app-live-sandbox` with required reviewers/branch protection as appropriate. The App must not be installed on production or shared repositories. Use the narrowest permissions compatible with the test: **Contents: write** for branch push/delete and **Pull requests: write** for create/read/close. No PAT fallback is supported.

Configure these environment-scoped secrets:

- `FACTORY_LIVE_GITHUB_APP_ID`
- `FACTORY_LIVE_GITHUB_PRIVATE_KEY`
- `FACTORY_LIVE_GITHUB_INSTALLATION_ID`

Configure these non-secret environment variables from the same approved provisioning record:

- `FACTORY_LIVE_GITHUB_OWNER` — exact owner, not a pattern.
- `FACTORY_LIVE_GITHUB_APPROVED_ORGANIZATION` — exact approved GitHub organization login; must exactly match the configured owner.
- `FACTORY_LIVE_GITHUB_REPOSITORY_NAME` — exact repository name.
- `FACTORY_LIVE_GITHUB_REPOSITORY` — exact `owner/repo`.
- `FACTORY_LIVE_GITHUB_ALLOWED_REPOSITORY` — exact duplicate `owner/repo` allowlist value.

The command validates all required secrets, parses the App key and positive numeric IDs, requires explicit opt-in, checks that the exact repository matches the allowlist and separate exact owner/repository/approved-organization values, and enforces the sandbox suffix. Before publishing and before cleanup mutations, the test makes a direct authenticated `GET /repos/{owner}/{repo}` and requires the provider response's exact `full_name`, owner login, and `owner.type == Organization`; user-owned repositories, mismatched organizations, API errors, and missing fields fail closed. No target is derived from the checkout, dispatch inputs, branch names, or a default. The manual dispatch itself requires the boolean `confirm_sandbox` to be true and must target `main`. A protected GitHub environment adds the operator approval gate. The token-mint helper validates the exact repository and required App credentials before making its first provider request, so absent configuration fails closed.

## Manual operation and behavior

1. Complete #134, set the protected environment/secrets/variables above, and verify the App installation is restricted to only the sandbox repository.
2. Set `FACTORY_LIVE_GITHUB_APPROVED_ORGANIZATION` to the organization explicitly approved by the operator and match `FACTORY_LIVE_GITHUB_OWNER` exactly. In Actions, dispatch **GitHub App live provider verification** against the trusted `main` ref and explicitly confirm `confirm_sandbox`. Review the workflow source and selected environment before approving it.
3. One dedicated workflow step receives the App ID, private key, and installation ID. A standard-library helper signs a short-lived RS256 GitHub App JWT and requests an installation token for the explicit installation ID. The API request restricts the token to the one configured repository and Contents/Pull requests write permissions. The helper emits only the required escaped Actions add-mask commands for the multiline App key and returned short-lived token, then places the token in `GITHUB_ENV` for later steps in this same protected job. Neither live-test nor cleanup Go test receives or parses the private key/App credentials or mints a token; both load the masked `FACTORY_LIVE_GITHUB_INSTALLATION_TOKEN` using `LoadProviderConfig`. The token is not written to an artifact or persistent file. The `GITHUB_TOKEN` itself has only `contents: read`.
4. The Go test uses a unique job ID derived from the Actions run/attempt, fetches the sandbox's actual `main` branch with the scoped token, creates a local disposable commit, and drives a job branch/PR through the production provider adapter. Git HTTPS and GitHub API traffic use direct connections with proxying disabled. A narrow HTTP `RoundTripper` seam validates the exact configured API host, repository, authorization token, title, job ID marker, branch, base, and actual create response including PR number, URL, body, and pushed commit SHA; it forwards that create POST to `https://api.github.com` exactly once, then closes and discards the successful response. The production adapter must recover the PR via read-only GET reconciliation; the seam refuses any second POST. A second explicit provider reconciliation is read-only. This causes one real PR creation and branch push in the sandbox and is not a dry run.
5. Cleanup is bounded and guarded by both test cleanup and a later `always()` step in the same job. Before any remote push/PR publication, the test records the exact run commit in `GITHUB_ENV`; cleanup runs only when that value exists and uses the already-minted token. It queries only the current run's branch/job marker, closes (never merges) only its exact PR, verifies the branch still points to this run's commit, then deletes only that branch using an atomic Git force-with-lease against the expected commit SHA. The REST ref GET remains a defense-in-depth ownership check; the lease prevents a branch replaced after that check from being deleted. If the branch changed, the lease fails closed and cleanup can leave the replacement/resources for operator inspection. Identity mismatch, duplicate identity, provider outage, timeout, cancellation before a commit is recorded, or GitHub outage fails closed; cleanup cannot guarantee execution after runner/service failure.

The App ID, private key, and installation ID are present only in the dedicated mint step; the live-test and cleanup steps receive only the masked installation token through the protected same-job environment plus explicit non-secret sandbox identity values. The private key, JWT, and token are not printed apart from the required escaped add-mask commands, or uploaded as artifacts. Do not broaden the allowlist, add PATs, add non-manual triggers, or use this environment for production validation.

## Manual invocation and verification status

The supported invocation is the repository Actions workflow, not a local shell command: open **Actions → GitHub App live provider verification → Run workflow**, select the trusted `main` ref, set `confirm_sandbox` to `true`, and review/approve the protected `github-app-live-sandbox` environment. Before doing this, complete #134, provision the dedicated sandbox App and repository, configure every exact environment value and secret listed above, confirm the App is installed only on that sandbox, and review the workflow source. The local test code is available for credential-free unit tests, but it is not a supported live invocation path. No PR is merged. The live result is currently **unverified** because #134 sandbox provisioning is not complete; no live requests have been made.

## Local checks

The normal credential-free suite validates missing/malformed configuration, exact organization allowlisting and GitHub organization type/identity through local HTTP responses, App JWT/token scope construction against a local HTTP server, exact repository/job/branch/base/commit mapping at the response-loss seam, one forwarded create followed by discarded success response, refusal to forward a second create, read-only reconciliation, and ownership-checked cleanup with atomic compare-and-swap branch deletion. The actual live test skips without `FACTORY_LIVE_GITHUB_OPT_IN=true`; never set that value locally unless deliberately running against the provisioned disposable sandbox.
