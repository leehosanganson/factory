# GitHub App delegated credential custody design

## Status

This is a proposed design gate for the REST MVP; it does not implement OAuth, credential storage, remote work admission, or worker execution. It builds on the [REST API/security contract](rest-api-contract.md), the [principal bearer verifier](../issues/usability/implement-principal-bearer-verifier-loader.md), and the existing single-host local state. The specific choices in this draft require review before code implementation. A Factory principal's API bearer token and its GitHub App user grant are separate credentials with separate roles.

## Security and identity model

- API bearer authentication resolves exactly one configured Factory principal. A client cannot supply a GitHub username, account ID, installation ID, or principal ID to choose the acting grant.
- Each principal may have at most one active GitHub App user grant. OAuth initiation requires that principal's API bearer token. The public callback binds the grant only through a server-side, short-lived, atomic single-use state record.
- GitHub user-token permissions and repository access are the intersection of the App grant and user's actual access. For source issue access, verify the user grant can read that issue; for target code changes, verify the user and App installation each cover the target repository and required operations.
- No credential is a WorkRequest field. Do not put bearer tokens, OAuth codes, access/refresh tokens, or encryption keys into queue records, instructions, agent prompts, logs, worktrees, PRs, or API responses.

## Grant record and cryptographic envelope

Persist only the credential material needed to maintain the GitHub App user grant, plus non-secret metadata:

- Factory principal ID (also the stable record identity/AAD binding);
- GitHub user numeric ID and login as verified identity metadata;
- installation/repository authorization metadata only if needed for efficient checks, with a freshness timestamp (never treat cached authorization as sufficient for final admission);
- encrypted user access token and refresh token, token expirations, and a record version/update time;
- a non-secret encryption key ID and envelope version.

Use a 32-byte random AES-256 key provisioned by the operator from a secret mount or equivalent external secret manager. The state-volume record contains ciphertext only, not the key. Avoid silently reading a secret from ordinary Factory JSON config or storing a fallback key alongside data. The exact key-delivery interface is a deployment gate: prefer opening an operator-mounted secret file with the strict regular-file, non-symlink, owner/mode, size, and non-blocking rules already used by the principal verifier; do not read key bytes from a process argument or loggable CLI flag. The verifier loader currently checks effective-UID ownership; a container deployment where the secret is provisioned by root must arrange matching runtime ownership or explicitly design a safe group-readable secret mechanism before selecting that deployment. Validate the key is exactly 32 bytes and fail startup/operation closed if missing or unsafe.

Encrypt the credential payload with standard-library `crypto/aes`, `crypto/cipher` AES-GCM, and a fresh `crypto/rand` nonce per write. Use an envelope with an explicit supported version, key ID, nonce, and ciphertext. AAD binds at least the envelope version, record type, and principal ID so ciphertext cannot be transplanted to another principal or record class. Reject malformed encodings, unsupported envelope versions, unknown key IDs, wrong nonce lengths, authentication-tag failures, and invalid decrypted schema. Never return partially decoded data on error. Use private atomic writes and the established single-host lock pattern; check symlinks/special files before reads and ensure `O_NONBLOCK` so unsafe path types cannot hang startup.

The decrypted payload is held only as long as needed for the GitHub operation. Go cannot promise secure zeroization of all copies; minimize copies and lifetime, never format or marshal plaintext credentials, and do not claim memory erasure.

## Key rotation and loss

- Configure one active key ID/key and an explicit bounded set of prior key IDs/keys accepted only for decryption during rotation.
- Rotation is staged: add new active key while retaining old decryption key; atomically decrypt/re-encrypt each grant under the new key; verify all records can be read under the new key; only then remove the old key from configuration. Interrupted rotation resumes by scanning records and re-encrypting idempotently. Never overwrite a record until the new envelope is fully encrypted and atomically durable. Recovery checks for an uncertain callback persistence outcome must use the configured active and prior decryption keys; inability to read a grant during migration or recovery is not evidence that the grant is absent.
- Missing key, wrong key, corrupted ciphertext, unknown key ID, or failed migration fails closed. No plaintext fallback and no automatic deletion. Report only a sanitized principal/record identifier to operator diagnostics; never include ciphertext or token bytes.
- Losing the only decryption key makes affected grants unusable. Require GitHub reauthorization through a fresh OAuth flow; do not attempt guessed recovery.

## OAuth lifecycle

The provider-specific facts below describe GitHub's **GitHub App user access token web application flow**, not the similarly named OAuth App flow. See GitHub's documentation for [generating a user access token](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-a-user-access-token-for-a-github-app) and [callback URLs](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/about-the-user-authorization-callback-url). The docs identify `https://github.com/login/oauth/authorize` and require `client_id`; they recommend `redirect_uri`, random `state`, and PKCE. If the authorization request includes PKCE parameters, `code_challenge` and `code_challenge_method` must be paired, the method must be `S256`, and token exchange must include the corresponding `code_verifier`. The callback URL docs allow up to 10 registered URLs and say the first is used when the request omits `redirect_uri`. The GitHub App parameter table says an explicit `redirect_uri` must match a registered callback URL and cannot contain additional parameters. The token-exchange parameter table marks `client_id`, `client_secret`, and `code` required; it says any `redirect_uri` must be registered.

These provider recommendations do not themselves choose Factory's PKCE policy. Whether Factory adopts PKCE, how a verifier would be durably bound to state, and GitHub App web-flow denial/error callback behavior remain open. Do not infer those details from OAuth App documentation. GitHub's GitHub App web-flow parameter table does not list `response_type`; that omission does not establish whether the parameter is accepted or rejected. The fetched GitHub App web-flow docs also do not establish an authorization-code expiry. Factory's proposed 10-minute state TTL is its own state-retention policy, not a claim about code lifetime.

1. An authenticated principal requests OAuth initiation.
2. Generate 256-bit cryptographically random state, persist only its digest with principal ID, explicit callback URI/config version, creation/expiry (10 minutes), and unused status. Do not encode the principal ID into externally visible state.
3. Return `302 Found` to the configured GitHub App authorization URL, including required `client_id` and an explicit configured, registered `redirect_uri`; the exact URL construction and GitHub App registration remain deployment gates. Use `Cache-Control: no-store`; apply strict referrer policy. Do not use untrusted host or forwarding headers to construct callback URL.
4. At public callback, atomically consume valid unexpired state before exchanging its code. Consumption is durable and single-use: concurrent/replayed callbacks cannot both succeed, and a failure after consumption requires a fresh authorization flow rather than replay. Redact query `code` and `state` at both proxy and application logs.
5. Exchange the code server-side; fetch/verify the GitHub user identity and validate the grant's granted permissions. Encrypt and atomically persist the grant for the bound principal; only report a minimal success/failure page. Never return tokens to a browser, redirect, durable work record, or logs.
6. If code exchange fails before a new grant is persisted, that callback creates no grant; any existing grant remains unchanged. Tell the user to start a fresh authorization flow and never replay a consumed code/state pair. If grant persistence succeeds but the callback response is lost, treat the valid persisted grant as linked and do not overwrite it or exchange the consumed callback again. If persistence outcome is uncertain, inspect the grant record before taking another linking action: a valid record for the bound principal is linked; confirmed absence from an otherwise healthy, readable store requires a fresh authorization flow; an unreadable, corrupt, key-inaccessible, or otherwise uncertain record fails closed for operator reconciliation and must not be treated as absence.

At most one active grant is allowed per principal. V1 does not auto-replace a grant from a callback. If one already exists, reject linking with a safe conflict and require an explicit operator-controlled unlink/revoke path. Do not remove a grant while accepted work references it; operator revoke first marks it unavailable for new admission/resume, then active work is paused and reconciled before ciphertext removal. There is no public API unlink/cancel route in v1.

References:

- [GitHub refreshing user access tokens](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/refreshing-user-access-tokens) documents token expiration and replacement of the old refresh/access token when a refresh token is used.
- [GitHub App user access token permissions](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-a-user-access-token-for-a-github-app)
- [GitHub App authentication on behalf of a user](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/authenticating-with-a-github-app-on-behalf-of-a-user)
- [GitHub App installation authentication and HTTPS Git access](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/authenticating-as-a-github-app-installation)
- [GitHub REST endpoints for Git references](https://docs.github.com/en/rest/git/refs)
- [GitHub REST endpoints for pull requests](https://docs.github.com/en/rest/pulls/pulls)
- [GitHub REST endpoints for issues](https://docs.github.com/en/rest/issues/issues)
- [Fine-grained token permission map](https://docs.github.com/en/rest/authentication/permissions-required-for-fine-grained-personal-access-tokens)

## Expiry, refresh, revocation, and use

- Respect GitHub's returned expiry and refresh-token rotation data; do not hard-code token lifetimes. Before a provider operation, refresh when required within a bounded clock-skew window. Serialize refresh per principal so concurrent workers cannot race one-time refresh tokens. GitHub documents that using a refresh token invalidates the old refresh token and old user access token. Persist the returned rotated tokens atomically before using them for additional work. If the response is lost or persistence outcome is uncertain, do not retry the consumed refresh token blindly; mark the grant unavailable and require a fresh OAuth flow unless the new grant can be safely reconciled.
- An expired grant, failed refresh, explicit revocation, permission reduction, or unknown grant status blocks new admission and pauses dependent work. Never fall back to the service's own installation credential to continue user work.
- A refreshed token is validated against the expected GitHub user identity before replacing the stored grant. A mismatch is a security failure; retain the old record only for operator diagnosis, mark the grant unusable, and require fresh authorization.
- Keep grant references out of WorkRequest secrets: store a stable principal/grant reference, not token data. Worker obtains a short-lived credential handle from a provider-scoped interface only for the operation requiring GitHub access; the worker/API projection cannot retrieve or serialize raw token strings.
- Before admitting work, verify current source issue readability, target repository access, App installation access, and the exact permission set needed. Recheck as necessary before write operations; access may be revoked after admission.

## Open implementation gates

1. Verify the exact permissions for all selected issue, repository, PR, and Git transport calls; current source evidence confirms Issues: read for issue retrieval, Contents: write for Git reference creation, Pull requests: write/read for PR creation/retrieval, and App/user permission intersection. Delegated user-token HTTPS Git transport remains unverified; do not infer it from REST token support.
2. Resolve and test the GitHub App authorization URL configuration and callback behavior, including whether to adopt PKCE and how its verifier is bound to one-time state, user-denial/provider-error handling, and installation selection. Use the dedicated GitHub App web-flow docs rather than importing OAuth App callback behavior. Preserve as unknowns the undocumented status of `response_type` and authorization-code expiry; do not invent a Factory policy based on those gaps.
3. Specify mounted key-file ownership/permission rules for a non-root container and secret rotation injection/rollback.
4. Define GitHub token refresh concurrency and outcome reconciliation against its rotating refresh-token behavior; a lost response after consumption may require reauthorization rather than retry.
5. Define which source issue and target repository combinations are supported, including cross-repository issue-to-code requests.
6. Specify accepted-work behavior on grant unlink, user access loss, app uninstall, permission reduction, and encryption-key loss.

No remote work admission or worker execution should be implemented until these gates have an approved, testable resolution.