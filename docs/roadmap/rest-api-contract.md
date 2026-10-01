# REST MVP API and security contract

## Status

This is a proposed contract for the agreed, containerized, single-host REST MVP. None of these routes, OAuth handlers, remote admission, or worker behavior is implemented. It selects bounded API defaults; it does not authorize provider writes or automated execution before their prerequisites are implemented and reviewed.

## Boundaries

- API version 1 uses `/v1`; no generic command, shell, repository URL, or arbitrary-work endpoint is provided.
- A work request must identify a GitHub issue and target repository. An optional, bounded `instruction` may add context to that issue request; it cannot replace the issue, override Factory policy, grant credentials, or request arbitrary commands.
- Work is submitted and inspected as the authenticated Factory principal. The client cannot set or override principal identity.
- Admission uses the existing canonical provider-neutral request and durable single-host `WorkQueue` path. A `202 Accepted` is returned only after the request and principal ownership have been durably persisted. API handlers do not run coding work inline.
- The API uses the per-principal bearer verifier. GitHub calls use only that principal's linked GitHub App user grant; they do not fall back to broader service credentials. Work is eligible only when the linked grant can read the source issue and both the user and App installation authorize the target repository.
- Merge, release, and deployment are never available to the worker. Cancellation and human-direction routes are not in v1.

## Limits and common errors

- Require `Content-Type: application/json` for JSON request bodies. Reject unsupported media types, malformed JSON, unknown fields, duplicate JSON object keys, and trailing values.
- Recommended initial cap: 64 KiB per JSON request body before decoding; cap `instruction` at 16 KiB UTF-8 bytes. Reject invalid UTF-8 and blank-after-trimming instructions; preserve the original accepted text rather than silently rewriting its meaning.
- Require `Idempotency-Key` on `POST /v1/work`: 1–128 visible ASCII characters. Scope the key to the authenticated principal. Retain the key or a stable digest of it with the durable request for as long as that request is retained.
- Return a JSON error object with stable `code`, safe human-readable `message`, and server-generated `request_id`. Never return stack traces, provider response bodies, bearer/OAuth tokens, filesystem paths, raw authorization headers, or unfiltered issue/agent payloads.
- Use `400` for malformed/invalid request data, `401` for missing/invalid bearer credentials, `403` for an authenticated principal without a linked/usable grant or insufficient repository access, `404` for a missing or non-owned work ID (do not reveal that another principal's work exists), `409` for idempotency-key/payload conflict, `413` for oversized bodies, `415` for unsupported media type, `429` for configured rate limiting, and `500`/`503` for safe generic internal/unavailable errors. Durable enqueue errors never produce an accepted response.
- Require configured per-principal and global rate limits for submissions and reads; exact numeric defaults remain a deployment/capacity decision and must be load-tested before production. Return `429` with a bounded `Retry-After`; do not reveal another principal's quota state. Configuration must enforce an operator-set maximum and reject unbounded/disabled limits in production mode.
- Concurrency, polling frequency, and outbound timeouts are server-configured bounded values. Limits must not be caller-controlled in the work payload.

## Routes

### `POST /v1/work`

Requires a valid Factory bearer token and `Idempotency-Key`.

Request:

```json
{
  "issue": {
    "repository": "acme/widget",
    "number": 42
  },
  "repository": "acme/widget",
  "instruction": "Add a bounded note to the README; do not change code."
}
```

The issue repository and target repository may differ; the example uses the same repository for both.

`issue.repository` and `repository` are canonical `owner/repo` identities and may differ: the issue is the source of work and `repository` is the target code repository. `number` is a positive integer. `instruction` is optional. Reject ambiguous casing/whitespace or noncanonical identities rather than silently changing which repository the caller selected. The issue remains the work's source of truth; the instruction is supplemental direction, not a general task payload. Reject issue references that resolve to pull requests. Before accepting, verify that the linked user's grant can read the source issue and that both the user and App installation authorize the target repository for the required operations. Authorization failures and unavailable/ambiguous authorization checks fail closed without a queue write.

The request fingerprint for idempotency includes the authenticated principal, normalized issue identity, target repository, and exact instruction bytes (or explicit absence). On the same principal and key:

- same fingerprint: return the existing request identity and current status, without another queue item or worker;
- different fingerprint: return `409 idempotency_conflict`, without mutation;
- first request: validate authorization, persist canonical request, caller principal, idempotency identity/fingerprint, and accepted timestamp atomically through the admission boundary; only after durable success return `202`.

A durable outbox or equivalent atomic mechanism is required if request data and ownership/idempotency metadata cannot be persisted atomically with the existing queue. Do not acknowledge acceptance if only some of those records were written.

Accepted response (`202`):

```json
{
  "id": "opaque-stable-id",
  "status": "queued",
  "created_at": "2026-10-01T12:00:00Z",
  "status_url": "/v1/work/opaque-stable-id",
  "history_url": "/v1/work/opaque-stable-id/history"
}
```

A same-key exact retry returns the same ID and URLs; return `200` for an idempotent replay and include `idempotent_replay: true`. It is not a fresh acceptance. An authorization or queue failure is a rejection, never a `202`. The idempotency mapping, principal ownership, canonical request, and outbox/accepted event must commit atomically in the single-host store before acknowledgment. Use one versioned work record containing those fields where feasible; if multiple files are required, use a recoverable transaction/journal with startup reconciliation, and never make partially committed data visible as accepted.

### `GET /v1/work/{id}`

Requires a valid Factory bearer token. Return `404` both when the ID is absent and when it belongs to another principal. The response is a bounded, sanitized projection; it does not return the whole durable record, prompt, issue body, agent transcript, or credentials.

```json
{
  "id": "opaque-stable-id",
  "status": "waiting_for_human",
  "issue": { "repository": "acme/widget", "number": 42, "url": "https://github.com/acme/widget/issues/42" },
  "repository": "acme/widget",
  "pull_request": null,
  "created_at": "2026-10-01T12:00:00Z",
  "updated_at": "2026-10-01T12:04:00Z",
  "latest_activity_at": "2026-10-01T12:04:00Z",
  "verification": null,
  "reason": { "code": "issue_changed", "message": "The issue changed and needs review." }
}
```

Statuses are a versioned API enum, not raw internal queue strings. The initial vocabulary is `queued`, `running`, `waiting_for_human`, `paused`, `succeeded`, `failed`, `cancelled`, and `stopped`. A PR existing does not mean work succeeded; only a recorded terminal outcome with explicit verification result may report `succeeded`. `reason` is absent when no safe reason is available; it never includes raw provider errors or sensitive content. `verification` is a bounded summary with result and limitations, not a claim of independent correctness.

### `GET /v1/work/{id}/history`

Requires a valid bearer token and ownership of the request; missing and non-owned IDs both return `404`. Return at most the latest 100 sanitized events in chronological order. When more events exist, return the latest 100 and set `truncated: true`; otherwise `false`. Include stable opaque event IDs and timestamps. Do not expose raw issue bodies, comments, instructions, agent output, credentials, or provider response payloads. A later API version can add cursor pagination if real usage requires it; clients must not assume an unbounded history.

```json
{
  "work_id": "opaque-stable-id",
  "events": [
    { "id": "opaque-event-id", "at": "2026-10-01T12:00:00Z", "type": "accepted", "summary": "Work request accepted." },
    { "id": "opaque-event-id-2", "at": "2026-10-01T12:04:00Z", "type": "paused", "summary": "The issue changed and needs review." }
  ],
  "truncated": false
}
```

## GitHub App authorization and permissions

- A Factory principal is authenticated by its configured bearer-token verifier and is linked to at most one active GitHub App user grant. Do not use client-provided owner names, account IDs, installation IDs, or GitHub usernames to choose the acting principal/grant.
- Repository authorization is checked against the actual delegated grant and the App installation. The user access token has only permissions and resource access common to the user and the App; user access to a repository alone is insufficient if the App installation lacks it.
- Candidate minimum repository permissions for the intended read/prepare flow are: **Metadata: read**, **Issues: read** for issue snapshots, **Contents: write** for branch/code writes, and **Pull requests: write** for PR creation/update (this should cover PR snapshot reads if the permission hierarchy grants read under write). GitHub documents PR create/update as requiring Pull requests: write and git-reference creation as requiring Contents: write. Exact permission requirements must be checked against every selected GitHub API and git transport operation before App registration or implementation; the precise issue-read endpoint permission remains unverified. In particular, GitHub App user-token use as HTTPS git credentials for clone/push needs an explicit supported-flow check; do not assume REST permissions alone define git transport authorization. Do not grant issue write, administration, Actions/workflow write, or broader organization permissions unless a documented required operation proves they are necessary. If a request would modify `.github/workflows` or another operation needing an ungranted permission, fail or pause; do not expand the App grant automatically. Never expose a merge operation to the worker.
- App user access token permissions/resources are bounded by both the App and user grants. Revocation or a failed refresh marks the grant unusable; it is not deleted automatically while work may reference it. Paused requests require successful reauthorization before resuming; retained ciphertext can be securely replaced by an operator revoke action only after no active work depends on it.
- The deployment key is supplied outside the durable state volume as exactly 32 random bytes. Each envelope carries a non-secret key ID and format version; use AES-256-GCM with a fresh nonce and authenticate principal ID plus record format as AAD. Support an active key and explicitly configured prior decryption keys during rotation; re-encrypt records under the active key through an atomic migration before retiring old keys. Missing keys, invalid tags, unknown versions, or unknown key IDs fail closed and never fall back to plaintext. These cryptographic and rotation details must be reviewed against the deployment secret-injection mechanism before implementation.

References:

- [GitHub fine-grained token permission map](https://docs.github.com/en/rest/authentication/permissions-required-for-fine-grained-personal-access-tokens)
- [Create a pull request](https://docs.github.com/en/rest/pulls/pulls#create-a-pull-request)
- [Create a Git reference](https://docs.github.com/en/rest/git/refs#create-a-reference)
- [Create or update repository contents](https://docs.github.com/en/rest/repos/contents#create-or-update-file-contents)
- [GitHub App user access token permissions](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-a-user-access-token-for-a-github-app)

## OAuth linking

- `POST /v1/oauth/github/authorization` requires Factory bearer authentication. It creates 256 bits of cryptographically random state, stores it server-side bound to the authenticated principal with a 10-minute expiry and single-use status, then returns `302 Found` to GitHub's App authorization URL. State is opaque and contains no principal ID or credential. Apply `Cache-Control: no-store` and a restrictive `Referrer-Policy` to authorization and callback responses. The browser-mediated flow is intended for a first-party trusted UI/client; a raw bearer credential should not be exposed to an untrusted browser environment.
- `GET /v1/oauth/github/callback` is public because GitHub redirects the browser to it. It accepts only the provider's `code` and `state`; state is validated, atomically claimed once, checked for expiry and principal binding, and cannot select a different Factory principal. Callback URL is an explicit configured public HTTPS URL; do not derive it from Host, `Forwarded`, or `X-Forwarded-*` headers. A failed exchange requires the client to start a fresh authorization flow; the same state/code pair is not replayed. A callback consumed while token exchange or grant persistence fails leaves the principal unlinked and reports only safe recovery instructions.
- Exchange the code server-side; never return access/refresh tokens to the browser or place them in work records, logs, redirects, or error messages. Encrypt refreshable grant material at rest with an external deployment key. Validate the returned user identity via GitHub and bind the grant to the state principal. A new link is rejected with `409 grant_already_linked` when the principal already has an active grant; v1 relinking requires an explicit local-operator revoke/unlink first. Do not silently replace credentials based on a new OAuth callback. Fail closed on invalid, expired, reused, or revoked state/grants.
- Redact OAuth `code` and `state` query parameters from proxy and application access logs. Return a minimal static success/failure page with no reflected provider data; do not place access/refresh tokens or authorization codes in a redirect. Use `Cache-Control: no-store`, `Referrer-Policy: no-referrer`, and a restrictive content security policy.

## Operational route behavior and exclusions

Readiness/health endpoints are separate from work data, disclose no principal or job information, and must not imply that GitHub authorization or work processing is usable when dependencies are unavailable. Their exact paths and readiness dependencies are a container-shell gate.

V1 deliberately has no endpoint to submit instructions to already accepted work, cancel work, inspect arbitrary provider payloads, or execute a command. Such operations require separate authorization and state-transition contracts. The API is not implemented until the separate credential-custody, admission, and container gates are met; this document alone does not enable accepting remote work.
