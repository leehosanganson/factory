# Proposed REST server MVP contract

## Status and scope

**Proposed, not implemented.** Factory currently has no REST server, SQL-backed remote job state, or shared-PAT server integration. This document records a planned single-host service that uses existing Factory business logic to start supported Factory/pi implementation jobs. It is not an implementation claim or delivery commitment.

The service accepts bounded task requests for configured repository aliases. It does not expose arbitrary shell commands or a generic command runner. On successful workflow completion, automatically pushing the branch and creating or updating a pull request is in scope. Merge, release, and deployment remain human-only.

## Authentication and GitHub authority

- API requests use one configured shared bearer API key. All holders have identical access to every endpoint and job; there is no per-caller identity, isolation, or ownership boundary. Do not accept caller-supplied principal identities. The API-key provisioning and rotation mechanism remains to be specified; it must not be persisted in PostgreSQL or exposed to Pi.
- Outbound GitHub operations use one shared fine-grained personal access token (PAT); a dedicated bot/service account is recommended. The PAT owner's GitHub authority applies to all server requests. OAuth and GitHub App authorization are deferred and may be revisited later.
- Never store raw PAT bytes in PostgreSQL, job records, logs, prompts, or Pi child-process environment. PAT provisioning details remain a design gate: an environment variable or configured secret-file path is the current preference, but the exact safe loading, permissions, rotation, and injection contract is unspecified. Do not put secret bytes in ordinary configuration. The current `Runner` starts Pi with the parent process environment; server integration must ensure the PAT is not inherited by Pi, while narrowly provisioning it only to Factory-owned Git/GitHub operations that require it. This process-environment boundary needs an implementation test before the PAT path is usable.
- GitHub documents that PATs can authenticate HTTPS Git operations. Its fine-grained-token API docs also list `Contents: write` for Git-reference creation and `Pull requests: write` for PR creation/update; however, those REST endpoint permissions do not by themselves establish every Git transport detail. Test the actual fine-grained PAT against the configured repository for clone/fetch, branch push, and the complete branch/PR flow. Determine the least combined permission set from those selected operations before production use, and do not grant merge permissions.
- Encryption-key design in the [credential-custody proposal](credential-custody-design.md) concerns future OAuth grant custody. OAuth is deferred, so that encryption key is not an MVP prerequisite for the shared-PAT flow. If needed in future, an encryption key may be provided through an environment variable or configured secret-file path, never as secret bytes in ordinary configuration.

## Request and admission

A proposed `POST /v1/jobs` request contains:

```json
{
  "repository": "widget",
  "task": "Add a bounded note to the README; do not change code.",
  "issue": 42
}
```

- `repository` is a configured repository alias, not an arbitrary repository URL. Server configuration maps each alias to an approved local checkout and its GitHub repository identity; callers cannot submit filesystem paths. `task` is a required nonblank task description. `issue` is optional supplemental GitHub issue context and may be an issue number or URL.
- The request must not accept arbitrary shell, command, or executable fields. The task description—not an issue—is the requested work.
- Exact validation and source semantics for optional issue numbers/URLs are an unresolved implementation gate. A number naturally refers to an issue in the alias's configured repository. A URL must not cause autonomous issue polling, reconciliation, or tracking unless that behavior is explicitly designed and approved; reject unsupported or ambiguous forms rather than guessing.
- Bound request body and task sizes before decoding/processing. Reject malformed JSON, unknown fields, unsupported media types, invalid UTF-8, and blank task descriptions. Exact limits are implementation choices to specify before deployment.
- Admission records the accepted request, durable job metadata/status, and initial history atomically in PostgreSQL before returning acceptance. Use an idempotency key or equivalent duplicate-submission protection; exact retention and replay response details remain implementation choices. Do not acknowledge a job if its durable acceptance transaction fails.

A successful admission may return `202 Accepted` with an opaque job ID, initial status, and links to status/history. Proposed read endpoints are `GET /v1/jobs/{id}` and `GET /v1/jobs/{id}/history`. Since API credentials are shared, any valid key holder can inspect any job; responses must still be bounded and sanitized and must not expose credentials, raw prompts beyond the submitted task where unnecessary, or unfiltered agent output.

## Persistence and recovery

- PostgreSQL stores accepted requests and durable job metadata, status, and history. Acceptance and each durable state/history update are atomic in the database.
- The current detached-job store is file-backed. Reusing Factory business logic does not by itself make job state PostgreSQL-backed; the server needs a deliberate service/store integration so the API request, job identity, and lifecycle have one durable authority rather than divergent SQL and file records. Preserve the existing CLI behavior unless a separate change is approved.
- A persistent mounted filesystem holds repository checkouts, worktrees, logs, and artifacts. Database records and filesystem data have separate responsibilities; do not claim database persistence alone recovers workspace or process memory.
- On restart, retain database records and mark jobs whose execution/side-effect outcome is uncertain as interrupted or requiring inspection. Preserve their workspaces and evidence where available. Do not blindly restart Pi, replay an uncertain Git push, or repeat an uncertain PR side effect. Inspect/reconcile before an operator-authorized continuation policy is defined.
- Process or agent memory is not recoverable and must not be described as durable.

## Limits, errors, and deferred controls

- Return safe generic errors without stack traces, API keys, PATs, authorization headers, raw provider payloads, or sensitive filesystem paths. Never log API-key or PAT values. Define stable response schemas and status codes during implementation.
- Rate limiting is deferred. This proposal does not promise rate limits, `429`, or a `Retry-After` contract. Request/body input bounds remain necessary and do not constitute rate limiting.
- Concurrency, timeouts, and resource bounds are server configuration, not caller-controlled job fields. Their defaults require implementation and deployment decisions.

## Explicit human boundary

The worker may push its implementation branch and create/update a PR after a successful workflow. It must never merge a PR, release artifacts, or deploy software. A PR or successful process exit is not an independent correctness verdict; verification evidence and limitations should remain reviewable by a human.
