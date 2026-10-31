# GitHub issue webhook

The REST control plane optionally accepts one GitHub `issues` webhook event at the exact route `POST /v1/webhooks/github/issues`. It only admits the `labeled` action when the open non-PR issue is from an exact configured source repository, the event's label is the exact configured trigger label, and the issue's current labels also include that exact name. Other valid but ineligible events are acknowledged as ignored. The adapter validates the event and maps its source repository to an operator-configured existing Factory repository alias and an enabled existing Factory principal ID. Authentication for delivery uses GitHub's `X-Hub-Signature-256` HMAC over the exact raw request bytes, independently from the Factory bearer-token identities; no event-supplied principal or owner is trusted.

Webhook delivery requires SQLite or PostgreSQL; memory persistence is rejected in configuration. The store atomically persists the Thread, its initial queued Job, membership, Thread idempotency key, one queued history event, stable GitHub delivery ID, and SHA-256 digest of the signed body. Exact delivery retries replay the same retained Thread+Job; reuse of a delivery ID with a different body returns a sanitized conflict. Only the digest is retained, not the raw webhook body, HMAC signature, or signing secret. Issue text is framed and escaped as untrusted task context and is subject to the configured request/task bounds. The Thread is owned by the principal mapped to that source in server configuration. The webhook only admits work; the existing workers execute the same canonical Job lifecycle.

Configuration is strict server JSON. `github_webhook` is optional; when set it requires an absolute private `secret_file` with at least 32 bytes, a nonempty exact `trigger_label`, and equal `sources` and `owners` maps. Each `sources` key is an exact GitHub `owner/repository` and maps to an existing `repositories` alias. The corresponding `owners` value must be the ID of an enabled Factory principal. For example:

```json
{
  "github_webhook": {
    "secret_file": "/run/secrets/factory-github-webhook",
    "trigger_label": "factory:ready",
    "sources": {
      "acme/issues": "widget"
    },
    "owners": {
      "acme/issues": "issue-owner"
    }
  }
}
```

The secret file must be an owner-readable regular file owned by the effective service user and inaccessible to group/others. Set `persistence.backend` to `sqlite` or `postgres`; configured durable storage fails closed if unavailable. Use `factory server doctor` to check the configured secret file and SQLite metadata locally. Rotation requires server restart.

This adapter deliberately does not coordinate with or claim exclusivity over `factory issue-poller`. If both are configured for the same source and trigger label, the webhook and poller can independently admit duplicate work. Operators should choose one intake mechanism per source/label. The system does not enforce this choice. The webhook does not scan issues, mutate labels/issues, synchronize issue lifecycle, execute jobs itself, create a second queue, or implement assignment, scheduling, replay commands, or automatic worker control. Work remains an ordinary Thread with one queued Job in the existing REST job and worker lifecycle. Current behavior is narrower than future architecture described in the roadmap.

The HTTP endpoint only responds through the REST server and has no bearer auth; deploy it behind GitHub's TLS delivery endpoint and suitable network controls. The local service and configured execution processes are not a security sandbox. Issue and repository content remain untrusted input.
