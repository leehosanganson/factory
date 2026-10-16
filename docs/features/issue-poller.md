# Autonomous GitHub issue poller

`factory issue-poller --config <absolute-path>` runs a dedicated process that reads GitHub issues and admits eligible work into the canonical PostgreSQL REST job Store. It does not start a second execution queue: the normal REST server/`factory worker` lifecycle claims and executes the admitted job. GitHub issue state and REST job delivery status remain separate; the poller does not close issues, create PRs, or report issue completion.

The poller requires a GitHub token in a private owner-readable file and a PostgreSQL DSN for the same database used by the REST server/worker. Its PostgreSQL capacity/retention settings must match those shared processes. It takes a database-wide PostgreSQL advisory singleton lock. A second poller exits instead of running concurrently. Each exact allowlisted source has a durable GitHub page cursor: successful pages advance after every returned candidate has either been durably admitted or confirmed ineligible, and the scan wraps to page one at the end. A candidate whose re-fetch, quarantine, or durable admission is uncertain holds the cursor on that page. An oversized task/title is recorded durably by exact source/issue/version and a bounded reason class before cursor progress, without storing issue text or mutating its label. If admission finds an existing provenance reservation belonging to a different principal or request, the poller records a `provenance_conflict` quarantine keyed by the candidate source/issue/version, advances only after that record is durable, and leaves the trigger label untouched. Operator listings expose only that bounded identity and reason class; they do not reveal the conflicting job, owner, or issue content. Operators can explicitly resolve the quarantine using the same `issue-poller resolve` command. Label-removal retries are independently tracked by the durable admission ledger. This lets later pages progress across poll cycles and resumes the scan after process restart. Shutdown is cooperative; active API calls use the process cancellation context. Unconfirmed durable admission leaves the trigger label untouched. If label removal is uncertain, the durable ledger retries by re-fetching current labels; the label may already be absent if GitHub applied a DELETE whose response was lost.

Configuration is strict JSON; unknown keys and trailing JSON values are rejected. Because it contains a PostgreSQL DSN, operators should protect the config file with owner-only permissions. `sources` is an exact, case-sensitive `owner/repo` to destination-alias allowlist. The trigger label is matched by exact name, not case-folded. The provider query only requests open issues, and every candidate is fetched again immediately before admission. The re-fetched snapshot is authoritative: if the issue changed after search but is still open, is not a pull request, and still has the exact trigger label, that currently eligible version is admitted; candidates that fail those checks, or have a wrong source identity, are not dispatched. Issue title/body text is bounded, escaped for control and bidi formatting characters, framed as untrusted context, and submitted using the existing ordinary REST task envelope.

```json
{
  "postgres_dsn": "postgres://factory:REDACTED@db.example/factory",
  "github_token_file": "/run/secrets/factory-issue-poller-token",
  "sources": {
    "acme/product": "product"
  },
  "trigger_label": "factory:ready",
  "poll_interval": "30s",
  "batch_size": 25,
  "queue_capacity": 32,
  "workers": 2,
  "max_records": 1000,
  "max_events_per_job": 200,
  "task_bytes": 262144,
  "registry_bytes": 268435456
}
```

`queue_capacity`, `workers`, `max_records`, `max_events_per_job`, `task_bytes`, and `registry_bytes` must match the canonical Store configuration. `batch_size` is 1–100 and `poll_interval` is 1 second–24 hours. Token files must be private (no group/other permissions), owner-readable regular files owned by the effective user. The poller does not load or use REST API caller credentials. GitHub calls use `gh api` with `GH_TOKEN` set only for the child and a fresh private `GH_CONFIG_DIR`; inherited `GH_*` and `GITHUB_TOKEN` variables are removed. The host still needs a trusted `gh` binary and operator-managed database/GitHub access. GitHub credentials and untrusted issue text are not a security sandbox boundary.

Admission uses deterministic idempotency derived from destination alias, exact source, issue number, and issue-version fingerprint, plus the Store's durable issue-provenance uniqueness. Admission and a bounded per-issue/version label-removal record are committed in the same PostgreSQL transaction. If Store admission is uncertain or fails, the trigger label is not touched. After confirmed durable admission, the poller re-fetches the issue and removes only the configured trigger label if the same issue identity is still open. Changes to title, body, or version after admission do not prevent this label cleanup; cleanup is tied to the admitted issue identity and label, not to an unchanged version. If DELETE response is lost, a later retry first reads current labels: an already-absent label is recorded complete without another DELETE. Label removal has a five-attempt durable cap. On exhaustion the record becomes terminal `operator_action_required`, leaves pending capacity, and is not retried automatically. `factory issue-poller list --config <path> [--limit 1..100]` displays unresolved quarantines/operator actions using only source, issue number, version, outcome, bounded reason class, and attempt count. After handling it, an operator records an explicit resolution with `factory issue-poller resolve --config <path> --source <owner/repo> --issue <number> --version <sha256>`. These commands use the protected PostgreSQL DSN from the poller config and only permit allowlisted sources; they do not call GitHub or expose issue text/secrets. The ledger retains at most 50,000 rows, evicting only completed/resolved history; it fails closed rather than evicting unresolved operator-action/quarantine records. Label removal failure does not change job status or undo admission. To enqueue a later issue version, an operator must explicitly reapply the trigger label after prior admission handling; the poller does not automatically queue edited issue contents.

```sh
factory issue-poller --config /etc/factory/issue-poller.json
```

The GitHub token should have only the minimum repository permissions required to read issues and remove the configured label (typically Issues read/write for each allowlisted source). The PostgreSQL credential should be scoped to the Factory job database/schema. This feature does not add HTTP endpoints or provider-side issue-status synchronization. PostgreSQL integration coverage is opt-in via `FACTORY_TEST_POSTGRES_URL`; deterministic unit tests do not require GitHub or PostgreSQL credentials.
