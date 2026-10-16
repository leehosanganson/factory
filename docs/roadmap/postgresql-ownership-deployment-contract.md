# PostgreSQL ownership protocol deployment contract

## Status

**Test-only protocol fixture; no production failover/HA guarantee.** The isolated `PostgresOwnership` API has PostgreSQL 18 integration coverage, but is not itself the REST Store, runtime option, or worker coordinator and provides no production ownership/failover/recovery authority. Separately, PostgreSQL REST persistence and separately launched workers are implemented; the server can use PostgreSQL with all-in-one workers or a workers-disabled API control plane sharing storage with `factory worker` processes. All-in-one workers remain the default, and there is no fleet coordinator. See the [implemented REST server features](../features/README.md), [PostgreSQL Store](../features/postgres-store.md), and [REST worker process](../features/rest-worker.md). These protocol-test restrictions are deliberate deployment prohibitions, not verified HA behavior.

## Supported topology for the protocol tests

- PostgreSQL 18, one writable primary, one process/test harness at a time except where independent clients are deliberately used to test concurrency.
- The PostgreSQL test fixture uses the server's default `READ COMMITTED` isolation. Claims and lifecycle mutations use transactions, row locks and conditional writes; lease checks use the database clock. Claim, heartbeat, and terminal finish update job/ownership/history records atomically within their transactions.
- The isolated protocol fixture's CI and local Compose tests exercise a PostgreSQL 18 primary. They do not test replication, failover, promotion, proxy routing, backup/restore, or production HA. Separate current Store/runtime coverage is documented in the [PostgreSQL Store feature](../features/postgres-store.md) and [REST worker feature](../features/rest-worker.md).
- No connection pooler/proxy behavior is included in the supported protocol-test contract. Any deployment integration requires its own reviewed assumptions and tests.

## Explicitly prohibited deployment assumptions

Do not deploy this protocol as a production HA or multi-host worker authority. In particular, this contract does not support or guarantee:

- automatic or operator-triggered primary promotion/failover while workers or clients may still use the former primary;
- multi-primary/split-brain operation, replica reads used to make ownership decisions, or routing that can send a transaction to a stale/non-authoritative node;
- transparent retry of a transaction after connection loss or an ambiguous commit;
- worker reassignment or provider writes based solely on a lease timeout;
- any production RPO/RTO, replication lag, failover durability, or backup/restore guarantee.

PostgreSQL's failover guidance requires a mechanism to prevent a former primary from continuing to act as primary; promotion without fencing can result in both systems believing they are primary and data loss. PostgreSQL streaming replication is asynchronous by default, so a primary failure can lose transactions not yet replicated. Synchronous replication offers different commit acknowledgements only when configured with the documented synchronous standby and `synchronous_commit` settings; this protocol selects/configures none of them. See the PostgreSQL 18 documentation on [failover](https://www.postgresql.org/docs/18/warm-standby-failover.html), [warm standby and synchronous replication](https://www.postgresql.org/docs/18/warm-standby.html), [transaction isolation](https://www.postgresql.org/docs/18/transaction-iso.html), and [explicit locking](https://www.postgresql.org/docs/18/explicit-locking.html).

## Ambiguous results and operator-only handling

A claim, heartbeat, or finish error after a transaction begins is not proof that the transaction rolled back. If the connection or result is lost and commit status is unknown:

1. Treat ownership/job/history state as **unknown**, not failed or unclaimed.
2. Stop the worker from starting further workflow, filesystem, or provider side effects. Do not blindly retry the mutation or issue a provider write.
3. Do not create a replacement attempt because a lease expired or because one endpoint reports no row.
4. Require an operator to establish the authoritative database endpoint and inspect durable job, ownership, and attempt-history state before any disposition or further execution.

The isolated protocol exposes no runtime reconciliation or operator recovery command. This procedure is a deployment restriction, not an implemented recovery capability.

## Backup, restore, and schema policy

Backup and restore are outside the verified protocol contract. Any experimental recovery must be operator-only and must first stop/fence all workers and clients, enforceably fence the old primary, and establish exactly one authoritative writable endpoint before inspecting or changing ownership. A backup may omit acknowledged changes made after its recovery point; such a restore can lose generations or outcomes. Never interpret a restored lease as proof that an old worker has stopped. Do not resume work or permit a provider write until the operator resolves every potentially active/ambiguous attempt.

Initialization is fresh-schema-only. Existing partial or incompatible ownership/history tables fail closed. No schema-version ledger, migration, backfill, or compatibility path is provided. Schema upgrades are prohibited unless a separately reviewed upgrade policy and tests are adopted; do not change tables in place or claim rollback compatibility.

## Gate before production integration

Before this isolated protocol is treated as production HA/failover authority, or used to authorize automatic worker reassignment or multi-host recovery, a separate reviewed design must select and test:

- supported primary/replica/failover manager and topology, including enforceable old-primary/client fencing and split-brain prevention;
- durability, acknowledged-commit RPO, recovery-point selection, RTO, backup/restore, and ambiguous commit resolution;
- connection routing and pooler behavior, supported isolation/linearization semantics, and fail-closed behavior during stale reads or authority changes;
- schema upgrade, rollback, and minimum compatible binary policy;
- authenticated worker identity, process supervision, cancellation/drain, workspaces/artifacts, and external provider-write uncertainty handling;
- deterministic integration/fault tests of the selected production topology, not only a local primary.

Until those gates are satisfied, do not treat the isolated ownership protocol as a supported production HA/failover authority, or infer automatic reassignment from lease expiry. The current REST service supports memory, SQLite, and PostgreSQL persistence, including separately launched PostgreSQL workers, but does not provide a fleet coordinator or production failover/restore guarantees. Lease expiry remains suspect, never automatic reassignment authority under this protocol. This contract does not make containers a security sandbox or provide exactly-once workflow/provider effects.
