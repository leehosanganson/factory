# GitHub issue intake ownership

The GitHub webhook and issue poller use a PostgreSQL registry to enforce one owner for each exact, case-sensitive `(source_repository, trigger_label)` pair. Both adapters must use the same PostgreSQL job database. A pair must be registered before an adapter starts. A missing database registration, wrong owner kind, stale generation, or unavailable database makes startup or admission fail closed. Multiple webhook server replicas can use the same registration. The issue poller keeps its existing single-instance database guard.

Register an owner with a small private config file containing the canonical job database DSN:

```json
{"postgres_dsn":"postgres://factory:REDACTED@db.example/factory"}
```

```sh
factory intake-owner register --config /etc/factory/intake-owner.json --source acme/issues --label 'factory:ready' --kind poller
factory intake-owner inspect --config /etc/factory/intake-owner.json --source acme/issues --label 'factory:ready'
```

The server config must include the registered generation for each webhook source, using `github_webhook.owner_generations`. The poller reads its current registration on startup. To transfer ownership, stop the old adapter, reconcile its source-side effects, then record an explicit attestation:

```sh
factory intake-owner transfer --config /etc/factory/intake-owner.json --source acme/issues --label 'factory:ready' --kind webhook --attest 'poller stopped; pending label effects reconciled'
```

A transfer increments the generation and appends an audit record. It does not automatically fail over or take ownership when a process disappears. The attestation is an operator statement, not machine proof. The registry transaction locks the pair and the same transaction used for Thread/Job admission; an admission from an old generation cannot commit after transfer commits. A transfer waits for already-locked admission transactions to finish.

This protocol only coordinates participating binaries using the same database. Different PostgreSQL databases, old binaries, or external GitHub activity cannot be detected or fenced remotely. On migration, stop old binaries first and reconcile any in-flight GitHub side effects. SQLite webhook service mode is no longer supported; existing SQLite webhook data remains readable by the job-store package but must be migrated operationally before enabling the system-enforced PostgreSQL path.

The system prevents concurrent system-controlled admission through the two participating adapters. It cannot guarantee that an event already sent by GitHub, a label already removed by the poller, or an old process outside the shared database has become quiescent. Never use registration transfer as a substitute for stopping processes and checking external effects.
