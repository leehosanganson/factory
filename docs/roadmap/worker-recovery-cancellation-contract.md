# Worker recovery, cancellation, and drain contract

## Status and current behavior

**Future distributed-worker contract only; none of the remote recovery protocol is implemented.** The current REST server supports memory, SQLite, and PostgreSQL persistence; PostgreSQL may be shared by a workers-disabled API control plane and separately launched workers. All-in-one server workers remain the default, with no fleet coordinator. Queued jobs in durable stores resume after restart; durable jobs found `running` remain for operator investigation or eligible read-only provider reconciliation and are not automatically replayed. Current per-job cancellation is cooperative: queued jobs can be canceled before claim; running jobs receive context cancellation and remain running until their executor exits. Service shutdown stops claims, closes the manager, cancels workers, and waits only until the shared shutdown deadline. A process or child that ignores cancellation can outlive that wait; there is no durable remote process supervisor or stop-proof protocol. See [implemented REST server features](../features/README.md) and [REST operations](../features/rest-server-operations.md).

Only the isolated `PostgresOwnership` protocol fixture is test-only. Its lease expiry is not proof of process death, does not authorize reassignment, and ambiguous transaction/commit results remain unknown; it establishes no production failover/HA guarantees. This document does not add distributed recovery behavior or permit automatic replay/reassignment. The PostgreSQL Store, REST runtime, and separately launched worker support are existing behavior documented in the [PostgreSQL Store](../features/postgres-store.md) and [REST worker process](../features/rest-worker.md) guides.

## Safety objective and invariants

The control plane must never start a replacement attempt or allow a competing external side effect solely because a lease expired, a worker stopped renewing, cancellation was requested, a worker acknowledged a signal, or a single status lookup found no owner. It must establish both:

1. **Execution stop:** there is trustworthy evidence that the prior execution tracked by the worker/supervisor is no longer running and the supervisor will not restart it; and
2. **Side-effect disposition:** all provider/other external operations are confirmed complete and recorded, or remain conclusively blocked/resolved under #261. Process-stop evidence does not undo or classify an external request already accepted by a provider.

This protocol requirement is not a guarantee that hostile repository or harness code was contained, could not escape tracking, or could not obtain authority available to the execution account. Factory makes no sandbox or security-isolation guarantee; users/operators own repository and model threat risk.

If either condition is absent or uncertain, persist/retain an operator-visible `suspect` or `needs_operator` condition, keep attempt/workspace/artifacts for investigation, and block replacement execution and provider writes. Operator-configured repositories, harness profiles, credentials, and bounded job policy remain authoritative; requests cannot select arbitrary host commands/access or authorize credential use. Availability is subordinate to avoiding overlapping work and duplicate side effects. Do not claim exactly-once workflow or provider effects.

## State model

Keep job lifecycle state separate from append-only attempt state and external side-effect state. The following is the recommended target model, not a current schema/API promise:

- **Job:** `queued`, `running`, `cancel_requested`, `needs_operator`, then a verified terminal outcome (`succeeded`, `failed`, `canceled`). Existing API terminal states may be mapped without exposing internal supervisor detail.
- **Attempt (per monotonically increasing generation):** `claimed`, `active`, optionally `cancel_requested`, `stopping`, then `stopped` or `suspect`; a terminal attempt outcome may be recorded only after execution-stop evidence and side-effect disposition are durable. Attempt history is append-only; correction/disposition is a new auditable record, not rewriting prior events.
- **External operation:** `not_started`, `in_flight_or_unknown`, `confirmed`, `conclusively_not_applied`, or `operator_blocked`, with an operation identity and durable provider-attempt record. `not_started` may be asserted only from evidence that the trusted publisher never initiated the request; timeout/disconnect/lost acknowledgement is `in_flight_or_unknown`, not absence.

Generation allocation and `queued → running` are one authoritative transaction. Cancellation and claim race on the same authoritative state: exactly one wins. Each mutation compares the authenticated worker principal/session, boot ID, job, generation, and fencing token; stale generations cannot renew, mutate, or finish. These controls protect protocol state and private job data in the API, but do not guarantee hostile code cannot read or alter other data accessible to the execution account. See [worker identity and credential contract](worker-identity-credential-contract.md). The active worker may not finalize a job using only its local result when control-plane commit status is unknown.

Transitions:

- `queued → canceled`: a queued cancel wins before claim; no attempt or execution starts.
- `queued → running/claimed → active`: a claim wins atomically; cancellation then becomes a persisted request to the active owner.
- `running → cancel_requested → stopping → stopped → canceled|failed|succeeded`: valid only after stop proof and side-effect resolution determine the terminal outcome. If the workflow/provider effect completed successfully before cancellation, record the confirmed outcome rather than falsely reporting cancellation.
- `running/active → suspect → needs_operator`: used for lease expiry, worker/control-plane/database loss, heartbeat/commit ambiguity, unexpected worker exit, revocation, restart, drain timeout, or inability to prove process stop. It is not a terminal failure and does not imply safe retry.
- `suspect/needs_operator → stopped → queued/new generation`: prohibited by default. A future explicit recovery protocol may permit a new generation only after authoritative ownership is established, stop proof exists, every external operation is classified, workspace/artifact state is preserved or isolated, and an audited policy/operator action authorizes continuation. This contract does not define automatic requeue.

## Cancellation protocol

1. The API records a durable cancellation request against the job's current generation. The claim-vs-cancel transaction determines whether the job remained queued (then cancel without execution) or has a current owner (then signal only that owner).
2. The supervisor receives the cancellation through an authenticated, generation-bound channel. It stops starting new stages, requests cooperative cancellation of the harness, and asks its process supervisor to stop the tracked attempt processes under a bounded grace period. This does not establish containment from other processes or host authority.
3. The worker/supervisor keeps renewing ownership only while it is still able to perform the bounded stop and report trustworthy state. It must not begin new workflow/provider side effects after cancellation. The supervisor reports stop evidence and classifies any in-flight external operation; a worker's cancellation acknowledgement alone is not stop proof.
4. If the tracked attempt processes exit and side effects are resolved, the control plane atomically records attempt history, process-stop evidence, outcome, and final job status. If a provider operation may still complete, leave the attempt/job blocked as uncertain even if the workload process is stopped.
5. If the deadline expires, supervisor is unavailable, stop cannot be verified, or the control-plane commit result is ambiguous, transition to `suspect/needs_operator`; retain workspace and evidence, block cleanup/reassignment/provider writes, and require investigation. Do not report successful cancellation merely because a signal was sent.

A cancel request is idempotent for the same active generation; repeats must not create unbounded duplicate events or silently target a later generation. Cancellation after terminal completion does not mutate that job. An operator disposition is distinct from a worker claim or cancellation acknowledgement and must record actor, reason/category, timestamp, evidence references, and selected resolution without erasing attempt history.

## Drain and shutdown protocol

A distributed worker supervisor must:

1. Atomically enter `draining` and stop claiming new work before reporting readiness false for worker assignment.
2. Continue lease/session renewal for active attempts while it stops new stages and follows each attempt's bounded finish/cancel policy. Do not drop the control channel first and assume that this stops work.
3. Enforce an operator-configured drain deadline. At deadline, request bounded termination of every active attempt through the supervisor and collect evidence about the tracked processes. Do not treat the deadline as evidence of termination or as proof that hostile code was contained.
4. Retire the worker session only after no tracked attempt process is active, or mark all remaining attempts suspect with durable evidence and retain their workspaces. If the control plane is unavailable, continue supervising tracked processes; shutdown of the agent/control client must not trigger an intentional workload restart or provider operation.
5. On restart, reconcile supervisor/runtime inventory with durable attempt records. Stop or quarantine tracked orphan processes before accepting new claims. Unknown runtime identity, missing records, stale database authority, or unavailable supervisor means no reassignment and operator intervention; absence from inventory is not proof that untracked hostile code cannot continue.

A bounded wait is an operational response, not permission to close the database, discard the only process handle, clean workspaces, or start replacements while the workload may remain alive. If an implementation cannot maintain this invariant on its supported host, that host is unsupported for independent workers.

## Stop-proof requirements

Process-stop evidence must be generated and observed by a trusted supervisor outside the repository-controlled workload, tied to the exact attempt/generation and tracked process/runtime scope, and durably recorded. It is an operational recovery signal, not a security-isolation or hostile-code containment guarantee. The evidence should establish at minimum:

- the attempt-specific process/runtime scope was identified before launch and is distinguishable from a later generation;
- termination completed and the supervisor/runtime reports no remaining processes in the tracked scope; if the supervisor cannot account for descendants or detect continuation outside that scope, stop is unproven; this does not establish that hostile code could not escape tracking or access host authority;
- the supervisor can distinguish the old attempt from a new container/PID reuse and records an authenticated timestamped stop result;
- where process/runtime state is missing, contradictory, or inaccessible after reboot/restart, stop is **unproven**, not assumed from the absence of a runtime record;
- workload process stop is evaluated separately from filesystem writers, provider requests, and any other side effects that may outlive the process.

A lease timeout, heartbeat loss, database row update, `SIGTERM`/`SIGKILL` request, process-group leader exit, HTTP cancellation response, runtime stop command return without verified process-stop evidence, host reboot, or operator belief is not sufficient by itself. The process-stop protocol depends on a trusted supervisor and observable process/runtime state, not on a Factory-provided sandbox or host-isolation profile. If an accepted provider request can complete asynchronously after the process exits, only #261's operation reconciliation resolves that side effect.

## Outage, restart, and ambiguity behavior

- **Worker-to-control-plane partition:** worker cannot claim, renew, commit, or start provider writes without verified current ownership. The supervisor stops or quarantines tracked execution under its bounded policy. The control plane marks the attempt suspect; partition healing does not revive old authority or permit the old worker to publish.
- **Database/control-plane outage or ambiguous commit:** state is unknown. Worker stops initiating side effects and does not blindly repeat claim, mutation, cancellation, or provider operations. No new generation begins until the authoritative endpoint/state is established under #258's currently test-only, single-primary restrictions.
- **Lost claim/heartbeat/finish acknowledgement:** query only the authoritative control plane according to a reviewed idempotency/reconciliation protocol. If state cannot be conclusively established, leave suspect. A new claim token or new generation is not a safe generic retry.
- **Worker crash/reboot:** absence of heartbeat or host process does not classify provider state. Reconcile persistent supervisor inventory and external operations; otherwise retain suspect and request operator disposition.
- **Control-plane restart:** rediscover active/suspect generations and their supervisor identities before admitting claims. Do not infer safety from lease rows alone. If supervisor identity/evidence is absent, block the job.
- **Database restore/failover:** #258 prohibits production failover/restore claims. Never treat restored queued/running rows or rolled-back generations as evidence that an old worker is dead or that a provider request did not happen. Fence the former authority and all workers before any experimental operator recovery.
- **Credential revocation:** reject further worker requests and claims. Revocation does not stop the already-running process; use the trusted supervisor and require stop proof. If unavailable, mark suspect.

## Workspace, provider, and cleanup coupling

Each attempt generation must have a private workspace/scratch identity. Do not let an old or suspect attempt write into a newer generation's workspace. Cleanup is forbidden until exact attempt stop evidence and required evidence/artifact preservation are established. Workspace separation and credential custody are protocol/configuration practices, not guarantees that hostile code cannot access data or authority available to the execution account. Provider credentials are not intentionally passed to the harness; publication must serialize by job/branch and block competing writes while any prior external operation may still complete. An immediate read-only lookup returning absence does not prove a delayed write cannot complete. See #261 and the [candidate OCI host-control design](oci-sandbox-host-contract.md); it is neither a security-isolation guarantee nor a prerequisite for this recovery contract.

Database generation fencing does not fence a filesystem, a provider, or a host process. This contract therefore does not claim that a generation number, lease, Kubernetes replacement, or container boundary alone makes retry safe.

## Required deterministic tests before implementation gate

Tests must use independent control-plane and worker/supervisor processes, a disposable real test database for transaction paths, barrier hooks, a controllable process supervisor, and a deterministic fake provider that can accept a write and delay/lose its response. These are future tests, not current evidence:

1. Race queued cancellation and claim behind transaction barriers; exactly one wins. If cancel wins, assert zero execution. If claim wins, cancellation targets only that generation.
2. Cancel during each workflow stage; assert no subsequent stage/provider write begins, no terminal state before process stop proof, and terminal outcome matches confirmed side effects.
3. Cancel while a fake provider accepts a write then withholds the response; stop the harness, return a read-only lookup with no result, and assert the job remains uncertain with no competing/retry write.
4. Drain with queued, active, cancellation-cooperative, and cancellation-ignoring work. Assert no new claims after drain begins; renewal remains active during bounded stopping; deadline leads to suspect and no replacement; late worker results are fenced.
5. Keep worker A alive after lease expiry/partition while B requests work. Assert A cannot mutate or publish and B does not execute until supervisor process-stop evidence and side-effect disposition satisfy the policy. Include PID/runtime identity reuse and descendant processes that remain observable to the supervisor; do not interpret test results as proof of hostile-code containment.
6. Kill/restart worker, supervisor, and control plane at claim, heartbeat, cancellation, stop-proof persistence, provider request, provider confirmation, and terminal commit barriers. Assert unknown/ambiguous states remain suspect and history is append-only.
7. Revoke a worker credential during active work, disconnect it, and bring it back; claims/mutations fail, revocation is not treated as stop proof, and no new generation/provider write starts.
8. Inject database partitions, lost acknowledgements, stale authority, restored old snapshots, and unavailable process supervisor; assert fail-closed behavior and sanitized operator-visible diagnostics.
9. Attempt cleanup while a child/process boundary is active or suspect; assert cleanup is blocked. After verified stop and disposition, assert only that attempt's private data is eligible under retention policy.
10. Verify logs/audit include enough stable attempt/session/decision references for investigation but no task transcript, secrets, raw provider response, host paths in submitter projections, or fencing credentials.

CI fault injection proves protocol behavior only for tested components. It does not prove a security boundary or prevent hostile code from accessing authority available to the execution account. Production topology/failover and live provider behavior remain separate validation concerns.

## Dependencies and rollout gates

- #258: currently closed test-only PostgreSQL 18 single-primary contract. It provides no production recovery authority, failover, or reassignment.
- #259: worker principal/session authentication and revocation contract; PR #270 documents design only and must be reviewed/tested before authentication is implemented.
- #264/#250 sandbox or host-isolation validation is not a prerequisite or Factory guarantee. The supervisor must still produce reliable evidence about tracked process stop before recovery/reassignment; if it cannot, work remains suspect for operator handling. See the [Factory-unit MVP architecture](factory-unit-mvp.md) for the canonical target boundaries.
- #261: per-attempt workspace/artifact isolation and serialized provider side-effect uncertainty protocol.
- #248: must incorporate #259/#260/#261 and implement/test production ownership, generations, supervisor coordination, cancellation, and conservative recovery before any independent worker deployment.
- #247/#262: worker progress remains unavailable until safe ownership/recovery state is implemented; submitter auth is distinct.
- #249 Kubernetes packaging remains downstream and cannot substitute for process-stop evidence or provider resolution; Kubernetes does not imply Factory security isolation.

No automatic requeue, production Store/runtime integration, direct worker-to-database access, Kubernetes worker deployment, or exactly-once guarantee is authorized. Keep current single-server behavior and operator-only handling until all gates are implemented, reviewed, and tested.
