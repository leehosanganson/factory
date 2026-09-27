# Fleet manager design

## Purpose and current state

This is a concise architecture brainstorm for a future Factory fleet. It follows the roadmap: Factory is the durable workflow runtime; `factory-control` coordinates instances. Today Factory is a Go CLI with local state and detached jobs. There is no Factory server, container image, or fleet. Kubernetes owns container lifecycle once containerized; remote deployments remain the responsibility of their existing operators and platforms.

The design goal is to run independently scaled Factory servers—including servers on remote hosts—while allowing each Factory instance to accept jobs for many projects. A job selects its project's Nix development environment; it does not require a separate Factory instance or a change to Factory's own global runtime environment.

## Topology and authority

```text
Clients / schedulers
        |
        v
factory-control  <---->  Durable coordination store
  request intake,           jobs, attempts, leases,
  dedup, placement,         events, checkpoints
  fleet overview                   ^       ^
        | outbound authenticated   |       | durable updates
        | control connection       |       |
        +--------------------------+       |
                     Factory server instances (scale independently)
                     (local Kubernetes containers or remote hosts)
                                  |
                         isolated per-job workspace
                         agent + job commands in the
                         project's locked Nix dev shell
```

- **`factory-control`** accepts and deduplicates requests, matches queued work to worker capability and available capacity, and provides fleet-wide job discovery and control. It coordinates but does not own workflow stages or duplicate Factory's retry logic.
- **Factory server instances** accept assignments, execute workflows, and own stage transitions, agent sessions, cancellation, and bounded recovery from recoverable workflow failures. Each instance can serve multiple projects and concurrent jobs within configured capacity; instances scale independently.
- **The durable store** is authoritative for job and attempt records, ownership leases, events, checkpoints, and cancellation requests. Neither a control-plane memory view nor a worker's local files are authoritative. Logs and artifacts need durable references and a retention policy; whether their contents share the coordination store is an implementation decision.
- **Kubernetes** starts, restarts, scales, and terminates local Factory containers. A Kubernetes restart is a lifecycle action, not proof that a job is safe to rerun. Remote deployments are externally managed but use the same worker/control contracts.

Workers should initiate an authenticated outbound control connection, by polling or a persistent connection, to support NAT and remote hosts without requiring inbound access to each worker. The control plane authorizes assignments and reports per worker identity. Transport, identity provider, and credential rotation are open implementation choices.

## Job flow and Nix environments

1. A client or scheduler submits a request with a deduplication key. The control plane validates it and records a durable queued job before acknowledging acceptance.
2. The request identifies a repository and ref, plus either a project flake or a remote flake reference. The control plane places the job on a Factory instance with capacity and the required basic capability to execute it.
3. Factory resolves the source and creates an isolated workspace for that job. It resolves and locks the requested flake, then runs the configured agent and job commands inside that project's Nix development shell. This shell is a per-job process environment; it does not replace or mutate the Factory server's global shell.
4. Factory persists workflow progress, events, and recovery checkpoints as it executes. Jobs have explicit concurrency and resource limits. On completion or final failure, workspace cleanup follows a configurable retention policy suitable for debugging and audit.

The Factory image should include Factory, Pi/the default agent, the Nix runtime, and minimal common tools needed to start jobs. Git, `gh`, language toolchains, and Make are included or made available according to supported workflows, not treated as a fixed exhaustive image package list. A shared Nix store or daemon can cache fetched sources and build outputs across jobs on an instance. It is a cache, not shared mutable job state: do not expose another job's workspace or writable project state. Cache access and cleanup must not let one job alter another job's execution inputs.

Selection should initially match only the capabilities needed to run a request and honor available capacity. Project-specific flakes provide most toolchain selection; avoid making an elaborate profile taxonomy a prerequisite. Resource and isolation requirements can be added where deployment needs demonstrate them.

## Durable ownership and failure recovery

A worker claims an attempt through the durable coordination boundary and receives a time-bounded lease and monotonically increasing fencing generation. Writes that change authoritative attempt state are accepted only from the current generation. This prevents a stale worker from reporting success after a replacement has claimed work, but a lease cannot stop a paused or partitioned process from performing external side effects.

Factory handles recoverable workflow errors itself according to bounded workflow policy and records the resulting progress or outcome. The control plane does not run a second retry loop for these failures. If a container or host disappears abruptly, control-plane reconciliation may consider recovery or reassignment after lease expiry and inspection of durable state. Kubernetes restarting the container does not by itself authorize a second execution.

Resume or reassignment is allowed only from a durable checkpoint where the prior attempt is fenced and the relevant work is known to be safe to resume—such as an idempotent operation or a well-defined checkpoint boundary. If a prior attempt may have changed an external system and the result is uncertain, require reconciliation or operator action rather than blindly replaying it. Network partitions make heartbeat loss imperfect evidence of death; fencing and side-effect handling are essential, not optional scheduler heuristics.

The milestone gate is to prove that one server can persist progress and recover or classify interrupted work before enabling fleet-level reassignment. The format of checkpoints and the operations safe to replay must be defined with Factory's workflow semantics.

## Reproducibility and trust

A user-provided flake or fetched remote flake is executable build configuration. Record the flake source and requested ref, resolved revision, `flake.lock` hash, and an environment identity with the job so an attempt can be audited and its environment reproduced. Apply a defined policy for evaluating/building flakes, including whether sources must be reviewed or pinned before execution. Pinning improves reproducibility but does not make a malicious flake safe.

Nix development shells configure process environments; they are not a security sandbox. Do not provide secrets to evaluation/build steps unless necessary. Scope repository, provider, and control-plane credentials to the job and required operations, and avoid exposing credentials in logs or artifacts. The level of stronger isolation between mutually untrusted jobs—such as separate OS identities, VMs, or dedicated hosts—remains a key product/security decision. Neither a container nor a Nix shell should be represented as sufficient isolation without an explicit threat model.

## Minimal rollout and open decisions

1. Add server-mode persistence and ownership to Factory; validate concurrent jobs, durable checkpoints, fencing, and process restart/recovery on one instance.
2. Package the server and run it under Kubernetes for local lifecycle management. Validate per-job workspaces, project flake resolution, retention, and shared-cache behavior.
3. Add `factory-control` request deduplication, worker registration/capacity, outbound authenticated connections, durable assignment, fleet overview, and safe reconciliation. Exercise remote workers, partitions, stale attempts, manager restart, and uncertain side effects before enabling automatic reassignment.

Keep the first version focused. Decide the durable store and its consistency/availability guarantees, the worker identity and connection protocol, the checkpoint and idempotency contract, flake trust/evaluation policy, and minimum isolation level before production fleet use. Logs/artifact retention and recovery requiring human approval also need explicit policies. Scheduled issue sources and a web application can build on these APIs later; they are not prerequisites for the execution architecture.
