# Optional host-isolation reference

## Status and product boundary

Factory does **not** provide or guarantee sandboxing, security isolation, or protection from repository-controlled or model/harness behavior. Users and operators are responsible for assessing and accepting those risks in their deployment environment. Operators still control the repositories, harness profiles, credentials, and bounded job policy that Factory will accept; request content cannot override those settings or authorize arbitrary host commands, network access, or credential use.

This document is retained as optional deployment-hardening reference material for operators considering host isolation. It is not a Factory support contract, product requirement, prerequisite for job execution or the Factory-unit MVP, or evidence that a host/runtime is secure. A container, rootless runtime, worktree, process, or Kubernetes Pod does not create a Factory security guarantee. No sandboxed executor is implemented or supported.

## Optional Linux OCI considerations

The former candidate was a dedicated Ubuntu Server 24.04 LTS x86-64 host using systemd, unified cgroup v2, and rootless Podman with crun. It was never selected or validated as a supported profile. The notes below describe possible operator hardening considerations only; they are not Factory requirements or promises. Any operator choosing such a configuration must validate it for their own threat model and deployment.

Rootless operation may reduce host authority, but OCI workloads share the host kernel. Runtime version, user namespace configuration, cgroup delegation, filesystem, networking, seccomp, and LSM behavior depend on the host and operator configuration. See [Podman rootless mode](https://docs.podman.io/en/latest/markdown/podman.1.html#rootless-mode), [Podman runtime options](https://docs.podman.io/en/latest/markdown/podman-run.1.html), the [OCI Linux runtime configuration](https://github.com/opencontainers/runtime-spec/blob/main/config-linux.md), and [Linux cgroup v2 delegation](https://docs.kernel.org/admin-guide/cgroup-v2.html#delegation). These references do not establish or imply Factory support.

## Operator considerations (not Factory guarantees)

Operators evaluating host hardening may consider:

- Using a dedicated service account, private per-job storage, immutable operator-managed images, and avoiding mounts of unrelated repositories, home directories, runtime sockets, or credential files.
- Applying host-verified CPU, memory, process, storage, and wall-clock limits appropriate to the deployment. Factory's configured job bounds and worker concurrency are policy controls; a container/runtime configuration alone is not proof that host resource limits are enforced.
- Restricting network access with mechanisms enforced outside workload-controlled configuration. Proxy environment variables alone do not constrain a process. Protect metadata services, internal services, and provider write authority according to the operator's environment.
- Avoiding deliberate disclosure of control-plane, database, unit identity, or provider credentials to repository-controlled harnesses. This reduces intentional exposure; it does not guarantee hostile code cannot access authority available to the execution account.
- Treating worker cancellation, process supervision, job-scoped workspace handling, artifact validation, and provider-write reconciliation as lifecycle and correctness controls. They do not establish isolation from host or other processes.

Depending on the threat model, an operator may evaluate OCI, a VM/microVM runtime, or another host-level control. Factory does not select, test, or endorse one as a security boundary. A VM still depends on operator-managed hypervisor, host, network, storage, and credential configuration.

## Factory lifecycle and policy boundaries

Factory's product requirements remain separate from optional host isolation:

- The control plane and registered unit enforce operator-configured repository aliases, fixed enabled harness profiles, credentials, and bounded job policy. Request data does not select arbitrary executables, host paths, or credentials.
- Authenticated unit identity, attempt ownership/fencing, cancellation/recovery, and conservative handling of ambiguous provider writes protect job lifecycle and provider correctness. They do not guarantee hostile code is contained or cannot affect authority available to the execution account.
- Job-specific workspaces and bounded artifact validation prevent accidental cross-job or stale-generation mix-ups when implemented. They are not a security boundary against malicious local processes with sufficient account access.
- Users and operators decide whether their repositories, models, harnesses, and deployment environment are trusted for the authority available to Factory.

The current REST worker executes local processes with server-account authority. See the [implemented REST server guide](../features/rest-server.md), [worker identity contract](worker-identity-credential-contract.md), [worker recovery contract](worker-recovery-cancellation-contract.md), and [workspace/provider contract](attempt-workspace-provider-write-contract.md) for current behavior and future lifecycle requirements. None of these documents promises sandboxing.
