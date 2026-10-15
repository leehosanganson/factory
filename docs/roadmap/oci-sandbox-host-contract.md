# OCI sandbox host contract

## Status and boundary

**No sandboxed executor is implemented or supported.** Factory's current REST worker executes local processes with server-account authority. A container, rootless runtime, worktree, or Kubernetes Pod by itself is not a security boundary. This document records a Linux-first candidate host contract and a strict no-go rule: untrusted work must not be launched until a concrete, tested platform profile and the trusted supervisor are implemented. There is no silent fallback to local execution.

The contract does not claim protection against a compromised Linux kernel, OCI runtime, host administrator, or operator-controlled image/policy. OCI shares the host kernel. If that residual risk is not acceptable for the intended threat model, compare a per-job VM/microVM before implementation rather than inflating container claims.

## Threat model

Treat task text, issue/PR/repository content, build scripts, dependencies, tests, and agent/harness behavior as hostile. They may attempt to read or modify host or other-job files, reach credentials/control-plane/provider endpoints, escape the container, misuse inherited descriptors, create persistent child processes, forge outputs, or exhaust host resources.

Protected assets include the host and control-plane state, other jobs' workspaces, API/database/worker/provider credentials, canonical checkouts and Git metadata, and provider write authority. The trusted computing base is the host kernel, rootless OCI runtime, supervisor, pinned image/policy, host network/storage enforcement, and trusted artifact/publisher path. Kernel/runtime vulnerabilities, host compromise, and administrator actions remain residual risk.

## Candidate platform profile—not an enabled support claim

The initial validation candidate is a dedicated **Ubuntu Server 24.04 LTS, x86-64** host using **systemd, unified cgroup v2, and rootless Podman with crun**. This is a profile to validate, not a claim that current Factory or every machine running those components is supported. Before any strict-mode implementation can be enabled, the project must record exact tested distro/kernel, Podman, crun, subordinate UID/GID, systemd delegation, filesystem, firewall, seccomp, and AppArmor versions/configuration in a versioned support matrix. Updates outside that matrix are unsupported until the conformance suite passes. macOS, other Linux distributions/architectures, rootful engines, Docker-compatible APIs, and Kubernetes are unsupported for untrusted execution in the first profile; they must be refused or remain explicitly trusted-only.

Rootless operation is defense in depth, not proof of isolation or resource control. Podman's own documentation notes cgroup/resource-limit behavior can depend on host delegation and that rootless networking/filesystem support has prerequisites. The host supervisor must probe the actual enforcement interfaces, not infer support from runtime name or version. See [Podman rootless mode](https://docs.podman.io/en/latest/markdown/podman.1.html#rootless-mode), [Podman runtime options](https://docs.podman.io/en/latest/markdown/podman-run.1.html), the [OCI Linux runtime configuration](https://github.com/opencontainers/runtime-spec/blob/main/config-linux.md), and [Linux cgroup v2 delegation](https://docs.kernel.org/admin-guide/cgroup-v2.html#delegation).

## Mandatory controls for a future strict profile

All controls below are conjunctive. A missing, unsupported, unconfigured, or unverified control means **refuse before starting workload code**. There is no local-process fallback.

### Identity and filesystem

- Use a dedicated non-root Factory service account, a rootless runtime under that identity, a per-attempt UID/GID mapping, and a private attempt directory. Do not allow untrusted workloads to share the Factory service account's home, runtime socket, credentials, host processes, or another attempt's paths.
- Use an operator-built immutable image referenced by digest; do not accept task-selected images, entrypoints, runtime options, mount paths, devices, or host executable paths. Pin and verify the image before launch.
- Make the image root filesystem read-only. Supply only an attempt-private workspace and explicitly sized temporary/artifact locations. Never mount host home, canonical checkout, `.git` metadata/object stores, API/config/database files, worker identity, provider credentials, runtime sockets, SSH agents, or unrelated repositories.
- Construct an attempt-private source snapshot from an operator-selected revision. Define and test treatment of submodules, Git LFS/filters, symlinks, ignored/untracked files, and hooks. Do not provide writable shared checkout or `.git` indirection. Results cross back only through a bounded artifact protocol with path, type, link, ownership, and size validation before a trusted integration/publisher step.
- Reject artifact traversal, symlink/hardlink escape, special files, path races, and metadata that could escape the attempt directory. Do not cleanup while the supervisor cannot prove the complete workload cgroup/process tree has stopped.

### Kernel isolation and privileges

- Require separate user, mount, PID, IPC, UTS, network, and cgroup namespaces; private mount propagation; a non-root workload UID; no capabilities; `no_new_privs`; no setuid/file-capability escalation; masked sensitive proc/sys paths; and a reviewed restrictive seccomp profile.
- Require an enforcing LSM profile (AppArmor for the candidate Ubuntu profile) with denial behavior verified on the host. A disabled, complain-only, unavailable, or bypassed LSM is unsupported.
- Do not use privileged mode, host namespaces, host devices, host PID/network, writable cgroup mounts, arbitrary bind mounts, or runtime daemon sockets inside the workload. Validate the actual created container configuration and kernel state before executing the entrypoint.

### Hard resource and storage limits

- Require verified cgroup v2 controller delegation and per-attempt hard CPU, memory, and PID limits. Check controller availability, effective limits, and the workload's placement in its dedicated cgroup; fail closed if the limit cannot be written and read back. Apply an independent wall-clock deadline in the trusted supervisor.
- Require a quota-capable per-attempt storage boundary (candidate: XFS project quota or an equivalently enforced dedicated filesystem/subvolume). The quota must cover workspace, scratch, writable layers, artifact staging, and runtime/supervisor logs. Put tmpfs under a hard size bound and within the enforced memory budget. If any writable surface escapes the aggregate limit, strict mode is unsupported.
- Bound and drain workload output, supervisor logs, request/response sizes, and artifact size. Accounting/alerts or post-run directory-size checks are not enforcement. Do not claim block-I/O limits unless the supported host mechanism is selected and tested.

### Network and credential boundary

- Default to no workload network. Package/dependency access, if approved later, must use a separate host-enforced default-deny egress design; proxy environment variables alone are not enforcement. A rootless runtime's default networking does not establish an egress security policy.
- Any future allowlisted proxy must be reachable only through the host-enforced network path and revalidate DNS answers, private/link-local/metadata ranges, redirects, IPv4/IPv6, reconnects, protocols, and byte/time bounds. Provider write endpoints, host control-plane/database endpoints, cloud metadata, loopback, and internal ranges are denied. Workloads never receive provider network credentials or direct provider-write authority.
- Keep worker authentication material in the host supervisor. The supervisor uses a narrow job/generation-bound authenticated protocol and validates each message. Do not pass API keys, database credentials, worker tokens, provider tokens, client keys, secret-bearing environment variables, or credential-bearing descriptors to workload code. Verify with hostile code inspecting environment, `/proc`, process arguments, file descriptors, mounts, and local sockets.
- Only a trusted publisher may push branches or create/update PRs after artifact validation and current job ownership checks. The publisher boundary depends on #248; OCI does not itself solve stale workers or ownership. If a provider write times out, disconnects, or may still complete, serialize and block competing writes for that job/branch; an immediate read-only lookup that finds no result is not proof that the prior write cannot complete. Do not blindly retry or permit another generation's provider write until the original operation is conclusively resolved or an operator explicitly dispositioned the uncertainty under #261. A current ownership check alone cannot make an uncertain external write safe.

### Stop, restart, and evidence

The trusted supervisor records the attempt and runtime/cgroup identity before execution. Cancellation and timeout must stop and verify the whole cgroup/container, not only the initial PID. After supervisor/runtime restart it must rediscover matching workloads and stop them or mark them suspect. If stop cannot be proved, leave the attempt suspect, retain evidence/workspace, block cleanup/reassignment/provider writes, and do not manufacture a terminal success/canceled result. This protocol depends on #248's ownership, cancellation/drain, and recovery work and is not implemented by this document.

## Required conformance/security tests before support

A local disposable runtime may support deterministic CI checks, but passing those tests is not evidence of kernel-escape resistance. The first supported profile must test:

1. Startup/admission refusal for wrong host/runtime version, non-unified cgroups, missing controller delegation, failed resource writes/readback, missing LSM/seccomp, unavailable quota, or failed network policy.
2. Cross-attempt filesystem/process isolation; read-only base; no host checkout, `.git`, home, runtime socket, secrets, or inherited descriptor; hostile environment/process/socket inspection.
3. Namespace/capability/`no_new_privs`/LSM/seccomp enforcement and attempts at mount, setuid, device, proc/sys, namespace, and syscall escape.
4. Traversal, symlink/hardlink, special-file, path-race, metadata, and size attacks against source snapshots and returned artifacts.
5. Exhaustion of each writable surface (workspace, scratch, `/tmp`, writable layer, tmpfs, artifact staging, logs), plus CPU, memory, PID, output, and wall-clock exhaustion; verify another attempt and the host remain within configured limits.
6. Network denial including direct IP, DNS rebinding, private/link-local/metadata/loopback, IPv6, redirect, proxy bypass, UDP/QUIC/raw socket, host/control-plane/database, and provider endpoints.
7. Cancellation with leaked child processes; supervisor restart/runtime outage; no cleanup or reassignment before full stop proof; retention and operator-visible failure when stop/cleanup cannot be verified.
8. Image digest drift, host config drift, cgroup/storage/network/LSM/seccomp prerequisite loss, and recovery from failed preflight without launching unsandboxed work.

A separate security review must assess the supported kernel/runtime version and threat model. Do not equate unit/CI conformance with resistance to unknown kernel or runtime vulnerabilities.

## Dependencies and decision gates

- #258 is now closed with a test-only, single-primary PostgreSQL ownership contract. It does not deliver production worker ownership, failover, or a sandbox.
- #259 worker identity depends on topology and credential custody; #260 cancellation/drain/recovery depends on worker identity and process containment; #261 private workspace/provider-write uncertainty depends on those protocols. No OCI rollout may bypass these gates.
- #247 submitter identity is not worker identity. Never give the workload the API credential that admitted the job.
- #249 Kubernetes packaging is downstream; a Pod/securityContext is not a substitute for the same cgroup, egress, storage, LSM, credential, stop-proof, and conformance controls.
- Before implementing, close #264's concrete support matrix and control-validation criteria: pin supported distro/kernel and exact runtime/build versions; select and test the host firewall/network-namespace enforcement mechanism and a versioned seccomp profile; select the quota-capable storage implementation and prove aggregate coverage of every writable surface; specify systemd/cgroup v2 delegation and AppArmor profile policy; and identify objective preflight evidence and fail-closed tests for each. Approve the OCI-vs-VM residual-risk decision under #250, and ensure #248 ownership/cancellation/provider handoff and #261 uncertain-write gates are implemented and validated.

Until then, keep current Factory execution boundaries unchanged and describe all local-process/container execution as unsandboxed/trusted-only.
