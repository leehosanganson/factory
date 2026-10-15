# Session observations: OCI host contract

- Confirmed PR #268 merged as `b658f2b`, and the Factory Agent attribution policy/check is now in `main`.
- Drafted a documentation-only Linux OCI host contract. It clearly marks the Ubuntu 24.04/x86-64 + systemd/cgroup v2 + rootless Podman/crun combination as a candidate for validation, not a selected/supported profile; exact versions, network mechanism, seccomp, quotas, LSM, and objective preflight evidence remain #264 gates.
- Checked proposed claims against Podman, OCI runtime-spec, and Linux cgroup v2 documentation. An independent review requested explicit future no-retry behavior for uncertain provider writes; added it and recorded the #250 update. No local/container execution behavior changed.
- Local Markdown references and `git diff --check` checked. Need run the independent review again, then commit/push/PR and monitor CI. No usability friction observed beyond the time needed to check linked runtime docs.
