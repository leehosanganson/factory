# Fleet coordination — later direction

Fleet scheduling, remote workers, and a control plane are outside Factory's initial REST job-service MVP. The MVP is a single service with an in-memory development backend, optional/recommended SQL persistence for restart durability, and required PR create/update through a configured repository provider.

Do not design fleet ownership or reassignment until durable job state, worker fencing, recovery, and uncertain provider-write reconciliation have been implemented and validated. A container restart or expired lease is not proof that a previous worker stopped or that replay is safe. Containers and Nix shells are not security sandboxes.

If scaling needs justify a fleet later, define a shared durable coordination boundary, authenticated worker identity, fencing, resource placement, artifact/log retention, and recovery behavior as a separate proposal. This is not an MVP commitment. See the [roadmap](roadmap.md) and [REST job contract](rest-api-contract.md) for canonical direction.