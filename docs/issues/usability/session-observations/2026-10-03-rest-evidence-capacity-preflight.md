# Session observation: REST evidence capacity follow-up

- **Type:** implementation-session observation (not a user report).
- **Context:** PR #121, which added durable REST verification evidence, merged before this separate follow-up. This branch transfers only the registry-capacity fix from its child commit.
- **Observation:** A regression setup with an evidence write too large for available bytes plus all reclaimable terminal bytes exposed an evicted terminal record despite returning `ErrRegistryFull`. The preflight rejects before mutation; the focused regression, `go test ./internal/restjobs`, and the full suite passed on this follow-up branch.
- **Friction:** `make help` is not a defined target; the available targets were read from the Makefile. The first `make test` run had a one-off failure in `TestRESTServerProcessRecoversUncertainProviderCreateWithoutDuplicate`; its focused rerun and the subsequent full-suite rerun passed.
- **Suggestion:** Keep bounded-registry rejection tests asserting retained records and idempotency state, not only the returned error and byte count.
