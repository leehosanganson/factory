# Session observation: REST evidence capacity preflight

- **Type:** implementation-session observation (not a user report).
- **Context:** Fixed PR #121's bounded-registry review finding in `RecordVerificationEvidence`.
- **Observation:** A regression setup with an evidence write too large for available bytes plus all reclaimable terminal bytes exposed an evicted terminal record despite returning `ErrRegistryFull`. The new preflight rejects before mutation; the focused regression passes after the fix.
- **Friction:** No additional workflow friction observed in this focused code change.
- **Suggestion:** Keep bounded-registry rejection tests asserting retained records and idempotency state, not only the returned error and byte count.
