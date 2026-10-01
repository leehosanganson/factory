# Clarify OAuth callback grant-persistence recovery

- **Type:** Proposed documentation improvement; not a user-reported usability finding.
- **Context:** The REST API and credential-custody drafts require OAuth state to be consumed before exchanging the authorization code. The credential design says uncertain grant persistence should be reconciled before a new link, but does not define how to handle a present, absent, or unreadable record. Because the proposed state is single-use, this recovery boundary needs to distinguish grant persistence outcomes without replaying the callback.
- **Observed gap:** After callback state is consumed, a process may lose its response after attempting encrypted grant persistence. The current draft does not say whether an apparent missing or unreadable grant means “unlinked,” nor what to do if the valid grant was saved but the callback response was lost.
- **Desired outcome:** The design documents fail-closed, operator-understandable outcomes for confirmed grant persistence, confirmed absence in a healthy store, and unreadable/uncertain grant state, without changing OAuth or credential-storage implementation scope.
- **Scope:** Clarify callback persistence/recovery semantics in the REST API contract or credential-custody design, keeping retry behavior consistent with consumed single-use state and atomic key-rotation migration.
- **Non-goals:** Implementing OAuth routes, grant persistence/encryption, provider calls, runtime/deployment configuration, or changing the separate REST MVP plan worktree.
- **Acceptance criteria:**
  1. A valid persisted grant is treated as linked even when the callback response was lost; a consumed callback cannot overwrite or duplicate it.
  2. Confirmed absence from a healthy grant store requires a fresh authorization flow; the old code/state pair is never replayed.
  3. Corrupt, unreadable, key-inaccessible, or otherwise uncertain state is not treated as absence; fail closed for operator reconciliation.
  4. Recovery wording remains compatible with atomic, resumable key rotation and the one-active-grant-per-principal rule.
  5. The change is documentation-only and does not claim that OAuth, credential storage, routes, or admission are implemented.
- **Status:** Implemented as a design clarification on `docs/oauth-callback-recovery-contract`; the REST API contract and credential-custody design now describe the recovery outcomes. This does not implement OAuth or grant storage.
