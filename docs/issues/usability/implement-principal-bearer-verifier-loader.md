# Implement the principal bearer-token verifier loader

- **Type:** Approved implementation opportunity recorded during the self-improvement cycle; not a user-reported usability finding.
- **Context:** The agreed REST-server direction requires per-user bearer credentials mapped to a Factory principal, but there is no API server or authentication mechanism yet. A narrow, server-independent verifier loader provides a tested security boundary without prematurely adding routes or OAuth behavior.
- **Desired outcome:** Factory can load a strictly validated startup configuration of principal IDs and SHA-256 digests for high-entropy bearer tokens, and resolve an incoming token to its principal without retaining raw tokens.
- **Scope:** Version-1 JSON `{ "version": 1, "principals": [{ "id": "...", "token_sha256": "<64 lowercase hex>" }] }`; explicit file path; bounded input; regular file; owner-readable with no group/other permissions (for example mode 0400/0600); reject unknown fields, trailing JSON, duplicate IDs or verifier digests, invalid IDs and digests; hash submitted tokens with SHA-256 and compare in constant time; configuration is immutable after load and token changes require process restart.
- **Non-goals:** HTTP server/routes, bearer middleware integration, GitHub App OAuth, principal-to-grant mapping, queue/request provenance, remote admission, worker execution, token-generation utility, hot reload, config documentation for deployment, container packaging, and changes to the separate `/tmp/factory-rest-server-mvp-plan` draft.
- **Acceptance criteria:**
  1. Valid configuration loads and a matching raw token returns only its configured principal ID.
  2. Empty/wrong tokens never authenticate; raw tokens are not retained or emitted in JSON/error messages.
  3. Missing, oversized, malformed, trailing-data, unknown-field, duplicate, invalid-ID/digest, symlink/non-regular, unreadable, or group/other-readable files fail closed.
  4. Tests verify constant-time comparison is used for matching, including matching and non-matching cases.
  5. Existing CLI behavior remains unchanged; `make test`, `make vet`, and `make build` pass.
- **Status:** Recorded before implementation; work is limited to this item on branch `feature/principal-bearer-auth`.
