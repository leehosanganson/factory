# Earlier REST security-contract proposal — superseded

- **Status:** Historical design record. The route shape, OAuth/principal model, and durable-queue assumptions below were an earlier proposal and are superseded by the current unified REST job-service direction in the [roadmap](../../roadmap/roadmap.md) and [REST job contract](../../roadmap/rest-api-contract.md).
- **Current direction:** one REST job API and lifecycle. The server currently uses volatile in-memory job state; optional SQL persistence is recommended for restart durability. Creating/updating a pull request through the configured code-repository provider is required for a target-MVP implementation job to succeed. The current runtime does not yet implement SQL persistence or provider PR writes.
- **Historical scope:** This record captured an earlier proposal for `POST /work`, `GET /work/{id}`, and `GET /work/{id}/history`, OAuth initiation/callback, principal-scoped durable acceptance, and GitHub App permissions. Those are not the current canonical API or product requirements.
- **Retained design evidence:** The linked [credential-custody design](../../roadmap/credential-custody-design.md) preserves GitHub App OAuth research as a possible future credential option, not an approved MVP requirement. Provider credential and authorization choices must be made against the configured code-repository-provider contract.
- **Reason for supersession:** Product direction consolidated around the REST job service with an in-memory starter backend, optional/recommended SQL persistence, and required provider PR create/update. Issue polling is not a separate MVP; future issue intake, if pursued, should feed the same job lifecycle.

Do not use this historical finding as implementation guidance. Update the canonical REST contract and roadmap when product decisions change; preserve this record only as provenance for the earlier discussion.

## Earlier proposal record

The previous proposal documented a request-scoped `Idempotency-Key`, bounded history, optional issue plus instruction text, OAuth state handling, principal-scoped authorization, and encrypted GitHub App grants. It was marked merged as documentation work, but those decisions do not define the current product direction. Historical PR references and unresolved provider research are intentionally omitted from this active guidance to avoid mistaking implementation progress for the MVP vision.
