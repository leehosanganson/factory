# Bounded issue-provenance inspection observation

The REST job detail response includes the ordinary `request.task`, which can contain a dispatched GitHub issue body. The design for reliable intake calls for bounded provenance inspection without exposing the full body. I added a separate authenticated `GET /v1/jobs/{id}/issue-provenance` projection and `factory rest issue-provenance` command, leaving the existing detail response unchanged.

The projection contains only the job ID and the bounded provenance object. A job with no provenance returns the same sanitized 404 as an unknown job. Operators can inspect retained jobs; submitters can inspect only their own jobs, with unauthorized and unknown IDs indistinguishable. Handler, client, CLI, authorization, OpenAPI, and output tests cover this behavior. No persistence, admission, worker, or provider behavior changed.

Verification: focused package tests, full `make test`, `make vet`, and `make build` passed. The first full test attempt had one cleanup race in an unrelated detached-tidy test; a rerun passed. The change is committed as `ba4e1d1` and published in PR #276. Its Ubuntu, macOS, PostgreSQL 18 integration, and commit-attribution checks passed; it remains open and unmerged pending review. PR #275 merged while this independent slice was underway.
