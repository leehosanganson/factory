# Bounded issue-provenance inspection observation

The REST job detail response includes the ordinary `request.task`, which can contain a dispatched GitHub issue body. The design for reliable intake calls for bounded provenance inspection without exposing the full body. I added a separate authenticated `GET /v1/jobs/{id}/issue-provenance` projection and `factory rest issue-provenance` command, leaving the existing detail response unchanged.

The projection contains only the job ID and the bounded provenance object. A job with no provenance returns the same sanitized 404 as an unknown job. Operators can inspect retained jobs; submitters can inspect only their own jobs, with unauthorized and unknown IDs indistinguishable. Handler, client, CLI, authorization, OpenAPI, and output tests cover this behavior. No persistence, admission, worker, or provider behavior changed.

Verification: focused package tests, full `make test`, `make vet`, and `make build` passed. The first full test attempt had one cleanup race in an unrelated detached-tidy test; a rerun passed. Changes remain uncommitted in a separate worktree while PR #275 is open and awaiting review.
