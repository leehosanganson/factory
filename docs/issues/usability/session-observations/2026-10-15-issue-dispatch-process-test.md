# Issue dispatch process-test observation

The process-level issue-dispatch test builds the compiled `factory` CLI, runs it as a subprocess with a temporary fake `gh` executable on its `PATH`, and sends its requests to the production issue-admission handler backed by a memory manager. The fake command returns a configurable issue response and records its arguments. The test verifies this flow:

- Preview makes no job-admission POST and leaves the manager empty.
- Explicit confirmation using the preview's matching version admits one job with the preview task and issue provenance recorded.
- Retrying confirmation at the same version reuses the existing job and the same idempotency key; no second job is admitted.
- Changing the fake `gh` response produces a preview with a different version.
- Confirming with the old version is rejected and admits nothing.
- Confirming again with the renewed version admits a distinct job with a distinct idempotency key and the new version in its provenance.

This exercises the compiled CLI-to-production-handler path using only a fake `gh`; it makes no live GitHub requests and uses no GitHub credentials. The test adds coverage only and does not change production behavior.
