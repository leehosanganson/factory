# REST MVP result lifecycle contract review

- **Type:** Session observation; not a user-reported finding.
- **Observation:** At the time of this design update, the REST roadmap described process-local job status/history and did not specify how a successful workspace should remain inspectable or be safely cleaned up after restart. The server runtime and multi-stage execution were added later. This historical observation does not define the current target persistence or publication requirements.
- **Outcome:** Updated the proposed REST contract and execution-mode note to specify multi-stage execution, job-wide limits, nonpublication, and a fail-closed successful-result retention/cleanup lifecycle, while keeping all future capability language explicitly proposed/not implemented. No server code was changed.
- **Caveat:** The protected completion metadata is intentionally outside the disposable job workspace. If that metadata is lost or unreadable, or a directory cannot be safely identified, cleanup must retain it for operator inspection. Startup/background sweeps are periodic and may remove eligible results after (not exactly at) 24 hours.
- **Verification:** Diff and local Markdown links checked. No code tests were needed for the docs-only change.
