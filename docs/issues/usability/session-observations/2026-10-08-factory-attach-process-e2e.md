# Factory detached attach subprocess test

- Added a process-level regression test for a fresh `factory job attach <id>` following an already-running detached implementation job, checking observable progress before releasing the fake agent and successful attach completion afterward.
- The existing isolated Git/fake-agent approach was straightforward to reuse. A useful test detail discovered during execution: attach streams persisted workflow/progress output, while fake-agent stdout is retained in its stage log; assertions now check each source for its intended markers.
- Worker cleanup checks terminal status, cleared worker record, released target lock, and worker process exit. Failure paths preserve the temporary directory and identify its job/marker locations for diagnosis.
- The focused test passed three consecutive runs. The first isolated full `make test` run failed at the unrelated timing-sensitive `TestLatestActivityTruncationPreservesUTF8`; the retry passed. `make vet` and `make build` also passed. Isolation initially redirected Go's module cache into the temporary HOME; keeping the existing Go cache while isolating Factory HOME/XDG state avoided that cleanup issue.
