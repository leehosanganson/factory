# REST worker executor job-ID seam session

- **Scope:** Updated `restworker.Executor` to receive the claimed `restjobs.Snapshot`, including its opaque ID and request. Worker lifecycle, manager status handling, cancellation, and panic recovery remain unchanged; the HTTP request/response contract was not modified.
- **Verification:** Added behavioral coverage that the executor receives the claimed job ID, request, and running snapshot.
- **Friction:** The original checkout had unrelated local changes and its feature branch was no longer on origin. A clean worktree from fetched `origin/main` isolated the requested change without disturbing them.
