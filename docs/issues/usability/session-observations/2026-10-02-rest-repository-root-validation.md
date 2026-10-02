# REST repository root validation session observations

- **Scope:** Added explicit startup-callable Git repository root validation and changed the default REST listener to `127.0.0.1:8080`. At the time of this observation no REST server entry point existed; the runtime was added in a later change. No HTTP/API or job-manager code was changed in this slice.
- **What worked:** `git rev-parse --show-toplevel` gives a direct way to distinguish a working-tree root from a subdirectory and supports linked worktree roots. Temporary repositories made these cases straightforward to cover.
- **Friction observed:** The repository has no existing server startup path, so integration at the listen boundary cannot yet be tested. The new public config method documents the required future call. Git was available in this environment and the tests exercise it directly.
- **Status:** Focused tests cover exact roots, linked worktrees, non-Git directories, subdirectories, symlinks, missing/noncanonical paths, bare repositories, alias-based error identification, and path redaction.
