# Session observations: REST parallel subtasks macOS CI

- The PR's macOS CI log exposed three helper subprocess exits as status 12 but hid the runtime error, leaving diagnosis blocked from the assertion alone.
- The REST workspace manager rejects noncanonical managed paths. The new subprocess fixtures passed `XDG_STATE_HOME` directly from `t.TempDir()` without resolving its parent; the fix canonicalizes that parent before building the state path.
- Added a symlink-based regression for state-home canonicalization and enabled runtime error reporting in the affected helpers. The focused E2E cases and full local verification gates passed on Linux; macOS could not be rerun in this environment.
