# Session observation: dashboard reduced motion

- **Verified:** selecting a job scrolls its details into view; the dashboard now uses automatic scrolling when the browser requests reduced motion and smooth scrolling otherwise.
- **Verification:** the focused Go regression test checks both preference outcomes in the embedded dashboard JavaScript without a JavaScript runtime dependency; serial full Go tests, `make vet`, `make build`, `node --check`, and `git diff --check` passed with XDG state isolated under `/tmp`.
- **Scope:** no worker progress or distributed worker behavior was changed.
