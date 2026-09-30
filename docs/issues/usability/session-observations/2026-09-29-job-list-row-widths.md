# Session observation: compact job-list rows

- The local job-list sample confirmed that bounding only target paths did not
  control total row width: output rows still reached 211 characters and the
  header reached 179. Choosing a 120-code-point row cap required deciding which
  diagnostic fields remained in the compact table; the implementation retains
  the full ID and status, with complete values and process-count diagnostics
  available through `job get <id> --details`.
- Focused tests, `make test`, `make vet`, `make build`, and `git diff --check`
  passed. One full test run that overlapped independent checks hit the existing
  TempDir cleanup race in `TestDetachedTidyCLIUsesDefaultDescriptionAndNeverPublishes`;
  the subsequent isolated full run passed.
- A built CLI sample measured the header and data row at exactly 120 characters.
