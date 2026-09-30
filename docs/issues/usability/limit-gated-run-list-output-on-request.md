# Limit gated-run list output on request

- **Finding:** `factory run list` prints every managed gated run, making recent
  runs harder to locate as the state directory grows.
- **Evidence:** A synthetic state with 25 managed run records produced 26 output
  lines (header plus every run). The local user's state had no gated runs, so
  this sample verifies list behavior rather than reproducing an existing large
  personal history.
- **Desired outcome:** Allow users to request only the newest N managed runs
  while preserving the current unbounded default and deterministic order.
- **Acceptance criteria:** `factory run list --limit <n>` shows at most N newest
  managed runs in existing update-time/ID order; omission remains unbounded;
  invalid limits report a clear error; help and tests cover the option. All
  records are still validated before limiting output.
- **Status:** Implemented in `internal/factory/managed_run.go`; list limits
  output after loading, validating, and sorting managed run records. Help,
  feature documentation, and behavioral tests cover limited/unlimited output
  and invalid options.
