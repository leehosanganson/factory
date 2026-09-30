# Session observation: human-direction retry fix

- **Type:** Implementation session observation (not a user-reported finding).
- **Observed:** The focused `RecordDirection` lifecycle test passed after adding retries after a newer observation and after closure, including conflicting-instruction retries.
- **Verification friction:** The first two `make test` runs failed during cleanup of `TestDetachedTidyCLIUsesDefaultDescriptionAndNeverPublishes` with a `TempDir RemoveAll` “directory not empty” error. Running that CLI test alone passed. The retry behavior itself was not implicated by the failure.
- **Change in this task:** Existing direction records are checked before lifecycle/latest-observation gates; absent records still require a current waiting/open/latest version.
