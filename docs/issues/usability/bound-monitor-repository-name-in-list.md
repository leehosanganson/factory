# Show complete repository names in monitor lists

- **Finding:** The `REPO` column in `factory monitor list` uses a fixed minimum
  width but does not truncate longer repository names, which can shift the PR
  and update columns.
- **Evidence:** Source inspection of the list renderer in
  `internal/factory/monitor.go` showed the raw repository value was printed
  with a 28-character minimum width, not a maximum. Local monitored repository
  names fit the displayed width, so no overlong value was reproduced
  interactively.
- **Desired outcome:** Keep monitor-list rows aligned by bounding the repository
  column, while retaining the full owner/repository value in
  `factory monitor get`.
- **Acceptance criteria:** Long repository values in list output are truncated
  predictably with an ellipsis; shorter names remain unchanged; get output
  retains the complete repository name; tests verify both and table alignment.
- **Status:** Implemented in `internal/factory/monitor.go`: list output
  truncates repository values longer than 28 Unicode code points to a
  27-code-point prefix and ellipsis. The detailed repository field is unchanged.
  Behavioral coverage verifies long and short values and confirms the following
  columns remain positioned correctly.
