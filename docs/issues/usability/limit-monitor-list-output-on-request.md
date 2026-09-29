# Limit monitor-list output on request

- **Finding:** `factory monitor list` prints every persisted monitor, making
  recent monitor jobs harder to find in a long history.
- **Evidence:** A local CLI audit ran `./bin/factory monitor list` against the
  configured state store and observed 12 historical monitor records.
- **Desired outcome:** Allow users to request only the newest N monitor jobs
  without changing the existing unbounded default or persisted records.
- **Acceptance criteria:** `factory monitor list --limit <n>` prints at most the
  N newest monitors in newest-first order; omission preserves existing output;
  invalid limits return a clear usage error; help and behavioral tests cover
  the option.
- **Status:** Implemented in `internal/factory/monitor.go`; the complete
  persisted monitor list is loaded and sorted newest-first before output is
  truncated. Both command-specific and canonical monitor help document the
  positive integer option. Behavioral tests cover truncation, order, unlimited
  default, and invalid arguments.
