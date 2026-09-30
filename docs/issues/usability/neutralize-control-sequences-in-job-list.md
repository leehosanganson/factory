# Neutralize terminal control sequences in compact job output

- **Finding:** Compact job-list descriptions are read from persisted task
  metadata and emitted without removing ANSI escape sequences, allowing such
  metadata to manipulate or disrupt a terminal display.
- **Evidence:** A local behavioral test created a completed job with task text
  `task\x1b[31mRED\x1b[0m`; `factory job list` emitted the raw `ESC[31m` and
  `ESC[0m` sequences in the `DESCRIPTION` field. Source inspection confirms
  `writeJobTable` normalizes whitespace but does not apply the existing
  `terminalSafeText` filter.
- **Desired outcome:** Render persisted descriptive text safely in compact job
  output while preserving ordinary readable content.
- **Acceptance criteria:** Job list/get table text strips terminal control
  sequences from task description and activity fields; ordinary text remains
  readable; detailed metadata remains available as persisted (no mutation of
  records). Tests cover CSI and OSC controls.
- **Status:** Implemented for compact job-list/get text using the existing
  `terminalSafeText` filter; stored records and detailed metadata remain
  unchanged. Regression tests cover CSI and OSC sequences.
