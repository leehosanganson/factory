# Issues and usability findings

This directory records user-reported product findings and reproducible usability observations. Keep reports separate from verified observations; include current status, impact, reproduction steps, and concrete evidence where available. Do not infer a root cause from an observation alone, and update or close stale findings when behavior changes.

- [Usability issues](usability-issues.md) — stable landing page and contribution rules.
- [`usability/`](usability/) — one Markdown file per user-reported finding, plus independently named session-observation files.
- `docs/to-fix.md` — actionable code-review findings, only when a code review is requested or produced and that workflow applies. It is not the destination for routine usability-audit observations.

## Updating usability documentation

- Keep each finding in its own file under `usability/`. Edit that file when the
  finding changes; do not append findings to a shared list or renumber existing
  files. Use a unique, descriptive slug as the filename.
- Record a new finding by adding a new file. Do not edit the landing page or
  another finding merely to add a link. Browse the directory or use the stable
  landing page to discover the topic groups.
- Record each session's observations in its own
  `usability/session-observations/<date>-<topic>.md` file. Do not append to the
  historical archive or a shared rolling log. Keep user reports and session
  observations distinct.
- The landing page is intentionally stable; change it only when the structure
  or recording rules change. This keeps unrelated parallel edits in separate
  files and limits conflicts to concurrent edits of the same topic.

For a usability audit, build and exercise the local CLI through realistic workflows, then record concise, reproducible friction. Propose one bounded issue at a time with a desired outcome and acceptance criteria. Do not treat an agent's successful exit as validation or an observation as proof of cause.
