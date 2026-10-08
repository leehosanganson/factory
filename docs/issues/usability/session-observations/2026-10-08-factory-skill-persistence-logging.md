# Factory skill persistence logging guidance

- Corrected two stale statements and the verification checklist in `.agents/skills/factory/SKILL.md`: after readiness, the runtime logs the selected persistence backend and never logs the SQLite path or secrets.
- Kept current REST-server behavior distinct from target MVP requirements; no code or feature documentation was changed.
- The focused Markdown-only task was straightforward to locate and scope. Local-link validation and `git diff --check` passed.
