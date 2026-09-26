# Agent guidance

- This is a Go project. Follow existing conventions and architecture; consult
  `README.md` and nearby code when needed.
- Keep changes focused. Avoid unnecessary dependencies, speculative abstractions,
  and unrelated refactors.
- Preserve user changes. Do not overwrite, revert, or reformat unrelated work.
- For suitable substantive engineering work, use Factory's `implement` workflow by default. Read [Factory guidance](.agents/skills/factory/SKILL.md) and the user-facing [Factory usage skill](.agents/skills/factory-usage/SKILL.md) for workflow choice and limits.
- If already working inside an active Factory run, continue that run; do not start a nested Factory workflow. Skip Factory for research, review-only work, and small isolated edits. Keep the user in control of scope and approvals. Record concrete, reproducible usability issues you discover in `docs/usability-issues.md`, clearly distinguishing user reports from observations you verified; do not expand the active task to fix them unless approved.
- Use the Makefile targets `make test`, `make vet`, and `make build`. Run the
  checks relevant to your changes and report their results.
- Linux and macOS are supported. CI runs build, test, vet, and Go formatting
  checks on both platforms.
- Do not stage, commit, push, or merge changes unless the user explicitly
  authorizes it.
