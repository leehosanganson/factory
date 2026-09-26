# Agent guidance

- This is a Go project. Follow existing conventions and architecture; consult
  `README.md` and nearby code when needed.
- Keep changes focused. Avoid unnecessary dependencies, speculative abstractions,
  and unrelated refactors.
- Preserve user changes. Do not overwrite, revert, or reformat unrelated work.
- For suitable substantive engineering work, follow [Factory guidance](.agents/skills/factory/SKILL.md).
- Use the Makefile targets `make test`, `make vet`, and `make build`. Run the
  checks relevant to your changes and report their results.
- Linux and macOS are supported. CI runs build, test, vet, and Go formatting
  checks on both platforms.
- Do not stage, commit, push, or merge changes unless the user explicitly
  authorizes it.
