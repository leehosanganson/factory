# Agent guidance

- This is a Go project. Follow existing conventions and architecture; consult
  `README.md` and nearby code when needed.
- Keep changes focused. Avoid unnecessary dependencies, speculative abstractions,
  and unrelated refactors.
- Preserve user changes. Do not overwrite, revert, or reformat unrelated work.
- For suitable substantive engineering work, use Factory's `implement` workflow by default. Read [Factory guidance](.agents/skills/factory/SKILL.md) and the user-facing [Factory usage skill](.agents/skills/factory-usage/SKILL.md) for workflow choice and limits.
- Use Factory's own local development build for regular, focused usability audits—not only user reports or ad-hoc subagents. Build with `make build`, then exercise `./bin/factory` through realistic workflows. Record reproducible friction in `docs/usability-issues.md`, distinguishing user reports from verified observations and capturing reproduction steps, impact, and evidence. Prioritize and propose one bounded issue at a time with desired outcome and acceptance criteria; get explicit user approval via `factory implement --gate <task>` before fixing it. If already in an active Factory run, continue there instead of nesting a workflow. After approval, review the diff, run relevant `make` checks, rebuild and validate with `./bin/factory`, update the issue's status and evidence, then repeat. Keep the user in control; do not treat Factory workflow results as a correctness verdict or `./bin/factory` as a versioned artifact.
- Use the Makefile targets `make test`, `make vet`, and `make build`. Run the
  checks relevant to your changes and report their results.
- Linux and macOS are supported. CI runs build, test, vet, and Go formatting
  checks on both platforms.
- Do not stage, commit, push, or merge changes unless the user explicitly
  authorizes it.
