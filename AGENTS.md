# Agent guidance

## Working in this repository

- This is a standard-library Go CLI with a local REST job server. Read `README.md`, the [implemented feature documentation](docs/features/README.md), the [target REST job contract](docs/roadmap/rest-api-contract.md), and nearby code before substantive changes.
- Preserve user changes. Keep changes focused, avoid speculative abstractions/dependencies, and do not reformat or alter unrelated files.
- For substantive engineering work, use Factory's [`implement` workflow](.agents/skills/factory/SKILL.md) by default. If already inside an active Factory workflow, continue there rather than starting a nested Factory workflow. Use a focused direct edit for small isolated fixes, questions, research, and review-only work; consult the skill for `tidy` and `monitor` limits.
- Clarify user goals, constraints, and acceptance criteria before consequential choices. Keep the user in control; request explicit approval for proposed usability fixes before implementation. Approval gates are opt-in unless requested.
- Treat successful agent exit as process success, not an independent correctness verdict. Inspect the resulting diff and run relevant checks.

## Positive feedback loop

Use Factory to build Factory: make regular, focused usability audits with a local development build, not only in response to user reports. Run `make build`, then exercise `./bin/factory` through realistic workflows. Record reproducible product friction in [usability issues](docs/issues/usability-issues.md), separating user reports from verified observations and capturing reproduction steps, impact, evidence, and current status. Store each finding in its own file under `docs/issues/usability/`; store each session's observations in a new dated file under `docs/issues/usability/session-observations/`. Do not append to shared issue lists or the historical archive. Propose one bounded issue at a time with a desired outcome and acceptance criteria; get explicit user approval via `factory implement --gate <task>` before fixing it. If already in an active Factory run, do not nest another workflow. After approval, inspect the diff, run relevant Make targets, rebuild and validate with `./bin/factory`, then update the individual issue file. Keep audits focused; do not treat `./bin/factory` as a versioned artifact.

At the end of each task, record concise session feedback about using Factory in a new, dated file under `docs/issues/usability/session-observations/`. Note what worked or caused friction, including bad surprises, and concrete ideas for improvement. Keep observations factual, distinguish them from user-reported findings, and do not infer causes without evidence.

Actionable code-review findings belong in `docs/to-fix.md` only when a code review is requested or produced and that workflow applies. Usability audits do not go there. See [issue documentation](docs/issues/README.md) for the distinction.

## Coding and verification standards

- Match existing Go style and architecture; avoid unnecessary dependencies and unrelated refactors.
- Add behavioral tests for changed logic, and integration tests for user-facing behavior when feasible.
- Use the Makefile targets `make test`, `make vet`, and `make build` as relevant. `make fmt` formats Go files; CI also checks formatting and runs build, tests, and vet on Linux and macOS.
- Do not stage, commit, push, or merge unless the user explicitly authorizes it.

## Repository structure

- `cmd/factory/` — CLI entry point and terminal-specific helpers.
- `internal/factory/` — workflow, job, monitor, configuration, agent execution, state, and safety logic.
- `internal/factory/prompts/` — embedded workflow prompts.
- `.agents/skills/` — agent procedures and workflow guidance.
- `docs/features/` — descriptions of capabilities implemented in the current CLI.
- `docs/issues/` — usability findings, session observations, and issue-recording guidance.
- `docs/roadmap/` — future direction and design notes; aspirational items are not claims of current behavior.
- `.github/workflows/` — CI and release automation.

The project has a local REST job server and CLI. The server operator selects memory (the default) or SQLite persistence in strict server JSON; configured SQLite fails closed when unavailable. The runtime does not log the selected backend or expose the SQLite path. After restart, queued SQLite jobs resume; interrupted running jobs require operator investigation and are not replayed. When GitHub is configured, PR publication is required for job success. Explicit authenticated reconciliation is limited to eligible failed SQLite jobs with a persisted provider attempt and retained workspace; it only confirms provider state read-only and does not run the harness or write to the provider. These are current server behaviors, distinct from the target contract's provider requirements. Do not describe a fleet manager, issue scheduler, generic arbitrary `run`, or chained jobs as implemented. Current behavior is documented in [features](docs/features/README.md); intended product direction is in the [roadmap](docs/roadmap/roadmap.md) and [REST job contract](docs/roadmap/rest-api-contract.md).
