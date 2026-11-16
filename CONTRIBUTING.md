# Contributing

Keep changes focused so they are easy to understand and review. Branches and pull requests should each cover a coherent, reviewable scope.

## Branch names and freshness

Use `type/short-kebab-case-summary`, with a concise type and a brief description of the work. For example:

- `feature/add-export`
- `docs/add-contributing-guide`

Before opening a pull request, update your branch from its target branch (usually `main`) so it is as current as practical. Fetch the latest target branch, integrate it using the repository's normal merge or rebase workflow, and check for merge conflicts. Resolve any conflicts and rerun relevant checks before requesting review. Repeat this if the target branch advances substantially while the pull request is open.

## Tests and documentation

Include relevant tests for changes in behavior. When a change affects behavior, update the documentation for the affected feature slice as part of the same pull request. Keep documentation partitioned by feature: add or update the focused page under [`docs/features/`](docs/features/) and update its [index](docs/features/README.md) when adding a page. Keep current behavior distinct from roadmap or other planned behavior.

## Pull requests

Use this title format:

`type(slice): description #PRNUM`

For example: `feat(rest-client): support job cancellation #123`. Use a Conventional Commit type (such as `feat`, `fix`, `docs`, but not `test` because tests should be a integral part of any PRs), name the affected feature slice in parentheses, write a concise description, and include the relevant pull request number in the `#PRNUM` position when available.

Describe the pull request with these sections:

- **What changed** — summarize the implementation or documentation changes.
- **Why** — explain the problem or need and the intended outcome.
- **GitHub Issue** — link the issue this work addresses. If there is no issue, say why or state that it is not applicable.
- **How to verify** — list the commands or steps a reviewer can use to verify the change, with results where available.
- **End-to-end test evidence** — record relevant end-to-end tests performed and their results. If none apply or could be run, say so and explain briefly; do not claim tests that were not run.

Keep each pull request scoped and reviewable; split unrelated work into separate pull requests where practical.

## Commit attribution

Commit authorship and co-authorship must identify human contributors only. Do not set a tool or agent as the commit author/committer or add an agent `Co-authored-by` trailer, including `Factory Agent`. Contributors remain responsible for their commits; review or tool assistance does not constitute authorship. Pull-request CI rejects Factory Agent attribution in commit author/committer metadata or commit-message trailers.
