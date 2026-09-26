---
name: factory-usage
description: >-
  Help users choose Factory workflows and safely manage detached jobs. Use when
  starting or managing Factory work; do not invoke nested workflows inside an
  active Factory run.
---

# Using Factory

Choose the narrowest workflow that fits the request:

- **`factory implement <description>`** — substantive engineering work. It
  starts a detached implementation job and attaches to its output. Use
  `--gate` for an interactive foreground run with stage approvals.
- **`factory tidy`** — repository-wide review, fixes, documentation, and
  verification. Pristine mode can publish only after explicit confirmation;
  dirty mode does not publish.
- **`factory monitor <description>`** — bounded, routine maintenance for an
  existing open PR. It is not a general implementation agent and may publish a
  guarded routine fix.
- **No Factory workflow** — research, review-only work, and small isolated
  edits. If already inside an active Factory run, continue that run rather than
  starting another workflow.

Keep the requested scope and decisions with the user. Approval gates are
opt-in where supported; automation does not replace inspecting changes or
running relevant checks. Factory workflows do not sandbox configured agents or
their tools. Report only observed, reproducible friction, with concrete
evidence; do not expand scope to address adjacent issues.

## Detached jobs and concurrency

Start an implementation or monitor job with `factory job start <type>
<description>`. Use `factory job list`, `get`, `logs`, `attach`, and `stop` to
inspect or control jobs. The agreed canonical inspection interface is
`factory job get <id> [--details]`: concise, tabulated output by default and
long values/session detail with `--details`. `factory job show <id>` remains a
compatibility alias. The same interface applies to run inspection
(`factory run get`, with `show` as an alias) and monitor inspection
(`factory monitor get`, with `describe` as an alias). Current list output is
tabulated; individual CLI summaries are concise labeled fields, and tabulating
those summaries remains outstanding.

Separate implementation jobs may run concurrently when they target different
canonical repository paths. Factory rejects a new active implementation job
for a canonical target already claimed by an active job; same-target work does
not queue or run concurrently. Canonical paths account for path aliases such as
symlinks.

Do not infer that all job types or workflow modes support the same concurrency
rules. Monitor jobs have a separate PR-specific duplicate guard. The optional
parallel implementation setting concerns implementation subtasks within a
workflow; it is distinct from starting multiple detached jobs. Consult
`factory help` and the README for current command details and limits.

## Inspecting jobs

```sh
factory job list
factory job get <id>
factory job get <id> --details
factory job logs <id> --follow
factory job attach <id>
factory job stop <id>

factory monitor list
factory monitor get <id>
factory monitor get <id> --details
```

For monitor management, `factory monitor describe <id>` remains a compatibility
alias for `get`. Monitor actions such as `approve`, `reject`, `stop`, and
`reset` remain separate commands. A job stop is a cooperative cancellation
request; it is not a promise that every process is forcibly terminated.
