---
name: factory-first
description: >-
  Use Factory's implement workflow first for substantive engineering changes in
  this repository. Skip it for research, review-only work, and small isolated
  edits; never start a nested Factory workflow from an active Factory run.
---

# Factory-first engineering

Use this skill when working on engineering changes in this repository. Follow
[Factory operating guidance](../factory/SKILL.md) for the full command behavior,
safety constraints, and workflow details.

## Choose when to use Factory

Use `factory implement <description>` for substantive changes: features,
behavioral fixes, multi-file work, changes with meaningful acceptance criteria,
or work that benefits from requirements, implementation, review, and
documentation stages. Use `factory implement --gate <description>` when the user
wants explicit approval between stages and an interactive terminal is available.

Do not start a Factory implementation workflow for a simple question, research
or explanation, review-only request, mechanical one-line correction, or other
small isolated edit whose scope and verification are already clear. Do not use
`factory monitor` as a general implementation workflow; it is for bounded,
routine maintenance on an existing open PR. Use `factory tidy` only when the
requested task is a repository-wide review/fix/document/verify pass.

## Active Factory workflows

Before launching a workflow, determine whether this task is already being run
inside Factory. If it is, continue within that workflow: do not launch
`factory implement`, `factory pipeline`, a detached implementation job, or any
other Factory workflow recursively. A nested run duplicates requirements and
can contend with the active run's changes. If unsure, ask rather than starting
another worker. Mention this no-recursion decision when it affects the chosen
workflow.

## Execution and verification

1. Clarify the requested outcome, constraints, and acceptance criteria; do not
   treat agent-generated assumptions as user approval.
2. Run `factory implement <description>` from this repository for suitable
   substantive work. By default it starts a detached job and attaches to its
   output. Use the reported job ID to reattach or request cooperative stop; use
   `--gate` only when an interactive approval workflow is appropriate.
3. Inspect the resulting diff and run relevant repository checks, such as
   `make test`, `make vet`, and `make build`. A successful agent exit is not an
   independent correctness evaluation. Factory does not commit or publish
   implementation-workflow changes; do not claim otherwise.
4. Report the workflow used (including when recursion was avoided), checks and
   their results, and any friction encountered. Report friction only when it
   was observed and can be described with reproducible steps or concrete
   evidence; do not invent issues. Record only current, reproducible product
   pain points in `to-fix.md`, with evidence, and close findings that have been
   fixed or made stale.
