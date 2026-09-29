# Usability issues

This page is the stable landing page and contribution guide for usability
findings. Findings live in separate files under [`usability/`](usability/) so
unrelated updates can proceed without editing the same list. See the [issues
index](README.md) for the distinction between usability observations and code
review findings.

## Record findings independently

- Keep each user-reported finding in its own Markdown file with a unique,
  descriptive filename. Do not renumber files or append to a shared list.
- Include the report, evidence, impact, desired outcome, acceptance criteria,
  and current status as relevant. Separate user reports from verified facts;
  do not infer causes from observations.
- Update or close the existing topic file when implementation changes its
  status. Avoid editing this landing page to add links for each finding.

## Record session observations independently

- Put each session's factual observations in a new file under
  [`usability/session-observations/`](usability/session-observations/), using a
  date and short topic in the filename.
- Do not append to the historical archive or a shared rolling log. Keep
  observations distinct from user-reported findings, note what worked or caused
  friction, and do not infer causes without evidence.

The original findings and session observations are preserved in their
individual topic files and the [historical observation archive](usability/session-observations/archive.md). New finding records—including those for active PRs—are added in separate files under `usability/`; they are not indexed here, avoiding a shared link-list edit on every addition.
