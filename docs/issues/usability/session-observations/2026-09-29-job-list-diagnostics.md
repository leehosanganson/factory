# Session observation: job-list diagnostic documentation

- `./bin/factory job list --limit 5` showed compact columns for ID, type,
  status, description, activity, publication, and target, without status-call or
  Pi-process counts. Source inspection confirmed those counts are printed by
  `writeJobSummary` in `job get --details`, while the feature docs incorrectly
  attributed them to compact list/get output.
- Correcting the feature description and tracking the observed mismatch were
  documentation-only. `git diff --check` and Markdown/source consistency review
  passed; behavior was already covered by job trace tests.
