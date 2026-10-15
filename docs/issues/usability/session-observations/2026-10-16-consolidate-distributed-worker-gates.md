# Session observations: consolidate distributed-worker gates

- After merged design PRs #270–#272, consolidated their statuses in #248 and added direct links to the worker identity, recovery/cancellation, workspace/provider-write, PostgreSQL topology, and OCI host contracts.
- Reinforced that design-document completion is not implementation readiness: current workers remain in-process; PostgreSQL protocol is test-only; OCI candidate is not an enabled sandbox; production worker identity, stop proof, private attempt workspaces, and publisher uncertainty handling remain unimplemented.
- Tried an inline multiline `gh issue comment --body` with Markdown backticks; shell interpreted backticks, so that command failed without posting. Retried using a `/tmp` body file and confirmed successful comment. No repository file was touched by the failed command.
- Issue-only reconciliation; no runtime/API changes. No additional usability observations.
