# Session observation: detached-job JSON output

## Context

Implemented and tested JSON inspection output for `factory job list` and `factory job get` in issue #223.

## Observations

- Existing detached-job filters, limits, and reconciliation were reused; JSON is encoded only after selection so list stdout remains a single JSON value.
- Persisted job records contain task text, absolute filesystem paths, publication summaries, monitor state, and other data that are not appropriate as an unreviewed export. A small explicit projection allowed the CLI response to omit those values and all logs.
- The process test could assert stdout/stderr and exit behavior independently from the command implementation, including a decoder check for trailing output.
- No unrelated command required changes. The feature documentation and command help had straightforward places to explain the schema and redaction boundary.

## Concrete improvement idea

Continue defining machine-readable CLI output as an explicit stable projection rather than serializing persisted records directly; tests should treat privacy and stdout purity as part of the schema contract.
