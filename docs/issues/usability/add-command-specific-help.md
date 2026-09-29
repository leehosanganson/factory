# Add command-specific help

- **Finding:** Root help is broad when users need instructions for a particular
  workflow.
- **Desired outcome:** Layered help so, for example, `factory tidy --help`
  documents `tidy` without unrelated workflow details.
- **Status:** Implemented in the current CLI source: `main.go` recognizes
  command-specific `-h`/`--help`/`help` requests for workflow and management
  commands. The integrated tree is verified by the current `make test` and
  `make vet` checks.
