# Improve unit-test seams with mock dependencies

- **Finding:** The user wants modules and tests structured for unit testing
  with mocked dependencies.
- **Desired outcome:** Add narrow dependency seams where they enable
  deterministic unit tests, while retaining integration tests for actual
  process, filesystem, Git, and CLI contracts. Avoid a broad rewrite or mocks
  that erase important end-to-end behavior.
- **Status:** Pilot implemented in `internal/factory/workflow.go`: pipeline
  orchestration accepts a narrow runner, and the production process adapter is
  supplied by both workflow and automatic-publication call sites. The adapter
  retains context-aware subprocess execution and platform process cancellation;
  orchestration retains transcript, state, and event handling. No wider
  interfaces or refactors were introduced.
- **Acceptance criteria:** Fake-runner unit tests cover success, nonzero exit,
  process-start failure, cancellation classification, and exact
  argument/workdir forwarding. Existing subprocess integration tests remain
  and continue to cover real process output, start/nonzero failures,
  cancellation, workdir, argument ordering, and shell-free execution.
- **Evidence:** The fake-runner cases verify persisted check outcomes,
  completed lifecycle events, transcript output, and exact command/workdir
  forwarding. A missing-executable workflow integration test verifies the
  start-failure result and persisted lifecycle. Existing subprocess tests in
  `pipeline_check_test.go` remain. `make test`, `make vet`, `make build`, and
  `git diff --check` pass.
- **Desired outcome:** Demonstrate a focused, maintainable unit-test seam before
  considering wider module or test restructuring.
