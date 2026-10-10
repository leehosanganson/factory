# 2026-10-10 REST cancellation implementation session

- Factory's local Make targets and contract/store abstractions gave clear entry points for the feature; a shared store-contract test exercised memory and SQLite behavior with the same assertions.
- The initial REST API test produced a clear 404 for the missing route, making the first RED result easy to interpret.
- Runtime process E2E fixtures needed explicit care around XDG state paths and hashed alias result directories; deriving the test result root from the runtime's configured state home allowed deterministic workspace checks.
- Concrete improvement idea: the process E2E helpers could centralize the relationship between configured XDG state home and REST workspace results path to make process tests less error-prone.
