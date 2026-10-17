# 2026-10-17 macOS process cancellation race

- The macOS CI log pinpointed the failing assertion and returned status/body, which made the cancellation race easy to isolate.
- Allowing the repeated request to return either an active pending response or the expected terminal-cancellation conflict preserved both lifecycle checks; three local process E2E repetitions passed on Linux.
- The local process tests do not establish macOS behavior; the updated PR still needs a macOS CI run.
