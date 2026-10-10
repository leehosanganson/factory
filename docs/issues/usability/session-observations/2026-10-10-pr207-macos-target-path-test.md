# PR #207 macOS CI investigation

- The failing process-level capacity test passed on Linux, while PR CI reported a macOS timeout in the final completion poll.
- Adding a symlink alias for the second target reproduced the timeout locally: the stored job target is canonicalized, but the test compared it with the unresolved alias.
- Comparing against `resolvedTestPath` made the focused test pass. No production-code change was needed.
- The branch was pushed; the new PR checks were pending at session end, so macOS verification remains open.
