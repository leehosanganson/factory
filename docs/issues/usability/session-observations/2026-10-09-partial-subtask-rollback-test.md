# Session observation — 2026-10-09

Added the focused issue #185 test for rollback when applying staged parallel-subtask files fails partway through. It confirms the first sorted file's original bytes and mode are restored, unrelated and pre-existing second-path content remain unchanged, and the workflow returns an error. The existing implementation passed this test; this is test-only coverage, not a reproduced production defect.

The first failure fixture used a missing staged path and then a directory destination; inspection/testing showed both were treated as deletion/no-op rather than integration errors. A dangling symlink provided a deterministic later integration error after the first output was copied. The canonical test gate initially failed once in `TestRunJobWorkerStopRequestCancelsWorkflow`; the named test passed 10 isolated repetitions, and the subsequent complete `make test && make vet && make build` passed.
