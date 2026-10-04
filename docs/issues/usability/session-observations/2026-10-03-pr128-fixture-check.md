# PR #128 mismatch fixture check

- The named repository/branch mismatch fixtures were already replacing valid JSON values correctly at PR head `74e0e599` and both current focused cases passed. The reported escaped-quote no-op was not present in the checked-out file.
- Added fixture assertions to ensure mismatch bodies differ from valid JSON and include the intended repository/branch mismatch.
- `make test` failed once on `TestSecondaryStatusRunsDuringActiveStageAndPersistsSanitizedUpdate` (2 status invocations instead of 1), then passed on retry. Package tests, vet, build, and diff check passed.
- Improvement idea: include an exact expected fixture snippet or regression-level assertion when reporting a suspected test-fixture defect, so the repository state can be compared immediately with the reported symptom.
