# Session observations: attempt workspace/provider contract

- Continued after #271 and drafted #261's dependency-specific workspace/artifact/publisher contract against current workspace, executor, provider, and SQLite recovery code.
- The first independent review found that a visible provider match could be misread as proof an in-flight request was quiescent. Tightened the rules: read-only visibility does not unlock serialization; provider-specific no-later-effect/quiescence evidence is required, and a future test covers visible-while-pending then late completion. Also qualified attempt persistence for volatile memory versus SQLite.
- Follow-up independent review found no P1/P2 issues. Docs-only link checks and `git diff --check` passed. No runtime behavior changed.
