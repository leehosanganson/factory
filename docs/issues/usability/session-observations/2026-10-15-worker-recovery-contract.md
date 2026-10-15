# Session observations: worker recovery contract

- Continued after merged #270, following the dependency graph to draft #260 after #258/#259/#264 design work.
- Read the current coordinator, REST runtime, and operations documentation. The contract states existing in-process cancellation/shutdown behavior separately from future distributed recovery and stop-proof requirements.
- Covered claim/cancel serialization, suspect states, bounded drain, trusted supervisor stop proof, ambiguous database/provider operations, retained workspaces, and deterministic future fault tests. Independent review found no P1/P2 issues.
- Docs-only validation (`git diff --check` and local-link targets) passed. No runtime changes or unrelated usability observations.
