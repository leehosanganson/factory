---
name: documentation
description: Make accurate, focused documentation changes grounded in current source and repository conventions.
---

# Documentation updates

1. **Establish scope and truth.** Identify the requested audience and claims. Read the relevant README, nearby docs, and current source/callers before describing behavior. Treat source and executable CLI help as authority for implemented behavior; do not infer capabilities from names, designs, or roadmap language.
2. **Separate current from future.** State what works today separately from proposals, planned direction, and open questions. Label aspirational capabilities clearly; do not imply commitments, release dates, or implementation where none exists.
3. **Make focused edits.** Follow repository terminology and structure. Prefer concise task-oriented guidance and links to canonical details over duplicating long explanations. Preserve unrelated and pre-existing user changes.
4. **Check paths and links.** After moves or edits, search for stale paths/references. Resolve relative Markdown links from the file that contains them and verify each local target exists. Check that index pages link to the intended canonical documents.
5. **Verify proportionately.** For docs-only changes, inspect the diff and run local link/path checks; do not run code-formatting commands that can rewrite source. Run build/tests only when documentation changes affect generated artifacts, examples, or behavior requiring executable validation. Report exactly what was checked and any limitations.
