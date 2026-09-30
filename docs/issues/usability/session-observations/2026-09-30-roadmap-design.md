# 2026-09-30 roadmap design

- Repository guidance and existing roadmap documents provided clear conventions for separating current features from future design; the new document links to the implemented-feature index and labels all planned capabilities as future work.
- `make help` was not available (`No rule to make target 'help'`); the Makefile's available targets were inspected directly. No Make target was needed for this documentation-only task.
- Markdown path checks and `git diff --check` were used for verification. A concise Make target listing or documented discovery command could make available targets easier to inspect.
