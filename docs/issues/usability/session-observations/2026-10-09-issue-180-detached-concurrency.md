# Session observations — 2026-10-09

- **Verified observation:** The per-state admission lock can be shared by CLI processes and atomically serialize starts on different repositories. A process-level fake-agent fixture reproduced two starts exceeding a cap of one before the guard was implemented.
- **Friction:** Existing in-process tests invoke start helpers with intentionally partial `Config` values. Validating the entire agent config at the helper boundary changed unrelated tests and was removed; cap validation remains in JSON config loading and capacity enforcement.
- **Improvement idea:** Keep start-helper configuration assumptions explicit in tests, especially where tests bypass normal CLI config loading.
