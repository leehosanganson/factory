# Make full use of Factory easier to learn

- **Finding:** Users want guidance for fuller use of Factory's capabilities,
  including running multiple jobs in parallel.
- **Desired outcome:** Discoverable workflow guidance that explains job
  concurrency and its limits.
- **Status:** Implemented in `.agents/skills/factory/SKILL.md`; multiple
  detached jobs are only claimed concurrently for different canonical
  implementation targets, while same-target implementation admission rejects
  an active duplicate. This documentation does not imply universal parallel
  support. The integrated tree is verified by the current `make test` and
  `make vet` checks.
