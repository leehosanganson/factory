# SQLite server instance lock session

## Verified observations

- The first cross-process regression reproduced the issue: a competing test process opened the same SQLite store while the owner had a running job.
- Acquiring server ownership only through the server-specific store constructor prevented the competitor from running startup recovery; administrative/test store openings remained available for live inspection.
- An initial full-suite run exposed existing runtime tests that intentionally inspect the live database using the ordinary local job manager. Moving ownership enforcement to the runtime server startup boundary preserved those tests and the server lock behavior.
- The owner process could be killed and a subsequent server store open classified the retained running job as requiring operator handling, preserving unclean-restart semantics.

## Friction and idea

- The shared `NewLocalJobManager` function was used for both server startup and external inspection, so applying server ownership there caused unrelated process E2E tests to fail. Keeping an explicit server-only manager constructor made the ownership boundary visible and avoided changing administrative access behavior.
