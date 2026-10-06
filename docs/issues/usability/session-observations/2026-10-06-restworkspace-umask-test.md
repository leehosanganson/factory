# REST workspace umask test

- The reported failure reproduced with `umask 0007` in the parent-mode test.
- Explicitly setting and checking fixture modes made both broad-mode tests pass under umasks `0007` and `0022`.
- Full test, vet, and build targets passed with `umask 0007`.
- No additional friction observed in this focused test fix.
