# Session observation: supervised REST server documentation

- The runtime source exposed the shared 15-second shutdown deadline and the docs could state the limit without extrapolating across shutdown phases.
- `systemd-analyze verify` was available, but the documented installed binary and working directory were not present in the scratch environment; validating a temporary unit with temporary path stand-ins worked.
- `python3` and standalone spelling/Markdown lint tools were unavailable. A Perl local-link check and `git diff --check` were available as deterministic alternatives.
