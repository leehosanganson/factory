# Principal verifier file safety follow-up

- PR #42 merged before the verifier file-safety follow-up could be added; it contains the exact-schema fix and passed both Ubuntu and macOS CI. The follow-up was published separately as PR #43.
- Review identified a check-then-open symlink race and missing owner validation; PR #43 added `O_NOFOLLOW` and effective-UID ownership validation. Subsequent review found that opening a FIFO could block before the regular-file check. Added non-blocking open and FIFO regression coverage as a follow-up within the same bounded change.
- The added FIFO test and `O_NONBLOCK` hardening passed focused unsafe-file tests (20 runs), `make test`, `make vet`, `make build`, and `git diff --check`; the earlier owner/symlink changes also passed Darwin/arm64 cross-compilation.
- This additional fix is part of PR #43, whose new CI run is pending. Keep it scoped there; do not begin another enhancement before this PR is completed or explicitly paused.