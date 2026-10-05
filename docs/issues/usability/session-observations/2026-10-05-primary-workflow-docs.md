# Session observations: primary workflow documentation

- Verified issue #153 against the current CLI source, README, feature guides, and configuration example before drafting. The implementation, detached-job, and issue-work guides documented relevant pieces separately; the new guide links to the existing deeper references rather than duplicating them.
- Verified CLI spellings and status/artifact behavior against help and implementation source. No Markdown lint/link checker or Makefile target is configured; checked local links and ran `git diff --check`.
- The only provider interaction was the requested read-only `gh issue view` for issue #153 and #151 metadata; the guide contains no provider-call example and no provider writes were made.
