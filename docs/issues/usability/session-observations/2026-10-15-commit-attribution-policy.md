# Session observations: commit attribution policy

- **User report:** The user noticed `Co-authored-by: Factory Agent` trailers on three historical commits in `main` and requested that it not recur.
- **Verified observation:** The merged PR commits e9ebf667, 496f6267, and ebea3725 have Anson Lee as author on GitHub's merge commits, but each merge message retains a Factory Agent co-author trailer. The corresponding PR branch commits list Factory Agent as author and committer; the PRs were merged by leehosanganson.
- Added a human-only authorship/co-authorship policy to `CONTRIBUTING.md` and a pull-request/push CI check rejecting Factory Agent identity in commit author, committer, or commit message metadata. The check does not enforce a single contributor identity.
- Local checker scenarios passed for human identity with ordinary agent mention, Factory Agent as author, Factory Agent as committer, Factory Agent co-author trailer, and unrelated-history fail-closed handling. The historical commit/trailer test also reports the existing attribution as expected.
- An independent read-only review found no P1/P2 issue. `bash -n` and `git diff --check` passed. The pull request will provide GitHub Actions validation; no production code changed.
