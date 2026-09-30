# Issue work intake and tracking

`factory work` provides a local CLI for durable issue-work request intake and inspection. Submission does not fetch an issue, create a pull request, or start engineering. The `issue` command fetches and displays one ephemeral read-only snapshot for an existing queued request. The explicit `refresh` command fetches once and persists an immutable observation when its snapshot version is new; `history` lists those durable observations.

The internal `IssueTracker` contract has a read-only GitHub implementation, `GitHubIssueTracker`. Its `GetIssue` method returns a provider-neutral snapshot containing repository, issue number, title, body, state, URL, update time, and a version fingerprint over the exposed snapshot fields. GitHub reads use `gh api` and the GitHub CLI's existing authentication; Factory does not store credentials. Repository references must be `owner/repo` and issue references positive numbers. GitHub pull requests are detected and rejected as non-issues. `work issue` first loads the queued item and rejects missing requests or non-GitHub trackers before making a provider call. It fetches once and prints the queued identity alongside snapshot title, state, update time, version, and URL. Inspection does not mutate or persist the queue item, poll, claim work, start a worker, or write to GitHub.

```sh
factory work submit --tracker <provider> --issue <issue-ref> --code-host <provider> --repository <repo> [--dedup-key <key>]
factory work list
factory work get <dedup-key>
factory work issue <dedup-key>
factory work refresh <dedup-key>
factory work history <dedup-key>
```

Tracker and code-host provider names must match the provider-name validation used by `WorkRequest` (`[a-z][a-z0-9-]*`). Issue references and repository identifiers are provider-specific strings. Unless `--dedup-key` is supplied, Factory derives a filesystem-safe key from a SHA-256 digest of tracker, issue reference, code host, and repository. Repeating the same identity is idempotent. Reusing a key for different identity fields reports a conflict; an explicit different key permits intentional separate work.

Requests are stored in the private local queue at `<state_dir>/factory/work-requests`, or `${XDG_STATE_HOME:-~/.local/state}/factory/work-requests` when `state_dir` is unset. Issue observations are stored separately under `<state_dir>/factory/work-observations`; snapshot records use hashed filenames, restrictive permissions, and atomic writes. A request key and snapshot `Version` identify one immutable observation: repeating the same snapshot version is idempotent, while a different version is retained as a separate history entry. `history` displays title, state, update time, version, and URL in update-time order. No credentials are stored in observation records.

`list` and `get` display persisted request identity and state; they do not claim requests or start work. `issue` performs a single GitHub read and displays the snapshot without persisting it. `refresh` performs one GitHub read, persists its snapshot, and reports whether it was newly recorded or already present. Both reject missing requests and non-GitHub trackers before network access. Neither changes the durable queue record. These are explicit commands: there is no polling, background worker, code-host PR adapter, or issue/PR write. The local queue is single-host and is not a distributed broker. See the [autonomous issue-to-PR design](../roadmap/autonomous-issue-to-pr-design.md) for future lifecycle scope and explicit exclusions.
