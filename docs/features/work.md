# Issue work intake

`factory work` provides a local CLI for durable issue-work request intake and inspection. It does not fetch an issue, create a pull request, or start engineering; there is no issue provider or worker yet.

```sh
factory work submit --tracker <provider> --issue <issue-ref> --code-host <provider> --repository <repo> [--dedup-key <key>]
factory work list
factory work get <dedup-key>
```

Tracker and code-host provider names must match the provider-name validation used by `WorkRequest` (`[a-z][a-z0-9-]*`). Issue references and repository identifiers are provider-specific strings. Unless `--dedup-key` is supplied, Factory derives a filesystem-safe key from a SHA-256 digest of tracker, issue reference, code host, and repository. Repeating the same identity is idempotent. Reusing a key for different identity fields reports a conflict; an explicit different key permits intentional separate work.

Requests are stored in the private local queue at `<state_dir>/factory/work-requests`, or `${XDG_STATE_HOME:-~/.local/state}/factory/work-requests` when `state_dir` is unset. `list` and `get` display persisted identity and state; they do not claim requests or start work. The local queue is a single-host durable queue, not a distributed broker. See the [autonomous issue-to-PR design](../roadmap/autonomous-issue-to-pr-design.md) for future lifecycle scope and explicit exclusions.
