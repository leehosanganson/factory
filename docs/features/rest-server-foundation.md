# REST server foundation

The repository contains a server-only configuration and startup migration foundation for the **proposed** REST MVP. This is not an HTTP server and does not change the existing Factory CLI.

The version-1 JSON config validates a listener address, PostgreSQL URL DSN, existing migration directory, and a non-empty map of repository aliases to existing absolute checkout directories and GitHub owner/repository names. API-key and PAT fields contain only absolute secret-file paths; secret bytes have a separate runtime type. Startup secret loading requires each file to be a regular non-symlink file owned by the effective user, owner-readable, and inaccessible to group/others. The loader uses non-blocking, no-follow opens and a 1 MiB size bound. Keep PAT bytes out of Pi, config serialization, logs, and durable records; this foundation does not integrate the PAT with any provider or child process.

`RunMigrations` provides an injectable migration factory and defaults to `golang-migrate` with its PostgreSQL driver and file source. Project SQL migration files live in `migrations/`. The runner is not connected to a serving command or existing CLI startup.

This slice deliberately does **not** implement HTTP routes or serving, API-key authentication, SQL job persistence, workers/Pi execution, GitHub/provider reads or writes, PAT delivery to Git operations, durable request admission, job lifecycle/recovery, or deployment behavior. Those remain future work under the [REST MVP contract](../roadmap/rest-api-contract.md). The selected contract uses one shared bearer API key and one shared GitHub PAT; per-principal authentication and OAuth/GitHub App credential custody are deferred.
