# Supervised single-host deployment with systemd

This recipe runs one installed Factory REST server under a dedicated local account. It assumes a Linux host with systemd and a trusted, operator-managed repository checkout. It complements [REST server operations](rest-server-operations.md), which documents API probes, SQLite recovery, and backup boundaries. Factory executes the configured harness and repository content with the service account's local authority; systemd service management is not a security sandbox.

## Prepare the service account and files

Choose the service account, install location, working directory, repository checkout, state location, and configuration path for the host. The paths below are examples; replace them consistently with operator-selected absolute paths. `WorkingDirectory` is the process's current directory, not a repository selector: each checkout must also be configured as an exact root in the server JSON.

Install the released binary at a stable absolute path (shown here as `/usr/local/bin/factory`) and make it executable by the service account. Create a dedicated account and private state/credential directories. The configured repository checkout and harness must be accessible to this account; Factory needs repository write access to create job worktrees. Ensure the selected working directory exists and is traversable by the account.

```sh
install -o root -g root -m 0755 /path/to/released/factory /usr/local/bin/factory
useradd --system --home-dir /var/lib/factory --create-home --shell /usr/sbin/nologin factory
install -d -o factory -g factory -m 0750 /srv/factory
install -d -o factory -g factory -m 0700 \
  /var/lib/factory/state /var/lib/factory/state/server /var/lib/factory/secrets
install -d -o root -g factory -m 0750 /etc/factory
```

Provision a fresh API key (and, only when GitHub publishing is configured, a provider token) through the host's secret-management process. Do not put secret values in this document, the unit, command arguments, or server JSON. Factory requires credential files to be regular, non-symlink files owned by the effective service user and inaccessible to group/others; use owner-only permissions. Keep the JSON readable by `factory` but not by other untrusted users; for example, install it as `root:factory` mode `0640`. Configure an absolute API-key file path and, if applicable, absolute provider-token path in the JSON. The SQLite database path and its already-existing parent directory must be private to the service user. With the example state environment below, a database path such as `/var/lib/factory/state/server/jobs.db` is suitable after creating its parent as `factory:factory` mode `0700`.

Set `repositories` in the strict server JSON to the exact canonical checkout root(s), and configure the fixed harness executable and arguments. Keep `listen_address` at the loopback default unless operator-managed network controls are deliberately configured. Use SQLite for durable job/history state; memory mode loses records on restart. The server's results/workspace tree is separate from the SQLite database and is rooted at `$XDG_STATE_HOME/factory/rest-server` (or `~/.local/state/factory/rest-server` when unset). Here `XDG_STATE_HOME=/var/lib/factory/state`; keep that tree private and available to the service user. See [server configuration](rest-server-config.md) for the full schema and permissions.

## Install the unit

Create `/etc/systemd/system/factory.service`, replacing the example binary, config, and working-directory paths if needed:

```ini
[Unit]
Description=Factory REST job server
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=5min
StartLimitBurst=3

[Service]
Type=simple
User=factory
Group=factory
WorkingDirectory=/srv/factory
Environment=XDG_STATE_HOME=/var/lib/factory/state
UMask=0077
ExecStart=/usr/local/bin/factory server --config /etc/factory/server.json
Restart=on-failure
RestartSec=5s
TimeoutStopSec=20s
KillSignal=SIGTERM

[Install]
WantedBy=multi-user.target
```

Factory handles `SIGTERM` and `SIGINT`. Runtime shutdown uses one shared **15-second default deadline**: it stops the workspace sweeper, closes HTTP serving, and cancels/waits for workers within that same deadline, rather than granting each phase a fresh timeout. `TimeoutStopSec=20s` gives that runtime deadline a short margin before systemd's stop timeout. A job still running when the process exits is not replayed automatically after an SQLite restart; inspect it and its history before taking an eligible operator recovery action. Do not treat service restart as job retry.

`Restart=on-failure` restarts a process that exits unsuccessfully; it does not restart an intentional clean stop. `RestartSec` delays retries. The unit's start-rate limit stops repeated rapid failures from restarting indefinitely; after correcting the cause, inspect the journal and use `systemctl reset-failed factory` if the unit remains rate-limited, then start it again. A configuration edit or credential rotation requires a service restart because configuration and credentials are read at startup. Before applying a JSON or credential change, run the doctor as `factory`, then restart and check readiness/logs. Use `systemctl daemon-reload` after changing the unit file, not merely after editing the JSON config:

```sh
sudo -u factory /usr/local/bin/factory server doctor --config /etc/factory/server.json
systemctl restart factory.service
systemctl status factory.service
journalctl -u factory.service -n 100 --no-pager
curl --fail --silent --show-error http://127.0.0.1:8080/readyz
```

Enable and start the service:

```sh
systemctl daemon-reload
systemctl enable --now factory.service
```

## Validate and operate

Run the read-only local preflight as the service account, so it checks the same file access and checkout prerequisites the daemon uses. It does not start the listener, open/create SQLite, test provider reachability, or run the workflow.

```sh
sudo -u factory /usr/local/bin/factory server doctor --config /etc/factory/server.json
systemd-analyze verify /etc/systemd/system/factory.service
systemctl status factory.service
journalctl -u factory.service -b --no-pager
curl --fail --silent --show-error http://127.0.0.1:8080/healthz
curl --fail --silent --show-error http://127.0.0.1:8080/readyz
```

`/healthz` is liveness only; `/readyz` indicates startup and configured dependency readiness. A readiness failure is not a successful admission signal. The journal includes the runtime's selected persistence backend at readiness, not the database path, credentials, or API key. For authenticated job inspection and aggregate operational status, follow [REST server operations](rest-server-operations.md); use a private authorization-header file rather than placing the API key in shell history or command arguments.

Useful routine commands:

```sh
systemctl restart factory.service
systemctl stop factory.service
systemctl reset-failed factory.service
journalctl -u factory.service -f
```

Do not launch a second `factory server` process against the same SQLite database, including as a manual troubleshooting instance while systemd's service is running. SQLite is single-server storage; a second owner is rejected and cannot safely perform startup recovery for the active server. Use the service's logs and authenticated API for inspection. The supported online backup command can read a live database without starting another server; see [backup and restore](rest-server-operations.md#backup-and-restore).

## Backup and restore boundaries

The online `factory server backup` command creates a consistent SQLite-only backup while the service is running. It does **not** include the separate results/workspace tree, server JSON, API key, provider token, installed binary, or repository checkout. Preserve the workspace tree separately under the same private access controls if a later read-only provider reconciliation may require it. For a stopped-server portable recovery bundle, stop the service cleanly first and use the bundle procedure in [REST server operations](rest-server-operations.md#portable-recovery-bundle); bundles include retained job artifacts but not configuration, credentials, or external provider state. Keep backups encrypted/access-controlled, restore to a separate private location, and verify the restored service configuration and repository/worktree assumptions before relying on it. A backup is not a production restore drill.

SQLite restart behavior is conservative: queued jobs may resume; jobs found running are not replayed because workflow or provider side effects may already have occurred. Keep one service instance per database and follow the [restart recovery procedure](rest-server-operations.md#restart-and-interrupted-work) after an unexpected stop.
