# Local Compose task board

The default Compose stack starts PostgreSQL, the Controller, one Pi Worker, and a loopback-only HTTPS task board. It runs tasks against a disposable Git checkout under the `local` repository alias. It does not need GitHub, a GitHub token, or pull request publication. This is a local development setup for a trusted operator, not the full [Factory Worker MVP](../roadmap/factory-unit-mvp.md) or a sandbox.

## Set up Pi and start the board

You need Docker with Compose, Python 3, and a Pi-supported model and credentials. The Worker image installs Pi 1.3.0 and Git; you do not need Pi on the host. The image build needs access to the base image registries and npm. Pi needs network access to your chosen model provider.

```sh
python3 scripts/compose-local.py
mkdir -m 700 .local/factory-compose/pi
```

Put a dedicated Pi `auth.json` and `settings.json` in that directory. Set `defaultProvider` and `defaultModel` in `settings.json`, and add `models.json` only if needed for your provider. Use credentials that work without an interactive login, with only the access this Worker needs. Do not copy your whole Pi home or reuse a directory with sessions, extensions, or unrelated credentials. Create the files with `umask 077` and keep them at mode `0600`. You can point Compose at a different private directory with `FACTORY_COMPOSE_PI_DIR`; use the same value for the preflight and startup.

The Worker runs as container UID/GID `65532:65532`. On Linux, a `0700` directory owned by your host user can block the Worker even if Compose reports that the stack is ready. Pi needs to write to the directory and to `auth.json`, which it may refresh. On a rootful Linux Docker host without user namespace remapping, transfer ownership of only the dedicated directory and files after creating them:

```sh
pi_dir=${FACTORY_COMPOSE_PI_DIR:-./.local/factory-compose/pi}
sudo chmod 0700 -- "$pi_dir"
sudo chmod 0600 -- "$pi_dir/auth.json" "$pi_dir/settings.json"
if sudo test -f "$pi_dir/models.json"; then
  sudo chmod 0600 -- "$pi_dir/models.json"
  sudo chown 65532:65532 -- "$pi_dir/models.json"
fi
sudo chown 65532:65532 -- "$pi_dir/auth.json" "$pi_dir/settings.json" "$pi_dir"
```

For rootless Docker, user namespace remapping, or Docker Desktop, host ownership may map differently. Set access for the mapped Worker identity and use the preflight below. Do not make the files group- or world-accessible or run the Worker as root to make the check pass.

From the repository root, build the Worker image and run the preflight before starting the stack:

```sh
docker compose build remote-worker
docker compose run --rm --no-deps -T --entrypoint /bin/sh remote-worker -eu -c '
  fail() { printf "Pi config preflight: %s\n" "$1" >&2; exit 1; }
  cd "$PI_CODING_AGENT_DIR" 2>/dev/null || fail "Worker cannot enter the Pi directory"
  [ -r . ] && [ -w . ] && [ "$(stat -c %a .)" = 700 ] ||
    fail "Worker needs a readable, writable Pi directory with mode 0700"

  private_file() {
    [ ! -L "$1" ] && [ -f "$1" ] && [ -r "$1" ] && [ "$(stat -c %a "$1")" = 600 ] ||
      fail "$1 must be a readable regular file (not a symlink) with mode 0600"
  }
  private_file auth.json
  [ -w auth.json ] || fail "Worker cannot write auth.json"
  private_file settings.json
  if [ -e models.json ] || [ -L models.json ]; then
    private_file models.json
  fi
  printf "%s\n" "Pi config preflight passed"
'
```

The one-off container uses the same user and Pi mount as the Worker. It does not start dependencies, open or print file contents, run Pi, or contact a model provider. If it fails, fix the mount path, ownership, or modes and rerun it. A pass checks filesystem access only; it does not validate the JSON, model settings, or credentials.

```sh
docker compose up -d
docker compose ps api remote-worker task-board
docker compose port task-board 443
```

Wait for `api` to show `healthy` and for `remote-worker` and `task-board` to be running. The API health check uses `/readyz` to check Controller readiness, including its PostgreSQL connection. Running containers and a passing Pi mount preflight do not prove that the Worker can claim a Job or that Pi can use the model provider.

Open `https://localhost:<port>/ui` using the returned port. Caddy issues a local certificate for `localhost`; export its public CA certificate with `docker compose cp task-board:/data/caddy/pki/authorities/local/root.crt .local/factory-compose/task-board-ca.crt` and trust it with your browser or OS tooling if you trust this local stack. Do not bypass certificate checks for normal use. The HTTPS endpoint serves only the board; the separate HTTP API port from `docker compose port api 8080` is bound to loopback and is not the browser endpoint.

## Verify one live Job

Use the default stack without the test-only `scripts/fixtures/task-board-pi.compose.yml` overlay. `docker compose config --services` lists service names only: if it includes `pi-provider`, you are using the mock overlay. Also check privately that Pi's configured provider and model point to the external service you intend to use, not a custom mock endpoint. Pi sends the task and any context it uses to that provider. Even one read-only Job can use quota or incur charges. If you do not want a live call, use the [mock smoke test](#verify-without-provider-credentials) instead.

1. Sign in to the HTTPS board with the token in `.local/factory-compose/owner-token`. Under **Start a Thread**, enter `local` as the **Repository alias** and use this **Task**:

   > Read only `README.md` in the current checkout. Reply with its first line and nothing else. Do not modify files, make commits, or contact any other service.

   The fresh disposable checkout is seeded by `remote-worker-init` in [`docker-compose.yml`](../../docker-compose.yml), not copied from the Factory source tree.
2. Click **Create Thread with Job** once. In the selected Thread, confirm that the Job reaches `succeeded` and its **Reply** contains `Disposable local Compose worker checkout.` That confirms a result through the configured harness, but the board does not identify the model endpoint. To corroborate live rather than mock execution, check the provider's request or usage records for the configured account and model around the Job time, if available.
3. If the Job stays `queued`, recheck `docker compose ps api remote-worker task-board` and verify that the HTTP Worker is configured for this Controller and the `local` alias. If it reaches `failed`, recheck the Pi mount preflight, model settings, credentials, and provider access or quota privately. Use the Job ID to inspect status and lifecycle in the operator dashboard with the separate `operator-token`; it does not show private replies. Inspect the original Job before retrying. Enter board tokens only in the HTTPS sign-in form, not in shell commands. Do not print credential files or share raw logs or Pi sessions.

**Optional next step:** Create two Threads using repository alias `local`. After they finish, select either Thread and send a follow-up using the same alias. These Jobs also use the configured model provider.

## What is retained

- Each Thread has a separate checkout and Pi session. Fresh Compose configuration sets the Pi Worker's `max_concurrent_jobs` and the Controller's `limits.workers` to two. Jobs from different Threads can overlap, but Jobs within a Thread run one at a time in submission order. Follow-ups use that Thread's existing files and conversation; if the retained state is missing, the Job fails instead of starting a new conversation. Do not scale this Worker, switch repositories mid-Thread, or mix it with another Worker claiming the same alias.
- Only a successful Thread Job's bounded text reply (up to 8 KiB) is shown to its owner in the browser. Diagnostic output and raw sessions are not published. A reply is not independent verification. Pi can still include sensitive text in its reply if it has access to that text.
- A normal `docker compose down` keeps PostgreSQL data, the checkout and Worker state, and the local HTTPS CA. Do not remove those volumes unless you intend to delete that state. State stays local to this Worker; there is no cross-Worker session transfer, automatic cleanup of Thread directories, or replay of interrupted Jobs. Treat failed or interrupted work as potentially having changed files or session state.
- Threads are separate for normal use, not a containment boundary. Pi and its tools act with the Worker's local authority. Use trusted tasks, repositories, and credentials. The Worker's HTTP connection to the Controller is unencrypted and allowed only on this private Compose network. Keep both host ports on loopback.

## Provisioning and compatibility

The generator creates private inputs under `.local/factory-compose/` and refuses to overwrite an existing setup. Once provisioned, Compose volumes retain immutable config files; provisioning verifies matching files and fails on differences instead of rotating credentials or deleting data. For older generated setups, use a fresh `COMPOSE_PROJECT_NAME`, a new `FACTORY_COMPOSE_CONFIG_DIR` under a private parent, and a separate `FACTORY_COMPOSE_PI_DIR`, or migrate the existing configuration and volumes after backup. Older setups may lack the owner principal, use a generic remote harness, or have a binary Worker secret that cannot be sent as a bearer token.

For a Controller-only setup, explicitly select `docker compose up -d --wait api` and leave the Worker stopped. The legacy direct-PostgreSQL `worker` profile is not part of the task board; it requires a separately configured Worker Controller connection and harness. See [REST Worker processes](rest-worker.md) for its configuration.

## Verify without provider credentials

The browser smoke test also requires Node.js 24.x and npm 11.x on the host (see [`frontend/package.json`](../../frontend/package.json)). These host tools are for the smoke test, not for running the Compose stack.

```sh
npm ci --prefix frontend
npm exec --prefix frontend -- playwright install chromium
sh scripts/test-task-board-compose.sh
```

The browser smoke runs the installed Pi in a disposable Compose project. A test-only overlay puts Pi and a deterministic mock model endpoint on an internal network. The test uses no real model credentials and makes no external provider calls. It holds two Jobs from different Threads open and checks that the next Job in the first Thread waits its turn. After `docker compose down` and `docker compose up` without removing volumes, it checks that both Threads retain ordered Job history and can read their own workspace and Pi conversation. It also checks reply truncation and the operator's redacted view. Set `PLAYWRIGHT_CHROMIUM_EXECUTABLE` if the Playwright browser cannot run on your host and you have a compatible Chromium binary.
