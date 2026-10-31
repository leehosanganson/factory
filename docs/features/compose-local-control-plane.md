# Local Compose API control plane

The root `docker-compose.yml` provides a local PostgreSQL-backed API control plane using the existing static `factory-web:local` image. It intentionally has **no worker service**: jobs can be admitted and stored, but remain queued and are not executed. This setup is for local API development, not a security sandbox, production deployment, or a Factory-unit runtime.

## Prerequisites and setup

Install Docker with Compose and Python 3. The smoke test uses Python's standard-library `urllib` on the host (not curl). From the repository root:

```sh
python3 scripts/compose-local.py
make container-build
docker compose -f docker-compose.yml up -d --wait postgres provision-config api
docker compose -f docker-compose.yml exec -T api /factory probe --url http://127.0.0.1:8080/readyz
```

The generator creates `.local/factory-compose/` with mode `0700`; each generated configuration/credential file is mode `0600`. It prints no secret values and refuses to run if the credential directory already exists. Generated inputs include strict server JSON, a legacy operator API key, a named operator verifier and its client token, and the PostgreSQL password. `.local/` is ignored by Git. Keep these files private. There is deliberately no implicit credential rotation: back up anything needed, stop the stack and remove the local generated setup and the named `factory-config` volume only when an explicit reset is intended, then rerun setup. Removing the `postgres-data` volume also destroys the database and is a separate destructive choice.

The one-shot root provisioning service stages each file in a unique private directory on the same named volume as its destination, applies owner-readable mode `0400` and runtime owners (`65532:65532` for config; `999:999` for the PostgreSQL password), and publishes it with a no-overwrite hard link. Each final file appears atomically, but the four-file set is not atomic: an interrupted run can leave a valid subset provisioned. Retrying verifies every existing final against the generated source bytes, regular-file type, private mode, and expected owner, retains matching files unchanged, and fills missing files. A differing source or mismatched final is a conflict: provisioning stops without overwriting or deleting final data. Normal exit cleanup removes only this invocation's own uniquely created private staging directories; a forced kill can leave its private staging directory behind, which retries do not inspect or remove. There is no transaction manifest, commit marker, final-file rollback, or automatic conflict cleanup. If generated inputs or volume contents conflict, an operator must inspect and resolve the discrepancy deliberately, taking backups before any manual changes; do not treat automatic deletion as recovery. Concurrent runs with identical source inputs safely converge through no-overwrite hard links and per-file verification; different inputs conflict safely. The API starts only after the provisioner exits successfully, mounts only its config volume read-only, and runs as the image's non-root UID. PostgreSQL receives its password from the private, read-only named secret volume, has durable named database storage, and no published host port. The API listens on `0.0.0.0:8080` inside its container but publishes only to host loopback (a randomly assigned loopback host port). The smoke test discovers that port rather than assuming a fixed local port. The readiness check invokes the static `/factory probe` command directly (no shell or curl in the API image); it makes a bounded, redirect-free GET request to `/readyz` and prints only a fixed success message.

The generated operator bearer token is in `.local/factory-compose/operator-token`; read it locally when configuring an API client. Do not paste it into shell history or logs. The API key and verifier are different credential materials; the verifier contains only the named token's SHA-256 digest. Server JSON and the PostgreSQL password are also sensitive and must remain private. The PostgreSQL password is not interpolated into container environment variables. This sample uses a local PostgreSQL connection without TLS inside the Compose network; do not expose that network or API beyond a trusted local environment.

## Verify admission without execution

Run the full disposable local smoke check:

```sh
make test-compose-local
```

It generates credentials only in a fresh setup and verifies modes and non-overwrite behavior. It gates and interrupts a provisioner after its first final file, then verifies retry completes provisioning without replacing that file; runs two concurrent provisioners with identical inputs and verifies they converge; and attempts provisioning with a different source API key, verifying the original remains unchanged. It also verifies mismatched operator data is preserved on conflict, rejects hard-linked source and final credentials, and confirms simultaneous provisioners with different inputs produce one immutable winner without replacing its files. A random project suffix isolates uniquely named smoke volumes and containers; the script refuses collisions before reset or cleanup and discovers the randomly published loopback API port. It then validates Compose configuration, builds the control-plane image, starts PostgreSQL and API, probes readiness, and submits one authenticated test job. Failure-path exercises use only volumes named for the unique disposable smoke-test project. It confirms the job status is `queued`, demonstrating that this stack has no job executor. It uses a unique Compose project name and removes only those smoke-test containers and named volumes on exit; it will not tear down a separately running default Compose stack. Generated credentials remain and are not printed. Use `docker compose -f docker-compose.yml down` for a normal stop that preserves PostgreSQL and config volume data.

The smoke check uses Python's standard-library `urllib` on the host to submit and inspect an example request; it does not add curl, a client, or an execution utility to the API image. The API exposes the existing job-service semantics and its normal trust boundary; local containers do not make repository-controlled content safe to execute. This stack itself has no worker, so admitted jobs remain queued across restarts with PostgreSQL persistence.
