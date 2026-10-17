# Embedded REST operator dashboard

The REST server can optionally serve an embedded browser dashboard from `/` on the existing API listener. It is disabled unless strict server JSON includes `web_ui.enabled: true` and `web_ui.operator_id` naming an enabled named operator. The browser login accepts only that operator's configured token; the shared legacy key and other named operators cannot log in through the UI.

The dashboard supports bounded operator job listing and inspection, safe worker/workflow stage summaries, retained lifecycle history and event streaming, job admission, and cooperative cancellation. The same-origin proxy exposes only `GET /v1/jobs`, `POST /v1/jobs`, `GET /v1/jobs/{id}`, `GET /v1/jobs/{id}/history`, `GET /v1/jobs/{id}/events`, and `POST /v1/jobs/{id}/cancel`. It cannot invoke operations, recovery, provenance, or arbitrary REST routes. Mutating requests require a same-origin check and CSRF token.

Sessions are held in process memory, capped at 1,000 live sessions, and expire after eight hours. The browser receives an HttpOnly, Secure, SameSite=Strict session cookie; the CSRF token exists only in page memory. HTTPS is required for the cookie, so deployments should use a trusted TLS endpoint/proxy and an appropriately controlled network. The UI uses a restrictive content security policy and renders API values as text. It does not display task content, raw event messages, transcripts, check output, or host paths. The dashboard is an operator convenience, not a separate security boundary; the API remains available under its existing authentication contract.

## Control-plane container

The root `Dockerfile` builds a static, non-root image intended for a PostgreSQL-backed API control plane. It compiles a binary that accepts only `factory server` (and version output), and server startup requires PostgreSQL persistence with local workers explicitly disabled. Configure the server JSON at `/etc/factory/server.json` and mount credentials/configuration securely; the image has no repository execution tools or harness bundled. The image uses the same service listener (port 8080) and includes the embedded dashboard assets. This container is not a sandbox or fleet manager.

Build locally with `make container-build` when Docker is available.
