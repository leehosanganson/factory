#!/usr/bin/env python3
"""Start an isolated Factory dashboard server behind a loopback HTTPS proxy."""

from __future__ import annotations

import argparse
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import sqlite3
import ssl
import subprocess
import tempfile
import threading
import time
from urllib.parse import urlsplit


class ProxyServer(http.server.ThreadingHTTPServer):
    def handle_error(self, _request: object, _client_address: object) -> None:
        pass


class Proxy(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    upstream_port = 0

    def forward(self) -> None:
        parsed = urlsplit(self.path)
        path = parsed.path
        allowed = (
            self.command == "GET" and path in ("/", "/ui", "/ui/", "/ui/session", "/ui/assets/dashboard.css", "/ui/assets/dashboard.js") and not parsed.query
            or self.command == "GET" and path in ("/ui/api/v1/jobs", "/ui/api/v1/threads") and parsed.query in ("limit=100", "limit=50")
            or self.command == "GET" and path == "/ui/api/v1/operations" and not parsed.query
            or self.command == "GET" and path == "/ui/api/v1/notifications" and parsed.query == "limit=50"
            or self.command == "GET" and re.fullmatch(r"/ui/api/v1/threads/[A-Za-z0-9_-]+", path) and not parsed.query
            or self.command == "GET" and path == "/ui/api/v1/worker-sessions" and parsed.query in ("limit=50", "limit=50&before_created_at=2025-01-02T23%3A11%3A00Z&before_id=session-48")
            or self.command == "POST" and path == "/ui/api/v1/threads" and not parsed.query
            or self.command == "POST" and re.fullmatch(r"/ui/api/v1/threads/[A-Za-z0-9_-]+/complete", path) and not parsed.query
            or self.command == "GET" and path == "/ui/api/v1/threads" and re.fullmatch(r"limit=50&cursor=[A-Za-z0-9_-]+", parsed.query)
            or self.command == "GET" and re.fullmatch(r"/ui/api/v1/threads/[A-Za-z0-9_-]+", path) and not parsed.query
            or self.command == "GET" and re.fullmatch(r"/ui/api/v1/jobs/[A-Za-z0-9_-]+/(history|events)", path) and not parsed.query
            or self.command == "GET" and re.fullmatch(r"/ui/api/v1/jobs/[A-Za-z0-9_-]+", path) and not parsed.query
            or self.command == "GET" and path == "/ui/api/v1/schedules" and not parsed.query
            or self.command == "GET" and re.fullmatch(r"/ui/api/v1/schedules/[A-Za-z0-9_-]+", path) and not parsed.query
            or self.command == "GET" and re.fullmatch(r"/ui/api/v1/schedules/[A-Za-z0-9_-]+/runs", path) and parsed.query == "limit=5"
            or self.command == "POST" and path in ("/ui/login", "/ui/logout", "/ui/api/v1/jobs", "/ui/api/v1/threads") and not parsed.query
            or self.command == "POST" and re.fullmatch(r"/ui/api/v1/schedules", path) and not parsed.query
            or self.command in ("PATCH", "DELETE") and re.fullmatch(r"/ui/api/v1/schedules/[A-Za-z0-9_-]+", path) and not parsed.query
        )
        if not allowed:
            self.send_error(404)
            return
        upstream = http.client.HTTPConnection("127.0.0.1", self.upstream_port, timeout=10)
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in ("connection", "transfer-encoding")
        }
        try:
            upstream.request(self.command, self.path, body=body, headers=headers)
            response = upstream.getresponse()
            is_stream = response.getheader("Content-Type", "").startswith("text/event-stream")
            self.send_response(response.status)
            for key, value in response.getheaders():
                if key.lower() not in ("connection", "transfer-encoding", "date", "server", "content-length"):
                    self.send_header(key, value)
            if is_stream:
                self.end_headers()
                while chunk := response.read1(4096):
                    self.wfile.write(chunk)
                    self.wfile.flush()
            else:
                payload = response.read()
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
        finally:
            upstream.close()

    do_GET = forward
    do_POST = forward
    do_PATCH = forward
    do_DELETE = forward

    def log_message(self, *_args: object) -> None:
        pass


def free_port(exclude: int | None = None) -> int:
    while True:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        if port != exclude:
            return port


def stop_fixture(_signum: int, _frame: object) -> None:
    raise SystemExit(0)


def main() -> int:
    signal.signal(signal.SIGINT, stop_fixture)
    signal.signal(signal.SIGTERM, stop_fixture)
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--runtime-file", required=True, type=Path)
    args = parser.parse_args()
    args.runtime_file.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    args.runtime_file.parent.chmod(0o700)
    args.runtime_file.parent.resolve(strict=True)
    if not 1 <= args.port <= 65535:
        raise SystemExit("dashboard E2E proxy port is invalid")
    if args.runtime_file.exists() or args.runtime_file.is_symlink():
        raise SystemExit("refusing to replace an existing dashboard E2E runtime file")
    binary = args.binary.resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise SystemExit("dashboard E2E requires an executable Factory binary")

    with tempfile.TemporaryDirectory(prefix="factory-dashboard-e2e-") as temporary:
        root = Path(temporary)
        root.chmod(0o700)
        token = secrets.token_hex(32)
        submitter_token = secrets.token_hex(32)
        key = secrets.token_hex(32)
        verifier = root / "principal-verifier.json"
        verifier.write_text(json.dumps({"version": 1, "principals": [
            {"id": "e2e-operator", "token_sha256": hashlib.sha256(token.encode()).hexdigest()},
            {"id": "e2e-submitter", "token_sha256": hashlib.sha256(submitter_token.encode()).hexdigest()},
        ]}))
        verifier.chmod(0o600)
        key_file = root / "api-key"
        key_file.write_text(key)
        key_file.chmod(0o600)

        repository = root / "repository"
        repository.mkdir(mode=0o700)
        subprocess.run(["git", "init", "-q", "-b", "main", str(repository)], check=True)
        subprocess.run(["git", "-C", str(repository), "config", "user.name", "Dashboard E2E"], check=True)
        subprocess.run(["git", "-C", str(repository), "config", "user.email", "dashboard-e2e@example.invalid"], check=True)
        (repository / "README.md").write_text("Disposable dashboard E2E repository.\n")
        (repository / "release.py").write_text('RELEASE_LABEL = "draft"\n')
        subprocess.run(["git", "-C", str(repository), "add", "README.md", "release.py"], check=True)
        subprocess.run(["git", "-C", str(repository), "commit", "-q", "-m", "Initialize fixture repository"], check=True)
        source_edit_task = "Change RELEASE_LABEL in release.py from draft to ready."
        harness = root / "harness.sh"
        harness.write_text(
            "#!/bin/sh\n"
            f'if [ "$2" = "{source_edit_task}" ] && [ -f release.py ]; then\n'
            '    printf \'RELEASE_LABEL = "ready"\\n\' > release.py\n'
            "fi\n"
        )
        harness.chmod(0o700)
        expected_source = root / "expected-release.py"
        expected_source.write_text('RELEASE_LABEL = "ready"\n')
        server_port = free_port(args.port)
        config = {
            "mode": "local_process",
            "listen_address": f"127.0.0.1:{server_port}",
            "repositories": {"fixture": str(repository)},
            "harness": {"executable": str(harness), "args": ["{system_prompt}", "{task}"]},
            "api_key_file": str(key_file),
            "principals": [
                {"id": "e2e-operator", "role": "operator", "token_file": str(verifier), "enabled": True},
                {"id": "e2e-submitter", "role": "submitter", "token_file": str(verifier), "enabled": True},
            ],
            "verification_checks": [["cmp", "-s", "release.py", str(expected_source)]],
            "web_ui": {"enabled": True, "operator_id": "e2e-operator"},
            "persistence": {"backend": "sqlite", "path": str(root / "schedules.db")},
            "workers_enabled": True,
            "limits": {"workers": 1, "queue_capacity": 4, "max_records": 128},
        }
        config_path = root / "server.json"
        config_path.write_text(json.dumps(config))
        config_path.chmod(0o600)

        cert = root / "localhost.crt"
        cert_key = root / "localhost.key"
        subprocess.run(
            [
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(cert_key),
                "-out", str(cert), "-days", "1", "-subj", "/CN=localhost",
                "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cert.chmod(0o600)
        cert_key.chmod(0o600)

        server_log = root / "server.log"
        with server_log.open("wb") as log:
            process = subprocess.Popen(
                [str(binary), "server", "--config", str(config_path)],
                cwd=root,
                env={**os.environ, "XDG_STATE_HOME": str(root / "state")},
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        proxy: ProxyServer | None = None
        proxy_thread: threading.Thread | None = None
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    details = server_log.read_text(errors="replace")[-4096:]
                    raise RuntimeError(f"Factory test server exited with status {process.returncode}: {details}")
                try:
                    connection = http.client.HTTPConnection("127.0.0.1", server_port, timeout=0.25)
                    connection.request("GET", "/readyz")
                    response = connection.getresponse()
                    response.read()
                    connection.close()
                    if response.status == 200:
                        break
                except OSError:
                    pass
                time.sleep(0.05)
            else:
                raise RuntimeError("Factory test server did not become ready")

            with sqlite3.connect(root / "schedules.db") as database:
                at = "2025-01-01T00:00:00Z"
                for sequence in range(1, 55):
                    owner = "e2e-operator" if sequence in (2, 54) else "e2e-submitter"
                    thread_id, job_id = f"retained-thread-{sequence}", f"retained-job-{sequence}"
                    database.execute(
                        "INSERT INTO factory_jobs (id,request_json,status,created_at,updated_at,queue_sequence,submitter) VALUES (?,?,'succeeded',?,?,?,?)",
                        (job_id, '{"repository":"fixture","task":"Private fixture task"}', at, at, sequence, owner),
                    )
                    database.execute("INSERT INTO factory_threads (id,submitter,created_at,initial_job_id) VALUES (?,?,?,?)", (thread_id, owner, at, job_id))
                    database.execute("INSERT INTO factory_thread_jobs (thread_id,job_id,position,created_at) VALUES (?,?,1,?)", (thread_id, job_id, at))
                # A pending occurrence lets the real dispatcher admit a Job without waiting for a cron boundary.
                occurrence = "2025-01-01T00:00:00Z"
                database.execute(
                    "INSERT INTO factory_schedules "
                    "(id,owner,cron,repository,task,status,revision,created_at,updated_at,next_occurrence_at,pending_occurrence_at) "
                    "VALUES (?,?,?,?,?,'active',1,?,?,?,?)",
                    ("fixture-schedule-run", "e2e-operator", "0 0 1 1 *", "fixture", source_edit_task,
                     "2024-12-31T00:00:00Z", "2024-12-31T00:00:00Z", occurrence, occurrence),
                )
                database.execute("DROP INDEX IF EXISTS factory_worker_sessions_one_active_principal")
                database.execute("DROP INDEX IF EXISTS factory_worker_sessions_principal_boot")
                sessions = [("session-latest", "worker-zeta", "hidden-boot-id", "active", "2025-01-03T00:00:00Z", None, None)]
                sessions.extend(
                    (f"session-{index}", f"worker-{index}", "hidden-boot-id", "active", f"2025-01-02T{23 - (index // 60):02d}:{59 - (index % 60):02d}:00Z", None, None)
                    for index in range(49)
                )
                sessions.extend([
                    ("session-first", "worker-alpha", "hidden-boot-id", "retired", "2025-01-02T00:00:00Z", "2025-01-02T00:01:00Z", "2025-01-02T00:00:30Z"),
                    ("session-oldest", "worker-beta", "hidden-boot-id", "active", "2025-01-01T00:00:00Z", None, "2025-01-01T00:00:30Z"),
                ])
                database.executemany(
                    "INSERT INTO factory_worker_sessions (id,principal_id,boot_id,status,created_at,retired_at,heartbeat_at) VALUES (?,?,?,?,?,?,?)",
                    sessions,
                )
            Proxy.upstream_port = server_port
            proxy = ProxyServer(("127.0.0.1", args.port), Proxy)
            proxy.daemon_threads = True
            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.load_cert_chain(cert, cert_key)
            proxy.socket = tls.wrap_socket(proxy.socket, server_side=True)
            proxy_thread = threading.Thread(target=proxy.serve_forever, daemon=True)
            proxy_thread.start()
            runtime_data = json.dumps({"url": f"https://127.0.0.1:{proxy.server_address[1]}", "token": token, "submitterToken": submitter_token, "fixture_pid": os.getpid(), "server_pid": process.pid}).encode()
            descriptor = os.open(args.runtime_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as runtime_output:
                runtime_output.write(runtime_data)
                runtime_output.flush()
                os.fsync(runtime_output.fileno())
            while process.poll() is None:
                time.sleep(0.2)
        finally:
            if proxy is not None:
                proxy.shutdown()
                proxy.server_close()
            if proxy_thread is not None:
                proxy_thread.join(timeout=5)
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
