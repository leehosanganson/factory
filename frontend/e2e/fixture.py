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
            or self.command == "GET" and path == "/ui/api/v1/jobs" and parsed.query == "limit=100"
            or self.command == "POST" and path == "/ui/api/v1/threads" and not parsed.query
            or self.command == "GET" and path == "/ui/api/v1/threads" and parsed.query == "limit=50"
            or self.command == "GET" and re.fullmatch(r"/ui/api/v1/threads/[A-Za-z0-9_-]+", path) and not parsed.query
            or self.command == "GET" and re.fullmatch(r"/ui/api/v1/jobs/[A-Za-z0-9_-]+/(history|events)", path) and not parsed.query
            or self.command == "GET" and re.fullmatch(r"/ui/api/v1/jobs/[A-Za-z0-9_-]+", path) and not parsed.query
            or self.command == "POST" and path in ("/ui/login", "/ui/logout", "/ui/api/v1/jobs") and not parsed.query
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
        key = secrets.token_hex(32)
        verifier = root / "operator-verifier.json"
        verifier.write_text(json.dumps({"version": 1, "principals": [{"id": "e2e-operator", "token_sha256": hashlib.sha256(token.encode()).hexdigest()}]}))
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
        subprocess.run(["git", "-C", str(repository), "add", "README.md"], check=True)
        subprocess.run(["git", "-C", str(repository), "commit", "-q", "-m", "Initialize fixture repository"], check=True)
        harness = root / "harness.sh"
        harness.write_text("#!/bin/sh\nprintf 'dashboard e2e completed\\n' > dashboard-e2e-result.txt\n")
        harness.chmod(0o700)
        server_port = free_port(args.port)
        config = {
            "mode": "local_process",
            "listen_address": f"127.0.0.1:{server_port}",
            "repositories": {"fixture": str(repository)},
            "harness": {"executable": str(harness), "args": ["{system_prompt}", "{task}"]},
            "api_key_file": str(key_file),
            "principals": [{"id": "e2e-operator", "role": "operator", "token_file": str(verifier), "enabled": True}],
            "verification_checks": [["test", "-f", "dashboard-e2e-result.txt"]],
            "web_ui": {"enabled": True, "operator_id": "e2e-operator"},
            "persistence": {"backend": "memory"},
            "workers_enabled": True,
            "limits": {"workers": 1, "queue_capacity": 4, "max_records": 20},
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

            Proxy.upstream_port = server_port
            proxy = ProxyServer(("127.0.0.1", args.port), Proxy)
            proxy.daemon_threads = True
            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.load_cert_chain(cert, cert_key)
            proxy.socket = tls.wrap_socket(proxy.socket, server_side=True)
            proxy_thread = threading.Thread(target=proxy.serve_forever, daemon=True)
            proxy_thread.start()
            runtime_data = json.dumps({"url": f"https://127.0.0.1:{proxy.server_address[1]}", "token": token, "fixture_pid": os.getpid(), "server_pid": process.pid}).encode()
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
