#!/usr/bin/env python3
"""Create private, non-overwriting inputs for the local Compose control plane."""

import hashlib
import json
import os
from pathlib import Path
import secrets
import stat
import sys


def write_private(path: Path, content: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise
    os.chmod(path, 0o600, follow_symlinks=False)


def create(directory: Path) -> None:
    directory = directory.absolute()
    if directory.exists() or directory.is_symlink():
        raise FileExistsError("generated credential directory already exists; refusing to overwrite")
    directory.mkdir(mode=0o700, parents=True)
    directory_info = directory.lstat()
    if not stat.S_ISDIR(directory_info.st_mode) or stat.S_ISLNK(directory_info.st_mode) or directory_info.st_uid != os.geteuid() or directory_info.st_mode & 0o077:
        directory.rmdir()
        raise PermissionError("generated credential directory must be owned and private")
    os.chmod(directory, 0o700)

    api_key = secrets.token_hex(32)
    operator_token = secrets.token_hex(32)
    postgres_password = secrets.token_hex(32)
    config = {
        "mode": "local_process",
        "execution": {"mode": "local"},
        "listen_address": "0.0.0.0:8080",
        "repositories": {"local": "/var/lib/factory/checkouts/local"},
        "harness": {
            "executable": "/usr/local/bin/pi",
            "args": ["-p", "--no-session", "--append-system-prompt", "{system_prompt}", "{task}"],
        },
        "api_key_file": "/etc/factory/api-key",
        "principals": [
            {
                "id": "local-operator",
                "role": "operator",
                "token_file": "/etc/factory/operator-verifier.json",
                "enabled": True,
            }
        ],
        "persistence": {
            "backend": "postgres",
            "dsn": f"postgres://factory:{postgres_password}@postgres:5432/factory?sslmode=disable",
        },
        "workers_enabled": False,
    }
    try:
        write_private(directory / "server.json", (json.dumps(config, indent=2) + "\n").encode())
        write_private(directory / "api-key", (api_key + "\n").encode())
        verifier = {
            "version": 1,
            "principals": [
                {"id": "local-operator", "token_sha256": hashlib.sha256(operator_token.encode()).hexdigest()}
            ],
        }
        write_private(directory / "operator-verifier.json", (json.dumps(verifier, indent=2) + "\n").encode())
        write_private(directory / "operator-token", (operator_token + "\n").encode())
        write_private(directory / "postgres-password", (postgres_password + "\n").encode())
    except BaseException:
        # Remove only regular generated files; never follow or remove unexpected entries.
        for name in ("server.json", "api-key", "operator-verifier.json", "operator-token", "postgres-password"):
            try:
                (directory / name).unlink()
            except FileNotFoundError:
                pass
        directory.rmdir()
        raise


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    target = root / ".local" / "factory-compose"
    try:
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent_info = target.parent.lstat()
        if not stat.S_ISDIR(parent_info.st_mode) or stat.S_ISLNK(parent_info.st_mode) or parent_info.st_uid != os.geteuid() or parent_info.st_mode & 0o077:
            raise PermissionError(".local must be an owned private directory")
        create(target)
    except (OSError, ValueError) as error:
        print(f"Compose setup failed: {error}", file=sys.stderr)
        return 1
    print("Generated private Compose credentials and configuration under .local/factory-compose.")
    print("Keep this directory private; setup does not display credential values.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
