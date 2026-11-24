"""Behavior tests for private local Compose configuration generation."""

import importlib.util
import json
from pathlib import Path
import re
import tempfile
import unittest


MODULE_PATH = Path(__file__).with_name("compose-local.py")
SPEC = importlib.util.spec_from_file_location("compose_local", MODULE_PATH)
assert SPEC and SPEC.loader
compose_local = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(compose_local)
COMPOSE_PATH = Path(__file__).parents[1] / "docker-compose.yml"
PROVISION_PATH = Path(__file__).with_name("provision-compose-config.sh")


def service_volume_mounts(compose_text, service):
    lines = compose_text.splitlines()
    start = lines.index(f"  {service}:") + 1
    body = []
    for line in lines[start:]:
        if line.startswith("  ") and not line.startswith("    "):
            break
        body.append(line)
    return re.findall(r"^      - ([\w-]+):(/[^:\n]+)(?::(ro))?$", "\n".join(body), re.MULTILINE)


class ComposeConfigTests(unittest.TestCase):
    def test_remote_worker_uses_remote_cli_entrypoint(self):
        compose_text = COMPOSE_PATH.read_text()
        lines = compose_text.splitlines()
        start = lines.index("  remote-worker:") + 1
        body = []
        for line in lines[start:]:
            if line.startswith("  ") and not line.startswith("    "):
                break
            body.append(line)

        self.assertIn('    entrypoint: ["/factory", "worker", "remote", "--config", "/etc/factory/remote-worker.json"]', body)
        self.assertFalse(any(line.startswith("    command:") for line in body))

    def test_remote_secret_is_provisioned_and_mounted_for_api_and_remote_worker(self):
        compose_text = COMPOSE_PATH.read_text()
        provision_text = PROVISION_PATH.read_text()
        secret_path = "/run/factory-secrets/remote-worker-secret"

        self.assertIn('remote-worker-secret) printf \'%s/remote-worker-secret\' "$remote_worker_secrets_volume" ;;', provision_text)
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "factory-compose"
            compose_local.create(directory)
            server = json.loads((directory / "server.json").read_text())
            worker = json.loads((directory / "remote-worker.json").read_text())
            self.assertEqual(server["remote_worker_api"]["secret_file"], secret_path)
            self.assertEqual(worker["shared_secret_file"], secret_path)

        for service in ("provision-config", "provision-remote-worker-config"):
            mounts = service_volume_mounts(compose_text, service)
            self.assertIn(("remote-worker-secrets", "/target/remote-worker-secrets", ""), mounts)
        for service in ("api", "remote-worker"):
            mounts = service_volume_mounts(compose_text, service)
            self.assertIn(("remote-worker-secrets", "/run/factory-secrets", "ro"), mounts)
            self.assertNotIn(("postgres-secrets", "/run/factory-secrets", "ro"), mounts)

    def test_fresh_setup_enables_http_remote_worker_without_database_credentials(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "factory-compose"
            compose_local.create(directory)

            server = json.loads((directory / "server.json").read_text())
            worker = json.loads((directory / "remote-worker.json").read_text())
            secret = (directory / "remote-worker-secret").read_bytes()

            self.assertEqual(server["remote_worker_api"], {
                "enabled": True,
                "secret_file": "/run/factory-secrets/remote-worker-secret",
            })
            self.assertRegex(secret, rb"^[0-9a-f]{64}$")
            self.assertEqual(worker["max_concurrent_jobs"], 2)
            self.assertEqual(server["limits"]["workers"], worker["max_concurrent_jobs"])
            self.assertEqual(worker["controller_base_url"], "http://api:8080")
            self.assertIs(worker["allow_insecure_http"], True)
            self.assertEqual(worker["shared_secret_file"], "/run/factory-secrets/remote-worker-secret")
            self.assertNotIn("persistence", worker)
            self.assertNotIn("dsn", worker)
            self.assertEqual(worker["repositories"]["local"], "/var/lib/factory/checkouts/local")
            self.assertEqual(worker["harness_profiles"]["compose"]["executable"], "/usr/local/bin/pi")
            self.assertEqual(worker["harness_profiles"]["compose"]["adapter"], "pi")
            self.assertEqual(server["principals"][1]["role"], "submitter")
            self.assertEqual((directory / "owner-token").stat().st_mode & 0o777, 0o600)
            self.assertEqual(worker["harness_profiles"]["compose"]["repositories"], ["local"])
            for name in ("server.json", "remote-worker.json", "remote-worker-secret"):
                self.assertEqual((directory / name).stat().st_mode & 0o777, 0o600)

    def test_remote_worker_only_helper_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "factory-compose"
            compose_local.create(directory)
            before = (directory / "remote-worker-secret").read_bytes()

            with self.assertRaises(FileExistsError):
                compose_local.add_remote_worker_config(directory)

            self.assertEqual((directory / "remote-worker-secret").read_bytes(), before)
            self.assertTrue((directory / "remote-worker.json").is_file())


if __name__ == "__main__":
    unittest.main()
