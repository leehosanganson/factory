#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

project_suffix=$(python3 -c 'import secrets; print(secrets.token_hex(8))')
COMPOSE_PROJECT_NAME="factory-compose-smoke-$project_suffix"
export COMPOSE_PROJECT_NAME

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

generated=0
if [ ! -e .local/factory-compose ] && [ ! -L .local/factory-compose ]; then
  python3 scripts/compose-local.py
  generated=1
fi
python3 - <<'PY'
import os
import subprocess

project = os.environ['COMPOSE_PROJECT_NAME']
volumes = [f'{project}_factory-config', f'{project}_postgres-secrets', f'{project}_postgres-data']
containers = [f'{project}-interrupted', f'{project}-concurrent']
for volume in volumes:
    result = subprocess.run(['docker', 'volume', 'inspect', volume], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert result.returncode != 0, f'refusing to reuse existing smoke volume {volume}'
for container in containers:
    result = subprocess.run(['docker', 'inspect', container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert result.returncode != 0, f'refusing to reuse existing smoke container {container}'
containers = subprocess.run(
    ['docker', 'ps', '-aq', '--filter', f'label=com.docker.compose.project={project}'],
    check=True, capture_output=True, text=True,
).stdout.strip()
assert not containers, f'refusing to clean up pre-existing Compose project containers: {containers}'
network = f'{project}_default'
result = subprocess.run(['docker', 'network', 'inspect', network], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
assert result.returncode != 0, f'refusing to reuse existing smoke network {network}'
PY
cleanup() {
  docker compose -f docker-compose.yml down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

python3 - <<'PY'
import json
from pathlib import Path
import stat

root = Path('.local/factory-compose')
config_path = root / 'server.json'
config = json.loads(config_path.read_text())
assert config['listen_address'] == '0.0.0.0:8080'
assert 'execution' in config and config['execution']['mode'] == 'local'
assert config['workers_enabled'] is False
assert config['persistence']['backend'] == 'postgres'
assert config['api_key_file'] == '/etc/factory/api-key'
assert Path('.local/factory-compose/postgres-password').stat().st_mode & 0o777 == 0o600
assert config['principals'][0]['token_file'] == '/etc/factory/operator-verifier.json'
assert config['harness']['args'].count('{task}') == 1
assert config['harness']['args'].count('{system_prompt}') == 1
assert Path(root / 'api-key').read_text().strip()
assert Path(root / 'operator-token').read_text().strip()
assert Path(root / 'postgres-password').read_text().strip()
for item in root.iterdir():
    info = item.lstat()
    assert stat.S_ISREG(info.st_mode) and not stat.S_ISLNK(info.st_mode), item
    assert stat.S_IMODE(info.st_mode) == 0o600, (item, oct(stat.S_IMODE(info.st_mode)))
    assert info.st_nlink == 1, (item, info.st_nlink)
assert stat.S_IMODE(root.stat().st_mode) == 0o700
PY

if [ "$generated" -eq 1 ]; then
  if python3 scripts/compose-local.py >/dev/null 2>&1; then
    fail 'credential generator unexpectedly overwrote existing credentials'
  fi
fi

docker compose -f docker-compose.yml config --quiet
make container-build
python3 - <<'PY'
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

root = Path('.local/factory-compose')
source_files = ('server.json', 'api-key', 'operator-verifier.json', 'postgres-password')
project = os.environ['COMPOSE_PROJECT_NAME']
compose = ['docker', 'compose', '-f', 'docker-compose.yml']
base = compose + ['run', '--no-deps']
config_volume = f'{project}_factory-config'
secret_volume = f'{project}_postgres-secrets'
volumes = [config_volume, secret_volume]

def reset_volumes():
    for volume in volumes:
        subprocess.run(['docker', 'volume', 'create', volume], check=True, stdout=subprocess.DEVNULL)
    subprocess.run(
        [
            'docker', 'run', '--rm', '-v', f'{config_volume}:/config', '-v', f'{secret_volume}:/secrets',
            'busybox:1.37', 'sh', '-ec',
            'rm -f /config/server.json /config/api-key /config/operator-verifier.json /secrets/postgres-password; '
            'rm -rf /config/.factory-provision-* /secrets/.factory-provision-*',
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )

def wait_for(path, process, description):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if path.exists():
            return
        if process.poll() is not None:
            raise AssertionError(f'{description} exited before reaching its gate: {process.returncode}')
        time.sleep(0.05)
    raise AssertionError(f'timed out waiting for {description}')

def start_gated(name, gate_dir):
    return subprocess.Popen(
        base + [
            '--name', name,
            '-v', f'{gate_dir}:/gate',
            '-e', 'FACTORY_PROVISION_TEST_GATE_AFTER=server.json',
            '-e', 'FACTORY_PROVISION_TEST_GATE_DIR=/gate',
            'provision-config',
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )

def remove_container(name):
    subprocess.run(['docker', 'rm', '-f', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def target_hash(name):
    volume = secret_volume if name == 'postgres-password' else config_volume
    target = f'/v/{name}'
    result = subprocess.run(
        ['docker', 'run', '--rm', '-v', f'{volume}:/v:ro', 'busybox:1.37', 'sha256sum', target],
        check=True, capture_output=True, text=True,
    )
    return result.stdout.split()[0]

def target_exists(name):
    volume = secret_volume if name == 'postgres-password' else config_volume
    return subprocess.run(
        ['docker', 'run', '--rm', '-v', f'{volume}:/v:ro', 'busybox:1.37', 'test', '-e', f'/v/{name}'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ).returncode == 0

def assert_complete():
    commands = [
        'set -eu',
        'for name in server.json api-key operator-verifier.json; do',
        '  test -f "/config/$name" && test ! -L "/config/$name"',
        '  test "$(stat -c %a "/config/$name")" = 400',
        '  test "$(stat -c %u:%g "/config/$name")" = 65532:65532',
        'done',
        'test -f /secrets/postgres-password && test ! -L /secrets/postgres-password',
        'test "$(stat -c %a /secrets/postgres-password)" = 400',
        'test "$(stat -c %u:%g /secrets/postgres-password)" = 999:999',
    ]
    subprocess.run(
        ['docker', 'run', '--rm', '-v', f'{config_volume}:/config:ro', '-v', f'{secret_volume}:/secrets:ro', 'busybox:1.37', 'sh', '-ec', '\n'.join(commands)],
        check=True,
    )

# An interrupted run leaves its already-published first file intact; retry fills the missing files.
reset_volumes()
with tempfile.TemporaryDirectory(prefix='factory-compose-interrupt-') as gate:
    os.chmod(gate, 0o700)
    name = f'{project}-interrupted'
    first = start_gated(name, gate)
    try:
        wait_for(Path(gate) / 'promoted-server.json', first, 'interrupted provisioner')
        first_hash = target_hash('server.json')
        subprocess.run(['docker', 'kill', '--signal=KILL', name], check=True, stdout=subprocess.DEVNULL)
        first.wait(timeout=10)
    finally:
        remove_container(name)
    subprocess.run(base + ['provision-config'], check=True, stdout=subprocess.DEVNULL)
    assert target_hash('server.json') == first_hash, 'retry replaced the already-provisioned first file'
    assert_complete()

# Two processes using the same generated inputs converge while one is paused after its first file.
reset_volumes()
with tempfile.TemporaryDirectory(prefix='factory-compose-concurrent-') as gate:
    os.chmod(gate, 0o700)
    name = f'{project}-concurrent'
    first = start_gated(name, gate)
    try:
        wait_for(Path(gate) / 'promoted-server.json', first, 'first concurrent provisioner')
        first_hash = target_hash('server.json')
        subprocess.run(base + ['provision-config'], check=True, stdout=subprocess.DEVNULL)
        assert target_hash('server.json') == first_hash, 'concurrent provisioner replaced the first file'
        assert_complete()
        (Path(gate) / 'release-server.json').touch()
        _, stderr = first.communicate(timeout=30)
        assert first.returncode == 0, f'first concurrent provisioner failed: {stderr}'
    finally:
        remove_container(name)

# Different sources racing for one missing target have exactly one winner; the loser never replaces it.
reset_volumes()
with tempfile.TemporaryDirectory(prefix='factory-compose-race-a-') as first_source, tempfile.TemporaryDirectory(prefix='factory-compose-race-b-') as second_source:
    first_path, second_path = Path(first_source), Path(second_source)
    for directory, marker in ((first_path, 'source-a'), (second_path, 'source-b')):
        os.chmod(directory, 0o700)
        for filename in source_files:
            shutil.copyfile(root / filename, directory / filename)
            os.chmod(directory / filename, 0o600)
        (directory / 'api-key').write_text(marker + '\\n')
    gate = Path(tempfile.mkdtemp(prefix='factory-compose-race-gate-'))
    os.chmod(gate, 0o700)
    # Each invocation blocks immediately before publishing any file.
    processes = []
    for index, directory in enumerate((first_path, second_path)):
        name = f'{project}-different-{index}'
        command = [
            'docker', 'run', '--rm', '--name', name, '--user', '0:0',
            '-v', f'{directory}:/source:ro', '-v', f'{config_volume}:/target/factory-config',
            '-v', f'{secret_volume}:/target/postgres-secrets', '-v', f'{gate}:/gate',
            '-e', 'FACTORY_PROVISION_TEST_START_GATE=/gate', '-e', f'FACTORY_PROVISION_TEST_START_ID={index}',
            '-v', f'{Path.cwd() / "scripts/provision-compose-config.sh"}:/provision/provision-compose-config.sh:ro',
            'busybox:1.37', '/bin/sh', '/provision/provision-compose-config.sh',
        ]
        processes.append(subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True))
    for index, process in enumerate(processes):
        wait_for(gate / f'ready-{index}', process, f'different-source provisioner {index}')
    (gate / 'release').touch()
    outcomes = [process.communicate(timeout=30) for process in processes]
    return_codes = [process.returncode for process in processes]
    assert sorted(return_codes) == [0, 1], f'expected exactly one differing source to win, got {return_codes}: {outcomes}'
    winning_hash = target_hash('api-key')
    valid_hashes = {hashlib.sha256(b'source-a\\n').hexdigest(), hashlib.sha256(b'source-b\\n').hexdigest()}
    assert winning_hash in valid_hashes, f'unexpected winning key digest {winning_hash}'
    assert_complete()
    shutil.rmtree(gate)

# A different source must conflict without changing the original final file.
reset_volumes()
subprocess.run(base + ['provision-config'], check=True, stdout=subprocess.DEVNULL)
with tempfile.TemporaryDirectory(prefix='factory-compose-different-source-') as alternate:
    alternate_path = Path(alternate)
    os.chmod(alternate_path, 0o700)
    for filename in source_files:
        shutil.copyfile(root / filename, alternate_path / filename)
        os.chmod(alternate_path / filename, 0o600)
    (alternate_path / 'api-key').write_text('different generated API key\n')
    before = target_hash('api-key')
    result = subprocess.run(
        [
            'docker', 'run', '--rm', '--user', '0:0',
            '-v', f'{alternate_path}:/source:ro',
            '-v', f'{config_volume}:/target/factory-config',
            '-v', f'{secret_volume}:/target/postgres-secrets',
            '-v', f'{Path.cwd() / "scripts/provision-compose-config.sh"}:/provision/provision-compose-config.sh:ro',
            'busybox:1.37', '/bin/sh', '/provision/provision-compose-config.sh',
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    assert result.returncode != 0, 'different generated source unexpectedly reused existing files'
    assert target_hash('api-key') == before, 'conflicting provisioner changed the original API key'
    assert_complete()

# Hard-linked source and final files are rejected without changing the other link.
reset_volumes()
with tempfile.TemporaryDirectory(prefix='factory-compose-hardlink-') as hardlink_source:
    hardlink_path = Path(hardlink_source)
    os.chmod(hardlink_path, 0o700)
    for filename in source_files:
        shutil.copyfile(root / filename, hardlink_path / filename)
        os.chmod(hardlink_path / filename, 0o600)
    os.link(hardlink_path / 'api-key', hardlink_path / 'api-key-linked')
    result = subprocess.run(
        ['docker', 'run', '--rm', '--user', '0:0', '-v', f'{hardlink_path}:/source:ro',
         '-v', f'{config_volume}:/target/factory-config', '-v', f'{secret_volume}:/target/postgres-secrets',
         '-v', f'{Path.cwd() / "scripts/provision-compose-config.sh"}:/provision/provision-compose-config.sh:ro',
         'busybox:1.37', '/bin/sh', '/provision/provision-compose-config.sh'],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    assert result.returncode != 0, 'provisioner accepted a hard-linked source credential'
    assert 'invalid source configuration' in result.stdout, result.stdout
    assert not target_exists('server.json'), 'hard-linked source was partially provisioned'

reset_volumes()
subprocess.run(base + ['provision-config'], check=True, stdout=subprocess.DEVNULL)
subprocess.run(
    ['docker', 'run', '--rm', '--user', '0:0', '-v', f'{config_volume}:/v', 'busybox:1.37',
     'sh', '-ec', 'ln /v/api-key /v/api-key-linked'], check=True, stdout=subprocess.DEVNULL,
)
result = subprocess.run(base + ['provision-config'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
assert result.returncode != 0, 'provisioner accepted a hard-linked final credential'
subprocess.run(
    ['docker', 'run', '--rm', '-v', f'{config_volume}:/v:ro', 'busybox:1.37',
     'sh', '-ec', 'test "$(stat -c %h /v/api-key)" = 2'], check=True, stdout=subprocess.DEVNULL,
)
reset_volumes()

# The final conflict case must preserve operator data and never disclose it in diagnostics.
reset_volumes()
subprocess.run(base + ['provision-config'], check=True, stdout=subprocess.DEVNULL)
subprocess.run(
    ['docker', 'run', '--rm', '-v', f'{config_volume}:/v', 'busybox:1.37', 'sh', '-ec', 'rm /v/operator-verifier.json; printf operator-data > /v/operator-verifier.json'],
    check=True,
    stdout=subprocess.DEVNULL,
)
result = subprocess.run(base + ['provision-config'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
assert result.returncode != 0, 'provisioner unexpectedly replaced mismatched generated inputs'
assert 'operator-data' not in result.stdout, 'provisioner unexpectedly read back operator data'
subprocess.run(
    ['docker', 'run', '--rm', '-v', f'{config_volume}:/v:ro', 'busybox:1.37', 'sh', '-ec', 'test "$(cat /v/operator-verifier.json)" = operator-data'],
    check=True,
    stdout=subprocess.DEVNULL,
)
reset_volumes()
PY

docker compose -f docker-compose.yml up -d --wait postgres provision-config api

docker compose -f docker-compose.yml exec -T api /factory probe --url http://127.0.0.1:8080/readyz
api_port=$(docker compose -f docker-compose.yml port api 8080 | awk -F: '{print $NF}')
case "$api_port" in ''|*[!0-9]*) fail 'could not determine Compose API host port' ;; esac
export FACTORY_COMPOSE_API_BASE="http://127.0.0.1:$api_port"

python3 - <<'PY'
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen

base = os.environ['FACTORY_COMPOSE_API_BASE']
token = Path('.local/factory-compose/operator-token').read_text().strip()
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json',
    'Idempotency-Key': 'compose-smoke-test',
}
request = Request(
    f'{base}/v1/jobs',
    data=json.dumps({'repository': 'local', 'task': 'compose queue smoke; do not execute'}).encode(),
    headers=headers,
    method='POST',
)
with urlopen(request, timeout=10) as response:
    job_id = json.load(response)['job']['id']
request = Request(f'{base}/v1/jobs/{job_id}', headers={'Authorization': f'Bearer {token}'})
with urlopen(request, timeout=10) as response:
    status = json.load(response)['status']
assert status == 'queued', f'expected admitted job to remain queued without workers; got {status}'
PY
docker compose -f docker-compose.yml run --rm --no-deps provision-config >/dev/null
docker run --rm -v "${COMPOSE_PROJECT_NAME}_factory-config:/volume" busybox:1.37 sh -ec 'printf mismatch > /volume/operator-verifier.json'
if docker compose -f docker-compose.yml run --rm --no-deps provision-config >/dev/null 2>&1; then
  fail 'volume provisioner unexpectedly replaced mismatched generated inputs'
fi
printf '%s\n' 'Compose smoke passed: API ready; admitted job remains queued (no worker service).'
