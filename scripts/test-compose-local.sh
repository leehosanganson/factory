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

if ! command -v openssl >/dev/null 2>&1; then
  fail 'OpenSSL 1.1.1 or newer is required by the dashboard smoke test; install or upgrade OpenSSL before running this test.'
fi
if ! openssl version 2>/dev/null | python3 -c 'import re, sys; output = sys.stdin.readline(); match = re.match(r"^OpenSSL (\d+)\.(\d+)\.(\d+)(?:[+a-zA-Z0-9.-]*)?(?:\s|$)", output); sys.exit(0 if match and tuple(map(int, match.groups())) >= (1, 1, 1) else 1)'; then
  fail 'OpenSSL 1.1.1 or newer is required by the dashboard smoke test; the installed version could not be verified as compatible.'
fi

python3 - <<'PY'
import os
import subprocess

project = os.environ['COMPOSE_PROJECT_NAME']
volumes = [f'{project}_factory-config', f'{project}_factory-worker-config', f'{project}_postgres-secrets', f'{project}_postgres-data', f'{project}_worker-checkout', f'{project}_worker-state']
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
generated=0
if [ ! -e .local/factory-compose ] && [ ! -L .local/factory-compose ]; then
  python3 scripts/compose-local.py
  generated=1
fi
python3 - <<'PY'
import json
from pathlib import Path
import stat
import sys

config_dir = Path('.local/factory-compose')
config_path = config_dir / 'server.json'
message = (
    'The Compose dashboard smoke test requires the current private '
    '.local/factory-compose/server.json to set web_ui.enabled to true and '
    'web_ui.operator_id to "local-operator" (the generated dashboard token principal). '
    'Edit that file in place; do not regenerate or overwrite existing credentials. '
    'Keep the directory mode 0700 and server.json mode 0600. For an already provisioned '
    'setup, stop Compose, back up the config, generated files, and any needed volume data, '
    'then deliberately remove only this Compose project’s factory-config volume so the '
    'updated config can be provisioned. Preserve the generated credential directory and '
    'postgres-data volume, then restart with the documented Compose command.'
)
try:
    directory_info = config_dir.lstat()
    file_info = config_path.lstat()
    if not stat.S_ISDIR(directory_info.st_mode) or stat.S_ISLNK(directory_info.st_mode) or stat.S_IMODE(directory_info.st_mode) != 0o700:
        raise ValueError('configuration directory is not private')
    if not stat.S_ISREG(file_info.st_mode) or stat.S_ISLNK(file_info.st_mode) or stat.S_IMODE(file_info.st_mode) != 0o600:
        raise ValueError('server.json is not a private regular file')
    config = json.loads(config_path.read_text())
    web_ui = config.get('web_ui') if isinstance(config, dict) else None
    if not isinstance(web_ui, dict) or web_ui.get('enabled') is not True or web_ui.get('operator_id') != 'local-operator':
        raise ValueError('dashboard settings are missing or incompatible')
except (OSError, ValueError, json.JSONDecodeError):
    print(message, file=sys.stderr)
    raise SystemExit(1)
PY
if [ ! -f .local/factory-compose/worker.json ]; then
  python3 scripts/compose-local.py --worker-config-only
fi
harness_dir=$(mktemp -d "${TMPDIR:-/tmp}/factory-compose-harness-$project_suffix.XXXXXX")
chmod 755 "$harness_dir"
export FACTORY_COMPOSE_HARNESS_DIR="$harness_dir"
cleanup() {
  docker compose -f docker-compose.yml --profile worker down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf -- "$harness_dir"
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
assert config['web_ui'] == {'enabled': True, 'operator_id': 'local-operator'}
assert config['persistence']['backend'] == 'postgres'
worker = json.loads((root / 'worker.json').read_text())
assert worker['limits'] == config.get('limits', {
    'request_body_bytes': 524288,
    'task_bytes': 262144,
    'queue_capacity': 32,
    'workers': 2,
    'max_records': 1000,
    'max_events_per_job': 200,
    'registry_bytes': 268435456,
    'job_timeout': '30m0s',
    'harness_output_bytes': 1048576,
})
assert worker['persistence'] == config['persistence']
assert 'api_key_file' not in worker and 'principals' not in worker
assert worker['repositories']['local'] == '/var/lib/factory/checkouts/local'
assert worker['harness']['executable'] == '/opt/factory/harness'
assert worker['harness']['args'] == ['-p', '--no-session', '--append-system-prompt', '{system_prompt}', '{task}']
assert worker['verification_checks'] == [['test', '-f', 'compose-worker-result.txt']]
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

env -u FACTORY_COMPOSE_HARNESS_DIR docker compose -f docker-compose.yml config --quiet
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
base = compose + ['run', '--rm', '--no-deps']
config_volume = f'{project}_factory-config'
secret_volume = f'{project}_postgres-secrets'
worker_config_volume = f'{project}_factory-worker-config'
volumes = [config_volume, worker_config_volume, secret_volume]

def reset_volumes():
    for volume in volumes:
        subprocess.run(['docker', 'volume', 'create', volume], check=True, stdout=subprocess.DEVNULL)
    subprocess.run(
        [
            'docker', 'run', '--rm', '-v', f'{config_volume}:/config', '-v', f'{worker_config_volume}:/worker-config', '-v', f'{secret_volume}:/secrets',
            'busybox:1.37', 'sh', '-ec',
            'rm -f /config/server.json /config/api-key /config/operator-verifier.json /worker-config/worker.json /secrets/postgres-password; '
            'rm -rf /config/.factory-provision-* /worker-config/.factory-provision-* /secrets/.factory-provision-*',
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
    volume = secret_volume if name == 'postgres-password' else worker_config_volume if name == 'worker.json' else config_volume
    target = f'/v/{name}'
    result = subprocess.run(
        ['docker', 'run', '--rm', '-v', f'{volume}:/v:ro', 'busybox:1.37', 'sha256sum', target],
        check=True, capture_output=True, text=True,
    )
    return result.stdout.split()[0]

def target_exists(name):
    volume = secret_volume if name == 'postgres-password' else worker_config_volume if name == 'worker.json' else config_volume
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
        ['docker', 'run', '--rm', '-v', f'{config_volume}:/config:ro', '-v', f'{worker_config_volume}:/worker-config:ro', '-v', f'{secret_volume}:/secrets:ro', 'busybox:1.37', 'sh', '-ec', '\n'.join(commands)],
        check=True,
    )

# Default provisioning accepts a legacy generated directory without worker.json; worker provisioning
# fails closed until the existing private worker-config helper has supplied that file.
with tempfile.TemporaryDirectory(prefix='factory-compose-legacy-source-') as legacy_source:
    source = Path(legacy_source)
    os.chmod(source, 0o700)
    for filename in source_files:
        shutil.copyfile(root / filename, source / filename)
        os.chmod(source / filename, 0o600)
    legacy_config = f'{project}_legacy-factory-config'
    legacy_worker_config = f'{project}_legacy-worker-config'
    legacy_secrets = f'{project}_legacy-postgres-secrets'
    for volume in (legacy_config, legacy_worker_config, legacy_secrets):
        subprocess.run(['docker', 'volume', 'create', volume], check=True, stdout=subprocess.DEVNULL)
    provisioner = [
        'docker', 'run', '--rm', '--user', '0:0', '-v', f'{source}:/source:ro',
        '-v', f'{legacy_config}:/target/factory-config',
        '-v', f'{legacy_worker_config}:/target/worker-config',
        '-v', f'{legacy_secrets}:/target/postgres-secrets',
        '-v', f'{Path.cwd() / "scripts/provision-compose-config.sh"}:/provision/provision-compose-config.sh:ro',
        'busybox:1.37', '/bin/sh', '/provision/provision-compose-config.sh',
    ]
    subprocess.run(provisioner, check=True)
    worker_absent = subprocess.run(
        ['docker', 'run', '--rm', '-v', f'{legacy_worker_config}:/v:ro', 'busybox:1.37', 'test', '-e', '/v/worker.json'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    assert worker_absent.returncode != 0, 'default provisioning unexpectedly created worker config'
    missing_worker = subprocess.run(provisioner + ['worker'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    assert missing_worker.returncode != 0, 'worker provisioning accepted missing worker.json'
    assert 'invalid worker source configuration' in missing_worker.stdout, missing_worker.stdout
    with tempfile.TemporaryDirectory(prefix='factory-compose-worker-helper-') as helper_dir:
        helper_root = Path(helper_dir)
        os.chmod(helper_root, 0o700)
        generated = helper_root / '.local' / 'factory-compose'
        generated.mkdir(mode=0o700, parents=True)
        shutil.copyfile(root / 'server.json', generated / 'server.json')
        os.chmod(generated / 'server.json', 0o600)
        helper_script = helper_root / 'scripts' / 'compose-local.py'
        helper_script.parent.mkdir()
        shutil.copyfile('scripts/compose-local.py', helper_script)
        subprocess.run(['python3', str(helper_script), '--worker-config-only'], check=True)
        shutil.copyfile(generated / 'worker.json', source / 'worker.json')
        os.chmod(source / 'worker.json', 0o600)
    subprocess.run(provisioner + ['worker'], check=True)
    subprocess.run(
        ['docker', 'run', '--rm', '-v', f'{legacy_worker_config}:/v:ro', 'busybox:1.37', 'sh', '-ec',
         'test -f /v/worker.json && test "$(stat -c %a /v/worker.json)" = 400 && test "$(stat -c %u:%g /v/worker.json)" = 65532:65532'],
        check=True,
    )
    subprocess.run(['docker', 'volume', 'rm', * (legacy_config, legacy_worker_config, legacy_secrets)], check=True, stdout=subprocess.DEVNULL)

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
            '-v', f'{worker_config_volume}:/target/worker-config', '-v', f'{secret_volume}:/target/postgres-secrets', '-v', f'{gate}:/gate',
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
            '-v', f'{worker_config_volume}:/target/worker-config',
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
         '-v', f'{config_volume}:/target/factory-config', '-v', f'{worker_config_volume}:/target/worker-config', '-v', f'{secret_volume}:/target/postgres-secrets',
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

job_id=$(python3 - <<'PY'
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
    data=json.dumps({'repository': 'local', 'task': 'compose fixture: create the expected worker result'}).encode(),
    headers=headers,
    method='POST',
)
with urlopen(request, timeout=10) as response:
    job_id = json.load(response)['job']['id']
request = Request(f'{base}/v1/jobs/{job_id}', headers={'Authorization': f'Bearer {token}'})
with urlopen(request, timeout=10) as response:
    status = json.load(response)['status']
assert status == 'queued', f'expected admitted job to remain queued without workers; got {status}'
print(job_id)
PY
)
export FACTORY_COMPOSE_JOB_ID="$job_id"
docker compose -f docker-compose.yml --profile worker config --quiet
python3 - <<'PY'
import json
import os
import subprocess
from pathlib import Path
from urllib.request import Request, urlopen

project = os.environ['COMPOSE_PROJECT_NAME']
harness_dir = Path(os.environ['FACTORY_COMPOSE_HARNESS_DIR'])
harness = Path('scripts/fixtures/compose-worker-harness.sh').resolve()
harness_target = harness_dir / 'harness'
if harness_target.exists() or harness_target.is_symlink():
    raise AssertionError(f'refusing to replace existing test harness: {harness_target}')
harness_target.write_bytes(harness.read_bytes())
harness_target.chmod(0o755)
subprocess.run(['docker', 'compose', '-f', 'docker-compose.yml', '--profile', 'worker', 'run', '--rm', '--no-deps', 'worker-init'], check=True)
queue_job_id = os.environ['FACTORY_COMPOSE_JOB_ID']
request = Request(
    f'{os.environ["FACTORY_COMPOSE_API_BASE"]}/v1/jobs',
    data=json.dumps({'repository': 'local', 'task': 'compose fixture: create the expected worker result'}).encode(),
    headers={
        'Authorization': f'Bearer {Path(".local/factory-compose/operator-token").read_text().strip()}',
        'Content-Type': 'application/json',
        'Idempotency-Key': f'compose-worker-fixture-{queue_job_id}',
    },
    method='POST',
)
with urlopen(request, timeout=10) as response:
    fixture_job_id = json.load(response)['job']['id']
os.environ['FACTORY_COMPOSE_JOB_ID'] = fixture_job_id
subprocess.run(['docker', 'run', '--rm', '--user', '65532:65532', '--entrypoint', '/bin/sh', '-v', f'{project}_worker-checkout:/checkout:ro', '-v', f'{project}_worker-state:/state', 'factory-worker:local', '-ec', 'test -f /checkout/.git/HEAD; test -z "$(git -C /checkout status --porcelain)"; test -w /state'], check=True)
subprocess.run(['docker', 'compose', '-f', 'docker-compose.yml', '--profile', 'worker', 'up', '-d', '--wait', 'worker'], check=True)
subprocess.run(['docker', 'exec', f'{project}-worker-1', 'test', '-f', '/var/lib/factory/checkouts/local/.git/HEAD'], check=True)
subprocess.run(['docker', 'exec', f'{project}-worker-1', 'test', '-w', '/var/lib/factory/state'], check=True)
subprocess.run(['docker', 'exec', f'{project}-worker-1', 'test', '-x', '/opt/factory/harness'], check=True)
PY
FACTORY_COMPOSE_JOB_ID="$FACTORY_COMPOSE_JOB_ID" FACTORY_COMPOSE_API_BASE="$FACTORY_COMPOSE_API_BASE" python3 - <<'PY'
import json
import os
from pathlib import Path
import subprocess
import time
from urllib.request import Request, urlopen

base = os.environ['FACTORY_COMPOSE_API_BASE']
token = Path('.local/factory-compose/operator-token').read_text().strip()
job_id = os.environ['FACTORY_COMPOSE_JOB_ID']
request = Request(f'{base}/v1/jobs/{job_id}', headers={'Authorization': f'Bearer {token}'})
deadline = time.monotonic() + 90
while time.monotonic() < deadline:
    with urlopen(request, timeout=10) as response:
        job = json.load(response)
    if job['status'] in ('succeeded', 'failed', 'canceled'):
        break
    time.sleep(0.25)
assert job['status'] == 'succeeded', f'worker job ended as {job["status"]}: {job}'
job_id = os.environ['FACTORY_COMPOSE_JOB_ID']
assert job['request']['task'] == 'compose fixture: create the expected worker result'
results = subprocess.run(
    ['docker', 'exec', f'{os.environ["COMPOSE_PROJECT_NAME"]}-worker-1', 'find', '/var/lib/factory/state', '-path', f'*/results/{job_id}/attempts/*/worktree/compose-worker-result.txt', '-type', 'f'],
    check=True, capture_output=True, text=True,
).stdout.splitlines()
assert len(results) == 1, f'expected one fixture result in the retained job attempt, found {len(results)}'
result = results[0]
content = subprocess.run(
    ['docker', 'exec', f'{os.environ["COMPOSE_PROJECT_NAME"]}-worker-1', 'cat', result],
    check=True, capture_output=True, text=True,
).stdout
assert content == 'compose worker fixture completed\n', f'unexpected fixture result: {content!r}'
PY
# Exercise the embedded dashboard through a test-only loopback HTTPS reverse proxy.
# The proxy is not part of the Compose product stack and forwards only these test routes.
python3 - <<'PY'
import http.client
import http.server
import json
import os
from pathlib import Path
import re
import ssl
import subprocess
import tempfile
import threading
import time
from http.cookiejar import CookieJar
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPCookieProcessor, HTTPSHandler, Request, build_opener

api_port = int(urlsplit(os.environ['FACTORY_COMPOSE_API_BASE']).port)
class DashboardProxy(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def forward(self):
        parsed = urlsplit(self.path)
        path = parsed.path
        allowed = (
            self.command == 'GET' and path in ('/', '/ui/session', '/ui/assets/dashboard.css', '/ui/assets/dashboard.js') and not parsed.query
            or self.command == 'GET' and path == '/ui/api/v1/jobs' and parsed.query in ('', 'limit=100')
            or self.command == 'POST' and path in ('/ui/login', '/ui/logout', '/ui/api/v1/jobs') and not parsed.query
            or self.command == 'GET' and re.fullmatch(r'/ui/api/v1/jobs/[A-Za-z0-9_-]+', path) and not parsed.query
        )
        if not allowed:
            self.send_error(404)
            return
        body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
        headers = {key: value for key, value in self.headers.items() if key.lower() not in ('connection', 'transfer-encoding')}
        upstream = http.client.HTTPConnection('127.0.0.1', api_port, timeout=10)
        try:
            upstream.request(self.command, self.path, body=body, headers=headers)
            response = upstream.getresponse()
            payload = response.read()
            self.send_response(response.status)
            for key, value in response.getheaders():
                if key.lower() not in ('connection', 'transfer-encoding', 'date', 'server'):
                    self.send_header(key, value)
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        finally:
            upstream.close()
    do_GET = forward
    do_POST = forward
    def log_message(self, *_args):
        pass

proxy = http.server.ThreadingHTTPServer(('127.0.0.1', 0), DashboardProxy)
proxy.daemon_threads = True
with tempfile.TemporaryDirectory(prefix='factory-compose-dashboard-tls-') as cert_dir:
    cert = Path(cert_dir) / 'proxy.pem'
    subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', str(cert), '-out', str(cert), '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1'], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    cert.chmod(0o600)
    server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_tls.load_cert_chain(cert)
    proxy.socket = server_tls.wrap_socket(proxy.socket, server_side=True)
    thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    thread.start()
    base = f'https://localhost:{proxy.server_port}'
    client_tls = ssl.create_default_context(cafile=str(cert))
    jar = CookieJar()
    client = build_opener(HTTPSHandler(context=client_tls), HTTPCookieProcessor(jar))
    origin = base
    operator_token = Path('.local/factory-compose/operator-token').read_text().strip()
    try:
        with client.open(base + '/', timeout=10) as response:
            assert response.status == 200 and 'Factory' in response.read().decode()
        for asset in ('/ui/assets/dashboard.css', '/ui/assets/dashboard.js'):
            with client.open(base + asset, timeout=10) as response:
                assert response.status == 200 and response.read(), f'dashboard asset unavailable: {asset}'
        login_request = Request(base + '/ui/login', data=urlencode({'token': operator_token}).encode(), headers={'Content-Type': 'application/x-www-form-urlencoded', 'Origin': origin}, method='POST')
        with client.open(login_request, timeout=10) as response:
            login = json.load(response)
        assert login.get('authenticated') is True and login.get('csrf'), f'dashboard login failed: {login}'
        cookie = next(cookie for cookie in jar if cookie.name == 'factory_operator_session')
        assert cookie.secure, 'dashboard session cookie was not marked Secure'
        with client.open(base + '/ui/session', timeout=10) as response:
            session = json.load(response)
        assert session.get('authenticated') is True and session.get('csrf') == login['csrf']
        csrf = session['csrf']
        with client.open(base + '/ui/api/v1/jobs?limit=100', timeout=10) as response:
            assert 'jobs' in json.load(response)
        subprocess.run(['docker', 'compose', '-f', 'docker-compose.yml', '--profile', 'worker', 'stop', 'worker'], check=True, stdout=subprocess.DEVNULL)
        request = Request(base + '/ui/api/v1/jobs', data=json.dumps({'repository': 'local', 'task': 'compose fixture: create the expected worker result'}).encode(), headers={'Origin': origin, 'X-CSRF-Token': csrf, 'Content-Type': 'application/json', 'Idempotency-Key': 'compose-dashboard-worker-fixture'}, method='POST')
        with client.open(request, timeout=10) as response:
            job_id = json.load(response)['job']['id']
        subprocess.run(['docker', 'compose', '-f', 'docker-compose.yml', '--profile', 'worker', 'up', '-d', '--wait', 'worker'], check=True)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            with client.open(base + f'/ui/api/v1/jobs/{job_id}', timeout=10) as response:
                job = json.load(response)
            if job['status'] in ('succeeded', 'failed', 'canceled'):
                break
            time.sleep(0.25)
        assert job['status'] == 'succeeded', f'dashboard job ended as {job["status"]}: {job}'
        results = subprocess.run(['docker', 'exec', f'{os.environ["COMPOSE_PROJECT_NAME"]}-worker-1', 'find', '/var/lib/factory/state', '-path', f'*/results/{job_id}/attempts/*/worktree/compose-worker-result.txt', '-type', 'f'], check=True, capture_output=True, text=True).stdout.splitlines()
        assert len(results) == 1, f'expected one dashboard job artifact, found {len(results)}'
        result = subprocess.run(['docker', 'exec', f'{os.environ["COMPOSE_PROJECT_NAME"]}-worker-1', 'cat', results[0]], check=True, capture_output=True, text=True).stdout
        assert result == 'compose worker fixture completed\n', f'unexpected dashboard job result: {result!r}'
        logout = Request(base + '/ui/logout', data=b'', headers={'Origin': origin, 'X-CSRF-Token': csrf}, method='POST')
        with client.open(logout, timeout=10) as response:
            assert response.status == 200
        with client.open(base + '/ui/session', timeout=10) as response:
            assert json.load(response).get('authenticated') is False, 'logout did not invalidate session'
        stale = Request(base + f'/ui/api/v1/jobs/{job_id}', headers={'Cookie': f'{cookie.name}={cookie.value}'})
        try:
            client.open(stale, timeout=10)
        except HTTPError as error:
            assert error.code == 401, f'stale session cookie returned HTTP {error.code}, want 401'
        else:
            raise AssertionError('logout left the previous dashboard session authorized')
    finally:
        proxy.shutdown()
        proxy.server_close()
        thread.join(timeout=5)
PY
printf '%s\n' 'Compose smoke passed: direct API fixture and HTTPS-proxied dashboard job completed through the profile Worker.'
