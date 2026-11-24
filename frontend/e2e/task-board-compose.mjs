import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { chromium, expect } from '@playwright/test';

const [inputDir] = process.argv.slice(2);
const project = process.env.COMPOSE_PROJECT_NAME ?? '';
assert.match(project, /^factory-task-board-[a-f0-9]{16}$/, 'Run via scripts/test-task-board-compose.sh');
const timeout = 25_000;
const alphaTask = 'start alpha: replace the seeded README.md line with alpha-workspace; change no other files';

function rawCompose(...args) {
  return execFileSync('docker', ['compose', '-p', project, ...args], { encoding: 'utf8', timeout: 120_000 });
}

function compose(...args) {
  return rawCompose(...args).trim();
}

function verifyTrackedEdit(threadId) {
  assert.match(threadId, /^[0-9a-f-]+$/);
  const alias = createHash('sha256').update('local').digest('hex');
  const worktree = `/var/lib/factory/state/factory/rest-server/${alias}/threads/${threadId}/worktree`;
  const git = (dir, ...args) => rawCompose('exec', '-T', 'remote-worker', 'git', '-C', dir, ...args);
  const seed = git('/var/lib/factory/checkouts/local', 'rev-parse', 'HEAD');
  assert.equal(git(worktree, 'rev-parse', 'HEAD'), seed, 'Alpha must still be based on the seeded commit');
  const diff = git(worktree, 'diff', '--no-ext-diff', '--no-textconv', '--no-color', 'HEAD');
  assert.equal(diff.replace(/^index [0-9a-f]+\.\.[0-9a-f]+ 100644\n/m, ''), [
    'diff --git a/README.md b/README.md',
    '--- a/README.md',
    '+++ b/README.md',
    '@@ -1 +1 @@',
    '-Disposable local Compose worker checkout.',
    '+alpha-workspace',
    '',
  ].join('\n'), 'Alpha must retain exactly the intended README diff after restart');
  assert.equal(git(worktree, 'status', '--porcelain=v1', '--untracked-files=all', '--ignored'),
    ' M README.md\n', 'Alpha must have no other file changes');
  console.log('Compose browser smoke: exact tracked README edit retained after down/up.');
}

function boardURL() {
  const port = compose('port', 'task-board', '443').split(':').at(-1);
  assert.match(port, /^\d+$/);
  return `https://localhost:${port}/ui`;
}

const browser = await chromium.launch({ executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE });
try {
  async function login(url, tokenFile) {
    const page = await browser.newPage({ ignoreHTTPSErrors: true });
    await page.goto(url);
    await page.getByLabel('Access token').fill((await readFile(join(inputDir, tokenFile), 'utf8')).trim());
    await page.getByRole('button', { name: 'Sign in' }).click();
    return page;
  }
  async function start(page, name) {
    const heading = page.getByRole('heading', { name: /^Thread [0-9a-f-]+$/ });
    const previous = await heading.count() ? await heading.textContent() : '';
    await page.getByLabel('Repository alias', { exact: true }).fill('local');
    await page.getByLabel('Task', { exact: true }).fill(name === 'alpha' ? alphaTask : `start ${name}`);
    await page.getByRole('button', { name: 'Create Thread with Job' }).click();
    await expect(heading).toBeVisible();
    await expect(heading).not.toHaveText(previous);
    return (await heading.textContent()).replace('Thread ', '');
  }
  async function select(page, id) {
    await page.locator('.thread-list').getByRole('button', { name: id, exact: true }).click();
    await expect(page.getByRole('heading', { name: `Thread ${id}`, exact: true })).toBeVisible();
  }
  async function followUp(page, task) {
    await page.getByLabel('Follow-up repository alias').fill('local');
    await page.getByLabel('Follow-up task').fill(task);
    await page.getByRole('button', { name: 'Send follow-up' }).click();
  }
  const statuses = (page) => page.locator('.thread-job .status');
  const jobIds = (page) => page.locator('.thread-job strong');
  const replies = (page) => page.locator('.thread-job .task-reply');

  let url = boardURL();
  let page = await login(url, 'owner-token');
  const liveOperator = await login(url, 'operator-token');
  await expect(liveOperator.getByRole('heading', { name: 'Jobs', exact: true })).toBeVisible();
  const alpha = await start(page, 'alpha');
  await expect(statuses(page)).toHaveText(['running'], { timeout });
  // Queue Alpha's second job before Beta, so Beta must pass the blocked job to reach the gate.
  await followUp(page, 'check first');
  await expect(statuses(page)).toHaveText(['running', 'queued']);
  const beta = await start(page, 'beta');
  assert.notEqual(alpha, beta);
  await expect(statuses(page)).toHaveText(['running'], { timeout });
  const betaJobID = (await jobIds(page).textContent()).replace(/^Job /, '');
  await expect.poll(() => compose('exec', '-T', 'pi-provider', 'sh', '-c',
    'for name in alpha beta; do if [ -f "/tmp/$name-ready" ]; then echo "$name"; fi; done'
  ), { message: 'Both Pi fixtures must reach the gate before either can finish', timeout }).toBe('alpha\nbeta');
  await select(page, alpha);
  await expect(statuses(page)).toHaveText(['running', 'queued']);

  const recentJobs = liveOperator.getByRole('region', { name: 'Recent jobs', exact: true });
  await recentJobs.getByRole('link', { name: betaJobID, exact: true }).click({ timeout });
  const jobDetail = liveOperator.getByRole('region', { name: `Job ${betaJobID}`, exact: true });
  const jobStatus = jobDetail.locator('.detail-grid > div')
    .filter({ has: liveOperator.getByText('Status', { exact: true }) })
    .locator('.detail-value');
  await expect(jobStatus.filter({ hasText: /^running$/ })).toBeVisible({ timeout });
  compose('exec', '-T', 'pi-provider', 'touch', '/tmp/release');
  await expect(jobStatus.filter({ hasText: /^succeeded$/ })).toBeVisible({ timeout });
  await liveOperator.context().close();

  const threads = [
    { id: alpha, name: 'alpha', other: 'beta', history: ['started alpha; remember alpha-session', 'checked alpha-workspace / alpha-session'] },
    { id: beta, name: 'beta', other: 'alpha', history: ['started beta; remember beta-session'] },
  ];
  for (const thread of threads) {
    await select(page, thread.id);
    await expect(statuses(page)).toHaveText(thread.history.map(() => 'succeeded'), { timeout });
    await expect(replies(page)).toHaveText(thread.history);
    thread.jobs = await jobIds(page).allTextContents();
  }
  await page.context().close();
  compose('down');
  compose('up', '-d', '--wait', '--wait-timeout', '90');

  url = boardURL();
  page = await login(url, 'owner-token');
  for (const { id, name, other, history, jobs } of threads) {
    await select(page, id);
    await expect(jobIds(page)).toHaveText(jobs);
    await expect(replies(page)).toHaveText(history);
    await followUp(page, 'recall both');
    await expect(statuses(page)).toHaveText(Array(history.length + 1).fill('succeeded'), { timeout });
    const latest = page.locator('.thread-job').last();
    await expect(latest.getByText('Reply truncated.')).toBeVisible();
    const continuation = await latest.locator('.task-reply').textContent();
    assert.ok(continuation.startsWith(`${name}-workspace / ${name}-session\n`));
    assert.ok(Buffer.byteLength(continuation) <= 8192);
    assert.ok(!continuation.includes(other) && !continuation.includes('private diagnostic'));
    assert.deepEqual((await jobIds(page).allTextContents()).slice(0, -1), jobs);
  }

  verifyTrackedEdit(alpha);

  const operator = await login(url, 'operator-token');
  await expect(operator.getByRole('heading', { name: 'Jobs', exact: true })).toBeVisible();
  const details = await operator.request.get(new URL(`/ui/api/v1/threads/${alpha}`, url).href);
  assert.equal(details.status(), 200);
  const { jobs } = await details.json();
  assert.equal(jobs.length, 3);
  assert.ok(jobs.every((job) => !('reply' in job) && !('verification' in job)));
  console.log('Compose browser smoke: overlapping Threads, ordered Alpha jobs, live operator job status, retained history and isolated follow-ups after down/up, bounded replies, and operator redaction.');
} finally {
  await browser.close();
}
