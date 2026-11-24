import { expect, test } from '@playwright/test';
import { readFile } from 'node:fs/promises';
import type { Runtime } from './runtime.js';

const sourceEditTask = 'Change RELEASE_LABEL in release.py from draft to ready.';

async function runtime(): Promise<Runtime> {
  const runtimeFile = test.info().config.metadata.dashboardRuntimeFile;
  if (typeof runtimeFile !== 'string') throw new Error('Playwright runtime fixture path is missing');
  return JSON.parse(await readFile(runtimeFile, 'utf8')) as Runtime;
}

test('operator can manage schedules without exposing definitions in summaries', async ({ page }) => {
  const fixture = await runtime();
  const marker = 'schedule task stays inside edit form';
  const requests: { method: string; path: string; ifMatch?: string }[] = [];
  page.on('request', (request) => {
    if (request.url().includes('/ui/api/v1/schedules/')) requests.push({ method: request.method(), path: new URL(request.url()).pathname, ifMatch: request.headers()['if-match'] });
  });
  await page.goto(fixture.url);
  await page.getByLabel('Access token').fill(fixture.token);
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page.getByRole('heading', { name: 'Schedules' })).toBeVisible();

  await page.getByLabel('Cron (UTC)').fill('0 9 * * 1-5');
  await page.getByLabel('Repository alias').last().fill('fixture');
  await page.locator('#schedule-task').fill(marker);
  await page.getByRole('button', { name: 'Create schedule' }).click();
  const createdRow = page.locator('.schedule-row').filter({ hasText: 'UTC · 0 9 * * 1-5' });
  await expect(createdRow).toBeVisible();
  const scheduleID = (await createdRow.locator('h3').textContent())?.trim();
  expect(scheduleID).toBeTruthy();
  const row = page.locator('.schedule-row').filter({ has: page.locator('h3', { hasText: scheduleID! }) });
  await expect(row.getByText(marker)).toHaveCount(0);
  expect(requests.some((request) => request.path.endsWith(`/${scheduleID}`))).toBe(false);

  await row.getByRole('button', { name: 'Edit' }).click();
  const editForm = row.getByRole('form', { name: `Edit schedule ${scheduleID}` });
  await expect(editForm.getByLabel('Task')).toHaveValue(marker);
  await expect.poll(() => requests.filter((request) => request.method === 'GET' && request.path === `/ui/api/v1/schedules/${scheduleID}`).length).toBe(1);
  await editForm.getByLabel('Cron (UTC)').fill('30 10 * * 1-5');
  await editForm.getByLabel('Task').fill('updated schedule task');
  await editForm.getByRole('button', { name: 'Save schedule' }).click();
  await expect(row).toContainText('UTC · 30 10 * * 1-5');
  await expect(row.getByText('updated schedule task')).toHaveCount(0);

  await row.getByRole('button', { name: 'Pause' }).click();
  await expect(row.locator('.status')).toHaveText('paused');
  await row.getByRole('button', { name: 'Resume' }).click();
  await expect(row.locator('.status')).toHaveText('active');
  page.once('dialog', (dialog) => dialog.accept());
  await row.getByRole('button', { name: 'Remove' }).click();
  await expect(row.locator('.status')).toHaveText('removed');
  await expect(page.getByRole('status').filter({ hasText: 'Schedule removed.' })).toBeVisible();
  expect(requests.filter((request) => request.method === 'GET' && request.path === `/ui/api/v1/schedules/${scheduleID}`).length).toBe(1);
  expect(requests.filter((request) => ['PATCH', 'DELETE'].includes(request.method)).map((request) => request.ifMatch)).toEqual(['"1"', '"2"', '"3"', '"4"']);
});

test('operator can inspect worker session summaries through the read-only dashboard', async ({ page }) => {
  const fixture = await runtime();
  const requests: { method: string; url: string }[] = [];
  page.on('request', (request) => {
    if (request.url().includes('/ui/api/v1/worker-sessions')) requests.push({ method: request.method(), url: request.url() });
  });
  await page.goto(fixture.url);
  await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();
  const denied = await page.request.get(`${fixture.url}/ui/api/v1/worker-sessions?limit=50`);
  expect(denied.status()).toBe(401);
  await page.getByLabel('Access token').fill(fixture.token);
  await page.getByRole('button', { name: 'Sign in' }).click();

  const panel = page.getByRole('region', { name: 'Worker sessions' });
  await expect(panel.getByRole('row')).toHaveCount(51);
  await expect(panel.getByText('worker-zeta')).toBeVisible();
  await expect(panel.getByText('session-latest')).toBeVisible();
  await expect(panel.getByRole('cell', { name: 'active' }).first()).toBeVisible();
  await expect(panel.getByRole('cell', { name: '—' }).first()).toBeVisible();
  await expect(panel.getByText('hidden-boot-id')).toHaveCount(0);
  const rendered = await panel.innerText();
  for (const forbidden of ['boot_id', 'capability', 'offline', 'assignment', 'progress']) expect(rendered).not.toContain(forbidden);
  expect(requests.some((request) => request.method !== 'GET')).toBe(false);
  expect(requests.some((request) => new URL(request.url).searchParams.has('before_id') !== new URL(request.url).searchParams.has('before_created_at'))).toBe(false);
  await panel.getByRole('button', { name: 'Load older sessions' }).click();
  await expect(panel.getByText('session-first')).toBeVisible();
  await expect(panel.getByText('session-oldest')).toBeVisible();
  const olderRequest = requests.find((request) => new URL(request.url).searchParams.has('before_id'));
  expect(olderRequest).toBeDefined();
  const cursor = new URL(olderRequest!.url).searchParams;
  expect(cursor.get('before_id')).toBe('session-48');
  expect(cursor.get('before_created_at')).toBe('2025-01-02T23:11:00Z');
  expect(requests.every((request) => request.method === 'GET')).toBe(true);
});

test('Thread owner sees a verified source edit and inbox notice after reload without operator controls', async ({ page }) => {
  const fixture = await runtime();
  await page.goto(fixture.url);
  await page.getByLabel('Access token').fill(fixture.submitterToken);
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page.getByRole('heading', { name: 'Threads', exact: true })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Inbox' })).toBeVisible();
  const submitterCookie = (await page.context().cookies(new URL(fixture.url).origin)).find((cookie) => cookie.name === 'factory_operator_session');
  expect(submitterCookie?.httpOnly).toBe(true);
  expect(submitterCookie?.secure).toBe(true);
  for (const forbidden of ['Schedules', 'Worker sessions', 'Recent jobs', 'Admit a job']) {
    await expect(page.getByRole('heading', { name: forbidden, exact: true })).toHaveCount(0);
  }

  await page.getByLabel('Repository alias', { exact: true }).fill('fixture');
  await page.getByLabel('Task', { exact: true }).fill(sourceEditTask);
  await page.getByRole('button', { name: 'Create Thread with Job' }).click();
  const threadHeading = page.getByRole('heading', { name: /^Thread [0-9a-f-]+$/ });
  await expect(threadHeading).toBeVisible();
  const threadID = (await threadHeading.textContent())?.replace('Thread ', '').trim();
  expect(threadID).toBeTruthy();
  const job = page.locator('.thread-job').first();
  await expect(job.locator('.status')).toHaveText('succeeded', { timeout: 30_000 });
  await expect(job.getByRole('heading', { name: 'Safe outcome' })).toBeVisible();
  const passedCheck = job.locator('.safe-outcome').getByText('check-01: passed', { exact: true });
  await expect(passedCheck).toBeVisible();
  await expect(page.locator('.notification-list').getByRole('button', { name: `Thread ${threadID}` })).toBeVisible({ timeout: 10_000 });
  await expect(page.getByText(sourceEditTask)).toHaveCount(0);

  await page.reload();
  await expect(page.getByRole('heading', { name: 'Threads', exact: true })).toBeVisible();
  await page.locator('.thread-list').getByRole('button', { name: threadID! }).click();
  await expect(page.getByRole('heading', { name: `Thread ${threadID}` })).toBeVisible();
  await expect(job.locator('.status')).toHaveText('succeeded');
  await expect(passedCheck).toBeVisible();
  await expect(page.locator('.notification-list').getByRole('button', { name: `Thread ${threadID}` })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Schedules' })).toHaveCount(0);
});

test('operator can sign in, admit and inspect a job, and sign out', async ({ page, context }) => {
  const fixture = await runtime();
  const origin = new URL(fixture.url).origin;
  await page.goto(fixture.url);
  await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Queue job' })).toHaveCount(0);

  await page.getByLabel('Access token').fill('not-the-operator-token');
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page.getByRole('alert')).toContainText('enabled account token was not accepted');
  await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();

  await page.getByLabel('Access token').fill(fixture.token);
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page.getByRole('heading', { name: 'Jobs', exact: true })).toBeVisible();
  const cookies = await context.cookies(origin);
  const session = cookies.find((cookie) => cookie.name === 'factory_operator_session');
  expect(session, 'dashboard session cookie').toBeDefined();
  expect(session?.secure).toBe(true);
  expect(session?.httpOnly).toBe(true);
  expect(session?.sameSite).toBe('Strict');
  expect(session?.value).not.toBe(fixture.token);

  await page.getByLabel('Repository alias').first().fill('fixture');
  await page.getByLabel('Task').first().fill(sourceEditTask);
  await page.getByRole('button', { name: 'Queue job' }).click();
  await expect(page.locator('.notice')).toContainText(' admitted.');
  const admissionNotice = (await page.locator('.notice').textContent()) || '';
  const jobID = admissionNotice.match(/Job ([0-9a-f-]+) admitted\./)?.[1];
  expect(jobID).toBeTruthy();
  const jobLink = page.locator('.job-link').filter({ hasText: jobID! });
  await expect(jobLink).toBeVisible({ timeout: 10_000 });
  await jobLink.click();
  await expect(page.getByRole('heading', { name: `Job ${jobID}` })).toBeVisible();
  await expect(page.getByText('succeeded', { exact: true }).last()).toBeVisible({ timeout: 15_000 });
  await expect(page.getByText('Pull request')).toHaveCount(0);

  await page.getByLabel('Repository alias').nth(1).fill('fixture');
  await page.getByLabel('Task').nth(1).fill(sourceEditTask);
  await page.getByRole('button', { name: 'Create Thread' }).click();
  const threadHeading = page.getByRole('heading', { name: /^Thread [0-9a-f-]+$/ });
  await expect(threadHeading).toBeVisible({ timeout: 10_000 });
  const threadID = (await threadHeading.textContent())?.replace('Thread ', '').trim();
  expect(threadID).toBeTruthy();
  await expect(page.getByText(sourceEditTask)).toHaveCount(0);
  await page.reload();
  await expect(page.getByRole('heading', { name: 'Jobs', exact: true })).toBeVisible();
  await page.getByRole('button', { name: threadID! }).click();
  await expect(page.getByRole('heading', { name: `Thread ${threadID}` })).toBeVisible();
  await expect(page.getByText(sourceEditTask)).toHaveCount(0);

  await page.getByRole('button', { name: 'Sign out' }).click();
  await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();
  const protectedResponse = await page.request.get(`${origin}/ui/api/v1/jobs?limit=100`);
  expect(protectedResponse.status()).toBe(401);
  await expect.poll(async () => (await context.cookies(origin)).some((cookie) => cookie.name === 'factory_operator_session')).toBe(false);
});
