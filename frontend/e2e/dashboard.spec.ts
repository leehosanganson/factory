import { expect, test } from '@playwright/test';
import { readFile } from 'node:fs/promises';
import type { Runtime } from './runtime.js';

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
  await page.getByLabel('Operator token').fill(fixture.token);
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

test('operator can sign in, admit and inspect a job, and sign out', async ({ page, context }) => {
  const fixture = await runtime();
  const origin = new URL(fixture.url).origin;
  await page.goto(fixture.url);
  await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Queue job' })).toHaveCount(0);

  await page.getByLabel('Operator token').fill('not-the-operator-token');
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page.getByRole('alert')).toContainText('configured named operator token was not accepted');
  await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();

  await page.getByLabel('Operator token').fill(fixture.token);
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
  await page.getByLabel('Task').first().fill('Write the fixture result file.');
  await page.getByRole('button', { name: 'Queue job' }).click();
  const jobLink = page.locator('.job-link').first();
  await expect(jobLink).toBeVisible({ timeout: 10_000 });
  const jobID = (await jobLink.textContent())?.trim();
  expect(jobID).toBeTruthy();
  await expect(page.getByRole('status').filter({ hasText: `Job ${jobID} admitted.` })).toBeVisible();
  await jobLink.click();
  await expect(page.getByRole('heading', { name: `Job ${jobID}` })).toBeVisible();
  await expect(page.getByText('succeeded', { exact: true }).last()).toBeVisible({ timeout: 15_000 });
  await expect(page.getByText('Pull request')).toHaveCount(0);

  const threadTask = 'Create a browser-visible Thread for follow-up.';
  await page.getByLabel('Repository alias').nth(1).fill('fixture');
  await page.getByLabel('Task').nth(1).fill(threadTask);
  await page.getByRole('button', { name: 'Create Thread' }).click();
  const threadHeading = page.getByRole('heading', { name: /^Thread [0-9a-f-]+$/ });
  await expect(threadHeading).toBeVisible({ timeout: 10_000 });
  const threadID = (await threadHeading.textContent())?.replace('Thread ', '').trim();
  expect(threadID).toBeTruthy();
  await expect(page.getByText(threadTask)).toHaveCount(0);
  await page.reload();
  await expect(page.getByRole('heading', { name: 'Jobs', exact: true })).toBeVisible();
  await page.getByRole('button', { name: threadID! }).click();
  await expect(page.getByRole('heading', { name: `Thread ${threadID}` })).toBeVisible();
  await expect(page.getByText(threadTask)).toHaveCount(0);

  await page.getByRole('button', { name: 'Sign out' }).click();
  await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();
  const protectedResponse = await page.request.get(`${origin}/ui/api/v1/jobs?limit=100`);
  expect(protectedResponse.status()).toBe(401);
  await expect.poll(async () => (await context.cookies(origin)).some((cookie) => cookie.name === 'factory_operator_session')).toBe(false);
});
