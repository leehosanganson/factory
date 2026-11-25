import { expect, test } from '@playwright/test';
import type { Route } from '@playwright/test';
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
  await page.getByRole('button', { name: 'Add draft' }).click();
  await page.getByRole('button', { name: 'Queue', exact: true }).click();
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
  await page.getByRole('region', { name: 'Your Threads' }).getByRole('button', { name: threadID!, exact: true }).click();
  await expect(page.getByRole('heading', { name: `Thread ${threadID}` })).toBeVisible();
  await expect(job.locator('.status')).toHaveText('succeeded');
  await expect(passedCheck).toBeVisible();
  await expect(page.locator('.notification-list').getByRole('button', { name: `Thread ${threadID}` })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Schedules' })).toHaveCount(0);
});

for (const role of ['submitter', 'operator'] as const) {
  test(`${role} Thread selection moves mobile focus and viewport, but polling does not`, async ({ page }) => {
    const fixture = await runtime();
    await page.setViewportSize({ width: 390, height: 844 });
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto(fixture.url);
    await page.getByLabel('Access token').fill(role === 'submitter' ? fixture.submitterToken : fixture.token);
    await page.getByRole('button', { name: 'Sign in' }).click();

    const threads = page.getByRole('region', { name: role === 'submitter' ? 'Your Threads' : 'Threads', exact: true });
    await threads.getByLabel('Repository alias').fill('fixture');
    await threads.getByLabel('Task', { exact: true }).fill('Create a Thread to reopen on mobile.');
    const create = threads.getByRole('button', { name: role === 'submitter' ? 'Add draft' : 'Create Thread', exact: true });
    await create.click();
    if (role === 'submitter') await threads.getByRole('button', { name: 'Queue', exact: true }).click();
    const heading = threads.getByRole('heading', { name: /^Thread [0-9a-f-]+$/ });
    await expect(heading).toBeVisible();
    const threadName = (await heading.innerText()).trim();
    const threadID = threadName.slice('Thread '.length);
    if (role === 'operator') {
      // Unrelated list updates above Threads can change the page height during this assertion.
      await page.route('**/ui/api/v1/jobs?limit=100', (route) => route.fulfill({ json: { jobs: [] } }));
      await page.route('**/ui/api/v1/schedules', (route) => route.fulfill({ json: { schedules: [] } }));
    }
    await page.reload();

    const select = threads.getByRole('button', { name: threadID, exact: true });
    const detail = threads.getByRole('heading', { name: threadName, exact: true });
    for (let selection = 0; selection < 2; selection++) {
      await select.evaluate((button) => button.scrollIntoView({ block: 'end' }));
      await expect(detail).not.toBeInViewport();
      await select.press('Enter');
      await expect(detail).toBeVisible();
      await expect.soft(detail).toBeFocused();
      await expect.soft(detail).toBeInViewport({ ratio: 0.95 });
      if (test.info().errors.length) return;
    }

    await create.evaluate((button) => {
      button.scrollIntoView({ block: 'end' });
      button.focus({ preventScroll: true });
    });
    await expect(detail).not.toBeInViewport();
    const scrollY = await page.evaluate(() => window.scrollY);
    await page.route(`**/ui/api/v1/threads/${threadID}`, async (route) => {
      const response = await route.fetch();
      const body = await response.json();
      body.thread.status = 'need_attention';
      await route.fulfill({ response, json: body });
    }, { times: 1 });
    await expect(threads.locator('.thread-result').getByText(role === 'submitter' ? 'Thread status: need_attention' : 'need_attention', { exact: true })).toBeVisible({ timeout: 10_000 });
    await expect(create).toBeFocused();
    expect(await page.evaluate(() => window.scrollY)).toBe(scrollY);
  });
}

for (const failure of ['none', 'refresh', 'selection'] as const) {
  test(`Thread reselection during completion (failure: ${failure})`, async ({ page }) => {
    const fixture = await runtime();
    await page.clock.install({ time: new Date('2025-01-01T00:00:00Z') });
    await page.clock.pauseAt(new Date('2025-01-01T00:01:00Z'));
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto(fixture.url);
    await page.getByLabel('Access token').fill(fixture.token);
    await page.getByRole('button', { name: 'Sign in' }).click();

    const threads = page.getByRole('region', { name: 'Threads', exact: true });
    await threads.getByLabel('Repository alias').fill('fixture');
    await threads.getByLabel('Task', { exact: true }).fill('Create a Thread for the completion race.');
    await threads.getByRole('button', { name: 'Create Thread', exact: true }).click();
    await expect(threads.locator('.thread-job')).toHaveCount(1);
    const heading = threads.getByRole('heading', { name: /^Thread [0-9a-f-]+$/ });
    const id = (await heading.innerText()).trim().slice('Thread '.length);
    const detailURL = `**/ui/api/v1/threads/${id}`;
    const selection = threads.getByRole('button', { name: id, exact: true });
    async function holdNextRequest(url: string) {
      let capture!: (route: Route) => void;
      const request = new Promise<Route>((resolve) => { capture = resolve; });
      let held = false;
      await page.route(url, (route) => {
        if (held) return route.fallback();
        held = true;
        capture(route);
      });
      return { request };
    }
    const completionRequest = await holdNextRequest(`${detailURL}/complete`);
    const selectionRequest = await holdNextRequest(detailURL);

    const complete = threads.getByRole('button', { name: 'Complete Thread' });
    await complete.click();
    const completion = await completionRequest.request;
    await selection.press('Enter');
    const selectionRoute = await selectionRequest.request;
    const openDetail = await selectionRoute.fetch();
    expect(openDetail.ok()).toBe(true);
    await expect(selection).toBeFocused();
    const refreshRequest = await holdNextRequest(detailURL);
    await completion.continue();
    const refreshRoute = await refreshRequest.request;
    const controls = threads.getByRole('region', { name: 'Current Thread actions' });
    const selectionFinished = page.waitForEvent('requestfinished', (request) => request === selectionRoute.request());
    if (failure === 'selection') {
      await selectionRoute.fulfill({ status: 503, json: { message: 'Selection unavailable' } });
      await selectionFinished;
      await refreshRoute.continue();
      await expect(controls).toHaveCount(0);
      await expect(selection).toBeFocused();
      return;
    }

    if (failure === 'refresh') {
      await refreshRoute.fulfill({ status: 503, json: { message: 'Completion refresh unavailable' } });
      await expect(page.getByRole('status').filter({ hasText: 'Completion refresh unavailable' })).toBeVisible();
      await expect(selection).toBeFocused();
    } else {
      await refreshRoute.continue();
      await expect(controls).toHaveCount(0);
      await expect(heading).toBeFocused();
    }
    await selectionRoute.fulfill({ response: openDetail });
    await selectionFinished;
    await page.unrouteAll({ behavior: 'wait' });
    if (failure === 'refresh') {
      await page.clock.runFor(6000);
      await expect(controls).toHaveCount(0);
      await expect(heading).toBeFocused();
      return;
    }

    await page.route(detailURL, (route) => route.fulfill({ status: 503, json: { message: 'Detail unavailable' } }), { times: 1 });
    await selection.click();
    await expect(page.getByRole('status').filter({ hasText: 'Detail unavailable' })).toBeVisible();
    await expect(controls).toHaveCount(0);
    await page.route(detailURL, async (route) => {
      const response = await route.fetch();
      const body = await response.json();
      body.thread.status = 'need_attention';
      await route.fulfill({ response, json: body });
    }, { times: 1 });
    await page.clock.runFor(6000);
    await expect(threads.locator('.thread-result').getByText('need_attention', { exact: true })).toBeVisible();
    await expect(selection).toBeFocused();
  });
}

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
