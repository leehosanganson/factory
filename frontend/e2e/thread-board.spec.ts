import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';
import { readFile } from 'node:fs/promises';
import type { Runtime } from './runtime.js';

async function fixture(): Promise<Runtime> {
  const path = test.info().config.metadata.dashboardRuntimeFile;
  if (typeof path !== 'string') throw new Error('Playwright runtime fixture path is missing');
  return JSON.parse(await readFile(path, 'utf8'));
}

async function signIn(page: Page, token: string) {
  await page.getByLabel('Access token').fill(token);
  await page.getByRole('button', { name: 'Sign in' }).click();
}

async function addDraft(page: Page, task: string) {
  await page.getByLabel('Repository alias', { exact: true }).fill('fixture');
  await page.getByLabel('Task', { exact: true }).fill(task);
  await page.getByRole('button', { name: 'Add draft', exact: true }).click();
}

const lane = (page: Page, name: string) => page.getByRole('region', { name, exact: true });

const firstPage = '**/ui/api/v1/threads?limit=50';
const olderPage = '**/ui/api/v1/threads?limit=50&cursor=*';

test('owned pagination opens real older Threads, retains them on polling, and resets on Refresh', async ({ page }) => {
  const { url, submitterToken } = await fixture();
  await page.clock.install();
  await page.goto(url);
  await signIn(page, submitterToken);
  const board = lane(page, 'Your Threads');
  const load = board.getByRole('button', { name: 'Load older Threads', exact: true });
  const oldest = board.getByRole('button', { name: 'retained-thread-1', exact: true });
  const checkPrivacy = async () => {
    for (const id of ['retained-thread-2', 'retained-thread-54']) {
      await expect(board.getByRole('button', { name: id, exact: true })).toHaveCount(0);
    }
    await expect(board.getByText('Private fixture task')).toHaveCount(0);
  };
  await expect(load).toBeEnabled();
  await expect(board.locator('[data-thread-id]')).toHaveCount(50);
  await expect(oldest).toHaveCount(0);
  await checkPrivacy();
  const continuation = page.waitForResponse(olderPage);
  await load.press('Enter');
  expect((await continuation).ok()).toBe(true);
  await expect(oldest).toBeVisible();
  await expect(load).toBeFocused();
  await expect(load).toBeDisabled();
  await expect(board.getByRole('status')).toHaveText(/No older Threads in this history view/);
  await checkPrivacy();
  const poll = page.waitForResponse(firstPage);
  await page.clock.runFor(5000);
  await poll;
  await oldest.press('Enter');
  await expect(board.getByRole('heading', { name: 'Thread retained-thread-1' })).toBeFocused();
  await expect(board.getByText('Job retained-job-1')).toBeVisible();
  await expect(board.getByText('Thread status: done')).toBeVisible();
  await checkPrivacy();
  await page.getByRole('button', { name: 'Refresh', exact: true }).click();
  await expect(oldest).toHaveCount(0);
  await expect(load).toBeEnabled();
});

test('owned pagination keeps loaded cards on cursor rejection and resumes after Refresh', async ({ page }) => {
  const { url, submitterToken } = await fixture();
  const now = new Date().toISOString();
  const thread = (id: string) => ({ id, status: 'done', created_at: now, updated_at: now });
  await page.route(firstPage, (route) => route.fulfill({ json: {
    threads: [thread('newest')], has_more: true, next_cursor: 'first',
  } }));
  await page.route('**/ui/api/v1/threads?limit=50&cursor=first', (route) => route.fulfill({ json: {
    threads: [thread('older')], has_more: true, next_cursor: 'no-such-cursor',
  } }));
  await page.goto(url);
  await signIn(page, submitterToken);
  const board = lane(page, 'Your Threads');
  const load = board.getByRole('button', { name: 'Load older Threads', exact: true });
  await load.click();
  await expect(board.getByRole('button', { name: 'older', exact: true })).toBeVisible();
  await load.press('Enter');
  await expect(board.getByRole('status')).toHaveText(/expired.*Refresh/);
  await expect(board.getByRole('button', { name: 'older', exact: true })).toBeVisible();
  await expect(load).toBeFocused();
  await expect(load).toBeDisabled();
  await page.getByRole('button', { name: 'Refresh', exact: true }).click();
  await expect(load).toBeEnabled();
  await expect(board.getByRole('button', { name: 'older', exact: true })).toHaveCount(0);
});

for (const pollFails of [false, true]) {
  test(`keyboard selection opens and focuses Thread detail while the background poll ${pollFails ? 'fails' : 'is stalled'}`, async ({ page }) => {
    const { url, submitterToken } = await fixture();
    const now = new Date().toISOString();
    const id = 'keyboard-thread';
    const thread = { id, status: 'done', created_at: now, updated_at: now };
    const detailURL = `**/ui/api/v1/threads/${id}`;
    await page.route(firstPage, (route) => route.fulfill({ json: { threads: [thread] } }));
    await page.clock.install();
    await page.goto(url);
    await signIn(page, submitterToken);
    const ticket = lane(page, 'Your Threads').getByRole('button', { name: id, exact: true });
    let releaseSelection!: () => void;
    let releasePoll!: () => void;
    const selectionHeld = new Promise<void>((resolve) => { releaseSelection = resolve; });
    const pollHeld = new Promise<void>((resolve) => { releasePoll = resolve; });
    const failure = 'Background detail temporarily unavailable';
    await page.route(detailURL, async (route) => {
      if (!pollFails) await pollHeld;
      await route.fulfill({ status: 503, json: { message: failure } });
    });
    await page.route(detailURL, async (route) => {
      await selectionHeld;
      await route.fulfill({ json: { thread } });
    }, { times: 1 });
    try {
      const selection = page.waitForRequest(detailURL);
      await ticket.press('Enter');
      await selection;
      const poll = page.waitForRequest(detailURL);
      const failed = pollFails ? page.waitForResponse((response) => response.url().endsWith(`/threads/${id}`) && response.status() === 503) : null;
      await page.clock.runFor(5000);
      await poll;
      await failed;
      releaseSelection();
      await expect(page.getByRole('heading', { name: `Thread ${id}` })).toBeFocused();
      await expect(page.getByText(failure)).toHaveCount(0);
    } finally {
      releaseSelection();
      releasePoll();
    }
  });
}

for (const stalled of ['quiet refresh', 'Load older Threads']) {
  test(`quiet refresh recovers after a stalled ${stalled}`, async ({ page }) => {
    const { url, submitterToken } = await fixture();
    const now = new Date().toISOString();
    const thread = (id: string) => ({ id, status: 'done', created_at: now, updated_at: now });
    await page.route(firstPage, (route) => route.fulfill({ json: {
      threads: [thread('before-stall')], has_more: true, next_cursor: 'first',
    } }));
    await page.clock.install();
    await page.goto(url);
    await signIn(page, submitterToken);
    const board = lane(page, 'Your Threads');
    const load = board.getByRole('button', { name: 'Load older Threads', exact: true });
    await expect(load).toBeEnabled();
    await page.route(firstPage, (route) => route.fulfill({ json: {
      threads: [thread('after-recovery')], has_more: true, next_cursor: 'first',
    } }));
    const quiet = stalled === 'quiet refresh';
    const stalledURL = quiet ? firstPage : olderPage;
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    await page.route(stalledURL, async (route) => {
      await held;
      await route.fulfill({ json: { threads: [thread('stale-result')] } });
    }, { times: 1 });
    try {
      const request = page.waitForRequest(stalledURL);
      if (quiet) await page.clock.runFor(5000);
      else await load.press('Enter');
      const stalledRequest = await request;
      const aborted = page.waitForEvent('requestfailed', { predicate: (failed) => failed === stalledRequest, timeout: 2500 });
      await page.clock.runFor(20_000);
      await Promise.all([
        expect(board.getByRole('button', { name: 'after-recovery' })).toBeVisible({ timeout: 2000 }),
        aborted,
      ]);
      release();
      await expect(board.getByRole('button', { name: 'stale-result' })).toHaveCount(0);
      await expect(load).toBeEnabled();
    } finally { release(); }
  });
}

test('a delayed Thread detail cannot undo a newer recorded status', async ({ page }) => {
  const { url, submitterToken } = await fixture();
  const now = new Date().toISOString();
  const id = 'changing-thread';
  const thread = { id, status: 'done', created_at: now, updated_at: now };
  await page.route(firstPage, (route) => route.fulfill({ json: { threads: [thread] } }));
  await page.clock.install();
  await page.goto(url);
  await signIn(page, submitterToken);
  const ticket = lane(page, 'Your Threads').getByRole('button', { name: id, exact: true });
  await expect(ticket).toBeVisible();
  let release!: () => void;
  const held = new Promise<void>((resolve) => { release = resolve; });
  const detailURL = `**/ui/api/v1/threads/${id}`;
  await page.route(detailURL, (route) => route.fulfill({ json: { thread: { ...thread, status: 'in_progress' } } }));
  await page.route(detailURL, async (route) => {
    await held;
    await route.fulfill({ json: { thread } });
  }, { times: 1 });
  try {
    const request = page.waitForRequest(detailURL);
    await ticket.click();
    await request;
    await page.clock.runFor(5000);
    await expect(page.getByText('Thread status: in_progress')).toBeVisible();
    const stale = page.waitForResponse(detailURL);
    release();
    await stale;
    await expect(page.getByText('Thread status: done')).not.toBeVisible({ timeout: 1000 });
    await expect(page.getByText('Thread status: in_progress')).toBeVisible();
    await expect(lane(page, 'Work').getByRole('button', { name: id, exact: true })).toBeVisible();
  } finally { release(); }
});

test('drafts admit only from the Work drop zone or Queue, and retry a lost response without duplicate admission', async ({ page, context }) => {
  const { url, submitterToken } = await fixture();
  const writes: { path: string; key: string; body: unknown }[] = [];
  page.on('request', (request) => {
    if (request.method() === 'POST' && request.url().includes('/ui/api/')) {
      writes.push({ path: new URL(request.url()).pathname, key: request.headers()['idempotency-key'], body: request.postDataJSON() });
    }
  });
  let release!: () => void;
  const held = new Promise<void>((resolve) => { release = resolve; });
  await page.route('**/ui/api/v1/threads', async (route) => {
    const response = await route.fetch();
    expect(response.ok()).toBe(true);
    await held;
    await route.abort();
  }, { times: 1 });
  await page.goto(url);
  await signIn(page, submitterToken);
  const task = 'Change RELEASE_LABEL in release.py from draft to ready.';
  const otherTask = 'Keep this task in this tab only.';
  const drafts = lane(page, 'Drafts');
  const ticket = drafts.getByRole('article').filter({ hasText: task });
  const queue = ticket.getByRole('button', { name: /^Queue(?:ing…)?$/ });
  try {
    await addDraft(page, task);
    await addDraft(page, otherTask);
    await expect(ticket).toBeVisible();
    const anotherTab = await context.newPage();
    await anotherTab.goto(url);
    await expect(lane(anotherTab, 'Drafts')).toBeVisible();
    await expect(lane(anotherTab, 'Drafts')).not.toContainText(task);
    await anotherTab.close();
    await ticket.dragTo(lane(page, 'Finished'));
    await ticket.dragTo(page.getByRole('heading', { name: 'Work', exact: true }));
    await expect(ticket).toBeVisible();
    expect(writes).toHaveLength(0);
    await ticket.dragTo(lane(page, 'Work drop zone'));
    await expect(queue).toBeDisabled();
    await expect(drafts.getByRole('button', { name: 'Queue', exact: true })).toBeDisabled();
    await expect(ticket).not.toHaveAttribute('draggable', 'true');
    expect(writes).toHaveLength(1);
  } finally { release(); }
  await expect(ticket.getByRole('alert')).toBeVisible();
  await queue.click();
  const heading = page.getByRole('heading', { name: /^Thread [0-9a-f-]+$/ });
  await expect(heading).toBeFocused();
  await expect(ticket).toHaveCount(0);
  await expect(drafts).toContainText(otherTask);
  expect(writes).toHaveLength(2);
  expect(writes[0]).toMatchObject({ path: '/ui/api/v1/threads', body: { repository: 'fixture', task } });
  expect(writes[0].key).toBeTruthy();
  expect(writes[1]).toEqual(writes[0]);
  await expect(page.locator('.thread-job')).toHaveCount(1);
  await page.reload();
  await expect(drafts).toBeVisible();
  await expect(drafts).not.toContainText(otherTask);
  await addDraft(page, 'Clear this draft at sign-out.');
  await page.getByRole('button', { name: 'Sign out' }).click();
  await signIn(page, submitterToken);
  await expect(drafts).toBeVisible();
  await expect(drafts).not.toContainText('Clear this draft at sign-out.');
  expect(writes).toHaveLength(2);
});

test('Queue keeps keyboard focus through a failed admission and can be retried in place', async ({ page }) => {
  const { url, submitterToken } = await fixture();
  const writes: string[] = [];
  page.on('request', (request) => {
    if (request.method() === 'POST' && new URL(request.url()).pathname === '/ui/api/v1/threads') {
      writes.push(request.headers()['idempotency-key']);
    }
  });
  let release!: () => void;
  const held = new Promise<void>((resolve) => { release = resolve; });
  await page.route('**/ui/api/v1/threads', async (route) => {
    await held;
    await route.abort();
  }, { times: 1 });
  await page.goto(url);
  await signIn(page, submitterToken);
  const task = 'Retry this draft without moving keyboard focus.';
  await addDraft(page, task);
  const ticket = lane(page, 'Drafts').getByRole('article').filter({ hasText: task });
  const queue = ticket.getByRole('button', { name: /^Queue(?:ing…)?$/ });
  await queue.focus();
  try {
    await page.keyboard.press('Enter');
    await expect(queue).toBeDisabled();
    await expect(queue).toBeFocused();
    await page.keyboard.press('Space');
    await page.keyboard.press('Enter');
    expect(writes).toHaveLength(1);
  } finally { release(); }
  await expect(ticket.getByRole('alert')).toBeVisible();
  await expect(queue).toBeEnabled();
  await expect(queue).toBeFocused();
  await page.keyboard.press('Enter');
  await expect(page.getByRole('heading', { name: /^Thread [0-9a-f-]+$/ })).toBeFocused();
  expect(writes).toHaveLength(2);
  expect(writes[0]).toBeTruthy();
  expect(writes[1]).toBe(writes[0]);
});

test('draft field borders contrast with the field and surrounding lane', async ({ page }) => {
  const { url, submitterToken } = await fixture();
  await page.goto(url);
  await signIn(page, submitterToken);
  for (const name of ['Repository alias', 'Task']) {
    const ratio = await lane(page, 'Drafts').getByLabel(name, { exact: true }).evaluate((field) => {
      function luminance(color: string) {
        const channels = color.match(/[\d.]+/g)!.slice(0, 3).map((channel) => {
          const value = Number(channel) / 255;
          return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
        });
        return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
      }
      const style = getComputedStyle(field);
      const border = luminance(style.borderTopColor);
      const backgrounds = [style.backgroundColor, getComputedStyle(field.closest('section')!).backgroundColor];
      return Math.min(...backgrounds.map((color) => {
        const background = luminance(color);
        return (Math.max(border, background) + 0.05) / (Math.min(border, background) + 0.05);
      }));
    });
    expect(ratio, name).toBeGreaterThanOrEqual(3);
  }
});

test('a replaced account session does not retain the old tab draft', async ({ page, context }) => {
  const { url, submitterToken, token } = await fixture();
  await page.goto(url);
  await signIn(page, submitterToken);
  const draftTask = 'Do not show this draft after the shared session changes.';
  await addDraft(page, draftTask);
  await expect(lane(page, 'Drafts')).toContainText(draftTask);

  const otherTab = await context.newPage();
  await otherTab.goto(url);
  await otherTab.getByRole('button', { name: 'Sign out' }).click();
  await signIn(otherTab, token);
  await expect(otherTab.getByRole('heading', { name: 'Jobs', exact: true })).toBeVisible();
  await page.bringToFront();
  await expect(page.getByRole('heading', { name: 'Jobs', exact: true })).toBeVisible({ timeout: 10_000 });
  await expect(page.getByText(draftTask)).toHaveCount(0);
  await page.getByRole('button', { name: 'Sign out' }).click();
  await signIn(page, submitterToken);
  await expect(lane(page, 'Drafts')).toBeVisible();
  await expect(lane(page, 'Drafts')).not.toContainText(draftTask);
});

test('submitted cards follow recorded Thread status without treating Finished as approval', async ({ page }) => {
  const { url, submitterToken } = await fixture();
  const now = new Date().toISOString();
  const thread = (id: string, status: string) => ({ id, status, created_at: now, updated_at: now });
  await page.route('**/ui/api/v1/threads?limit=50', (route) => route.fulfill({ json: {
    threads: [thread('active-ticket', 'in_progress'), thread('attention-ticket', 'need_attention'), thread('finished-ticket', 'done'), thread('unknown-ticket', 'future_state')],
  } }));
  await page.goto(url);
  await signIn(page, submitterToken);
  await expect(lane(page, 'Work').getByRole('button', { name: 'active-ticket' })).toBeVisible();
  await expect(lane(page, 'Needs Attention').getByRole('button', { name: 'attention-ticket' })).toBeVisible();
  await expect(lane(page, 'Finished').getByRole('button', { name: 'finished-ticket' })).toBeVisible();
  await expect(lane(page, 'Finished')).toContainText('Not an approval');
  await expect(lane(page, 'Unclassified')).toContainText('future_state');
  await expect(lane(page, 'Your Threads').locator('[draggable="true"]')).toHaveCount(0);
});

test('refresh preserves focus on a ticket that moves lanes without scrolling or stealing focus', async ({ page }) => {
  const { url, submitterToken } = await fixture();
  const now = new Date().toISOString();
  let status = 'in_progress';
  await page.route('**/ui/api/v1/threads?limit=50', (route) => route.fulfill({ json: {
    threads: [{ id: 'moving-ticket', status, created_at: now, updated_at: now }],
  } }));
  await page.setViewportSize({ width: 390, height: 480 });
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await page.clock.install();
  await page.goto(url);
  await signIn(page, submitterToken);
  const workTicket = lane(page, 'Work').getByRole('button', { name: 'moving-ticket' });
  const finishedTicket = lane(page, 'Finished').getByRole('button', { name: 'moving-ticket' });
  await workTicket.focus();
  await expect(workTicket).toBeFocused();
  await page.evaluate(() => window.scrollTo(0, 0));
  status = 'done';
  await page.clock.runFor(5000);
  await expect(finishedTicket).toBeFocused();
  await expect(workTicket).toHaveCount(0);
  expect(await page.evaluate(() => window.scrollY)).toBe(0);

  const task = page.getByLabel('Task', { exact: true });
  await task.focus();
  status = 'in_progress';
  await page.clock.runFor(5000);
  await expect(workTicket).toBeVisible();
  await expect(task).toBeFocused();
});
