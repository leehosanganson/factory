import { expect, test } from '@playwright/test';
import { readFile } from 'node:fs/promises';
import type { Runtime } from './runtime.js';

const scheduleID = 'fixture-schedule-run';
const occurrence = '2025-01-01T00:00:00Z';

test('operator opens the recorded outcome from a scheduled run', async ({ page }) => {
  const runtimeFile = test.info().config.metadata.dashboardRuntimeFile;
  if (typeof runtimeFile !== 'string') throw new Error('Playwright runtime fixture path is missing');
  const fixture = JSON.parse(await readFile(runtimeFile, 'utf8')) as Runtime;

  await page.goto(fixture.url);
  await page.getByLabel('Access token').fill(fixture.token);
  await page.getByRole('button', { name: 'Sign in' }).click();

  const schedule = page.getByRole('region', { name: 'Schedules' }).getByRole('article').filter({
    has: page.getByRole('heading', { name: scheduleID, exact: true }),
  });
  const run = schedule.getByRole('listitem').filter({ hasText: new Date(occurrence).toISOString() });
  await expect(run.getByText('succeeded', { exact: true })).toBeVisible({ timeout: 20_000 });

  const runsResponse = await page.request.get(`${fixture.url}/ui/api/v1/schedules/${scheduleID}/runs?limit=5`);
  expect(runsResponse.ok()).toBe(true);
  const { runs } = await runsResponse.json() as { runs: { occurrence_at: string; job_id: string; status: string }[] };
  const recordedRun = runs.find((entry) => entry.occurrence_at === occurrence);
  if (!recordedRun) throw new Error(`Controller did not record the ${occurrence} run`);
  expect(recordedRun.status).toBe('succeeded');
  const jobID = recordedRun.job_id;
  const detail = page.getByRole('region', { name: `Job ${jobID}`, exact: true });
  await expect(detail).toHaveCount(0);
  const jobResponse = page.waitForResponse((response) => response.request().method() === 'GET'
    && response.url() === new URL(`/ui/api/v1/jobs/${jobID}`, fixture.url).href);
  await run.getByRole('link', { name: `Job ${jobID}`, exact: true }).click();
  const response = await jobResponse;
  expect(response.ok()).toBe(true);
  expect(await response.json()).toMatchObject({
    id: jobID,
    status: 'succeeded',
    verification: { checks: [{ name: 'check-01', outcome: 'passed' }] },
  });

  await expect(detail.getByRole('heading', { name: `Job ${jobID}`, exact: true })).toBeVisible();
  await expect(detail.getByText('succeeded', { exact: true })).toBeVisible();
  await expect(detail.getByText('check-01: passed', { exact: true })).toBeVisible();
});
