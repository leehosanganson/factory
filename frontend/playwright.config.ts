import { defineConfig, devices } from '@playwright/test';
import { resolve } from 'node:path';
import { chmodSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';

const frontendDir = resolve(import.meta.dirname);
const runtimeDir = mkdtempSync(resolve(tmpdir(), 'factory-dashboard-playwright-'));
const runtimeFile = resolve(runtimeDir, 'dashboard-runtime.json');
const port = 47000 + Math.floor(Math.random() * 15000);
const fixture = resolve(frontendDir, 'e2e/fixture.py');
const binary = resolve(frontendDir, '../bin/factory');

export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 1 : 0,
  workers: 1,
  reporter: 'list',
  outputDir: 'test-results',
  metadata: { dashboardRuntimeFile: runtimeFile },
  globalTeardown: './e2e/global-teardown.ts',
  use: {
    ...devices['Desktop Chrome'],
    launchOptions: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE ? { executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE } : {},
    baseURL: `https://127.0.0.1:${port}`,
    ignoreHTTPSErrors: true,
    trace: 'off',
    screenshot: 'off',
    video: 'off',
  },
  webServer: {
    command: `exec python3 "${fixture}" --binary "${binary}" --port ${port} --runtime-file "${runtimeFile}"`,
    url: `https://127.0.0.1:${port}/ui/session`,
    ignoreHTTPSErrors: true,
    reuseExistingServer: false,
    timeout: 60_000,
    stdout: 'pipe',
    stderr: 'pipe',
  },
});
