import { readFile, rm } from 'node:fs/promises';
import { dirname } from 'node:path';
import type { FullConfig } from '@playwright/test';

export default async function globalTeardown(config: FullConfig) {
  const runtimeFile = config.metadata.dashboardRuntimeFile;
  if (typeof runtimeFile !== 'string') return;
  try {
    const runtime = JSON.parse(await readFile(runtimeFile, 'utf8')) as { fixture_pid?: number; server_pid?: number };
    for (const pid of [runtime.server_pid, runtime.fixture_pid]) {
      if (typeof pid !== 'number' || !Number.isSafeInteger(pid) || pid <= 1) continue;
      try {
        process.kill(pid, 'SIGTERM');
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error;
      }
    }
  } finally {
    await rm(dirname(runtimeFile), { recursive: true, force: true });
  }
}
