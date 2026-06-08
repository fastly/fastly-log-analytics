import { defineConfig } from 'vitest/config'
import path from 'path'

export default defineConfig({
  test: {
    environment: 'jsdom',
    setupFiles: ['./vitest.setup.ts'],
    // Playwright specs live under e2e/ and are run by `npm run test:e2e`
    // — vitest must skip them or it errors on the @playwright/test import.
    exclude: ['**/node_modules/**', '**/.next/**', '**/e2e/**'],
    // jsdom + React 19 + MSW + many large component trees running in
    // parallel under vitest's worker pool can spike CPU enough that
    // the default 5s test timeout starts to fire on heavy tests
    // (admin/page render, LogSettingsModal wizard walk-throughs,
    // CustomFieldDrawer with fake timers + debounced lint). Each of
    // these passes in isolation in <2s; the 15s budget absorbs
    // worker contention without masking real hangs.
    testTimeout: 15000,
    // Cap parallelism. Default = cpu_count() workers, each loading a
    // jsdom + React + MSW workspace (~250 MB). On 8-10 core Macs that's
    // 2-3 GB just for vitest, which combined with `make ci -j2` running
    // pytest-xdist in parallel pushed 16 GB Macs into swap. 4 keeps wall
    // time competitive without saturating memory.
    //
    // Vitest 4 moved the cap from `poolOptions.threads.maxThreads` to
    // top-level `maxWorkers` on InlineConfig — the older poolOptions
    // shape no longer typechecks under tsc (which `next build` runs
    // over every .ts file, including this config).
    maxWorkers: 4,
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, '.'),
    },
  },
})
