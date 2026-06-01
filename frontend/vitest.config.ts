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
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, '.'),
    },
  },
})
