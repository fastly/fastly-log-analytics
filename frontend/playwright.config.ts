/**
 * R-3a (testing_suite_audit_2026-06-14.md): Playwright config.
 *
 * Backend is spawned by `e2e/global-setup.ts` on 127.0.0.1:18004 with
 * FASTLY_MOCK_MODE=1 so every Fastly/NGWAF API call is short-circuited
 * to the canned fixtures in backend/core/fastly/mock_fixtures.py.
 *
 * Frontend is started by this config via `webServer` on port 13004
 * with NEXT_PUBLIC_BACKEND_PORT=18004 so its API base resolves to the
 * mocked backend. `reuseExistingServer: true` lets a long-running
 * `npx playwright test --ui` session keep the same server alive.
 *
 * Local: workers: 1 (per the resource-limits memory).
 * CI: workers: 1 + retries: 1.
 */
import { defineConfig, devices } from '@playwright/test'
import { join } from 'node:path'

const BACKEND_PORT = 18004
const FRONTEND_PORT = 13004

export const E2E_BACKEND_PORT = BACKEND_PORT
export const E2E_FRONTEND_PORT = FRONTEND_PORT

const isCI = !!process.env.CI

export default defineConfig({
  testDir: './e2e',
  timeout: 90_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  forbidOnly: isCI,
  // Retries are opt-in so CI surfaces backend failures instead of masking them
  // or doubling suite time. The E2E workflow sets workers=1 because every
  // browser worker shares one backend and DuckLake serializes catalog attaches.
  retries: process.env.PLAYWRIGHT_RETRIES ? parseInt(process.env.PLAYWRIGHT_RETRIES, 10) : 0,
  workers: process.env.PLAYWRIGHT_WORKERS ? parseInt(process.env.PLAYWRIGHT_WORKERS, 10) : 2,
  reporter: isCI ? [['list'], ['html', { open: 'never' }]] : 'list',
  use: {
    baseURL: `http://127.0.0.1:${FRONTEND_PORT}`,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },
  // Spawn the frontend pointed at the mocked backend.
  // NEXT_DIST_DIR points next dev at a separate build tree so its
  // lockfile doesn't collide with the dev shell running out of `.next/`
  // on port 13002 (next dev refuses to start a second instance against
  // the same lock dir).
  webServer: {
    command: `npm run build && npx next start -H 127.0.0.1 -p ${FRONTEND_PORT}`,
    env: {
      NEXT_DIST_DIR: '.next-e2e',
      // NEXT_PUBLIC_API_URL pins the openapi-fetch base to the
      // frontend's OWN origin so client-side API calls go through
      // Next.js's `/api/*` rewrite proxy → backend instead of the
      // admin-SSH-tunnel branch in `lib/api.getApiBase()` (which
      // would hit the backend directly on 18004 and trip CORS
      // preflight from the 13004 origin — every page that depends
      // on /api/services then renders empty).
      NEXT_PUBLIC_API_URL: `http://127.0.0.1:${FRONTEND_PORT}`,
      // The Next.js rewrite proxy for /api/* (next.config.ts) reads
      // API_PROXY_URL; redirect it at the mocked backend too so
      // SSR / route-handler calls go through the mock layer.
      API_PROXY_URL: `http://127.0.0.1:${BACKEND_PORT}`,
    },
    url: `http://127.0.0.1:${FRONTEND_PORT}`,
    reuseExistingServer: !isCI,
    timeout: 180_000,
    cwd: join(__dirname),
  },
  globalSetup: require.resolve('./e2e/global-setup'),
  globalTeardown: require.resolve('./e2e/global-teardown'),

  projects:
    process.env.PROJECTS === 'all'
      ? [
          { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
          { name: 'firefox', use: { ...devices['Desktop Firefox'] } },
          { name: 'webkit', use: { ...devices['Desktop Safari'] } },
        ]
      : process.env.PROJECTS
      ? [
          { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
          { name: 'firefox', use: { ...devices['Desktop Firefox'] } },
          { name: 'webkit', use: { ...devices['Desktop Safari'] } },
        ].filter((p) => process.env.PROJECTS!.split(',').includes(p.name))
      : [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
})
