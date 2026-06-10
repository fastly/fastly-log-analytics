/**
 * MSW migration (TESTING_PLAN_3 item 9). Previously
 * ``vi.mock('@/lib/api')`` stubbed ``client.GET`` at the module
 * boundary. Now the real openapi-fetch client runs and MSW intercepts
 * at the fetch boundary so the request-middleware (header injection)
 * and response-middleware (error throwing) get exercised too.
 */
import { render, screen, waitFor } from '@testing-library/react'
import { http, HttpResponse } from 'msw'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { expect, test, vi, beforeEach } from 'vitest'
import React from 'react'

import { useServiceStore } from '@/stores/serviceStore'
import { server } from '../../tests/msw/server'

vi.mock('next/navigation', () => ({
  usePathname: vi.fn(() => '/admin'),
  useRouter: vi.fn(() => ({ replace: vi.fn(), push: vi.fn() })),
}))


import { getApiBase } from '@/lib/api'

const API_BASE = getApiBase()
console.log('API_BASE IS', API_BASE)

vi.mock('@tanstack/react-virtual', () => ({
  useVirtualizer: (options: any) => ({
    getVirtualItems: () => {
      const count = options.count || 0
      return Array.from({ length: count }).map((_, i) => ({
        index: i,
        start: i * 40,
        size: 40,
      }))
    },
    getTotalSize: () => (options.count || 0) * 40,
  }),
}))

beforeEach(() => {
  vi.clearAllMocks()
  useServiceStore.setState({ activeServiceId: 'test-svc', isInitialized: true } as never)
})

test('renders admin page and lists services', async () => {
  server.use(
    http.get(`${API_BASE}/api/services`, () =>
      HttpResponse.json({
        services: [
          {
            service_id: 'test-svc',
            name: 'Test Service',
            fos_bucket: 'test-bucket',
            access_level: 'read_write',
          },
        ],
      }),
    ),
    http.get(`${API_BASE}/api/admin/bot-sources`, () =>
      HttpResponse.json({ sources: [], rdns: { total: 0, pending: 0 } }),
    ),
    http.get(`${API_BASE}/api/admin/system-jobs`, () =>
      HttpResponse.json({ jobs: [] }),
    ),
    http.get(`${API_BASE}/api/admin/usage-logging`, () =>
      HttpResponse.json({ enabled: false, retention_days: 30 }),
    ),
  )

  // Per-test QueryClient avoids cross-test cache leakage and lets us
  // disable retries cleanly.
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: 0 } },
  })
  const { default: AdminPage } = await import('@/app/admin/page')
  render(
    <QueryClientProvider client={queryClient}>
      <AdminPage />
    </QueryClientProvider>,
  )

  expect(screen.getByText('Admin')).toBeInTheDocument()

  await waitFor(
    () => {
      expect(screen.getByText('Test Service')).toBeInTheDocument()
      expect(screen.getByText('test-svc')).toBeInTheDocument()
      expect(screen.getByText('test-bucket')).toBeInTheDocument()
    },
    { timeout: 3000 },
  )
})
