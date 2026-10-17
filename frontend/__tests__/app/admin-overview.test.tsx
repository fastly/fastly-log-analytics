import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import { expect, test, vi, beforeEach } from 'vitest'
import React from 'react'

import AdminPage from '@/app/admin/page'
import Loading from '@/app/admin/loading'
import { OperationsOverview } from '@/app/admin/_sections/OperationsOverview'
import { ServicesTable } from '@/app/admin/_sections/ServicesTable'
import { useServiceStore } from '@/stores/serviceStore'
import { getApiBase } from '@/lib/api'
import { server } from '../../tests/msw/server'

const API_BASE = getApiBase()

vi.mock('next/navigation', () => ({
  usePathname: vi.fn(() => '/admin'),
  useRouter: vi.fn(() => ({ replace: vi.fn(), push: vi.fn() })),
}))

vi.mock('@tanstack/react-virtual', () => ({
  useVirtualizer: (options: { count?: number }) => ({
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

test('Loading skeleton renders zero-CLS reservation containers and contextual messages', () => {
  render(<Loading />)
  expect(screen.getByTestId('admin-overview-loading')).toBeInTheDocument()
  expect(screen.getByText('Inspecting background scheduler state...')).toBeInTheDocument()
  expect(screen.getByText('Loading service configs...')).toBeInTheDocument()
})

test('AdminPage wraps all child sections in zero-CLS contain-intrinsic-size reservations', () => {
  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
  const { container } = render(
    <QueryClientProvider client={queryClient}>
      <AdminPage />
    </QueryClientProvider>,
  )
  expect(screen.getByTestId('admin-overview-root')).toBeInTheDocument()
  expect(screen.getByTestId('admin-operations-reservation')).toBeInTheDocument()
  expect(screen.getByTestId('admin-services-reservation')).toBeInTheDocument()
  expect(screen.getByTestId('admin-health-reservation')).toBeInTheDocument()
  expect(screen.getByTestId('admin-quarantine-reservation')).toBeInTheDocument()
  expect(screen.getByTestId('admin-global-settings-reservation')).toBeInTheDocument()
  expect(screen.getByTestId('admin-pricing-reservation')).toBeInTheDocument()

  const reserved = container.querySelectorAll('[class*="contain-intrinsic-size"]')
  expect(reserved.length).toBeGreaterThanOrEqual(6)
})

test('OperationsOverview renders live query, ingest gap, slow query, and worker status cards', async () => {
  server.use(
    http.get(`${API_BASE}/api/admin/queries/summary`, () =>
      HttpResponse.json({
        active_total: 2,
        by_db_type: { duckdb: 2 },
        recent_count: 15,
      }),
    ),
    http.get(`${API_BASE}/api/admin/log-accounting`, () =>
      HttpResponse.json({
        configured: true,
        service_id: 'test-svc',
        hours: 24,
        buckets: [],
        totals: {
          requests: 1000,
          log_lines: 1000,
          log_bytes: 50000,
          ingested_rows: 1000,
          gap: 0,
          gap_pct: 0,
        },
        sustained_loss: null,
      }),
    ),
    http.get(`${API_BASE}/api/admin/slow-queries/count`, () =>
      HttpResponse.json({ count: 0, since_hours: 24, threshold_ms: 1000 }),
    ),
    http.get(`${API_BASE}/api/admin/celery/status`, () =>
      HttpResponse.json({
        workers: { count: 4 },
        queues: { 'q.ingest': 0, 'q.control': 0 },
        ledger: { dead_letter: 0, quarantined: 0 },
      }),
    ),
  )

  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
  render(
    <QueryClientProvider client={queryClient}>
      <OperationsOverview />
    </QueryClientProvider>,
  )

  expect(screen.getByText('Live Queries')).toBeInTheDocument()
  expect(screen.getByText('Ingest Gap')).toBeInTheDocument()
  expect(screen.getByText('Notable Slow Queries')).toBeInTheDocument()
  expect(screen.getByText('Workers & Queues')).toBeInTheDocument()

  await waitFor(() => {
    expect(screen.getByText('0%')).toBeInTheDocument()
    expect(screen.getByText('healthy · 24h')).toBeInTheDocument()
    expect(screen.getByText('4 workers')).toBeInTheDocument()
  })
})

test('ServicesTable renders loading status, services list, and manual Refresh Status trigger', async () => {
  let fetchCount = 0
  server.use(
    http.get(`${API_BASE}/api/services`, () => {
      fetchCount += 1
      return HttpResponse.json({
        services: [
          {
            service_id: 'test-svc',
            name: 'Production Edge CDN',
            fos_bucket: 'prod-logs-bucket',
            access_level: 'read_write',
          },
        ],
      })
    }),
  )

  const user = userEvent.setup()
  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
  render(
    <QueryClientProvider client={queryClient}>
      <ServicesTable />
    </QueryClientProvider>,
  )

  await waitFor(() => {
    expect(screen.getByText('Production Edge CDN')).toBeInTheDocument()
    expect(screen.getByText('prod-logs-bucket')).toBeInTheDocument()
  })

  const refreshBtn = screen.getByTestId('admin-overview-refresh-btn')
  expect(refreshBtn).toBeInTheDocument()
  await user.click(refreshBtn)

  await waitFor(() => {
    expect(fetchCount).toBeGreaterThanOrEqual(2)
  })
})
