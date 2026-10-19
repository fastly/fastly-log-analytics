import React from 'react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import QueryMonitorPage from '@/app/admin/queries/page'
import Loading from '@/app/admin/queries/loading'

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/admin/queries'))

const mockAdminJson = vi.fn()
const mockAdminJsonResult = vi.fn()

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  return {
    ...actual,
    adminJson: (...args: unknown[]) => mockAdminJson(...args),
    adminJsonResult: (...args: unknown[]) => mockAdminJsonResult(...args),
  }
})

describe('Admin Live Query Monitor (/admin/queries)', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders Loading skeleton with zero-CLS reservations and contextual copy', () => {
    render(<Loading />)

    const loadingRoot = screen.getByTestId('admin-queries-loading')
    expect(loadingRoot).toBeDefined()
    expect(screen.getByText('Live Query Monitor')).toBeDefined()
    expect(screen.getAllByText('Inspecting DuckDB connection pool...').length).toBeGreaterThan(0)
    expect(screen.getAllByText('Loading query telemetry...').length).toBeGreaterThan(0)

    const summarySkel = screen.getByTestId('admin-queries-summary-skeleton')
    expect(summarySkel.className).toContain('[contain-intrinsic-size:auto_36px]')
    expect(summarySkel.className).toContain('[content-visibility:auto]')
  })

  it('renders QueryMonitorPage with zero-CLS reservation containers and contextual in-place skeletons while pending', () => {
    // Keep promises pending to verify initial skeleton state
    mockAdminJson.mockImplementation((route: string) => {
      if (route === '/api/admin/app-config/query-monitor') {
        return Promise.resolve({ enabled: true })
      }
      return new Promise(() => {})
    })

    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <QueryMonitorPage />
      </QueryClientProvider>,
    )

    expect(screen.getByTestId('admin-queries-root')).toBeDefined()
    expect(screen.getByText('Live Query Monitor')).toBeDefined()

    const reservations = [
      'admin-queries-summary-reservation',
      'admin-queries-active-reservation',
      'admin-queries-slow-reservation',
      'admin-queries-completed-reservation',
    ]

    for (const id of reservations) {
      const el = screen.getByTestId(id)
      expect(el).toBeDefined()
      expect(el.className).toContain('[contain-intrinsic-size:auto_')
      expect(el.className).toContain('[content-visibility:auto]')
    }

    expect(screen.getByTestId('admin-queries-active-skeleton')).toBeDefined()
    expect(screen.getByTestId('admin-queries-slow-skeleton')).toBeDefined()
    expect(screen.getByText('Inspecting DuckDB connection pool...')).toBeDefined()
    expect(screen.getByText('Loading query telemetry...')).toBeDefined()
  })

  it('renders populated active, slow, and completed queries and supports refresh and pause controls', async () => {
    const nowSec = Math.floor(Date.now() / 1000)
    mockAdminJson.mockImplementation(async (route: string) => {
      if (route === '/api/admin/app-config/query-monitor') {
        return { enabled: true }
      }
      if (route === '/api/admin/queries') {
        return {
          last_seq: 12,
          active: [
            {
              query_id: 11,
              db_type: 'DuckDB',
              sql_preview: 'SELECT count(*) FROM logs WHERE status >= 500',
              sql: 'SELECT count(*) FROM logs WHERE status >= 500',
              sql_len: 46,
              service_id: 'svc_alpha',
              started_at_utc: nowSec - 2,
              duration_ms: 1450.0,
              cancellable: true,
              cancelled_at: null,
              attribution: {
                kind: 'admin',
                label: 'admin @ 127.0.0.1',
                principal_id: '127.0.0.1',
                caller_qualname: 'dashboard.aggregates',
                caller_file: 'backend/routers/dashboard.py:42',
                request_path: '/api/dashboard/aggregates',
                request_id: 'req-11',
                cron_job: null,
                cron_run_id: null,
                pool_slot: 'pool#0',
              },
            },
          ],
          completed: [
            {
              query_id: 12,
              db_type: 'Postgres',
              sql_preview: 'SELECT * FROM slow_queries WHERE service_id = $1',
              sql: 'SELECT * FROM slow_queries WHERE service_id = $1',
              sql_len: 49,
              service_id: 'svc_alpha',
              started_at_utc: nowSec - 20,
              ended_at_utc: nowSec - 19,
              duration_ms: 820.0,
              outcome: 'ok',
              error_type: null,
              error_message: null,
              peak_memory_mb: 18.2,
              attribution: {
                kind: 'cron',
                label: 'cron:full_sync',
                principal_id: null,
                caller_qualname: 'slow_queries.list_slow_queries',
                caller_file: 'backend/core/metadata/slow_queries.py:187',
                request_path: null,
                request_id: null,
                cron_job: 'full_sync',
                cron_run_id: 42,
                pool_slot: null,
              },
            },
          ],
        }
      }
      if (route === '/api/admin/slow-queries') {
        return {
          rows: [],
          since_hours: 24,
          threshold_ms: 500,
        }
      }
      return {}
    })

    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <QueryMonitorPage />
      </QueryClientProvider>,
    )

    // Wait for populated summary strip to reflect active DuckDB query
    const statusLive = await screen.findByText('1 active query')
    expect(statusLive).toBeDefined()
    expect(screen.getByText(/longest:\s*1\.45 s/)).toBeDefined()

    // Check section titles
    expect(screen.getByText(/Active & Just-Finished/)).toBeDefined()
    expect(screen.getByText('Notable Slow Queries')).toBeDefined()
    expect(screen.getByText('Recently Completed')).toBeDefined()

    // Test Pause -> Resume toggle
    const pauseBtn = screen.getByRole('button', { name: /Pause/i })
    fireEvent.click(pauseBtn)
    expect(await screen.findByRole('button', { name: /Resume/i })).toBeDefined()

    // Test manual Refresh button presence
    const refreshBtn = screen.getByTestId('admin-queries-refresh-btn')
    expect(refreshBtn).toBeDefined()
    fireEvent.click(refreshBtn)
  })
})
