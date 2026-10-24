import React from 'react'
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderWithQuery } from '@/__tests__/helpers/query'

const { mockGet } = vi.hoisted(() => ({
  mockGet: vi.fn(),
}))

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  return {
    ...actual,
    client: {
      ...actual.client,
      GET: mockGet,
    },
  }
})

import AdminTrendsClient from '@/app/admin/trends/_sections/AdminTrendsClient'
import Loading from '@/app/admin/trends/loading'

const SAMPLE_SERIES = {
  cpu_load_1m: [
    { ts: '2026-04-19T12:00:00Z', value: 0.45 },
    { ts: '2026-04-19T12:01:00Z', value: 0.82 },
  ],
  mem_used_pct: [
    { ts: '2026-04-19T12:00:00Z', value: 52.0 },
    { ts: '2026-04-19T12:01:00Z', value: 54.3 },
  ],
  disk_used_pct: [{ ts: '2026-04-19T12:01:00Z', value: 31.2 }],
  disk_used_pct_root: [{ ts: '2026-04-19T12:01:00Z', value: 44.8 }],
  active_query_count: [{ ts: '2026-04-19T12:01:00Z', value: 2.0 }],
  'pool_wait_p95_ms|svc-a': [{ ts: '2026-04-19T12:01:00Z', value: 4.2 }],
  'pool_wait_p95_ms|svc-b': [{ ts: '2026-04-19T12:01:00Z', value: 11.5 }],
  'ingest_lag_s|svc-a': [{ ts: '2026-04-19T12:01:00Z', value: 18.0 }],
  'ingest_lag_s|svc-b': [{ ts: '2026-04-19T12:01:00Z', value: 7.0 }],
}

describe('/admin/trends Page & Client', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders loading.tsx with geometry-matched zero-CLS reservation containers', () => {
    render(<Loading />)

    expect(screen.getByTestId('admin-trends-loading')).toBeInTheDocument()
    const toolbar = screen.getByTestId('admin-trends-toolbar')
    expect(toolbar.className).toContain('[contain-intrinsic-size:auto_36px]')
    expect(toolbar.className).toContain('[content-visibility:auto]')

    const grid = screen.getByTestId('admin-trends-grid-reservation')
    expect(grid.className).toContain('[contain-intrinsic-size:auto_680px]')
    expect(grid.className).toContain('[content-visibility:auto]')
    expect(screen.getAllByText('Loading system vital trends...').length).toBeGreaterThanOrEqual(1)
  })

  it('renders pending skeleton reservations before batch data resolves', () => {
    mockGet.mockImplementation(() => new Promise(() => {}))

    renderWithQuery(<AdminTrendsClient />)

    expect(screen.getByTestId('admin-trends-root')).toBeInTheDocument()
    const grid = screen.getByTestId('admin-trends-grid-reservation')
    expect(grid.className).toContain('[contain-intrinsic-size:auto_680px]')
    expect(grid.className).toContain('[content-visibility:auto]')
    expect(screen.getAllByText('Loading system vital trends...').length).toBeGreaterThanOrEqual(1)
  })

  it('renders all 7 vital series cards and computes max-per-ts across services', async () => {
    mockGet.mockResolvedValue({
      data: { series: SAMPLE_SERIES },
      response: new Response(null, { status: 200 }),
    })

    renderWithQuery(<AdminTrendsClient />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-trends-latest-cpu_load_1m')).toHaveTextContent('0.82')
    })

    expect(screen.getByTestId('admin-trends-latest-mem_used_pct')).toHaveTextContent('54.3%')
    expect(screen.getByTestId('admin-trends-latest-disk_used_pct')).toHaveTextContent('31.2%')
    expect(screen.getByTestId('admin-trends-latest-disk_used_pct_root')).toHaveTextContent('44.8%')
    expect(screen.getByTestId('admin-trends-latest-active_query_count')).toHaveTextContent('2.00')
    // Max across svc-a (4.2ms) and svc-b (11.5ms) -> 11.5ms
    expect(screen.getByTestId('admin-trends-latest-pool_wait_p95_ms')).toHaveTextContent('11.5ms')
    // Max across svc-a (18s) and svc-b (7s) -> 18s
    expect(screen.getByTestId('admin-trends-latest-ingest_lag_s')).toHaveTextContent('18s')
  })

  it('switches lookback windows (1h -> 6h -> 24h -> 7d) and triggers manual Refresh', async () => {
    mockGet.mockResolvedValue({
      data: { series: SAMPLE_SERIES },
      response: new Response(null, { status: 200 }),
    })

    renderWithQuery(<AdminTrendsClient />)

    await waitFor(() => {
      expect(mockGet).toHaveBeenCalledWith(
        '/api/admin/metric-history/batch',
        expect.objectContaining({
          params: { query: { since: '1h' } },
        }),
      )
    })

    for (const win of ['6h', '24h', '7d'] as const) {
      const btn = screen.getByTestId(`admin-trends-window-${win}`)
      fireEvent.click(btn)

      await waitFor(() => {
        expect(mockGet).toHaveBeenCalledWith(
          '/api/admin/metric-history/batch',
          expect.objectContaining({
            params: { query: { since: win } },
          }),
        )
      })
      expect(btn).toHaveAttribute('aria-pressed', 'true')
    }

    const callsBeforeRefresh = mockGet.mock.calls.length
    const refreshBtn = screen.getByTestId('admin-trends-refresh-btn')
    fireEvent.click(refreshBtn)

    await waitFor(() => {
      expect(mockGet.mock.calls.length).toBeGreaterThan(callsBeforeRefresh)
    })
  })
})
