import React from 'react'
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderWithQuery } from '@/__tests__/helpers/query'

const { mockGet, mockDelete, mockPush } = vi.hoisted(() => ({
  mockGet: vi.fn(),
  mockDelete: vi.fn(),
  mockPush: vi.fn(),
}))

vi.mock('@tanstack/react-virtual', async () => (await import('../../helpers/virtual-mock')).reactVirtualModuleMock())

vi.mock('next/navigation', () => ({
  useRouter: () => ({
    push: mockPush,
    replace: vi.fn(),
    prefetch: vi.fn(),
  }),
}))

vi.mock('@/components/charts/TimeSeriesChart', () => ({
  TimeSeriesChart: () => <div data-testid="mock-time-series-chart" className="h-[240px]" />,
}))

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  return {
    ...actual,
    client: {
      ...actual.client,
      GET: mockGet,
      DELETE: mockDelete,
    },
  }
})

import UsageLogClient from '@/app/admin/usage-log/_sections/UsageLogClient'
import Loading from '@/app/admin/usage-log/loading'

const SAMPLE_USAGE_LOG = {
  service_id: 'svc-alpha',
  total: 2,
  aggregate: {
    total_class_a: 1200,
    total_class_b: 45000,
    total_cdn_downloads: 18,
    total_cdn_bytes: 1073741824,
    total_fos_bytes: 5242880,
    estimated_cost_class_a: 0.006,
    estimated_cost_class_b: 0.045,
    estimated_cost_cdn: 0.12,
    estimated_cost_total: 0.171,
    class_a_breakdown: { PutObject: 800, ListObjectsV2: 400 },
    class_b_breakdown: { GetObject: 45000 },
  },
  entries: [
    {
      id: 1,
      timestamp: '2026-06-01T12:00:00Z',
      operation_class: 'A',
      operation_type: 'PutObject',
      url: 's3://fos-bucket/lake/part-001.parquet',
      bytes: 2048,
      duration_ms: 14.2,
      function_name: 'commit_files',
      process_context: 'cron:commit:svc-alpha',
      status: 'OK',
      estimated_cost: 0.000005,
      count: 1,
    },
    {
      id: 2,
      timestamp: '2026-06-01T12:01:00Z',
      operation_class: 'B',
      operation_type: 'GetObject',
      url: 's3://fos-bucket/raw/request/batch.gz',
      bytes: 4096,
      duration_ms: 9.1,
      function_name: 'download_batch',
      process_context: 'cron:sync:svc-alpha',
      status: 'OK',
      estimated_cost: 0.000001,
      count: 1,
    },
  ],
}

const SAMPLE_ACCOUNTING = {
  by: 'hour',
  from_ts: '2026-05-31T12:00:00Z',
  to_ts: '2026-06-01T12:00:00Z',
  buckets: [
    {
      ts: '2026-06-01T11:00:00Z',
      fastly_requests: 5000,
      our_rows: 5000,
      file_count: 12,
      gap: 0,
      gap_pct: 0,
    },
  ],
  totals: {
    fastly_requests: 5000,
    our_rows: 5000,
    gap: 0,
    gap_pct: 0,
    worst_bucket_ts: null,
    worst_bucket_gap_pct: null,
  },
  sustained_loss: null,
  catchup: {
    latest_ingest_ts: '2026-06-01T12:00:00Z',
    lag_seconds: 12,
    status: 'caught_up',
  },
}

describe('/admin/usage-log Page & Client', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders loading.tsx with geometry-matched zero-CLS reservation containers', () => {
    render(<Loading />)

    expect(screen.getByTestId('admin-usage-log-loading')).toBeInTheDocument()
    const toolbar = screen.getByTestId('admin-usage-log-toolbar')
    expect(toolbar.className).toContain('[contain-intrinsic-size:auto_36px]')
    expect(toolbar.className).toContain('[content-visibility:auto]')

    const kpi = screen.getByTestId('admin-usage-log-kpi-reservation')
    expect(kpi.className).toContain('[contain-intrinsic-size:auto_164px]')
    expect(kpi.className).toContain('[content-visibility:auto]')

    const chart = screen.getByTestId('admin-usage-log-chart-reservation')
    expect(chart.className).toContain('[contain-intrinsic-size:auto_380px]')
    expect(chart.className).toContain('[content-visibility:auto]')

    const table = screen.getByTestId('admin-usage-log-table-reservation')
    expect(table.className).toContain('[contain-intrinsic-size:auto_520px]')
    expect(table.className).toContain('[content-visibility:auto]')
    expect(screen.getByText('Loading FOS usage ledger...')).toBeInTheDocument()
  })

  it('renders pending skeleton reservations before usage-log data resolves', () => {
    mockGet.mockImplementation(() => new Promise(() => {}))

    renderWithQuery(<UsageLogClient initialServiceId="svc-alpha" initialNowMs={1780315200000} />)

    expect(screen.getByTestId('admin-usage-log-root')).toBeInTheDocument()
    expect(screen.getByTestId('admin-usage-log-kpi-reservation').className).toContain(
      '[contain-intrinsic-size:auto_164px]',
    )
    expect(screen.getByTestId('admin-usage-log-chart-reservation').className).toContain(
      '[contain-intrinsic-size:auto_380px]',
    )
    expect(screen.getByTestId('admin-usage-log-table-reservation').className).toContain(
      '[contain-intrinsic-size:auto_520px]',
    )
    expect(screen.getByText('Loading log accounting…')).toBeInTheDocument()
  })

  it('renders aggregate KPIs, ingest accounting, and usage log rows', async () => {
    mockGet.mockImplementation((url: string) => {
      if (url === '/api/admin/usage-log') {
        return Promise.resolve({
          data: SAMPLE_USAGE_LOG,
          response: new Response(null, { status: 200 }),
        })
      }
      if (url === '/api/admin/log-accounting') {
        return Promise.resolve({
          data: SAMPLE_ACCOUNTING,
          response: new Response(null, { status: 200 }),
        })
      }
      return Promise.resolve({ data: {}, response: new Response(null, { status: 200 }) })
    })

    renderWithQuery(<UsageLogClient initialServiceId="svc-alpha" initialNowMs={1780315200000} />)

    await waitFor(() => {
      expect(screen.getByText('1.2K')).toBeInTheDocument()
    })

    expect(screen.getByText('45.0K')).toBeInTheDocument()
    expect(screen.getByText('$0.17')).toBeInTheDocument()
    expect(screen.getByText('s3://fos-bucket/lake/part-001.parquet')).toBeInTheDocument()
    expect(screen.getByText('cron:commit:svc-alpha')).toBeInTheDocument()
    expect(screen.getByText('Caught up')).toBeInTheDocument()
  })

  it('switches time-window presets, filters by process/operation, refreshes, and purges logs', async () => {
    mockGet.mockImplementation((url: string) => {
      if (url === '/api/admin/usage-log') {
        return Promise.resolve({
          data: SAMPLE_USAGE_LOG,
          response: new Response(null, { status: 200 }),
        })
      }
      return Promise.resolve({
        data: SAMPLE_ACCOUNTING,
        response: new Response(null, { status: 200 }),
      })
    })
    mockDelete.mockResolvedValue({
      data: { ok: true },
      response: new Response(null, { status: 200 }),
    })

    renderWithQuery(<UsageLogClient initialServiceId="svc-alpha" initialNowMs={1780315200000} />)

    await waitFor(() => {
      expect(screen.getByText('s3://fos-bucket/lake/part-001.parquet')).toBeInTheDocument()
    })

    // Preset toggle (24h -> 1h -> 168h)
    expect(screen.getByTestId('admin-usage-log-preset-24h')).toHaveAttribute('aria-pressed', 'true')
    fireEvent.click(screen.getByTestId('admin-usage-log-preset-1h'))
    expect(screen.getByTestId('admin-usage-log-preset-1h')).toHaveAttribute('aria-pressed', 'true')

    // Filter by process_context
    const procInput = screen.getByTestId('admin-usage-log-process-input')
    fireEvent.change(procInput, { target: { value: 'cron:commit' } })

    await waitFor(() => {
      expect(mockGet).toHaveBeenCalledWith(
        '/api/admin/usage-log',
        expect.objectContaining({
          params: {
            query: expect.objectContaining({
              process_context: 'cron:commit',
            }),
          },
        }),
      )
    })

    // Filter by operation_type
    const opInput = screen.getByTestId('admin-usage-log-operation-input')
    fireEvent.change(opInput, { target: { value: 'PutObject' } })

    await waitFor(() => {
      expect(mockGet).toHaveBeenCalledWith(
        '/api/admin/usage-log',
        expect.objectContaining({
          params: {
            query: expect.objectContaining({
              operation_type: 'PutObject',
            }),
          },
        }),
      )
    })

    // Manual Refresh
    const callsBeforeRefresh = mockGet.mock.calls.length
    const refreshBtn = screen.getByTestId('admin-usage-log-refresh-btn')
    fireEvent.click(refreshBtn)
    await waitFor(() => {
      expect(mockGet.mock.calls.length).toBeGreaterThan(callsBeforeRefresh)
    })

    // Export CSV href is populated after mount
    const exportBtn = screen.getByTestId('admin-usage-log-export-btn')
    expect(exportBtn.getAttribute('href')).toContain('/api/admin/usage-log/export?')
    expect(exportBtn.getAttribute('href')).toContain('process_context=cron%3Acommit')

    // Purge Logs dialog workflow
    fireEvent.click(screen.getByTestId('admin-usage-log-purge-btn'))
    await waitFor(() => {
      expect(screen.getByText('Purge all usage logs?')).toBeInTheDocument()
    })
    fireEvent.click(screen.getByRole('button', { name: /Purge all logs/i }))

    await waitFor(() => {
      expect(mockDelete).toHaveBeenCalledWith(
        '/api/admin/usage-log',
        expect.objectContaining({
          params: { query: { service_id: 'svc-alpha' } },
        }),
      )
    })
  })
})
