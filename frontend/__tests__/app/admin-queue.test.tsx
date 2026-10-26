import React from 'react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import { renderWithQuery } from '../helpers/page-smoke'
import AdminQueuePage from '@/app/admin/queue/page'
import Loading from '@/app/admin/queue/loading'

const mockGet = vi.fn()
const mockPost = vi.fn()

vi.mock('@/lib/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/api')>()),
  client: {
    GET: (...args: unknown[]) => mockGet(...args),
    POST: (...args: unknown[]) => mockPost(...args),
  },
}))

vi.mock('@/hooks/useSSE', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useSSE')>()),
  useAdminEventStream: () => ({ state: 'open' as const }),
}))

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/page-smoke')).reactVirtualModuleMock())

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/admin/queue'))

describe('AdminQueuePage (/admin/queue)', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders route-level loading skeleton with zero-CLS reservation containers and contextual messages', () => {
    render(<Loading />)
    expect(screen.getByTestId('admin-queue-loading')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-toolbar-skeleton')).toBeInTheDocument()
    expect(screen.getAllByText(/Inspecting Celery task broker/i).length).toBeGreaterThan(0)
    expect(screen.getAllByText(/Loading queue telemetry/i).length).toBeGreaterThan(0)
  })

  it('renders pre-allocated zero-CLS reservation containers and contextual in-place skeletons while queries are pending', () => {
    mockGet.mockImplementation(() => new Promise(() => {}))

    renderWithQuery(<AdminQueuePage />)
    expect(screen.getByTestId('admin-queue-root')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-trends-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-depths-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-workers-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-ledger-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-schedules-reservation')).toBeInTheDocument()
    expect(screen.getAllByText(/Inspecting Celery task broker/i).length).toBeGreaterThan(0)
    expect(screen.getAllByText(/Loading queue telemetry/i).length).toBeGreaterThan(0)
    expect(screen.getByTestId('admin-queue-refresh-btn')).toBeDisabled()
  })

  it('renders High-Scale queue depths, active workers, ingest ledger cards, in-process schedules, and supports manual Refresh', async () => {
    let statusCalls = 0
    let batchCalls = 0

    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/admin/celery/status') {
        statusCalls += 1
        return {
          data: {
            broker_reachable: true,
            workers: {
              count: 1,
              active_tasks: {
                'celery@worker-1': [
                  {
                    id: 'task-uuid-101',
                    name: 'backend.tasks.ingest.ingest_batch',
                    args: ['svc-alpha', 25],
                    time_start: 1700000000,
                  },
                ],
              },
              stats: { 'celery@worker-1': {} },
              registered: { 'celery@worker-1': ['backend.tasks.ingest.ingest_batch'] },
              scheduled: { 'celery@worker-1': [] },
            },
            queues: {
              celery: 0,
              'q.control': 2,
              'q.ingest': 9,
            },
            inprocess_schedules: [
              {
                name: 'log_discovery_svc-alpha',
                trigger: 'interval[0:00:10]',
                next_run_at: 1700000010,
              },
            ],
            ledger: {
              discovered: 5,
              claimed: 2,
              committed: 140,
              quarantined: 1,
              dead_letter: 0,
            },
          },
          response: { ok: true, status: 200 },
        }
      }
      if (path === '/api/admin/metric-history/batch') {
        batchCalls += 1
        return {
          data: {
            series: {
              celery_queue_depth: [
                { ts: 1700000000, value: 4 },
                { ts: 1700000060, value: 11 },
              ],
              celery_active_tasks: [
                { ts: 1700000000, value: 1 },
                { ts: 1700000060, value: 1 },
              ],
            },
          },
          response: { ok: true, status: 200 },
        }
      }
      if (path === '/api/admin/celery/ledger') {
        return {
          data: {
            items: [
              {
                service_id: 'svc-alpha',
                object_key: 'raw/rum/year=2026/month=10/day=20/hour=04/minute=28/2026-10-20T04_28_00.000-test.log.gz',
                source_type: 'rum',
                status: 'quarantined',
                attempts: 1,
                error_kind: 'corrupt_gzip',
                last_error: 'Not a gzipped file',
                originating_task: 'rum_discovery',
                originating_run_id: 42,
                discovered_at: 1700000000,
              },
            ],
            total: 1,
          },
          response: { ok: true, status: 200 },
        }
      }
      return { data: {}, response: { ok: true, status: 200 } }
    })

    renderWithQuery(<AdminQueuePage />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-queue-broker-badge')).toHaveTextContent('Broker Reachable')
    })

    expect(screen.getByTestId('admin-queue-depths-table')).toBeInTheDocument()
    expect(screen.getByText('q.ingest')).toBeInTheDocument()
    expect(screen.getByText('q.control')).toBeInTheDocument()

    expect(screen.getByTestId('admin-queue-workers-list')).toBeInTheDocument()
    expect(screen.getByText('celery@worker-1')).toBeInTheDocument()
    expect(screen.getByText('task-uuid-101')).toBeInTheDocument()

    expect(screen.getByTestId('admin-queue-ledger-cards')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-ledger-discovered')).toHaveTextContent('5')
    expect(screen.getByTestId('admin-queue-ledger-committed')).toHaveTextContent('140')

    expect(screen.getByTestId('admin-queue-inprocess-schedules-table')).toBeInTheDocument()
    expect(screen.getByText('log_discovery_svc-alpha')).toBeInTheDocument()

    const refreshBtn = screen.getByTestId('admin-queue-refresh-btn')
    expect(refreshBtn).not.toBeDisabled()
    fireEvent.click(refreshBtn)

    await waitFor(() => {
      expect(statusCalls).toBeGreaterThanOrEqual(2)
      expect(batchCalls).toBeGreaterThanOrEqual(2)
    })
  })

  it('renders Standard mode fallback cleanly when broker is unreachable or Celery queues are empty', async () => {
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/admin/celery/status') {
        return {
          data: {
            broker_reachable: false,
            workers: {
              count: 0,
              active_tasks: {},
              stats: {},
              registered: {},
              scheduled: {},
              error: 'Valkey unreachable',
            },
            queues: {},
            inprocess_schedules: [],
            ledger: {},
          },
          response: { ok: true, status: 200 },
        }
      }
      if (path === '/api/admin/metric-history/batch') {
        return {
          data: { series: {} },
          response: { ok: true, status: 200 },
        }
      }
      return { data: {}, response: { ok: true, status: 200 } }
    })

    renderWithQuery(<AdminQueuePage />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-queue-broker-badge')).toHaveTextContent(
        'Broker Unreachable / Standard Mode',
      )
    })

    expect(screen.getByText(/No queues found or Celery mode is inactive/i)).toBeInTheDocument()
    expect(screen.getByText(/No active workers found/i)).toBeInTheDocument()
    expect(screen.getByText(/Ledger is empty/i)).toBeInTheDocument()
    expect(screen.getByText(/No in-process schedules registered/i)).toBeInTheDocument()
  })

  it('renders Standard Mode in-process scheduler & ingest telemetry and hides Celery sections when deployment_mode is standard', async () => {
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/admin/celery/status') {
        return {
          data: {
            broker_reachable: false,
            deployment_mode: 'standard',
            workers: {
              count: 0,
              active_tasks: {},
              stats: {},
              registered: {},
              scheduled: {},
              error: 'Celery broker not configured in standard mode',
            },
            queues: {},
            inprocess_schedules: [
              {
                name: 'log_discovery_svc-alpha',
                trigger: 'interval[0:00:10]',
                next_run_at: 1700000010,
              },
              {
                name: 'commit_svc-alpha',
                trigger: 'interval[0:05:00]',
                next_run_at: 1700000300,
              },
            ],
            inprocess_active_runs: [
              {
                run_id: 101,
                service_id: 'svc-alpha',
                task: 'sync',
                started_at: 1700000000,
              },
            ],
            standard_ingest: {
              ingested_files: 1250,
              quarantined_items: 2,
            },
            ledger: {
              committed: 1250,
              quarantined: 2,
              dead_letter: 0,
            },
          },
          response: { ok: true, status: 200 },
        }
      }
      if (path === '/api/admin/metric-history/batch') {
        return { data: { series: {} }, response: { ok: true, status: 200 } }
      }
      return { data: {}, response: { ok: true, status: 200 } }
    })

    renderWithQuery(<AdminQueuePage />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-queue-broker-badge')).toHaveTextContent(
        'Standard Mode · In-Process Scheduler',
      )
    })

    // High-scale Celery queue sections and false-alarm broker warning should be hidden
    expect(screen.queryByText(/^Queue Depths$/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/Total Queue Depth \(1h\)/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/Inspect warning:/i)).not.toBeInTheDocument()

    // Standard mode cards and tables should be rendered
    expect(screen.getByTestId('admin-queue-depths-reservation')).toBeInTheDocument()
    expect(screen.getByText(/Quarantined Files & Corrupt Lines \(2\)/i)).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-standard-ingest-summary')).toBeInTheDocument()
    expect(screen.getByText('1,250')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-standard-scheduler-summary')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-inprocess-active-table')).toBeInTheDocument()
    expect(screen.getByTestId('admin-queue-inprocess-schedules-table')).toBeInTheDocument()
    expect(screen.getByText('log_discovery_svc-alpha')).toBeInTheDocument()
    expect(screen.getByText('commit_svc-alpha')).toBeInTheDocument()
  })

  it('displays dead-letter entries in the ledger inspector and handles Retry and Dismiss actions', async () => {
    mockGet.mockImplementation(async (path: string) => {
      if (path === '/api/admin/celery/status') {
        return {
          data: {
            broker_reachable: true,
            workers: { count: 1, active_tasks: {}, stats: {}, registered: {}, scheduled: {} },
            queues: { celery: 0, 'q.ingest': 0, 'q.control': 0 },
            inprocess_schedules: [],
            ledger: {
              discovered: 0,
              claimed: 0,
              committed: 120,
              quarantined: 0,
              dead_letter: 3,
            },
          },
          response: { ok: true, status: 200 },
        }
      }
      if (path === '/api/admin/celery/ledger') {
        return {
          data: {
            items: [
              {
                service_id: 'svc-alpha',
                object_key: 'raw/rum/year=2026/month=10/day=20/hour=04/minute=28/sample-dead.log.gz',
                source_type: 'rum',
                status: 'dead_letter',
                attempts: 1,
                error_kind: 'object_missing',
                last_error: 'object missing from FOS (already ingested+deleted, or expired)',
                originating_task: 'rum_discovery',
                originating_run_id: 24368,
                claimed_at: 1729400000,
              },
            ],
            total: 1,
          },
          response: { ok: true, status: 200 },
        }
      }
      if (path === '/api/admin/metric-history/batch') {
        return { data: { series: {} }, response: { ok: true, status: 200 } }
      }
      return { data: {}, response: { ok: true, status: 200 } }
    })

    mockPost.mockImplementation(async () => ({
      data: { ok: true, affected: 1, dispatched: 1 },
      response: { ok: true, status: 200 },
    }))

    renderWithQuery(<AdminQueuePage />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-queue-ledger-entries')).toBeInTheDocument()
    })

    expect(
      screen.getByText('raw/rum/year=2026/month=10/day=20/hour=04/minute=28/sample-dead.log.gz'),
    ).toBeInTheDocument()
    expect(screen.getByText('object_missing')).toBeInTheDocument()
    expect(
      screen.getByText('object missing from FOS (already ingested+deleted, or expired)'),
    ).toBeInTheDocument()

    fireEvent.click(screen.getByTestId('admin-queue-ledger-retry-row'))
    await waitFor(() => {
      expect(mockPost).toHaveBeenCalledWith(
        '/api/admin/celery/ledger/retry',
        expect.objectContaining({
          body: {
            items: [
              {
                service_id: 'svc-alpha',
                object_key: 'raw/rum/year=2026/month=10/day=20/hour=04/minute=28/sample-dead.log.gz',
              },
            ],
          },
        }),
      )
    })

    fireEvent.click(screen.getByTestId('admin-queue-ledger-purge-all'))
    await waitFor(() => {
      expect(mockPost).toHaveBeenCalledWith(
        '/api/admin/celery/ledger/purge',
        expect.objectContaining({
          body: { status: 'problem' },
        }),
      )
    })
  })
})
