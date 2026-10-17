import React from 'react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import AdminQueuePage from '@/app/admin/queue/page'
import Loading from '@/app/admin/queue/loading'

const mockGet = vi.fn()

vi.mock('@/lib/api', () => ({
  client: {
    GET: (...args: unknown[]) => mockGet(...args),
  },
}))

vi.mock('@/hooks/useAdminEventStream', () => ({
  useAdminEventStream: () => ({ state: 'open' as const }),
}))

vi.mock('next/navigation', () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => '/admin/queue',
  useSearchParams: () => new URLSearchParams(),
}))

function renderWithQuery(ui: React.ReactElement) {
  const qc = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  })
  return render(<QueryClientProvider client={qc}>{ui}</QueryClientProvider>)
}

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

  it('renders High-Scale queue depths, active workers, ingest ledger cards, RedBeat schedules, and supports manual Refresh', async () => {
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
            schedules: [
              {
                name: 'discover-svc-alpha',
                task: 'backend.tasks.ingest.discover_service',
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

    expect(screen.getByTestId('admin-queue-schedules-table')).toBeInTheDocument()
    expect(screen.getByText('discover-svc-alpha')).toBeInTheDocument()

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
            schedules: [],
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
    expect(screen.getByText(/No schedules found/i)).toBeInTheDocument()
  })
})
