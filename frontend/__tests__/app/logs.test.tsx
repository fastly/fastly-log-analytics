import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, test, vi, beforeEach, afterEach } from 'vitest'
import LogsClient from '@/app/logs/_sections/LogsClient'
import { QuickActionsBar } from '@/app/logs/_sections/QuickActionsBar'
import { CRON_EXPLANATIONS, CRON_DISPLAY_NAMES, CRON_GROUPS } from '@/app/logs/_sections/CronExplanations'
import { renderWithQuery } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'

const { showToastMock, postMock } = vi.hoisted(() => ({
  showToastMock: vi.fn(),
  postMock: vi.fn(),
}))

vi.mock('@/lib/toast', () => ({ showToast: showToastMock }))
vi.mock('@/lib/cron-cache-bust', () => ({ cronCacheBust: vi.fn() }))

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())
vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/logs'))

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: postMock, use: vi.fn() },
  extractApiError: (e: unknown) => (e instanceof Error ? e.message : String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/app/logs/_state', () => ({
  useLogsPageState: vi.fn(() => ({
    activeServiceId: 'test-svc',
    activeTab: 'cron',
    handleTabChange: vi.fn(),
    isAnalyst: false,
    status: { idle: true, access_level: 'read_write' },
    catalogMaps: {},
    recentCrons: {},
    cronLogs: {},
    auditLogs: [],
    ingestedFiles: [],
    orderedSchedules: [],
    isLoadingSchedule: false,
    schemaData: { tables: [] },
    consoleOpen: false,
    setConsoleOpen: vi.fn(),
    displayedJobs: [],
    setDisplayedJobs: vi.fn(),
    removeDisplayedJob: vi.fn(),
    selectedConsoleJobId: null,
    setSelectedConsoleJobId: vi.fn(),
    eventFilter: '',
    setEventFilter: vi.fn(),
    isFetchingAudit: false,
    isFetchingCron: false,
    isLoadingAudit: false,
    isLoadingCron: false,
    isLoadingIngested: false,
    isLoadingSchema: false,
    isPurgeOpen: false,
    setIsPurgeOpen: vi.fn(),
    isSSEModalOpen: false,
    setIsSSEModalOpen: vi.fn(),
    isSyncModalOpen: false,
    setIsSyncModalOpen: vi.fn(),
    lines: [],
    purgeMutation: { isPending: false, mutate: vi.fn() },
    reset: vi.fn(),
    setSseDescription: vi.fn(),
    setSseTitle: vi.fn(),
    setStatusFilter: vi.fn(),
    setTaskFilter: vi.fn(),
    sseDescription: '',
    sseError: null,
    sseStatus: 'idle',
    sseTitle: '',
    start: vi.fn(),
    statusFilter: 'all',
    stop: vi.fn(),
    taskFilter: '',
  })),
}))

vi.mock('@/app/logs/_sections/CronColumns', () => ({ useCronColumns: () => [] }))
vi.mock('@/app/logs/_sections/AuditColumns', () => ({ useAuditColumns: () => [] }))
vi.mock('@/app/logs/_sections/FloatingOperationsDock', () => ({ FloatingOperationsDock: () => null }))
vi.mock('@/app/logs/_sections/CronTab', () => ({ CronTab: () => <div data-testid="cron-tab" /> }))
vi.mock('@/app/logs/_sections/ServiceHistoryTab', () => ({ ServiceHistoryTab: () => null }))
vi.mock('@/app/logs/_sections/IngestionTab', () => ({ IngestionTab: () => null }))
vi.mock('@/components/SSEModal', () => ({
  ControlledSSEModal: () => null,
  SSEModal: () => null,
  SSEProgressView: () => null,
  SSEExecutionSection: () => null,
  SSEDialogFooter: () => null,
}))
vi.mock('@/components/SyncFromCloudModal/SyncFromCloudModal', () => ({
  SyncFromCloudModal: () => null,
}))
vi.mock('@/components/NoServiceSelected', () => ({ NoServiceSelected: () => null }))

let errorSpy: ReturnType<typeof spyOnConsoleError>

beforeEach(() => {
  showToastMock.mockClear()
  postMock.mockReset()
  postMock.mockResolvedValue({ data: {} })
  errorSpy = spyOnConsoleError()
})

afterEach(() => {
  errorSpy.mockRestore()
})

test('logs page mounts and renders title in cron mode', () => {
  renderWithQuery(<LogsClient initialTab="cron" />)
  expect(screen.getByText('Data Management')).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})

function renderBar() {
  const start = vi.fn()
  const props: React.ComponentProps<typeof QuickActionsBar> = {
    isAnalyst: false,
    status: { access_level: 'read_write' },
    activeServiceId: 'svc',
    recentCrons: {},
    cronLogs: {},
    setSseTitle: vi.fn(),
    setSseDescription: vi.fn(),
    setIsSSEModalOpen: vi.fn(),
    setIsSyncModalOpen: vi.fn(),
    reset: vi.fn(),
    start,
    setDisplayedJobs: vi.fn(),
    setSelectedConsoleJobId: vi.fn(),
    setConsoleOpen: vi.fn(),
  }
  renderWithQuery(<QuickActionsBar {...props} />)
  return start
}

describe('QuickActionsBar quick-action failures (UX-9)', () => {
  test('toasts and does not open an SSE stream when Import Logs POST 5xxs', async () => {
    postMock.mockRejectedValue(new Error('ingest boom'))
    const start = renderBar()
    await userEvent.click(screen.getByRole('button', { name: /import logs/i }))
    await waitFor(() => expect(showToastMock).toHaveBeenCalledWith('ingest boom'))
    expect(start).not.toHaveBeenCalled()
  })

  test('toasts and does not open /cron-runs/undefined/stream when the 200 body has no run_id', async () => {
    postMock.mockResolvedValue({ data: {} })
    const start = renderBar()
    await userEvent.click(screen.getByRole('button', { name: /import logs/i }))
    await waitFor(() => expect(showToastMock).toHaveBeenCalledWith('No run id returned'))
    expect(start).not.toHaveBeenCalled()
  })
})

describe('partial_hour_merge cron registry entry', () => {
  it('has a label', () => {
    expect(CRON_DISPLAY_NAMES.partial_hour_merge).toBeTruthy()
  })

  it('has a description', () => {
    expect(CRON_EXPLANATIONS.partial_hour_merge).toBeTruthy()
  })

  it('is grouped alongside the other rollup jobs', () => {
    const rollupGroup = CRON_GROUPS.find((g) => g.tasks.includes('rollup_hour_heal'))
    expect(rollupGroup?.tasks).toContain('partial_hour_merge')
  })
})
