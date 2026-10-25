/**
 * @vitest-environment jsdom
 *
 * Consolidated CronLiveLog, CronSettingsModal (LogSyncSection), and Logs page tests.
 */
import React from 'react'
import { render, screen, cleanup, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, test, vi } from 'vitest'
import { createUseSSEMock } from '../helpers/page-smoke'
import { renderWithQuery } from '../helpers/page-smoke'
import { spyOnConsoleError } from '../helpers/page-smoke'

const { state: sseState, resetState: resetSSE } = createUseSSEMock()

const { showToastMock, postMock } = vi.hoisted(() => ({
  showToastMock: vi.fn(),
  postMock: vi.fn(),
}))

vi.mock('@/hooks/useSSE', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useSSE')>()),
  useSSE: () => sseState,
  useDateFormat: () => ({
    full: (d: string) => `FULL(${d})`,
    abbr: () => 'TZ',
  }),
}))

vi.mock('@/lib/utils', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/utils')>()),
  showToast: showToastMock,
}))
vi.mock('@/lib/admin-stream-apply', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/admin-stream-apply')>()),
  cronCacheBust: vi.fn(),
}))

vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())
vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/logs'))

vi.mock('@/lib/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/api')>()),
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
vi.mock('@/app/logs/_sections/FloatingOperationsDock', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/app/logs/_sections/FloatingOperationsDock')>()),
  FloatingOperationsDock: () => null,
}))
vi.mock('@/app/logs/_sections/CronTab', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/app/logs/_sections/CronTab')>()),
  CronTab: () => <div data-testid="cron-tab" />,
}))
vi.mock('@/components/SSEModal', () => ({
  ControlledSSEModal: () => null,
  SSEModal: () => null,
  SSEProgressView: () => null,
  SSEExecutionSection: () => null,
  SSEDialogFooter: () => null,
}))
vi.mock('@/components/SyncStatusBadge/SyncStatusBadge', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/SyncStatusBadge/SyncStatusBadge')>()),
  SyncFromCloudModal: () => null,
}))
vi.mock('@/components/ReportLayout', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/ReportLayout')>()),
  NoServiceSelected: () => null,
}))

import { CronLiveLog } from '@/app/logs/_sections/FloatingOperationsDock'
import { LogSyncSection } from '@/components/CronSettingsModal/Schedule'
import LogsClient, { QuickActionsBar } from '@/app/logs/_sections/LogsClient'
import { CRON_EXPLANATIONS, CRON_DISPLAY_NAMES, CRON_GROUPS } from '@/app/logs/_sections/CronTab'

let errorSpy: ReturnType<typeof spyOnConsoleError>

beforeEach(() => {
  resetSSE()
  showToastMock.mockClear()
  postMock.mockReset()
  postMock.mockResolvedValue({ data: {} })
  errorSpy = spyOnConsoleError()
})

afterEach(() => {
  cleanup()
  errorSpy.mockRestore()
})

describe('CronLiveLog — message parsing', () => {
  it('renders each line type with the right text in terminalMode', () => {
    sseState.status = 'streaming'
    sseState.lines = [
      { _id: 1, type: 'status', message: 'Starting ingest' },
      { _id: 2, type: 'file_done', file_name: 'a.gz' },
      { _id: 3, type: 'error', message: 'Boom' },
      { _id: 4, type: 'done', message: 'All clean' },
    ]
    render(<CronLiveLog runId={1} terminalMode />)
    expect(screen.getByText('Starting ingest')).toBeInTheDocument()
    expect(screen.getByText(/Processed a\.gz/)).toBeInTheDocument()
    expect(screen.getByText('Boom')).toBeInTheDocument()
    expect(screen.getByText('All clean')).toBeInTheDocument()
  })
})

describe('CronLiveLog — onDone callback', () => {
  it('fires onDone exactly once on streaming → done', () => {
    const onDone = vi.fn()
    sseState.status = 'streaming'
    sseState.lines = [{ _id: 1, type: 'status', message: 'go' }]
    const { rerender } = render(<CronLiveLog runId={1} onDone={onDone} />)
    expect(onDone).not.toHaveBeenCalled()

    sseState.status = 'done'
    sseState.lines = [...sseState.lines, { _id: 2, type: 'done', message: 'ok' }]
    rerender(<CronLiveLog runId={1} onDone={onDone} />)
    expect(onDone).toHaveBeenCalledTimes(1)
  })

  it('fires onDone on streaming → error', () => {
    const onDone = vi.fn()
    sseState.status = 'streaming'
    const { rerender } = render(<CronLiveLog runId={2} onDone={onDone} />)
    expect(onDone).not.toHaveBeenCalled()

    sseState.status = 'error'
    sseState.lines = [{ _id: 1, type: 'error', message: '503' }]
    rerender(<CronLiveLog runId={2} onDone={onDone} />)
    expect(onDone).toHaveBeenCalledTimes(1)
  })

  it('does not double-fire onDone across successive terminal transitions', () => {
    const onDone = vi.fn()
    sseState.status = 'streaming'
    const { rerender } = render(<CronLiveLog runId={3} onDone={onDone} />)

    sseState.status = 'done'
    sseState.lines = [{ _id: 1, type: 'done', message: 'first' }]
    rerender(<CronLiveLog runId={3} onDone={onDone} />)
    expect(onDone).toHaveBeenCalledTimes(1)

    sseState.status = 'error'
    sseState.lines = [...sseState.lines, { _id: 2, type: 'error', message: 'late' }]
    rerender(<CronLiveLog runId={3} onDone={onDone} />)
    expect(onDone).toHaveBeenCalledTimes(1)

    sseState.status = 'done'
    rerender(<CronLiveLog runId={3} onDone={onDone} />)
    expect(onDone).toHaveBeenCalledTimes(1)
  })
})

describe('CronLiveLog — singleLine mode', () => {
  it('renders only the latest line; older lines are not in the DOM', () => {
    sseState.status = 'streaming'
    sseState.lines = [
      { _id: 1, type: 'status', message: 'old-one' },
      { _id: 2, type: 'status', message: 'old-two' },
      { _id: 3, type: 'status', message: 'latest-line' },
    ]
    render(<CronLiveLog runId={4} singleLine />)
    expect(screen.getByText('latest-line')).toBeInTheDocument()
    expect(screen.queryByText('old-one')).toBeNull()
    expect(screen.queryByText('old-two')).toBeNull()
  })

  it('truncates lines longer than 80 chars with an ellipsis suffix', () => {
    const long = 'x'.repeat(120)
    sseState.status = 'streaming'
    sseState.lines = [{ _id: 1, type: 'status', message: long }]
    render(<CronLiveLog runId={5} singleLine />)
    const expected = 'x'.repeat(80) + '...'
    expect(screen.getByText(expected)).toBeInTheDocument()
    expect(screen.queryByText(long)).toBeNull()
  })
})

describe('CronLiveLog — terminalMode', () => {
  it('accumulates every line in the DOM (no slicing)', () => {
    sseState.status = 'streaming'
    sseState.lines = Array.from({ length: 6 }, (_, i) => ({
      _id: i + 1,
      type: 'status',
      message: `line-${i + 1}`,
    }))
    render(<CronLiveLog runId={6} terminalMode />)
    for (let i = 1; i <= 6; i += 1) {
      expect(screen.getByText(`line-${i}`)).toBeInTheDocument()
    }
  })

  it('renders a SESSION INITIATED header when startedAt is provided', () => {
    sseState.status = 'streaming'
    sseState.lines = [{ _id: 1, type: 'status', message: 'hi' }]
    render(<CronLiveLog runId={7} terminalMode startedAt="2026-06-16T00:00:00Z" />)
    expect(
      screen.getByText(/SESSION INITIATED AT: FULL\(2026-06-16T00:00:00Z\) TZ/),
    ).toBeInTheDocument()
  })
})

const baseLogSyncProps = {
  syncEnabled: true,
  setSyncEnabled: vi.fn(),
  pollingMode: 'regular' as const,
  setPollingMode: vi.fn(),
  deleteAfter: false,
  setDeleteAfter: vi.fn(),
  dataRetention: '30',
  setDataRetention: vi.fn(),
  cacheRetention: '90',
  setCacheRetention: vi.fn(),
  rollupRetention: '12',
  setRollupRetention: vi.fn(),
  commitInterval: '5',
  setCommitInterval: vi.fn(),
  syncLogEnabled: true,
  setSyncLogEnabled: vi.fn(),
  syncRetention: '7',
  setSyncRetention: vi.fn(),
  syncFreqLabel: 'every 2 minutes',
  isAnalyst: false,
  syncIntervalNum: 120,
  adminSyncSeconds: 120,
  rumRetention: '30',
  setRumRetention: vi.fn(),
  rumEnabled: false,
  rumLogPeriod: '300',
  setRumLogPeriod: vi.fn(),
  rumDeleteAfter: false,
  setRumDeleteAfter: vi.fn(),
}

describe('CronSettingsModal — LogSyncSection', () => {
  test('polling mode selector exposes Adaptive LIST-cost disclosure and updates the mode', async () => {
    const user = userEvent.setup()
    const setPollingMode = vi.fn()
    render(<LogSyncSection {...baseLogSyncProps} setPollingMode={setPollingMode} />)

    expect(screen.getByText(/Adaptive makes up to two short follow-up discovery passes/)).toBeInTheDocument()
    await user.click(screen.getByRole('combobox', { name: 'Discovery Polling Mode' }))
    await user.click(await screen.findByRole('option', { name: 'Adaptive' }))

    expect(setPollingMode).toHaveBeenCalledWith('adaptive')
  })

  test('displays banner with Configure RUM link when RUM is disabled on edge', () => {
    render(
      <LogSyncSection
        {...baseLogSyncProps}
        rumEnabled={false}
        rumSyncEnabled={false}
        setRumSyncEnabled={vi.fn()}
        serviceId="test-service-123"
      />,
    )

    expect(screen.getByText(/Real User Monitoring is not currently enabled on Fastly edge/)).toBeInTheDocument()
    const link = screen.getByRole('link', { name: 'Configure RUM' })
    expect(link).toBeInTheDocument()
    expect(link).toHaveAttribute('href', '/admin/rum?service=test-service-123')
  })

  test('renders Enable RUM Beacon Sync switch when RUM is enabled on edge and allows toggling', async () => {
    const user = userEvent.setup()
    const setRumSyncEnabled = vi.fn()

    render(
      <LogSyncSection
        {...baseLogSyncProps}
        rumEnabled={true}
        rumSyncEnabled={false}
        setRumSyncEnabled={setRumSyncEnabled}
        serviceId="test-service-123"
      />,
    )

    expect(screen.getByText('Enable RUM Beacon Sync')).toBeInTheDocument()
    const rumSwitch = screen.getByRole('switch', { name: 'Enable RUM Beacon Sync' })
    expect(rumSwitch).toBeInTheDocument()
    expect(rumSwitch).toHaveAttribute('aria-checked', 'false')

    await user.click(rumSwitch)
    expect(setRumSyncEnabled).toHaveBeenCalledWith(true, expect.anything())
  })
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
