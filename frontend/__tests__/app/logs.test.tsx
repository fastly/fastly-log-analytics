import { render, screen } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
// The route's page.tsx is now an async RSC that pre-fetches the cron
// runs server-side and dehydrates the cache for the client island.
// The unit test exercises the client island directly so it stays
// compatible with vitest's synchronous render.
import LogsClient from '@/app/logs/_sections/LogsClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'

// R-6 (testing_suite_audit_2026-06-14.md). Logs is a tabs-driven
// admin page (not a ReportLayout consumer), so the smoke test mocks
// the state hook + the heavy tab/section components and asserts on
// the PageHeader title only.

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/logs'))

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: vi.fn().mockResolvedValue({ data: {} }), use: vi.fn() },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/app/logs/_state', () => ({
  useLogsPageState: vi.fn(() => ({
    activeServiceId: 'test-svc',
    activeTab: 'cron',
    handleTabChange: vi.fn(),
    isAnalyst: false,
    status: { idle: true },
    catalogMaps: {},
    recentCrons: [],
    cronLogs: [],
    auditLogs: [],
    ingestedFiles: [],
    orderedSchedules: [],
    schemaData: { tables: [] },
    backgroundCronToast: null,
    setBackgroundCronToast: vi.fn(),
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
vi.mock('@/app/logs/_sections/QuickActionsBar', () => ({ QuickActionsBar: () => null }))
vi.mock('@/app/logs/_sections/CronTab', () => ({ CronTab: () => <div data-testid="cron-tab" /> }))
vi.mock('@/app/logs/_sections/ServiceHistoryTab', () => ({ ServiceHistoryTab: () => null }))
vi.mock('@/app/logs/_sections/IngestionTab', () => ({ IngestionTab: () => null }))
vi.mock('@/app/logs/_sections/SSEModal', () => ({ SSEModal: () => null }))
vi.mock('@/components/SyncFromCloudModal/SyncFromCloudModal', () => ({
  SyncFromCloudModal: () => null,
}))
vi.mock('@/components/NoServiceSelected', () => ({ NoServiceSelected: () => null }))

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

let errorSpy: ReturnType<typeof spyOnConsoleError>

beforeEach(() => {
  queryClient.clear()
  errorSpy = spyOnConsoleError()
})

afterEach(() => {
  errorSpy.mockRestore()
})

test('logs page mounts and renders title', () => {
  render(
    <QueryClientProvider client={queryClient}>
      <LogsClient />
    </QueryClientProvider>,
  )
  expect(screen.getByText('Data Management')).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})
