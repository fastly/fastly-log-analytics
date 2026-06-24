import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, test, expect, vi, beforeEach } from 'vitest'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../../helpers/query'
import React from 'react'

// UX-9: Import/Commit quick actions swallowed POST failures (lib/api auto-toasts
// only PUT/PATCH/DELETE) and opened /api/cron-runs/undefined/stream when the 200
// body lacked a run_id. The fix toasts the error and guards the run_id.

const { showToastMock, postMock } = vi.hoisted(() => ({ showToastMock: vi.fn(), postMock: vi.fn() }))

vi.mock('@/lib/toast', () => ({ showToast: showToastMock }))
vi.mock('@/lib/cron-cache-bust', () => ({ cronCacheBust: vi.fn() }))
vi.mock('@/lib/api', () => ({
  client: { POST: postMock, GET: vi.fn(), use: vi.fn() },
  extractApiError: (e: unknown) => (e instanceof Error ? e.message : String(e)),
}))

import { QuickActionsBar } from '@/app/logs/_sections/QuickActionsBar'

const queryClient = createTestQueryClient()

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
    setHasSyncedExtents: vi.fn(),
    reset: vi.fn(),
    start,
    setDisplayedJobs: vi.fn(),
    setSelectedConsoleJobId: vi.fn(),
    setConsoleOpen: vi.fn(),
  }
  render(
    <QueryClientProvider client={queryClient}>
      <QuickActionsBar {...props} />
    </QueryClientProvider>,
  )
  return start
}

beforeEach(() => {
  showToastMock.mockClear()
  postMock.mockReset()
})

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
