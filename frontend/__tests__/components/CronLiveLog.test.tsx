/**
 * @vitest-environment jsdom
 *
 * Consolidated CronLiveLog and CronSettingsModal (LogSyncSection) tests.
 */
import React from 'react'
import { render, screen, cleanup } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, test, vi } from 'vitest'
import { createUseSSEMock } from '../helpers/sse-mock'

const { state: sseState, resetState: resetSSE } = createUseSSEMock()

vi.mock('@/hooks/useSSE', () => ({
  useSSE: () => sseState,
}))

vi.mock('@/hooks/useDateFormat', () => ({
  useDateFormat: () => ({
    full: (d: string) => `FULL(${d})`,
    abbr: () => 'TZ',
  }),
}))

import { CronLiveLog } from '@/components/CronLiveLog'
import { LogSyncSection } from '@/components/CronSettingsModal/Schedule'

beforeEach(() => {
  resetSSE()
})

afterEach(() => {
  cleanup()
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
