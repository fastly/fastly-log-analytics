/**
 * @vitest-environment jsdom
 *
 * CronLiveLog — renders SSE-driven cron output in two display modes
 * (singleLine compact / terminalMode full scroll) and notifies the parent
 * via `onDone` when the stream terminates.
 *
 * Audit finding this test addresses: the component carried two correctness
 * invariants that were never pinned by a test —
 *   1. `onDone` must fire EXACTLY ONCE per stream lifecycle, regardless of
 *      how many terminal events the SSE source emits (the component uses
 *      `doneFired` ref guard at CronLiveLog.tsx:21,38). A regression that
 *      drops the ref guard would silently re-trigger downstream side
 *      effects (refreshes, toast spam, modal re-opens).
 *   2. `singleLine` mode must collapse history to the tail AND truncate
 *      messages over 80 chars (CronLiveLog.tsx:46–47,92). Without coverage,
 *      a refactor could trivially break the marquee-line UI used in the
 *      compact cron-status widget.
 *
 * The underlying SSE transport is covered exhaustively by
 * frontend/__tests__/hooks/useSSE.test.ts, so we mock `useSSE` and drive
 * the component with controlled state transitions via rerender().
 */
import React from 'react'
import { render, screen, cleanup } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// Mock useSSE — the component just consumes { lines, status, start, stop }.
// Tests mutate this object between renders to simulate transitions.
const sseState: {
  lines: Array<Record<string, unknown>>
  status: 'idle' | 'streaming' | 'done' | 'error'
  error: string | null
  start: ReturnType<typeof vi.fn>
  stop: ReturnType<typeof vi.fn>
} = {
  lines: [],
  status: 'idle',
  error: null,
  start: vi.fn(),
  stop: vi.fn(),
}

vi.mock('@/hooks/useSSE', () => ({
  useSSE: () => sseState,
}))

// useDateFormat pulls in zustand + date-fns; stub to flat strings.
vi.mock('@/hooks/useDateFormat', () => ({
  useDateFormat: () => ({
    full: (d: string) => `FULL(${d})`,
    abbr: () => 'TZ',
  }),
}))

import { CronLiveLog } from '@/components/CronLiveLog'

function resetSSE() {
  sseState.lines = []
  sseState.status = 'idle'
  sseState.error = null
  sseState.start = vi.fn()
  sseState.stop = vi.fn()
}

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

    // First terminal event — fires.
    sseState.status = 'done'
    sseState.lines = [{ _id: 1, type: 'done', message: 'first' }]
    rerender(<CronLiveLog runId={3} onDone={onDone} />)
    expect(onDone).toHaveBeenCalledTimes(1)

    // A second terminal event (e.g. late `error` after `done`) must NOT
    // re-fire — the doneFired ref guards against downstream side-effect
    // spam (refreshes / toasts / modal re-opens).
    sseState.status = 'error'
    sseState.lines = [...sseState.lines, { _id: 2, type: 'error', message: 'late' }]
    rerender(<CronLiveLog runId={3} onDone={onDone} />)
    expect(onDone).toHaveBeenCalledTimes(1)

    // And a redundant done → done bounce stays guarded.
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
    // Truncation rule lives at CronLiveLog.tsx:92 — first 80 chars + '...'.
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
