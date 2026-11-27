import React from 'react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import type { CronRun } from '@/app/logs/_sections/useLogsPageState'

const mockStart = vi.fn()
const mockStop = vi.fn()
let mockCloudMode = true

vi.mock('@/hooks/useSSE', () => ({
  useSSE: () => ({
    lines: [],
    status: 'error',
    start: mockStart,
    stop: mockStop,
  }),
  useDateFormat: () => ({
    full: (v: string) => v,
    abbr: () => 'UTC',
  }),
}))

vi.mock('@/lib/cloud-engine/mode', () => ({
  get IS_CLOUD_MODE() {
    return mockCloudMode
  },
  isCloudMode: () => mockCloudMode,
  setCloudModeRuntime: vi.fn(),
}))

import { CronLiveLog, FloatingOperationsDock } from '@/app/logs/_sections/LogsCronConsole'

function makeCloudRun(overrides: Partial<CronRun> = {}): CronRun {
  return {
    id: 1795554422123,
    task: 'request_ingest',
    started_at: '2026-11-24T21:07:02.000Z',
    duration_s: 0.155,
    status: 'success',
    error_message: null,
    files_downloaded: 5,
    files_deleted_fos: 0,
    rows_ingested: 412,
    corrupt_rows: 0,
    parquet_files_created: 2,
    parquet_keys: [],
    summary: 'request_ingest: more work queued, continues next turn',
    run_id: 'cron-1795554422-abc-request_ingest',
    outcome: 'partial',
    listed_files: 99,
    more_pending: true,
    vcpu_ms: 29,
    kv_ops: 6,
    backend_requests_used: 3,
    details_json: JSON.stringify({
      execution_metrics: { calls: 17, duration_ms: 1631, heap_mib: 47, vcpu_ms: 40 },
      worker_executions_metrics: [
        { kind: 'request_ingest', shard: '0', status: 'ok', duration_ms: 48, vcpu_ms: 4, calls: 2 },
        { kind: 'request_ingest', shard: '1', status: 'error', duration_ms: 25, vcpu_ms: 3, calls: 2 },
      ],
    }),
    ...overrides,
  }
}

describe('CronLiveLog in Cloud mode', () => {
  beforeEach(() => {
    mockStart.mockClear()
    mockStop.mockClear()
    mockCloudMode = true
  })

  it('renders structured run details instead of calling /stream for persisted Cloud runs', () => {
    const job = makeCloudRun()
    render(
      <FloatingOperationsDock
        displayedJobs={[job]}
        setDisplayedJobs={vi.fn()}
        isOpen={true}
        setIsOpen={vi.fn()}
        selectedJobId={job.id}
        setSelectedJobId={vi.fn()}
        onDismiss={vi.fn()}
      />,
    )

    expect(mockStart).not.toHaveBeenCalled()
    expect(screen.queryByText('Failed to stream logs.')).toBeNull()
    expect(screen.getByText(/request_ingest: more work queued/)).toBeTruthy()
    expect(screen.getByText(/listed=99/)).toBeTruthy()
    expect(screen.getByText(/converted=5/)).toBeTruthy()
    expect(screen.getByText(/rows=412/)).toBeTruthy()
    expect(screen.getByText(/shard=0.*status=ok/)).toBeTruthy()
    expect(screen.getByText(/shard=1.*status=error/)).toBeTruthy()
  })

  it('still streams /api/cron-runs/cloud-tick/stream for manual Cloud ticks', () => {
    render(<CronLiveLog runId="cloud-tick" terminalMode />)
    expect(mockStart).toHaveBeenCalledWith('/api/cron-runs/cloud-tick/stream')
  })

  it('keeps SSE streaming for High-Scale runs', () => {
    mockCloudMode = false
    render(<CronLiveLog runId={42} job={makeCloudRun({ id: 42 })} terminalMode />)
    expect(mockStart).toHaveBeenCalledWith('/api/cron-runs/42/stream')
  })
})
