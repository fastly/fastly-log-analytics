import '@testing-library/jest-dom/vitest'
import React from 'react'
import { render, screen } from '@testing-library/react'
import { flexRender } from '@tanstack/react-table'
import type { CellContext } from '@/lib/table'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { CronScheduleBox } from './LogsCronSchedule'
import { useCronColumns } from './LogsCronColumns'
import { CronTab } from './LogsCronTab'
import type { CronRun } from './useLogsPageState'

vi.mock('@/hooks/useSSE', () => ({
  useDateFormat: () => ({ full: (value: string) => value, abbr: () => 'UTC' }),
  useNowMs: () => Date.parse('2026-01-01T00:00:00Z'),
}))

describe('Cloud cron schedule evidence', () => {
  it('shows the native disabled reason instead of a countdown for RUM', () => {
    render(<CronScheduleBox schedule={{ task: 'rum_ingest', next_run_time: null, disabled_reason: 'RUM is disabled' }} />)
    expect(screen.getByText('RUM is disabled')).toBeInTheDocument()
    expect(screen.queryByText('Next:')).not.toBeInTheDocument()
  })

  it('does not call an opportunistic maintenance stage disabled or claim it never ran', () => {
    render(<CronScheduleBox schedule={{
      task: 'retention_sweep', last_run_time: null, next_run_time: null,
      ...{ schedule_basis: 'maintenance_turn' },
    }} />)
    expect(screen.getByText('Next: Waiting')).toBeInTheDocument()
    expect(screen.getByText('Last: No record')).toBeInTheDocument()
    expect(screen.queryByText('Disabled')).not.toBeInTheDocument()
    expect(screen.queryByText('Never')).not.toBeInTheDocument()
  })

  it('shows request compaction as a scheduled job with its own countdown when enabled', () => {
    render(<CronScheduleBox schedule={{
      task: 'request_compact', last_run_time: '2025-12-31T23:59:40Z', next_run_time: '2026-01-01T00:00:10Z',
      ...{ schedule_basis: 'dispatcher' },
    }} />)
    expect(screen.queryByText('Next: Waiting')).not.toBeInTheDocument()
    expect(screen.queryByText('Last: No record')).not.toBeInTheDocument()
    expect(screen.getByText(/^Last:/)).toBeInTheDocument()
    expect(screen.getByText(/^Next:/)).toBeInTheDocument()
  })

  it('does not label an idle Cloud coordinator as No Alerts', () => {
    function Status() {
      const column = useCronColumns().find((item) => item.id === 'status')!
      return <>{flexRender(column.cell, {
        row: { original: { task: 'cloud_ingest_clock', status: 'skipped' } },
      } as CellContext<CronRun, unknown>)}</>
    }
    render(<Status />)
    expect(screen.getByText('Skipped')).toBeInTheDocument()
    expect(screen.queryByText('No Alerts')).not.toBeInTheDocument()
  })

  it('marks a Cloud stage that stopped at its budget as successful with more work queued', async () => {
    function Status({ run }: { run: Record<string, unknown> }) {
      const column = useCronColumns().find((item) => item.id === 'status')!
      return <>{flexRender(column.cell, { row: { original: run } } as CellContext<CronRun, unknown>)}</>
    }
    const { rerender } = render(<Status run={{ task: 'request_compact', status: 'success', more_pending: true }} />)
    expect(screen.getByText('Success')).toBeInTheDocument()
    expect(screen.getByText('More queued')).toBeInTheDocument()
    expect(screen.queryByText('Partial Success')).not.toBeInTheDocument()
    await userEvent.hover(screen.getByText('More queued'))
    expect(await screen.findByText(/continues on the next turn/)).toBeInTheDocument()
    rerender(<Status run={{ task: 'request_ingest', status: 'partial_success', corrupt_rows: 2, more_pending: true }} />)
    expect(screen.getByText('Partial Success')).toBeInTheDocument()
    expect(screen.queryByText('More queued')).not.toBeInTheDocument()
  })

  it('does not display zero when a failed Cloud stage has no counter evidence', () => {
    function MissingCounters() {
      const columns = useCronColumns()
      return <>{columns.filter((column) => 'accessorKey' in column &&
        ['files_downloaded', 'rows_ingested'].includes(String(column.accessorKey)))
        .map((column) => <div key={column.id ?? ('accessorKey' in column ? String(column.accessorKey) : '')}>{flexRender(column.cell, {
          row: { original: { task: 'request_ingest', cron_kind: 'request_ingest', status: 'error',
            files_downloaded: null, rows_ingested: null } },
        } as unknown as CellContext<CronRun, unknown>)}</div>)}</>
    }
    render(<MissingCounters />)
    expect(screen.getAllByText('—')).toHaveLength(2)
    expect(screen.queryByText('0')).not.toBeInTheDocument()
  })

  it('shows recorded raw-delete counts for Cloud cleanup executions', () => {
    function DeletedFiles() {
      const column = useCronColumns().find((item) => 'accessorKey' in item && item.accessorKey === 'files_deleted_fos')!
      return <>{flexRender(column.cell, {
        row: { original: { task: 'raw_delete', status: 'success', files_deleted_fos: 7 } },
      } as CellContext<CronRun, unknown>)}</>
    }
    render(<DeletedFiles />)
    expect(screen.getByText('7')).toBeInTheDocument()
  })

  it('offers the actual Cloud tasks in the task filter, not High-Scale-only tasks', async () => {
    const noop = () => {}
    render(
      <QueryClientProvider client={new QueryClient()}>
        <CronTab
          cronColumns={[]} cronLogs={undefined} isLoadingCron={false} isFetchingCron={false}
          orderedSchedules={[
            { task: 'request_ingest', activeJob: undefined, schedule: { task: 'request_ingest', next_run_time: null } },
            { task: 'log_flush', activeJob: undefined, schedule: { task: 'log_flush', next_run_time: null } },
          ]}
          taskFilter="all" setTaskFilter={noop} statusFilter="all" setStatusFilter={noop}
          status={undefined} activeServiceId="fixture-service"
          setDisplayedJobs={noop} setSelectedConsoleJobId={noop} setConsoleOpen={noop}
          isPurgeOpen={false} setIsPurgeOpen={noop} purgeMutation={{ isPending: false, mutate: noop }}
        />
      </QueryClientProvider>,
    )
    await userEvent.click(screen.getAllByRole('combobox')[0])
    expect(await screen.findByRole('option', { name: 'Request Ingest' })).toBeInTheDocument()
    expect(screen.getByRole('option', { name: 'Telemetry Flush' })).toBeInTheDocument()
    expect(screen.queryByRole('option', { name: 'Log Discovery' })).not.toBeInTheDocument()
  })

  it('says how often the Cloud clock ticks and which log period sets it', () => {
    const noop = () => {}
    const tab = (clock: React.ComponentProps<typeof CronTab>['clock']) => (
      <QueryClientProvider client={new QueryClient()}>
        <CronTab
          cronColumns={[]} cronLogs={undefined} isLoadingCron={false} isFetchingCron={false}
          orderedSchedules={[{ task: 'request_ingest', activeJob: undefined, schedule: { task: 'request_ingest', next_run_time: null } }]}
          taskFilter="all" setTaskFilter={noop} statusFilter="all" setStatusFilter={noop}
          status={undefined} activeServiceId="fixture-service"
          setDisplayedJobs={noop} setSelectedConsoleJobId={noop} setConsoleOpen={noop}
          isPurgeOpen={false} setIsPurgeOpen={noop} purgeMutation={{ isPending: false, mutate: noop }}
          clock={clock}
        />
      </QueryClientProvider>
    )
    const { rerender } = render(tab({ probe_interval_s: 20, log_period_s: 40, rum_log_period_s: null }))
    expect(screen.getByTestId('cron-clock')).toHaveTextContent('Each POP probes every 20 s (½ of the 40 s request log period)')
    rerender(tab({ probe_interval_s: 10, log_period_s: 60, rum_log_period_s: 20 }))
    expect(screen.getByTestId('cron-clock')).toHaveTextContent('Each POP probes every 10 s (½ of the 20 s RUM log period)')
    rerender(tab({ probe_interval_s: 30, log_period_s: 300, rum_log_period_s: null }))
    expect(screen.getByTestId('cron-clock')).toHaveTextContent('Each POP probes every 30 s (the 30 s maximum; 300 s request log period)')
    rerender(tab(null))
    expect(screen.queryByTestId('cron-clock')).not.toBeInTheDocument()
  })
})
