import React from 'react'
import { render, screen } from '@testing-library/react'
import { flexRender, type CellContext } from '@tanstack/react-table'
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
    expect(screen.getByRole('option', { name: 'Request Ingest' })).toBeInTheDocument()
    expect(screen.getByRole('option', { name: 'Telemetry Flush' })).toBeInTheDocument()
    expect(screen.queryByRole('option', { name: 'Log Discovery' })).not.toBeInTheDocument()
  })
})
