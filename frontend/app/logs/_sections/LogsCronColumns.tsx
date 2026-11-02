'use client'

import React from 'react'
import { Loader2 } from 'lucide-react'
import { cn, formatCount } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { CopyButton } from '@/components/ui/confirm-dialog'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { LiveElapsed } from '@/components/SyncStatusBadge/SyncStatusBadge'
import { DateTimeCell } from '@/components/DataTable'
import type { ColumnDef } from '@tanstack/react-table'
import type { CronRun } from './useLogsPageState'
import { CRON_DISPLAY_NAMES, CRON_EXPLANATIONS } from './LogsCronSchedule'

const DASH_CELL = <span className="text-muted-foreground">—</span>
const PROCESSING_CELL = <span className="font-mono text-muted-foreground text-xs italic">Processing...</span>
const NO_FILES_TASKS = new Set([
  'expire_snapshots',
  'rollup_compact_daily',
  'rollup_hour_heal',
  'metadata_cleanup',
  'insights_prewarmer',
  'partial_hour_merge',
])
const NO_ROWS_TASKS = new Set([...NO_FILES_TASKS, 'optimize', 'commit', 'rum_commit', 'local_compact'])

const COMMIT_CRON_COLUMNS: ColumnDef<CronRun>[] = [
  {
    accessorKey: 'files_deleted_fos',
    header: 'Log Files Deleted',
    cell: ({ row }) =>
      row.original.status === 'running' || row.original.task !== 'log_discovery' ? (
        DASH_CELL
      ) : (
        <span className="font-mono text-muted-foreground tabular-nums text-xs">
          {formatCount(row.original.files_deleted_fos || 0)}
        </span>
      ),
  },
  {
    id: 'rows_committed',
    header: 'Rows Committed',
    cell: ({ row }) => {
      if (row.original.status === 'running') return PROCESSING_CELL
      const val = row.original.task === 'commit' ? row.original.rows_ingested : null
      return (
        <span className="font-mono text-muted-foreground tabular-nums text-xs">
          {val != null ? formatCount(val) : DASH_CELL}
        </span>
      )
    },
  },
]

export function useCronColumns(): ColumnDef<CronRun>[] {
  return React.useMemo(
    () => [
      {
        accessorKey: 'started_at',
        id: 'started_at',
        meta: { label: 'Started At' },
        header: 'Started At',
        cell: ({ row }) => <DateTimeCell iso={row.original.started_at} />,
      },
      {
        id: 'finished_at',
        meta: { label: 'Finished At' },
        enableSorting: false,
        accessorFn: (row) =>
          row.started_at && row.duration_s != null
            ? new Date(new Date(row.started_at).getTime() + row.duration_s * 1000).toISOString()
            : null,
        header: () => <span className="text-xs font-medium px-2.5">Finished At</span>,
        cell: ({ row }) => {
          const { status, started_at: startIso, duration_s: dur } = row.original
          return status === 'running' || !startIso || dur == null ? (
            DASH_CELL
          ) : (
            <DateTimeCell iso={new Date(new Date(startIso).getTime() + dur * 1000).toISOString()} />
          )
        },
      },
      {
        accessorKey: 'task',
        id: 'task',
        meta: { label: 'Task' },
        header: 'Task',
        cell: ({ row }) => {
          const { task, summary } = row.original
          const isSync = task === 'log_discovery'
          return (
            <div className="flex flex-col gap-1 py-1">
              <TooltipProvider delay={200}>
                <Tooltip>
                  <TooltipTrigger
                    render={
                      <Badge
                        className={cn(
                          'w-fit px-1.5 py-0 shadow-none text-[10px] uppercase font-bold',
                          isSync
                            ? 'bg-blue-500/10 text-blue-600 hover:bg-blue-500/20'
                            : 'bg-purple-500/10 text-purple-600 hover:bg-purple-500/20',
                        )}
                      >
                        {CRON_DISPLAY_NAMES[task] || task}
                      </Badge>
                    }
                  />
                  <TooltipContent side="right" className="max-w-[250px] text-xs">
                    <p>{CRON_EXPLANATIONS[task] || 'Background job.'}</p>
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
              {summary && (
                <span className="text-[11px] text-muted-foreground whitespace-normal break-words leading-tight">
                  {summary}
                </span>
              )}
            </div>
          )
        },
      },
      {
        accessorKey: 'status',
        id: 'status',
        meta: { label: 'Status' },
        header: 'Status',
        cell: ({ row }) => {
          const { status: val, error_message: err, corrupt_rows } = row.original
          if (val === 'running') {
            return (
              <Badge
                variant="outline"
                className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold border-blue-500/30 text-blue-700 dark:text-blue-400 bg-blue-500/10 flex items-center gap-1 w-fit"
              >
                <Loader2 className="w-3 h-3 animate-spin" /> Running
              </Badge>
            )
          }
          if (val === 'skipped') {
            return (
              <Badge variant="secondary" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">
                No Alerts
              </Badge>
            )
          }
          if (val === 'success' && (!corrupt_rows || corrupt_rows === 0)) {
            return (
              <Badge variant="success" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">
                Success
              </Badge>
            )
          }
          if (val === 'partial_success' || (val === 'success' && (corrupt_rows ?? 0) > 0)) {
            return (
              <Badge variant="warning" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">
                Partial Success
              </Badge>
            )
          }
          return (
            <div className="flex items-center gap-1.5">
              <Tooltip>
                <TooltipTrigger
                  render={
                    <Badge variant="destructive" className="px-1.5 py-0 shadow-none uppercase text-[10px] font-bold" />
                  }
                >
                  Error
                </TooltipTrigger>
                <TooltipContent className="max-w-[400px] break-words bg-destructive text-white dark:text-white">
                  <p className="text-xs font-mono">{err || 'Unknown error'}</p>
                </TooltipContent>
              </Tooltip>
              {err && (
                <CopyButton
                  text={err}
                  ariaLabel="Copy full error message"
                  title="Copy full error message"
                  stopPropagation
                  className="h-6 w-6 text-muted-foreground hover:text-foreground"
                />
              )}
            </div>
          )
        },
      },
      {
        accessorKey: 'duration_s',
        id: 'duration_s',
        meta: { label: 'Duration' },
        header: 'Duration',
        cell: ({ row }) => {
          const s = row.original.duration_s
          if (s == null) return <span className="font-mono text-muted-foreground tabular-nums text-xs">—</span>
          if (row.original.status === 'running') {
            return (
              <LiveElapsed
                startedAt={row.original.started_at}
                className="font-mono text-blue-700 dark:text-blue-300 tabular-nums text-xs font-medium"
              />
            )
          }
          const fmt =
            s < 1 ? `${Math.round(s * 1000)}ms` : s < 60 ? `${s.toFixed(1)}s` : `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`
          return <span className="font-mono text-muted-foreground tabular-nums text-xs">{fmt}</span>
        },
      },
      {
        accessorKey: 'files_downloaded',
        id: 'files_downloaded',
        meta: { label: 'Files Downloaded' },
        header: 'Files Processed',
        cell: ({ row }) => {
          if (row.original.status === 'running') return PROCESSING_CELL
          const { task, files_downloaded, rows_ingested, parquet_files_optimized } = row.original
          if (NO_FILES_TASKS.has(task) || ((task === 'commit' || task === 'rum_commit') && !rows_ingested)) {
            return DASH_CELL
          }
          const count =
            task === 'commit' || task === 'rum_commit'
              ? 1
              : task === 'optimize'
                ? parquet_files_optimized || 0
                : files_downloaded || 0
          const label =
            task === 'alerts'
              ? count === 1
                ? 'alert evaluated'
                : 'alerts evaluated'
              : task === 'commit' || task === 'rum_commit'
                ? 'cloud file cached'
                : task === 'optimize' || task === 'local_compact'
                  ? 'files merged'
                  : task === 'rum_sync' || task === 'rum_discovery'
                    ? 'raw RUM logs'
                    : task === 'ngwaf_sync'
                      ? count === 1
                        ? 'bot record'
                        : 'bot records'
                      : 'raw logs'
          return (
            <span className="font-mono text-muted-foreground tabular-nums text-xs" title={label}>
              {formatCount(count)} <span className="text-[10px] text-muted-foreground">{label}</span>
            </span>
          )
        },
      },
      {
        accessorKey: 'rows_ingested',
        header: 'Log Entries Processed',
        cell: ({ row }) => {
          if (row.original.status === 'running') return PROCESSING_CELL
          const { task, rows_ingested, corrupt_rows, error_message } = row.original
          if (NO_ROWS_TASKS.has(task)) return DASH_CELL
          if (task === 'alerts') {
            const triggered = rows_ingested || 0
            return triggered === 0 ? (
              DASH_CELL
            ) : (
              <span className="font-mono tabular-nums text-xs text-amber-500 font-medium">
                {triggered} {triggered === 1 ? 'alert triggered' : 'alerts triggered'}
              </span>
            )
          }
          const rows = rows_ingested || 0
          const corrupt = corrupt_rows || 0
          return (
            <div className="flex items-center gap-2">
              <span className="font-mono text-muted-foreground tabular-nums text-xs">{formatCount(rows)}</span>
              {corrupt > 0 && (
                <div className="flex items-center gap-1 group/corrupt">
                  <Tooltip>
                    <TooltipTrigger
                      render={
                        <Badge variant="destructive" className="px-1.5 py-0 shadow-none text-[10px] font-bold" />
                      }
                    >
                      {formatCount(corrupt)} Skipped
                    </TooltipTrigger>
                    <TooltipContent>
                      These lines were skipped due to missing timestamps or invalid JSON structure.
                    </TooltipContent>
                  </Tooltip>
                  {error_message && (
                    <CopyButton
                      text={error_message}
                      ariaLabel="Copy corrupt-line details"
                      title="Copy corrupt lines"
                      stopPropagation
                      className="h-5 w-5 text-muted-foreground hover:text-foreground opacity-0 group-hover/corrupt:opacity-100 transition-opacity"
                    />
                  )}
                </div>
              )}
            </div>
          )
        },
      },
      ...COMMIT_CRON_COLUMNS,
    ],
    [],
  )
}
