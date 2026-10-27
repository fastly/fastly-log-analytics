'use client'

import React from 'react'
import { AlertTriangle, CheckCircle2, RotateCcw, Trash2 } from 'lucide-react'
import { formatLocalDateTime } from '@/lib/utils'
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
} from '@tanstack/react-table'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import {
  type LedgerEntry,
  type LedgerStatusFilter,
  VALID_LEDGER_STATUSES,
} from './QueueOverviewPanels'

function formatLedgerBytes(bytes: number | null | undefined): string | null {
  if (bytes == null || bytes <= 0) return null
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`
}

export function LedgerEntriesTable({
  items,
  actionBusyKey,
  onRetry,
  onPurge,
}: {
  items: LedgerEntry[]
  actionBusyKey: string | null
  onRetry: (entry: LedgerEntry) => Promise<void>
  onPurge: (entry: LedgerEntry) => Promise<void>
}) {
  const columns = React.useMemo<ColumnDef<LedgerEntry>[]>(
    () => [
      {
        id: 'status',
        header: 'Status',
        cell: ({ row }) => {
          const item = row.original
          return (
            <Badge
              variant={item.status === 'dead_letter' ? 'destructive' : 'outline'}
              className={
                item.status === 'quarantined'
                  ? 'border-amber-500/40 text-amber-600 dark:text-amber-400'
                  : item.status === 'committed'
                    ? 'border-emerald-500/40 text-emerald-600 dark:text-emerald-400'
                    : undefined
              }
            >
              {item.status}
            </Badge>
          )
        },
      },
      {
        id: 'source',
        header: 'Source',
        cell: ({ row }) => {
          const item = row.original
          return (
            <div className="flex flex-col gap-0.5">
              <Badge variant="secondary" className="w-fit uppercase text-[10px]">
                {item.source_type}
              </Badge>
              <span className="font-mono text-[11px] text-muted-foreground">
                {item.service_id}
              </span>
            </div>
          )
        },
      },
      {
        id: 'object_key',
        header: 'Object Key & Error Details',
        cell: ({ row }) => {
          const item = row.original
          const sizeLabel = formatLedgerBytes(item.size_bytes)
          return (
            <>
              <div className="font-mono text-xs break-all text-foreground">
                {item.object_key}
              </div>
              {item.error_kind || item.last_error ? (
                <div className="mt-1 flex flex-wrap items-baseline gap-1.5 text-[11px] text-red-600 dark:text-red-400">
                  {item.error_kind ? (
                    <span className="rounded bg-red-500/10 px-1.5 py-0.5 font-mono font-medium">
                      {item.error_kind}
                    </span>
                  ) : null}
                  {item.last_error ? (
                    <span className="font-mono break-all">{item.last_error}</span>
                  ) : null}
                </div>
              ) : null}
              {sizeLabel ? (
                <div className="mt-0.5 text-[11px] text-muted-foreground">
                  Size: {sizeLabel}
                </div>
              ) : null}
            </>
          )
        },
      },
      {
        id: 'attempts',
        header: 'Attempts / Task',
        cell: ({ row }) => {
          const item = row.original
          return (
            <>
              <div className="tabular-nums font-medium">
                {item.attempts ?? 0} attempt{(item.attempts ?? 0) === 1 ? '' : 's'}
              </div>
              {item.originating_task ? (
                <div className="font-mono text-[11px] text-muted-foreground">
                  {item.originating_task}
                  {item.originating_run_id ? ` #${item.originating_run_id}` : ''}
                </div>
              ) : null}
            </>
          )
        },
      },
      {
        id: 'last_activity',
        header: 'Last Activity',
        cell: ({ row }) => {
          const item = row.original
          const tsSec =
            item.claimed_at ?? item.discovered_at ?? item.committed_at ?? null
          return tsSec ? formatLocalDateTime(tsSec * 1000) : '—'
        },
      },
      {
        id: 'actions',
        header: 'Actions',
        cell: ({ row }) => {
          const item = row.original
          const isProblemRow =
            item.status === 'dead_letter' || item.status === 'quarantined'
          const rowBusyRetry =
            actionBusyKey === `retry:${item.service_id}:${item.object_key}`
          if (!isProblemRow) {
            return <span className="text-muted-foreground">—</span>
          }
          return (
            <div className="inline-flex items-center gap-1.5">
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="h-7 px-2 text-xs gap-1"
                disabled={actionBusyKey !== null}
                data-testid="admin-queue-ledger-retry-row"
                onClick={() => void onRetry(item)}
              >
                <RotateCcw
                  className={`h-3 w-3 ${rowBusyRetry ? 'animate-spin' : ''}`}
                />
                Retry
              </Button>
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="h-7 px-2 text-xs gap-1 text-red-600 dark:text-red-400 hover:text-red-700"
                disabled={actionBusyKey !== null}
                data-testid="admin-queue-ledger-purge-row"
                onClick={() => void onPurge(item)}
              >
                <Trash2 className="h-3 w-3" />
                Dismiss
              </Button>
            </div>
          )
        },
      },
    ],
    [actionBusyKey, onPurge, onRetry],
  )

  const table = useReactTable({
    data: items,
    columns,
    getRowId: (row) => `${row.service_id}:${row.object_key}`,
    getCoreRowModel: getCoreRowModel(),
  })

  return (
    <div
      className="max-h-[380px] overflow-auto rounded-md border border-border bg-card"
      data-testid="admin-queue-ledger-entries"
    >
      <Table className="w-full text-xs border-collapse">
        <TableHeader className="bg-muted/50 text-muted-foreground sticky top-0 z-10 border-b">
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <TableHead
                  key={header.id}
                  className={
                    header.column.id === 'actions'
                      ? 'text-right font-medium px-3 py-2 whitespace-nowrap'
                      : 'text-left font-medium px-3 py-2 whitespace-nowrap'
                  }
                >
                  {header.isPlaceholder
                    ? null
                    : flexRender(
                        header.column.columnDef.header,
                        header.getContext(),
                      )}
                </TableHead>
              ))}
            </TableRow>
          ))}
        </TableHeader>
        <TableBody className="divide-y divide-border">
          {table.getRowModel().rows.map((row) => (
            <TableRow
              key={row.id}
              data-testid="admin-queue-ledger-row"
              className="hover:bg-muted/30 align-top"
            >
              {row.getVisibleCells().map((cell) => (
                <TableCell
                  key={cell.id}
                  className={
                    cell.column.id === 'object_key'
                      ? 'px-3 py-2.5 min-w-[280px]'
                      : cell.column.id === 'last_activity'
                        ? 'px-3 py-2.5 whitespace-nowrap text-muted-foreground'
                        : cell.column.id === 'actions'
                          ? 'px-3 py-2.5 whitespace-nowrap text-right'
                          : 'px-3 py-2.5 whitespace-nowrap'
                  }
                >
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  )
}

export function IngestLedgerSection({
  isLoading,
  isStatusRefetching,
  error,
  deploymentMode,
  ledger,
  ledgerFilter,
  setLedgerFilter,
  actionNotice,
  setActionNotice,
  shouldInspectLedger,
  problemCount,
  deadLetterCount,
  quarantinedCount,
  actionBusyKey,
  handleBulkRetry,
  handleBulkPurge,
  ledgerError,
  ledgerLoading,
  ledgerItems,
  handleRetryItem,
  handlePurgeItem,
}: {
  isLoading: boolean
  isStatusRefetching: boolean
  error: unknown
  deploymentMode?: string | null
  ledger?: Record<string, number> | null
  ledgerFilter: LedgerStatusFilter
  setLedgerFilter: React.Dispatch<React.SetStateAction<LedgerStatusFilter>>
  actionNotice: { kind: 'success' | 'error'; message: string } | null
  setActionNotice: React.Dispatch<
    React.SetStateAction<{ kind: 'success' | 'error'; message: string } | null>
  >
  shouldInspectLedger: boolean
  problemCount: number
  deadLetterCount: number
  quarantinedCount: number
  actionBusyKey: string | null
  handleBulkRetry: () => Promise<void>
  handleBulkPurge: () => Promise<void>
  ledgerError: unknown
  ledgerLoading: boolean
  ledgerItems?: LedgerEntry[]
  handleRetryItem: (entry: LedgerEntry) => Promise<void>
  handlePurgeItem: (entry: LedgerEntry) => Promise<void>
}) {
  return (
    <div
      data-testid="admin-queue-ledger-reservation"
      className="min-h-[240px] [contain-intrinsic-size:auto_240px] [content-visibility:auto]"
    >
      <AnalyticsCard
        title="Ingest Ledger Status"
        helpTitle="Ingest Ledger Status"
        className="min-h-[240px]"
        isLoading={isLoading}
        isFetching={isStatusRefetching}
        loadingMessage="Loading queue telemetry..."
        error={error as AnalyticsCardError | null}
        helpContent={
          <div className="space-y-4 text-sm">
            <p>
              Shows the global sum of tracked file ingestion states across all services in
              the PostgreSQL ingest_ledger table.
            </p>
            <ul className="list-disc pl-5 space-y-2">
              <li>
                <strong>DISCOVERED:</strong> Found in FOS, waiting to be claimed by a
                worker.
              </li>
              <li>
                <strong>CLAIMED:</strong> Currently being downloaded, transformed, or
                queued for commit.
              </li>
              <li>
                <strong>COMMITTED:</strong> Fully ingested into the long-term{' '}
                {deploymentMode === 'high_scale' ? 'ClickHouse' : 'DuckLake'} table.
              </li>
              <li>
                <strong>QUARANTINED:</strong> Skipped due to corruption or parsing errors.
              </li>
              <li>
                <strong>DEAD_LETTER:</strong> Failed all retries and needs operator
                attention.
              </li>
            </ul>
          </div>
        }
      >
        {!ledger || Object.keys(ledger).length === 0 ? (
          <p className="text-sm text-muted-foreground">Ledger is empty.</p>
        ) : (
          <div className="space-y-4">
            <div className="flex flex-wrap gap-4" data-testid="admin-queue-ledger-cards">
              {Object.entries(ledger).map(([status, count]) => {
                const isDeadLetter = status === 'dead_letter' && count > 0
                const isQuarantined = status === 'quarantined' && count > 0
                const isSelected = ledgerFilter === status
                const validFilterStatus = (
                  VALID_LEDGER_STATUSES as readonly string[]
                ).includes(status)
                return (
                  <button
                    type="button"
                    key={status}
                    data-testid={`admin-queue-ledger-${status}`}
                    onClick={() => {
                      if (validFilterStatus) {
                        setLedgerFilter((prev) =>
                          prev === status ? 'problem' : (status as LedgerStatusFilter),
                        )
                      }
                    }}
                    className={[
                      'flex flex-col items-center justify-center p-4 border rounded-lg bg-card min-w-[120px] transition-colors text-left cursor-pointer hover:border-foreground/30',
                      isDeadLetter
                        ? 'border-red-500/40 bg-red-500/5 text-red-600 dark:text-red-400'
                        : isQuarantined
                          ? 'border-amber-500/40 bg-amber-500/5 text-amber-600 dark:text-amber-400'
                          : '',
                      isSelected ? 'ring-2 ring-primary/40 border-primary/50' : '',
                    ]
                      .filter(Boolean)
                      .join(' ')}
                  >
                    <span className="text-3xl font-bold tabular-nums">{count}</span>
                    <span className="text-xs text-muted-foreground uppercase mt-1">
                      {status}
                    </span>
                  </button>
                )
              })}
            </div>

            {actionNotice ? (
              <div
                role="status"
                data-testid="admin-queue-ledger-notice"
                className={`flex items-center justify-between gap-2 rounded-md border px-3 py-2 text-xs ${
                  actionNotice.kind === 'error'
                    ? 'border-red-500/40 bg-red-500/10 text-red-600 dark:text-red-400'
                    : 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300'
                }`}
              >
                <span>{actionNotice.message}</span>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-6 px-2 text-xs"
                  onClick={() => setActionNotice(null)}
                >
                  Dismiss
                </Button>
              </div>
            ) : null}

            {!shouldInspectLedger ? (
              <div className="flex flex-wrap items-center justify-between gap-2 rounded-md border border-border/60 bg-muted/20 px-3 py-2 text-xs text-muted-foreground">
                <span className="flex items-center gap-1.5">
                  <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                  No dead-letter or quarantined files requiring operator attention. Click
                  any status card above to inspect ledger rows.
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  className="h-7 text-xs"
                  data-testid="admin-queue-ledger-inspect-all-btn"
                  onClick={() => setLedgerFilter('all')}
                >
                  Inspect Recent Entries
                </Button>
              </div>
            ) : (
              <div
                data-testid="admin-queue-ledger-inspector"
                className="rounded-lg border border-border bg-muted/10 p-3 space-y-3"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div className="flex flex-wrap items-center gap-1.5">
                    <span className="text-xs font-medium text-muted-foreground mr-1 flex items-center gap-1">
                      {problemCount > 0 ? (
                        <AlertTriangle className="h-3.5 w-3.5 text-amber-500" />
                      ) : null}
                      Filter:
                    </span>
                    {(
                      [
                        ['problem', `Needs Attention (${problemCount})`],
                        ['dead_letter', `Dead Letter (${deadLetterCount})`],
                        ['quarantined', `Quarantined (${quarantinedCount})`],
                        ['all', 'All Recent'],
                      ] as const
                    ).map(([filterKey, label]) => (
                      <Button
                        key={filterKey}
                        type="button"
                        variant={ledgerFilter === filterKey ? 'secondary' : 'ghost'}
                        size="sm"
                        data-testid={`admin-queue-ledger-filter-${filterKey}`}
                        className="h-7 px-2.5 text-xs"
                        onClick={() => setLedgerFilter(filterKey)}
                      >
                        {label}
                      </Button>
                    ))}
                  </div>

                  {problemCount > 0 &&
                  (ledgerFilter === 'problem' ||
                    ledgerFilter === 'dead_letter' ||
                    ledgerFilter === 'quarantined') ? (
                    <div className="flex flex-wrap items-center gap-2">
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        className="h-7 gap-1 text-xs"
                        disabled={actionBusyKey !== null}
                        data-testid="admin-queue-ledger-retry-all"
                        onClick={() => void handleBulkRetry()}
                      >
                        <RotateCcw className="h-3 w-3" /> Retry All
                      </Button>
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        className="h-7 gap-1 text-xs text-red-600 dark:text-red-400 hover:text-red-700"
                        disabled={actionBusyKey !== null}
                        data-testid="admin-queue-ledger-purge-all"
                        onClick={() => void handleBulkPurge()}
                      >
                        <Trash2 className="h-3 w-3" /> Dismiss All
                      </Button>
                    </div>
                  ) : null}
                </div>

                {ledgerError ? (
                  <p role="alert" className="text-xs text-red-500">
                    Failed to load ledger entries: {(ledgerError as Error).message}
                  </p>
                ) : ledgerLoading ? (
                  <div className="space-y-2 py-2" data-testid="admin-queue-ledger-loading">
                    <Skeleton className="h-10 w-full" />
                    <Skeleton className="h-10 w-full" />
                  </div>
                ) : !ledgerItems || ledgerItems.length === 0 ? (
                  <p className="text-xs text-muted-foreground py-2">
                    No ingest ledger entries match filter{' '}
                    <span className="font-mono">{ledgerFilter}</span>.
                  </p>
                ) : (
                  <LedgerEntriesTable
                    items={ledgerItems}
                    actionBusyKey={actionBusyKey}
                    onRetry={handleRetryItem}
                    onPurge={handlePurgeItem}
                  />
                )}
              </div>
            )}
          </div>
        )}
      </AnalyticsCard>
    </div>
  )
}
