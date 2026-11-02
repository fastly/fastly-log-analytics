'use client'

import React, { useMemo } from 'react'
import { flexRender, getCoreRowModel, useReactTable, type ColumnDef } from '@tanstack/react-table'
import { Database, DollarSign, HardDrive, Zap } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { StatCard } from '@/components/ui/confirm-dialog'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableRow } from '@/components/ui/table'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import {
  buildServiceHref,
  cn,
  formatAbbreviatedCount,
  formatBytes,
  formatCount,
  formatUSD,
} from '@/lib/utils'
import type { operations } from '@/types/api.generated'

export type StorageUsageData =
  operations['usage_current_storage_api_usage_current_storage_get']['responses'][200]['content']['application/json']

export type BreakdownRow = {
  id: string
  label: React.ReactNode
  value: React.ReactNode
  rowClassName?: string
}

const BREAKDOWN_COLUMNS: ColumnDef<BreakdownRow>[] = [
  { id: 'label', cell: ({ row }) => row.original.label },
  { id: 'value', cell: ({ row }) => <div className="text-right">{row.original.value}</div> },
]

export function BreakdownTable({ rows }: { rows: BreakdownRow[] }) {
  const table = useReactTable({
    data: rows,
    columns: BREAKDOWN_COLUMNS,
    getCoreRowModel: getCoreRowModel(),
  })
  return (
    <Table className="mt-1 text-[11px] font-normal">
      <TableBody>
        {table.getRowModel().rows.map((row) => (
          <TableRow
            key={row.original.id}
            className={cn('border-0 hover:bg-transparent', row.original.rowClassName)}
          >
            {row.getVisibleCells().map((cell) => (
              <TableCell key={cell.id} className="p-0 py-0.5 align-top">
                {flexRender(cell.column.columnDef.cell, cell.getContext())}
              </TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
    </Table>
  )
}

export function UsageStatCards({
  storage,
  loadingStorage,
  errorStorage,
  loadingOps,
  loadingBw,
  isFetchingAny,
  isLoadingInitial,
  totalClassA,
  totalClassB,
  totalBwGB,
  rateA,
  rateB,
  rateBPer10k,
  rateEgress,
  rateStorage,
  activeServiceId,
  onNavigateToAdmin,
}: {
  storage: StorageUsageData | undefined
  loadingStorage: boolean
  errorStorage: Error | null | undefined | boolean
  loadingOps: boolean
  loadingBw: boolean
  isFetchingAny: boolean
  isLoadingInitial: boolean
  totalClassA: number
  totalClassB: number
  totalBwGB: number
  rateA: number
  rateB: number
  rateBPer10k: number
  rateEgress: number
  rateStorage: number
  activeServiceId: string | null
  onNavigateToAdmin: (href: string) => void
}) {
  const roughCostA = (totalClassA / 1000) * rateA
  const roughCostB = (totalClassB / 1000) * rateB
  const roughStorage = ((storage?.total_billed_gb_hours ?? 0) / 720) * rateStorage
  const roughBandwidth = totalBwGB * rateEgress
  const roughTotal = roughCostA + roughCostB + roughStorage + roughBandwidth
  const fmtUSD = (n: number) => formatUSD(n, { subDollarDigits: 2 })

  const storageBreakdownRows = useMemo<BreakdownRow[]>(
    () => [
      {
        id: 'live',
        label: <span>Live Storage:</span>,
        value: <strong className="text-foreground">{formatBytes(storage?.live_bytes ?? 0)}</strong>,
      },
      ...((storage?.rum_bytes ?? 0) > 0
        ? [
            {
              id: 'rum',
              rowClassName: 'text-blue-700 dark:text-blue-400',
              label: <span>RUM Logs:</span>,
              value: <strong>{formatBytes(storage?.rum_bytes ?? 0)}</strong>,
            },
          ]
        : []),
      ...((storage?.regular_log_bytes ?? 0) > 0
        ? [
            {
              id: 'regular',
              label: <span>Request Logs:</span>,
              value: (
                <strong className="text-foreground">
                  {formatBytes(storage?.regular_log_bytes ?? 0)}
                </strong>
              ),
            },
          ]
        : []),
      ...((storage?.archive_bytes ?? 0) > 0
        ? [
            {
              id: 'archive',
              rowClassName: 'text-blue-700 dark:text-blue-400',
              label: <span>High-Scale Archive:</span>,
              value: (
                <strong>
                  {formatBytes(storage?.archive_bytes ?? 0)}{' '}
                  <span className="font-normal text-muted-foreground">
                    ({formatCount(storage?.archive_objects ?? 0)} objects)
                  </span>
                </strong>
              ),
            },
          ]
        : []),
      ...((storage?.quarantine_bytes ?? 0) > 0
        ? [
            {
              id: 'quarantine',
              label: <span>Quarantine:</span>,
              value: (
                <strong className="text-foreground">
                  {formatBytes(storage?.quarantine_bytes ?? 0)}
                </strong>
              ),
            },
          ]
        : []),
      {
        id: 'deleted',
        rowClassName: 'text-amber-700 dark:text-amber-500',
        label: (
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger>
                <span className=" underline underline-offset-2 decoration-dotted">
                  Deleted (Still Billed):
                </span>
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-[280px] text-xs">
                Objects deleted early (raw logs after ingest), data files the cloud copy or
                compaction removed from Fastly Object Storage, and High-Scale archive artifacts the
                worker expired in the last 30 days (past the service&apos;s retention). Fastly
                Object Storage bills every object for a minimum of 30 days.
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        ),
        value: <strong>{formatBytes(storage?.deleted_bytes ?? 0)}</strong>,
      },
      ...((storage?.cloud_copy_deleted_bytes ?? 0) > 0
        ? [
            {
              id: 'deleted-cloud-copy',
              rowClassName: 'text-amber-700/80 dark:text-amber-500/80',
              label: <span>↳ Cloud copy &amp; compaction:</span>,
              value: <strong>{formatBytes(storage?.cloud_copy_deleted_bytes ?? 0)}</strong>,
            },
          ]
        : []),
      ...((storage?.archive_expired_bytes ?? 0) > 0
        ? [
            {
              id: 'deleted-archive',
              rowClassName: 'text-amber-700/80 dark:text-amber-500/80',
              label: <span>↳ Expired archive:</span>,
              value: <strong>{formatBytes(storage?.archive_expired_bytes ?? 0)}</strong>,
            },
          ]
        : []),
    ],
    [
      storage?.live_bytes,
      storage?.rum_bytes,
      storage?.regular_log_bytes,
      storage?.quarantine_bytes,
      storage?.deleted_bytes,
      storage?.cloud_copy_deleted_bytes,
      storage?.archive_expired_bytes,
      storage?.archive_bytes,
      storage?.archive_objects,
    ],
  )

  const costBreakdownRows = useMemo<BreakdownRow[]>(
    () => [
      {
        id: 'storage',
        label: <span>Storage:</span>,
        value: (
          <div className="flex flex-col items-end">
            {errorStorage ? (
              <strong className="text-destructive">n/a</strong>
            ) : (
              <strong className="text-foreground">{fmtUSD(roughStorage)}</strong>
            )}
            <span className="text-[9px] uppercase tracking-tighter text-muted-foreground">
              Rate: ${rateStorage}/GB
            </span>
          </div>
        ),
      },
      {
        id: 'ops',
        label: <span>Operations:</span>,
        value: (
          <div className="flex flex-col items-end">
            <strong className="text-foreground">{fmtUSD(roughCostA + roughCostB)}</strong>
            <span className="text-[9px] uppercase tracking-tighter text-muted-foreground">
              A: ${rateA}/1K · B: ${rateBPer10k}/10K
            </span>
          </div>
        ),
      },
      {
        id: 'egress',
        label: <span>CDN Egress:</span>,
        value: (
          <div className="flex flex-col items-end">
            <strong className="text-foreground">{fmtUSD(roughBandwidth)}</strong>
            <span className="text-[9px] uppercase tracking-tighter text-muted-foreground">
              Rate: ${rateEgress}/GB
            </span>
          </div>
        ),
      },
    ],
    [
      errorStorage,
      roughStorage,
      rateStorage,
      roughCostA,
      roughCostB,
      rateA,
      rateBPer10k,
      roughBandwidth,
      rateEgress,
    ],
  )

  return (
    <div
      className={cn(
        'grid grid-cols-2 lg:grid-cols-4 gap-4 transition-opacity duration-100 min-h-[140px] [contain-intrinsic-size:140px] [content-visibility:auto]',
        isFetchingAny && !isLoadingInitial && 'opacity-40 pointer-events-none',
      )}
    >
      <StatCard
        title="Storage Impact (Period)"
        value={
          errorStorage ? (
            <span className="text-muted-foreground">—</span>
          ) : (
            <span className="text-green-600">
              {(storage?.total_billed_gb_hours ?? 0).toFixed(2)} GB-hrs
            </span>
          )
        }
        sub={
          <div className="min-h-[96px]">
            {errorStorage ? (
              <span role="alert" className="text-[11px] text-destructive">
                Failed to load storage usage.
              </span>
            ) : loadingStorage ? (
              <div className="flex flex-col gap-1.5 mt-2">
                <Skeleton className="h-3 w-full" />
                <Skeleton className="h-3 w-full" />
              </div>
            ) : (
              <BreakdownTable rows={storageBreakdownRows} />
            )}
          </div>
        }
        icon={HardDrive}
        loading={loadingStorage}
        tooltip="Estimated minimum billed storage hours for objects created in this period."
      />
      <StatCard
        title="Class A Ops (period)"
        value={formatAbbreviatedCount(totalClassA)}
        sub={
          <div className="min-h-[96px] space-y-1">
            <div>Writes, lists, deletes</div>
            <div className="flex items-center justify-between">
              <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
                Rate: ${rateA}/1k
              </span>
              <Button
                variant="link"
                size="sm"
                className="h-auto p-0 text-[10px] font-bold text-primary"
                onClick={() => onNavigateToAdmin(buildServiceHref('/admin', activeServiceId))}
              >
                Edit
              </Button>
            </div>
          </div>
        }
        icon={Zap}
        loading={loadingOps}
        tooltip="Writes (log uploads) and lists (checking for new logs). Note: This is an account-wide total."
      />
      <StatCard
        title="Class B Ops (period)"
        value={formatAbbreviatedCount(totalClassB)}
        sub={
          <div className="min-h-[96px] space-y-1">
            <div>Reads / downloads</div>
            <div className="flex items-center justify-between">
              <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
                Rate: ${rateBPer10k}/10k
              </span>
              <Button
                variant="link"
                size="sm"
                className="h-auto p-0 text-[10px] font-bold text-primary"
                onClick={() => onNavigateToAdmin(buildServiceHref('/admin', activeServiceId))}
              >
                Edit
              </Button>
            </div>
          </div>
        }
        icon={Database}
        loading={loadingOps}
        tooltip="Reads (downloading logs for processing). Note: This is an account-wide total."
      />
      <StatCard
        title="Estimated Incurred Cost"
        value={fmtUSD(roughTotal)}
        sub={
          <div className="min-h-[96px]">
            {loadingOps || loadingStorage || loadingBw ? (
              <div className="flex flex-col gap-1.5 mt-2">
                <Skeleton className="h-3 w-full" />
                <Skeleton className="h-3 w-full" />
                <Skeleton className="h-3 w-full" />
              </div>
            ) : (
              <div>
                <BreakdownTable rows={costBreakdownRows} />
                {errorStorage && (
                  <div role="alert" className="text-[10px] text-destructive mt-0.5">
                    Partial estimate — storage cost excluded.
                  </div>
                )}
              </div>
            )}
          </div>
        }
        icon={DollarSign}
        loading={loadingOps || loadingStorage || loadingBw}
        tooltip="Total estimated cost incurred for the selected period (Storage, Ops, Egress). Note: Operations are account-wide totals."
      />
    </div>
  )
}
