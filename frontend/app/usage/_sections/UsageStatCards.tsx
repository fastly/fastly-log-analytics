'use client'

import React, { useMemo } from 'react'
import { flexRender, useTable } from '@tanstack/react-table'
import { basicTableFeatures, type BasicColumnDef } from '@/lib/table'
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
  formatDuration,
  formatUSD,
} from '@/lib/utils'
import type { ComputeUsageResponse } from '@/lib/cloud-engine/usage/computeSeries'
import type { components, operations } from '@/types/api.generated'

const formatVcpu = (ms: number) => (ms < 1000 ? `${Math.round(ms)}ms` : formatDuration(ms / 1000))

export type StorageUsageData =
  operations['usage_current_storage_api_usage_current_storage_get']['responses'][200]['content']['application/json']

export type BreakdownRow = {
  id: string
  label: React.ReactNode
  value: React.ReactNode
  rowClassName?: string
}

const BREAKDOWN_COLUMNS: BasicColumnDef<BreakdownRow>[] = [
  { id: 'label', cell: ({ row }) => row.original.label },
  { id: 'value', cell: ({ row }) => <div className="text-right">{row.original.value}</div> },
]

export function BreakdownTable({ rows }: { rows: BreakdownRow[] }) {
  const table = useTable({
    features: basicTableFeatures,
    data: rows,
    columns: BREAKDOWN_COLUMNS,
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
  compute,
  platformStorage,
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
  /** Cloud only: Fastly Platform Metrics Compute for the range; logs are the fallback. */
  compute?: ComputeUsageResponse
  platformStorage?: components['schemas']['PlatformStorageUsage']
}) {
  const observedStorageGbHours =
    platformStorage?.status === 'ok' ? platformStorage.gb_hours ?? null : null
  const deletedBytesComplete = storage?.deleted_bytes_complete
  const deletedBytesProvenance = storage?.deleted_bytes_provenance
  const billedGbHours = observedStorageGbHours ?? storage?.total_billed_gb_hours ?? 0
  const loadingLocalStorage = loadingStorage && observedStorageGbHours == null
  const roughCostA = (totalClassA / 1000) * rateA
  const roughCostB = (totalClassB / 1000) * rateB
  const roughStorage = (billedGbHours / 720) * rateStorage
  const roughBandwidth = totalBwGB * rateEgress
  const roughTotal = roughCostA + roughCostB + roughStorage + roughBandwidth
  const fmtUSD = (n: number) => formatUSD(n, { subDollarDigits: 2 })

  const loggedRequests = ((storage as Record<string, unknown> | undefined)?.compute_requests as number | undefined) ?? 0
  const computeRow = useMemo(
    () =>
      compute?.source === 'fastly'
        ? { source: 'fastly' as const, requests: compute.requests, vcpuMs: compute.vcpu_ms, billedMs: compute.billed_ms }
        : loggedRequests > 0
          ? {
              source: 'logged' as const,
              requests: loggedRequests,
              vcpuMs: ((storage as Record<string, unknown>)?.compute_vcpu_ms as number) ?? 0,
              billedMs: null,
            }
          : null,
    [compute, loggedRequests, storage],
  )
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
        value:
          deletedBytesComplete === false ? (
            <strong title={`Provenance: ${deletedBytesProvenance}`}>Incomplete</strong>
          ) : (
            <strong>{formatBytes(storage?.deleted_bytes ?? 0)}</strong>
          ),
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
      deletedBytesComplete,
      deletedBytesProvenance,
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
            {errorStorage && observedStorageGbHours == null ? (
              <strong className="text-destructive">n/a</strong>
            ) : (
              <strong className="text-foreground">{fmtUSD(roughStorage)}</strong>
            )}
            <span className="text-[9px] uppercase tracking-tighter text-muted-foreground">
              {observedStorageGbHours != null
                ? 'Measured · Fastly Platform (account-wide)'
                : `Rate: $${rateStorage}/GB`}
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
      ...(computeRow
        ? [
            {
              id: 'compute',
              label: <span>Compute &amp; vCPU:</span>,
              value: (
                <div className="flex flex-col items-end">
                  <strong className="text-foreground">
                    {formatAbbreviatedCount(computeRow.requests)} reqs
                  </strong>
                  <span className="text-[9px] uppercase tracking-tighter text-muted-foreground">
                    vCPU: {formatVcpu(computeRow.vcpuMs)}
                    {computeRow.billedMs != null && <> · billed: {formatVcpu(computeRow.billedMs)}</>}
                    {' '}· KV: {formatAbbreviatedCount(((storage as Record<string, unknown>)?.kv_ops as number) ?? 0)}
                  </span>
                  <span
                    className="text-[9px] uppercase tracking-tighter text-muted-foreground"
                    data-testid="compute-source"
                  >
                    {computeRow.source === 'fastly' ? 'Fastly stats · KV logged' : 'logged'}
                  </span>
                </div>
              ),
            },
          ]
        : []),
    ],
    [
      errorStorage,
      observedStorageGbHours,
      roughStorage,
      rateStorage,
      roughCostA,
      roughCostB,
      rateA,
      rateBPer10k,
      roughBandwidth,
      rateEgress,
      storage,
      computeRow,
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
          errorStorage && observedStorageGbHours == null ? (
            <span className="text-muted-foreground">—</span>
          ) : (
            <span className="text-green-600">
              {billedGbHours.toFixed(2)} GB-hrs
            </span>
          )
        }
        sub={
          <div className="min-h-[96px]">
            {errorStorage && observedStorageGbHours == null ? (
              <span role="alert" className="text-[11px] text-destructive">
                Failed to load storage usage.
              </span>
            ) : loadingLocalStorage ? (
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
        loading={loadingLocalStorage}
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
            {loadingOps || loadingLocalStorage || loadingBw ? (
              <div className="flex flex-col gap-1.5 mt-2">
                <Skeleton className="h-3 w-full" />
                <Skeleton className="h-3 w-full" />
                <Skeleton className="h-3 w-full" />
              </div>
            ) : (
              <div>
                <BreakdownTable rows={costBreakdownRows} />
                {errorStorage && observedStorageGbHours == null && (
                  <div role="alert" className="text-[10px] text-destructive mt-0.5">
                    Partial estimate — storage cost excluded.
                  </div>
                )}
              </div>
            )}
          </div>
        }
        icon={DollarSign}
        loading={loadingOps || loadingLocalStorage || loadingBw}
        tooltip="Total estimated cost incurred for the selected period (Storage, Ops, Egress). Note: Operations are account-wide totals."
      />
    </div>
  )
}
