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
import type { operations } from '@/types/api.generated'

const formatVcpu = (ms: number) => (ms < 1000 ? `${Math.round(ms)}ms` : formatDuration(ms / 1000))

export type StorageUsageData =
  operations['usage_current_storage_api_usage_current_storage_get']['responses'][200]['content']['application/json'] & {
    storage_billing_complete?: boolean
    storage_billing_provenance?: string[]
    live_billed_gb_hours?: number
    deleted_billed_gb_hours?: number
    minimum_billed_gb_hours?: number
    /** Cloud: every product the install uses, priced at the service's configured rates (`incurredCost`). */
    incurred?: { total: number; lines: IncurredCostLine[] }
  }

export type IncurredCostLine = {
  id: string
  label: string
  /** USD, or null when the quantity is unavailable. */
  usd: number | null
  detail: string
  source: 'fastly' | 'logged' | 'none'
}

const INCURRED_SOURCE_LABEL: Record<IncurredCostLine['source'], string> = {
  fastly: 'Fastly stats',
  logged: 'Measured · this install',
  none: 'Unavailable',
}

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
  errorOps,
  loadingBw,
  errorBw,
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
  localStorageGbHours,
  storageBillingComplete,
  storageBillingProvenance,
  operationsSource,
  operationsComplete,
}: {
  storage: StorageUsageData | undefined
  loadingStorage: boolean
  errorStorage: Error | null | undefined | boolean
  loadingOps: boolean
  errorOps: Error | null | undefined | boolean
  loadingBw: boolean
  errorBw: Error | null | undefined | boolean
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
  localStorageGbHours?: number | null
  storageBillingComplete?: boolean
  storageBillingProvenance?: string[]
  operationsSource?: string
  operationsComplete?: boolean
}) {
  const deletedBytesComplete = storage?.deleted_bytes_complete
  const deletedBytesProvenance = storage?.deleted_bytes_provenance
  const resolvedLocalGbHours = localStorageGbHours ?? storage?.total_billed_gb_hours ?? null
  const billedGbHours = resolvedLocalGbHours ?? 0
  const storageIncomplete = (storageBillingComplete ?? storage?.storage_billing_complete) === false
  const storageProvenance = storageBillingProvenance ?? storage?.storage_billing_provenance
  const opsIncomplete = operationsComplete === false
  const opsUnavailable = Boolean(errorOps) || opsIncomplete
  const roughCostA = opsUnavailable ? 0 : (totalClassA / 1000) * rateA
  const roughCostB = opsUnavailable ? 0 : (totalClassB / 1000) * rateB
  const roughStorage = errorStorage ? 0 : (billedGbHours / 720) * rateStorage
  const roughBandwidth = errorBw ? 0 : totalBwGB * rateEgress
  // Cloud prices every product (storage, operations, delivery, Compute, KV, secrets) in one table;
  // operations still follow this card's own availability.
  const pricedLines = errorStorage
    ? null
    : storage?.incurred?.lines.map((line) => (line.id === 'ops' && opsUnavailable ? { ...line, usd: null } : line)) ?? null
  const roughTotal = pricedLines
    ? pricedLines.reduce((sum, line) => sum + (line.usd ?? 0), 0)
    : roughCostA + roughCostB + roughStorage + roughBandwidth
  const excludedCosts = pricedLines
    ? pricedLines.filter((line) => line.usd === null).map((line) => line.label)
    : [
        errorStorage ? 'storage' : null,
        opsUnavailable ? 'operations' : null,
        errorBw ? 'CDN egress' : null,
      ].filter((cost): cost is string => Boolean(cost))
  const hasPartialCost = excludedCosts.length > 0
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
    () => pricedLines ? pricedLines.map((line) => ({
      id: line.id,
      label: <span>{line.label}:</span>,
      value: (
        <div className="flex flex-col items-end">
          {line.usd === null ? (
            <strong className="text-muted-foreground">n/a</strong>
          ) : (
            <strong className="text-foreground">{fmtUSD(line.usd)}</strong>
          )}
          <span className="text-[9px] uppercase tracking-tighter text-muted-foreground">{line.detail}</span>
          <span className="text-[9px] uppercase tracking-tighter text-muted-foreground">
            {INCURRED_SOURCE_LABEL[line.source]}
          </span>
        </div>
      ),
    })) : [
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
              Measured · this install
            </span>
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
            {errorOps ? (
              <strong className="text-destructive">n/a</strong>
            ) : opsIncomplete ? (
              <strong className="text-muted-foreground">Unavailable</strong>
            ) : (
              <strong className="text-foreground">{fmtUSD(roughCostA + roughCostB)}</strong>
            )}
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
            {errorBw ? (
              <strong className="text-destructive">n/a</strong>
            ) : (
              <strong className="text-foreground">{fmtUSD(roughBandwidth)}</strong>
            )}
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
      roughStorage,
      rateStorage,
      roughCostA,
      roughCostB,
      errorOps,
      opsIncomplete,
      rateA,
      rateBPer10k,
      roughBandwidth,
      errorBw,
      rateEgress,
      storage,
      computeRow,
      pricedLines,
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
              {billedGbHours.toFixed(2)} GB-hrs
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
              <div>
                <BreakdownTable rows={storageBreakdownRows} />
                <div className="mt-1 text-[9px] uppercase tracking-tighter text-muted-foreground">
                  Measured · this install
                </div>
              </div>
            )}
          </div>
        }
        icon={HardDrive}
        loading={loadingStorage}
        tooltip="Selected-period billed storage hours for live and deleted-but-still-billed objects owned by this install."
      />
      <StatCard
        title="Class A Ops (period)"
        value={opsUnavailable ? 'Unavailable' : formatAbbreviatedCount(totalClassA)}
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
        tooltip={
          operationsSource === 'fastly'
            ? 'Writes (log uploads) and lists (checking for new logs). Fallback: Fastly account-wide total.'
            : 'Writes (log uploads) and lists (checking for new logs) recorded locally for this install.'
        }
      />
      <StatCard
        title="Class B Ops (period)"
        value={opsUnavailable ? 'Unavailable' : formatAbbreviatedCount(totalClassB)}
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
        tooltip={
          operationsSource === 'fastly'
            ? 'Reads (downloading logs for processing). Fallback: Fastly account-wide total.'
            : 'Reads (downloading logs for processing) recorded locally for this install.'
        }
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
                {hasPartialCost && (
                  <div
                    role={errorStorage || errorOps || errorBw ? 'alert' : 'status'}
                    className="text-[10px] text-destructive mt-0.5"
                  >
                    Partial estimate — {excludedCosts.join(excludedCosts.length > 2 ? ', ' : ' and ')} cost excluded.
                  </div>
                )}
                {storageIncomplete && (
                  <div role="status" className="text-[10px] text-amber-700 dark:text-amber-500 mt-0.5">
                    Partial storage cost — incomplete lifecycle data
                    {storageProvenance && storageProvenance.length > 0
                      ? ` (${storageProvenance.join(', ')})`
                      : ''}.
                  </div>
                )}
              </div>
            )}
          </div>
        }
        icon={DollarSign}
        loading={loadingOps || loadingStorage || loadingBw}
        tooltip={pricedLines
          ? 'Estimated cost for the selected period of every product this install uses, at the service\'s configured rates. Account free tiers are not subtracted.'
          : 'Total estimated cost incurred for the selected period (service-local Storage, Ops, and Egress).'}
      />
    </div>
  )
}
