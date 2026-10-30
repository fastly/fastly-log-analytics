'use client'

import React from 'react'
import { Database, Zap, Globe, DollarSign } from 'lucide-react'
import type { ColumnDef } from '@tanstack/react-table'
import { formatAbbreviatedCount, formatBytes, formatCount } from '@/lib/utils'
import type { components } from '@/types/api'
import { Button } from '@/components/ui/button'
import { StatCard } from '@/components/ui/confirm-dialog'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

export type UsageLogResponse = components['schemas']['UsageLogResponse']

export type UsageLogEntry = components['schemas']['UsageLogEntry'] & {
  service_id: string | null
}

export const DATE_PRESETS = [
  { label: 'Last 1h', hours: 1 },
  { label: 'Last 24h', hours: 24 },
  { label: 'Last 7d', hours: 168 },
  { label: 'Last 30d', hours: 720 },
]

export function fmtCost(n: number): string {
  if (n === 0) return '$0.000000'
  if (n < 0.000001) return `$${n.toExponential(2)}`
  return `$${n.toFixed(6)}`
}

const monoMutedCell = <K extends keyof UsageLogEntry>(key: K) =>
  function MonoMutedCell({ row }: { row: { original: UsageLogEntry } }) {
    return (
      <span className="font-mono text-xs text-muted-foreground">{(row.original[key] as string | null) ?? '—'}</span>
    )
  }

export function buildUsageLogColumns(full: (ts: string) => string): ColumnDef<UsageLogEntry>[] {
  return [
    {
      accessorKey: 'timestamp',
      header: 'Timestamp',
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground whitespace-nowrap">{full(row.original.timestamp)}</span>
      ),
    },
    { accessorKey: 'service_id', header: 'Service', cell: monoMutedCell('service_id') },
    {
      accessorKey: 'operation_class',
      header: 'Class',
      cell: ({ row }) => {
        const cls = row.original.operation_class
        if (!cls) return <span className="text-muted-foreground text-xs">—</span>
        const variant = cls === 'A' ? 'default' : cls === 'B' ? 'secondary' : 'outline'
        return (
          <Badge variant={variant} className="text-[10px] px-1.5 py-0 font-mono">
            {cls === 'CDN' ? 'CDN' : `FOS ${cls}`}
          </Badge>
        )
      },
    },
    {
      accessorKey: 'operation_type',
      header: 'Operation',
      cell: ({ row }) => <span className="font-mono text-xs">{row.original.operation_type ?? '—'}</span>,
    },
    { accessorKey: 'url', header: 'URL / Path', cell: monoMutedCell('url') },
    {
      accessorKey: 'bytes',
      header: 'Bytes',
      cell: ({ row }) =>
        row.original.bytes != null ? (
          <span className="font-mono text-xs tabular-nums">{formatBytes(row.original.bytes)}</span>
        ) : (
          <span className="text-muted-foreground text-xs">—</span>
        ),
    },
    { accessorKey: 'function_name', header: 'Function', cell: monoMutedCell('function_name') },
    { accessorKey: 'process_context', header: 'Process', cell: monoMutedCell('process_context') },
    {
      accessorKey: 'status',
      header: 'Status',
      cell: ({ row }) => {
        const s = row.original.status
        return (
          <Badge variant={s === 'OK' ? 'secondary' : 'destructive'} className="text-[10px] px-1.5 py-0">
            {s ?? '—'}
          </Badge>
        )
      },
    },
    {
      accessorKey: 'estimated_cost',
      header: 'Est. Cost',
      cell: ({ row }) =>
        row.original.estimated_cost != null ? (
          <span className="font-mono text-xs tabular-nums">{fmtCost(row.original.estimated_cost)}</span>
        ) : (
          <span className="text-muted-foreground text-xs">—</span>
        ),
    },
  ]
}

export interface UsageLogFiltersProps {
  preset: number
  setPreset: (n: number) => void
  usageType: string
  setUsageType: (v: string) => void
  operationFilter: string
  setOperationFilter: (v: string) => void
  processFilter: string
  setProcessFilter: (v: string) => void
  isFetching: boolean
  isLoading: boolean
}

export function UsageLogFilters({
  preset,
  setPreset,
  usageType,
  setUsageType,
  operationFilter,
  setOperationFilter,
  processFilter,
  setProcessFilter,
  isFetching,
  isLoading,
}: UsageLogFiltersProps) {
  return (
    <div data-testid="admin-usage-log-filters" className="flex flex-wrap items-center gap-3 px-4 py-3 border-b">
      <div className="flex items-center gap-1.5">
        {DATE_PRESETS.map((p) => (
          <Button
            key={p.hours}
            data-testid={`admin-usage-log-preset-${p.hours}h`}
            aria-pressed={preset === p.hours}
            size="sm"
            variant={preset === p.hours ? 'default' : 'outline'}
            className="h-7 px-3 text-xs"
            onClick={() => setPreset(p.hours)}
          >
            {p.label}
          </Button>
        ))}
      </div>

      <div className="flex items-center gap-1.5">
        <Label htmlFor="usage-log-type" className="text-xs text-muted-foreground shrink-0">
          Type
        </Label>
        <Select value={usageType || 'all'} onValueChange={(v) => setUsageType(!v || v === 'all' ? '' : v)}>
          <SelectTrigger id="usage-log-type" data-testid="admin-usage-log-type-select" className="h-7 text-xs w-32">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {[
              ['all', 'All'],
              ['FOS', 'FOS (A+B)'],
              ['FOS-A', 'FOS Class A'],
              ['FOS-B', 'FOS Class B'],
              ['CDN', 'CDN'],
            ].map(([val, label]) => (
              <SelectItem key={val} value={val} className="text-xs">
                {label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="flex items-center gap-1.5">
        <Label htmlFor="usage-log-operation" className="text-xs text-muted-foreground shrink-0">
          Operation
        </Label>
        <Input
          id="usage-log-operation"
          data-testid="admin-usage-log-operation-input"
          className="h-7 text-xs w-40 font-mono"
          placeholder="e.g. GET_OBJECT"
          value={operationFilter}
          onChange={(e) => setOperationFilter(e.target.value)}
        />
      </div>

      <div className="flex items-center gap-1.5">
        <Label htmlFor="usage-log-process" className="text-xs text-muted-foreground shrink-0">
          Process
        </Label>
        <Input
          id="usage-log-process"
          data-testid="admin-usage-log-process-input"
          className="h-7 text-xs w-44 font-mono"
          placeholder="e.g. cron:sync"
          value={processFilter}
          onChange={(e) => setProcessFilter(e.target.value)}
        />
      </div>

      {isFetching && !isLoading && (
        <span data-testid="admin-usage-log-refreshing-badge" className="text-xs text-muted-foreground animate-pulse">
          Refreshing…
        </span>
      )}
    </div>
  )
}

function renderBreakdown(breakdown: Record<string, number> | undefined) {
  if (!breakdown || Object.keys(breakdown).length === 0) return null
  return (
    <div className="mt-1.5 pt-1.5 border-t border-border/50 space-y-0.5">
      {Object.entries(breakdown)
        .sort((a, b) => b[1] - a[1])
        .map(([op, count]) => (
          <div
            key={op}
            className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground"
          >
            <span className="truncate mr-2">{op}</span>
            <span className="font-mono">{formatCount(count)}</span>
          </div>
        ))}
    </div>
  )
}

interface UsageLogKpiCardsProps {
  agg: UsageLogResponse['aggregate'] | undefined
  isLoading: boolean
  isRefreshing: boolean
}

export function UsageLogKpiCards({ agg, isLoading, isRefreshing }: UsageLogKpiCardsProps) {
  return (
    <div
      data-testid="admin-usage-log-kpi-reservation"
      className={`grid grid-cols-2 md:grid-cols-4 gap-3 min-h-[216px] [contain-intrinsic-size:auto_164px] [content-visibility:visible] transition-opacity ${
        isRefreshing ? 'opacity-40 pointer-events-none' : ''
      }`}
    >
      <StatCard
        title="FOS Class A Ops"
        value={agg ? formatAbbreviatedCount(agg.total_class_a) : '—'}
        icon={Zap}
        iconClassName="text-blue-500"
        className="h-[216px] overflow-hidden"
        sub={
          <div className="space-y-1">
            <div>{agg ? fmtCost(agg.estimated_cost_class_a) : '—'}</div>
            {renderBreakdown(agg?.class_a_breakdown)}
          </div>
        }
        loading={isLoading}
      />
      <StatCard
        title="FOS Class B Ops"
        value={agg ? formatAbbreviatedCount(agg.total_class_b) : '—'}
        icon={Database}
        iconClassName="text-green-500"
        className="h-[216px] overflow-hidden"
        sub={
          <div className="space-y-1">
            <div>{agg ? fmtCost(agg.estimated_cost_class_b) : '—'}</div>
            {renderBreakdown(agg?.class_b_breakdown)}
          </div>
        }
        loading={isLoading}
      />
      <StatCard
        title="CDN Egress"
        value={agg ? formatBytes(agg.total_cdn_bytes) : '—'}
        icon={Globe}
        iconClassName="text-purple-500"
        className="h-[216px] overflow-hidden"
        sub={
          <div className="space-y-1">
            <div>{agg ? fmtCost(agg.estimated_cost_cdn) : '—'}</div>
            {agg && (
              <div className="mt-1.5 pt-1.5 border-t border-border/50 space-y-0.5">
                <div className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground">
                  <span className="truncate mr-2">Requests</span>
                  <span className="font-mono">{formatCount(agg.total_cdn_downloads)}</span>
                </div>
              </div>
            )}
          </div>
        }
        loading={isLoading}
      />
      <StatCard
        title="Est. Total Cost"
        value={agg ? `$${agg.estimated_cost_total.toFixed(2)}` : '—'}
        icon={DollarSign}
        iconClassName="text-amber-500"
        className="h-[216px] overflow-hidden"
        sub={
          <div className="space-y-1">
            <div>for selected period</div>
            {agg && (
              <div className="mt-1.5 pt-1.5 border-t border-border/50 space-y-0.5">
                {[
                  ['FOS Class A', agg.estimated_cost_class_a],
                  ['FOS Class B', agg.estimated_cost_class_b],
                  ['CDN Egress', agg.estimated_cost_cdn],
                ].map(([lbl, cost]) => (
                  <div
                    key={lbl as string}
                    className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground"
                  >
                    <span className="truncate mr-2">{lbl}</span>
                    <span className="font-mono">{fmtCost(cost as number)}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        }
        loading={isLoading}
      />
    </div>
  )
}
