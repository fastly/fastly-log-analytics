'use client'

import React from 'react'
import { Globe, MapPin, Server } from 'lucide-react'
import type { ColumnDef, VisibilityState } from '@tanstack/react-table'
import { useActiveLogFields } from '@/hooks/useBootstrap'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import {
  columnHeader,
  ColumnVisibilityDropdown,
  DataTable,
  FilterValueCell,
  msCell,
  PopLabel,
} from '@/components/DataTable'
import { cn, formatBytes, formatCount } from '@/lib/utils'
import {
  ApproxBadge,
  type OriginIpHealth,
  type OriginIpHealthRow,
  type OriginPopLatency,
  type OriginPopLatencyRow,
  type OriginSlowUrlRow,
  type OriginSlowUrls,
  type OriginSummary,
  type SectionQuery,
} from './Aggregates'

type LatencyRow = { p50_ms?: number | null; p95_ms?: number | null; requests: number }

const requestsCol = <TRow extends LatencyRow>(): ColumnDef<TRow> => ({
  accessorKey: 'requests', id: 'requests', meta: { label: 'Requests' }, header: columnHeader('Reqs'),
  cell: ({ row }) => formatCount(row.original.requests),
})
const p50Col = <TRow extends LatencyRow>(): ColumnDef<TRow> => ({
  accessorKey: 'p50_ms', id: 'p50_ms', meta: { label: 'Median (P50)' }, header: columnHeader('P50'),
  cell: ({ row }) => msCell(row.original.p50_ms),
})
const p95Col = <TRow extends LatencyRow>(): ColumnDef<TRow> => ({
  accessorKey: 'p95_ms', id: 'p95_ms', meta: { label: 'P95 Latency' }, header: columnHeader('P95'),
  cell: ({ row }) => msCell(row.original.p95_ms),
})

const URL_COLUMNS: ColumnDef<OriginSlowUrlRow>[] = [
  {
    accessorKey: 'url', id: 'url', meta: { label: 'URL' }, header: columnHeader('URL'),
    cell: ({ row }) => <FilterValueCell filters={[{ column: 'url', value: row.original.url }]} className="font-mono text-xs" containerClassName="max-w-[400px]" />,
  },
  requestsCol(),
  p50Col(),
  p95Col(),
  { accessorKey: 'p99_ms', id: 'p99_ms', meta: { label: 'P99 Latency' }, header: columnHeader('P99'), cell: ({ row }) => msCell(row.original.p99_ms) },
]

const POP_COLUMNS: ColumnDef<OriginPopLatencyRow>[] = [
  {
    accessorKey: 'pop', id: 'pop', meta: { label: 'POP' }, header: columnHeader('POP'),
    cell: ({ row }) => <FilterValueCell filters={[{ column: 'pop', value: row.original.pop }]} display={<PopLabel code={row.original.pop} />} className="font-bold" />,
  },
  requestsCol(),
  p50Col(),
  {
    accessorKey: 'p95_ms', id: 'p95_ms', meta: { label: 'P95 Latency' }, header: columnHeader('P95'),
    cell: ({ row }) => <span className={cn(row.original.elevated ? 'text-destructive font-bold' : '')}>{row.original.p95_ms?.toFixed(1)}ms</span>,
  },
]

const IP_COLUMNS: ColumnDef<OriginIpHealthRow>[] = [
  {
    accessorKey: 'oip', id: 'oip', meta: { label: 'Origin IP' }, header: columnHeader('Origin IP'),
    cell: ({ row }) => <FilterValueCell filters={[{ column: 'oip', value: row.original.oip }]} className="font-mono text-xs" />,
  },
  requestsCol(),
  p50Col(),
  p95Col(),
  {
    accessorKey: 'error_pct', id: 'error_pct', meta: { label: '5xx Errors %' }, header: columnHeader('5xx %'),
    cell: ({ row }) => <span className={cn((row.original.error_pct ?? 0) > 1 ? 'text-destructive font-bold' : '')}>{row.original.error_pct}%</span>,
  },
]

const COLUMN_LABELS: Record<string, string> = {
  url: 'URL', pop: 'POP', oip: 'Origin IP', requests: 'Requests',
  p50_ms: 'Median (P50)', p95_ms: 'P95 Latency', p99_ms: 'P99 Latency', error_pct: 'Error Rate %',
}
const getLabels = (ids: string[]) => ids.map((id) => ({ id, label: COLUMN_LABELS[id] || id }))

export interface LatencyHeatmapProps {
  slowUrls: SectionQuery<OriginSlowUrls>
  popLatency: SectionQuery<OriginPopLatency>
  ipHealth: SectionQuery<OriginIpHealth>
  summary: SectionQuery<OriginSummary>
  urlVisibility: VisibilityState
  setUrlVisibility: (visibility: VisibilityState) => void
  onUrlVisChange: (id: string, visible: boolean) => void
  popVisibility: VisibilityState
  setPopVisibility: (visibility: VisibilityState) => void
  onPopVisChange: (id: string, visible: boolean) => void
  ipVisibility: VisibilityState
  setIpVisibility: (visibility: VisibilityState) => void
  onIpVisChange: (id: string, visible: boolean) => void
}

export function LatencyHeatmap({
  slowUrls, popLatency, ipHealth, summary,
  urlVisibility, setUrlVisibility, onUrlVisChange,
  popVisibility, setPopVisibility, onPopVisChange,
  ipVisibility, setIpVisibility, onIpVisChange,
}: LatencyHeatmapProps) {
  const { isFieldActive } = useActiveLogFields()

  const renderTableCard = <TRow,>(cfg: {
    title: string
    icon: React.ReactNode
    query: {
      data?: { rows?: TRow[]; _approx?: boolean | null; _approx_note?: string | null }
      isLoading: boolean
      isFetching: boolean
      error: unknown
    }
    loadingMessage: string
    help: string
    defaultApproxNote: string
    columnIds: string[]
    visibility: VisibilityState
    setVisibility: (v: VisibilityState) => void
    onVisChange: (id: string, visible: boolean) => void
    columns: ColumnDef<TRow>[]
    emptyMessage: string
  }) => (
    <AnalyticsCard
      title={cfg.title}
      icon={cfg.icon}
      isLoading={cfg.query.isLoading}
      isFetching={cfg.query.isFetching}
      loadingMessage={cfg.loadingMessage}
      error={cfg.query.error as AnalyticsCardError | null}
      className="min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
      contentClassName="p-0"
      helpContent={<p>{cfg.help}</p>}
      headerAction={
        <div className="flex items-center gap-2">
          {cfg.query.data?._approx === true && <ApproxBadge message={cfg.query.data?._approx_note ?? cfg.defaultApproxNote} />}
          <ColumnVisibilityDropdown columns={getLabels(cfg.columnIds)} visibility={cfg.visibility} onChange={cfg.onVisChange} />
        </div>
      }
    >
      <DataTable
        columns={cfg.columns}
        data={cfg.query.data?.rows || []}
        emptyMessage={cfg.query.isLoading ? '' : cfg.emptyMessage}
        hideToolbar
        columnVisibility={cfg.visibility}
        onColumnVisibilityChange={cfg.setVisibility}
      />
    </AnalyticsCard>
  )

  return (
    <>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        {renderTableCard({
          title: 'Slowest URLs at Origin',
          icon: <Server className="h-4 w-4" />,
          query: slowUrls,
          loadingMessage: 'Loading slowest URLs at origin...',
          help: 'A list of specific URLs that take the longest time to fetch from the origin.',
          defaultApproxNote: 'P50/P95/P99 latency on this window are request-weighted averages of per-hour percentiles. Request counts are exact.',
          columnIds: ['url', 'requests', 'p50_ms', 'p95_ms', 'p99_ms'],
          visibility: urlVisibility,
          setVisibility: setUrlVisibility,
          onVisChange: onUrlVisChange,
          columns: URL_COLUMNS,
          emptyMessage: isFieldActive('ottfb') ? 'No slow URLs in this time range.' : 'Requires Origin Metrics (Group L) fields to be enabled.',
        })}
        {renderTableCard({
          title: 'Origin Performance by POP',
          icon: <MapPin className="h-4 w-4" />,
          query: popLatency,
          loadingMessage: 'Loading origin performance by POP...',
          help: 'Backend latency aggregated by Fastly POP location.',
          defaultApproxNote: 'P50/P95 latency on this window are request-weighted averages of per-hour percentiles. Request counts are exact.',
          columnIds: ['pop', 'requests', 'p50_ms', 'p95_ms'],
          visibility: popVisibility,
          setVisibility: setPopVisibility,
          onVisChange: onPopVisChange,
          columns: POP_COLUMNS,
          emptyMessage: isFieldActive('ottfb') && isFieldActive('pop')
            ? 'No POP latency data in this time range.'
            : 'Requires Origin Metrics (Group L) and Infrastructure (Group C) fields to be enabled.',
        })}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {renderTableCard({
          title: 'Origin IP Health',
          icon: <Globe className="h-4 w-4" />,
          query: ipHealth,
          loadingMessage: 'Loading origin IP health...',
          help: 'Latency and error rates for individual backend IP addresses.',
          defaultApproxNote: 'P50/P95 latency on this window are request-weighted averages of per-hour percentiles. Request counts and the 5xx error rate are exact.',
          columnIds: ['oip', 'requests', 'p50_ms', 'p95_ms', 'error_pct'],
          visibility: ipVisibility,
          setVisibility: setIpVisibility,
          onVisChange: onIpVisChange,
          columns: IP_COLUMNS,
          emptyMessage: isFieldActive('oip') ? 'No origin IP data in this time range.' : 'Requires Origin Metrics (Group L) fields to be enabled.',
        })}

        <AnalyticsCard
          title="Origin Payload Size"
          icon={<Globe className="h-4 w-4" />}
          isLoading={summary.isLoading}
          isFetching={summary.isFetching}
          loadingMessage="Loading origin payload size..."
          error={summary.error as AnalyticsCardError | null}
          className="min-h-[220px] [contain-intrinsic-size:220px] [content-visibility:auto]"
          helpContent={<p>The median size of the response body transferred from the origin to Fastly.</p>}
        >
          <div className="flex flex-col items-center justify-center py-4 text-center">
            <div className="text-2xl font-bold mb-1">
              {summary.data?.obytes_p50 != null ? formatBytes(summary.data.obytes_p50) : 'N/A'}
            </div>
            <div className="text-xs text-muted-foreground">Median Response Size (obytes)</div>
            <div className="w-full h-2 bg-muted rounded-full mt-4 overflow-hidden flex">
              <div
                className="bg-primary h-full transition-all"
                style={{ width: `${Math.min(100, ((summary.data?.ottfb_p50_ms || 0) / (summary.data?.ottlb_p50_ms || 1)) * 100)}%` }}
              />
            </div>
            <div className="flex justify-between w-full mt-1 text-[10px] uppercase font-bold text-muted-foreground">
              <span>TTFB</span>
              <span>TTLB</span>
            </div>
          </div>
        </AnalyticsCard>
      </div>
    </>
  )
}
