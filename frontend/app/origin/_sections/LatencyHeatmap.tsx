'use client'

import React from 'react'
import { DataTable, ColumnVisibilityDropdown } from '@/components/DataTable'
import { FilterValueCell } from '@/components/FilterValueCell'
import { PopLabel } from '@/components/PopLabel'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { ApproxBadge } from './ApproxBadge'
import { cn } from '@/lib/utils';
import { formatBytes } from '@/lib/format'
import { Server, MapPin, Globe } from 'lucide-react'
import { useActiveLogFields } from '@/hooks/useActiveLogFields'

// Shared column scaffolding — every column header is the same dense
// uppercase span, and requests/p50/p95 are byte-identical across the
// url/pop/ip variants. Factor them out so the three column sets compose
// from one definition each (rendered output is unchanged).
const metricHeader = (label: string) => () => (
  <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">{label}</span>
)
const msCell = (info: any) => <span>{info.getValue()?.toFixed(1)}ms</span>

const requestsCol = { accessorKey: 'requests', id: 'requests', meta: { label: 'Requests' }, header: metricHeader('Reqs'), cell: (info: any) => info.getValue().toLocaleString() }
const p50Col = { accessorKey: 'p50_ms', id: 'p50_ms', meta: { label: 'Median (P50)' }, header: metricHeader('P50'), cell: msCell }
const p95Col = { accessorKey: 'p95_ms', id: 'p95_ms', meta: { label: 'P95 Latency' }, header: metricHeader('P95'), cell: msCell }

const COLUMNS = {
  url: [
    {
      accessorKey: 'url',
      id: 'url', meta: { label: 'URL' }, header: metricHeader('URL'),
      cell: (info: any) => (
        <FilterValueCell
          filters={[{ column: 'url', value: info.getValue() }]}
          className="font-mono text-xs"
          containerClassName="max-w-[400px]"
        />
      )
    },
    requestsCol,
    p50Col,
    p95Col,
    { accessorKey: 'p99_ms', id: 'p99_ms', meta: { label: 'P99 Latency' }, header: metricHeader('P99'), cell: msCell },
  ],
  pop: [
    {
      accessorKey: 'pop',
      id: 'pop', meta: { label: 'POP' }, header: metricHeader('POP'),
      cell: (info: any) => (
        <FilterValueCell
          filters={[{ column: 'pop', value: info.getValue() }]}
          display={<PopLabel code={info.getValue()} />}
          className="font-bold"
        />
      )
    },
    requestsCol,
    p50Col,
    { accessorKey: 'p95_ms', id: 'p95_ms', meta: { label: 'P95 Latency' }, header: metricHeader('P95'), cell: (info: any) => (
      <span className={cn(info.row.original.elevated ? "text-destructive font-bold" : "")}>
        {info.getValue()?.toFixed(1)}ms
      </span>
    )},
  ],
  ip: [
    {
      accessorKey: 'oip',
      id: 'oip', meta: { label: 'Origin IP' }, header: metricHeader('Origin IP'),
      cell: (info: any) => (
        <FilterValueCell
          filters={[{ column: 'oip', value: info.getValue() }]}
          className="font-mono text-xs"
        />
      )
    },
    requestsCol,
    p50Col,
    p95Col,
    { accessorKey: 'error_pct', id: 'error_pct', meta: { label: '5xx Errors %' }, header: metricHeader('5xx %'), cell: (info: any) => (
      <span className={cn(info.getValue() > 1 ? "text-destructive font-bold" : "")}>
        {info.getValue()}%
      </span>
    )},
  ]
}

const COLUMN_LABELS: Record<string, string> = {
  url: 'URL',
  pop: 'POP',
  oip: 'Origin IP',
  requests: 'Requests',
  p50_ms: 'Median (P50)',
  p95_ms: 'P95 Latency',
  p99_ms: 'P99 Latency',
  error_pct: 'Error Rate %',
}

const getLabels = (ids: string[]) => ids.map(id => ({ id, label: COLUMN_LABELS[id] || id }))

export function LatencyHeatmap({
  slowUrls,
  popLatency,
  ipHealth,
  summary,
  urlVisibility,
  setUrlVisibility,
  onUrlVisChange,
  popVisibility,
  setPopVisibility,
  onPopVisChange,
  ipVisibility,
  setIpVisibility,
  onIpVisChange,
}: any) {
  // Distinguish "field group not enabled" from "enabled but no data in this
  // window yet" so the empty tables don't falsely read as misconfigured.
  const { isFieldActive } = useActiveLogFields()
  return (
    <>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Slowest URLs at Origin"
          icon={<Server className="h-4 w-4" />}
          isLoading={slowUrls.isLoading}
          isFetching={slowUrls.isFetching}
          error={slowUrls.error as AnalyticsCardError | null}
          contentClassName="p-0"
          helpContent={<p>A list of specific URLs that take the longest time to fetch from the origin.</p>}
          headerAction={
            <div className="flex items-center gap-2">
              {slowUrls.data?._approx === true && (
                <ApproxBadge message="P50/P95/P99 latency on this window are request-weighted averages of per-hour percentiles. Request counts are exact." />
              )}
              <ColumnVisibilityDropdown
                columns={getLabels(['url', 'requests', 'p50_ms', 'p95_ms', 'p99_ms'])}
                visibility={urlVisibility}
                onChange={onUrlVisChange}
              />
            </div>
          }
        >
          <DataTable
            columns={COLUMNS.url}
            data={slowUrls.data?.rows || []}
            emptyMessage={slowUrls.isLoading ? "" : (isFieldActive('ottfb') ? "No slow URLs in this time range." : "Requires Origin Metrics (Group L) fields to be enabled.")}
            hideToolbar
            columnVisibility={urlVisibility}
            onColumnVisibilityChange={setUrlVisibility}
          />
        </AnalyticsCard>

        <AnalyticsCard
          title="Origin Performance by POP"
          icon={<MapPin className="h-4 w-4" />}
          isLoading={popLatency.isLoading}
          isFetching={popLatency.isFetching}
          error={popLatency.error as AnalyticsCardError | null}
          contentClassName="p-0"
          helpContent={<p>Backend latency aggregated by Fastly POP location.</p>}
          headerAction={
            <div className="flex items-center gap-2">
              {popLatency.data?._approx === true && (
                <ApproxBadge message="P50/P95 latency on this window are request-weighted averages of per-hour percentiles. Request counts are exact." />
              )}
              <ColumnVisibilityDropdown
                columns={getLabels(['pop', 'requests', 'p50_ms', 'p95_ms'])}
                visibility={popVisibility}
                onChange={onPopVisChange}
              />
            </div>
          }
        >
          <DataTable
            columns={COLUMNS.pop}
            data={popLatency.data?.rows || []}
            emptyMessage={popLatency.isLoading ? "" : ((isFieldActive('ottfb') && isFieldActive('pop')) ? "No POP latency data in this time range." : "Requires Origin Metrics (Group L) and Infrastructure (Group C) fields to be enabled.")}
            hideToolbar
            columnVisibility={popVisibility}
            onColumnVisibilityChange={setPopVisibility}
          />
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <AnalyticsCard
          title="Origin IP Health"
          icon={<Globe className="h-4 w-4" />}
          isLoading={ipHealth.isLoading}
          isFetching={ipHealth.isFetching}
          error={ipHealth.error as AnalyticsCardError | null}
          contentClassName="p-0"
          helpContent={<p>Latency and error rates for individual backend IP addresses.</p>}
          headerAction={
            <div className="flex items-center gap-2">
              {ipHealth.data?._approx === true && (
                <ApproxBadge message="P50/P95 latency on this window are request-weighted averages of per-hour percentiles. Request counts and the 5xx error rate are exact." />
              )}
              <ColumnVisibilityDropdown
                columns={getLabels(['oip', 'requests', 'p50_ms', 'p95_ms', 'error_pct'])}
                visibility={ipVisibility}
                onChange={onIpVisChange}
              />
            </div>
          }
        >
          <DataTable
            columns={COLUMNS.ip}
            data={ipHealth.data?.rows || []}
            emptyMessage={ipHealth.isLoading ? "" : (isFieldActive('oip') ? "No origin IP data in this time range." : "Requires Origin Metrics (Group L) fields to be enabled.")}
            hideToolbar
            columnVisibility={ipVisibility}
            onColumnVisibilityChange={setIpVisibility}
          />
        </AnalyticsCard>

        <AnalyticsCard
          title="Origin Payload Size"
          icon={<Globe className="h-4 w-4" />}
          isLoading={summary.isLoading}
          isFetching={summary.isFetching}
          error={summary.error as AnalyticsCardError | null}
          helpContent={<p>The median size of the response body transferred from the origin to Fastly.</p>}
        >
          <div className="flex flex-col items-center justify-center py-4 text-center">
            <div className="text-2xl font-bold mb-1">
              {summary.data?.obytes_p50 != null
                ? formatBytes(summary.data.obytes_p50)
                : 'N/A'}
            </div>
            <div className="text-xs text-muted-foreground">Median Response Size (obytes)</div>
            <div className="w-full h-2 bg-muted rounded-full mt-4 overflow-hidden flex">
              <div
                className="bg-primary h-full transition-all"
                style={{ width: `${Math.min(100, (summary.data?.ottfb_p50_ms || 0) / (summary.data?.ottlb_p50_ms || 1) * 100)}%` }}
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
