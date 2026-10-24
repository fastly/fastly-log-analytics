'use client'

import { SlowestUrlsHelp, SlowestNetworksHelp, CacheTtlHelp, OriginVsEdgeHelp } from "../help-content";
import React from 'react'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import type { components } from '@/types/api'
import { useAnalyticsReportQuery } from '@/hooks/useAnalyticsReportQuery'
import { useColumnVisibility } from '@/hooks/useColumnVisibility'
import { useReportRangeAnchor } from '@/hooks/useReportRangeAnchor'
import { PlotlyChart } from '@/components/PlotlyChart'
import { DataTable } from '@/components/DataTable'
import { Zap, Server, Shield, Clock, Network } from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { ColumnVisibilityDropdown } from '@/components/DataTable'
import { makeLatencyColumns } from '@/lib/table-utils'
import { useFieldLabel } from '@/hooks/useLogFieldsCatalog';
import { CHART_COLORS } from '@/lib/chart-builders'
import type { PlotlyLayout, PlotlyTrace } from '@/types/plotly'
import type { FiltersPayload } from '@/types/filters'
type PerformanceUrlRow = components['schemas']['PerformanceUrlRow']
type PerformanceAsnRow = components['schemas']['PerformanceAsnRow']
const URL_COLUMN_IDS = ['url', 'requests', 'avg', 'p50', 'p95', 'p99']
const ASN_COLUMN_IDS = ['label', 'requests', 'avg', 'p50', 'p95', 'p99']

const urlColumns = makeLatencyColumns<PerformanceUrlRow>('url', 'URL', 'url')
const asnColumns = makeLatencyColumns<PerformanceAsnRow>('label', 'ASN', 'asn')

// Module-level so identity is stable across renders — PlotlyChart's React.memo
// shallow-compares layout, and a fresh object literal each render forces a
// full re-plot.
const WATERFALL_LAYOUT: PlotlyLayout = {
  xaxis: { title: 'Latency (ms)', ticksuffix: 'ms', separatethousands: true, exponentformat: 'none' },
  yaxis: { autorange: 'reversed' },
  margin: { l: 140, r: 20, t: 20, b: 40 },
  showlegend: false,
}
const TTL_DIST_LAYOUT: PlotlyLayout = { yaxis: { title: 'Count' } }
const SCATTER_LAYOUT: PlotlyLayout = {
  xaxis: { title: 'Origin TTFB (ms)', ticksuffix: 'ms', separatethousands: true, exponentformat: 'none' },
  yaxis: { title: 'Edge Processing (ms)', ticksuffix: 'ms', separatethousands: true, exponentformat: 'none' },
}

// Per-section field lists. The backend gates each section block on
// `sections is None or 'name' in sections` and enforces two coupling pairs:
// {top_urls, top_asns} (shared 2-pass CTE from 8fc53e1) and {waterfall,
// scatter} (shared MATERIALIZED components CTE). To avoid the backend
// auto-coupling forcing the same section into BOTH requests (which would
// double-compute the shared CTE), bundle scatter together with waterfall
// in core, leaving ttl_dist as the only truly independent section.
//   core          → waterfall + scatter + top_urls + top_asns
//   distributions → ttl_dist
//
// MUST stay in lockstep with PERFORMANCE_*_SSR_SECTIONS in lib/ssr/performance.ts
// — the section lists are part of each POST body, so a divergence would change
// the response and miss the dehydrated cache on first paint.

interface PerformanceBodyProps {
  activeServiceId: string | null
  filterPayload: FiltersPayload
  // startTime/endTime feed the custom-absolute-range scan window (lib/range-wire).
  startTime: string | null
  endTime: string | null
  relativeRange: string | null
  isAutoRange: boolean
  anchor: string
  getFieldLabel: (field: string) => string
  urlVisibility: ReturnType<typeof useColumnVisibility>[0]
  setUrlVisibility: ReturnType<typeof useColumnVisibility>[1]
  onUrlVisChange: ReturnType<typeof useColumnVisibility>[2]
  asnVisibility: ReturnType<typeof useColumnVisibility>[0]
  setAsnVisibility: ReturnType<typeof useColumnVisibility>[1]
  onAsnVisChange: ReturnType<typeof useColumnVisibility>[2]
}

// Lifted out of the ReportLayout render-prop so the hooks (useServiceQuery,
// useMemo) live at the top of a STABLE component instead of inside the
// `{(ctx) => { …hooks… }}` callback — mirrors OriginReportContent / SecurityBody
// / InsightsBody. (The prior in-callback hooks risked React #310 if a
// conditional ever appears above them.)
function PerformanceBody({
  activeServiceId,
  filterPayload,
  startTime,
  endTime,
  relativeRange,
  isAutoRange,
  anchor,
  getFieldLabel,
  urlVisibility,
  setUrlVisibility,
  onUrlVisChange,
  asnVisibility,
  setAsnVisibility,
  onAsnVisChange,
}: PerformanceBodyProps) {
  // SSR-seed key contract (lib/ssr/performance.ts + app/performance/page.tsx): in
  // token mode keys on the SERVER-REPRODUCIBLE (rangeKey, anchor) pair instead of
  // client-now()-anchored start/end, so the seed byte-matches. On a cold load
  // rangeKey is "24h". A custom absolute range instead keys on "abs:<start>|<end>"
  // and sends the explicit bounds, so the tables/distributions reflect exactly the
  // selected window. Both queries share one wire so they stay on one cache key.
  const performanceQuery = useAnalyticsReportQuery({
    namespace: ['performance', 'aggregates'],
    serviceId: activeServiceId,
    filterPayload,
    range: { relativeRange, isAutoRange, startTime, endTime, anchor },
    keyParts: ['p99'],
    queryFn: async ({ rangeBody, signal }) => {
      return unwrapClient(
        client.POST("/api/performance/aggregates", {
          signal,
          body: {
            filters: filterPayload,
            sort_by: 'p99',
            sections: undefined,
            ...rangeBody,
          },
        }),
      )
    }
  })

  const coreData = performanceQuery.data
  const distData = performanceQuery.data

  // ── Charts ──────────────────────────────────────────────────────────────

  const ttlDist = distData?.ttl_dist
  const ttlDistData = React.useMemo((): PlotlyTrace[] => {
    if (!ttlDist?.length) return []
    return [{
      x: ttlDist.map(d => d.bucket),
      y: ttlDist.map(d => d.count),
      type: 'bar',
      marker: { color: '#6366f1' }
    }]
  }, [ttlDist])

  const scatterRows = coreData?.scatter
  const scatterData = React.useMemo((): PlotlyTrace[] => {
    if (!scatterRows?.length) return []
    // The backend can legitimately emit a null `cache`; a dirty row falls into MISS
    // instead of throwing `null.startsWith` during render and crashing the whole
    // route to the error boundary.
    const isHit = (c: string | null | undefined) => c?.startsWith('HIT') === true
    const hit = scatterRows.filter(d => isHit(d.cache))
    const miss = scatterRows.filter(d => !isHit(d.cache))
    return [
      {
        x: hit.map(d => d.origin),
        y: hit.map(d => d.edge),
        mode: 'markers',
        type: 'scatter',
        name: 'HIT',
        marker: { color: CHART_COLORS.success, size: 4, opacity: 0.6 }
      },
      {
        x: miss.map(d => d.origin),
        y: miss.map(d => d.edge),
        mode: 'markers',
        type: 'scatter',
        name: 'MISS',
        marker: { color: CHART_COLORS.warning, size: 4, opacity: 0.6 }
      }
    ]
  }, [scatterRows])
  // One bar per component, each on its own y-row. Averages are additive,
  // but stacking them buries the small components when one (typically
  // origin_wait) dominates. Per-row bars keep every component visible at
  // its true scale on the shared x-axis. The y-axis label identifies each
  // bar, so no legend is needed.
  const avg = coreData?.waterfall?.avg
  const waterfallData = React.useMemo((): PlotlyTrace[] => {
    if (!avg) return []
    return [
      { x: [avg.edge_processing || 0], y: ['Edge Processing'],  type: 'bar', orientation: 'h', marker: { color: '#8b5cf6' }, showlegend: false },
      { x: [avg.origin_wait || 0],     y: ['Origin TTFB Wait'], type: 'bar', orientation: 'h', marker: { color: CHART_COLORS.warning }, showlegend: false },
      { x: [avg.origin_download || 0], y: ['Origin Download'],  type: 'bar', orientation: 'h', marker: { color: '#ec4899' }, showlegend: false },
      { x: [avg.client_download || 0], y: ['Client Download'],  type: 'bar', orientation: 'h', marker: { color: CHART_COLORS.success }, showlegend: false },
    ]
  }, [avg])

  return (
    <>
      <div className="mb-6">
        <AnalyticsCard
          title="End-to-End Latency Waterfall (Average)"
          icon={<Network className="h-4 w-4" />}
          isLoading={performanceQuery.isLoading}
          isFetching={performanceQuery.isFetching}
          loadingMessage="Calculating latency percentiles..."
          error={performanceQuery.error as AnalyticsCardError | null}
          isEmpty={!coreData?.waterfall?.avg}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpContent={
            <div className="space-y-4">
              <p>Breakdown of where request time is spent, averaged across the selected time window and filters.</p>
              <ul className="list-disc pl-4 space-y-2">
                <li><strong>Edge Processing:</strong> Time Fastly spends before sending the first byte (WAF, VCL processing) outside of origin wait.</li>
                <li><strong>Origin TTFB Wait:</strong> Time Fastly waits for the origin to send the first byte (Origin TTFB).</li>
                <li><strong>Origin Download:</strong> Time taken to download the rest of the response from the origin (Origin TTLB - Origin TTFB).</li>
                <li><strong>Client Download:</strong> Time taken to finish sending the response to the client after edge/origin processing.</li>
              </ul>
            </div>
          }
        >
          <PlotlyChart
            data={waterfallData}
            layout={WATERFALL_LAYOUT}
            height="100%"
          />
        </AnalyticsCard>
      </div>

      {/* Backend sets _approx when top_urls/top_asns are served from the
          perf_latency rollup (>= 48h unfiltered): percentiles are request-
          weighted averages of per-hour percentiles. Request counts are exact. */}
      {coreData?._approx === true && (
        <div className="flex justify-end mb-2">
          <span
            title="Latency percentiles on this window are request-weighted averages of per-hour percentiles (sub-second accuracy on most services). Request counts are exact."
            className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-muted text-muted-foreground text-[10px] font-bold uppercase tracking-wider cursor-help"
          >
            <span className="w-1.5 h-1.5 rounded-full bg-muted-foreground/60" />
            Approximate
          </span>
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard
          title="Slowest URLs"
          icon={<Server className="h-4 w-4" />}
          headerAction={
            <div className="flex items-center gap-2">
              <ColumnVisibilityDropdown columns={URL_COLUMN_IDS.map(id => ({ id, label: getFieldLabel(id) }))} visibility={urlVisibility} onChange={onUrlVisChange} />
            </div>
          }
          isLoading={performanceQuery.isLoading}
          isFetching={performanceQuery.isFetching}
          loadingMessage="Profiling endpoint latency..."
          error={performanceQuery.error as AnalyticsCardError | null}
          isEmpty={(coreData?.top_urls?.length ?? 0) === 0}
          className="min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
          contentClassName="p-0"
          helpContent={<SlowestUrlsHelp />}
        >
          <DataTable
            columns={urlColumns}
            data={coreData?.top_urls || []}
            hideToolbar
            columnVisibility={urlVisibility}
            onColumnVisibilityChange={setUrlVisibility}
          />
        </AnalyticsCard>

        <AnalyticsCard
          title="Slowest Networks"
          icon={<Shield className="h-4 w-4" />}
          headerAction={
            <div className="flex items-center gap-2">
              <ColumnVisibilityDropdown
                columns={ASN_COLUMN_IDS.map(id => ({ id, label: getFieldLabel(id) }))}
                visibility={asnVisibility}
                onChange={onAsnVisChange}
              />
            </div>
          }
          isLoading={performanceQuery.isLoading}
          isFetching={performanceQuery.isFetching}
          loadingMessage="Profiling network latency..."
          error={performanceQuery.error as AnalyticsCardError | null}
          isEmpty={(coreData?.top_asns?.length ?? 0) === 0}
          className="min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
          contentClassName="p-0"
          helpContent={<SlowestNetworksHelp />}
        >
          <DataTable
            columns={asnColumns}
            data={coreData?.top_asns || []}
            hideToolbar
            columnVisibility={asnVisibility}
            onColumnVisibilityChange={setAsnVisibility}
          />
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard
          title="Cache TTL Distribution"
          icon={<Clock className="h-4 w-4" />}
          isLoading={performanceQuery.isLoading}
          isFetching={performanceQuery.isFetching}
          loadingMessage="Profiling cache TTL distribution..."
          error={performanceQuery.error as AnalyticsCardError | null}
          isEmpty={(distData?.ttl_dist?.length ?? 0) === 0}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpContent={<CacheTtlHelp />}
        >
          <PlotlyChart
            data={ttlDistData}
            layout={TTL_DIST_LAYOUT}
            height="100%"
          />
        </AnalyticsCard>

        <AnalyticsCard
          title="Origin vs Edge Processing (ms)"
          icon={<Zap className="h-4 w-4" />}
          isLoading={performanceQuery.isLoading}
          isFetching={performanceQuery.isFetching}
          loadingMessage="Profiling origin vs edge latency..."
          error={performanceQuery.error as AnalyticsCardError | null}
          isEmpty={(coreData?.scatter?.length ?? 0) === 0}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpContent={<OriginVsEdgeHelp />}
        >
          <PlotlyChart
            data={scatterData}
            layout={SCATTER_LAYOUT}
            height="100%"
          />
        </AnalyticsCard>
      </div>
    </>
  )
}

export default function PerformanceClient({ nowServerStr }: { nowServerStr?: string }) {
  const getFieldLabel = useFieldLabel()

  const [urlVisibility, setUrlVisibility, onUrlVisChange] = useColumnVisibility()
  const [asnVisibility, setAsnVisibility, onAsnVisChange] = useColumnVisibility()

  const { relativeRange, isAutoRange, anchor } = useReportRangeAnchor(nowServerStr)

  return (
    <ReportLayout
      title="Performance"
      description="Analyze latency, cache efficiency, and origin vs edge processing time."
      icon={Zap}
    >
      {({ startTime, endTime, activeServiceId, filterPayload }) => (
        <PerformanceBody
          activeServiceId={activeServiceId}
          filterPayload={filterPayload}
          startTime={startTime}
          endTime={endTime}
          relativeRange={relativeRange}
          isAutoRange={isAutoRange}
          anchor={anchor}
          getFieldLabel={getFieldLabel}
          urlVisibility={urlVisibility}
          setUrlVisibility={setUrlVisibility}
          onUrlVisChange={onUrlVisChange}
          asnVisibility={asnVisibility}
          setAsnVisibility={setAsnVisibility}
          onAsnVisChange={onAsnVisChange}
        />
      )}
    </ReportLayout>
  )
}
