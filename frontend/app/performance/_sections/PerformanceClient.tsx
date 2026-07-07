'use client'

import { SlowestUrlsHelp, SlowestNetworksHelp, CacheTtlHelp, OriginVsEdgeHelp } from "../help-content";
import React from 'react'
import { client } from '@/lib/api'
import type { components } from '@/types/api'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { useColumnVisibility } from '@/hooks/useColumnVisibility'
import { useQueryClient } from '@tanstack/react-query'
import { useFilterStore } from '@/stores/filterStore'
import { useServiceStore } from '@/stores/serviceStore'
import { quantizeAnchor } from '@/lib/time-window'
import { resolveSnappedWindow, type LogExtents } from '@/lib/log-extents-snap'
import { resolveRangeWire } from '@/lib/range-wire'
import { PlotlyChart } from '@/components/PlotlyChart'
import { DataTable } from '@/components/DataTable'
import { Zap, Server, Shield, Clock, Network } from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { ColumnVisibilityDropdown } from '@/components/DataTable'
import { makeLatencyColumns } from '@/lib/table-utils'
import { useFieldLabel } from '@/hooks/useFieldLabel';
const URL_COLUMN_IDS = ['url', 'requests', 'avg', 'p50', 'p95', 'p99']
const ASN_COLUMN_IDS = ['label', 'requests', 'avg', 'p50', 'p95', 'p99']

const urlColumns = makeLatencyColumns('url', 'URL', 'url')
const asnColumns = makeLatencyColumns('label', 'ASN', 'asn')

// Module-level so identity is stable across renders — PlotlyChart's React.memo
// shallow-compares layout, and a fresh object literal each render forces a
// full re-plot.
const WATERFALL_LAYOUT = {
  xaxis: { title: 'Latency (ms)', ticksuffix: 'ms', separatethousands: true, exponentformat: 'none' },
  yaxis: { autorange: 'reversed' },
  margin: { l: 140, r: 20, t: 20, b: 40 },
  showlegend: false,
}
const TTL_DIST_LAYOUT = { yaxis: { title: 'Count' } }
const SCATTER_LAYOUT = {
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
type PerformanceSections = NonNullable<components['schemas']['PerformanceRequest']['sections']>
const PERFORMANCE_CORE_SECTIONS: PerformanceSections = ['waterfall', 'scatter', 'top_urls', 'top_asns']
const PERFORMANCE_DISTRIBUTIONS_SECTIONS: PerformanceSections = ['ttl_dist']

interface PerformanceBodyProps {
  activeServiceId: string | null
  filterPayload: any
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
  const { rangeKey, rangeBody } = resolveRangeWire({ relativeRange, isAutoRange, startTime, endTime, anchor })
  const coreQuery = useServiceQuery(
    ['performance', 'aggregates', 'core', activeServiceId, rangeKey, anchor, filterPayload, 'p99'],
    async ({ signal }) => {
      const { data } = await client.POST("/api/performance/aggregates", { signal,
        body: {
          filters: filterPayload,
          sort_by: 'p99',
          sections: PERFORMANCE_CORE_SECTIONS,
          ...rangeBody,
        }
      })
      return data
    }
  )

  const distributionsQuery = useServiceQuery(
    ['performance', 'aggregates', 'distributions', activeServiceId, rangeKey, anchor, filterPayload, 'p99'],
    async ({ signal }) => {
      const { data } = await client.POST("/api/performance/aggregates", { signal,
        body: {
          filters: filterPayload,
          sort_by: 'p99',
          sections: PERFORMANCE_DISTRIBUTIONS_SECTIONS,
          ...rangeBody,
        }
      })
      return data
    }
  )

  const coreData = coreQuery.data
  const distData = distributionsQuery.data

  // ── Charts ──────────────────────────────────────────────────────────────

  const ttlDistData = React.useMemo(() => {
    if (!distData?.ttl_dist?.length) return []
    return [{
      x: distData.ttl_dist.map(d => d.bucket),
      y: distData.ttl_dist.map(d => d.count),
      type: 'bar',
      marker: { color: '#6366f1' }
    }]
  }, [distData?.ttl_dist])

  const scatterData = React.useMemo(() => {
    if (!coreData?.scatter?.length) return []
    // `cache` is typed `unknown` in the generated schema and the backend can
    // legitimately emit null; narrow at runtime so a dirty row falls into MISS
    // instead of throwing `null.startsWith` during render and crashing the
    // whole route to the error boundary.
    const isHit = (c: unknown) => typeof c === 'string' && c.startsWith('HIT')
    const hit = coreData.scatter.filter(d => isHit(d.cache))
    const miss = coreData.scatter.filter(d => !isHit(d.cache))
    return [
      {
        x: hit.map(d => d.origin),
        y: hit.map(d => d.edge),
        mode: 'markers',
        type: 'scatter',
        name: 'HIT',
        marker: { color: '#10b981', size: 4, opacity: 0.6 }
      },
      {
        x: miss.map(d => d.origin),
        y: miss.map(d => d.edge),
        mode: 'markers',
        type: 'scatter',
        name: 'MISS',
        marker: { color: '#f59e0b', size: 4, opacity: 0.6 }
      }
    ]
  }, [coreData?.scatter])
  // One bar per component, each on its own y-row. Averages are additive,
  // but stacking them buries the small components when one (typically
  // origin_wait) dominates. Per-row bars keep every component visible at
  // its true scale on the shared x-axis. The y-axis label identifies each
  // bar, so no legend is needed.
  const waterfallData = React.useMemo(() => {
    const avg = coreData?.waterfall?.avg
    if (!avg) return []
    return [
      { x: [avg.edge_processing || 0], y: ['Edge Processing'],  type: 'bar', orientation: 'h', marker: { color: '#8b5cf6' }, showlegend: false },
      { x: [avg.origin_wait || 0],     y: ['Origin TTFB Wait'], type: 'bar', orientation: 'h', marker: { color: '#f59e0b' }, showlegend: false },
      { x: [avg.origin_download || 0], y: ['Origin Download'],  type: 'bar', orientation: 'h', marker: { color: '#ec4899' }, showlegend: false },
      { x: [avg.client_download || 0], y: ['Client Download'],  type: 'bar', orientation: 'h', marker: { color: '#10b981' }, showlegend: false },
    ]
  }, [coreData?.waterfall])

  return (
    <>
      <div className="mb-6">
        <AnalyticsCard
          title="End-to-End Latency Waterfall (Average)"
          icon={<Network className="h-4 w-4" />}
          isLoading={coreQuery.isLoading}
          isFetching={coreQuery.isFetching}
          error={coreQuery.error as AnalyticsCardError | null}
          isEmpty={!coreData?.waterfall?.avg}
          className="h-[360px]"
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
          isLoading={coreQuery.isLoading}
          isFetching={coreQuery.isFetching}
          error={coreQuery.error as AnalyticsCardError | null}
          isEmpty={(coreData?.top_urls?.length ?? 0) === 0}
          className="min-h-[300px]"
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
          isLoading={coreQuery.isLoading}
          isFetching={coreQuery.isFetching}
          error={coreQuery.error as AnalyticsCardError | null}
          isEmpty={(coreData?.top_asns?.length ?? 0) === 0}
          className="min-h-[300px]"
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
          isLoading={distributionsQuery.isLoading}
          isFetching={distributionsQuery.isFetching}
          error={distributionsQuery.error as AnalyticsCardError | null}
          isEmpty={(distData?.ttl_dist?.length ?? 0) === 0}
          className="h-[360px]"
          contentClassName="p-2"
          helpContent={<CacheTtlHelp />}
        >
          <PlotlyChart
            data={ttlDistData}
            layout={TTL_DIST_LAYOUT}
            height="100%"
          />        </AnalyticsCard>

        <AnalyticsCard
          title="Origin vs Edge Processing (ms)"
          icon={<Zap className="h-4 w-4" />}
          isLoading={coreQuery.isLoading}
          isFetching={coreQuery.isFetching}
          error={coreQuery.error as AnalyticsCardError | null}
          isEmpty={(coreData?.scatter?.length ?? 0) === 0}
          className="h-[360px]"
          contentClassName="p-2"
          helpContent={<OriginVsEdgeHelp />}
        >
          <PlotlyChart
            data={scatterData}
            layout={SCATTER_LAYOUT}
            height="100%"
          />        </AnalyticsCard>
      </div>
    </>
  )
}

export default function PerformanceClient() {
  const getFieldLabel = useFieldLabel()

  const [urlVisibility, setUrlVisibility, onUrlVisChange] = useColumnVisibility()
  const [asnVisibility, setAsnVisibility, onAsnVisChange] = useColumnVisibility()

  // Time-range wire inputs (lib/range-wire.ts; resolved in PerformanceBody). A
  // quick-preset pill / the cold-load default → a server-reproducible token
  // ("24h"), keeping the SSR seed key byte-matched. A custom absolute range
  // (relativeRange null + isAutoRange false) → the explicit start/end bounds, so
  // the latency tables/distributions reflect exactly the selected window.
  const relativeRange = useFilterStore((s) => s.relativeRange)
  const isAutoRange = useFilterStore((s) => s.isAutoRange)
  const hasSyncedExtents = useFilterStore((s) => s.hasSyncedExtents)
  const storeEndTime = useFilterStore((s) => s.endTime)
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const queryClient = useQueryClient()
  // Anchor the keyed path to the SELECTED window's end, not to mount time: a
  // preset clicked in a long-lived tab re-anchors at click time so the scan
  // matches the hard-clamped x-axis (a mount-pinned anchor could leave the
  // short 1h..12h presets fully disjoint from the display). Memoized on endTime
  // so a re-render can't advance the key; cold-load endTime ≈ mount now, so the
  // SSR seed (same 60s floor, quantizeAnchor ≡ backend quantize_anchor) still
  // byte-matches within the quantum.
  //
  // Before FilterBar's extents-sync effect runs (isAutoRange && !hasSyncedExtents),
  // prefer the anchor implied by the service's real log extents — already warm
  // in the cache (root layout seeds ['log-extents', sid] on every route) — via
  // the SAME resolveSnappedWindow the SSR seed and FilterBar's autoSetRange both
  // use. Feeds BOTH the core and distributions query keys below since they
  // share this single anchor. Once hasSyncedExtents flips true, autoSetRange
  // has already written the same value into storeEndTime, so the fallback
  // branch recomputes identically.
  const anchor = React.useMemo(() => {
    if (isAutoRange && !hasSyncedExtents && activeServiceId) {
      const logExtents = queryClient.getQueryData(['log-extents', activeServiceId]) as LogExtents | undefined
      const snapped = resolveSnappedWindow(logExtents, new Date())
      if (snapped) return quantizeAnchor(snapped.end)
    }
    return quantizeAnchor(storeEndTime)
  }, [isAutoRange, hasSyncedExtents, activeServiceId, storeEndTime, queryClient])

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
