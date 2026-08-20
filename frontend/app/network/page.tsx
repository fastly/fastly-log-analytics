'use client'

import { GlobalHealthHelp, AvgRttHelp, WorstAsnHelp, WorstRegionHelp, HeatmapHelp, AsnLeaderboardHelp, MetroLeaderboardHelp, ShieldingHelp, NetworkQualityHelp, HealthBadge, SHIELDING_COLUMNS, getShieldingLabels } from "./help-content";
import React, { useState } from 'react'
import { PopHealthHeatmap } from '@/components/network/PopHealthHeatmap'
import { DataTable, ColumnVisibilityDropdown } from '@/components/DataTable'
import { client } from '@/lib/api'
import type { components } from '@/types/api'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { useColumnVisibility } from '@/hooks/useColumnVisibility'
import { useActiveLogFields } from '@/hooks/useActiveLogFields'
import { FilterValueCell } from '@/components/FilterValueCell'
import { PopLabel } from '@/components/PopLabel'
import { downloadAsCsv } from '@/lib/utils'
import { cn } from '@/lib/utils'
import dynamic from 'next/dynamic'
import { Skeleton } from '@/components/ui/skeleton'
// PlotlyChart renders conditionally on heatmapData (the RTT heatmap card).
// Static-importing it dragged the ~1MB plotly chunk into the critical path
// for every /network cold load even when the heatmap wasn't being rendered.
// Dynamic-import defers the chunk to when the heatmap card actually mounts.
// Unified loading idiom: Skeleton blocks for every dynamic-imported heavy
// visualization, matching the /security page pattern (also Skeleton) and
// the route-level PageSkeleton family. Replaces the prior bespoke
// "Loading chart..." / "Loading Map..." plain-text placeholders — the
// audit flagged the inconsistency across the analyst routes.
const PlotlyChart = dynamic(
  () => import('@/components/PlotlyChart').then(mod => mod.PlotlyChart),
  {
    ssr: false,
    loading: () => <Skeleton className="w-full h-[300px]" />,
  },
)
const NetworkMap = dynamic(() => import('@/components/Map/NetworkMap').then(mod => mod.NetworkMap), {
  ssr: false,
  loading: () => <Skeleton className="w-full h-full min-h-[400px]" />,
})
const ShieldingMap = dynamic(() => import('@/components/Map/ShieldingMap').then(mod => mod.ShieldingMap), {
  ssr: false,
  loading: () => <Skeleton className="w-full h-[420px] rounded-xl" />,
})
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { quantizeAnchor } from '@/lib/time-window'
import { resolveRangeWire } from '@/lib/range-wire'
import { useFilterStore } from '@/stores/filterStore'
import { LazyMount } from '@/components/LazyMount'
import { StatCard } from '@/components/ui/stat-card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { SkeletonGrid } from '@/components/ui/skeleton-grid'
import { Network as NetworkIcon, AlertCircle, Globe, Zap, Activity, Shield, Download, Info } from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { adjustShieldingRows, SHIELDING_MIN_REQUESTS_OPTIONS, SHIELDING_MIN_REQUESTS_DEFAULT } from './shielding-rows'

// Static — module-level keeps the reference stable across renders without
// adding a hook call (we can't safely add hooks after the render-prop's
// early-return at `data.available === false`).
const HEATMAP_LAYOUT = { xaxis: { tickangle: -45 } }

// RTT-vs-TTFB scatter axis titles. Module-scope for stable PlotlyChart identity.
const QUALITY_SCATTER_LAYOUT = {
  xaxis: { title: { text: 'TCP RTT (ms)' } },
  yaxis: { title: { text: 'TTFB (ms)' } },
}

type QualityBarRow = { value?: string; label: string; rtt_ms: number; reqs: number }

// Horizontal RTT bar list — a lightweight alternative to a Plotly bar trace for
// the four Network Quality breakdowns (by country / ASN / region / POP). Bars
// are scaled to the largest RTT in the set. When `filterColumn` is given each
// row label becomes a click-to-filter cell, matching the ASN leaderboard.
function RttBarList({ rows, filterColumn }: { rows: QualityBarRow[]; filterColumn?: string }) {
  if (!rows?.length) {
    return <div className="p-6 text-center text-sm text-muted-foreground">No data in this range</div>
  }
  const max = Math.max(...rows.map((r) => r.rtt_ms), 1)
  return (
    <div className="space-y-1.5 p-3">
      {rows.map((r) => {
        // `value` is the raw group key (ASN number / PoP code) used for
        // click-to-filter; `label` is the display (e.g. "Comcast (7922)" for
        // ASN). PoP rows render via the shared <PopLabel> so they read
        // "SJC (San Jose, CA - USA)" consistently with everywhere else.
        const filterVal = r.value ?? r.label
        const display = filterColumn === 'pop' ? <PopLabel code={filterVal} /> : r.label
        return (
          <div key={filterVal} className="flex items-center gap-2 text-xs">
            <div className="w-48 truncate shrink-0" title={r.label}>
              {filterColumn ? (
                <FilterValueCell filters={[{ column: filterColumn, value: filterVal }]} display={display} />
              ) : (
                <span>{display}</span>
              )}
            </div>
            <div className="flex-1 h-4 bg-muted/40 rounded-sm overflow-hidden">
              <div className="h-full bg-primary/70" style={{ width: `${(r.rtt_ms / max) * 100}%` }} />
            </div>
            <div className="w-16 text-right font-mono shrink-0">{r.rtt_ms.toFixed(1)}ms</div>
            <div className="w-16 text-right font-mono text-muted-foreground shrink-0">{(r.reqs ?? 0).toLocaleString()}</div>
          </div>
        )
      })}
    </div>
  )
}

// Per-section field lists. The backend (slice 2) gates each section block
// on `sections is None or 'name' in sections`. Splitting the monolithic
// /api/network-health into three parallel POSTs lets the slow shielding
// scan (origin repo) and the heavy map payload paint independently from
// the small summary + leaderboard cards above the fold.
//   core      → summary + leaderboard + metro_leaderboard (shared temp)
//   map       → heatmap/buckets + cities/map_buckets (big payload)
//   shielding → shielding_analysis (origin path, most-likely-empty card)
type NetworkSections = NonNullable<components['schemas']['NetworkHealthRequest']['sections']>
const NETWORK_CORE_SECTIONS: NetworkSections = ['summary', 'leaderboard', 'metro_leaderboard']
const NETWORK_MAP_SECTIONS: NetworkSections = ['heatmap', 'buckets', 'cities', 'map_buckets']
const NETWORK_SHIELDING_SECTIONS: NetworkSections = ['shielding_analysis']

export default function NetworkPage() {
  const [metric, setMetric] = useState('health_score')
  const [mapAsn, setMapAsn] = useState('all')
  const [animBucketSeconds, setAnimBucketSeconds] = useState(5)
  const [regionCountry, setRegionCountry] = useState('US')
  const [shieldingVisibility, setShieldingVisibility, onShieldingVisChange] = useColumnVisibility()
  // Min edge→shield requests before a route's transit median is trusted enough
  // to colour-grade / flag. Recomputed client-side from the backend's
  // anomaly_eligible verdict — no refetch (see ./shielding-rows).
  const [minRequests, setMinRequests] = useState(SHIELDING_MIN_REQUESTS_DEFAULT)

  // Time-range wire inputs (the network 30d analyst cliff fix + lib/range-wire).
  // The wire is resolved inside the render-prop body where startTime/endTime are
  // available. A quick-preset pill / the cold-load default → a server-
  // reproducible token ("24h"), which the backend keyed path (routers/network.py)
  // resolves into the scan window and which keeps the response memo — keyed on
  // (token, quantized_anchor, invite-clamp fingerprint) — STABLE within the 60s
  // quantum so the ~26s 30d pipeline can serve across rolling-minute reloads. A
  // custom absolute range (relativeRange null + isAutoRange false) → no token, so
  // the backend falls back to the absolute start/end we always send, scanning
  // exactly what the chart x-axis (hard-clamped to startTime/endTime) displays.
  const relativeRange = useFilterStore((s) => s.relativeRange)
  const isAutoRange = useFilterStore((s) => s.isAutoRange)
  const storeEndTime = useFilterStore((s) => s.endTime)
  // Anchor the keyed path to the SELECTED window's end (floored to the 60s
  // grid), not to mount time: every explicit range selection writes a fresh
  // endTime (ReportLayout's ctx times ARE the store times), so a preset clicked
  // in a long-lived tab re-anchors at click time and scans [click−N, click] —
  // matching the hard-clamped x-axis — instead of a mount-pinned window that,
  // for the short presets (1h..12h), could be fully disjoint from the display.
  // Memoized on endTime so a cross-minute re-render still can't advance the key
  // (no scan refire); on cold load endTime is the store-init default (≈ mount
  // now), so behavior there is unchanged. quantizeAnchor mirrors the backend's
  // quantize_anchor byte-for-byte (lib/time-window.ts ≡ backend/utils/time_window.py).
  const anchor = React.useMemo(() => quantizeAnchor(storeEndTime), [storeEndTime])

  return (
    <ReportLayout
      title="Network & ASN Health"
      description="Analysis of TCP performance, packet loss, and jitter by ASN and geography."
      icon={NetworkIcon}
    >
      {({
        startTime,
        endTime,
        activeServiceId,
        filterPayload,
        bucketSeconds: chartBucketSeconds,
        intervalButtons,
      }) => {

  // Distinguish "field group not enabled" from "enabled but no data in this
  // window yet" so a low-traffic/fresh service doesn't read as misconfigured.
  // Called above the `data?.available === false` early-return to keep hook
  // order stable across renders.
  const { isFieldActive } = useActiveLogFields()

  // Token mode → range_token drives the scan (server-reproducible, memo-stable);
  // custom-absolute mode → rangeToken is null so the backend uses the start/end
  // bounds the bodies always send. The cache keys already carry startTime/endTime,
  // so a custom range gets its own entry.
  const { rangeToken } = resolveRangeWire({ relativeRange, isAutoRange, startTime, endTime, anchor })

  const coreQuery = useServiceQuery(
    ['network', 'health', 'core', activeServiceId, startTime, endTime, filterPayload, animBucketSeconds, mapAsn, metric, rangeToken, anchor],
    async ({ signal }) => {
      const { data } = await client.POST("/api/network-health", { signal,
        body: {
          start_time: startTime!,
          end_time: endTime!,
          filters: filterPayload,
          metric,
          bucket_seconds: animBucketSeconds,
          top_n: 30,
          map_asn: mapAsn,
          sections: NETWORK_CORE_SECTIONS,
          // Keyed path: server resolves the scan window from (range_token,
          // quantized anchor) and ignores the absolute start/end above (kept for
          // the legacy fallback when an old client omits the token).
          range_token: rangeToken,
          anchor,
        }
      })
      return data
    }
  )

  const mapQuery = useServiceQuery(
    ['network', 'health', 'map', activeServiceId, startTime, endTime, filterPayload, animBucketSeconds, mapAsn, metric, rangeToken, anchor],
    async ({ signal }) => {
      const { data } = await client.POST("/api/network-health", { signal,
        body: {
          start_time: startTime!,
          end_time: endTime!,
          filters: filterPayload,
          metric,
          bucket_seconds: animBucketSeconds,
          top_n: 30,
          map_asn: mapAsn,
          sections: NETWORK_MAP_SECTIONS,
          range_token: rangeToken,
          anchor,
        }
      })
      return data
    }
  )

  const shieldingQuery = useServiceQuery(
    ['network', 'health', 'shielding', activeServiceId, startTime, endTime, filterPayload, rangeToken, anchor],
    async ({ signal }) => {
      const { data } = await client.POST("/api/network-health", { signal,
        body: {
          start_time: startTime!,
          end_time: endTime!,
          filters: filterPayload,
          metric,
          bucket_seconds: animBucketSeconds,
          top_n: 30,
          map_asn: mapAsn,
          sections: NETWORK_SHIELDING_SECTIONS,
          range_token: rangeToken,
          anchor,
        }
      })
      return data
    }
  )

  // TCP-RTT quality breakdowns (by country/ASN/region/POP) + RTT-vs-TTFB
  // scatter. Separate endpoint from /network-health; `region_country` scopes
  // the by_region bars and is the only param that varies independently.
  const qualityQuery = useServiceQuery(
    ['network', 'quality', activeServiceId, startTime, endTime, filterPayload, regionCountry],
    async ({ signal }) => {
      const { data } = await client.POST("/api/network-quality", { signal,
        body: {
          start_time: startTime!,
          end_time: endTime!,
          filters: filterPayload,
          region_country: regionCountry,
        }
      })
      return data
    }
  )

  // `available`/`has_metro`/`reason` come back on every section response
  // (the backend sets them on the base get_health result). Use core as
  // the canonical source for these top-level signals.
  const data = coreQuery.data
  const mapData = mapQuery.data
  const qualityData = qualityQuery.data
  const isCoreLoadingInitial = coreQuery.isLoading || (coreQuery.isFetching && !coreQuery.data)
  const isMapLoadingInitial = mapQuery.isLoading || (mapQuery.isFetching && !mapQuery.data)
  const isShieldingLoadingInitial = shieldingQuery.isLoading || (shieldingQuery.isFetching && !shieldingQuery.data)
  const isQualityLoadingInitial = qualityQuery.isLoading || (qualityQuery.isFetching && !qualityQuery.data)

  const shieldingData = shieldingQuery.data?.shielding_analysis as any
  // Apply the user-chosen min-requests floor to greying/flagging without a
  // refetch. Feeds the map, the table, and the CSV export so all three agree.
  const adjustedShieldingRows = React.useMemo(
    () => adjustShieldingRows(shieldingData?.rows ?? [], minRequests),
    [shieldingData?.rows, minRequests],
  )

  const qualityScatterTrace = React.useMemo(() => {
    const pts = (qualityData?.scatter ?? []) as Array<{ rtt_ms: number; ttfb_ms: number }>
    if (!pts.length) return []
    return [{
      type: 'scattergl',
      mode: 'markers',
      x: pts.map((p) => p.rtt_ms),
      y: pts.map((p) => p.ttfb_ms),
      marker: { size: 4, opacity: 0.45 },
      hovertemplate: 'RTT %{x}ms<br>TTFB %{y}ms<extra></extra>',
    }]
  }, [qualityData?.scatter])

  const asnOptions = React.useMemo(() => {
    if (!data?.leaderboard) return []
    return data.leaderboard.map((a: any) => ({ value: String(a.asn), label: a.label }))
  }, [data?.leaderboard])

  // Pre-memoise the ASN leaderboard rows so toggling the metric / mapAsn
  // selectors (which re-render the parent) doesn't rebuild the
  // 30-row × 7-column subtree every time. Row identity is keyed on
  // `data?.leaderboard` so a real data refetch still re-renders.
  const asnLeaderboardRows = React.useMemo(() => {
    return (data?.leaderboard ?? []).map((asn: any) => {
      const delta = (asn.health_score_now ?? 0) - (asn.health_score_1h_ago ?? 0)
      return (
        <tr key={asn.asn} className="border-b last:border-0 hover:bg-muted/50 transition-colors">
          <td className="px-4 py-3 font-medium">
            <FilterValueCell
              filters={[{ column: 'asn', value: String(asn.asn) }]}
              display={asn.label}
            />
          </td>
          <td className="px-4 py-3 text-right font-mono text-xs">{(asn.total_reqs ?? 0).toLocaleString()}</td>
          <td className="px-4 py-3 text-right"><HealthBadge score={asn.health_score_now} /></td>
          <td className="px-4 py-3 text-right font-mono text-xs">
            {asn.p95_rtt_us != null ? `${(asn.p95_rtt_us / 1000).toFixed(1)}ms` : '—'}
          </td>
          <td className="px-4 py-3 text-right font-mono text-xs">
            {asn.p99_rtt_us != null ? `${(asn.p99_rtt_us / 1000).toFixed(1)}ms` : '—'}
          </td>
          <td className="px-4 py-3 text-right font-mono text-xs">
            <span className={delta > 0 ? 'text-green-500' : delta < 0 ? 'text-red-500' : 'text-muted-foreground'}>
              {delta > 0 ? '+' : ''}{delta.toFixed(1)}
            </span>
          </td>
          <td className="px-4 py-3 text-right">
            <Badge
              variant={asn.trend === 'degrading' ? 'destructive' : 'outline'}
              className={cn("text-[10px]", asn.trend === 'improving' ? 'text-green-600 dark:text-green-400 border-green-300 dark:border-green-700' : '')}
            >
              {(asn.trend ?? 'stable').toUpperCase()}
            </Badge>
          </td>
        </tr>
      )
    })
  }, [data?.leaderboard])

  const heatmapData = React.useMemo(() => {
    if (!mapData?.heatmap?.length || !mapData.buckets?.length) return null
    const yLabels = mapData.heatmap.map((d: any) => d.label)
    const xBuckets = mapData.buckets
    const z = mapData.heatmap.map((row: any) => {
      const byBucket: Record<string, number | null> = {}
      row.buckets?.forEach((b: any) => { byBucket[b.bucket] = b.health_score })
      return xBuckets.map((bk: string) => byBucket[bk] ?? null)
    })
    return { x: xBuckets, y: yLabels, z }
  }, [mapData?.heatmap, mapData?.buckets])

  // Stable ref so PlotlyChart's React.memo treats unrelated parent re-renders
  // as no-ops. Must live ABOVE the `data.available === false` early-return so
  // the hook order is identical on every render.
  const heatmapTrace = React.useMemo(() => (
    heatmapData ? [{
      type: 'heatmap',
      x: heatmapData.x,
      y: heatmapData.y,
      z: heatmapData.z,
      colorscale: 'RdYlGn',
      zmin: 0,
      zmax: 100,
      colorbar: { title: 'Score', thickness: 12 },
      hovertemplate: '<b>%{y}</b><br>%{x}<br>Score: %{z}<extra></extra>',
    }] : []
  ), [heatmapData])

        if (data?.available === false) {
          return (
            <div className="flex flex-col items-center justify-center h-[50vh] text-center max-w-md mx-auto">
              <AlertCircle className="h-10 w-10 text-yellow-500 mb-4" />
              <h2 className="text-xl font-semibold">Network Metrics Unavailable</h2>
              <p className="text-muted-foreground mt-2">{data.reason || 'Log fields required for network analysis are not enabled.'}</p>
            </div>
          )
        }

        // `summary` is an untyped object (`{}`) in the generated schema, so
        // narrow it here at the opaque boundary. Top-level response keys
        // (summary/leaderboard/by_*/available/…) are now type-checked since the
        // queries dropped their `return data as any` — a backend rename of those
        // keys is a compile error.
        const summary = (data?.summary ?? {}) as {
          global_health_score?: number
          avg_rtt_ms?: number
          worst_asn?: { label?: string; score?: number }
          worst_country?: { label?: string; score?: number }
        }

  // heatmapTrace + HEATMAP_LAYOUT defined above the early-return / at
  // module scope — keeps hook order stable and gives PlotlyChart stable
  // identity for its React.memo shallow-compare.

        return (
          <>
      {coreQuery.error && !coreQuery.data && (
        <div className="rounded-lg border border-destructive/50 bg-destructive/10 text-destructive p-3 text-sm flex items-start gap-2">
          <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
          <div>
            <div className="font-medium">Network health unavailable</div>
            <div className="text-xs opacity-80">
              {coreQuery.error instanceof Error ? coreQuery.error.message : 'The backend returned an error. Leaderboards below may show incorrect "fields not enabled" hints.'}
            </div>
          </div>
        </div>
      )}
      {/* ── Map ── */}
      <NetworkMap
        data={mapData}
        isLoading={isMapLoadingInitial}
        error={mapQuery.error}
        metric={metric}
        onMetricChange={setMetric}
        bucketSeconds={animBucketSeconds}
        onBucketChange={setAnimBucketSeconds}
        mapAsn={mapAsn}
        onAsnChange={setMapAsn}
        asnOptions={asnOptions}
      />

      {/* ── Summary cards ── */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          title="Global Health"
          icon={Activity}
          loading={isCoreLoadingInitial}
          value={<>{summary.global_health_score ?? '--'}/100</>}
          sub="Average across top ASNs"
          helpTitle="Global Health Score"
          helpContent={<GlobalHealthHelp />}
        />
        <StatCard
          title="Avg RTT"
          icon={Zap}
          loading={isCoreLoadingInitial}
          value={<>{summary.avg_rtt_ms ?? '--'}ms</>}
          sub="Across all regions"
          helpTitle="Average RTT"
          helpContent={<AvgRttHelp />}
        />
        <StatCard
          title="Worst ASN"
          icon={AlertCircle}
          iconClassName="text-red-500"
          loading={isCoreLoadingInitial}
          value={<span className="text-sm truncate block">{summary.worst_asn?.label ?? '--'}</span>}
          sub={<span className="text-red-500 font-medium">Score: {summary.worst_asn?.score ?? '--'}</span>}
          helpTitle="Worst-Performing ASN"
          helpContent={<WorstAsnHelp />}
        />
        <StatCard
          title="Worst Region"
          icon={Globe}
          loading={isCoreLoadingInitial}
          value={<span className="text-sm truncate block">{summary.worst_country?.label ?? '--'}</span>}
          sub={<span className="text-red-500 font-medium">Score: {summary.worst_country?.score ?? '--'}</span>}
          helpTitle="Worst-Performing Region"
          helpContent={<WorstRegionHelp />}
        />
      </div>

      <div className="my-6">
        {activeServiceId && (
          <PopHealthHeatmap serviceId={activeServiceId} startTime={startTime} endTime={endTime} />
        )}
      </div>

      {/* ── Heatmap ── */}
      {heatmapData && (
        <AnalyticsCard
          title="ASN Health Score over Time"
          helpTitle="ASN Health Score over Time"
          helpContent={<HeatmapHelp />}
          isFetching={mapQuery.isFetching}
          error={mapQuery.error as AnalyticsCardError | null}
        >
          <PlotlyChart data={heatmapTrace as any[]} layout={HEATMAP_LAYOUT} height={Math.min(60 + heatmapData.y.length * 28, 600)} />
        </AnalyticsCard>
      )}

      {/* ── Edge → Shield POP Map ── */}
      {(shieldingData || isShieldingLoadingInitial || shieldingQuery.error) && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <AnalyticsCard
            title="Edge → Shield Transit Map"
            helpTitle="Shielding Analysis"
            helpContent={<ShieldingHelp />}
            isFetching={shieldingQuery.isFetching}
            error={shieldingQuery.error as AnalyticsCardError | null}
          >
            {/* ShieldingMap eagerly constructs a MapLibre GL map (WebGL
                context + tile fetches) on mount. It sits below the fold on
                /network, so defer the mount until it nears the viewport.
                minHeight matches the component's own h-[420px] skeleton so
                the placeholder→map swap introduces no new CLS; the
                mapContainer ref attaches on the deferred mount pass. */}
            <LazyMount minHeight={420}>
              <ShieldingMap
                rows={adjustedShieldingRows}
                isLoading={isShieldingLoadingInitial}
                edgeOnly={Boolean((shieldingData as { edge_only?: boolean } | undefined)?.edge_only) && !isFieldActive('prid')}
                errored={Boolean((shieldingData as { error?: boolean } | undefined)?.error)}
                expandable
              />
            </LazyMount>
          </AnalyticsCard>

          <AnalyticsCard
            title="Shielding Analysis"
            contentClassName="p-0"
            helpTitle="Shielding Analysis"
            helpContent={<ShieldingHelp />}
            isFetching={shieldingQuery.isFetching}
            error={shieldingQuery.error as AnalyticsCardError | null}
            headerAction={
              <div className="flex items-center gap-2">
                {/* Min-requests floor: greys + un-flags routes whose median
                    transit is too small a sample to trust. Recomputed
                    client-side (./shielding-rows) from anomaly_eligible. */}
                <div className="flex items-center gap-1.5">
                  <span className="hidden sm:inline text-[11px] uppercase font-semibold tracking-wide text-muted-foreground">
                    Min reqs
                  </span>
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger
                        render={
                          <span
                            className="inline-flex items-center text-muted-foreground hover:text-foreground transition-colors cursor-help"
                          />
                        }
                      >
                        <Info className="w-3.5 h-3.5 opacity-60" />
                        {/* Name the trigger via text content, not aria-label:
                            axe flags aria-prohibited-attr for aria-label on a
                            roleless <span> (the Base UI render override). */}
                        <span className="sr-only">What does the minimum requests floor do?</span>
                      </TooltipTrigger>
                      <TooltipContent side="top" className="max-w-[260px] text-xs leading-relaxed">
                        Routes with fewer than this many edge→shield requests are greyed and never flagged as “suboptimal peering” — their transit median is too small a sample to trust. Raise it on busy services; lower it (or pick “No minimum”) to scrutinize quiet routes.
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                  <Select value={String(minRequests)} onValueChange={(v) => v && setMinRequests(Number(v))}>
                    <SelectTrigger className="h-8 w-[124px] text-xs" aria-label="Minimum requests to flag a shielding route">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {SHIELDING_MIN_REQUESTS_OPTIONS.map((o) => (
                        <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8"
                  onClick={() => {
                    if (!adjustedShieldingRows.length) return
                    downloadAsCsv(
                      adjustedShieldingRows,
                      // Include distance_km + anomaly_static so the export
                      // carries the same signals the map/table show (L8), plus
                      // low_sample so a consumer can tell which rows are below
                      // the (user-chosen) anomaly-flag floor. (low-sample gating)
                      ['edge_pop', 'shield_pop', 'requests', 'p50_ms', 'p95_ms', 'p99_ms', 'distance_km', 'light_speed_rtt_ms', 'efficiency_ratio', 'anomaly_static', 'low_sample'],
                      'shielding_analysis.csv'
                    )
                  }}
                  disabled={!shieldingData?.has_data}
                >
                  <Download className="w-3.5 h-3.5 mr-2" />
                  Export
                </Button>
                <ColumnVisibilityDropdown
                  columns={getShieldingLabels(['edge_pop', 'shield_pop', 'requests', 'p50_ms', 'p95_ms', 'p99_ms', 'light_speed_rtt_ms', 'efficiency_ratio'])}
                  visibility={shieldingVisibility}
                  onChange={onShieldingVisChange}
                />
              </div>
            }
          >
            <div className="flex flex-col h-[420px] overflow-auto">
              {shieldingData?.has_data ? (
                <>
                  <DataTable
                    columns={SHIELDING_COLUMNS}
                    data={adjustedShieldingRows}
                    hideToolbar
                    columnVisibility={shieldingVisibility}
                    onColumnVisibilityChange={setShieldingVisibility}
                  />
                  {/* M1: the backend returns a (top-by-volume ∪ top-by-overhead)
                      subset when there are more routes than the cap, so a
                      buried anomaly still surfaces — say so instead of implying
                      the table is complete. (shieldingData is already `any`.) */}
                  {shieldingData?.truncated && (
                    <p className="px-4 py-2 text-[11px] text-muted-foreground border-t bg-muted/20">
                      Showing {(shieldingData?.rows?.length ?? 0).toLocaleString()} of {Number(shieldingData.total_routes ?? 0).toLocaleString()} routes — top by request volume and by transit overhead.
                    </p>
                  )}
                </>
              ) : (
                <div className="flex flex-col items-center justify-center py-12 h-full text-center px-4 gap-1">
                  <Shield className={cn("h-8 w-8 mb-2 opacity-20", shieldingData?.error ? 'text-destructive opacity-30' : 'text-muted-foreground')} />
                  {/* Only call it "edge-only" when origin/shield logging (Group L)
                      is genuinely NOT enabled. With full logging on, an empty
                      shield result just means no shield traffic in this window —
                      not an edge-only configuration. */}
                  {shieldingData?.error ? (
                    <>
                      <p className="text-sm text-destructive font-medium">Shielding analysis unavailable</p>
                      <p className="text-xs text-muted-foreground max-w-sm">The analysis failed to compute for this window (server-side error, not an absence of data). Try again or narrow the time range.</p>
                    </>
                  ) : ((shieldingData as any)?.edge_only && !isFieldActive('prid')) ? (
                    <>
                      <p className="text-sm text-muted-foreground font-medium">Edge-only logging detected</p>
                      <p className="text-xs text-muted-foreground max-w-sm">Edge-to-shield transit analysis requires log lines from both edge and shield POPs. Your service is currently logging edge requests only — enable full (non-edge-only) logging to use this analysis.</p>
                    </>
                  ) : (shieldingData as any)?.requires_fields ? (
                    <p className="text-sm text-muted-foreground italic">Required fields missing: {(shieldingData as any).requires_fields.join(', ')}</p>
                  ) : (
                    <p className="text-sm text-muted-foreground italic">No shielding data detected in this time range.</p>
                  )}
                </div>
              )}
            </div>
          </AnalyticsCard>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* ── ASN Leaderboard ── */}
        <AnalyticsCard
          title="ASN Performance Leaderboard"
          className="flex flex-col"
          contentClassName="p-0 flex-1"
          helpTitle="ASN Performance Leaderboard"
          helpContent={<AsnLeaderboardHelp />}
          isFetching={coreQuery.isFetching}
          error={coreQuery.error as AnalyticsCardError | null}
        >
          {isCoreLoadingInitial ? (
            <div className="space-y-2 p-4">
              <SkeletonGrid count={5} height="48px" className="rounded-md" />
            </div>
          ) : (!data?.leaderboard || data.leaderboard.length === 0) ? (
            <div className="p-8 text-center text-sm text-muted-foreground">
              <p className="mb-1">No data available</p>
              <p className="text-[10px] opacity-70">
                {isFieldActive('asn')
                  ? "No ASN data in this time range."
                  : "Requires Network Quality (Group F) fields to be enabled in Fastly logging."}
              </p>
            </div>
          ) : (
            <div className="relative w-full overflow-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b bg-muted/30">
                    <th className="h-10 px-4 text-left font-medium text-muted-foreground">ASN</th>
                    <th className="h-10 px-4 text-right font-medium text-muted-foreground">Requests</th>
                    <th className="h-10 px-4 text-right font-medium text-muted-foreground">Health Score</th>
                    <th className="h-10 px-4 text-right font-medium text-muted-foreground">P95 RTT</th>
                    <th className="h-10 px-4 text-right font-medium text-muted-foreground">P99 RTT</th>
                    <th className="h-10 px-4 text-right font-medium text-muted-foreground">1h Change</th>
                    <th className="h-10 px-4 text-right font-medium text-muted-foreground">Trend</th>
                  </tr>
                </thead>
                <tbody>{asnLeaderboardRows}</tbody>
              </table>
            </div>
          )}
        </AnalyticsCard>

        {/* ── Metro Leaderboard ── */}
        {data?.has_metro && (
          <AnalyticsCard
            title="Metro / Region Leaderboard"
            className="flex flex-col"
            contentClassName="p-0 flex-1"
            helpTitle="Metro / Region Leaderboard"
            helpContent={<MetroLeaderboardHelp />}
            isFetching={coreQuery.isFetching}
            error={coreQuery.error as AnalyticsCardError | null}
          >
            {isCoreLoadingInitial ? (
              <div className="space-y-2 p-4">
                <SkeletonGrid count={5} height="48px" className="rounded-md" />
              </div>
            ) : (!data?.metro_leaderboard || data.metro_leaderboard.length === 0) ? (
              <div className="p-8 text-center text-sm text-muted-foreground">
                <p className="mb-1">No data available</p>
                <p className="text-[10px] opacity-70">
                  {isFieldActive('metro')
                    ? "No metro data in this time range."
                    : "Requires Geolocation (Group D/E) fields to be enabled in Fastly logging."}
                </p>
              </div>
            ) : (
              <div className="relative w-full overflow-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b bg-muted/30">
                      <th className="h-10 px-4 text-left font-medium text-muted-foreground">Metro / City</th>
                      <th className="h-10 px-4 text-right font-medium text-muted-foreground">Requests</th>
                      <th className="h-10 px-4 text-right font-medium text-muted-foreground">Health Score</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(data?.metro_leaderboard ?? []).map((m: any) => (
                      <tr key={`${m.city}-${m.region}-${m.country}`} className="border-b last:border-0 hover:bg-muted/50 transition-colors">
                        <td className="px-4 py-3 font-medium">
                          {m.raw_city ? (
                            <FilterValueCell
                              filters={[
                                { column: 'city', value: m.raw_city },
                                ...(m.region ? [{ column: 'region', value: m.region }] : []),
                                ...(m.country ? [{ column: 'country', value: m.country }] : []),
                              ]}
                              display={m.city}
                            />
                          ) : (
                            <span>{m.city}</span>
                          )}
                        </td>
                        <td className="px-4 py-3 text-right font-mono text-xs">{(m.total_reqs ?? 0).toLocaleString()}</td>
                        <td className="px-4 py-3 text-right"><HealthBadge score={m.health_score} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </AnalyticsCard>
        )}
      </div>

      {/* ── Network Quality (TCP RTT breakdowns + RTT-vs-TTFB scatter) ── */}
      {(isQualityLoadingInitial || qualityData?.available || qualityQuery.error) && (
        <div className="space-y-4">
          <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
            Network Quality
          </h2>
          {isQualityLoadingInitial ? (
            <SkeletonGrid count={4} height="180px" className="rounded-xl" />
          ) : (
            <>
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <AnalyticsCard
                  title="Avg RTT by Country"
                  contentClassName="p-0"
                  helpTitle="Network Quality"
                  helpContent={<NetworkQualityHelp />}
                  isFetching={qualityQuery.isFetching}
                  error={qualityQuery.error as AnalyticsCardError | null}
                >
                  <RttBarList rows={(qualityData?.by_country ?? []) as QualityBarRow[]} filterColumn="country" />
                </AnalyticsCard>

                <AnalyticsCard
                  title="Avg RTT by ASN"
                  contentClassName="p-0"
                  helpTitle="Network Quality"
                  helpContent={<NetworkQualityHelp />}
                  isFetching={qualityQuery.isFetching}
                  error={qualityQuery.error as AnalyticsCardError | null}
                >
                  <RttBarList rows={(qualityData?.by_asn ?? []) as QualityBarRow[]} filterColumn="asn" />
                </AnalyticsCard>

                <AnalyticsCard
                  title="Avg RTT by Region"
                  contentClassName="p-0"
                  helpTitle="Network Quality"
                  helpContent={<NetworkQualityHelp />}
                  isFetching={qualityQuery.isFetching}
                  error={qualityQuery.error as AnalyticsCardError | null}
                  headerAction={
                    <select
                      aria-label="Region breakdown country"
                      value={regionCountry}
                      onChange={(e) => setRegionCountry(e.target.value)}
                      className="h-7 rounded-md border bg-background px-2 text-xs"
                    >
                      {(qualityData?.countries ?? []).length === 0 && (
                        <option value={regionCountry}>{regionCountry}</option>
                      )}
                      {(qualityData?.countries ?? []).map((c: string) => (
                        <option key={c} value={c}>{c}</option>
                      ))}
                    </select>
                  }
                >
                  <RttBarList rows={(qualityData?.by_region ?? []) as QualityBarRow[]} filterColumn="region" />
                </AnalyticsCard>

                <AnalyticsCard
                  title="Avg RTT by POP"
                  contentClassName="p-0"
                  helpTitle="Network Quality"
                  helpContent={<NetworkQualityHelp />}
                  isFetching={qualityQuery.isFetching}
                  error={qualityQuery.error as AnalyticsCardError | null}
                >
                  <RttBarList rows={(qualityData?.by_pop ?? []) as QualityBarRow[]} filterColumn="pop" />
                </AnalyticsCard>
              </div>

              {qualityScatterTrace.length > 0 && (
                <AnalyticsCard
                  title="RTT vs TTFB"
                  helpTitle="Network Quality"
                  helpContent={<NetworkQualityHelp />}
                  isFetching={qualityQuery.isFetching}
                  error={qualityQuery.error as AnalyticsCardError | null}
                >
                  <PlotlyChart data={qualityScatterTrace as any[]} layout={QUALITY_SCATTER_LAYOUT} height={360} />
                </AnalyticsCard>
              )}
            </>
          )}
        </div>
      )}
      </>
        )
      }}
    </ReportLayout>
  )
}
