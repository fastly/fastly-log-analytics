'use client'

import React, { useEffect, useMemo, useRef, useState } from 'react'
import Link from 'next/link'
import * as maplibregl from 'maplibre-gl'
import { useTheme } from 'next-themes'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'
import {
  Activity, AlertCircle, ArrowUpRight, Bot, CheckCircle2, Clock, FileText,
  Fingerprint, Globe, HardDrive, Hash, Info, Loader2, Scale, ScanSearch,
  ShieldAlert, Users, Zap,
} from 'lucide-react'
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { TimeSeriesChart } from '@/components/AnalyticsCard'
import { DataTableReadonly } from '@/components/DataTable'
import {
  addCountryBaseLayer, updateCountryBaseLayerTheme, useMapResize,
} from '@/components/Map/ChoroplethMap'
import {
  InsightStatCard, buildInvestigateHref,
  type BotFindingModalData, type ImpossibleDistanceData,
} from '@/components/Insights/InsightCard'
import { client, extractApiError, unwrapClient } from '@/lib/api'
import { CHART_COLORS, formatBytes, formatCount, statusBadgeClass, cn } from "@/lib/utils";
import { useDateFormat } from '@/hooks/useSSE'
import { useServiceStore } from '@/stores/filterStore'
import type { components } from '@/types/api.generated'

maplibregl.setWorkerUrl('/maplibre-gl-worker.mjs')

// ---------------------------------------------------------------------------
// Shared Modal Shell & Section Primitives
// ---------------------------------------------------------------------------

interface InsightModalShellProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  maxWidthClass?: string
  scrollable?: boolean
  icon: React.ReactNode
  title: React.ReactNode
  description: React.ReactNode
  footer?: React.ReactNode
  children: React.ReactNode
}

function InsightModalShell({
  isOpen, onOpenChange, maxWidthClass = 'max-w-3xl', scrollable = true,
  icon, title, description, footer, children,
}: InsightModalShellProps) {
  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent className={cn(maxWidthClass, scrollable ? 'max-h-[90vh] flex flex-col p-6' : 'p-6')}>
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-lg font-bold">
            {icon}
            {title}
          </DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>
        {scrollable ? (
          <div className="flex-1 min-h-0 flex flex-col gap-6 py-4 overflow-y-auto">{children}</div>
        ) : (
          children
        )}
        {footer ?? <DialogFooter showCloseButton />}
      </DialogContent>
    </Dialog>
  )
}

interface FindingStatDef {
  icon: React.ReactNode
  label: string
  value: string
  valueClass?: string
  hint?: string
}

interface FindingEvidenceItem {
  lead: string
  body: React.ReactNode
}

function BotFindingSection({
  summary, stats, cols = 3, evidence,
}: {
  summary: React.ReactNode
  stats: FindingStatDef[]
  cols?: 3 | 4
  evidence: FindingEvidenceItem[]
}) {
  return (
    <>
      <div className="p-4 rounded-lg bg-muted/30 border border-border">
        <p className="text-sm leading-relaxed">{summary}</p>
      </div>
      <div className={cn('grid grid-cols-2 gap-4', cols === 3 ? 'md:grid-cols-3' : 'md:grid-cols-4')}>
        {stats.map((s) => (
          <InsightStatCard
            key={s.label}
            icon={s.icon}
            label={s.label}
            value={s.value}
            valueClass={s.valueClass}
            hint={s.hint}
          />
        ))}
      </div>
      <div className="rounded-lg bg-muted/10 border border-border p-4">
        <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
          <ScanSearch className="h-3.5 w-3.5" /> How we read the evidence
        </h4>
        <ul className="space-y-2.5 text-xs leading-relaxed">
          {evidence.map((ev) => (
            <li key={ev.lead}>
              <span className="font-semibold">{ev.lead}</span> {ev.body}
            </li>
          ))}
        </ul>
      </div>
    </>
  )
}

// ---------------------------------------------------------------------------
// 1. ImpossibleDistanceModal
// ---------------------------------------------------------------------------

function buildPhysicsFeatures(data: ImpossibleDistanceData): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: [
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [data.client_lon, data.client_lat] },
        properties: { type: 'client', title: 'Client' },
      },
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [data.pop_lon, data.pop_lat] },
        properties: { type: 'pop', title: `POP: ${data.pop}` },
      },
      {
        type: 'Feature',
        geometry: {
          type: 'LineString',
          coordinates: [[data.client_lon, data.client_lat], [data.pop_lon, data.pop_lat]],
        },
        properties: { type: 'path' },
      },
    ],
  }
}

function fitPhysicsBounds(map: maplibregl.Map, data: ImpossibleDistanceData): void {
  const bounds = new maplibregl.LngLatBounds()
  bounds.extend([data.client_lon, data.client_lat])
  bounds.extend([data.pop_lon, data.pop_lat])
  map.fitBounds(bounds, { padding: 50, maxZoom: 5, duration: 0 })
}

function PhysicsMap({ data, isDark }: { data: ImpossibleDistanceData; isDark: boolean }) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const [mapError, setMapError] = useState<string | null>(null)

  const hasCoords = Number.isFinite(data.client_lat) && Number.isFinite(data.client_lon)
    && Number.isFinite(data.pop_lat) && Number.isFinite(data.pop_lon)

  useEffect(() => {
    if (!mapContainer.current || !data || !hasCoords) return

    if (!map.current) {
      try {
        map.current = new maplibregl.Map({
          container: mapContainer.current,
          style: {
            version: 8,
            sources: {},
            layers: [{ id: 'background', type: 'background', paint: { 'background-color': isDark ? '#18181b' : '#f4f4f5' } }],
          },
          center: [(data.client_lon + data.pop_lon) / 2, (data.client_lat + data.pop_lat) / 2],
          zoom: 1,
          renderWorldCopies: false,
          preserveDrawingBuffer: true,
          interactive: false,
        } as maplibregl.MapOptions)

        map.current.on('load', () => {
          if (!map.current || !data) return
          addCountryBaseLayer(map.current, { isDark })
          map.current.addSource('points', { type: 'geojson', data: buildPhysicsFeatures(data) })
          map.current.addLayer({
            id: 'path-layer',
            type: 'line',
            source: 'points',
            filter: ['==', ['get', 'type'], 'path'],
            paint: { 'line-color': CHART_COLORS.danger, 'line-width': 2, 'line-dasharray': [2, 2] },
          })
          map.current.addLayer({
            id: 'points-layer',
            type: 'circle',
            source: 'points',
            filter: ['!=', ['get', 'type'], 'path'],
            paint: {
              'circle-radius': 6,
              'circle-color': ['match', ['get', 'type'], 'client', CHART_COLORS.info, 'pop', '#22c55e', '#000000'],
              'circle-stroke-width': 2,
              'circle-stroke-color': '#ffffff',
            },
          })
          fitPhysicsBounds(map.current, data)
        })
      } catch {
        map.current = null
        requestAnimationFrame(() => setMapError('Map failed to initialize.'))
      }
    } else if (map.current.isStyleLoaded()) {
      map.current.setPaintProperty('background', 'background-color', isDark ? '#18181b' : '#f4f4f5')
      updateCountryBaseLayerTheme(map.current, isDark)
      const source = map.current.getSource('points') as maplibregl.GeoJSONSource
      if (source) {
        source.setData(buildPhysicsFeatures(data))
        fitPhysicsBounds(map.current, data)
      }
    }
  }, [data, isDark, hasCoords])

  useMapResize(map, mapContainer)

  useEffect(() => {
    const t = setTimeout(() => {
      if (map.current) {
        map.current.resize()
        if (data && map.current.isStyleLoaded()) fitPhysicsBounds(map.current, data)
      }
    }, 150)
    return () => clearTimeout(t)
  }, [data])

  useEffect(() => () => {
    if (map.current) {
      map.current.remove()
      map.current = null
    }
  }, [])

  if (!hasCoords || mapError) {
    return (
      <div className="absolute inset-0 flex items-center justify-center text-xs text-muted-foreground">
        {mapError ?? 'Location data unavailable.'}
      </div>
    )
  }

  return <div ref={mapContainer} className="absolute inset-0 w-full h-full min-h-[300px]" />
}

export interface ImpossibleDistanceModalProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  data: ImpossibleDistanceData | null
}

export function ImpossibleDistanceModal({ isOpen, onOpenChange, data }: ImpossibleDistanceModalProps) {
  const { theme } = useTheme()
  const isDark = theme === 'dark'

  if (!data) return null

  const hasCoords = Number.isFinite(data.client_lat) && Number.isFinite(data.client_lon)
    && Number.isFinite(data.pop_lat) && Number.isFinite(data.pop_lon)

  const c = 299792.458
  const c_fibre = 200000
  const one_way_ms = data.tcp_rtt / 2 / 1000
  const required_speed = data.distance_km / (one_way_ms / 1000)
  const violation_ratio = required_speed / c_fibre
  const exceeds_vacuum = required_speed > c

  return (
    <InsightModalShell
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      maxWidthClass="max-w-3xl"
      scrollable={false}
      icon={<Zap className="h-5 w-5 text-yellow-600" />}
      title={`Physics Violation: ${data.label}`}
      description="Geographic distance between client and POP is physically impossible given the TCP RTT."
    >
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 py-4">
        <div className="md:col-span-2 relative border rounded-lg overflow-hidden bg-muted/20 h-[350px]">
          {isOpen && <PhysicsMap data={data} isDark={isDark} />}
          {hasCoords && (
            <div className="absolute top-2 left-2 flex flex-col gap-1 z-10 pointer-events-none">
              <div className="flex items-center gap-2 bg-background/90 backdrop-blur-sm px-2 py-1 rounded border text-[11px] sm:text-[10px] shadow-sm">
                <div className="h-2 w-2 rounded-full bg-[#3b82f6]" />
                <span>Client: {data.client_lat.toFixed(2)}, {data.client_lon.toFixed(2)}{data.city || data.country ? ` (${[data.city, data.country].filter(Boolean).join(', ')})` : ''}</span>
              </div>
              <div className="flex items-center gap-2 bg-background/90 backdrop-blur-sm px-2 py-1 rounded border text-[11px] sm:text-[10px] shadow-sm">
                <div className="h-2 w-2 rounded-full bg-[#22c55e]" />
                <span>POP ({data.pop}): {data.pop_lat.toFixed(2)}, {data.pop_lon.toFixed(2)}</span>
              </div>
            </div>
          )}
        </div>

        <div className="flex flex-col gap-4">
          <div className="space-y-3">
            <h4 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground flex items-center gap-1">
              <Globe className="h-3 w-3" /> Measured Stats
            </h4>
            <div className="grid grid-cols-2 gap-2">
              <div className="p-2 rounded-md bg-muted/50 border">
                <p className="text-[11px] sm:text-[10px] text-muted-foreground">TCP RTT</p>
                <p className="font-mono text-sm">{(data.tcp_rtt / 1000).toFixed(1)} ms</p>
              </div>
              <div className="p-2 rounded-md bg-muted/50 border">
                <p className="text-[11px] sm:text-[10px] text-muted-foreground">Geo Distance</p>
                <p className="font-mono text-sm">{formatCount(data.distance_km)} km</p>
              </div>
            </div>
          </div>

          <div className={cn(
            'p-3 rounded-lg border space-y-2',
            exceeds_vacuum ? 'bg-red-500/10 border-red-500/50' : 'bg-yellow-500/10 border-yellow-500/50',
          )}>
            <h4 className="text-xs font-bold uppercase flex items-center gap-1">
              <Zap className="h-3 w-3" /> Violation Report
            </h4>
            <div className="space-y-1">
              <p className="text-xs leading-relaxed">
                To cover {formatCount(data.distance_km)} km in {one_way_ms.toFixed(2)} ms (one-way),
                the signal would need to travel at:
              </p>
              <p className="font-mono text-lg font-bold text-center py-1">
                {formatCount(required_speed, { maximumFractionDigits: 0 })} km/s
              </p>
              <div className="space-y-1 text-xs sm:text-[11px]">
                <div className="flex justify-between items-center">
                  <span>vs. Fibre Speed (~200k km/s)</span>
                  <span className="font-bold text-red-500">{(violation_ratio * 100).toFixed(0)}% of limit</span>
                </div>
                {exceeds_vacuum && (
                  <div className="flex justify-between items-center font-bold text-red-600 animate-pulse">
                    <span>FASTER THAN LIGHT (VACUUM)</span>
                    <span>YES</span>
                  </div>
                )}
              </div>
            </div>
          </div>

          <div className="p-2 rounded bg-blue-500/5 border border-blue-500/20 text-[11px] sm:text-[10px] leading-relaxed">
            <Info className="h-3 w-3 inline mr-1 mb-0.5 text-blue-600" />
            <strong>Conclusion:</strong> The user is likely using a VPN, proxy, or GPS spoofing to appear in a location physically distant from the Fastly POP they are actually hitting.
          </div>
        </div>
      </div>
    </InsightModalShell>
  )
}

// ---------------------------------------------------------------------------
// 2. CacheCollapseModal
// ---------------------------------------------------------------------------

type CacheMiss = components['schemas']['CacheMissPoint']

export interface CacheCollapseModalProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  url: string | null
  windowHours: string
  baselineHours: string
}

export function CacheCollapseModal({
  isOpen, onOpenChange, url, windowHours, baselineHours,
}: CacheCollapseModalProps) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const { full, abbr } = useDateFormat()

  const { data, isLoading, error } = useQuery({
    queryKey: ['insights', 'cache-collapse-detail', activeServiceId, url, windowHours, baselineHours],
    queryFn: async ({ signal }) => unwrapClient(client.POST('/api/insights/cache-collapse-detail', {
      signal,
      body: { url: url!, window_size_hrs: parseFloat(windowHours), baseline_hours: parseFloat(baselineHours) },
    })),
    enabled: isOpen && !!url && !!activeServiceId,
    staleTime: 30000,
  })

  const errorMsg = error ? extractApiError(error) : null

  const stats = useMemo(() => {
    if (!data?.timeline) return null
    const totalRequests = data.timeline.reduce((acc, curr) => acc + (curr.total_requests || 0), 0)
    const realHits = data.timeline.reduce((acc, curr) => acc + (curr.real_hits || 0), 0)
    return { totalRequests, realHits, baselineHitRate: data.baseline_hit_rate, windowHitRate: data.window_hit_rate }
  }, [data])

  const chartData = useMemo(() => {
    if (!data?.timeline) return []
    return [
      {
        x: data.timeline.map((p) => p.bucket),
        y: data.timeline.map((p) => p.expected_hits),
        type: 'scatter',
        mode: 'lines',
        name: 'Expected HITs',
        line: { color: CHART_COLORS.info, width: 2, dash: 'dash' },
      },
      {
        x: data.timeline.map((p) => p.bucket),
        y: data.timeline.map((p) => p.real_hits),
        type: 'scatter',
        mode: 'lines',
        name: 'Real HITs',
        line: { color: CHART_COLORS.danger, width: 2 },
      },
    ]
  }, [data])

  const missColumns = useMemo<ColumnDef<CacheMiss>[]>(
    () => [
      {
        id: 'timestamp',
        header: `Timestamp (${abbr()})`,
        accessorFn: (e) => e.timestamp,
        size: 200,
        enableSorting: false,
        cell: ({ row }) => <span className="font-mono text-xs font-medium whitespace-nowrap">{full(row.original.timestamp)}</span>,
      },
      {
        id: 'cache',
        header: 'Cache Status',
        accessorFn: (e) => e.cache,
        size: 130,
        enableSorting: false,
        cell: ({ row }) => {
          const cache = row.original.cache
          return (
            <span className={cn(
              'px-1.5 py-0.5 rounded text-[10px] font-bold border',
              cache.includes('PASS') ? 'bg-amber-500/10 text-amber-500 border-amber-500/20' :
              cache.includes('MISS') ? 'bg-red-500/10 text-red-500 border-red-500/20' :
              'bg-muted text-muted-foreground border-border',
            )}>
              {cache}
            </span>
          )
        },
      },
      {
        id: 'pop',
        header: 'POP',
        accessorFn: (e) => e.pop ?? '',
        size: 90,
        enableSorting: false,
        cell: ({ row }) => <span className="font-mono text-xs">{row.original.pop || '—'}</span>,
      },
      {
        id: 'ip',
        header: 'Client IP',
        accessorFn: (e) => e.ip ?? '',
        size: 150,
        enableSorting: false,
        cell: ({ row }) => <span className="font-mono text-xs text-muted-foreground">{row.original.ip || '—'}</span>,
      },
      {
        id: 'status',
        header: 'HTTP Status',
        accessorFn: (e) => e.status ?? 0,
        size: 110,
        enableSorting: false,
        cell: ({ row }) => (
          <span className="block text-right">
            <span className={cn('px-1.5 py-0.5 rounded font-mono font-semibold border text-[10px]', statusBadgeClass(row.original.status, 'traffic-light'))}>
              {row.original.status || '—'}
            </span>
          </span>
        ),
      },
    ],
    [abbr, full],
  )

  const chartLayout = useMemo(() => ({ yaxis: { title: { text: 'Hits' } } }), [])

  return (
    <InsightModalShell
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      maxWidthClass="max-w-4xl"
      icon={<ShieldAlert className="h-5 w-5 text-red-500 shrink-0" />}
      title={<span className="truncate max-w-[90%]">Cache Analysis: {url}</span>}
      description="Cacheable hit ratio (HIT/(HIT+MISS), PASS excluded) and cacheability (PASS share) vs. baseline."
    >
      {isLoading && (
        <div className="flex-1 flex flex-col items-center justify-center py-20 text-muted-foreground text-sm gap-2">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
          <span>Analyzing historical logs and calculating cache timelines…</span>
        </div>
      )}

      {errorMsg && (
        <div className="flex-1 flex flex-col items-center justify-center py-20 text-center px-4">
          <AlertCircle className="h-10 w-10 text-destructive mb-3" />
          <h3 className="text-sm font-medium text-destructive">Failed to load collapse details</h3>
          <p className="text-xs text-muted-foreground mt-1 max-w-md">{errorMsg}</p>
        </div>
      )}

      {!isLoading && !errorMsg && data && stats && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <InsightStatCard icon={<Clock className="h-3 w-3" />} label="Baseline Hit Ratio" value={`${stats.baselineHitRate.toFixed(2)}%`} valueClass="text-blue-500" hint="cacheable (HIT/(HIT+MISS))" />
            <InsightStatCard icon={<ShieldAlert className="h-3 w-3 text-red-400" />} label="Recent Hit Ratio" value={`${stats.windowHitRate.toFixed(2)}%`} valueClass="text-red-500" hint="cacheable (HIT/(HIT+MISS))" />
            <InsightStatCard icon={<Activity className="h-3 w-3" />} label="Total Requests" value={formatCount(stats.totalRequests)} />
            <InsightStatCard icon={<CheckCircle2 className="h-3 w-3 text-emerald-500" />} label="Total Cache Hits" value={formatCount(stats.realHits)} />
          </div>

          <div className="rounded-lg bg-muted/10 border border-border p-4">
            <div className="flex items-center justify-between mb-3">
              <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground flex items-center gap-1.5">
                <Activity className="h-3.5 w-3.5" /> Recent Window Breakdown
              </h4>
              <span className="text-[11px] text-muted-foreground">
                PASS (uncacheable): {data.baseline_pass_rate.toFixed(1)}% → <span className="font-semibold text-amber-500">{data.window_pass_rate.toFixed(1)}%</span>
              </span>
            </div>
            <div className="grid grid-cols-3 gap-3 text-center">
              <div className="p-2 rounded-md bg-emerald-500/10 border border-emerald-500/20">
                <div className="font-mono text-lg font-bold text-emerald-500">{formatCount(data.breakdown?.hits ?? 0)}</div>
                <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Cacheable Hits</div>
              </div>
              <div className="p-2 rounded-md bg-red-500/10 border border-red-500/20">
                <div className="font-mono text-lg font-bold text-red-500">{formatCount(data.breakdown?.misses ?? 0)}</div>
                <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Misses</div>
              </div>
              <div className="p-2 rounded-md bg-amber-500/10 border border-amber-500/20">
                <div className="font-mono text-lg font-bold text-amber-500">{formatCount(data.breakdown?.passes ?? 0)}</div>
                <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Uncacheable (PASS)</div>
              </div>
            </div>
          </div>

          <div className="border border-border rounded-lg bg-muted/10 p-4 flex flex-col min-h-[300px]">
            <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
              <Activity className="h-3.5 w-3.5" /> Hits Timeline (Expected vs. Real Hits)
            </h4>
            <div className="flex-1 w-full min-h-[220px]">
              <TimeSeriesChart data={chartData} layout={chartLayout} timezone="UTC" height={220} />
            </div>
          </div>

          <div className="flex flex-col min-h-[250px] border border-border rounded-lg bg-muted/10 p-4">
            <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
              <FileText className="h-3.5 w-3.5" /> Recent Cache Misses (Max 50)
            </h4>
            {(!data.recent_misses || data.recent_misses.length === 0) ? (
              <div className="flex-1 flex items-center justify-center text-xs text-muted-foreground italic">
                No cache misses captured for this lookback.
              </div>
            ) : (
              <div className="flex-1">
                <DataTableReadonly
                  columns={missColumns}
                  data={data.recent_misses}
                  hideToolbar
                  showPagination={false}
                  tableCaption="Recent cache misses"
                />
              </div>
            )}
          </div>
        </>
      )}
    </InsightModalShell>
  )
}

// ---------------------------------------------------------------------------
// 3. BotFindingModal
// ---------------------------------------------------------------------------

export interface BotFindingModalProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  data: BotFindingModalData | null
}

export function BotFindingModal({ isOpen, onOpenChange, data }: BotFindingModalProps) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const investigateUrl = data?.item.investigate_url

  const investigateHref = useMemo(
    () => buildInvestigateHref(investigateUrl, activeServiceId),
    [investigateUrl, activeServiceId],
  )

  if (!data) return null

  const { insightId, item } = data

  const renderContent = () => {
    switch (insightId) {
      case 'crawler_disparity': {
        const m = item.meta
        const wBytes = m.w_bytes ?? 0
        const bytesPerRef = m.bytes_per_referral ?? 0
        const baseBytesPerRef = m.baseline_bytes_per_referral ?? 0
        const bwSharePct = m.bandwidth_share_pct ?? 0
        const isZeroReferral = m.referrals === 0
        return (
          <BotFindingSection
            cols={3}
            summary={
              <>
                <span className="font-semibold">{item.label}</span> executed{' '}
                <span className="font-semibold">{formatCount(m.crawls)}</span> automated requests
                {wBytes > 0 && (
                  <>
                    {' '}consuming <span className="font-semibold">{formatBytes(wBytes)}</span> of egress bandwidth
                    {bwSharePct > 0 && <> (<span className="font-semibold">{bwSharePct.toFixed(1)}%</span> of site bandwidth)</>}
                  </>
                )}{' '}
                while driving only{' '}
                <span className={cn('font-semibold', isZeroReferral ? 'text-red-500' : 'text-amber-500')}>
                  {formatCount(m.referrals)}
                </span>{' '}
                unique human referral {m.referrals === 1 ? 'IP' : 'IPs'} back to your service. That equates to{' '}
                <span className="font-semibold text-red-500">
                  {formatCount(m.crawls_per_referral, { maximumFractionDigits: 1 })} crawls/referral
                </span>
                {bytesPerRef > 0 && <> and <span className="font-semibold">{formatBytes(bytesPerRef)}/referral</span></>}.
              </>
            }
            stats={[
              { icon: <Hash className="h-3 w-3" />, label: 'Bot / Crawl Requests', value: formatCount(m.crawls), hint: 'current window' },
              { icon: <HardDrive className="h-3 w-3" />, label: 'Bandwidth Used', value: formatBytes(wBytes), hint: bwSharePct > 0 ? `${bwSharePct.toFixed(1)}% of site bandwidth` : 'total resp_bytes' },
              { icon: <Users className="h-3 w-3" />, label: 'Human Referrals', value: formatCount(m.referrals), valueClass: isZeroReferral ? 'text-red-500' : 'text-amber-500', hint: 'distinct referred IPs' },
              { icon: <Scale className="h-3 w-3" />, label: 'Crawls / Referral', value: formatCount(m.crawls_per_referral, { maximumFractionDigits: 1 }), valueClass: 'text-red-500', hint: `baseline: ${formatCount(m.baseline_crawls_per_referral, { maximumFractionDigits: 1 })}` },
              { icon: <HardDrive className="h-3 w-3" />, label: 'Bandwidth / Referral', value: formatBytes(bytesPerRef), hint: baseBytesPerRef > 0 ? `baseline: ${formatBytes(baseBytesPerRef)}` : 'cost per referred visitor' },
              { icon: <Bot className="h-3 w-3" />, label: 'Site Egress Share', value: `${bwSharePct.toFixed(1)}%`, hint: 'of total resp_bytes' },
            ]}
            evidence={[
              { lead: 'Unified bot & crawler attribution.', body: 'Requests are grouped dynamically from automated User-Agent tokens — covering search indexers, AI answer crawlers, training scrapers, headless browsers, and programmatic HTTP clients.' },
              { lead: 'Human referral correlation.', body: <>We count distinct client IPs arriving with a non-bot User-Agent and an HTTP <code>Referer</code> matching the platform&apos;s domain.</> },
              { lead: 'Value vs. cost disparity.', body: 'High crawl volume and egress bandwidth combined with near-zero referrals means this bot or crawler is harvesting content without returning organic search or discovery traffic.' },
            ]}
          />
        )
      }

      case 'stale_browser_version': {
        const m = item.meta
        return (
          <BotFindingSection
            cols={3}
            summary={
              <>
                Clients claiming <span className="font-semibold">{item.label}</span> generated{' '}
                <span className="font-semibold">{formatCount(m.requests)}</span> requests across{' '}
                <span className="font-semibold">{formatCount(m.ips)}</span> distinct IPs — surging{' '}
                <span className="font-semibold text-amber-500">{m.surge_ratio.toFixed(1)}×</span> over baseline.
              </>
            }
            stats={[
              { icon: <Hash className="h-3 w-3" />, label: 'Requests', value: formatCount(m.requests), hint: 'current window' },
              { icon: <Users className="h-3 w-3" />, label: 'Distinct IPs', value: formatCount(m.ips), hint: 'unique client IPs' },
              { icon: <Scale className="h-3 w-3" />, label: 'Surge Ratio', value: `${m.surge_ratio.toFixed(1)}×`, valueClass: 'text-amber-500', hint: 'vs. baseline rate' },
            ]}
            evidence={[
              { lead: 'Evergreen browser drift.', body: 'Real desktop and mobile Chrome installations auto-update silently. Pinned, outdated major versions almost always come from containerized headless browsers (Puppeteer, Playwright, Selenium) or hardcoded User-Agent strings.' },
              { lead: 'Multi-IP coordination.', body: `${formatCount(m.ips)} source IPs presenting the same outdated browser version indicates a distributed scraping pool sharing a single container image.` },
            ]}
          />
        )
      }

      case 'orphaned_deep_crawl': {
        const m = item.meta
        return (
          <BotFindingSection
            cols={4}
            summary={
              <>
                Client IP <span className="font-semibold">{item.label}</span> fetched{' '}
                <span className="font-semibold text-red-500">{formatCount(m.requests)}</span> deep resource requests without an HTTP{' '}
                <code>Referer</code> header across{' '}
                <span className="font-semibold">{formatCount(m.distinct_urls)}</span> distinct URLs (
                <span className="font-semibold">{m.requests_per_url.toFixed(1)}</span> requests/URL,{' '}
                <span className="font-semibold text-amber-500">{m.surge_ratio.toFixed(1)}×</span> baseline).
              </>
            }
            stats={[
              { icon: <ShieldAlert className="h-3 w-3" />, label: 'Orphan Requests', value: formatCount(m.requests), valueClass: 'text-red-500', hint: 'missing HTTP Referer' },
              { icon: <Globe className="h-3 w-3" />, label: 'Distinct URLs', value: formatCount(m.distinct_urls), hint: 'unique deep paths' },
              { icon: <Hash className="h-3 w-3" />, label: 'Requests / URL', value: m.requests_per_url.toFixed(1), hint: 'sweep density' },
              { icon: <Scale className="h-3 w-3" />, label: 'Surge Ratio', value: `${m.surge_ratio.toFixed(1)}×`, valueClass: 'text-amber-500', hint: 'vs. baseline rate' },
            ]}
            evidence={[
              { lead: 'Broken navigation graph.', body: <>Genuine users reach deep catalog or detail pages by clicking links from landing or search pages, which populates the <code>Referer</code> header. Hitting hundreds of deep paths directly with an empty referer is a hallmark of queue-driven scrapers.</> },
              { lead: `URL breadth (${formatCount(m.distinct_urls)} paths).`, body: 'High path diversity rules out a single bookmarked page or asset refresh and confirms systematic enumeration.' },
            ]}
          />
        )
      }

      case 'residential_fingerprint_dispersion': {
        const m = item.meta
        return (
          <BotFindingSection
            cols={4}
            summary={
              <>
                TLS fingerprint <span className="font-mono font-semibold">{m.ja4 || item.label}</span> appeared across{' '}
                <span className="font-semibold text-red-500">{formatCount(m.asns)}</span> distinct ASNs and{' '}
                <span className="font-semibold">{formatCount(m.ips)}</span> unique IPs over{' '}
                <span className="font-semibold">{formatCount(m.requests)}</span> requests (
                <span className="font-semibold text-amber-500">{m.asn_expansion_ratio.toFixed(1)}×</span> ASN expansion).
              </>
            }
            stats={[
              { icon: <Globe className="h-3 w-3" />, label: 'Distinct ASNs', value: formatCount(m.asns), valueClass: 'text-red-500', hint: 'autonomous systems' },
              { icon: <Users className="h-3 w-3" />, label: 'Distinct IPs', value: formatCount(m.ips), hint: 'rotated source IPs' },
              { icon: <Hash className="h-3 w-3" />, label: 'Total Requests', value: formatCount(m.requests), hint: 'current window' },
              { icon: <Fingerprint className="h-3 w-3" />, label: 'ASN Expansion', value: `${m.asn_expansion_ratio.toFixed(1)}×`, valueClass: 'text-amber-500', hint: `${formatCount(m.ips / Math.max(1, m.asns), { maximumFractionDigits: 1 })} IPs/ASN` },
            ]}
            evidence={[
              { lead: 'Residential proxy rotation.', body: 'Attackers route requests through residential proxy networks so each request arrives from a different home ISP IP and ASN, evading per-IP rate limits.' },
              { lead: 'Invariant TLS stack.', body: 'Because the underlying scraping binary or headless engine is identical across all proxy exits, the TLS ClientHello fingerprint (JA4/JA3) remains constant across dozens of autonomous systems.' },
            ]}
          />
        )
      }
    }
  }

  return (
    <InsightModalShell
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      maxWidthClass="max-w-3xl"
      icon={<ScanSearch className="h-5 w-5 text-primary shrink-0" />}
      title={<span className="truncate max-w-[90%]">Finding Details: {item.label}</span>}
      description="Detailed telemetry and behavioral evidence behind this bot detection finding."
      footer={
        <DialogFooter className="flex items-center justify-between sm:justify-between gap-2">
          {investigateHref ? (
            <Link href={investigateHref} target="_blank" rel="noopener noreferrer">
              <Button variant="outline" size="sm">
                Investigate in Logs <ArrowUpRight className="ml-1.5 h-3.5 w-3.5" />
              </Button>
            </Link>
          ) : (
            <div />
          )}
          <Button variant="secondary" size="sm" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        </DialogFooter>
      }
    >
      {renderContent()}
    </InsightModalShell>
  )
}
