'use client'

import React from 'react'
import { Shield } from 'lucide-react'
import { PopLabel } from '@/components/DataTable'
import type { ShieldingRowInput } from '@/app/network/page'
import { formatCount, CHART_COLORS, keyByOccurrence } from '@/lib/utils'

export interface ShieldingTooltipInfo {
  clientX: number
  clientY: number
  props: Record<string, unknown>
}

// ── Geometry helpers ──────────────────────────────────────────────────────────

export function greatCirclePoints(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number,
  n = 32,
): [number, number][] {
  const toRad = (d: number) => (d * Math.PI) / 180
  const toDeg = (r: number) => (r * 180) / Math.PI
  const φ1 = toRad(lat1),
    λ1 = toRad(lon1)
  const φ2 = toRad(lat2),
    λ2 = toRad(lon2)

  // Clamp dot product to [-1, 1] to prevent Math.acos from returning NaN due to floating point inaccuracy
  const dotProduct = Math.sin(φ1) * Math.sin(φ2) + Math.cos(φ1) * Math.cos(φ2) * Math.cos(λ2 - λ1)
  const clampedDot = Math.max(-1, Math.min(1, dotProduct))
  const d = Math.acos(clampedDot)

  if (isNaN(d) || d < 0.001) return [[lon1, lat1], [lon2, lat2]]

  const pts: [number, number][] = []
  let prevLon = lon1

  for (let i = 0; i <= n; i++) {
    const f = i / n
    const A = Math.sin((1 - f) * d) / Math.sin(d)
    const B = Math.sin(f * d) / Math.sin(d)
    const x = A * Math.cos(φ1) * Math.cos(λ1) + B * Math.cos(φ2) * Math.cos(λ2)
    const y = A * Math.cos(φ1) * Math.sin(λ1) + B * Math.cos(φ2) * Math.sin(λ2)
    const z = A * Math.sin(φ1) + B * Math.sin(φ2)

    const lat = toDeg(Math.atan2(z, Math.sqrt(x * x + y * y)))
    let lon = toDeg(Math.atan2(y, x))

    // Handle antimeridian crossing (prevents lines drawing across the entire map)
    if (i > 0 && Math.abs(lon - prevLon) > 180) {
      if (prevLon < 0) lon -= 360
      else lon += 360
    }

    pts.push([lon, lat])
    prevLon = lon
  }
  return pts
}

export function efficiencyColor(ratio: number | null | undefined): string {
  // A negative ratio (transit delta < 0 — possible on sparse routes or with
  // clock skew) is "no meaningful efficiency", NOT "excellent": treat it like
  // null/indigo rather than letting `ratio < 1.5` paint it green. (L5)
  if (ratio == null || ratio < 0) return '#6366f1'
  if (ratio < 1.5) return '#22c55e'
  if (ratio < 3.0) return '#eab308'
  return CHART_COLORS.danger
}

export function lineWidth(requests: number): number {
  return Math.max(1.5, Math.min(6, 1.5 + Math.log10(Math.max(1, requests)) * 1.2))
}

// ── Tooltip ───────────────────────────────────────────────────────────────────

export function ShieldingTooltip({ info }: { info: ShieldingTooltipInfo }) {
  const { props, clientX, clientY } = info
  const flipLeft = clientX > window.innerWidth * 0.7
  // MapLibre can stringify boolean feature-properties, so accept both forms.
  const lowSample = props.low_sample === true || props.low_sample === 'true'

  return (
    <div
      style={{
        position: 'fixed',
        top: clientY - 12,
        left: flipLeft ? clientX - 14 : clientX + 14,
        transform: flipLeft ? 'translate(-100%, -100%)' : 'translateY(-100%)',
        zIndex: 9999,
        pointerEvents: 'none',
      }}
      className="bg-popover text-popover-foreground border border-border rounded-lg shadow-xl px-3 py-2.5 font-sans min-w-[180px]"
    >
      <div className="font-semibold text-xs leading-tight">
        <PopLabel code={String(props.edge_pop ?? '')} /> <span className="text-muted-foreground">→</span>{' '}
        <PopLabel code={String(props.shield_pop ?? '')} className="text-purple-500" />
      </div>
      <div className="mt-2 space-y-1">
        {/* These percentiles are the edge→shield transit delta (E→S), NOT a
            TCP round-trip time — label them the same way the data table does
            so the two surfaces don't disagree. (L4) */}
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">Requests</span>
          <span className="text-[11px] font-semibold tabular-nums">{formatCount(Number(props.requests))}</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">P50 (E→S)</span>
          <span className="text-[11px] font-semibold tabular-nums">{Number(props.p50_ms).toFixed(1)}ms</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">P95 (E→S)</span>
          <span className="text-[11px] font-semibold tabular-nums">{Number(props.p95_ms).toFixed(1)}ms</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-[11px] text-muted-foreground">P99 (E→S)</span>
          <span className="text-[11px] font-semibold tabular-nums">{Number(props.p99_ms).toFixed(1)}ms</span>
        </div>
        {props.distance_km != null && (
          <div className="flex justify-between gap-4">
            <span className="text-[11px] text-muted-foreground">Distance</span>
            <span className="text-[11px] font-semibold tabular-nums">{formatCount(Number(props.distance_km))}km</span>
          </div>
        )}
        {props.light_speed_rtt_ms != null && (
          <div className="flex justify-between gap-4">
            <span className="text-[11px] text-muted-foreground">Light floor</span>
            <span className="text-[11px] font-semibold tabular-nums">
              {Number(props.light_speed_rtt_ms).toFixed(1)}ms
            </span>
          </div>
        )}
        {props.efficiency_ratio != null && (
          <div className="flex justify-between gap-4">
            <span className="text-[11px] text-muted-foreground">Efficiency</span>
            <span
              className="text-[11px] font-semibold tabular-nums"
              // Low-sample routes don't get an efficiency colour — the ratio is
              // noise below the flag floor, so render it neutral. (low-sample gating)
              style={{ color: lowSample ? CHART_COLORS.neutral : efficiencyColor(Number(props.efficiency_ratio)) }}
            >
              {Number(props.efficiency_ratio).toFixed(1)}× light speed
            </span>
          </div>
        )}
        {/* anomaly_static is computed server-side (efficiency > 3× AND ≥20ms
            absolute overhead, AND enough requests to trust the median).
            Surface it on the map too — previously it only appeared as the red
            edge-POP cell in the table. (L7) */}
        {(props.anomaly_static === true || props.anomaly_static === 'true') && (
          <div className="mt-1 flex items-center gap-1 text-[11px] font-semibold text-destructive">
            <span aria-hidden="true">⚠</span>
            <span>Suboptimal peering</span>
          </div>
        )}
        {/* Below the anomaly-flag floor: too few requests for the percentiles
            to mean anything. Say so instead of leaving a scary-looking ratio
            unqualified. (low-sample gating) */}
        {lowSample && (
          <div className="mt-1 flex items-center gap-1 text-[11px] text-muted-foreground">
            <span aria-hidden="true">ⓘ</span>
            <span>Low sample (&lt;30 reqs) — not flagged</span>
          </div>
        )}
      </div>
    </div>
  )
}

// ── GeoJSON builders ──────────────────────────────────────────────────────────

export function buildArcFeatures(rows: readonly ShieldingRowInput[]): GeoJSON.FeatureCollection {
  const features: GeoJSON.Feature[] = []
  for (const row of rows) {
    if (row.edge_lat == null || row.edge_lon == null || row.shield_lat == null || row.shield_lon == null) continue

    // Skip 0-length arcs (same POP or coordinates) to prevent MapLibre WebGL triangulation crashes
    if (Math.abs(row.edge_lat - row.shield_lat) < 0.001 && Math.abs(row.edge_lon - row.shield_lon) < 0.001) {
      continue
    }

    const coords = greatCirclePoints(row.edge_lat, row.edge_lon, row.shield_lat, row.shield_lon)
    features.push({
      type: 'Feature',
      geometry: { type: 'LineString', coordinates: coords },
      properties: {
        edge_pop: row.edge_pop,
        shield_pop: row.shield_pop,
        requests: row.requests,
        p50_ms: row.p50_ms,
        p95_ms: row.p95_ms,
        p99_ms: row.p99_ms,
        distance_km: row.distance_km,
        light_speed_rtt_ms: row.light_speed_rtt_ms,
        efficiency_ratio: row.efficiency_ratio,
        anomaly_static: row.anomaly_static,
        low_sample: row.low_sample,
        // Too few requests for the median to be trustworthy → paint a neutral
        // grey arc instead of an efficiency colour so a quiet route can't read
        // as a red "problem". The route stays on the map for visibility; it
        // just isn't colour-graded or anomaly-flagged. (low-sample gating)
        color: row.low_sample ? CHART_COLORS.neutral : efficiencyColor(row.efficiency_ratio),
        line_width: lineWidth(row.requests ?? 0),
      },
    })
  }
  return { type: 'FeatureCollection', features }
}

export function buildDotFeatures(rows: readonly ShieldingRowInput[]): GeoJSON.FeatureCollection {
  const seen = new Set<string>()
  const features: GeoJSON.Feature[] = []
  for (const row of rows) {
    if (row.edge_lat != null && row.edge_lon != null) {
      const key = `edge:${row.edge_pop}`
      if (!seen.has(key)) {
        seen.add(key)
        features.push({
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [row.edge_lon, row.edge_lat] },
          properties: { pop: row.edge_pop, role: 'edge' },
        })
      }
    }
    if (row.shield_lat != null && row.shield_lon != null && row.shield_pop !== 'Direct to Origin') {
      const key = `shield:${row.shield_pop}`
      if (!seen.has(key)) {
        seen.add(key)
        features.push({
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [row.shield_lon, row.shield_lat] },
          properties: { pop: row.shield_pop, role: 'shield' },
        })
      }
    }
  }
  return { type: 'FeatureCollection', features }
}

// ── Empty & Accessibility Subcomponents ───────────────────────────────────────

export function ShieldingEmptyState({
  errored,
  edgeOnly,
  isLoading,
  rows,
  arcCount,
}: {
  errored?: boolean
  edgeOnly?: boolean
  isLoading?: boolean
  rows: readonly ShieldingRowInput[]
  arcCount: number
}): React.ReactElement | null {
  if (errored) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center px-4 border rounded-xl border-dashed border-destructive/40 space-y-1">
        <Shield className="h-8 w-8 text-destructive mb-2 opacity-30" />
        <p className="text-sm text-destructive font-medium">Shielding analysis unavailable</p>
        <p className="text-xs text-muted-foreground max-w-sm">
          The edge-to-shield transit analysis failed to compute for this window. This is a server-side error, not an
          absence of data — try again or narrow the time range.
        </p>
      </div>
    )
  }

  if (edgeOnly) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center px-4 border rounded-xl border-dashed space-y-1">
        <Shield className="h-8 w-8 text-muted-foreground mb-2 opacity-20" />
        <p className="text-sm text-muted-foreground font-medium">Edge-only logging detected</p>
        <p className="text-xs text-muted-foreground max-w-sm">
          Edge-to-shield transit analysis requires log lines from both edge and shield POPs. Enable full logging (remove
          edge-only filtering) to see POP-to-POP transit arcs here.
        </p>
      </div>
    )
  }

  if (!isLoading && rows.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center px-4 border rounded-xl border-dashed">
        <Shield className="h-8 w-8 text-muted-foreground mb-2 opacity-20" />
        <p className="text-sm text-muted-foreground italic">No shielding path data available for this time range.</p>
      </div>
    )
  }

  if (!isLoading && rows.length > 0 && arcCount === 0) {
    const popCodes = [...new Set(rows.flatMap((r) => [r.edge_pop, r.shield_pop]).filter(Boolean))].join(', ')
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center px-4 border rounded-xl border-dashed space-y-1">
        <Shield className="h-8 w-8 text-muted-foreground mb-2 opacity-20" />
        <p className="text-sm text-muted-foreground">POP coordinates unavailable for the observed paths.</p>
        <p className="text-xs text-muted-foreground italic">Observed POP codes: {popCodes}</p>
      </div>
    )
  }

  return null
}

export function ShieldingA11yTable({
  a11yRows,
  totalSortedCount,
}: {
  a11yRows: readonly ShieldingRowInput[]
  totalSortedCount: number
}) {
  return (
    <table className="sr-only">
      <caption>
        {a11yRows.length > 0
          ? `Shielding paths — ${a11yRows.length} routes shown${totalSortedCount > a11yRows.length ? ` of ${totalSortedCount} total` : ''}.`
          : 'Shielding paths — none available.'}
      </caption>
      <thead>
        <tr>
          <th scope="col">Edge POP</th>
          <th scope="col">Shield POP</th>
          <th scope="col">Requests</th>
          <th scope="col">p50 transit (ms)</th>
          <th scope="col">p95 transit (ms)</th>
          <th scope="col">p99 transit (ms)</th>
          <th scope="col">Distance (km)</th>
          <th scope="col">Light-speed floor (ms)</th>
          <th scope="col">Efficiency ratio</th>
          <th scope="col">Anomalous</th>
        </tr>
      </thead>
      <tbody>
        {keyByOccurrence(a11yRows, (row) => `${row.edge_pop}-${row.shield_pop}`).map(({ key, item: r }) => (
          <tr key={key}>
            <td>
              <PopLabel code={r.edge_pop} />
            </td>
            <td>
              <PopLabel code={r.shield_pop} />
            </td>
            <td>
              {formatCount(r.requests ?? 0)}
              {r.low_sample ? ' (low sample)' : ''}
            </td>
            <td>{r.p50_ms != null ? r.p50_ms.toFixed(0) : 'n/a'}</td>
            <td>{r.p95_ms != null ? r.p95_ms.toFixed(0) : 'n/a'}</td>
            <td>{r.p99_ms != null ? r.p99_ms.toFixed(0) : 'n/a'}</td>
            <td>{r.distance_km != null ? formatCount(r.distance_km) : 'n/a'}</td>
            <td>{r.light_speed_rtt_ms != null ? r.light_speed_rtt_ms.toFixed(1) : 'n/a'}</td>
            <td>{r.efficiency_ratio != null ? r.efficiency_ratio.toFixed(2) : 'n/a'}</td>
            <td>{r.anomaly_static ? 'yes' : 'no'}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
