import type * as React from 'react'
import type { ReportConfiguration } from '@/hooks/useReportConfig'
import type { DashboardCard } from '@/hooks/useDashboardCards'
import type { FiltersPayload } from '@/types/filters'
import type { components } from '@/types/api.generated'
import { TIME_HOVER_LAYOUT, makeTimeXAxis } from '@/lib/chart-helpers'
import type { PlotlyLayout, PlotlyTrace } from '@/types/plotly'

type CatalogEntry = components['schemas']['LogFieldCatalogEntry']
type CatalogGroup = components['schemas']['LogFieldCatalogGroup']
type Aggregates = components['schemas']['AggregatesResponse']

/** The slice of the log-fields catalog the dashboard cards read. */
export type DashboardCatalog = {
  fields?: Pick<CatalogEntry, 'id' | 'label' | 'group' | 'unit' | 'precision'>[] | null
  groups?: Pick<CatalogGroup, 'id' | 'label'>[] | null
}

/** The slice of the aggregates payload each dashboard section reads. */
export type DashboardAggregates = Partial<Pick<Aggregates, 'data' | 'map_data' | 'time_series' | 'metric' | 'interval'>>

export type CardTint = {
  bg: string
  border: string
  dot: string
}

export type CardCategory = {
  id: string
  label: string
  cardIds: string[]
  tint: CardTint
}

export interface DashboardBodyProps {
  startTime: string | null
  endTime: string | null
  timezone: string
  activeServiceId: string | null
  filterPayload: FiltersPayload
  config: ReportConfiguration
  metric?: string
  setMetric?: (metric: string) => void
  trend: string
  setTrend: (trend: string) => void
  intervalButtons: React.ReactNode
  allCards: DashboardCard[]
  visibleCards: Set<string>
  relativeRange: string | null
  isAutoRange: boolean
  anchor: string
}

interface BuildChartLayoutParams {
  trafficData: PlotlyTrace[]
  aggregates: Pick<DashboardAggregates, 'metric'> | null | undefined
  metric: string
  startTime: string | null
  endTime: string | null
  timezone: string
  catalog: DashboardCatalog | null | undefined
}

export function buildChartLayout({
  trafficData,
  aggregates,
  metric,
  startTime,
  endTime,
  timezone,
  catalog,
}: BuildChartLayoutParams): PlotlyLayout {
  const actualMetric = aggregates?.metric || metric
  const metricField = catalog?.fields?.find((f) => f.id === actualMetric)

  return {
    ...TIME_HOVER_LAYOUT,
    barmode: trafficData.length > 1 && trafficData[0]?.type === 'bar' ? 'stack' : undefined,
    bargap: 0.1,
    bargroupgap: 0.05,
    showlegend: trafficData.some(t => t.showlegend !== false),
    yaxis: {
      title: metricField?.unit || (actualMetric === 'requests' ? 'reqs' : ''),
      ticksuffix: metricField?.unit || '',
      separatethousands: true,
      exponentformat: 'none'
    },
    xaxis: makeTimeXAxis(startTime, endTime, timezone),
  }
}

// Visible cards are rendered in this order, sectioned by category. Unknown card
// IDs (e.g. custom dashboard cards from bootstrap) fall through to "Custom" at
// the bottom. Categories with no visible cards are skipped entirely.
//
// `tint` pairs a subtle background + border + accent-dot color per section so
// each group reads as its own zone without overpowering the cards inside.
export const CARD_CATEGORIES: CardCategory[] = [
  {
    id: 'request',
    label: 'Request',
    cardIds: ['ip', 'asn', 'host', 'url', 'method', 'status', 'cache', 'proto', 'ua', 'referer', 'cookie_session', 'resp_header_content_encoding'],
    tint: { bg: 'bg-blue-50/60 dark:bg-blue-950/40', border: 'border-blue-200/70 dark:border-blue-900/60', dot: 'bg-blue-500' },
  },
  {
    id: 'cache',
    label: 'Cache',
    cardIds: ['ttl', 'age', 'hits', 'digest'],
    tint: { bg: 'bg-amber-50/60 dark:bg-amber-950/40', border: 'border-amber-200/70 dark:border-amber-900/60', dot: 'bg-amber-500' },
  },
  {
    id: 'geo',
    label: 'Geography',
    cardIds: ['city', 'region', 'country', 'metro'],
    tint: { bg: 'bg-emerald-50/60 dark:bg-emerald-950/40', border: 'border-emerald-200/70 dark:border-emerald-900/60', dot: 'bg-emerald-500' },
  },
  {
    id: 'network',
    label: 'Network & Connection',
    cardIds: [
      'tcp_rtt', 'transport', 'ploss', 'rtt_min', 'rtt_var', 'retrans',
      'c_speed', 'c_type', 'delivery_rate', 'data_segs_out',
    ],
    tint: { bg: 'bg-cyan-50/60 dark:bg-cyan-950/40', border: 'border-cyan-200/70 dark:border-cyan-900/60', dot: 'bg-cyan-500' },
  },
  {
    id: 'edge',
    label: 'Edge Infrastructure',
    cardIds: ['pop', 'backend', 'edge', 'server_region', 'tls', 'is_ipv6', 'conn_requests'],
    tint: { bg: 'bg-violet-50/60 dark:bg-violet-950/40', border: 'border-violet-200/70 dark:border-violet-900/60', dot: 'bg-violet-500' },
  },
  {
    id: 'security',
    label: 'Security',
    cardIds: [
      '_bot_name', '_ngwaf_bot_name', 'waf_sig_ind', 'edge_score_reason_ind',
      'waf', 'waf_resp', 'waf_ms',
      'p_type', 'p_desc',
      'ja3', 'ja4', 'tls_ciphers_sha',
    ],
    tint: { bg: 'bg-rose-50/60 dark:bg-rose-950/40', border: 'border-rose-200/70 dark:border-rose-900/60', dot: 'bg-rose-500' },
  },
  {
    id: 'origin',
    label: 'Origin',
    cardIds: ['ottfb', 'ottlb', 'ost', 'obytes', 'oip', 'oretries'],
    tint: { bg: 'bg-yellow-50/60 dark:bg-yellow-950/40', border: 'border-yellow-200/70 dark:border-yellow-900/60', dot: 'bg-yellow-500' },
  },
  {
    id: 'quic',
    label: 'QUIC / HTTP3',
    cardIds: ['bw', 'q_rtt', 'q_rtt_var', 'q_lost', 'q_cwnd'],
    tint: { bg: 'bg-indigo-50/60 dark:bg-indigo-950/40', border: 'border-indigo-200/70 dark:border-indigo-900/60', dot: 'bg-indigo-500' },
  },
  {
    id: 'io',
    label: 'Image Optimization',
    cardIds: ['io_input_format', 'io_output_format'],
    tint: { bg: 'bg-fuchsia-50/60 dark:bg-fuchsia-950/40', border: 'border-fuchsia-200/70 dark:border-fuchsia-900/60', dot: 'bg-fuchsia-500' },
  },
]

export const CUSTOM_TINT: CardTint = {
  bg: 'bg-slate-50/60 dark:bg-slate-900/30',
  border: 'border-slate-200/60 dark:border-slate-800/50',
  dot: 'bg-slate-400',
}

export const CATEGORIZED_CARD_IDS = new Set(CARD_CATEGORIES.flatMap(c => c.cardIds))

export const COLLAPSED_SECTIONS_KEY = 'dashboard_collapsed_sections'
