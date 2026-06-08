'use client'

import React from 'react'
import dynamic from 'next/dynamic'
import { useCardVisibility } from '@/hooks/useCardVisibility'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { client } from '@/lib/api'
import { STALE_VIEW_RETRY_OPTIONS, throwIfStaleAggregates } from '@/lib/staleViewRetry'
import { useFilterStore } from '@/stores/filterStore'
import { useServiceStore } from '@/stores/serviceStore'
import { useIsDataReady } from '@/hooks/useIsDataReady'
import { useFieldLabel } from '@/hooks/useFieldLabel'
import { TimeSeriesChart } from '@/components/charts/TimeSeriesChart'
import { FilterPopover } from '@/components/FilterPopover'
import { LazyMount } from '@/components/LazyMount'

// ChoroplethMap pulls in d3-geo and the world-110m topojson. Static-import
// blocked the dashboard's initial JS parse/eval; dynamic-import slices it
// off the critical path so the rest of the page paints immediately.
// ssr:false because d3-geo uses canvas/SVG measurement APIs that don't
// work in the server-render pass.
const ChoroplethMap = dynamic(
  () => import('@/components/Map/ChoroplethMap').then((m) => ({ default: m.ChoroplethMap })),
  {
    ssr: false,
    loading: () => (
      <div
        className="flex-1 min-h-[300px] flex items-center justify-center bg-muted/20 rounded"
        aria-busy="true"
      >
        <span className="text-muted-foreground text-xs animate-pulse">Loading map…</span>
      </div>
    ),
  },
)
import { TopTenTable } from '@/components/Dashboard/TopTenTable'
import { DashboardHeader } from '@/components/Dashboard/DashboardHeader'
import { DataTable } from '@/components/DataTable'
import { ColumnVisibilityDropdown } from '@/components/DataTable'
import { ColumnDef, SortingState } from '@tanstack/react-table'
import { Button, buttonVariants } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import { badgeVariants } from '@/components/ui/badge'
import { useDateFormat } from '@/hooks/useDateFormat'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { formatDate, parseFromInput } from '@/lib/date'
import { LayoutDashboard, ChevronDown, ChevronRight, Download, Bot } from 'lucide-react'
import { cn, downloadBlob } from '@/lib/utils'
import { ReportLayout } from '@/components/ReportLayout'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { useShallow } from 'zustand/react/shallow'
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'
import { useDashboardCards } from '@/hooks/useDashboardCards'
import { FlagSessionPopover, type LabelValue } from '@/components/SessionScoring/FlagSessionPopover'

// ── Constants ──────────────────────────────────────────────────────────────────

import {
  INTERVAL_SECONDS,
  TRENDS,
} from '@/lib/constants'
import { makeTimeXAxis, TIME_HOVER_LAYOUT } from '@/lib/chart-helpers'

// ── Card categories ────────────────────────────────────────────────────────────
// Visible cards are rendered in this order, sectioned by category. Unknown card
// IDs (e.g. custom dashboard cards from bootstrap) fall through to "Custom" at
// the bottom. Categories with no visible cards are skipped entirely.
//
// `tint` pairs a subtle background + border + accent-dot color per section so
// each group reads as its own zone without overpowering the cards inside.
type CardCategory = {
  id: string
  label: string
  cardIds: string[]
  tint: { bg: string; border: string; dot: string }
}

const CARD_CATEGORIES: CardCategory[] = [
  {
    id: 'request',
    label: 'Request',
    cardIds: ['ip', 'asn', 'host', 'url', 'method', 'status', 'cache', 'proto', 'ua', 'referer'],
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
      '_bot_name', '_ngwaf_bot_name', 'waf_sig_ind',
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
]

const CUSTOM_TINT = {
  bg: 'bg-slate-50/60 dark:bg-slate-900/30',
  border: 'border-slate-200/60 dark:border-slate-800/50',
  dot: 'bg-slate-400',
}

const CATEGORIZED_CARD_IDS = new Set(CARD_CATEGORIES.flatMap(c => c.cardIds))

const COLLAPSED_SECTIONS_KEY = 'dashboard_collapsed_sections'

// Raw-logs panel: which columns to fetch. Previously the panel pulled SELECT *
// (~75 cols) on every dashboard load, which dominated /api/dashboard/raw time
// because wide text fields (ua, referer, url, ja3, etc.) bloat the parquet
// read. Default set covers the columns most users actually look at; everything
// else can be opted in via the column dropdown (which triggers a refetch).
// `timestamp` is always included so the default sort doesn't break.
const RAW_COLUMNS_STORAGE_KEY = 'dashboard_raw_columns'
const DEFAULT_RAW_COLUMNS = [
  'timestamp', 'ip', 'country', 'host', 'url', 'method',
  'status', 'cache', 'elapsed', 'resp_bytes', 'ttfb', 'ua', 'edge_sid',
]
// Catalog ids that aren't real parquet columns and can't be returned per-row
// (they're aggregate-only views like the exploded waf_sig signal breakdown).
const RAW_DROPDOWN_EXCLUDE = new Set(['waf_sig_ind', 'edge_score_reason_ind', '_source_file'])

// ── Page ───────────────────────────────────────────────────────────────────────

export default function DashboardPage() {
  const allCards = useDashboardCards()
  const { data: catalog } = useLogFieldsCatalog()
  
  const {
    addFilter,
    setRange,
    compareMode,
    compareStartTime,
    compareEndTime,
  } = useFilterStore(useShallow(state => ({
    addFilter: state.addFilter,
    setRange: state.setRange,
    compareMode: state.compareMode,
    compareStartTime: state.compareStartTime,
    compareEndTime: state.compareEndTime,
  })))
  
  const { visibleCards, toggleCard, showAll, reset: resetCards } = useCardVisibility(
    'dashboard_cards',
    allCards.map((c: any) => c.id),
    allCards.filter((c: any) => c.inActiveFormat).map((c: any) => c.id),
  )

  const [metric, setMetric] = React.useState("requests")
  const getFieldLabel = useFieldLabel()
  const { full, abbr } = useDateFormat()

  const [hiddenCategories, setHiddenCategories] = React.useState<Set<string>>(new Set())

  const toggleCategory = React.useCallback((cat: string) => {
    setHiddenCategories(prev => {
      const next = new Set(prev)
      if (next.has(cat)) next.delete(cat)
      else next.add(cat)
      return next
    })
  }, [])

  // Collapsed-section state, persisted to localStorage so user's choices stick
  // across reloads. Lazy initializer reads localStorage once on mount.
  const [collapsedSections, setCollapsedSections] = React.useState<Set<string>>(() => {
    if (typeof window === 'undefined') return new Set()
    try {
      const raw = localStorage.getItem(COLLAPSED_SECTIONS_KEY)
      return raw ? new Set<string>(JSON.parse(raw)) : new Set()
    } catch {
      return new Set()
    }
  })

  const toggleSectionCollapsed = React.useCallback((id: string) => {
    setCollapsedSections(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      try {
        localStorage.setItem(COLLAPSED_SECTIONS_KEY, JSON.stringify([...next]))
      } catch { /* ignore quota / private-mode errors */ }
      return next
    })
  }, [])

  return (
    <ReportLayout
      title="Dashboard"
      description="Drill down into traffic details and analyze request trends."
      icon={LayoutDashboard}
      defaultInterval="1 minute"
      headerActions={
        <DashboardHeader
          visibleCardsCount={visibleCards.size}
          allCards={allCards}
          visibleCards={visibleCards}
          onToggleCard={toggleCard}
          onShowAll={showAll}
          onResetCards={resetCards}
        />
      }
    >
      {({
        startTime,
        endTime,
        timezone,
        activeServiceId,
        filterPayload,
        config,
        setChartInterval,
        trend,
        setTrend,
        intervalButtons,
      }) => {
        // Clear hidden categories when metric changes to avoid confusing states
        React.useEffect(() => {
          setHiddenCategories(new Set())
        }, [metric])

        const isReady = useIsDataReady()

        const { data: aggregates, isLoading: isLoadingAggs, isFetching: isFetchingAggs } = useServiceQuery(
          ['dashboard', 'aggregates', activeServiceId, startTime, endTime, filterPayload, metric, config.effectiveInterval],
          async ({ signal }) => {
            const { data } = await client.POST("/api/dashboard/aggregates", { signal,
              body: {
                start_time: startTime!,
                end_time: endTime!,
                filters: filterPayload,
                chart_metric: metric as any,
                chart_interval: config.effectiveInterval
              }
            })
            return throwIfStaleAggregates(data)
          },
          STALE_VIEW_RETRY_OPTIONS,
        )

        const { data: compareAggregates } = useQuery({
          queryKey: ['dashboard', 'aggregates', 'compare', activeServiceId, compareStartTime, compareEndTime, filterPayload, metric, config.effectiveInterval],
          queryFn: async ({ signal }) => {
            const { data } = await client.POST("/api/dashboard/aggregates", { signal,
              body: {
                start_time: compareStartTime!,
                end_time: compareEndTime!,
                filters: filterPayload,
                chart_metric: metric as any,
                chart_interval: config.effectiveInterval
              }
            })
            return throwIfStaleAggregates(data)
          },
          enabled: isReady && compareMode && !!compareStartTime && !!compareEndTime,
          ...STALE_VIEW_RETRY_OPTIONS,
        })

        const [sorting, setSorting] = React.useState<SortingState>([{ id: 'timestamp', desc: true }])

        // User-selected raw-log columns. `timestamp` is forced into the list
        // because the default sort references it; without it the API picks an
        // arbitrary sort col and the table feels broken.
        const [selectedRawColumns, setSelectedRawColumns] = React.useState<string[]>(() => {
          if (typeof window === 'undefined') return DEFAULT_RAW_COLUMNS
          try {
            const raw = localStorage.getItem(RAW_COLUMNS_STORAGE_KEY)
            const parsed = raw ? JSON.parse(raw) : null
            if (Array.isArray(parsed) && parsed.length > 0) {
              return parsed.includes('timestamp') ? parsed : ['timestamp', ...parsed]
            }
          } catch { /* fall through to default */ }
          return DEFAULT_RAW_COLUMNS
        })

        const toggleRawColumn = React.useCallback((id: string, visible: boolean) => {
          setSelectedRawColumns(prev => {
            const set = new Set(prev)
            if (visible) set.add(id)
            else if (id !== 'timestamp') set.delete(id)
            const next = Array.from(set)
            try {
              localStorage.setItem(RAW_COLUMNS_STORAGE_KEY, JSON.stringify(next))
            } catch { /* ignore quota / private-mode errors */ }
            return next
          })
        }, [])

        const { data: rawLogs, isLoading: isLoadingRaw, isFetching: isFetchingRaw } = useServiceQuery(
          ['dashboard', 'raw', activeServiceId, startTime, endTime, filterPayload, sorting, selectedRawColumns],
          async ({ signal }) => {
            const sort = sorting[0]
            const { data } = await client.POST("/api/dashboard/raw", { signal, 
              body: {
                start_time: startTime!,
                end_time: endTime!,
                filters: filterPayload,
                limit: 500,
                page: 1,
                sort_col: sort?.id,
                sort_dir: sort?.desc ? 'desc' : 'asc',
                columns: selectedRawColumns
              }
            })
            return data
          }
        )

        const { data: topBotsData } = useQuery({
          queryKey: ['dashboard', 'top-bots', activeServiceId, startTime, endTime, filterPayload],
          queryFn: async ({ signal }) => {
            const { data } = await client.POST("/api/security/top-bots", { signal, 
              body: {
                start_time: startTime!,
                end_time: endTime!,
                filters: filterPayload,
              }
            })
            return data
          },
          enabled: isReady,
          placeholderData: keepPreviousData,
        })

        // ── Chart data ────────────────────────────────────────────────────────────

        const trafficData = React.useMemo(() => {
          const time_series = aggregates?.time_series
          if (!time_series?.length) return []

          const actualMetric = aggregates?.metric || metric
          const isBar = actualMetric === 'requests' || actualMetric === '5xx' || actualMetric === '4xx'
          
          // Find metric metadata from catalog
          const metricField = catalog?.fields?.find(f => f.id === actualMetric)
          const unit = metricField?.unit || ''
          const precision = metricField?.precision ?? (actualMetric === 'requests' ? 0 : 1)
          
          const getHoverTemplate = (m: string, label?: string) => {
            const pre = label ? `${label}: ` : ''
            const format = precision > 0 ? `.${precision}f` : ','
            return `${pre}%{y:${format}}${unit}<extra></extra>`
          }

          // If we have categories (e.g. 5xx/4xx breakdown), group by category.
          // Pydantic serializes optional fields as null, so null and undefined both mean "no category".
          const hasCategories = time_series.some(d => d.category != null)

          let traces: any[] = []

          if (hasCategories) {
            const catMap: Record<string, { x: string[], y: number[] }> = {}
            time_series.forEach(d => {
              const cat = d.category || 'Other'
              if (!catMap[cat]) catMap[cat] = { x: [], y: [] }
              // Use a standard format that Plotly recognizes as a date but is in the target timezone
              catMap[cat].x.push(formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss"))
              catMap[cat].y.push(d.value)
            })
            
            // Standardize colors for common error statuses to keep them consistent
            const colorMap: Record<string, string> = {
              '400': '#fbbf24', '401': '#f59e0b', '403': '#d97706', '404': '#b45309',
              '500': '#ef4444', '502': '#dc2626', '503': '#b91c1c', '504': '#991b1b'
            }

            traces = Object.entries(catMap).map(([cat, data], i) => ({
              x: data.x,
              y: data.y,
              type: 'bar',
              name: cat,
              showlegend: false, // Custom legend will handle these
              visible: hiddenCategories.has(cat) ? 'legendonly' : true,
              hovertemplate: `Status ${cat}: %{y:,}<extra></extra>`,
              marker: { color: colorMap[cat] || `hsl(${(i * 50) % 360}, 70%, 50%)` }
            }))
          } else {
            const xValues = time_series.map(d => formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss"))
            const yValues = time_series.map(d => d.value)
        
            traces = [{
              x: xValues,
              y: yValues,
              type: isBar ? 'bar' : 'scatter',
              mode: isBar ? undefined : 'lines+markers',
              name: compareMode ? 'Primary Range' : (metricField?.label || actualMetric),
              showlegend: compareMode,
              hovertemplate: getHoverTemplate(actualMetric, compareMode ? 'Primary' : undefined),
              marker: { color: '#3b82f6' }
            }]
          }

          if (compareMode && compareAggregates?.time_series?.length && !hasCategories && startTime && compareStartTime) {
            const currentStart = new Date(startTime).getTime()
            const compareStart = new Date(compareStartTime).getTime()
            const shift = currentStart - compareStart

            const compX = compareAggregates.time_series.map(d => {
              const t = new Date(d.time).getTime() + shift
              return formatDate(new Date(t).toISOString(), timezone, "yyyy-MM-dd HH:mm:ss")
            })
            const compY = compareAggregates.time_series.map(d => d.value)

            traces.push({
              x: compX,
              y: compY,
              type: 'scatter',
              mode: 'lines',
              name: 'Comparison Range',
              line: { color: '#f97316', dash: 'dash', width: 2 },
              hovertemplate: getHoverTemplate(actualMetric, 'Comparison')
            })
          }

          if (!hasCategories && time_series.some(d => d.baseline != null)) {
            traces.push({
              x: time_series.map(d => formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss")),
              y: time_series.map(d => d.baseline),
              type: 'scatter', mode: 'lines',
              name: 'Baseline (7d prior)',
              hovertemplate: getHoverTemplate(actualMetric, 'Baseline'),
              line: { color: '#a1a1aa', dash: 'dot', width: 2 }
            })
          }

          if (!hasCategories && trend !== 'off') {
            const xValues = time_series.map(d => formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss"))
            const yValues = time_series.map(d => d.value)
            const n = yValues.length
            let windowSize = 0
            if (trend === 'auto') {
              if (n > 1000) windowSize = Math.floor(n / 20)
              else if (n > 100) windowSize = Math.floor(n / 10)
              else windowSize = Math.floor(n / 5)
            } else {
              const trendMap: Record<string, number> = { '1m': 60, '5m': 300, '1h': 3600, '1d': 86400 }
              const actualInterval = aggregates?.interval || config.effectiveInterval
              windowSize = Math.floor((trendMap[trend] ?? 0) / (INTERVAL_SECONDS[actualInterval as keyof typeof INTERVAL_SECONDS] ?? 60))
            }
            if (windowSize > 1) {
              const trendY = new Array(n).fill(null)
              for (let i = windowSize - 1; i < n; i++) {
                let sum = 0, count = 0
                for (let j = 0; j < windowSize; j++) {
                  const v = yValues[i - j]
                  if (v != null) { sum += v; count++ }
                }
                trendY[i] = count > 0 ? sum / count : null
              }
              traces.push({
                x: xValues, y: trendY,
                type: 'scatter', mode: 'lines',
                name: `${trend === 'auto' ? 'Auto ' : ''}Trend`,
                hovertemplate: getHoverTemplate(actualMetric),
                line: { color: '#f97316', width: 3 }
              })
            }
          }
          return traces
        }, [aggregates?.time_series, aggregates?.metric, aggregates?.interval, compareAggregates?.time_series, compareMode, compareStartTime, startTime, trend, timezone, metric, config.effectiveInterval, hiddenCategories, catalog])

        const chartLayout = React.useMemo(() => {
          const actualMetric = aggregates?.metric || metric
          const metricField = catalog?.fields?.find(f => f.id === actualMetric)
          
          return {
            ...TIME_HOVER_LAYOUT,
            barmode: trafficData.length > 1 && trafficData[0]?.type === 'bar' ? 'stack' : undefined,
            showlegend: trafficData.some(t => t.showlegend !== false),
            yaxis: {
              title: metricField?.unit || (actualMetric === 'requests' ? 'reqs' : ''),
              ticksuffix: metricField?.unit || '',
              separatethousands: true,
              exponentformat: 'none'
            },
            xaxis: makeTimeXAxis(startTime, endTime, timezone),
          }
        }, [trafficData, aggregates?.metric, metric, startTime, endTime, timezone, catalog])

        const handleRowClick = React.useCallback((column: string, value: string | number) => {
          React.startTransition(() => {
            addFilter(column, String(value), 'include')
          })
        }, [addFilter])

        const handleChartRelayout = React.useCallback((event: any) => {
          // Skip non-range events (autorange toggle, spike config, etc.)
          if (event?.['xaxis.autorange'] === true || event?.['xaxis.showspikes'] !== undefined) return

          const x0 = event?.['xaxis.range[0]'] ?? event?.['xaxis.range']?.[0]
          const x1 = event?.['xaxis.range[1]'] ?? event?.['xaxis.range']?.[1]

          if (x0 === undefined || x1 === undefined) return

          try {
            const toLocalStr = (val: string | number) => {
              if (typeof val === 'number') {
                const d = new Date(val)
                const pad = (n: number) => n.toString().padStart(2, '0')
                return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
              }
              return val.replace(' ', 'T')
            }
            const parsedStart = parseFromInput(toLocalStr(x0), timezone)
            const parsedEnd = parseFromInput(toLocalStr(x1), timezone)
            if (parsedStart && parsedEnd) {
              setRange(parsedStart, parsedEnd)
            }
          } catch (e) {
            console.error("Failed to parse chart relayout event", e)
          }
        }, [setRange, timezone])

        const handleCountryClick = React.useCallback((countryName: string) => {
          React.startTransition(() => {
            addFilter('country', countryName, 'include')
          })
        }, [addFilter])

        // ── Raw logs columns ───────────────────────────────────────────────────────

        // Catalog-driven option list for the raw-logs column dropdown. Lets
        // users toggle on heavy fields (ua, referer, ja4, etc.) that aren't in
        // DEFAULT_RAW_COLUMNS — toggling refetches with the expanded set.
        const rawColumnOptions = React.useMemo(() => {
          const fields = (catalog?.fields as any[]) || []
          const seen = new Set<string>()
          const out: { id: string; label: string }[] = []
          for (const f of fields) {
            if (!f?.id || RAW_DROPDOWN_EXCLUDE.has(f.id) || f.group === 'METRICS') continue
            if (seen.has(f.id)) continue
            seen.add(f.id)
            out.push({ id: f.id, label: getFieldLabel(f.id) })
          }
          // Defensive: ensure any currently-selected column not present in the
          // catalog (e.g. custom field that bootstrap hasn't loaded yet) still
          // shows up checked in the dropdown.
          for (const id of selectedRawColumns) {
            if (!seen.has(id)) {
              seen.add(id)
              out.push({ id, label: getFieldLabel(id) })
            }
          }
          return out
        }, [catalog, getFieldLabel, selectedRawColumns])

        const rawColumnVisibility = React.useMemo(() => {
          const v: Record<string, boolean> = {}
          for (const opt of rawColumnOptions) v[opt.id] = selectedRawColumns.includes(opt.id)
          return v
        }, [rawColumnOptions, selectedRawColumns])

        // hasSidCol still drives the FLAG-COLUMN render below — it can't
        // be determined until rawLogs returns. labelsQuery, however, fires
        // immediately on serviceId (see comment on labelsQuery below).
        const hasSidCol = !!rawLogs?.columns?.includes('edge_sid')

        // Pull session-labels for the active service so we can render a
        // colored Flag icon per row reflecting the current label state.
        // Fire as soon as a serviceId is known — previously this was gated
        // on `hasSidCol`, which created a real request waterfall: rawLogs
        // took ~1s on prod, and this 10ms query couldn't start until then,
        // blocking DataTable's first paint by the full rawLogs round-trip.
        // The result is harmless when the service has no edge_sid column
        // (the FLAG column simply doesn't render and the data goes unused).
        const labelsQuery = useQuery({
          queryKey: ['scoring-labels', activeServiceId],
          enabled: !!activeServiceId,
          queryFn: async ({ signal }) => {
            const { data, response } = await client.GET(
              '/api/services/{service_id}/scoring/labels' as any,
              { params: { path: { service_id: activeServiceId || '' } } } as any,
            )
            if (!response.ok) throw new Error(`status ${response.status}`)
            return data as { labels: Array<{ sid: string; label: LabelValue }> }
          },
        })
        const labelBySid = React.useMemo(() => {
          const m = new Map<string, LabelValue>()
          for (const l of labelsQuery.data?.labels ?? []) m.set(l.sid, l.label)
          return m
        }, [labelsQuery.data])

        const columns: ColumnDef<any>[] = React.useMemo(() => {
          if (!rawLogs?.columns) return []
          const dataCols: ColumnDef<any>[] = rawLogs.columns.map((col: string): ColumnDef<any> => ({
            id: col,
            accessorFn: (row) => row[col],
            meta: { label: getFieldLabel(col) },
            header: getFieldLabel(col),
            cell: ({ row }: { row: any }) => {
              const value = row.original[col]
              if (col === 'timestamp') return (
                <span className="text-xs font-mono whitespace-nowrap">
                  {full(value as string)} {abbr()}
                </span>
              )
              if (col === 'status') {
                const status = Number(value)
                const variant = status >= 500 ? 'destructive' : 'outline'
                return (
                  <FilterPopover
                    col={col}
                    value={String(status)}
                    onInclude={() => React.startTransition(() => addFilter(col, String(status), 'include'))}
                    onExclude={() => React.startTransition(() => addFilter(col, String(status), 'exclude'))}
                    triggerClassName={badgeVariants({ variant: variant as any, className: 'cursor-pointer' })}
                    triggerLabel={<span>{status}</span>}
                    header={<p className="text-xs text-muted-foreground mb-2 font-mono">{col}: {status}</p>}
                    contentClassName="w-44 p-2"
                  />
                )
              }
              const strVal = String(value ?? '')
              if (strVal === '') {
                return <span className="text-muted-foreground/40 text-xs">—</span>
              }
              return (
                <FilterPopover
                  col={col}
                  value={strVal}
                  onInclude={() => React.startTransition(() => addFilter(col, strVal, 'include'))}
                  onExclude={() => React.startTransition(() => addFilter(col, strVal, 'exclude'))}
                  triggerClassName="text-xs font-mono cursor-pointer hover:text-primary underline-offset-2 hover:underline"
                  triggerLabel={<span className="truncate max-w-[200px] inline-block">{strVal}</span>}
                />
              )
            }
          }))
          // Flag column: only shown when edge_sid is present in the schema
          // (i.e. session scoring is enabled). Disabled for rows where the
          // sid is empty (cookieless requests — already caught by L1).
          if (hasSidCol && activeServiceId) {
            dataCols.push({
              id: '__flag',
              accessorFn: (_row: any) => '',
              meta: { label: 'Flag' },
              header: 'Flag',
              cell: ({ row }: { row: any }) => {
                const sid = String(row.original['edge_sid'] ?? '')
                return (
                  <FlagSessionPopover
                    serviceId={activeServiceId}
                    sid={sid}
                    sampleIp={String(row.original['ip'] ?? '')}
                    sampleUa={String(row.original['ua'] ?? '')}
                    sampleUrl={String(row.original['url'] ?? '')}
                    currentLabel={labelBySid.get(sid) ?? null}
                  />
                )
              },
            } as ColumnDef<any>)
          }
          return dataCols
        }, [rawLogs?.columns, full, abbr, addFilter, getFieldLabel, hasSidCol, activeServiceId, labelBySid])

        const visibleCardList = React.useMemo(
          () => allCards.filter((c: any) => visibleCards.has(c.id)),
          [allCards, visibleCards]
        )

        return (
          <>
            {/* ── Main charts ── */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="border rounded-lg p-4 flex flex-col relative overflow-hidden">
                <div className="flex flex-col xl:flex-row xl:items-center justify-between gap-3 mb-4 relative z-10">
                  <div className="flex flex-row items-center gap-2 xl:gap-4 flex-wrap">
                    <h3 className="text-sm font-medium whitespace-nowrap hidden sm:block">Traffic over Time</h3>
                    <div className="flex flex-row items-center gap-2">
                      <ButtonGroup>
                        {(() => {
                          const metricsFields = catalog?.fields?.filter(f => f.group === 'METRICS') || []
                          const shortLabels: Record<string, string> = {
                            'requests': 'Reqs',
                            'hit_rate': 'CHR',
                            '5xx': '5xx',
                            '4xx': '4xx',
                            'p50_latency': 'p50',
                            'p95_latency': 'p95',
                            'p99_latency': 'p99',
                            'throughput': 'Throughput',
                            'req_size': 'Req Size',
                            'ttfb': 'TTFB'
                          }

                          // We want to group latencies into a dropdown
                          const latencyIds = ['p50_latency', 'p95_latency', 'p99_latency']
                          const otherMetrics = metricsFields.filter(f => !latencyIds.includes(f.id))
                          
                          // Re-order to match desired UI layout: Reqs, 5xx, 4xx, CHR, Latency, ...
                          const order = ['requests', '5xx', '4xx', 'hit_rate']
                          const orderedMetrics = [
                            ...order.map(id => otherMetrics.find(f => f.id === id)).filter(Boolean),
                            ...otherMetrics.filter(f => !order.includes(f.id))
                          ] as any[]

                          const elements = orderedMetrics.map(m => (
                            <Button
                              key={m.id}
                              variant={metric === m.id ? 'default' : 'ghost'}
                              size="sm"
                              onClick={() => React.startTransition(() => setMetric(m.id))}
                              className={cn(
                                "h-6 text-[10px] px-2 shadow-none transition-colors",
                                metric === m.id ? "bg-primary text-primary-foreground hover:bg-primary/90" : "hover:text-primary hover:bg-muted"
                              )}
                            >
                              {shortLabels[m.id] || m.label}
                            </Button>
                          ))

                          // Insert Latency dropdown after CHR (hit_rate)
                          const isLatency = metric.endsWith('_latency')
                          const latLabel = isLatency ? metric.split('_')[0] : 'p95'
                          const latencyDropdown = (
                            <DropdownMenu key="latency">
                              <DropdownMenuTrigger className={cn(
                                buttonVariants({ variant: isLatency ? 'default' : 'ghost', size: 'sm' }),
                                "h-6 text-[10px] px-2 shadow-none transition-colors",
                                isLatency ? "bg-primary text-primary-foreground hover:bg-primary/90" : "hover:text-primary hover:bg-muted"
                              )}>
                                Latency ({latLabel}) <ChevronDown className="ml-1 h-3 w-3" />
                              </DropdownMenuTrigger>
                              <DropdownMenuContent align="start">
                                <DropdownMenuItem onClick={() => setMetric('p50_latency')} className="text-xs">p50 Latency</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => setMetric('p95_latency')} className="text-xs">p95 Latency</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => setMetric('p99_latency')} className="text-xs">p99 Latency</DropdownMenuItem>
                              </DropdownMenuContent>
                            </DropdownMenu>
                          )

                          const chrIndex = orderedMetrics.findIndex(m => m.id === 'hit_rate')
                          if (chrIndex !== -1) {
                            elements.splice(chrIndex + 1, 0, latencyDropdown)
                          } else {
                            elements.push(latencyDropdown)
                          }

                          return elements
                        })()}
                      </ButtonGroup>
                      
                      {intervalButtons}
                    </div>
                  </div>
                  <div className="flex items-center gap-3">
                    {isFetchingAggs && !isLoadingAggs && (
                      <div className="flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-primary/10 text-primary text-[10px] font-bold uppercase tracking-wider animate-pulse">
                        <span className="w-1.5 h-1.5 rounded-full bg-primary" />
                        Updating
                      </div>
                    )}
                  </div>
                </div>

                {/* Custom Category Legend */}
                {trafficData.length > 1 && trafficData[0]?.type === 'bar' && (
                  <div className="flex items-center gap-2 mb-2 relative z-10 flex-wrap">
                    <ButtonGroup>
                      {trafficData.filter(t => t.type === 'bar').map(trace => {
                        const isHidden = hiddenCategories.has(trace.name)
                        return (
                          <Button
                            key={trace.name}
                            variant={isHidden ? 'ghost' : 'default'}
                            size="sm"
                            onClick={() => React.startTransition(() => toggleCategory(trace.name))}
                            className={cn(
                              "h-6 text-[10px] px-2 shadow-none transition-colors",
                              !isHidden ? "bg-primary text-primary-foreground hover:bg-primary/90" : "hover:text-primary hover:bg-muted"
                            )}
                          >
                            <span className="w-1.5 h-1.5 rounded-full mr-1.5" style={{ backgroundColor: trace.marker.color as string }} />
                            {trace.name}
                          </Button>
                        )
                      })}
                    </ButtonGroup>
                  </div>
                )}

                <div className="relative flex-1 mb-4">
                  {(!isReady || (isLoadingAggs && !aggregates)) || (isFetchingAggs && trafficData.length === 0) ? (
                    <div className="h-[300px] flex items-center justify-center bg-muted/20 rounded-md">
                      <span className="text-muted-foreground text-sm animate-pulse">
                        {!isReady ? 'Initializing...' : 'Crunching logs...'}
                      </span>
                    </div>
                  ) : trafficData.length === 0 ? (
                    <div className="h-[300px] flex items-center justify-center bg-muted/10 border border-dashed rounded-md">
                      <div className="flex flex-col items-center text-muted-foreground text-center px-4">
                        <span className="text-sm font-medium">No data available</span>
                        <span className="text-xs mt-1">
                          {(() => {
                            if (metric === 'ttfb_client') {
                              return "Requires Infrastructure (Group C) fields to be enabled in Fastly logging."
                            }
                            if (metric === 'req_size') {
                              return "Requires Request Identity (Group A) fields to be enabled in Fastly logging."
                            }
                            return "No logs found for this period."
                          })()}
                        </span>
                      </div>
                    </div>
                  ) : (
                    <div className={cn("transition-opacity duration-100", isFetchingAggs && "opacity-40 pointer-events-none")}>
                      <TimeSeriesChart
                        data={trafficData}
                        layout={chartLayout}
                        height={300}
                        onRelayout={handleChartRelayout}
                        startTime={startTime}
                        endTime={endTime}
                        timezone={timezone}
                      />
                    </div>
                  )}
                </div>

                <div className="mt-auto pt-2 border-t flex items-center gap-2 relative z-10">
                  <span className="text-[10px] uppercase font-bold text-muted-foreground">Trend:</span>
                  <ButtonGroup className="bg-muted/50 p-1">
                    {TRENDS.map(t => (
                      <Button
                        key={t.value}
                        variant={trend === t.value ? 'secondary' : 'ghost'}
                        size="sm"
                        onClick={() => React.startTransition(() => setTrend(t.value))}
                        disabled={!config.validTrends.has(t.value)}
                        className="h-6 text-[10px] px-2 shadow-none disabled:opacity-30"
                      >
                        {t.label}
                      </Button>
                    ))}
                  </ButtonGroup>
                </div>
              </div>

              <div className={cn("border rounded-lg p-4 flex flex-col transition-opacity duration-100", isFetchingAggs && "opacity-40 pointer-events-none")}>
                <h3 className="text-sm font-medium mb-4">Requests by Country</h3>
                {(!isReady || (isLoadingAggs && !aggregates)) || (isFetchingAggs && (!aggregates?.map_data || aggregates.map_data.length === 0)) ? (
                  <div className="flex-1 min-h-[300px] flex items-center justify-center bg-muted/20 rounded-md">
                    <span className="text-muted-foreground text-sm animate-pulse">
                      {!isReady ? 'Initializing...' : 'Mapping traffic...'}
                    </span>
                  </div>
                ) : !aggregates?.map_data || aggregates.map_data.length === 0 ? (
                  <div className="flex-1 min-h-[300px] flex items-center justify-center bg-muted/10 border border-dashed rounded-md">
                    <div className="flex flex-col items-center text-muted-foreground text-center px-4">
                      <span className="text-sm font-medium mb-1">No data available</span>
                      <span className="text-[10px] opacity-70">
                        {(() => {
                          const countryField = (catalog?.fields as any[])?.find(f => f.id === 'country')
                          const groupId = countryField?.group
                          if (groupId) {
                            const groupMeta = (catalog?.groups as any[])?.find(g => g.id === groupId)
                            if (groupMeta) {
                              return `Requires ${groupMeta.label} fields to be enabled in Fastly logging.`
                            }
                          }
                          return "Requires Geolocation fields to be enabled in Fastly logging."
                        })()}
                      </span>
                    </div>
                  </div>
                ) : (
                  <ChoroplethMap
                    data={aggregates?.map_data || []}
                    className="flex-1 min-h-[300px]"
                    onCountryClick={handleCountryClick}
                  />
                )}
              </div>
            </div>

            {/* ── Aggregation cards ── */}
            {visibleCardList.length > 0 && (() => {
              const visibleById = new Map(visibleCardList.map((c: any) => [c.id, c]))
              // Wrap each card in LazyMount so the FIRST dashboard paint
              // only mounts the cards above the fold (~5-10) instead of
              // all 86. Off-screen cards land as the user scrolls — the
              // rootMargin of 600px (one screen) pre-mounts before the
              // user actually reaches them, so they feel instant. Cuts
              // initial DOM nodes from ~860 to ~100 and skips ~80
              // TopTenTable mount cycles on first render. The loading
              // placeholder branch is NOT wrapped — it's already cheap
              // and we want every "Initializing..." tile visible.
              const renderCard = (card: any) => {
                if (!isReady || (isLoadingAggs && !aggregates)) {
                  return (
                    <div key={card.id} className="border rounded-lg p-4 h-[300px] flex items-center justify-center bg-muted/20">
                      <span className="text-muted-foreground text-xs animate-pulse">
                        {!isReady ? 'Initializing...' : 'Loading...'}
                      </span>
                    </div>
                  )
                }
                if (card.id === '_bot_name') {
                  return (
                    <LazyMount key={card.id} minHeight={300}>
                      <TopTenTable
                        title={card.label}
                        icon={<Bot className="h-4 w-4" />}
                        field="_bot_name"
                        inActiveFormat={card.inActiveFormat}
                        data={{
                          total: topBotsData?.bots?.reduce((acc: number, b: any) => acc + b.request_count, 0) || 0,
                          top: (topBotsData?.bots ?? []).map((b: any) => ({ value: b.id, label: b.name, count: b.request_count }))
                        }}
                        compareData={undefined}
                        onRowClick={handleRowClick}
                      />
                    </LazyMount>
                  )
                }
                if (card.id === '_ngwaf_bot_name') {
                  return (
                    <LazyMount key={card.id} minHeight={300}>
                      <TopTenTable
                        title={card.label}
                        field="_ngwaf_bot_name"
                        inActiveFormat={card.inActiveFormat}
                        data={{
                          total: (topBotsData?.ngwaf_bots ?? []).reduce((acc: number, b: any) => acc + b.request_count, 0),
                          top: (topBotsData?.ngwaf_bots ?? []).map((b: any) => ({ value: b.name, label: b.name, count: b.request_count }))
                        }}
                        compareData={undefined}
                        onRowClick={handleRowClick}
                      />
                    </LazyMount>
                  )
                }
                return (
                  <LazyMount key={card.id} minHeight={300}>
                    <TopTenTable
                      title={card.label}
                      field={card.id}
                      inActiveFormat={card.inActiveFormat}
                      data={aggregates?.data?.[card.id]}
                      compareData={compareMode ? compareAggregates?.data?.[card.id] : undefined}
                      onRowClick={handleRowClick}
                    />
                  </LazyMount>
                )
              }

              const sections = CARD_CATEGORIES.map(cat => ({
                ...cat,
                cards: cat.cardIds.map(id => visibleById.get(id)).filter(Boolean),
              })).filter(s => s.cards.length > 0)

              const customCards = visibleCardList.filter((c: any) => !CATEGORIZED_CARD_IDS.has(c.id))
              if (customCards.length > 0) {
                sections.push({ id: 'custom', label: 'Custom', cardIds: [], cards: customCards, tint: CUSTOM_TINT })
              }

              return (
                <div className={cn("flex flex-col gap-4 transition-opacity duration-100", isFetchingAggs && "opacity-40 pointer-events-none")}>
                  {sections.map(section => {
                    const isCollapsed = collapsedSections.has(section.id)
                    const Chevron = isCollapsed ? ChevronRight : ChevronDown
                    return (
                      <section
                        key={section.id}
                        className={cn("rounded-lg border", section.tint.bg, section.tint.border)}
                      >
                        <button
                          type="button"
                          onClick={() => toggleSectionCollapsed(section.id)}
                          aria-expanded={!isCollapsed}
                          aria-controls={`section-${section.id}-cards`}
                          className="w-full flex items-center gap-2 px-4 py-2.5 text-left hover:bg-black/[0.02] dark:hover:bg-white/[0.03] rounded-t-lg transition-colors group"
                        >
                          <Chevron className="h-3.5 w-3.5 text-muted-foreground group-hover:text-foreground transition-colors" />
                          <span className={cn("inline-block w-1.5 h-1.5 rounded-full", section.tint.dot)} />
                          <h3 className="text-[10px] uppercase font-bold tracking-wider text-muted-foreground group-hover:text-foreground transition-colors">
                            {section.label}
                          </h3>
                          <span className="text-[10px] text-muted-foreground/60 font-mono">
                            {section.cards.length}
                          </span>
                        </button>
                        {!isCollapsed && (
                          <div
                            id={`section-${section.id}-cards`}
                            className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4 px-4 pb-4 pt-1"
                          >
                            {section.cards.map((card: any) => renderCard(card))}
                          </div>
                        )}
                      </section>
                    )
                  })}
                </div>
              )
            })()}

            {/* ── Raw logs table ── */}
            <AnalyticsCard
              title="Raw Logs"
              isLoading={!isReady || (isLoadingRaw && !rawLogs)}
              isFetching={isFetchingRaw}
              className="min-h-[400px]"
              contentClassName="p-0"
              headerAction={
                <div className="flex items-center gap-2">
                  <ColumnVisibilityDropdown
                    columns={rawColumnOptions}
                    visibility={rawColumnVisibility}
                    onChange={toggleRawColumn}
                  />

                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 text-[10px] gap-1.5"
                    onClick={async () => {
                      const body = {
                        start_time: startTime,
                        end_time: endTime,
                        filters: filterPayload,
                        columns: rawLogs?.columns || []
                      }
                      // Raw fetch (not typed `client`): this endpoint
                      // streams a CSV body; openapi-fetch's JSON
                      // deserialization in middleware would corrupt it.
                      const { getApiBase } = await import('@/lib/api')
                      const res = await fetch(`${getApiBase()}/api/dashboard/raw/csv`, {
                        method: 'POST',
                        headers: { 
                          'Content-Type': 'application/json',
                          'x-service-id': useServiceStore.getState().activeServiceId || ''
                        },
                        body: JSON.stringify(body)
                      })
                      const blob = await res.blob()
                      downloadBlob(blob, `logs_${activeServiceId}_${Date.now()}.csv`)
                    }}
                  >
                    <Download className="h-3 w-3" />
                    Export CSV
                  </Button>
                </div>
              }
            >
              <DataTable
                columns={columns}
                data={rawLogs?.data || []}
                hideToolbar={true}
                sorting={sorting}
                onSortingChange={setSorting}
              />
            </AnalyticsCard>
          </>
        )
      }}
    </ReportLayout>
  )
}
