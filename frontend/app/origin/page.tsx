'use client'

import { useTimeseriesToTraces } from '@/hooks/useTimeseriesToTraces'
import React from 'react'
import { client } from '@/lib/api'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { useColumnVisibility } from '@/hooks/useColumnVisibility'
import { PlotlyChart } from '@/components/PlotlyChart'
import { DataTable, ColumnVisibilityDropdown } from '@/components/DataTable'
import { DashboardLinkCell } from '@/components/DashboardLinkCell'
import { Server, Activity, MapPin, Globe } from 'lucide-react'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { cn, formatBytes } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import { makeTimeXAxis } from '@/lib/chart-helpers'
import { ReportLayout } from '@/components/ReportLayout'
import { TRENDS, INTERVAL_SECONDS } from '@/lib/constants'
import { formatDate } from '@/lib/date'

const COLUMNS = {
  url: [
    {
      accessorKey: 'url',
      id: 'url', meta: { label: 'URL' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">URL</span>,
      cell: (info: any) => (
        <DashboardLinkCell
          value={info.getValue()}
          href={`/dashboard?filter_url=${encodeURIComponent(info.getValue())}`}
          className="font-mono text-xs"
          containerClassName="max-w-[400px]"
        />
      )
    },
    { accessorKey: 'requests', id: 'requests', meta: { label: 'Requests' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">Reqs</span>, cell: (info: any) => info.getValue().toLocaleString() },
    { accessorKey: 'p50_ms', id: 'p50_ms', meta: { label: 'Median (P50)' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">P50</span>, cell: (info: any) => <span>{info.getValue()?.toFixed(1)}ms</span> },
    { accessorKey: 'p95_ms', id: 'p95_ms', meta: { label: 'P95 Latency' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">P95</span>, cell: (info: any) => <span>{info.getValue()?.toFixed(1)}ms</span> },
    { accessorKey: 'p99_ms', id: 'p99_ms', meta: { label: 'P99 Latency' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">P99</span>, cell: (info: any) => <span>{info.getValue()?.toFixed(1)}ms</span> },
  ],
  pop: [
    {
      accessorKey: 'pop',
      id: 'pop', meta: { label: 'POP' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">POP</span>,
      cell: (info: any) => (
        <DashboardLinkCell
          value={info.getValue()}
          href={`/dashboard?filter_pop=${encodeURIComponent(info.getValue())}`}
          className="font-bold"
        />
      )
    },
    { accessorKey: 'requests', id: 'requests', meta: { label: 'Requests' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">Reqs</span>, cell: (info: any) => info.getValue().toLocaleString() },
    { accessorKey: 'p50_ms', id: 'p50_ms', meta: { label: 'Median (P50)' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">P50</span>, cell: (info: any) => <span>{info.getValue()?.toFixed(1)}ms</span> },
    { accessorKey: 'p95_ms', id: 'p95_ms', meta: { label: 'P95 Latency' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">P95</span>, cell: (info: any) => (
      <span className={cn(info.row.original.elevated ? "text-destructive font-bold" : "")}>
        {info.getValue()?.toFixed(1)}ms
      </span>
    )},
  ],
  ip: [
    {
      accessorKey: 'oip',
      id: 'oip', meta: { label: 'Origin IP' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">Origin IP</span>,
      cell: (info: any) => (
        <DashboardLinkCell
          value={info.getValue()}
          href={`/dashboard?filter_origin_ip=${encodeURIComponent(info.getValue())}`}
          className="font-mono text-xs"
        />
      )
    },
    { accessorKey: 'requests', id: 'requests', meta: { label: 'Requests' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">Reqs</span>, cell: (info: any) => info.getValue().toLocaleString() },
    { accessorKey: 'p50_ms', id: 'p50_ms', meta: { label: 'Median (P50)' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">P50</span>, cell: (info: any) => <span>{info.getValue()?.toFixed(1)}ms</span> },
    { accessorKey: 'p95_ms', id: 'p95_ms', meta: { label: 'P95 Latency' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">P95</span>, cell: (info: any) => <span>{info.getValue()?.toFixed(1)}ms</span> },
    { accessorKey: 'error_pct', id: 'error_pct', meta: { label: '5xx Errors %' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">5xx %</span>, cell: (info: any) => (
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

function OriginReportContent({
  startTime,
  endTime,
  timezone,
  activeServiceId,
  filterPayload,
  config,
  trend,
  setTrend,
  intervalButtons
}: any) {
  const [originMetric, setOriginMetric] = React.useState<'ttfb' | 'ttlb'>('ttfb')
  const [originPercentile, setOriginPercentile] = React.useState<'p50' | 'p95' | 'p99'>('p95')

  const [urlVisibility, setUrlVisibility, onUrlVisChange] = useColumnVisibility()
  const [popVisibility, setPopVisibility, onPopVisChange] = useColumnVisibility()
  const [ipVisibility, setIpVisibility, onIpVisChange] = useColumnVisibility()

  const summary = useServiceQuery(
    ['origin', 'summary', activeServiceId, startTime, endTime, filterPayload],
    async () => {
      const { data } = await client.POST("/api/origin/summary", {
        body: { start_time: startTime, end_time: endTime, filters: filterPayload }
      })
      return data as any
    }
  )

  const originTs = useServiceQuery(
    ['origin', 'timeseries', activeServiceId, startTime, endTime, filterPayload, config.effectiveInterval, originMetric, originPercentile],
    async () => {
      const intervalMap = {
        "1 second": 1 / 60,
        "1 minute": 1,
        "5 minutes": 5,
        "15 minutes": 15,
        "30 minutes": 30,
        "1 hour": 60,
        "6 hours": 360,
        "12 hours": 720,
        "1 day": 1440,
      }
      const bucketMinutes = (intervalMap as Record<string, number>)[config.effectiveInterval] || 5

      const { data } = await client.POST('/api/origin/timeseries', {
        body: {
          start_time: startTime,
          end_time: endTime,
          filters: filterPayload,
          bucket_minutes: bucketMinutes,
          split_by_leg: false,
          metric: originMetric,
          percentile: originPercentile,
        },
      })
      return data as any
    },
  )

  const slowUrls = useServiceQuery(
    ['origin', 'slow-urls', activeServiceId, startTime, endTime, filterPayload],
    async () => {
      const { data } = await client.POST("/api/origin/slow-urls", {
        body: { start_time: startTime, end_time: endTime, filters: filterPayload, limit: 20, min_requests: 10 }
      })
      return data as any
    }
  )

  const statusCodes = useServiceQuery(
    ['origin', 'status-codes', activeServiceId, startTime, endTime, filterPayload],
    async () => {
      const { data } = await client.POST("/api/origin/status-codes", {
        body: { start_time: startTime, end_time: endTime, filters: filterPayload }
      })
      return data as any
    }
  )

  const popLatency = useServiceQuery(
    ['origin', 'pop-latency', activeServiceId, startTime, endTime, filterPayload],
    async () => {
      const { data } = await client.POST("/api/origin/pop-latency", {
        body: { start_time: startTime, end_time: endTime, filters: filterPayload, limit: 30 }
      })
      return data as any
    }
  )

  const ipHealth = useServiceQuery(
    ['origin', 'ip-health', activeServiceId, startTime, endTime, filterPayload],
    async () => {
      const { data } = await client.POST("/api/origin/ip-health", {
        body: { start_time: startTime, end_time: endTime, filters: filterPayload, limit: 30 }
      })
      return data as any
    }
  )

  const baseOriginTraces = useTimeseriesToTraces(originTs.data?.series, React.useMemo(() => [
    { key: 'value', name: originMetric === 'ttfb' ? 'Origin TTFB' : 'Origin TTLB', color: '#ef4444', fill: 'tozeroy' }
  ], [originMetric]), timezone)

  const originTsChartData = React.useMemo(() => {
    // Clone to avoid mutating the hook's cached array
    const traces = baseOriginTraces.map(t => ({
      ...t,
      mode: 'lines+markers',
      marker: { size: 4, color: t.line?.color }
    }))

    if (trend !== 'off' && originTs.data?.series?.length) {
      const time_series = originTs.data.series
      const xValues = time_series.map((d: any) => formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss"))
      const yValues = time_series.map((d: any) => Number(d.value) || 0)
      const n = yValues.length
      let windowSize = 0
      if (trend === 'auto') {
        if (n > 1000) windowSize = Math.floor(n / 20)
        else if (n > 100) windowSize = Math.floor(n / 10)
        else windowSize = Math.floor(n / 5)
      } else {
        const trendMap: Record<string, number> = { '1m': 60, '5m': 300, '1h': 3600, '1d': 86400 }
        const actualInterval = config.effectiveInterval
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
          x: xValues,
          y: trendY,
          type: 'scatter',
          mode: 'lines',
          name: `${trend === 'auto' ? 'Auto ' : ''}Trend`,
          line: { color: '#f97316', width: 3 }
        } as any)
      }
    }

    return traces
  }, [baseOriginTraces, originTs.data?.series, timezone, trend, config.effectiveInterval])

  const statusData = React.useMemo(() => {
    if (!statusCodes.data?.rows?.length) return []
    return [{
      values: statusCodes.data.rows.map((r: any) => r.count),
      labels: statusCodes.data.rows.map((r: any) => `HTTP ${r.status}`),
      type: 'pie',
      hole: 0.4,
      marker: {
        colors: statusCodes.data.rows.map((r: any) =>
          r.status >= 500 ? '#ef4444' :
          r.status >= 400 ? '#f59e0b' :
          r.status >= 300 ? '#3b82f6' : '#10b981'
        )
      }
    }]
  }, [statusCodes.data?.rows])

  if (summary.data?.has_data === false) {
    return (
      <div className="flex flex-col items-center justify-center py-24 text-center">
         <div className="bg-muted h-16 w-16 rounded-full flex items-center justify-center mb-4">
           <Server className="h-8 w-8 text-muted-foreground" />
         </div>
         <h3 className="text-lg font-bold">No Origin Metrics Found</h3>
         <p className="text-muted-foreground max-w-md mt-2">
           Origin metrics require Group L (Origin Metrics) to be enabled in your log settings.
           If recently enabled, it may take a few minutes for data to appear.
         </p>
      </div>
    )
  }

  return (
    <>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        <AnalyticsCard
          title="Origin TTFB (P50)"
          isLoading={summary.isLoading}
          isFetching={summary.isFetching}
          className="h-auto"
          helpContent={<p>Median time taken by your backend to start returning a response after Fastly forwards a request. Lower is better.</p>}
        >
          <div className="flex flex-col">
            <div className="text-3xl font-bold">{summary.data?.ottfb_p50_ms?.toFixed(1)}ms</div>
            <div className="text-xs text-muted-foreground mt-1">Median backend response time</div>
          </div>
        </AnalyticsCard>
        <AnalyticsCard
          title="Origin TTFB (P95)"
          isLoading={summary.isLoading}
          isFetching={summary.isFetching}
          helpContent={<p>The 95th percentile of backend response times. Indicates the tail latency experienced by the slowest 5% of requests.</p>}
        >
          <div className="flex flex-col">
            <div className="text-3xl font-bold">{summary.data?.ottfb_p95_ms?.toFixed(1)}ms</div>
            <div className="text-xs text-muted-foreground mt-1">Tail latency (95th percentile)</div>
          </div>
        </AnalyticsCard>
        <AnalyticsCard
          title="Origin Error Rate"
          isLoading={summary.isLoading}
          isFetching={summary.isFetching}
          helpContent={<p>Percentage of cache miss/pass requests where the backend returned a 5xx HTTP status code.</p>}
        >
          <div className="flex flex-col">
            <div className={cn("text-3xl font-bold", (summary.data?.origin_error_rate || 0) > 0.01 ? "text-destructive" : "")}>
              {((summary.data?.origin_error_rate || 0) * 100).toFixed(2)}%
            </div>
            <div className="text-xs text-muted-foreground mt-1">Percentage of 5xx responses</div>
          </div>
        </AnalyticsCard>
        <AnalyticsCard
          title="Fetch Volume"
          isLoading={summary.isLoading}
          isFetching={summary.isFetching}
          helpContent={<p>The total number of requests sent to the backend (cache misses and passes) during this time window.</p>}
        >
          <div className="flex flex-col">
            <div className="text-3xl font-bold">
              {((summary.data?.total_misses || 0) + (summary.data?.total_passes || 0)).toLocaleString()}
            </div>
            <div className="text-xs text-muted-foreground mt-1">Total cache misses & passes</div>
          </div>
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
        <AnalyticsCard
          title="Origin Latency"
          icon={<Activity className="h-4 w-4" />}
          className="lg:col-span-2 h-[400px]"
          isLoading={originTs.isLoading}
          isFetching={originTs.isFetching}
          helpContent={<p>Time to First Byte (TTFB) measures the time to receive the first byte of the response headers from the origin. Time to Last Byte (TTLB) measures the time to receive the full response body.</p>}
          headerAction={
            <div className="flex items-center gap-2">
              <ButtonGroup>
                {(['ttfb', 'ttlb'] as const).map(m => (
                  <Button
                    key={m}
                    variant={originMetric === m ? 'default' : 'ghost'}
                    size="sm"
                    onClick={() => React.startTransition(() => setOriginMetric(m))}
                    className={cn(
                      "h-6 text-[10px] px-2 shadow-none transition-colors uppercase",
                      originMetric === m ? "bg-primary text-primary-foreground hover:bg-primary/90" : "hover:text-primary hover:bg-muted"
                    )}
                  >
                    {m}
                  </Button>
                ))}
              </ButtonGroup>
              <ButtonGroup>
                {(['p50', 'p95', 'p99'] as const).map(p => (
                  <Button
                    key={p}
                    variant={originPercentile === p ? 'default' : 'ghost'}
                    size="sm"
                    onClick={() => React.startTransition(() => setOriginPercentile(p))}
                    className={cn(
                      "h-6 text-[10px] px-2 shadow-none transition-colors",
                      originPercentile === p ? "bg-primary text-primary-foreground hover:bg-primary/90" : "hover:text-primary hover:bg-muted"
                    )}
                  >
                    {p}
                  </Button>
                ))}
              </ButtonGroup>
              <div className="ml-2">
                {intervalButtons}
              </div>
            </div>
          }
        >
          {originTs.isLoading || (originTs.isFetching && originTsChartData.length === 0) ? (
            <div className="h-[300px] flex items-center justify-center bg-muted/20 rounded-md">
              <span className="text-muted-foreground text-sm animate-pulse">Crunching logs...</span>
            </div>
          ) : originTsChartData.length === 0 ? (
            <div className="h-[300px] flex items-center justify-center bg-muted/10 border border-dashed rounded-md">
              <div className="flex flex-col items-center text-muted-foreground">
                <span className="text-sm font-medium">No data available</span>
                <span className="text-xs mt-1">No origin timing data found for this period.</span>
              </div>
            </div>
          ) : (
            <div className="flex flex-col h-full">
              <div className="relative flex-1 mb-4">
                <PlotlyChart
                  data={originTsChartData}
                  layout={{
                    hovermode: 'x unified',
                    yaxis: { title: 'ms', ticksuffix: 'ms', separatethousands: true, exponentformat: 'none' },
                    xaxis: makeTimeXAxis(startTime, endTime, timezone),
                  }}
                  height="100%"
                />
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
          )}
        </AnalyticsCard>

        <AnalyticsCard
          title="Status Code Distribution"
          icon={<Activity className="h-4 w-4" />}
          isLoading={statusCodes.isLoading}
          isFetching={statusCodes.isFetching}
          className="h-[400px]"
          contentClassName="p-2"
          helpContent={<p>A breakdown of the HTTP status codes returned directly by your backend servers during the selected time period.</p>}
        >
          <PlotlyChart
            data={statusData}
            height="100%"
          />
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Slowest URLs at Origin"
          icon={<Server className="h-4 w-4" />}
          isLoading={slowUrls.isLoading}
          isFetching={slowUrls.isFetching}
          contentClassName="p-0"
          helpContent={<p>A list of specific URLs that take the longest time to fetch from the origin.</p>}
          headerAction={
            <ColumnVisibilityDropdown
              columns={getLabels(['url', 'requests', 'p50_ms', 'p95_ms', 'p99_ms'])}
              visibility={urlVisibility}
              onChange={onUrlVisChange}
            />
          }
        >
          <DataTable
            columns={COLUMNS.url}
            data={slowUrls.data?.rows || []}
            emptyMessage={slowUrls.isLoading ? "" : "Requires Origin Metrics (Group L) fields to be enabled."}
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
          contentClassName="p-0"
          helpContent={<p>Backend latency aggregated by Fastly POP location.</p>}
          headerAction={
            <ColumnVisibilityDropdown
              columns={getLabels(['pop', 'requests', 'p50_ms', 'p95_ms'])}
              visibility={popVisibility}
              onChange={onPopVisChange}
            />
          }
        >
          <DataTable
            columns={COLUMNS.pop}
            data={popLatency.data?.rows || []}
            emptyMessage={popLatency.isLoading ? "" : "Requires Origin Metrics (Group L) and Infrastructure (Group C) fields to be enabled."}
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
          contentClassName="p-0"
          helpContent={<p>Latency and error rates for individual backend IP addresses.</p>}
          headerAction={
            <ColumnVisibilityDropdown
              columns={getLabels(['oip', 'requests', 'p50_ms', 'p95_ms', 'error_pct'])}
              visibility={ipVisibility}
              onChange={onIpVisChange}
            />
          }
        >
          <DataTable
            columns={COLUMNS.ip}
            data={ipHealth.data?.rows || []}
            emptyMessage={ipHealth.isLoading ? "" : "Requires Origin Metrics (Group L) fields to be enabled."}
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

export default function OriginPage() {
  return (
    <ReportLayout
      title="Origin Performance"
      description="Real-time backend health, fetch timing, and error analysis."
      icon={Server}
      defaultInterval="1 minute"
    >
      {(props) => <OriginReportContent {...props} />}
    </ReportLayout>
  )
}
