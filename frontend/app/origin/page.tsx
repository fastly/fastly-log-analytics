'use client'

import { useTimeseriesToTraces } from '@/hooks/useTimeseriesToTraces'
import React from 'react'
import { client } from '@/lib/api'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { useColumnVisibility } from '@/hooks/useColumnVisibility'
import { Server } from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { INTERVAL_SECONDS } from '@/lib/constants'
import { formatDate } from '@/lib/date'
import { Aggregates } from './_sections/Aggregates'
import { Timeseries } from './_sections/Timeseries'
import { LatencyHeatmap } from './_sections/LatencyHeatmap'

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
    async ({ signal }) => {
      const { data } = await client.POST("/api/origin/summary", { signal,
        body: { start_time: startTime, end_time: endTime, filters: filterPayload }
      })
      return data as any
    }
  )

  const originTs = useServiceQuery(
    ['origin', 'timeseries', activeServiceId, startTime, endTime, filterPayload, config.effectiveInterval, originMetric, originPercentile],
    async ({ signal }) => {
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

      const { data } = await client.POST('/api/origin/timeseries', { signal,
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
    async ({ signal }) => {
      const { data } = await client.POST("/api/origin/slow-urls", { signal,
        body: { start_time: startTime, end_time: endTime, filters: filterPayload, limit: 20, min_requests: 10 }
      })
      return data as any
    }
  )

  const statusCodes = useServiceQuery(
    ['origin', 'status-codes', activeServiceId, startTime, endTime, filterPayload],
    async ({ signal }) => {
      const { data } = await client.POST("/api/origin/status-codes", { signal,
        body: { start_time: startTime, end_time: endTime, filters: filterPayload }
      })
      return data as any
    }
  )

  const popLatency = useServiceQuery(
    ['origin', 'pop-latency', activeServiceId, startTime, endTime, filterPayload],
    async ({ signal }) => {
      const { data } = await client.POST("/api/origin/pop-latency", { signal,
        body: { start_time: startTime, end_time: endTime, filters: filterPayload, limit: 30 }
      })
      return data as any
    }
  )

  const ipHealth = useServiceQuery(
    ['origin', 'ip-health', activeServiceId, startTime, endTime, filterPayload],
    async ({ signal }) => {
      const { data } = await client.POST("/api/origin/ip-health", { signal,
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
      <Aggregates summary={summary} />
      <Timeseries
        originTs={originTs}
        originTsChartData={originTsChartData}
        statusCodes={statusCodes}
        statusData={statusData}
        originMetric={originMetric}
        setOriginMetric={setOriginMetric}
        originPercentile={originPercentile}
        setOriginPercentile={setOriginPercentile}
        trend={trend}
        setTrend={setTrend}
        config={config}
        intervalButtons={intervalButtons}
        startTime={startTime}
        endTime={endTime}
        timezone={timezone}
      />
      <LatencyHeatmap
        slowUrls={slowUrls}
        popLatency={popLatency}
        ipHealth={ipHealth}
        summary={summary}
        urlVisibility={urlVisibility}
        setUrlVisibility={setUrlVisibility}
        onUrlVisChange={onUrlVisChange}
        popVisibility={popVisibility}
        setPopVisibility={setPopVisibility}
        onPopVisChange={onPopVisChange}
        ipVisibility={ipVisibility}
        setIpVisibility={setIpVisibility}
        onIpVisChange={onIpVisChange}
      />
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
