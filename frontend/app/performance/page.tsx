'use client'

import { SlowestUrlsHelp, SlowestNetworksHelp, CacheTtlHelp, OriginVsEdgeHelp } from "./help-content";
import React from 'react'
import { client } from '@/lib/api'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { useColumnVisibility } from '@/hooks/useColumnVisibility'
import { PlotlyChart } from '@/components/PlotlyChart'
import { DataTable } from '@/components/DataTable'
import { Zap, Server, Shield, Clock, Activity, TrendingUp, Network } from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { ColumnVisibilityDropdown } from '@/components/DataTable'
import { makeLatencyColumns } from '@/lib/table-utils'
import { useFieldLabel } from '@/hooks/useFieldLabel';

const URL_COLUMN_IDS = ['url', 'requests', 'avg', 'p50', 'p95', 'p99']
const ASN_COLUMN_IDS = ['label', 'requests', 'avg', 'p50', 'p95', 'p99']

const urlColumns = makeLatencyColumns('url', 'URL', 'url')
const asnColumns = makeLatencyColumns('label', 'ASN', 'asn')

export default function PerformancePage() {
  const getFieldLabel = useFieldLabel()

  const [urlVisibility, setUrlVisibility, onUrlVisChange] = useColumnVisibility()
  const [asnVisibility, setAsnVisibility, onAsnVisChange] = useColumnVisibility()

  return (
    <ReportLayout
      title="Performance"
      description="Analyze latency, cache efficiency, and origin vs edge processing time."
      icon={Zap}
    >
      {({
        startTime,
        endTime,
        activeServiceId,
        filterPayload,
      }) => {
        const { data, isLoading, isFetching } = useServiceQuery(
    ['performance', 'aggregates', activeServiceId, startTime, endTime, filterPayload, 'p99'],
    async () => {
      const { data } = await client.POST("/api/performance/aggregates", {
        body: {
          start_time: startTime!,
          end_time: endTime!,
          filters: filterPayload,
          sort_by: 'p99',
        }
      })
      return data
    }
  )

  // ── Charts ──────────────────────────────────────────────────────────────

  const ttlDistData = React.useMemo(() => {
    if (!data?.ttl_dist?.length) return []
    return [{
      x: data.ttl_dist.map(d => d.bucket),
      y: data.ttl_dist.map(d => d.count),
      type: 'bar',
      marker: { color: '#6366f1' }
    }]
  }, [data?.ttl_dist])

  const scatterData = React.useMemo(() => {
    if (!data?.scatter?.length) return []
    const hit = data.scatter.filter(d => (d.cache as string).startsWith('HIT'))
    const miss = data.scatter.filter(d => !(d.cache as string).startsWith('HIT'))
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
  }, [data?.scatter])

  return (
    <>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard
          title="Slowest URLs"
          icon={<Server className="h-4 w-4" />}
          headerAction={
            <div className="flex items-center gap-2">
              <ColumnVisibilityDropdown columns={URL_COLUMN_IDS.map(id => ({ id, label: getFieldLabel(id) }))} visibility={urlVisibility} onChange={onUrlVisChange} />
            </div>
          }
          isLoading={isLoading}
          isFetching={isFetching}
          className="min-h-[300px]"
          contentClassName="p-0"
          helpContent={<SlowestUrlsHelp />}
        >
          <DataTable
            columns={urlColumns}
            data={data?.top_urls || []}
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
          isLoading={isLoading}
          isFetching={isFetching}
          className="min-h-[300px]"
          contentClassName="p-0"
          helpContent={<SlowestNetworksHelp />}
        >
          <DataTable
            columns={asnColumns}
            data={data?.top_asns || []}
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
          isLoading={isLoading}
          isFetching={isFetching}
          className="h-[360px]"
          contentClassName="p-2"
          helpContent={<CacheTtlHelp />}
        >
          <PlotlyChart
            data={ttlDistData}
            layout={{
              yaxis: { title: 'Count' }
            }}
            height="100%"
          />        </AnalyticsCard>

        <AnalyticsCard
          title="Origin vs Edge Processing (ms)"
          icon={<Zap className="h-4 w-4" />}
          isLoading={isLoading}
          isFetching={isFetching}
          className="h-[360px]"
          contentClassName="p-2"
          helpContent={<OriginVsEdgeHelp />}
        >
          <PlotlyChart
            data={scatterData}
            layout={{
              xaxis: { title: 'Origin TTFB (ms)', ticksuffix: 'ms', separatethousands: true, exponentformat: 'none' },
              yaxis: { title: 'Edge Processing (ms)', ticksuffix: 'ms', separatethousands: true, exponentformat: 'none' }
            }}
            height="100%"
          />        </AnalyticsCard>
      </div>
    </>
      )
    }}
  </ReportLayout>
  )
}
