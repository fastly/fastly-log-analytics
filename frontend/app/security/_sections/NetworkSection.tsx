import React from 'react'
import { Globe, Network, Repeat } from 'lucide-react'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { useTimeseriesToTraces, type TimeseriesDataPoint } from '@/hooks/useTimeseriesToTraces'
import { SECURITY_INFO } from './securityInfo'
import type { components } from '@/types/api.generated'

type SecurityData = components['schemas']['SecurityAggregatesResponse']

type Props = {
  data: SecurityData | undefined
  isLoading: boolean
  isFetching: boolean
  timezone: string
  commonTimeLayout: any
}

export function NetworkSection({
  data,
  isLoading,
  isFetching,
  timezone,
  commonTimeLayout,
}: Props) {
  // Re-narrow: schema types these dict-of-unknown rows opaquely; the
  // backend invariant is `{ time, ...metric_keys }`.
  const ipv6Data = useTimeseriesToTraces(
    data?.ipv6_adoption as TimeseriesDataPoint[] | undefined,
    [{ key: 'pct', name: 'IPv6 %', color: '#8b5cf6', fill: 'tozeroy' }],
    timezone,
  )

  const proxyData = React.useMemo(() => {
    const proxy_dist = data?.proxy_dist
    if (!proxy_dist?.length) return []
    return [{
      values: proxy_dist.map((d: any) => d.count),
      labels: proxy_dist.map((d: any) => d.type),
      type: 'pie',
      hole: 0.4,
      marker: { colors: ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'] }
    }]
  }, [data])

  const connReuseData = React.useMemo(() => {
    const conn_reuse_dist = data?.conn_reuse_dist
    if (!conn_reuse_dist?.length) return []
    return [{
      x: conn_reuse_dist.map((d: any) => d.bucket),
      y: conn_reuse_dist.map((d: any) => d.count),
      type: 'bar',
      marker: { color: '#06b6d4' }
    }]
  }, [data])

  return (
    <>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="IPv6 Adoption over Time"
          icon={<Globe className="h-4 w-4" />}
          isLoading={isLoading}
          isFetching={isFetching}
          className="h-[360px]"
          contentClassName="p-2"
          helpTitle={SECURITY_INFO.ipv6.title}
          helpContent={SECURITY_INFO.ipv6.body}
        >
          {ipv6Data.length === 0 && !isLoading ? (
            <div className="flex flex-col items-center justify-center h-full text-muted-foreground text-center px-4">
              <span className="text-sm font-medium mb-1">No data available</span>
              <span className="text-[10px] opacity-70">
                Requires Infrastructure (Group C) fields to be enabled in Fastly logging.
              </span>
            </div>
          ) : (
            <PlotlyChart
              data={ipv6Data as any[]}
              layout={{
                ...commonTimeLayout,
                yaxis: { title: 'IPv6 %', range: [0, 100] }
              }}
              height="100%"
            />
          )}
        </AnalyticsCard>

        <AnalyticsCard
          title="Proxy/Anonymizer Breakdown"
          icon={<Network className="h-4 w-4" />}
          isLoading={isLoading}
          isFetching={isFetching}
          className="h-[360px]"
          contentClassName="p-2"
          helpTitle={SECURITY_INFO.proxy.title}
          helpContent={SECURITY_INFO.proxy.body}
        >
          {proxyData.length === 0 && !isLoading ? (
            <div className="flex flex-col items-center justify-center h-full text-muted-foreground text-center px-4">
              <span className="text-sm font-medium mb-1">No data available</span>
              <span className="text-[10px] opacity-70">
                Requires Security: Proxy & Anonymization (Group I) fields to be enabled in Fastly logging.
              </span>
            </div>
          ) : (
            <PlotlyChart
              data={proxyData as any[]}
              height="100%"
            />
          )}
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard
          title="Connection Reuse (Requests per Connection)"
          icon={<Repeat className="h-4 w-4" />}
          isLoading={isLoading}
          isFetching={isFetching}
          className="h-[360px]"
          contentClassName="p-2"
          helpTitle={SECURITY_INFO.conn_reuse.title}
          helpContent={SECURITY_INFO.conn_reuse.body}
        >
          {connReuseData.length === 0 && !isLoading ? (
            <div className="flex flex-col items-center justify-center h-full text-muted-foreground text-center px-4">
              <span className="text-sm font-medium mb-1">No data available</span>
              <span className="text-[10px] opacity-70">
                Requires Infrastructure (Group C) fields to be enabled in Fastly logging.
              </span>
            </div>
          ) : (
            <PlotlyChart
              data={connReuseData as any[]}
              layout={{ yaxis: { title: 'Count' } }}
              height="100%"
            />
          )}
        </AnalyticsCard>
      </div>
    </>
  )
}
