'use client'

import React from 'react'
import { ArrowRight } from 'lucide-react'
import Link from 'next/link'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { useServiceStore } from '@/stores/filterStore'
import { buildServiceHref, CHART_COLORS } from '@/lib/utils'
import {
  type MetricsData,
  type TabCommonProps,
  RealtimeChart,
  MetricCardGrid,
  TabPanelShell,
  SectionHeading,
  seriesTraces,
} from './ControlRoomWidgets'

export function AdminHealthTab({
  tick,
  rtDown,
  connected,
  series,
  rollingAvg,
}: Omit<TabCommonProps, 'historicalHref'> & { connected: boolean }) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const data = tick?.data
  const vclSubs = [
    {
      sub: 'recv',
      color: CHART_COLORS.info,
      timeMs: data?.vcl_recv_time_ms,
      timeFn: (d: MetricsData) => d.vcl_recv_time_ms ?? 0,
      countFn: (d: MetricsData) => d.vcl_recv_count ?? 0,
      helpText:
        'Time in vcl_recv — request routing, ACL checks, header manipulation. High values may indicate complex routing logic or excessive table lookups.',
    },
    {
      sub: 'fetch',
      color: '#f97316',
      timeMs: data?.vcl_fetch_time_ms,
      timeFn: (d: MetricsData) => d.vcl_fetch_time_ms ?? 0,
      countFn: (d: MetricsData) => d.vcl_fetch_count ?? 0,
      helpText:
        'Time in vcl_fetch — origin response processing, cache TTL setting, header rewriting. Only runs on cache misses.',
    },
    {
      sub: 'deliver',
      color: '#22c55e',
      timeMs: data?.vcl_deliver_time_ms,
      timeFn: (d: MetricsData) => d.vcl_deliver_time_ms ?? 0,
      countFn: (d: MetricsData) => d.vcl_deliver_count ?? 0,
      helpText:
        'Time in vcl_deliver — final response modification before sending to the client. Runs on every request (hits and misses).',
    },
    {
      sub: 'error',
      color: CHART_COLORS.danger,
      timeMs: data?.vcl_error_time_ms,
      timeFn: (d: MetricsData) => d.vcl_error_time_ms ?? 0,
      countFn: (d: MetricsData) => d.vcl_error_count ?? 0,
      helpText:
        'Time in vcl_error — synthetic error response generation. Only runs when VCL explicitly triggers an error or the origin is unreachable.',
    },
  ]

  return (
    <TabPanelShell rtDown={rtDown}>
      <MetricCardGrid
        cols={3}
        data={data}
        rollingAvg={rollingAvg}
        dimmed={rtDown}
        prefix={
          <Card>
            <CardHeader className="pb-0">
              <CardTitle className="text-xs font-medium tracking-wide text-muted-foreground uppercase">
                RT Stream
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className={`text-3xl font-bold tabular-nums ${!connected ? 'opacity-50' : ''}`}>
                {connected ? (rtDown ? 'Degraded' : 'Healthy') : 'Disconnected'}
              </div>
            </CardContent>
          </Card>
        }
        cards={[
          { title: 'Active PoPs', rateFn: (d) => d.pop_count ?? 0 },
          {
            title: 'Degraded PoPs',
            rateFn: (d) => d.degraded_pops?.length ?? 0,
            thresholds: { warn: 1, critical: 3, direction: 'above' },
          },
        ]}
      />
      <SectionHeading>Scorer Health (Compute@Edge)</SectionHeading>
      <MetricCardGrid
        cols={3}
        data={data}
        rollingAvg={rollingAvg}
        dimmed={rtDown}
        cards={[
          {
            title: 'Execution Time',
            decimals: 1,
            suffix: ' ms',
            thresholds: { warn: 50, critical: 200, direction: 'above' },
            rateFn: (d) => d.compute_exec_time_ms ?? 0,
            helpText:
              'Compute@Edge Wasm execution time (CPU-only, excludes I/O waits). The scorer is cold on every request — watch for execution time creep after code changes.',
          },
          {
            title: 'Request Time',
            decimals: 1,
            suffix: ' ms',
            thresholds: { warn: 100, critical: 500, direction: 'above' },
            rateFn: (d) => d.compute_req_time_ms ?? 0,
            helpText:
              'Total Compute@Edge request time including I/O (backend fetches, KV lookups). The gap between this and execution time is I/O wait.',
          },
          {
            title: 'RAM Used',
            value: Number(((data?.compute_ram_used ?? 0) / 1_048_576).toFixed(1)),
            suffix: ' MB',
            helpText:
              'Peak memory usage of the Compute@Edge scorer per request. The Wasm runtime allocates a fresh heap per request — if this climbs toward the limit, the scorer may start hitting Resource Exceeded errors.',
          },
          {
            title: 'Backend Errors',
            value: data?.compute_bereq_errors ?? 0,
            thresholds: { warn: 1, critical: 10, direction: 'above' },
            helpText:
              'Failed backend requests from the Compute scorer (e.g., origin unreachable). The scorer fails open on these — sessions are passed without scoring.',
          },
          {
            title: 'Guest Errors',
            value: data?.compute_guest_errors ?? 0,
            thresholds: { warn: 1, critical: 5, direction: 'above' },
            helpText:
              'Wasm runtime errors in the scorer (panics, traps, invalid memory access). Any non-zero value warrants investigation — it means the scorer code is hitting an unhandled edge case.',
          },
          {
            title: 'Resource Exceeded',
            value: data?.compute_resource_exceeded ?? 0,
            thresholds: { warn: 1, critical: 5, direction: 'above' },
            helpText:
              'Requests where the scorer exceeded its CPU or memory allocation. The scorer is cold on every request — if this grows, the scorer binary may need optimization or wasm-opt tuning.',
          },
        ]}
      />
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Compute Latency (ms)"
          yAxisSuffix=" ms"
          helpText="Execution = Wasm CPU time only. Request = total time including I/O waits. The gap between them is I/O wait (backend fetches, KV lookups). Watch for execution time creep after scorer code changes."
          traces={seriesTraces(series, [
            ['Execution', CHART_COLORS.info, (d) => d.compute_exec_time_ms ?? 0],
            ['Request', '#f97316', (d) => d.compute_req_time_ms ?? 0],
          ])}
        />
        <RealtimeChart
          title="Compute Errors"
          yAxisSuffix=" err/s"
          stacked
          helpText="Backend = origin request failures. Guest = Wasm runtime panics/traps. Resource Limit = CPU or memory exceeded. Any sustained non-zero errors mean the scorer is degraded and sessions are failing open."
          traces={seriesTraces(series, [
            ['Backend', CHART_COLORS.danger, (d) => d.compute_bereq_errors ?? 0],
            ['Guest', '#eab308', (d) => d.compute_guest_errors ?? 0],
            ['Resource Limit', '#8b5cf6', (d) => d.compute_resource_exceeded ?? 0],
          ])}
        />
      </div>
      <SectionHeading>VCL Execution</SectionHeading>
      <MetricCardGrid
        cols={5}
        data={data}
        rollingAvg={rollingAvg}
        dimmed={rtDown}
        cards={[
          {
            title: 'Restarts',
            suffix: '/s',
            rateFn: (d) => d.restarts ?? 0,
            thresholds: { warn: 1, critical: 10, direction: 'above' },
            helpText:
              'VCL restart count per second. Restarts re-enter vcl_recv with modified state — used for routing fallbacks but expensive. High rates indicate a misconfigured restart loop.',
          },
          ...vclSubs.map((v) => ({
            title: `vcl_${v.sub}`,
            value: Number((v.timeMs ?? 0).toFixed(1)),
            suffix: ' ms',
            helpText: v.helpText,
          })),
        ]}
      />
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="VCL Subroutine Time (ms)"
          yAxisSuffix=" ms"
          stacked
          helpText="Time spent in each VCL subroutine per request. recv = request handling, fetch = origin response processing, deliver = client response, error = error page generation. High recv time may indicate complex routing logic."
          traces={seriesTraces(
            series,
            vclSubs.map((v) => [v.sub, v.color, v.timeFn]),
          )}
        />
        <RealtimeChart
          title="VCL Subroutine Calls"
          yAxisSuffix=" calls/s"
          stacked
          helpText="Number of times each VCL subroutine was invoked. Call count × time per call = total VCL overhead. A high error count means many requests are hitting synthetic error responses."
          traces={seriesTraces(
            series,
            vclSubs.map((v) => [v.sub, v.color, v.countFn]),
          )}
        />
      </div>
      <div className="flex justify-end">
        <Link href={buildServiceHref('/admin/trends', activeServiceId)} prefetch={false}>
          <Button variant="outline" size="sm">
            View admin dashboard
            <ArrowRight className="ml-1.5 h-3.5 w-3.5" />
          </Button>
        </Link>
      </div>
    </TabPanelShell>
  )
}
