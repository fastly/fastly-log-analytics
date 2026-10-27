'use client'

import React from 'react'
import { ArrowRight, AlertTriangle } from 'lucide-react'
import Link from 'next/link'
import dynamic from 'next/dynamic'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { useServiceStore } from '@/stores/filterStore'
import { formatDate, buildServiceHref, CHART_COLORS } from '@/lib/utils'
import {
  type MetricsData,
  type MetricsTick,
  type ErrorEvent,
  type TabCommonProps,
  RealtimeMetricCard,
  RealtimeChart,
  CorrelatorLeaderboard,
  ConnectionBadge,
  FreshnessBadge,
  RtDownBanner,
  MetricCardGrid,
  TabPanelShell,
  SectionHeading,
  seriesTraces,
} from './ControlRoomWidgets'

const PopTrafficMap = dynamic(() => import('@/components/Map/NetworkMap').then((m) => m.PopTrafficMap), { ssr: false })

function ErrorFilterCell({
  filterKey,
  value,
  activeServiceId,
  className = 'px-3 py-2',
  bold = false,
}: {
  filterKey: string
  value: string | number
  activeServiceId: string | null
  className?: string
  bold?: boolean
}) {
  const str = String(value)
  return (
    <td className={className} title={filterKey === 'url' ? str : undefined}>
      {str ? (
        <Link
          className={`${bold ? 'font-semibold ' : ''}underline decoration-dotted underline-offset-2`}
          href={buildServiceHref(`/dashboard?filter_${filterKey}=${encodeURIComponent(str)}`, activeServiceId)}
          prefetch={false}
        >
          {value}
        </Link>
      ) : (
        '(unknown)'
      )}
    </td>
  )
}

function RecentErrorsCard({
  events,
  connected,
  timezone,
}: {
  events: ErrorEvent[]
  connected: boolean
  timezone: string
}) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  return (
    <Card className="contain-intrinsic-size-[320px]">
      <CardHeader className="flex flex-row items-center justify-between gap-3">
        <CardTitle>Live errors</CardTitle>
        <Badge variant={connected ? 'success' : 'destructive'}>{connected ? 'Listening' : 'Disconnected'}</Badge>
      </CardHeader>
      <CardContent>
        {events.length === 0 ? (
          <p className="py-8 text-center text-sm text-muted-foreground">
            {connected ? 'No recent 4xx/5xx errors.' : 'Listening for errors...'}
          </p>
        ) : (
          <div className="overflow-x-auto rounded-lg border">
            <table className="w-full text-sm">
              <caption className="sr-only">Recent HTTP errors</caption>
              <thead className="bg-muted/50 text-left">
                <tr>
                  {['Time', 'Status', 'Path', 'PoP', 'Client IP'].map((h) => (
                    <th key={h} className="px-3 py-2 font-medium">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {events.map((event) => (
                  <tr key={event.event_id} className="border-t">
                    <td className="whitespace-nowrap px-3 py-2">{formatDate(event.timestamp, timezone, 'h:mm:ss a')}</td>
                    <ErrorFilterCell filterKey="status" value={event.status} activeServiceId={activeServiceId} bold />
                    <ErrorFilterCell
                      filterKey="url"
                      value={event.url}
                      activeServiceId={activeServiceId}
                      className="max-w-sm truncate px-3 py-2"
                    />
                    <ErrorFilterCell filterKey="pop" value={event.pop} activeServiceId={activeServiceId} />
                    <ErrorFilterCell
                      filterKey="client_ip"
                      value={event.client_ip}
                      activeServiceId={activeServiceId}
                      className="px-3 py-2 font-mono"
                    />
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  )
}

export function OverviewTab({
  tick,
  rtDown,
  historicalHref,
  series,
  rollingAvg,
  errorEvents,
  errorsConnected,
  timezone,
}: TabCommonProps & {
  errorEvents: ErrorEvent[]
  errorsConnected: boolean
  timezone: string
}) {
  const data = tick?.data
  return (
    <TabPanelShell rtDown={rtDown} historicalHref={historicalHref} historicalLabel="overview">
      <MetricCardGrid
        data={data}
        rollingAvg={rollingAvg}
        rtDown={rtDown}
        cards={[
          {
            title: 'Requests/s',
            suffix: ' req/s',
            rateFn: (d) => d.requests_per_second,
            helpText:
              'Total HTTP requests per second across all Fastly edge PoPs. Rolling averages smooth out per-second spikes — the 60s average is most representative of sustained load.',
          },
          {
            title: 'Error Rate',
            decimals: 2,
            suffix: '%',
            rateFn: (d) => d.error_rate * 100,
            thresholds: { warn: 1, critical: 5, direction: 'above' },
            helpText:
              'Percentage of responses with 4xx or 5xx status codes. Card turns amber above 1% and red above 5%.',
          },
          {
            title: 'Cache Hit Ratio',
            decimals: 1,
            suffix: '%',
            rateFn: (d) => d.cache_hit_ratio * 100,
            thresholds: { warn: 90, critical: 50, direction: 'below' },
            helpText:
              "Percentage of requests served from Fastly's edge cache without reaching your origin. Higher is better — below 90% warns, below 50% is critical. Drops may indicate cache-busting query strings, low TTLs, or new uncacheable content.",
          },
          {
            title: 'Bandwidth',
            decimals: 2,
            suffix: ' Mbps',
            rateFn: (d) => d.bandwidth_mbps,
            helpText:
              'Total bandwidth delivered across all PoPs in megabits per second, including both cached and origin-fetched content.',
          },
        ]}
      />
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Total Requests"
          yAxisSuffix=" req/s"
          helpText="Per-second request count across all PoPs. Sudden spikes may indicate traffic surges, bot activity, or DDoS events."
          traces={seriesTraces(series, [
            ['Requests', CHART_COLORS.info, (d) => d.total_requests ?? d.requests_per_second],
          ])}
        />
        <RealtimeChart
          title="Status Code Breakdown"
          yAxisSuffix=" req/s"
          stacked
          helpText="Per-second breakdown by HTTP status class. 2xx = success, 3xx = redirects, 4xx = client errors (broken links, auth failures), 5xx = origin/server errors."
          traces={seriesTraces(series, [
            ['1xx', CHART_COLORS.neutral, (d) => d.status_breakdown?.status_1xx ?? 0],
            ['2xx', '#22c55e', (d) => d.status_breakdown?.status_2xx ?? 0],
            ['3xx', CHART_COLORS.info, (d) => d.status_breakdown?.status_3xx ?? 0],
            ['4xx', '#eab308', (d) => d.status_breakdown?.status_4xx ?? 0],
            ['5xx', CHART_COLORS.danger, (d) => d.status_breakdown?.status_5xx ?? 0],
          ])}
        />
      </div>
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Cache Performance"
          yAxisSuffix=" req/s"
          stacked
          helpText="Hit = served from edge cache, Miss = fetched from origin and cached, Pass = bypassed cache entirely (uncacheable or VCL pass rules). A growing Pass ratio may indicate misconfigured cache rules."
          traces={seriesTraces(series, [
            ['Hit', '#22c55e', (d) => d.total_hits ?? 0],
            ['Miss', '#f97316', (d) => d.total_miss ?? 0],
            ['Pass', CHART_COLORS.neutral, (d) => d.total_pass ?? 0],
          ])}
        />
        <RealtimeChart
          title="Origin Load"
          dualYAxis
          yAxisSuffix=" req/s"
          y2AxisSuffix="%"
          helpText="Left axis: requests reaching your origin per second. Right axis: cache hit ratio — when it drops, more requests flow through to origin."
          traces={seriesTraces(series, [
            ['Origin req/s', CHART_COLORS.info, (d) => d.origin_requests_per_second ?? 0],
            ['Cache Hit %', '#22c55e', (d) => (d.cache_hit_ratio ?? 0) * 100, 'y2'],
          ])}
        />
      </div>
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="2xx/3xx Status Codes"
          yAxisSuffix=" req/s"
          stacked
          traces={seriesTraces(
            series,
            (
              [
                ['200', '#22c55e'],
                ['204', '#86efac'],
                ['301', CHART_COLORS.info],
                ['302', '#93c5fd'],
                ['304', '#6366f1'],
              ] as const
            ).map(([c, col]) => [c, col, (d: MetricsData) => d.status_detail?.[c] ?? 0]),
          )}
        />
        <RealtimeChart
          title="4xx/5xx Status Codes"
          yAxisSuffix=" req/s"
          stacked
          traces={seriesTraces(
            series,
            (
              [
                ['400', '#eab308'],
                ['401', CHART_COLORS.warning],
                ['403', '#f97316'],
                ['404', '#fb923c'],
                ['429', CHART_COLORS.danger],
                ['500', '#dc2626'],
                ['502', '#b91c1c'],
                ['503', '#991b1b'],
                ['504', '#7f1d1d'],
              ] as const
            ).map(([c, col]) => [c, col, (d: MetricsData) => d.status_detail?.[c] ?? 0]),
          )}
        />
      </div>
      <RecentErrorsCard events={errorEvents} connected={errorsConnected} timezone={timezone} />
    </TabPanelShell>
  )
}

export function PerformanceTab({ tick, rtDown, historicalHref, series, rollingAvg }: TabCommonProps) {
  const data = tick?.data
  return (
    <TabPanelShell rtDown={rtDown} historicalHref={historicalHref} historicalLabel="performance">
      <MetricCardGrid
        data={data}
        rollingAvg={rollingAvg}
        rtDown={rtDown}
        cards={[
          {
            title: 'Cache Hit Ratio',
            decimals: 1,
            suffix: '%',
            rateFn: (d) => d.cache_hit_ratio * 100,
            thresholds: { warn: 90, critical: 50, direction: 'below' },
            helpText:
              'Percentage of requests served from edge cache. Below 90% warns, below 50% is critical. Drops may indicate cache-busting query strings, low TTLs, or new uncacheable content.',
          },
          {
            title: 'Origin Requests/s',
            suffix: ' req/s',
            rateFn: (d) => d.origin_requests_per_second ?? 0,
            helpText:
              "Requests that reached your origin server per second. Lower is better — it means more traffic is being served from Fastly's edge cache.",
          },
          {
            title: 'Bandwidth Offload',
            decimals: 1,
            suffix: '%',
            thresholds: { warn: 80, critical: 50, direction: 'below' },
            rateFn: (d) => (d.origin_offload ?? 0) * 100,
            helpText:
              'Percentage of total bandwidth served from cache vs. origin. Higher means less origin load and lower egress costs.',
          },
          {
            title: 'Pass-through',
            suffix: ' req',
            rateFn: (d) => d.pass_requests ?? 0,
            helpText:
              'Requests that bypassed cache entirely — either uncacheable content types or explicit VCL pass rules. Every pass request hits your origin.',
          },
        ]}
      />
      <MetricCardGrid
        data={data}
        rollingAvg={rollingAvg}
        rtDown={rtDown}
        cards={[
          {
            title: 'Collapse Usable',
            suffix: ' req',
            rateFn: (d) => d.request_collapse_usable ?? 0,
            helpText:
              'Requests eligible for request collapsing — multiple identical requests are collapsed into a single origin fetch. Higher counts mean more origin load reduction.',
          },
          {
            title: 'Collapse Unusable',
            suffix: ' req',
            rateFn: (d) => d.request_collapse_unusable ?? 0,
            helpText:
              'Requests that could not be collapsed (e.g., different Vary headers, POST requests). High unusable counts may indicate opportunities to improve cache key design.',
          },
          {
            title: 'Segment Origin Fetches',
            suffix: ' req',
            rateFn: (d) => d.segblock_origin_fetches ?? 0,
            helpText:
              'Segment block fetches to origin for large objects. Fastly breaks large files into segments cached independently, enabling range requests and parallel fetching.',
          },
          {
            title: 'Segment Shield Fetches',
            suffix: ' req',
            rateFn: (d) => d.segblock_shield_fetches ?? 0,
            helpText:
              'Segment block fetches handled at the shield PoP. When these are high relative to origin fetches, segmented caching is working well.',
          },
        ]}
      />
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Cache Hit Ratio"
          yAxisSuffix="%"
          traces={seriesTraces(series, [['Hit Ratio %', '#22c55e', (d) => d.cache_hit_ratio * 100]])}
        />
        <RealtimeChart
          title="Origin Latency Breakdown"
          yAxisSuffix=" req/s"
          traces={seriesTraces(series, [
            ['Origin req/s', CHART_COLORS.info, (d) => d.origin_requests_per_second ?? 0],
            ['Pass', '#f97316', (d) => d.pass_requests ?? 0],
          ])}
        />
      </div>
      <RealtimeChart
        title="Cache Path Latency (ms)"
        yAxisSuffix=" ms"
        helpText="Time-to-first-byte by cache path. Hit latency is edge-only (fast). Miss includes origin round-trip plus caching. Pass includes origin round-trip with no caching overhead."
        traces={seriesTraces(series, [
          ['Hit', '#22c55e', (d) => d.hit_latency_ms ?? 0],
          ['Miss', '#f97316', (d) => d.miss_latency_ms ?? 0],
          ['Pass', CHART_COLORS.neutral, (d) => d.pass_latency_ms ?? 0],
        ])}
      />
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="Object Size Distribution"
          yAxisSuffix=" req/s"
          stacked
          helpText="Distribution of response body sizes. Large objects may benefit from segmented caching; many small objects benefit from request collapsing."
          traces={seriesTraces(
            series,
            (
              [
                ['<1KB', CHART_COLORS.neutral, '1k'],
                ['1-10KB', CHART_COLORS.info, '10k'],
                ['10-100KB', '#22c55e', '100k'],
                ['100KB-1MB', '#eab308', '1m'],
                ['1-10MB', '#f97316', '10m'],
                ['10-100MB', CHART_COLORS.danger, '100m'],
                ['100MB-1GB', '#8b5cf6', '1g'],
                ['Other', '#6b7280', 'other'],
              ] as const
            ).map(([n, c, k]) => [n, c, (d: MetricsData) => d.object_size_distribution?.[k] ?? 0]),
          )}
        />
        <RealtimeChart
          title="Origin Response Time (ms)"
          yAxisSuffix=" req/s"
          stacked
          helpText="Histogram of origin response times for cache misses. A shift toward higher buckets (>250ms) signals origin degradation or upstream latency."
          traces={seriesTraces(series, [
            ...(
              [
                ['<10ms', '#22c55e', '10'],
                ['10-20ms', '#86efac', '20'],
                ['20-30ms', CHART_COLORS.info, '30'],
                ['30-60ms', '#93c5fd', '60'],
                ['60-120ms', '#eab308', '120'],
                ['120-250ms', '#f97316', '250'],
                ['250-500ms', CHART_COLORS.danger, '500'],
                ['500ms-1s', '#dc2626', '1000'],
              ] as const
            ).map(([n, c, k]) => [n, c, (d: MetricsData) => d.miss_histogram?.[k] ?? 0] as const),
            [
              '>1s',
              '#7f1d1d',
              (d: MetricsData) =>
                Object.entries(d.miss_histogram ?? {}).reduce(
                  (sum, [k, v]) => (['10', '20', '30', '60', '120', '250', '500', '1000'].includes(k) ? sum : sum + v),
                  0,
                ),
            ],
          ])}
        />
      </div>
    </TabPanelShell>
  )
}

export function OriginTab({ tick, rtDown, historicalHref, series, rollingAvg }: TabCommonProps) {
  const data = tick?.data
  return (
    <TabPanelShell rtDown={rtDown} historicalHref={historicalHref} historicalLabel="origin">
      <MetricCardGrid
        data={data}
        rollingAvg={rollingAvg}
        dimmed={rtDown}
        cards={[
          {
            title: 'Origin Requests/s',
            suffix: ' req/s',
            rateFn: (d) => d.origin_requests_per_second ?? 0,
            helpText:
              'Requests per second reaching your origin server. Lower is better — cache and shield should absorb most traffic before it gets here.',
          },
          {
            title: 'Origin Bandwidth',
            decimals: 2,
            suffix: ' Mbps',
            rateFn: (d) => d.origin_bandwidth_mbps ?? 0,
            helpText:
              'Bandwidth transferred from your origin server to Fastly. This is your origin egress — lower means better cache offload.',
          },
          {
            title: 'Shield Requests',
            suffix: ' req',
            rateFn: (d) => d.shield_requests ?? 0,
            helpText:
              'Requests routed through the shield PoP — a mid-tier cache between edge PoPs and your origin. The shield absorbs duplicate misses from multiple edge PoPs.',
          },
          {
            title: 'Shield Hit Ratio',
            decimals: 1,
            suffix: '%',
            rateFn: (d) => (d.shield_hit_ratio ?? 0) * 100,
            helpText:
              'Percentage of shield requests served from the shield cache. Higher means fewer requests reach your origin.',
          },
          {
            title: 'Origin Fetches',
            suffix: ' req',
            rateFn: (d) => d.origin_fetches ?? 0,
            helpText:
              'Total fetch requests sent from Fastly to your origin. Includes full fetches, revalidations, and cache-eligible fetches. This is the broadest measure of origin load.',
          },
          {
            title: 'Revalidations',
            suffix: ' req',
            rateFn: (d) => d.origin_revalidations ?? 0,
            helpText:
              'Conditional requests (If-Modified-Since/If-None-Match) sent to your origin. When the origin returns 304 Not Modified, Fastly reuses the cached copy — cheaper than a full fetch.',
          },
          {
            title: 'Cache Fetches',
            suffix: ' req',
            rateFn: (d) => d.origin_cache_fetches ?? 0,
            helpText:
              'Origin fetches that returned a cacheable response (the response will be stored and served to future requests). A low ratio of cache fetches to total fetches may indicate your origin is sending too many no-cache/private headers.',
          },
        ]}
      />
      <RealtimeChart
        title="Origin Load"
        dualYAxis
        yAxisSuffix=" req/s"
        y2AxisSuffix="%"
        traces={seriesTraces(series, [
          ['Origin req/s', CHART_COLORS.info, (d) => d.origin_requests_per_second ?? 0],
          ['Shield Hit %', '#22c55e', (d) => (d.shield_hit_ratio ?? 0) * 100, 'y2'],
        ])}
      />
      <RealtimeChart
        title="Origin Fetch Breakdown"
        yAxisSuffix=" req/s"
        stacked
        helpText="Fetches = full origin requests. Revalidations = conditional requests (If-Modified-Since/ETag) that are cheaper than full fetches. Cache Fetches = requests to origin that resulted in a cacheable response."
        traces={seriesTraces(series, [
          ['Fetches', CHART_COLORS.info, (d) => d.origin_fetches ?? 0],
          ['Revalidations', '#22c55e', (d) => d.origin_revalidations ?? 0],
          ['Cache Fetches', '#eab308', (d) => d.origin_cache_fetches ?? 0],
        ])}
      />
      <SectionHeading>Shield Efficiency</SectionHeading>
      <MetricCardGrid
        data={data}
        rollingAvg={rollingAvg}
        dimmed={rtDown}
        cards={[
          {
            title: 'Shield Hits',
            rateFn: (d) => d.shield_hit_requests ?? 0,
            helpText:
              "Requests served from the shield PoP's cache, preventing duplicate origin fetches from multiple edge PoPs.",
          },
          {
            title: 'Shield Misses',
            rateFn: (d) => d.shield_miss_requests ?? 0,
            helpText:
              'Requests that missed the shield cache and were forwarded to your origin. Every shield miss becomes an origin fetch.',
          },
          {
            title: 'Shield Revalidations',
            rateFn: (d) => d.shield_revalidations ?? 0,
            helpText:
              'Conditional requests at the shield tier — the shield checks with origin whether stale content is still valid. A 304 response avoids transferring the full object.',
          },
          {
            title: 'Shield Fetch Bytes',
            decimals: 2,
            suffix: ' MB',
            rateFn: (d) => (d.shield_fetch_body_bytes ?? 0) / 1_000_000,
            helpText:
              'Total bytes fetched through the shield tier from origin. This represents the bandwidth your origin actually serves — compare against total edge bandwidth to measure offload efficiency.',
          },
        ]}
      />
      <RealtimeChart
        title="Shield Hit/Miss/Revalidation"
        yAxisSuffix=" req/s"
        stacked
        helpText="Shield-tier cache breakdown. Hits = served from shield cache. Misses = forwarded to origin. Revalidations = conditional requests (304 responses) that avoid full origin fetches."
        traces={seriesTraces(series, [
          ['Hits', '#22c55e', (d) => d.shield_hit_requests ?? 0],
          ['Misses', '#f97316', (d) => d.shield_miss_requests ?? 0],
          ['Revalidations', CHART_COLORS.info, (d) => d.shield_revalidations ?? 0],
        ])}
      />
    </TabPanelShell>
  )
}

export function SecurityTab({ tick, rtDown, historicalHref, series, rollingAvg }: TabCommonProps) {
  const data = tick?.data
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const issued = data?.bot_challenges_issued ?? 0
  const succeeded = data?.bot_challenges_succeeded ?? 0
  const failed = data?.bot_challenges_failed ?? 0

  return (
    <TabPanelShell rtDown={rtDown} historicalHref={historicalHref} historicalLabel="security">
      <MetricCardGrid
        data={data}
        rollingAvg={rollingAvg}
        dimmed={rtDown}
        cards={[
          {
            title: 'WAF Blocked',
            suffix: ' req',
            rateFn: (d) => d.waf_blocked ?? 0,
            helpText:
              'Requests blocked by the Web Application Firewall. These matched a WAF rule configured to block — the request never reached your origin.',
          },
          {
            title: 'WAF Logged',
            suffix: ' req',
            rateFn: (d) => d.waf_logged ?? 0,
            helpText:
              'Requests that triggered a WAF rule in logging mode — suspicious but allowed through. Use logged events to tune rules before switching them to block mode.',
          },
          {
            title: '4xx Responses',
            suffix: ' req',
            rateFn: (d) => d.status_breakdown?.status_4xx ?? 0,
            helpText:
              'Client error responses (400 Bad Request, 401 Unauthorized, 403 Forbidden, 404 Not Found, 429 Rate Limited). Spikes may indicate scanning activity, broken links, or auth issues.',
          },
          {
            title: '5xx Responses',
            suffix: ' req',
            rateFn: (d) => d.status_breakdown?.status_5xx ?? 0,
            thresholds: { warn: 1, critical: 10, direction: 'above' },
            helpText:
              'Server error responses. 502/503 typically indicate origin unreachable or overloaded. 500 indicates an application error. Any sustained 5xx traffic warrants immediate investigation.',
          },
        ]}
      />
      <div className="grid gap-6 md:grid-cols-2">
        <RealtimeChart
          title="WAF Events"
          yAxisSuffix=" req/s"
          stacked
          helpText="Blocked = attack prevented at the edge. Logged = suspicious traffic allowed through for monitoring. Passed = clean traffic that matched no WAF rules."
          traces={seriesTraces(series, [
            ['Blocked', CHART_COLORS.danger, (d) => d.waf_blocked ?? 0],
            ['Logged', '#eab308', (d) => d.waf_logged ?? 0],
            ['Passed', '#22c55e', (d) => d.waf_passed ?? 0],
          ])}
        />
        <RealtimeChart
          title="Error Codes"
          yAxisSuffix=" req/s"
          helpText="4xx errors are client-side (bad requests, auth failures, not found). 5xx errors are server-side (origin errors, timeouts). Correlated spikes suggest an incident."
          traces={seriesTraces(series, [
            ['4xx', '#eab308', (d) => d.status_breakdown?.status_4xx ?? 0],
            ['5xx', CHART_COLORS.danger, (d) => d.status_breakdown?.status_5xx ?? 0],
          ])}
        />
      </div>
      <SectionHeading>Bot Challenge Funnel</SectionHeading>
      <MetricCardGrid
        dimmed={rtDown}
        cards={[
          {
            title: 'Challenges Issued',
            value: issued,
            suffix: ' req',
            helpText:
              'Number of bot challenges (CAPTCHAs, JS challenges) issued to suspicious clients. High counts indicate active bot pressure.',
          },
          {
            title: 'Challenges Passed',
            value: succeeded,
            suffix: ' req',
            helpText: 'Clients that successfully completed the challenge — likely legitimate users or sophisticated bots.',
          },
          {
            title: 'Challenges Failed',
            value: failed,
            suffix: ' req',
            helpText:
              'Clients that failed the challenge — almost certainly automated bots unable to solve CAPTCHAs or execute JavaScript.',
          },
          {
            title: 'Pass Rate',
            value: Number((issued > 0 ? (succeeded / issued) * 100 : 0).toFixed(1)),
            suffix: '%',
            helpText:
              'Percentage of issued challenges that were passed. Very low rates suggest the challenges are effectively blocking bots. Very high rates may mean challenges are too easy or mostly hitting real users.',
          },
        ]}
      />
      <MetricCardGrid
        cols={3}
        dimmed={rtDown}
        cards={[
          {
            title: 'Bots Detected',
            value: data?.bot_detected ?? 0,
            suffix: ' req',
            helpText:
              "Requests classified as bot traffic by Fastly's bot detection engine, based on behavioral fingerprinting and reputation data.",
          },
          {
            title: 'Verified Bots',
            value: data?.bot_verified ?? 0,
            suffix: ' req',
            helpText:
              'Verified legitimate bots like Googlebot, Bingbot, and other search engine crawlers that have been authenticated via reverse DNS or IP verification.',
          },
          {
            title: 'AI Crawlers',
            value: data?.bot_ai_crawlers ?? 0,
            suffix: ' req',
            helpText:
              'Requests from identified AI training crawlers (GPTBot, ClaudeBot, etc.). These can consume significant bandwidth — consider blocking via robots.txt or WAF rules.',
          },
        ]}
      />
      <RealtimeChart
        title="Bot Challenge Pipeline"
        yAxisSuffix=" req/s"
        stacked
        helpText="Bot management funnel. Passed = completed challenge (likely human). Failed = failed challenge (likely bot). Detected = classified as bot by fingerprinting. AI Crawlers = identified AI training crawlers."
        traces={seriesTraces(series, [
          ['Passed', '#22c55e', (d) => d.bot_challenges_succeeded ?? 0],
          ['Failed', CHART_COLORS.danger, (d) => d.bot_challenges_failed ?? 0],
          ['Detected', '#eab308', (d) => d.bot_detected ?? 0],
          ['AI Crawlers', '#8b5cf6', (d) => d.bot_ai_crawlers ?? 0],
        ])}
      />
      <SectionHeading>DDoS Protection</SectionHeading>
      <MetricCardGrid
        cols={2}
        dimmed={rtDown}
        cards={[
          {
            title: 'DDoS Detected',
            value: data?.ddos_detect ?? 0,
            suffix: ' req',
            helpText:
              "Requests classified as distributed denial-of-service by Fastly's automatic DDoS protection. Detection is always on — no configuration required.",
          },
          {
            title: 'DDoS Mitigated',
            value: data?.ddos_mitigate ?? 0,
            suffix: ' req',
            helpText:
              'DDoS requests that were actively mitigated (blackholed, tarpitted, closed, or downgraded). The gap between detected and mitigated indicates requests still under analysis.',
          },
        ]}
      />
      <RealtimeChart
        title="DDoS Mitigation Actions"
        yAxisSuffix=" req/s"
        stacked
        helpText="Blackhole = all traffic dropped. Tarpit = connections intentionally slowed. Close = connections terminated. Downgrade = service quality reduced. Any non-zero values indicate active DDoS mitigation."
        traces={seriesTraces(series, [
          ['Blackhole', '#1e293b', (d) => d.ddos_action_blackhole ?? 0],
          ['Tarpit', '#7c3aed', (d) => d.ddos_action_tarpit ?? 0],
          ['Close', CHART_COLORS.danger, (d) => d.ddos_action_close ?? 0],
          ['Downgrade', '#eab308', (d) => d.ddos_action_downgrade ?? 0],
        ])}
      />
      <SectionHeading>WAF Signal Classification</SectionHeading>
      <Card>
        <CardContent className="pt-4">
          <CorrelatorLeaderboard dimension="waf_sig" serviceId={activeServiceId} limit={5} />
        </CardContent>
      </Card>
    </TabPanelShell>
  )
}

export function NetworkTab({ tick, rtDown, historicalHref, series, rollingAvg }: TabCommonProps) {
  const data = tick?.data
  const degradedCount = data?.degraded_pops?.length ?? 0
  return (
    <TabPanelShell rtDown={rtDown} historicalHref={historicalHref} historicalLabel="network">
      <MetricCardGrid
        cols={3}
        data={data}
        rollingAvg={rollingAvg}
        dimmed={rtDown}
        cards={[
          {
            title: 'Active PoPs',
            rateFn: (d) => d.pop_count ?? 0,
            helpText:
              'Number of Fastly Points of Presence actively serving traffic for this service. Each PoP is a data center location worldwide.',
          },
          {
            title: 'Healthy PoPs',
            rateFn: (d) => (d.pop_count ?? 0) - (d.degraded_pops?.length ?? 0),
            helpText: 'PoPs with error rates below the degradation threshold. A sudden drop may indicate a regional issue.',
          },
          {
            title: 'Degraded PoPs',
            thresholds: { warn: 1, critical: 3, direction: 'above' },
            rateFn: (d) => d.degraded_pops?.length ?? 0,
            helpText: 'PoPs with elevated error rates. Low-traffic PoPs may appear degraded during traffic lulls.',
          },
        ]}
      />
      {degradedCount > 0 && !rtDown && (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription>
            {degradedCount} PoP(s) showing elevated errors: {data?.degraded_pops?.join(', ')}
          </AlertDescription>
        </Alert>
      )}
      <RealtimeChart
        title="PoP Count"
        yAxisSuffix=" PoPs"
        traces={seriesTraces(series, [
          ['Active PoPs', CHART_COLORS.info, (d) => d.pop_count ?? 0],
          ['Degraded', CHART_COLORS.danger, (d) => d.degraded_pops?.length ?? 0],
        ])}
      />
      <SectionHeading>Traffic Distribution</SectionHeading>
      <Card>
        <CardContent className="pt-4">
          <PopTrafficMap allPops={data?.all_pops ?? {}} className="h-[420px]" />
        </CardContent>
      </Card>
      <SectionHeading>Protocol Adoption</SectionHeading>
      <MetricCardGrid
        cols={5}
        dimmed={rtDown}
        cards={(
          [
            ['HTTP/2', data?.h2_pct],
            ['HTTP/3', data?.h3_pct],
            ['IPv6', data?.ipv6_pct],
            ['TLS 1.2', data?.tls12_pct],
            ['TLS 1.3', data?.tls13_pct],
          ] as const
        ).map(([title, pct]) => ({ title, value: Number((pct ?? 0).toFixed(1)), suffix: '%' }))}
      />
      <RealtimeChart
        title="Protocol Distribution"
        yAxisSuffix=" req/s"
        stacked
        helpText="HTTP/2 uses multiplexed TCP streams. HTTP/3 uses QUIC (UDP-based) for better performance on lossy networks. 'Other' includes HTTP/1.1 and legacy protocols."
        traces={seriesTraces(series, [
          ['HTTP/2', CHART_COLORS.info, (d) => d.http2 ?? 0],
          ['HTTP/3', '#22c55e', (d) => d.http3 ?? 0],
          ['Other', CHART_COLORS.neutral, (d) => Math.max(0, (d.total_requests ?? 0) - (d.http2 ?? 0) - (d.http3 ?? 0))],
        ])}
      />
    </TabPanelShell>
  )
}

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

export function MobileOverview({
  connected,
  lastTickTime,
  timezone,
  tick,
  rtDown,
}: {
  connected: boolean
  lastTickTime: Date | null
  timezone: string
  tick: MetricsTick | null
  rtDown: boolean
}) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const data = tick?.data
  return (
    <div className="space-y-4 md:hidden">
      <div className="flex items-center gap-2 flex-wrap">
        <ConnectionBadge connected={connected} />
        <FreshnessBadge lastTickTime={lastTickTime} timezone={timezone} />
      </div>
      {rtDown && <RtDownBanner />}
      <Card>
        <CardHeader>
          <CardTitle>Live Overview</CardTitle>
        </CardHeader>
        <CardContent>
          {data ? (
            <div className={`grid grid-cols-2 gap-3 ${rtDown ? 'opacity-50' : ''}`}>
              <RealtimeMetricCard title="Requests/s" value={data.requests_per_second} suffix=" req/s" />
              <RealtimeMetricCard
                title="Error Rate"
                value={Number((data.error_rate * 100).toFixed(2))}
                suffix="%"
                thresholds={{ warn: 1, critical: 5, direction: 'above' }}
              />
              <RealtimeMetricCard
                title="Cache Hit"
                value={Number((data.cache_hit_ratio * 100).toFixed(1))}
                suffix="%"
                thresholds={{ warn: 90, critical: 50, direction: 'below' }}
              />
              <RealtimeMetricCard title="Bandwidth" value={Number(data.bandwidth_mbps.toFixed(2))} suffix=" Mbps" />
            </div>
          ) : (
            <p className="text-muted-foreground text-sm">Waiting for metrics stream...</p>
          )}
        </CardContent>
      </Card>
      <Link href={buildServiceHref('/dashboard', activeServiceId)} prefetch={false}>
        <Button variant="outline" size="sm" className="w-full">
          View full dashboard
          <ArrowRight className="ml-1.5 h-3.5 w-3.5" />
        </Button>
      </Link>
    </div>
  )
}
