'use client'

import React from 'react'
import { CHART_COLORS } from '@/lib/utils'
import {
  type MetricsData,
  type TabCommonProps,
  RealtimeChart,
  MetricCardGrid,
  TabPanelShell,
  SectionHeading,
  seriesTraces,
} from './ControlRoomWidgets'

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
