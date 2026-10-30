'use client'

import React from 'react'
import { AlertTriangle } from 'lucide-react'
import dynamic from 'next/dynamic'
import { Card, CardContent } from '@/components/ui/card'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { useServiceStore } from '@/stores/filterStore'
import { CHART_COLORS } from '@/lib/utils'
import {
  type TabCommonProps,
  RealtimeChart,
  CorrelatorLeaderboard,
  MetricCardGrid,
  TabPanelShell,
  SectionHeading,
  seriesTraces,
} from './ControlRoomWidgets'

const PopTrafficMap = dynamic(() => import('@/components/Map/NetworkMap').then((m) => m.PopTrafficMap), { ssr: false })

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
