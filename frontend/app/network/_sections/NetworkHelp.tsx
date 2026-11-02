'use client'

import React from 'react'
import {
  Network as NetworkIcon,
  AlertCircle,
  Globe,
  Zap,
  Activity,
} from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'

function HelpSection({
  intro,
  items,
}: {
  intro: React.ReactNode
  items: Array<{ icon: React.ComponentType<{ className?: string }>; color: string; label: string; body: React.ReactNode }>
}) {
  return (
    <div className="space-y-4 text-sm text-muted-foreground">
      <p>{intro}</p>
      <ul className="space-y-3 list-none pl-0">
        {items.map(({ icon: Icon, color, label, body }) => (
          <li key={label} className="flex gap-3">
            <Icon className={cn('h-5 w-5 shrink-0 mt-0.5', color)} />
            <span><strong>{label}:</strong> {body}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}

export const GlobalHealthHelp = () => (
  <HelpSection
    intro={<>A composite <strong>0–100 score</strong> aggregated across your top ASNs, weighted by request volume. 100 = perfect conditions, 0 = severe degradation.</>}
    items={[
      { icon: Activity, color: 'text-blue-500', label: "How it's calculated", body: <>Combines TCP RTT, packet loss (<code>ploss</code>), and RTT variance (jitter) into a single normalized score for each ASN, then takes a traffic-weighted average.</> },
      { icon: AlertCircle, color: 'text-yellow-500', label: 'When to act', body: 'A score below 80 warrants investigation. A sustained drop often precedes visible user complaints or error-rate spikes by several minutes — making this a leading indicator.' },
    ]}
  />
)

export const AvgRttHelp = () => (
  <HelpSection
    intro={<>Mean <strong>TCP Round Trip Time</strong> in milliseconds across all logged requests in the selected time window.</>}
    items={[
      { icon: Zap, color: 'text-blue-500', label: 'Healthy baselines', body: '10–50ms for regional users, 100–200ms for international. Values above 300ms typically result in noticeable UX degradation.' },
      { icon: AlertCircle, color: 'text-yellow-500', label: 'Sudden increases', body: 'A 2× or greater spike often indicates ISP congestion, a Fastly routing change, or a DDoS volumetric attack saturating a link. Cross-reference with the ASN Leaderboard to isolate the source.' },
    ]}
  />
)

export const WorstAsnHelp = () => (
  <HelpSection
    intro={<>The <strong>Internet Service Provider or network</strong> (identified by Autonomous System Number) with the lowest composite health score during the selected window.</>}
    items={[
      { icon: NetworkIcon, color: 'text-blue-500', label: 'What causes this', body: "High RTT + packet loss from a specific ASN typically indicates a peering issue between that ISP and Fastly's edge nodes, or congestion on a transit link." },
      { icon: Activity, color: 'text-yellow-500', label: 'How to act', body: "Check the ASN Leaderboard to see how many requests this affects and whether the trend is degrading. If it's a large ISP with a worsening trend, Fastly support can investigate peering." },
    ]}
  />
)

export const WorstRegionHelp = () => (
  <HelpSection
    intro={<>The <strong>country experiencing the lowest composite network health score</strong>, based on TCP metrics from users in that geography.</>}
    items={[
      { icon: Globe, color: 'text-blue-500', label: 'ISP vs. regional', body: "If the same country appears in both this card and the Worst ASN, the issue is likely a single dominant ISP. If the ASN is healthy but the country isn't, a regional routing or infrastructure problem is more likely." },
      { icon: Activity, color: 'text-yellow-500', label: 'Follow-up', body: 'Use the Metro Leaderboard to narrow down to a specific city. A single underperforming city within an otherwise healthy country usually points to a specific datacenter or peering point.' },
    ]}
  />
)

export const HeatmapHelp = () => (
  <HelpSection
    intro={<>A grid where each <strong>row is an ISP (ASN)</strong>, each <strong>column is a time bucket</strong>, and the color represents the composite health score — red (0) to green (100).</>}
    items={[
      { icon: Activity, color: 'text-blue-500', label: 'Reading it', body: 'A row that turns red at a specific column pinpoints exactly when degradation started for a given ISP. Persistent red rows indicate long-standing peering or routing problems that predate your current time window.' },
      { icon: AlertCircle, color: 'text-yellow-500', label: 'Shared incidents', body: 'Multiple rows going red simultaneously suggests a widespread event — a major transit outage, a volumetric attack, or a Fastly-side issue rather than an ISP-specific problem.' },
    ]}
  />
)

export const AsnLeaderboardHelp = () => (
  <HelpSection
    intro={<>Rankings of every observed <strong>Internet Service Provider</strong> by composite health score, with tail-latency breakdowns and change signals.</>}
    items={[
      { icon: Zap, color: 'text-blue-500', label: 'P95 / P99 RTT', body: 'Tail latency — the slowest 5% and 1% of TCP handshakes for users on this network. High P99 with a healthy P95 means most users are fine but a small percentage are experiencing severe problems.' },
      { icon: Activity, color: 'text-yellow-500', label: '1h Change', body: 'Real-time signal — a negative value in red means the score is actively declining, often indicating an in-progress incident. A negative value in a currently-healthy ASN is worth watching.' },
      { icon: AlertCircle, color: 'text-red-500', label: 'Trend', body: 'Based on a longer trajectory than 1h Change. An ASN showing "DEGRADING" trend but a high current score may be slowly sliding toward a threshold breach.' },
    ]}
  />
)

export const MetroLeaderboardHelp = () => (
  <HelpSection
    intro={<>Health rankings segmented by <strong>city or metro area</strong> rather than ISP — useful when a carrier performs well nationally but has localized issues.</>}
    items={[
      { icon: Globe, color: 'text-blue-500', label: 'ISP vs. city', body: 'A low-scoring city with an otherwise-healthy ASN usually points to a specific datacenter, peering point, or submarine cable serving that metro. Large ISPs like Comcast or AT&T can have poor performance in one city while fine everywhere else.' },
      { icon: Activity, color: 'text-yellow-500', label: 'Requires metro logging', body: <>This panel only appears when <code>req.http.Fastly-Metro</code> or equivalent metro-level fields are present in your logs. If you don&apos;t see it, check your log field configuration.</> },
    ]}
  />
)

export const ShieldingHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground">
    <p>Edge-to-Shield round-trip latency, isolated by subtracting the Shield-to-Origin fetch time from the Edge&apos;s total upstream wait. Requests are correlated via the <code>rid</code>/<code>prid</code> fields.</p>
    <div className="space-y-2 border-t pt-4">
      <h4 className="font-semibold text-foreground text-xs uppercase tracking-wider">Efficiency Legend</h4>
      <ul className="space-y-2 list-none pl-0">
        {[
          { cls: 'bg-[#22c55e]', title: '< 1.5× (Excellent):', text: 'Near-optimal transit performance.' },
          { cls: 'bg-[#eab308]', title: '1.5–3× (Moderate):', text: 'Acceptable but has room for improvement.' },
          { cls: 'bg-[#ef4444]', title: '> 3× (Investigate):', text: 'Significant latency overhead; check for suboptimal peering.' },
          { cls: 'bg-[#94a3b8]', title: 'Grey (Low sample):', text: 'Fewer than 30 requests — too few samples for the median/percentiles to be reliable. Shown for visibility but never colour-graded or flagged.' },
        ].map(({ cls, title, text }) => (
          <li key={title} className="flex items-center gap-2">
            <span className={cn('w-3 h-1 rounded-full', cls)} />
            <span><strong>{title}</strong> {text}</span>
          </li>
        ))}
      </ul>
    </div>
    <div className="space-y-2 border-t pt-4">
      <h4 className="font-semibold text-foreground text-xs uppercase tracking-wider">Map Markers & Arcs</h4>
      <ul className="space-y-2 list-none pl-0">
        {[
          { swatch: <span className="w-2.5 h-2.5 rounded-full bg-[#3b82f6]" />, title: 'Blue Dot:', body: 'Edge POP (first arrival).' },
          { swatch: <span className="w-2.5 h-2.5 rounded-full bg-[#a855f7]" />, title: 'Purple Dot:', body: 'Shield POP.' },
          { swatch: null, title: 'Arc Width:', body: 'Proportional to request volume.' },
          { swatch: <span className="w-4 border-t-2 border-dashed border-[#ef4444]" />, title: 'Dashed Red Arc:', body: <>Flagged route — efficiency &gt; 3× <em>and</em> &ge; 20ms overhead above the light-speed floor, <em>and</em> at least 30 requests. Worth investigating for suboptimal peering.</> },
          { swatch: <span className="w-4 border-t-2 border-solid border-[#94a3b8]" />, title: 'Grey Arc:', body: 'Low-sample route (< 30 requests) — drawn for visibility but not colour-graded or flagged.' },
        ].map(({ swatch, title, body }) => (
          <li key={title} className="flex items-center gap-2">{swatch}<span><strong>{title}</strong> {body}</span></li>
        ))}
      </ul>
    </div>
  </div>
)

export const NetworkQualityHelp = () => (
  <HelpSection
    intro={<>Breaks down <strong>average TCP Round Trip Time</strong> (<code>tcp_rtt</code>) by country, ASN, region, and Fastly POP so you can localise where latency is concentrated, plus an RTT-vs-TTFB sample to separate network latency from origin/edge processing time.</>}
    items={[
      { icon: NetworkIcon, color: 'text-blue-500', label: 'Bars (by country / ASN / region / POP)', body: 'mean RTT in ms for the busiest groups in the window. The region breakdown is scoped to the selected country.' },
      { icon: Zap, color: 'text-blue-500', label: 'RTT vs TTFB scatter', body: 'points far above the diagonal mean time-to-first-byte is dominated by something other than network RTT (origin or edge work); points hugging it are network-bound.' },
      { icon: AlertCircle, color: 'text-yellow-500', label: 'Requires Network Quality (Group F) fields', body: <><code>tcp_rtt</code> must be enabled in Fastly logging, with <code>ttfb</code> for the scatter. The cards hide when those fields aren&apos;t present.</> },
    ]}
  />
)

export function HealthBadge({ score }: { score: number | null }) {
  if (score == null) return <span className="text-muted-foreground">—</span>
  const variant = score >= 80 ? 'outline' : score >= 50 ? 'secondary' : 'destructive'
  const color = score >= 80 ? 'text-green-600 dark:text-green-400' : score >= 50 ? 'text-yellow-600 dark:text-yellow-400' : ''
  return <Badge variant={variant} className={cn('text-[10px]', color)}>{score.toFixed(0)}/100</Badge>
}
