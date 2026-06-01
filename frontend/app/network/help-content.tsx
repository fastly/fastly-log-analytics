import React from "react";
import { Badge } from "@/components/ui/badge";
import { DashboardLinkCell } from "@/components/DashboardLinkCell";
import { cn } from "@/lib/utils";
import { Activity, Shield, AlertTriangle, Search, ActivitySquare, AlertCircle, Globe, Zap, Network as NetworkIcon } from "lucide-react";
export const GlobalHealthHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground">
    <p>A composite <strong>0–100 score</strong> aggregated across your top ASNs, weighted by request volume. 100 = perfect conditions, 0 = severe degradation.</p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Activity className="h-5 w-5 shrink-0 text-blue-500 mt-0.5" />
        <span><strong>How it's calculated:</strong> Combines TCP RTT, packet loss (<code>ploss</code>), and RTT variance (jitter) into a single normalized score for each ASN, then takes a traffic-weighted average.</span>
      </li>
      <li className="flex gap-3">
        <AlertCircle className="h-5 w-5 shrink-0 text-yellow-500" />
        <span><strong>When to act:</strong> A score below 80 warrants investigation. A sustained drop often precedes visible user complaints or error-rate spikes by several minutes — making this a leading indicator.</span>
      </li>
    </ul>
  </div>
)

export const AvgRttHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground">
    <p>Mean <strong>TCP Round Trip Time</strong> in milliseconds across all logged requests in the selected time window.</p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Zap className="h-5 w-5 shrink-0 text-blue-500 mt-0.5" />
        <span><strong>Healthy baselines:</strong> 10–50ms for regional users, 100–200ms for international. Values above 300ms typically result in noticeable UX degradation.</span>
      </li>
      <li className="flex gap-3">
        <AlertCircle className="h-5 w-5 shrink-0 text-yellow-500" />
        <span><strong>Sudden increases:</strong> A 2× or greater spike often indicates ISP congestion, a Fastly routing change, or a DDoS volumetric attack saturating a link. Cross-reference with the ASN Leaderboard to isolate the source.</span>
      </li>
    </ul>
  </div>
)

export const WorstAsnHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground">
    <p>The <strong>Internet Service Provider or network</strong> (identified by Autonomous System Number) with the lowest composite health score during the selected window.</p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <NetworkIcon className="h-5 w-5 shrink-0 text-blue-500 mt-0.5" />
        <span><strong>What causes this:</strong> High RTT + packet loss from a specific ASN typically indicates a peering issue between that ISP and Fastly's edge nodes, or congestion on a transit link.</span>
      </li>
      <li className="flex gap-3">
        <Activity className="h-5 w-5 shrink-0 text-yellow-500" />
        <span><strong>How to act:</strong> Check the ASN Leaderboard to see how many requests this affects and whether the trend is degrading. If it's a large ISP with a worsening trend, Fastly support can investigate peering.</span>
      </li>
    </ul>
  </div>
)

export const WorstRegionHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground">
    <p>The <strong>country experiencing the lowest composite network health score</strong>, based on TCP metrics from users in that geography.</p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Globe className="h-5 w-5 shrink-0 text-blue-500 mt-0.5" />
        <span><strong>ISP vs. regional:</strong> If the same country appears in both this card and the Worst ASN, the issue is likely a single dominant ISP. If the ASN is healthy but the country isn't, a regional routing or infrastructure problem is more likely.</span>
      </li>
      <li className="flex gap-3">
        <Activity className="h-5 w-5 shrink-0 text-yellow-500" />
        <span><strong>Follow-up:</strong> Use the Metro Leaderboard to narrow down to a specific city. A single underperforming city within an otherwise healthy country usually points to a specific datacenter or peering point.</span>
      </li>
    </ul>
  </div>
)

export const HeatmapHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground">
    <p>A grid where each <strong>row is an ISP (ASN)</strong>, each <strong>column is a time bucket</strong>, and the color represents the composite health score — red (0) to green (100).</p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Activity className="h-5 w-5 shrink-0 text-blue-500" />
        <span><strong>Reading it:</strong> A row that turns red at a specific column pinpoints exactly when degradation started for a given ISP. Persistent red rows indicate long-standing peering or routing problems that predate your current time window.</span>
      </li>
      <li className="flex gap-3">
        <AlertCircle className="h-5 w-5 shrink-0 text-yellow-500" />
        <span><strong>Shared incidents:</strong> Multiple rows going red simultaneously suggests a widespread event — a major transit outage, a volumetric attack, or a Fastly-side issue rather than an ISP-specific problem.</span>
      </li>
    </ul>
  </div>
)

export const AsnLeaderboardHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground">
    <p>Rankings of every observed <strong>Internet Service Provider</strong> by composite health score, with tail-latency breakdowns and change signals.</p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Zap className="h-5 w-5 shrink-0 text-blue-500 mt-0.5" />
        <span><strong>P95 / P99 RTT:</strong> Tail latency — the slowest 5% and 1% of TCP handshakes for users on this network. High P99 with a healthy P95 means most users are fine but a small percentage are experiencing severe problems.</span>
      </li>
      <li className="flex gap-3">
        <Activity className="h-5 w-5 shrink-0 text-yellow-500" />
        <span><strong>1h Change:</strong> Real-time signal — a negative value in red means the score is actively declining, often indicating an in-progress incident. A negative value in a currently-healthy ASN is worth watching.</span>
      </li>
      <li className="flex gap-3">
        <AlertCircle className="h-5 w-5 shrink-0 text-red-500" />
        <span><strong>Trend:</strong> Based on a longer trajectory than 1h Change. An ASN showing "DEGRADING" trend but a high current score may be slowly sliding toward a threshold breach.</span>
      </li>
    </ul>
  </div>
)

export const MetroLeaderboardHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground">
    <p>Health rankings segmented by <strong>city or metro area</strong> rather than ISP — useful when a carrier performs well nationally but has localized issues.</p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Globe className="h-5 w-5 shrink-0 text-blue-500" />
        <span><strong>ISP vs. city:</strong> A low-scoring city with an otherwise-healthy ASN usually points to a specific datacenter, peering point, or submarine cable serving that metro. Large ISPs like Comcast or AT&T can have poor performance in one city while fine everywhere else.</span>
      </li>
      <li className="flex gap-3">
        <Activity className="h-5 w-5 shrink-0 text-yellow-500" />
        <span><strong>Requires metro logging:</strong> This panel only appears when <code>req.http.Fastly-Metro</code> or equivalent metro-level fields are present in your logs. If you don't see it, check your log field configuration.</span>
      </li>
    </ul>
  </div>
)

export const ShieldingHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground">
    <p>Edge-to-Shield round-trip latency, isolated by subtracting the Shield-to-Origin fetch time from the Edge&apos;s total upstream wait. Requests are correlated via the <code>rid</code>/<code>prid</code> fields.</p>
    
    <div className="space-y-2 border-t pt-4">
      <h4 className="font-semibold text-foreground text-xs uppercase tracking-wider">Efficiency Legend</h4>
      <ul className="space-y-2 list-none pl-0">
        <li className="flex items-center gap-2">
          <span className="w-3 h-1 rounded-full bg-[#22c55e]" />
          <span><strong>&lt; 1.5&times; (Excellent):</strong> Near-optimal transit performance.</span>
        </li>
        <li className="flex items-center gap-2">
          <span className="w-3 h-1 rounded-full bg-[#eab308]" />
          <span><strong>1.5&ndash;3&times; (Moderate):</strong> Acceptable but has room for improvement.</span>
        </li>
        <li className="flex items-center gap-2">
          <span className="w-3 h-1 rounded-full bg-[#ef4444]" />
          <span><strong>&gt; 3&times; (Investigate):</strong> Significant latency overhead; check for suboptimal peering.</span>
        </li>
      </ul>
    </div>

    <div className="space-y-2 border-t pt-4">
      <h4 className="font-semibold text-foreground text-xs uppercase tracking-wider">Map Markers & Arcs</h4>
      <ul className="space-y-2 list-none pl-0">
        <li className="flex items-center gap-2">
          <span className="w-2.5 h-2.5 rounded-full bg-[#3b82f6]" />
          <span><strong>Blue Dot:</strong> Edge POP (first arrival).</span>
        </li>
        <li className="flex items-center gap-2">
          <span className="w-2.5 h-2.5 rounded-full bg-[#a855f7]" />
          <span><strong>Purple Dot:</strong> Shield POP.</span>
        </li>
        <li className="flex items-center gap-2">
          <div className="flex flex-col gap-0.5">
            <span><strong>Arc Width:</strong> Proportional to request volume.</span>
          </div>
        </li>
      </ul>
    </div>
  </div>
)

// ── Helpers ───────────────────────────────────────────────────────────────────

export function HealthBadge({ score }: { score: number | null }) {
  if (score == null) return <span className="text-muted-foreground">—</span>
  const variant = score >= 80 ? 'outline' : score >= 50 ? 'secondary' : 'destructive'
  const color = score >= 80 ? 'text-green-600 dark:text-green-400' : score >= 50 ? 'text-yellow-600 dark:text-yellow-400' : ''
  return (
    <Badge variant={variant as any} className={cn("text-[10px]", color)}>
      {score.toFixed(0)}/100
    </Badge>
  )
}

export const SHIELDING_COLUMNS = [
  {
    accessorKey: 'edge_pop',
    id: 'edge_pop', meta: { label: 'Edge POP' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">Edge POP</span>,
    cell: (info: any) => (
      <DashboardLinkCell
        value={info.getValue()}
        href={`/dashboard?filter_pop=${encodeURIComponent(info.getValue())}`}
        className={cn('font-bold', info.row.original.anomaly_static ? 'text-destructive' : '')}
      />
    )
  },
  {
    accessorKey: 'shield_pop',
    id: 'shield_pop', meta: { label: 'Shield POP' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">Shield POP</span>,
    cell: (info: any) => (
      <DashboardLinkCell
        value={info.getValue()}
        href={`/dashboard?filter_shield_pop=${encodeURIComponent(info.getValue())}`}
        className="font-bold text-purple-500"
      />
    )
  },
  { accessorKey: 'requests', id: 'requests', meta: { label: 'Requests' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">Reqs</span>, cell: (info: any) => info.getValue().toLocaleString() },
  { accessorKey: 'p50_ms', id: 'p50_ms', meta: { label: 'Median (P50)' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">P50 (E→S)</span>, cell: (info: any) => <span>{info.getValue()?.toFixed(1)}ms</span> },
  { accessorKey: 'p95_ms', id: 'p95_ms', meta: { label: 'P95 Latency' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">P95 (E→S)</span>, cell: (info: any) => <span>{info.getValue()?.toFixed(1)}ms</span> },
  { accessorKey: 'p99_ms', id: 'p99_ms', meta: { label: 'P99 Latency' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">P99 (E→S)</span>, cell: (info: any) => <span>{info.getValue()?.toFixed(1)}ms</span> },
  { accessorKey: 'light_speed_rtt_ms', id: 'light_speed_rtt_ms', meta: { label: 'Light-Speed Floor' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">Light Floor</span>, cell: (info: any) => info.getValue() != null ? <span className="text-muted-foreground">{info.getValue().toFixed(1)}ms</span> : <span className="text-muted-foreground">—</span> },
  {
    accessorKey: 'efficiency_ratio', id: 'efficiency_ratio', meta: { label: 'Efficiency' }, header: () => <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">Efficiency</span>, cell: (info: any) => {
      const ratio = info.getValue()
      if (ratio == null) return <span className="text-muted-foreground">—</span>
      const cls = ratio < 1.5 ? 'bg-green-500/10 text-green-600 dark:text-green-400' : ratio < 3 ? 'bg-yellow-500/10 text-yellow-600 dark:text-yellow-400' : 'bg-destructive/10 text-destructive'
      return <span className={cn("inline-flex items-center rounded-full px-1.5 py-0.5 text-[10px] font-bold tabular-nums", cls)}>{ratio.toFixed(1)}×</span>
    }
  },
]

export const SHIELDING_LABELS: Record<string, string> = {
  edge_pop: 'Edge POP', shield_pop: 'Shield POP', requests: 'Requests',
  p50_ms: 'Median (P50)', p95_ms: 'P95 Latency', p99_ms: 'P99 Latency',
  light_speed_rtt_ms: 'Light-Speed Floor', efficiency_ratio: 'Efficiency',
}
export const getShieldingLabels = (ids: string[]) => ids.map(id => ({ id, label: SHIELDING_LABELS[id] || id }))

// ── Page ──────────────────────────────────────────────────────────────────────

