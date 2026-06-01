import React from "react";
import { Activity, Shield, Server, TrendingUp, Filter, BarChart3, Fingerprint, Network, Globe, Clock } from "lucide-react";
export const SlowestUrlsHelp = () => (
  <div className="space-y-4">
    <p>The top URLs ranked by a chosen latency metric, calculated across the selected time range.</p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Activity className="h-5 w-5 shrink-0 text-blue-500 mt-0.5" />
        <span><strong>Sort by P99</strong> to find endpoints that are occasionally extremely slow — these often have the worst user impact even with low request volume.</span>
      </li>
      <li className="flex gap-3">
        <Server className="h-5 w-5 shrink-0 text-yellow-500 mt-0.5" />
        <span><strong>Sort by Avg</strong> to identify consistently slow routes that may be worth caching or optimizing at the origin.</span>
      </li>
      <li className="flex gap-3">
        <TrendingUp className="h-5 w-5 shrink-0 text-red-500 mt-0.5" />
        <span>Cross-reference high P99 URLs with the <strong>Origin vs Edge</strong> scatter to determine whether the slowness is edge-side or origin-side.</span>
      </li>
    </ul>
  </div>
)

export const SlowestNetworksHelp = () => (
  <div className="space-y-4">
    <p>Top Autonomous Systems (ISPs / networks) ranked by latency, showing which client networks experience the slowest responses.</p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Network className="h-5 w-5 shrink-0 text-blue-500 mt-0.5" />
        <span><strong>ASN</strong> identifies the Internet Service Provider or hosting network the client is connecting from (e.g., Comcast AS7922, AWS AS16509).</span>
      </li>
      <li className="flex gap-3">
        <Activity className="h-5 w-5 shrink-0 text-yellow-500 mt-0.5" />
        <span>High latency for a specific ASN with many requests may indicate a <strong>peering issue</strong> between that network and Fastly, rather than an origin problem.</span>
      </li>
      <li className="flex gap-3">
        <TrendingUp className="h-5 w-5 shrink-0 text-red-500 mt-0.5" />
        <span>Hosting ASNs (AWS, GCP, Azure) appearing here may indicate <strong>bot or scraper traffic</strong> that naturally has different latency characteristics than consumer ISPs.</span>
      </li>
    </ul>
  </div>
)

export const CacheTtlHelp = () => (
  <div className="space-y-4">
    <p>A histogram showing the distribution of <code className="text-xs bg-muted px-1 py-0.5 rounded">Cache-Control: max-age</code> (TTL) values set on responses during the selected period.</p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Clock className="h-5 w-5 shrink-0 text-blue-500 mt-0.5" />
        <span><strong>Short TTLs</strong> (near 0s) increase origin load because objects expire quickly and must be re-fetched. Review whether frequently-requested objects can be cached longer.</span>
      </li>
      <li className="flex gap-3">
        <Activity className="h-5 w-5 shrink-0 text-green-500 mt-0.5" />
        <span><strong>Longer TTLs</strong> (hours or days) reduce origin round-trips. If most of your content clusters in a short bucket, increasing TTLs is the single highest-leverage caching improvement.</span>
      </li>
      <li className="flex gap-3">
        <TrendingUp className="h-5 w-5 shrink-0 text-yellow-500 mt-0.5" />
        <span>A bimodal distribution (two peaks) often means different content types — static assets vs API responses — are being served with very different TTL strategies.</span>
      </li>
    </ul>
  </div>
)

export const OriginVsEdgeHelp = () => (
  <div className="space-y-4">
    <p>A scatter plot of individual requests, plotting <strong>Origin TTFB</strong> on the X axis against <strong>Edge Processing time</strong> on the Y axis, colored by cache status.</p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Activity className="h-5 w-5 shrink-0 text-green-500 mt-0.5" />
        <span><strong>HIT points</strong> cluster near the left axis (low origin time) — the edge served from cache without contacting the origin at all.</span>
      </li>
      <li className="flex gap-3">
        <Server className="h-5 w-5 shrink-0 text-yellow-500 mt-0.5" />
        <span><strong>MISS points</strong> spread further right as origin TTFB varies. A long horizontal spread means inconsistent backend response times.</span>
      </li>
      <li className="flex gap-3">
        <TrendingUp className="h-5 w-5 shrink-0 text-red-500 mt-0.5" />
        <span>Points high on the Y axis (slow edge processing) regardless of origin time may indicate complex VCL logic, large response bodies, or edge-side compression overhead.</span>
      </li>
    </ul>
  </div>
)

