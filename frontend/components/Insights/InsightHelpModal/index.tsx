import React from 'react'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Badge } from '@/components/ui/badge'
import {
  Globe,
  User,
  Zap,
  TrendingDown,
  Info,
  Activity,
  MapPin,
  AlertTriangle,
  BarChart,
  Server,
  WifiOff,
  Clock,
  Database,
  ShieldAlert,
  TrendingUp,
  Building2,
  Network,
} from 'lucide-react'
import { getSecurityContent, type InsightContent, type InsightHelpModalProps } from '@/components/Insights'

export function getOptimizationContent(id: string): InsightContent | null {
  switch (id) {
    case 'image_optimization_opportunities':
      return {
        title: 'Image Optimization Opportunities',
        icon: <Zap className="h-5 w-5 text-primary" />,
        fields: ['url', 'resp_bytes', 'ua'],
        description: (
          <div className="space-y-4">
            <p>Identifies images served without optimization parameters, which leads to unnecessarily high bandwidth usage and slower page loads.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <TrendingDown className="h-5 w-5 shrink-0 text-green-500" />
                <span><strong>Byte Savings:</strong> Modern formats like WebP or AVIF can often reduce image sizes by 50-80% without visible quality loss.</span>
              </li>
              <li className="flex gap-3">
                <User className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Mobile Impact:</strong> Large images sent to mobile devices are particularly expensive for users on limited data plans and slow down mobile page performance.</span>
              </li>
              <li className="flex gap-3">
                <Zap className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Easy Win:</strong> Most of these images can be optimized by enabling Fastly Image Optimizer and appending <code>?auto=webp</code> to your image URLs.</span>
              </li>
            </ul>
          </div>
        )
      }

    default:
      return null
  }
}

export function getDefaultContent(): InsightContent {
  return {
    title: 'Insight Analysis',
    icon: <Info className="h-5 w-5 text-primary" />,
    fields: [],
    description: (
      <div className="space-y-4">
        <p>This insight is powered by comparing your current traffic patterns against your selected historical baseline.</p>
        <p className="text-sm text-muted-foreground">We look for statistical outliers in volume, error rates, or performance metrics to surface potential issues before they become outages.</p>
      </div>
    )
  }
}

export function getErrorsContent(id: string): InsightContent | null {
  switch (id) {
    case 'error_spikes':
      return {
        title: 'Global Error Spikes',
        icon: <AlertTriangle className="h-5 w-5 text-primary" />,
        fields: ['status'],
        description: (
          <div className="space-y-4">
            <p>Detects sudden, dramatic increases in 5xx server errors across your entire service.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <BarChart className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Error Rate Tracking:</strong> We compare the current 5xx error percentage to the historical average.</span>
              </li>
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>Spike Threshold:</strong> Triggers when the error rate triples the baseline and exceeds a strict minimum threshold, indicating a system-wide incident.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'city_error_spikes':
      return {
        title: 'City-Level Error Spikes',
        icon: <Globe className="h-5 w-5 text-primary" />,
        fields: ['status', 'city'],
        description: (
          <div className="space-y-4">
            <p>Detects localized outages by tracking 5xx error rates segmented by individual cities.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <MapPin className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Geographic Segmentation:</strong> Errors are calculated per city rather than globally, uncovering issues that only affect specific regions.</span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Routing Issues:</strong> Often indicates a regional routing problem or an origin server in a specific geography failing.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'origin_error_rate':
      return {
        title: 'Origin Error Rate',
        icon: <AlertTriangle className="h-5 w-5 text-primary" />,
        fields: ['ost', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Flags a significant, statistical spike in HTTP 5xx error responses returned directly by your upstream origin application servers.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Backend Application Failures:</strong> Monitors HTTP response statuses (such as 500 Internal Server Error, 502 Bad Gateway, 503 Service Unavailable, or 504 Gateway Timeout) originating from your backend servers.
                </span>
              </li>
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>Outlier Detection:</strong> Triggers when the backend error rate rises dramatically above historical baseline levels, signifying an active outage or application crash.
                </span>
              </li>
              <li className="flex gap-3">
                <Globe className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Check your application server logs, verify database connection health, and review CPU/RAM utilization on your backend hosts.
                </span>
              </li>
            </ul>
          </div>
        )
      }

    case 'origin_ip_failure':
      return {
        title: 'Specific Origin IP Failing',
        icon: <Globe className="h-5 w-5 text-primary" />,
        fields: ['oip', 'ost', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Pinpoints a specific load-balanced backend IP address that is returning a higher rate of errors than other peers in the same pool.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <MapPin className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>Peer Outlier Analysis:</strong> Groups origin-bound traffic by individual IP addresses (OIP) to isolate failures to specific hosts rather than the entire cluster.
                </span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>Deployment or Node Crashes:</strong> Uncovers bad deployments, localized server failures, or configuration drift on a single host behind a global load balancer.
                </span>
              </li>
              <li className="flex gap-3">
                <Globe className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Temporarily drain/disable the failing IP in your Fastly backend list or load balancer, then inspect that host for service crashes or configuration mismatches.
                </span>
              </li>
            </ul>
          </div>
        )
      }

    default:
      return null
  }
}

export function getCacheContent(id: string): InsightContent | null {
  switch (id) {
    case 'cache_collapse':
      return {
        title: 'Cache Efficiency Collapse',
        icon: <WifiOff className="h-5 w-5 text-primary" />,
        fields: ['cache', 'url'],
        description: (
          <div className="space-y-4">
            <p>Detects URLs where the Cache Hit Ratio (CHR) has dropped dramatically, potentially causing an &quot;origin fire.&quot;</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <TrendingDown className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>Efficiency Drop:</strong> Flags URLs that previously had &gt;80% CHR but have suddenly dropped to &lt;20%.</span>
              </li>
              <li className="flex gap-3">
                <Server className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Origin Impact:</strong> This usually indicates a change in query parameters (cache busting) or a deployment that accidentally disabled caching for a hot route.</span>
              </li>
              <li className="flex gap-3">
                <Database className="h-5 w-5 shrink-0 text-muted-foreground" />
                <span><strong>CHR = HIT / (HIT + MISS).</strong> PASS (uncacheable) requests are excluded — a surge of those is reported separately under <em>Cacheability Regression</em>, not here.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'cacheability_regression':
      return {
        title: 'Cacheability Regression',
        icon: <Database className="h-5 w-5 text-primary" />,
        fields: ['cache', 'url'],
        description: (
          <div className="space-y-4">
            <p>Detects URLs that flipped from cacheable to mostly <strong>PASS</strong> — Fastly bypassed the cache entirely, so requests now go straight to origin.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <TrendingDown className="h-5 w-5 shrink-0 text-amber-500" />
                <span><strong>PASS surge:</strong> Flags URLs whose PASS share (PASS / all requests) jumped from a low baseline to a majority of recent traffic.</span>
              </li>
              <li className="flex gap-3">
                <Server className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Common causes:</strong> origin started sending <code>Set-Cookie</code> or <code>Cache-Control: private</code>, a newly-varying query param, or a VCL change forcing <code>pass</code>.</span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Why it&apos;s separate:</strong> PASS was never cacheable, so it isn&apos;t a hit-ratio miss. The fix is to restore cacheability, not to warm the cache.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'cache_pressure':
      return {
        title: 'Cache Pressure & Evictions',
        icon: <Database className="h-5 w-5 text-primary" />,
        fields: ['digest', 'ttl', 'age', 'pop', 'cache', 'resp_bytes'],
        description: (
          <div className="space-y-4">
            <p>Detects when objects are being prematurely evicted from the edge cache before their TTL (Time To Live) expires.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Clock className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Age vs TTL:</strong> We analyze cache misses and compare the object&apos;s expected TTL against the time since it was last fetched.</span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Capacity Warning:</strong> High rates of premature eviction mean your Fastly service is under &quot;Cache Pressure&quot; and objects are being pushed out of memory to make room for new ones. You may need to increase your Cache Reservation.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'cache_hit_cliff':
      return {
        title: 'Cache HIT-Ratio Cliff',
        icon: <TrendingDown className="h-5 w-5 text-primary" />,
        fields: ['cache'],
        description: (
          <div className="space-y-4">
            <p>A single headline card: your whole service&apos;s edge cache HIT ratio fell off a cliff vs baseline. This is the aggregate counterpart to the per-URL Cache Efficiency Collapse.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Database className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Cacheable ratio:</strong> We measure HIT / (HIT + MISS) across all traffic — PASS is excluded because it was never eligible to cache — and compare the window to baseline.</span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>What it catches:</strong> a purge storm, a global TTL change, an origin <code>Cache-Control</code> regression, or a VCL deploy that started passing traffic — each pushes load onto origin and slows delivery site-wide.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'cache_ttl_mismatch':
      return {
        title: 'Cache TTL Inefficiency',
        icon: <Clock className="h-5 w-5 text-primary" />,
        fields: ['ttl', 'hits', 'age', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Identifies URLs configured with very high Cache TTL (Time To Live) but receiving extremely low hit counts, which can lead to inefficient cache allocation.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Database className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>Inefficient Cache Retention:</strong> Objects with long TTLs but very few requests occupy high-value edge memory slots without providing equivalent cache-efficiency returns.
                </span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>Wasted Cache Slots:</strong> These items can push out hot, high-demand resources, contributing to overall cache pressure and premature evictions elsewhere.
                </span>
              </li>
              <li className="flex gap-3">
                <Clock className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Consider reducing the TTL for low-traffic or cold resources using Cache-Control directives from the origin, or optimize cache key clustering in VCL.
                </span>
              </li>
            </ul>
          </div>
        )
      }

    default:
      return null
  }
}

export function getTrafficContent(id: string): InsightContent | null {
  switch (id) {
    case 'city_surges':
      return {
        title: 'City Traffic Surges',
        icon: <TrendingUp className="h-5 w-5 text-primary" />,
        fields: ['city'],
        description: (
          <div className="space-y-4">
            <p>Identifies cities experiencing massive, anomalous spikes in traffic volume.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Volume Comparison:</strong> We compare current request counts per city to their historical average.</span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>Attack Indicator:</strong> A 10x or 100x spike from a single city is a strong indicator of a localized botnet or DDoS attack originating from that region.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'new_city_traffic':
    case 'new_country_traffic':
      return {
        title: id === 'new_city_traffic' ? 'New City Traffic' : 'New Country Traffic',
        icon: <Globe className="h-5 w-5 text-primary" />,
        fields: [id === 'new_city_traffic' ? 'city' : 'country'],
        description: (
          <div className="space-y-4">
            <p>Flags traffic from locations that have had absolute zero presence in your historical baseline.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Database className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Historical Absence:</strong> The system verifies that this location generated 0 requests over the entire baseline period.</span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Botnet Shift:</strong> While it could be legitimate new users, sudden high-volume traffic from entirely new regions often indicates a botnet shifting its attack infrastructure.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'asn_concentration':
      return {
        title: 'ASN Concentration',
        icon: <Building2 className="h-5 w-5 text-primary" />,
        fields: ['asn'],
        description: (
          <div className="space-y-4">
            <p>Detects when a single ISP or Hosting Provider (ASN) begins dominating your traffic volume.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <BarChart className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Traffic Share:</strong> We calculate the percentage of total requests originating from each ASN.</span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Datacenter Attacks:</strong> Legitimate traffic is usually distributed across consumer ISPs. Heavy concentration in a single hosting ASN (like AWS, DigitalOcean, or Hetzner) strongly suggests a scraper or volumetric attack.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'referer_monoculture':
      return {
        title: 'Referer Monoculture',
        icon: <Globe className="h-5 w-5 text-primary" />,
        fields: ['referer'],
        description: (
          <div className="space-y-4">
            <p>Flags a single <code>Referer</code> value driving an outsized, spiking share of your total traffic.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <BarChart className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Traffic share:</strong> We measure each Referer&apos;s percentage of window traffic and compare it to its baseline share, flagging one that jumps past a dominance threshold.</span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>What it catches:</strong> a scraper or embed hotlinking your content, a viral inbound source, or a spoofed-Referer flood trying to blend in as organic traffic.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'method_drift':
      return {
        title: 'HTTP Method Drift',
        icon: <TrendingUp className="h-5 w-5 text-primary" />,
        fields: ['method'],
        description: (
          <div className="space-y-4">
            <p>Flags a write method (<code>POST</code>, <code>PUT</code>, <code>DELETE</code>, …) surging to an outsized share of traffic against a normally read-dominated (<code>GET</code>) baseline.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Verb mix:</strong> We track each non-<code>GET</code> method&apos;s share of window traffic vs its baseline, so a sudden write-heavy shift stands out.</span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>What it catches:</strong> API abuse, form/credential POST floods, or unexpected <code>PUT</code>/<code>DELETE</code> activity against endpoints that normally only serve reads.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'new_asn_traffic':
      return {
        title: 'New ASN Traffic',
        icon: <Building2 className="h-5 w-5 text-primary" />,
        fields: ['asn'],
        description: (
          <div className="space-y-4">
            <p>Flags an ASN (ISP or datacenter) with absolute zero baseline presence that is now sending meaningful traffic — the ASN-level sibling of New City / New Country Traffic.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Database className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Historical absence:</strong> We confirm the ASN generated zero requests across the entire baseline period before flagging its window traffic.</span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>What it catches:</strong> a new botnet source, a proxy/VPN pool coming online, or a datacenter range suddenly directed at your site.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'asn_hosting_shift':
      return {
        title: 'ASN Hosting Shift',
        icon: <Building2 className="h-5 w-5 text-primary" />,
        fields: ['asn', 'p_type'],
        description: (
          <div className="space-y-4">
            <p>Detects when an ASN&apos;s traffic composition shifts from consumer to datacenter/hosting — e.g., an ASN that was 5% hosting-flagged in baseline but 60% in the current window.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <BarChart className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Hosting ratio:</strong> We compute each ASN&apos;s share of <code>hosting</code>-classified requests (via Fastly&apos;s proxy-type geo-IP classification) in the window vs baseline.</span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>What it catches:</strong> bot pools or scrapers spinning up on hosting/datacenter infrastructure behind an ASN that normally carries consumer traffic.</span>
              </li>
            </ul>
          </div>
        )
      }

    default:
      return null
  }
}

export function getPerformanceContent(id: string): InsightContent | null {
  switch (id) {
    case 'city_latency_regressions':
      return {
        title: 'City Latency Regressions',
        icon: <Clock className="h-5 w-5 text-primary" />,
        fields: ['city', 'elapsed'],
        description: (
          <div className="space-y-4">
            <p>Detects when specific cities begin experiencing severe latency (slowness) compared to their normal baseline.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>P95 Latency:</strong> We track the 95th percentile response time (`elapsed`) for every city.</span>
              </li>
              <li className="flex gap-3">
                <Clock className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Slowdown Detection:</strong> Triggers when a city&apos;s P95 latency doubles or triples, often indicating congestion at a specific edge node or peering point.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'asn_metro_performance':
      return {
        title: 'ASN/Metro Performance Regressions',
        icon: <Network className="h-5 w-5 text-primary" />,
        fields: ['asn', 'metro', 'tcp_rtt'],
        description: (
          <div className="space-y-4">
            <p>Monitors network-level degradation by tracking TCP Round Trip Time (RTT) across specific Internet Service Providers (ASNs) in specific geographic metros.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Zap className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Granular Tracking:</strong> Network performance varies wildly by region. We establish baselines for each ISP in each specific city/metro area.</span>
              </li>
              <li className="flex gap-3">
                <TrendingUp className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>ISP Outages:</strong> A sudden spike in TCP RTT for Comcast users in Chicago indicates a localized ISP peering issue or fiber cut.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'latency_regression':
      return {
        title: 'Global Latency Regression',
        icon: <Clock className="h-5 w-5 text-primary" />,
        fields: ['url', 'elapsed'],
        description: (
          <div className="space-y-4">
            <p>Detects specific URLs or API endpoints that have become significantly slower to process compared to their historical baseline.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Endpoint Profiling:</strong> We track the P95 latency for every unique URL path over the historical baseline.</span>
              </li>
              <li className="flex gap-3">
                <TrendingUp className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>Bad Deployments:</strong> Flags URLs where the processing time has doubled or worse, commonly highlighting an unoptimized database query or a regression in a recent code deployment.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'network_asn_health':
      return {
        title: 'Network & ASN Health',
        icon: <Activity className="h-5 w-5 text-primary" />,
        fields: ['asn', 'tcp_rtt', 'ploss', 'rtt_min', 'rtt_var'],
        description: (
          <div className="space-y-4">
            <p>Analyzes the fundamental TCP connection quality between end users and the Fastly edge, segmented by ISP.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Network className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Deep Metrics:</strong> Uses low-level kernel metrics like Packet Loss (`ploss`), Jitter (`rtt_var`), and minimum latency (`rtt_min`).</span>
              </li>
              <li className="flex gap-3">
                <Info className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Video & Gaming:</strong> Essential for highly-sensitive workloads like streaming video or multiplayer gaming where packet loss and jitter impact user experience far more than pure throughput.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'pop_latency_regression':
      return {
        title: 'PoP Latency Regression',
        icon: <Zap className="h-5 w-5 text-primary" />,
        fields: ['pop', 'elapsed'],
        description: (
          <div className="space-y-4">
            <p>Isolates a single Fastly PoP (datacenter) whose P95 edge latency regressed sharply vs its own baseline — finer-grained than region or city latency.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Per-PoP P95:</strong> We compute each PoP&apos;s window P95 <code>elapsed</code> time and flag it only when it is well above baseline in both ratio and absolute milliseconds.</span>
              </li>
              <li className="flex gap-3">
                <Info className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Why it matters:</strong> a single degraded PoP (a bad cache node, congested transit, or a partial outage) can slow a whole geography while the global average still looks fine.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'metro_delivery_degradation':
      return {
        title: 'Metro Delivery-Rate Degradation',
        icon: <Network className="h-5 w-5 text-primary" />,
        fields: ['metro', 'delivery_rate'],
        description: (
          <div className="space-y-4">
            <p>Flags US metro areas whose median kernel-measured TCP delivery rate (throughput) collapsed vs baseline — a regional last-mile or peering problem.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <TrendingUp className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Median throughput:</strong> We compare each metro&apos;s median <code>delivery_rate</code> (bytes/sec, shown as Mbps) in the window vs baseline, flagging a halving or worse.</span>
              </li>
              <li className="flex gap-3">
                <Info className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Why it matters:</strong> throughput collapse in one metro points at an ISP peering dispute or last-mile congestion — invisible in latency-only views but painful for large-object delivery.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'connection_type_mix':
      return {
        title: 'Connection-Type Mix Shift',
        icon: <Network className="h-5 w-5 text-primary" />,
        fields: ['c_type', 'c_speed'],
        description: (
          <div className="space-y-4">
            <p>Flags a client connection type/speed combination surging to an outsized, spiking share of your typed traffic vs baseline.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Population mix:</strong> We track the share of each <code>c_type</code>/<code>c_speed</code> combo (residential, cellular, corporate, datacenter…) and flag a sudden dominance swing.</span>
              </li>
              <li className="flex gap-3">
                <Info className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Why it matters:</strong> a jump in datacenter/cellular share often means a bot pool coming online or a routing change, and shifts the throughput/latency profile you should expect.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'http3_fallback':
      return {
        title: 'HTTP/3 → TCP Fallback Spike',
        icon: <Network className="h-5 w-5 text-primary" />,
        fields: ['transport'],
        description: (
          <div className="space-y-4">
            <p>Flags a service-wide drop in QUIC (HTTP/3) share vs a QUIC-healthy baseline — clients failing to sustain QUIC and falling back to TCP.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <TrendingUp className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Transport share:</strong> We compare the share of requests served over <code>quic</code> in the window vs baseline and flag a material collapse.</span>
              </li>
              <li className="flex gap-3">
                <Info className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Why it matters:</strong> a sudden QUIC→TCP fallback points at a network path newly throttling or blocking UDP (a middlebox, mobile carrier, or corporate firewall change) and usually raises tail latency.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'payload_compression_regression':
      return {
        title: 'Payload Compression Regression',
        icon: <Zap className="h-5 w-5 text-primary" />,
        fields: ['url', 'resp_header_content_encoding', 'resp_bytes'],
        description: (
          <div className="space-y-4">
            <p>Flags compressible responses (JS, CSS, HTML, JSON, SVG, XML) that flipped from compressed (<code>gzip</code>/<code>br</code>) to served <strong>uncompressed</strong> vs baseline.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Encoding rate:</strong> per compressible URL we track the share of 200-responses served with no <code>Content-Encoding</code>, and flag a URL that was compressed at baseline but is now mostly uncompressed.</span>
              </li>
              <li className="flex gap-3">
                <Info className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Why it matters:</strong> losing compression on text assets inflates egress bandwidth and TTFB for every client — usually a broken <code>Accept-Encoding</code> path, a VCL change that unset the encoding, or an origin regression.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'timeout_split':
      return {
        title: 'Origin Connect vs Read Timeout Split',
        icon: <Clock className="h-5 w-5 text-primary" />,
        fields: ['oconnect_ms', 'ottfb'],
        description: (
          <div className="space-y-4">
            <p>Splits origin slowness into its two phases and flags whichever P95 regressed: <strong>connect</strong> (TCP + TLS handshake to origin) vs <strong>read</strong> (time from connect to first byte).</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Network className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Slow connect:</strong> a handshake-time (<code>oconnect_ms</code>) spike points at origin TCP/TLS or load-balancer saturation — the 503-class failure mode.</span>
              </li>
              <li className="flex gap-3">
                <TrendingUp className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Slow read:</strong> a read-time spike (TTFB minus connect) points at slow origin application or database processing — the 504-class failure mode.</span>
              </li>
              <li className="flex gap-3">
                <Info className="h-5 w-5 shrink-0 text-muted-foreground" />
                <span><strong>Note:</strong> connect time is null on cache HITs and on connections that never established, so this surfaces connections that established <em>slowly</em>.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'origin_latency_spike':
      return {
        title: 'Origin Latency Spike',
        icon: <Clock className="h-5 w-5 text-primary" />,
        fields: ['ottfb', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Detects a sudden and significant increase in the 95th percentile (P95) response time (TTFB) directly from your origin servers for specific endpoints.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Clock className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Slow Backend Processing:</strong> Compares the current window&apos;s P95 origin time-to-first-byte (<code>ottfb</code>) against its baseline distribution per URL to identify specific route slowdowns.
                </span>
              </li>
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>Infrastructure Bottlenecks:</strong> Highlights database lockups, slow code execution paths, or API bottlenecks that are dragging down backend performance.
                </span>
              </li>
              <li className="flex gap-3">
                <Info className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Investigate backend database queries or memory utilization, profile the slow endpoint, or adjust CDN caching rules to shield your origin.
                </span>
              </li>
            </ul>
          </div>
        )
      }

    case 'origin_retries':
      return {
        title: 'Origin Retries Elevated',
        icon: <Activity className="h-5 w-5 text-primary" />,
        fields: ['oretries', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Flags specific endpoints that are experiencing a high volume of connection or fetch retries from Fastly to your origin servers.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Network className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>Failed Fetch Attempts:</strong> Tracks requests where Fastly encountered a connection timeout or handshake failure on the first try and had to retry the fetch.
                </span>
              </li>
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>Origin Connection Instability:</strong> High retry rates point at origin thread pool exhaustion, aggressive firewall blocks, or regional packet loss.
                </span>
              </li>
              <li className="flex gap-3">
                <Info className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Verify origin TCP keep-alive, firewall rules, and load balancer capacity, ensuring they match peak concurrent connections.
                </span>
              </li>
            </ul>
          </div>
        )
      }

    case 'shield_path_degradation':
      return {
        title: 'Shield Path Degradation',
        icon: <Network className="h-5 w-5 text-primary" />,
        fields: ['rid', 'prid', 'edge', 'pop', 'ottfb', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Detects a significant latency increase on the specific network transit path between your edge POPs and your designated shield POPs.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <strong>POP-to-POP Transit:</strong> Measures transit time specifically between the receiving edge node and the shield layer before forwarding to the origin.
              </li>
              <li className="flex gap-3">
                <strong>Regional Congestion:</strong> Isolates regional cloud routing delays or peering bottlenecks between Fastly POPs from standard origin slowness.
              </li>
              <li className="flex gap-3">
                <strong>Actionable Mitigation:</strong> Check Fastly network status or consider switching to a different shield location if transit degradation persists.
              </li>
            </ul>
          </div>
        )
      }

    case 'region_latency':
      return {
        title: 'Regional Latency Degradation',
        icon: <Clock className="h-5 w-5 text-primary" />,
        fields: ['server_region', 'elapsed', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Flags broad geographical regions experiencing a significant aggregate increase in response time compared to baseline.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <strong>Geographic Impact:</strong> Aggregates response time (<code>elapsed</code>) by continent or geographic server region to pinpoint location-specific slowness.
              </li>
              <li className="flex gap-3">
                <strong>Transit Outages:</strong> Often indicates submarine cable cuts, ISP routing loops, or global transit provider disruptions affecting specific regions.
              </li>
              <li className="flex gap-3">
                <strong>Actionable Mitigation:</strong> Verify routing loops or latency patterns per ISP in the affected region, or check for regional CDN routing updates.
              </li>
            </ul>
          </div>
        )
      }

    case 'tail_latency':
      return {
        title: 'Tail Latency Anomaly',
        icon: <TrendingUp className="h-5 w-5 text-primary" />,
        fields: ['url', 'elapsed', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Uncovers endpoints experiencing a severe &quot;long tail&quot; latency distribution, where P99 response time is more than 5× higher than P50 response time.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <TrendingUp className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>Outlier Response Distribution:</strong> Identifies routes where most requests are served quickly, but a small percentage of users experience extreme, frustrating delays.
                </span>
              </li>
              <li className="flex gap-3">
                <Clock className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>Resource Contention:</strong> Usually caused by backend locking issues, memory garbage collection pauses, un-indexed database queries, or occasional huge payloads.
                </span>
              </li>
              <li className="flex gap-3">
                <Info className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Profile the slow P99 requests to check for resource locks or database table scans, and optimize heavy query paths.
                </span>
              </li>
            </ul>
          </div>
        )
      }

    default:
      return null
  }
}

function getContent(id: string): InsightContent {
  return (
    getSecurityContent(id) ||
    getCacheContent(id) ||
    getErrorsContent(id) ||
    getTrafficContent(id) ||
    getPerformanceContent(id) ||
    getOptimizationContent(id) ||
    getDefaultContent()
  )
}

export function InsightHelpModal({ insightId, isOpen, onOpenChange }: InsightHelpModalProps) {
  const content = getContent(insightId)
  if (!content) return null

  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl p-6 md:p-8 overflow-y-auto max-h-[90vh]">
        <DialogHeader>
          <DialogTitle className="text-xl flex items-center gap-2">
            {content.icon}
            {content.title}
          </DialogTitle>
          <DialogDescription className="sr-only">
            What this insight measures and the log fields it relies on.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-6 mt-2">
          {content.diagram && content.diagram}

          <div className="text-sm text-muted-foreground leading-relaxed">
            {content.description}
          </div>

          {content.fields.length > 0 && (
            <div className="bg-muted/50 p-4 rounded-lg border">
              <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-2">
                <Globe className="h-4 w-4" /> Required Log Fields
              </h4>
              <div className="flex flex-wrap gap-2">
                {content.fields.map(f => (
                  <Badge key={f} variant="outline" className="font-mono bg-background">{f}</Badge>
                ))}
              </div>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}
