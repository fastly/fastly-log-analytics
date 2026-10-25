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
} from 'lucide-react'
import type { InsightContent, InsightHelpModalProps } from '../shared'
import { getSecurityContent } from './sections/security'
import { getPerformanceContent } from './sections/performance'

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
