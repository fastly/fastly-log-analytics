import React from 'react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Badge } from '@/components/ui/badge'
import { 
  Server, 
  User, 
  Zap, 
  ShieldAlert, 
  Globe, 
  Activity, 
  MapPin, 
  Fingerprint, 
  Search, 
  AlertTriangle, 
  WifiOff, 
  Clock, 
  TrendingDown,
  TrendingUp,
  Lock,
  Info,
  Building2,
  Database,
  BarChart,
  Network
} from 'lucide-react'
import { cn } from '@/lib/utils'

interface InsightHelpModalProps {
  insightId: string
  isOpen: boolean
  onOpenChange: (open: boolean) => void
}

interface InsightContent {
  title: string
  icon: React.ReactNode
  description: React.ReactNode
  diagram?: React.ReactNode
  fields: string[]
}

export function InsightHelpModal({ insightId, isOpen, onOpenChange }: InsightHelpModalProps) {
  const getContent = (id: string): InsightContent | null => {
    switch (id) {
      case 'impossible_distance':
        return {
          title: 'The Physics of "Impossible Distance"',
          icon: <ShieldAlert className="h-5 w-5 text-primary" />,
          fields: ['client_ip', 'pop', 'lat', 'lon', 'tcp_rtt'],
          description: (
            <div className="space-y-4">
              <p>This insight acts as a <strong>physics check</strong> to detect users spoofing their location via VPNs, proxies, or private relays.</p>
              <ul className="space-y-3 list-none pl-0">
                <li className="flex gap-3">
                  <MapPin className="h-5 w-5 shrink-0 text-blue-500" />
                  <span><strong>The Claim:</strong> We check the geographical location the user's IP address claims to be from, and calculate the distance to the exact Fastly datacenter they connected to.</span>
                </li>
                <li className="flex gap-3">
                  <Activity className="h-5 w-5 shrink-0 text-yellow-500" />
                  <span><strong>The Reality:</strong> Data travels through fiber optic cables at roughly 200,000 km/s. Using the exact <code>TCP RTT</code> (Network Latency), we calculate the absolute maximum distance the client could physically be from the server.</span>
                </li>
                <li className="flex gap-3">
                  <ShieldAlert className="h-5 w-5 shrink-0 text-red-500" />
                  <span><strong>The Catch:</strong> We double the theoretical limit to account for network routing. If their claimed location is still further away than the speed of light allows, they are spoofing their location.</span>
                </li>
              </ul>
            </div>
          ),
          diagram: (
            <div className="bg-muted/30 p-6 rounded-xl border">
              <div className="flex justify-between items-start relative z-10">
                <div className="absolute top-[44px] left-[100px] right-[100px] h-0.5 border-t-2 border-dashed border-primary/30 z-[-1]" />
                <div className="flex flex-col items-center gap-2 bg-background p-3 rounded-xl border shadow-sm w-32 relative">
                  <div className="h-10 w-10 rounded-full bg-blue-500/10 flex items-center justify-center shrink-0">
                    <User className="h-5 w-5 text-blue-500" />
                  </div>
                  <span className="text-xs font-bold text-center">Client IP</span>
                  <span className="text-[10px] text-muted-foreground text-center">Claimed: Sydney<br/>(13,000 km)</span>
                </div>
                <div className="flex flex-col items-center bg-background/80 backdrop-blur-sm p-2 rounded-lg mt-3 relative">
                  <Zap className="h-5 w-5 text-yellow-500 mb-1" />
                  <Badge variant="secondary" className="text-[10px] font-mono mb-1">TCP RTT: 20ms</Badge>
                  <span className="text-[10px] font-bold text-foreground mt-1">Max Physical: 2,000 km</span>
                </div>
                <div className="flex flex-col items-center gap-2 bg-background p-3 rounded-xl border shadow-sm w-32 relative">
                  <div className="h-10 w-10 rounded-full bg-green-500/10 flex items-center justify-center shrink-0">
                    <Server className="h-5 w-5 text-green-500" />
                  </div>
                  <span className="text-xs font-bold text-center">Fastly POP</span>
                  <span className="text-[10px] text-muted-foreground text-center">Seattle, WA</span>
                </div>
              </div>
              <div className="mt-6 flex items-center justify-center gap-2 text-red-600 dark:text-red-400 bg-red-500/10 py-2.5 rounded-lg border border-red-500/20">
                <ShieldAlert className="h-4 w-4 shrink-0" />
                <span className="text-xs font-bold uppercase tracking-wider">Speed of Light Violation Detected</span>
              </div>
            </div>
          )
        }

      case 'ua_monoculture':
        return {
          title: 'User-Agent Monoculture Analysis',
          icon: <Fingerprint className="h-5 w-5 text-primary" />,
          fields: ['ua'],
          description: (
            <div className="space-y-4">
              <p>Identifies when a single User-Agent suddenly accounts for a disproportionate share of your traffic compared to your historical baseline.</p>
              <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
                <li className="flex gap-3">
                  <Activity className="h-5 w-5 shrink-0 text-blue-500" />
                  <span><strong>Baseline Comparison:</strong> We calculate the "normal" percentage of traffic for each User-Agent over your selected baseline period.</span>
                </li>
                <li className="flex gap-3">
                  <Fingerprint className="h-5 w-5 shrink-0 text-yellow-500" />
                  <span><strong>Anomaly Detection:</strong> We flag any User-Agent that has jumped significantly (e.g., from 2% to 40% of total traffic) in the current window.</span>
                </li>
                <li className="flex gap-3">
                  <AlertTriangle className="h-5 w-5 shrink-0 text-orange-500" />
                  <span><strong>Security Risk:</strong> Automated bot waves often hit with a single, static User-Agent before rotating to another. This is a high-signal indicator of a scraper or credential stuffing attempt.</span>
                </li>
              </ul>
            </div>
          )
        }

      case 'new_probe_urls':
        return {
          title: 'New Probe URL Detection',
          icon: <Search className="h-5 w-5 text-primary" />,
          fields: ['url'],
          description: (
            <div className="space-y-4">
              <p>Flags requests to "sensitive" paths that have never appeared in your logs before today.</p>
              <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
                <li className="flex gap-3">
                  <Clock className="h-5 w-5 shrink-0 text-blue-500" />
                  <span><strong>Zero-Trust History:</strong> We maintain a bloom filter/index of every unique URL ever requested on your service.</span>
                </li>
                <li className="flex gap-3">
                  <Search className="h-5 w-5 shrink-0 text-yellow-500" />
                  <span><strong>Vulnerability Patterns:</strong> We specifically look for "new" URLs containing patterns like <code>/admin</code>, <code>.env</code>, <code>wp-login.php</code>, or <code>config.json</code>.</span>
                </li>
                <li className="flex gap-3">
                  <ShieldAlert className="h-5 w-5 shrink-0 text-red-500" />
                  <span><strong>Intent:</strong> These are almost exclusively automated scanners looking for misconfigured servers or unpatched vulnerabilities.</span>
                </li>
              </ul>
            </div>
          )
        }

      case 'waf_signal_spikes':
        return {
          title: 'WAF Signal Spikes',
          icon: <ShieldAlert className="h-5 w-5 text-primary" />,
          fields: ['waf_sig'],
          description: (
            <div className="space-y-4">
              <p>Monitors Next-Gen WAF (NGWAF) signals for sudden increases in attack patterns like SQL Injection or Cross-Site Scripting.</p>
              <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
                <li className="flex gap-3">
                  <Activity className="h-5 w-5 shrink-0 text-blue-500" />
                  <span><strong>Statistical Baseline:</strong> We track the rate of every WAF signal (SQLI, XSS, CMDEXE, etc) over the selected historical baseline.</span>
                </li>
                <li className="flex gap-3">
                  <Zap className="h-5 w-5 shrink-0 text-yellow-500" />
                  <span><strong>Spike Detection:</strong> Flags any signal where the current frequency is at least 3x higher than the baseline average.</span>
                </li>
              </ul>
            </div>
          )
        }

      case 'cache_collapse':
        return {
          title: 'Cache Efficiency Collapse',
          icon: <WifiOff className="h-5 w-5 text-primary" />,
          fields: ['cache', 'url'],
          description: (
            <div className="space-y-4">
              <p>Detects URLs where the Cache Hit Ratio (CHR) has dropped dramatically, potentially causing an "origin fire."</p>
              <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
                <li className="flex gap-3">
                  <TrendingDown className="h-5 w-5 shrink-0 text-red-500" />
                  <span><strong>Efficiency Drop:</strong> Flags URLs that previously had &gt;80% CHR but have suddenly dropped to &lt;20%.</span>
                </li>
                <li className="flex gap-3">
                  <Server className="h-5 w-5 shrink-0 text-blue-500" />
                  <span><strong>Origin Impact:</strong> This usually indicates a change in query parameters (cache busting) or a deployment that accidentally disabled caching for a hot route.</span>
                </li>
              </ul>
            </div>
          )
        }

      case 'proxy_surge':
        return {
          title: 'Anonymizing Proxy Surge',
          icon: <Lock className="h-5 w-5 text-primary" />,
          fields: ['p_type'],
          description: (
            <div className="space-y-4">
              <p>Identifies a sudden increase in traffic originating from VPNs, Tor exit nodes, or public proxies.</p>
              <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
                <li className="flex gap-3">
                  <Globe className="h-5 w-5 shrink-0 text-blue-500" />
                  <span><strong>Proxy Metadata:</strong> Powered by Fastly's real-time Geolocation metadata which identifies the "type" of IP address (hosting, vpn, proxy, tor).</span>
                </li>
                <li className="flex gap-3">
                  <TrendingUp className="h-5 w-5 shrink-0 text-yellow-500" />
                  <span><strong>Volume Check:</strong> We flag when these "anonymous" traffic types suddenly account for a larger-than-normal percentage of your overall requests.</span>
                </li>
              </ul>
            </div>
          )
        }

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
                  <span><strong>Slowdown Detection:</strong> Triggers when a city's P95 latency doubles or triples, often indicating congestion at a specific edge node or peering point.</span>
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
                  <span><strong>Age vs TTL:</strong> We analyze cache misses and compare the object's expected TTL against the time since it was last fetched.</span>
                </li>
                <li className="flex gap-3">
                  <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                  <span><strong>Capacity Warning:</strong> High rates of premature eviction mean your Fastly service is under "Cache Pressure" and objects are being pushed out of memory to make room for new ones. You may need to increase your Cache Reservation.</span>
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

      case 'botnet_grouping':
        return {
          title: 'Botnet Fingerprinting',
          icon: <Fingerprint className="h-5 w-5 text-primary" />,
          fields: ['ip', 'ua', 'ja4'],
          description: (
            <div className="space-y-4">
              <p>Groups suspicious traffic by combining multiple identifiers (IP, User-Agent, and JA4 TLS Fingerprints) to identify coordinated botnets.</p>
              <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
                <li className="flex gap-3">
                  <Lock className="h-5 w-5 shrink-0 text-blue-500" />
                  <span><strong>JA4 Fingerprints:</strong> We look beyond just IPs (which can be rotated easily) to TLS negotiation patterns, which reliably identify the underlying software/script being used by the attacker.</span>
                </li>
                <li className="flex gap-3">
                  <ShieldAlert className="h-5 w-5 shrink-0 text-red-500" />
                  <span><strong>Distributed Attacks:</strong> Uncovers the true size of an attack by linking thousands of seemingly unrelated IPs that are all using the exact same custom scripting tool.</span>
                </li>
              </ul>
            </div>
          )
        }

      case 'low_and_slow':
        return {
          title: 'Low & Slow Attack Detection',
          icon: <Search className="h-5 w-5 text-primary" />,
          fields: ['ip', 'url', 'ua'],
          description: (
            <div className="space-y-4">
              <p>Detects stealthy, distributed attacks where individual IPs stay below traditional rate-limiting thresholds.</p>
              <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
                <li className="flex gap-3">
                  <Clock className="h-5 w-5 shrink-0 text-blue-500" />
                  <span><strong>Time Analysis:</strong> We analyze the time span between requests from the same IP, looking for unnaturally consistent or deliberately spaced intervals.</span>
                </li>
                <li className="flex gap-3">
                  <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                  <span><strong>Evasion Tactics:</strong> Flags traffic that generates a small but steady stream of errors over hours or days, bypassing standard WAF velocity rules.</span>
                </li>
              </ul>
            </div>
          )
        }

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
  }

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
