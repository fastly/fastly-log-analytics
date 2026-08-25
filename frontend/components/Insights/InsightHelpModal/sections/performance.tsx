import React from 'react'
import {
  Zap,
  Activity,
  Clock,
  TrendingUp,
  Info,
  Network,
} from 'lucide-react'
import type { InsightContent } from '../types'

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
