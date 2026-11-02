import React from 'react'
import {
  Activity,
  AlertTriangle,
  Bot,
  Clock,
  Fingerprint,
  Globe,
  KeyRound,
  Lock,
  Search,
  Server,
  ShieldAlert,
  User,
} from 'lucide-react'
import type { InsightContent } from '../../types'

export function getAdvancedSecurityContent(id: string): InsightContent | null {
  switch (id) {
    case 'repeated_patterns':
      return {
        title: 'Scripted Traffic Patterns',
        icon: <Bot className="h-5 w-5 text-primary" />,
        fields: ['ip', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>Flags IPs sending requests on a near-constant cadence — automated scrapers, pollers, or cron-scheduled scripts that evade volumetric rate limits by staying slow.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Clock className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Cadence, not volume:</strong> We measure the time between consecutive requests from each IP. Human traffic is bursty and irregular; scripts fire on a metronome.</span>
              </li>
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Two robust signals:</strong> a Sheppard-corrected coefficient of variation (how much the gaps jitter) plus modal dominance (the share of gaps that are <em>exactly</em> identical). Low jitter and a high identical-gap share together score as machine-driven.</span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>Below the radar:</strong> these clients deliberately stay under volumetric rate limits (we exclude bursts of ≥2 req/s), so velocity rules never trip — the classic signature of scrapers, pollers, and cron jobs.</span>
              </li>
              <li className="flex gap-3">
                <Search className="h-5 w-5 shrink-0 text-emerald-500" />
                <span><strong>Per-IP evidence:</strong> click the magnifier on any flagged row to see exactly why it was flagged — the regularity score, mean interval, jitter, modal gap, and request volume.</span>
              </li>
            </ul>
          </div>
        ),
        diagram: (
          <div className="bg-muted/30 p-6 rounded-xl border space-y-5">
            <div className="space-y-1.5">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold flex items-center gap-1.5"><Bot className="h-3.5 w-3.5 text-red-500" /> Automated script</span>
                <span className="text-[10px] text-muted-foreground font-mono">even gaps · ~0 jitter</span>
              </div>
              <div className="relative h-6 rounded-md bg-background border overflow-hidden" aria-hidden="true">
                {[6, 19, 32, 45, 58, 71, 84, 94].map((l) => (
                  <span key={l} className="absolute top-1 bottom-1 w-0.5 bg-red-500" style={{ left: `${l}%` }} />
                ))}
              </div>
            </div>
            <div className="space-y-1.5">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold flex items-center gap-1.5"><User className="h-3.5 w-3.5 text-blue-500" /> Human browsing</span>
                <span className="text-[10px] text-muted-foreground font-mono">bursty · irregular</span>
              </div>
              <div className="relative h-6 rounded-md bg-background border overflow-hidden" aria-hidden="true">
                {[4, 9, 12, 28, 33, 57, 79, 84, 92].map((l) => (
                  <span key={l} className="absolute top-1 bottom-1 w-0.5 bg-blue-500/70" style={{ left: `${l}%` }} />
                ))}
              </div>
            </div>
          </div>
        )
      }

    case 'repeated_patterns_fp':
      return {
        title: 'Scripted Traffic Patterns (by TLS Fingerprint)',
        icon: <Bot className="h-5 w-5 text-primary" />,
        fields: ['ip', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>Flags TLS fingerprints (JA3/JA4) sending requests on a near-constant cadence across multiple source IPs — automated scrapers, pollers, or cron-scheduled scripts that rotate IP addresses to evade per-IP detection.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Fingerprint className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Fingerprint-keyed:</strong> instead of grouping by client IP, we group by TLS fingerprint. A scraper rotating through proxies or VPNs changes IP on every request, but its TLS stack (cipher suites, extensions, curves) stays the same.</span>
              </li>
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Same statistical engine:</strong> the cadence regularity algorithm is identical to the IP variant — Sheppard-corrected coefficient of variation plus modal dominance — just applied to the fingerprint&apos;s aggregate traffic instead of a single IP&apos;s.</span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>Catches IP rotation:</strong> a single IP sending periodic requests is caught by the standard insight. This variant catches the complementary case: the script rotates IPs, but keeps the same TLS fingerprint and cadence.</span>
              </li>
              <li className="flex gap-3">
                <Search className="h-5 w-5 shrink-0 text-emerald-500" />
                <span><strong>Per-fingerprint evidence:</strong> click the magnifier on any flagged row to see the regularity score, mean interval, jitter, modal gap, and the number of distinct source IPs behind that fingerprint.</span>
              </li>
            </ul>
          </div>
        ),
        diagram: (
          <div className="bg-muted/30 p-6 rounded-xl border space-y-5">
            <div className="space-y-1.5">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold flex items-center gap-1.5"><Bot className="h-3.5 w-3.5 text-red-500" /> Automated script</span>
                <span className="text-[10px] text-muted-foreground font-mono">even gaps · ~0 jitter</span>
              </div>
              <div className="relative h-6 rounded-md bg-background border overflow-hidden" aria-hidden="true">
                {[6, 19, 32, 45, 58, 71, 84, 94].map((l) => (
                  <span key={l} className="absolute top-1 bottom-1 w-0.5 bg-red-500" style={{ left: `${l}%` }} />
                ))}
              </div>
            </div>
            <div className="space-y-1.5">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold flex items-center gap-1.5"><User className="h-3.5 w-3.5 text-blue-500" /> Human browsing</span>
                <span className="text-[10px] text-muted-foreground font-mono">bursty · irregular</span>
              </div>
              <div className="relative h-6 rounded-md bg-background border overflow-hidden" aria-hidden="true">
                {[4, 9, 12, 28, 33, 57, 79, 84, 92].map((l) => (
                  <span key={l} className="absolute top-1 bottom-1 w-0.5 bg-blue-500/70" style={{ left: `${l}%` }} />
                ))}
              </div>
            </div>
          </div>
        )
      }

    case 'session_harvesting':
      return {
        title: 'Session-ID Harvesting',
        icon: <KeyRound className="h-5 w-5 text-primary" />,
        fields: ['ip', 'cookie_session'],
        description: (
          <div className="space-y-4">
            <p>Flags a single IP presenting a large, spiking number of <em>distinct</em> session cookies — the fingerprint of session-token brute forcing, cookie replay, or credential stuffing that mints a fresh session per attempt.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Fingerprint className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Hashed at the edge:</strong> the session cookie is SHA-256 hashed at the true edge before it ever reaches storage — we only ever <em>count distinct</em> hashes per IP, never store or show a raw session token.</span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Rotation spike:</strong> a normal client reuses one session for many requests. An IP cycling through dozens of distinct sessions in the window, far above its baseline, is enumerating or replaying tokens.</span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>Privacy:</strong> the client IP on this card is masked for analysts (like every IP-keyed insight), and the session hash itself is never surfaced.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'crawler_disparity':
      return {
        title: 'Bot & Crawler Referral & Bandwidth Disparity',
        icon: <Bot className="h-5 w-5 text-primary" />,
        fields: ['ua', 'referer', 'ip', 'resp_bytes', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Identifies bots, scrapers, and search/AI crawlers consuming high request volume and egress bandwidth (<code>resp_bytes</code>) while driving negligible human referral traffic back to your platform.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Bot className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>Requests &amp; Bandwidth Cost:</strong> Automated crawlers, AI training scrapers, headless browsers, and programmatic HTTP clients consume significant egress bandwidth (<code>resp_bytes</code>) and origin resources.
                </span>
              </li>
              <li className="flex gap-3">
                <Search className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>Negligible Referral Traffic:</strong> Genuine search or AI answer engines drive human referral clicks (where User-Agent is human and HTTP <code>Referer</code> matches the platform domain). When crawls-per-referral or bytes-per-referral is high, the bot is extracting content without returning organic value.
                </span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Click the magnifier on any flagged row to inspect total bandwidth, site egress share, request count, and referred human IPs, then throttle or challenge zero-referral scrapers at the Fastly CDN edge.
                </span>
              </li>
            </ul>
          </div>
        )
      }

    case 'stale_browser_version':
      return {
        title: 'Stale Headless Browser',
        icon: <Fingerprint className="h-5 w-5 text-primary" />,
        fields: ['ua', 'ip', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Detects anomalous clusters of request traffic using stale Chrome major versions, a classic signature of un-updated Puppeteer/Playwright headless bots.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>Outdated User-Agents:</strong> Real human users naturally auto-update to the latest Chrome major versions. In contrast, automated scraping farms and vulnerability scanners typically run inside static Docker containers configured with older, pinned browser versions.
                </span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>High-Signal Threat:</strong> A sudden spike in requests using old major releases (e.g. Chrome 100) indicates coordinated automated scanning or content scraping.
                </span>
              </li>
              <li className="flex gap-3">
                <Lock className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Set up edge challenge policies (e.g., CAPTCHA or JS challenge) for requests matching extremely outdated browser versions, or use client-side fingerprinting to verify the TLS stack.
                </span>
              </li>
            </ul>
          </div>
        )
      }

    case 'orphaned_deep_crawl':
      return {
        title: 'Orphaned Deep Crawl',
        icon: <Search className="h-5 w-5 text-primary" />,
        fields: ['ip', 'url', 'referer', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Flags specific client IP addresses that are performing deep, direct scans of nested catalog or resource database routes with empty HTTP referer headers.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Globe className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>Direct Link Traversals:</strong> Human browsing naturally flows from landing pages to categories and finally to items, generating consecutive HTTP <code>Referer</code> headers. Scrapers and API harvesters bypass these pathways, hitting nested resource links directly.
                </span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>Pricing & Inventory Scraping:</strong> High counts of direct catalog requests with empty referers strongly signal automated inventory, pricing, or catalog scraping.
                </span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Set up Fastly rate limits or challenge triggers targeting direct traversals of nested resource pages that lack referer headers.
                </span>
              </li>
            </ul>
          </div>
        )
      }

    case 'residential_fingerprint_dispersion':
      return {
        title: 'Residential Fingerprint Dispersion',
        icon: <ShieldAlert className="h-5 w-5 text-primary" />,
        fields: ['ja4', 'ip', 'asn', 'p_type', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Identifies identical client TLS/TCP handshakes (JA4/JA3) distributed across multiple residential ASNs, signaling rotated botnet scans evading per-IP limits.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Fingerprint className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>TLS Handshake Tracking:</strong> Traditional per-IP blocks are ineffective against botnets using residential proxy pools (rotating through home IPs). However, the botnet client&apos;s TLS stack (ciphers, curves, extensions) remains static, producing identical JA4 handshakes.
                </span>
              </li>
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>ASN Dispersion:</strong> Finding the exact same JA4 handshake footprint operating concurrently across many distinct residential ASNs is statistically impossible for real users and confirms a rotating botnet.
                </span>
              </li>
              <li className="flex gap-3">
                <Lock className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Rate-limit or block the specific JA4 handshake fingerprint directly at the edge, rendering their rotating IP proxy pool completely ineffective.
                </span>
              </li>
            </ul>
          </div>
        )
      }

    case 'cipher_spread':
      return {
        title: 'Cipher Fingerprint Clustering',
        icon: <Fingerprint className="h-5 w-5 text-primary" />,
        fields: ['tls_ciphers_sha', 'ip', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Flags client TLS cipher suites being used by an unusually large and spiking number of distinct IP addresses.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Fingerprint className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>Unique TLS Signature:</strong> Client TLS libraries present a specific list and order of cipher suites during the TLS handshake. A coordinated botnet or scraping application built using a specific library (e.g. Go, Python HTTP client, older OpenSSL version) exhibits the exact same TLS cipher footprint.
                </span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>Coordinated Attack Signature:</strong> If a rare or specific cipher suite list suddenly experiences a massive spike in unique source IPs, it confirms a highly distributed, coordinated bot campaign.
                </span>
              </li>
              <li className="flex gap-3">
                <Lock className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Create custom edge challenge rules or ACL blocks matching the flagged cipher list or JA3/JA4 fingerprint representing the bot cluster.
                </span>
              </li>
            </ul>
          </div>
        )
      }

    case 'connection_abuse':
      return {
        title: 'Connection Reuse Anomaly',
        icon: <Activity className="h-5 w-5 text-primary" />,
        fields: ['ip', 'conn_requests', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Detects client IP addresses making an exceptionally high number of HTTP requests over a single TCP connection, well beyond the standard baseline.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Server className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>Connection Reuse Patterns:</strong> Standard web browsers naturally reuse TCP connections (keep-alive) but limit requests per connection (usually 10 to 100) before opening new ones or refreshing. Automated scripts, scraper pipelines, or DoS tools bypass this, pushing thousands of requests over a single persistent TCP tunnel.
                </span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>Infrastructure Strain:</strong> Connection reuse abuse can saturate origin threads and keep CDN/WAF resources open indefinitely, leading to resource starvation on upstream application servers.
                </span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Set maximum request limits per connection in your origin or CDN configuration, challenging or disconnecting clients exceeding standard browser thresholds.
                </span>
              </li>
            </ul>
          </div>
        )
      }

    case 'request_size_anomaly':
      return {
        title: 'Oversized Request Headers',
        icon: <AlertTriangle className="h-5 w-5 text-primary" />,
        fields: ['ip', 'req_header_bytes', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Flags client IPs transmitting HTTP request headers that are significantly larger than your baseline distribution, signaling potential DoS or data exfiltration.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Lock className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>Header Size Abuse:</strong> Extremely large request headers (e.g. bloated Cookie fields, custom authorization payload stuffing) can trigger buffer overflows or crash upstream web servers (known as HTTP Request Smuggling or Slowloris variants).
                </span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>Data Exfiltration/Exploitation:</strong> Malicious actors also pack structured data or exploit payloads into request headers to bypass standard URL-based WAF rule checks.
                </span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Enforce strict request header size limits (e.g., maximum 8KB or 16KB total) at the Fastly edge, rejecting oversized headers with an immediate HTTP 400 or 431 Bad Request.
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
