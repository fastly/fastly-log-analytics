import React from 'react'
import { Badge } from '@/components/ui/badge'
import {
  Server,
  User,
  Zap,
  ShieldAlert,
  Activity,
  MapPin,
  Fingerprint,
  Search,
  AlertTriangle,
  Clock,
  Lock,
  Bot,
  KeyRound,
} from 'lucide-react'
import type { InsightContent } from '../types'

export function getSecurityContent(id: string): InsightContent | null {
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
                <span><strong>The Claim:</strong> We check the geographical location the user&apos;s IP address claims to be from, and calculate the distance to the exact Fastly datacenter they connected to.</span>
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
                <span><strong>Baseline Comparison:</strong> We calculate the &quot;normal&quot; percentage of traffic for each User-Agent over your selected baseline period.</span>
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
            <p>Flags requests to &quot;sensitive&quot; paths that have never appeared in your logs before today.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Clock className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Zero-Trust History:</strong> We maintain a bloom filter/index of every unique URL ever requested on your service.</span>
              </li>
              <li className="flex gap-3">
                <Search className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Vulnerability Patterns:</strong> We specifically look for &quot;new&quot; URLs containing patterns like <code>/admin</code>, <code>.env</code>, <code>wp-login.php</code>, or <code>config.json</code>.</span>
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
                <MapPin className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Proxy Metadata:</strong> Powered by Fastly&apos;s real-time Geolocation metadata which identifies the &quot;type&quot; of IP address (hosting, vpn, proxy, tor).</span>
              </li>
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Volume Check:</strong> We flag when these &quot;anonymous&quot; traffic types suddenly account for a larger-than-normal percentage of your overall requests.</span>
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

    case 'credential_enumeration':
      return {
        title: 'Credential Enumeration / Brute Force',
        icon: <KeyRound className="h-5 w-5 text-primary" />,
        fields: ['ip', 'url', 'status'],
        description: (
          <div className="space-y-4">
            <p>Detects spikes of authentication failures (<code>401</code> / <code>403</code>) concentrated on login and identity paths — the fingerprint of credential stuffing and brute-force attempts.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <KeyRound className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Auth path focus:</strong> We isolate requests to sensitive endpoints such as <code>/login</code>, <code>/auth</code>, <code>/oauth</code>, and <code>/password-reset</code>, where a burst of rejected credentials is meaningful rather than noise.</span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Failure-rate spike:</strong> We flag when the volume of <code>401</code>/<code>403</code> responses on those paths jumps well above the historical baseline — a single IP hammering a login, or many IPs replaying leaked credential lists.</span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>Intent:</strong> Attackers cycle through username/password pairs looking for a valid one. A sudden cluster of auth failures is the earliest signal of an account-takeover campaign in progress.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'content_discovery':
      return {
        title: 'Content-Discovery Scanning',
        icon: <Search className="h-5 w-5 text-primary" />,
        fields: ['ip', 'url', 'status'],
        description: (
          <div className="space-y-4">
            <p>Detects a single IP generating a burst of <code>404 Not Found</code> responses across many <em>distinct</em> URLs — the fingerprint of directory and endpoint enumeration probing for hidden or vulnerable paths.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Search className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Breadth, not one broken link:</strong> We require many distinct 404 URLs from the same IP, so a single stale asset or a bad deploy won&apos;t trip the card — only a client sweeping the URL space does.</span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>404-dominated traffic:</strong> We flag IPs whose window traffic is overwhelmingly 404s, the signature of a scanner walking a wordlist rather than a user browsing real pages.</span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>Intent:</strong> Attackers enumerate paths (<code>/.git</code>, <code>/admin</code>, backup files, API routes) to find something the app never meant to expose. A 404 sweep is reconnaissance that usually precedes a targeted exploit.</span>
              </li>
            </ul>
          </div>
        )
      }

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

    default:
      return null
  }
}
