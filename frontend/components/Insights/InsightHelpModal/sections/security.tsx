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
                <span><strong>Proxy Metadata:</strong> Powered by Fastly's real-time Geolocation metadata which identifies the "type" of IP address (hosting, vpn, proxy, tor).</span>
              </li>
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Volume Check:</strong> We flag when these "anonymous" traffic types suddenly account for a larger-than-normal percentage of your overall requests.</span>
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

    default:
      return null
  }
}
