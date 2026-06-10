import React from 'react'
import { Shield, Fingerprint, Scale, Globe, Network, Repeat, Bot, CheckCircle2, AlertTriangle, Clock, HelpCircle } from 'lucide-react'

export type NgwafVerifiedBot = {
  bot_name?: string
  category?: string
  request_count?: number
  [key: string]: any
}

export const FINGERPRINT_COLUMN_IDS = ['fingerprint', 'ip_count', 'request_count']
export const TOP_IP_COLUMN_IDS = ['ip', 'max_header']
export const BOT_COLUMN_IDS = ['name', 'category', 'request_count', 'verified_count', 'impersonator_count', 'unverified_count', 'pending_count']
export const NGWAF_BOT_COLUMN_IDS = ['bot_name', 'category', 'request_count']

export const SECURITY_INFO = {
  wellknown_bots: {
    title: 'Well-Known Bots',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>Detects bot traffic based on a continuously updated database of well-known User-Agent patterns and verifies them using FCrDNS and CIDR matches.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <CheckCircle2 className="h-5 w-5 shrink-0 text-green-500" />
            <span><strong>Verified:</strong> The IP address matched the official CIDR block or passed Forward-Confirmed reverse DNS for the bot's known domains.</span>
          </li>
          <li className="flex gap-3">
            <AlertTriangle className="h-5 w-5 shrink-0 text-red-500" />
            <span><strong>Spoofed:</strong> The request claimed to be this bot in the User-Agent, but the IP failed verification. Highly likely to be malicious scrapers or scammers.</span>
          </li>
          <li className="flex gap-3">
            <HelpCircle className="h-5 w-5 shrink-0 text-muted-foreground" />
            <span><strong>Unverified:</strong> The bot source does not provide official IPs or domains for verification.</span>
          </li>
          <li className="flex gap-3">
            <Clock className="h-5 w-5 shrink-0 text-yellow-500" />
            <span><strong>Pending:</strong> The reverse DNS lookup is still pending in the background. Check back soon.</span>
          </li>
        </ul>
      </div>
    )
  },
  fingerprints: {
    title: 'Top TLS Fingerprints',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>Identifies groups of traffic sharing the exact same TLS negotiation parameters (cipher suites, extensions), often indicating the same underlying software or script.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <Fingerprint className="h-5 w-5 shrink-0 text-blue-500" />
            <span><strong>Botnet Detection:</strong> IP addresses change frequently, but the custom scripting tools attackers use rarely change their TLS handshakes. A single fingerprint spread across thousands of IPs usually indicates a coordinated botnet.</span>
          </li>
        </ul>
      </div>
    )
  },
  req_size: {
    title: 'Request Header Size Distribution',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>A histogram showing the distribution of HTTP request header sizes across your traffic.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <Scale className="h-5 w-5 shrink-0 text-blue-500" />
            <span><strong>Anomaly Detection:</strong> Normal web requests have header sizes between 500 bytes and 2KB. Spikes in the 8KB+ range can indicate buffer overflow attempts or overly aggressive cookie stuffing.</span>
          </li>
        </ul>
      </div>
    )
  },
  top_ips_header: {
    title: 'Oversized Request Headers',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>Highlights specific IP addresses sending the largest request headers.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <Shield className="h-5 w-5 shrink-0 text-yellow-500" />
            <span><strong>Investigation:</strong> Helps isolate the source of oversized requests seen in the distribution chart. These IPs may be malfunctioning clients or malicious actors probing for vulnerabilities.</span>
          </li>
        </ul>
      </div>
    )
  },
  ipv6: {
    title: 'IPv6 Adoption over Time',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>Tracks the percentage of requests connecting to Fastly via IPv6 vs IPv4.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <Globe className="h-5 w-5 shrink-0 text-blue-500" />
            <span><strong>Infrastructure Readiness:</strong> Sudden drops in IPv6 traffic might indicate an ISP routing failure or a DNS configuration issue dropping AAAA records.</span>
          </li>
        </ul>
      </div>
    )
  },
  proxy: {
    title: 'Proxy/Anonymizer Breakdown',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>Categorizes traffic by the underlying network type, using Fastly's geolocation intelligence.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <Network className="h-5 w-5 shrink-0 text-yellow-500" />
            <span><strong>Traffic Quality:</strong> A high percentage of traffic from 'hosting' or 'tor' categories is a strong indicator of non-human traffic, scraping, or evasion attempts.</span>
          </li>
        </ul>
      </div>
    )
  },
  conn_reuse: {
    title: 'Connection Reuse',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>Shows how many HTTP requests are made over a single TCP connection.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <Repeat className="h-5 w-5 shrink-0 text-blue-500" />
            <span><strong>Efficiency:</strong> High reuse (10+ requests per connection) is highly efficient and typical for browsers loading a webpage. A spike in '1' (no reuse) means connections are constantly being torn down, which is typical of basic scraping tools or misconfigured API clients.</span>
          </li>
        </ul>
      </div>
    )
  },
  ngwaf_bots: {
    title: 'Verified Bots (NGWAF)',
    body: (
      <div className="space-y-4 text-sm text-muted-foreground">
        <p>Shows named bots identified by Fastly NGWAF. By definition, all traffic matching these signals has been verified by Fastly's Signal Sciences engine.</p>
        <ul className="space-y-3 list-none pl-0">
          <li className="flex gap-3">
            <Bot className="h-5 w-5 shrink-0 text-blue-500" />
            <span><strong>Bot Name:</strong> The verified bot name extracted from the NGWAF VERIFIED-BOT signal (e.g. "OpenAI SearchBot").</span>
          </li>
        </ul>
      </div>
    )
  }
}
