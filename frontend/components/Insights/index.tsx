'use client'

import React, { useEffect, useMemo, useRef, useState } from 'react'
import { Badge } from '@/components/ui/badge'
import { Server, User, Zap, ShieldAlert, Activity, MapPin, Fingerprint, Search, AlertTriangle, Clock, Lock, Bot, KeyRound, Globe, AlertCircle, ArrowUpRight, CheckCircle2, FileText, HardDrive, Hash, Info, Loader2, Scale, ScanSearch, Users, CheckCircle, HelpCircle, Gauge, Timer, Repeat, TrendingUp, TrendingDown, Minus, MapIcon, LineChart, Lightbulb, Filter, RefreshCw } from 'lucide-react'
import Link from 'next/link'
import * as maplibregl from 'maplibre-gl'
import { useTheme } from 'next-themes'
import { useQuery, keepPreviousData, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { TimeSeriesChart } from '@/components/AnalyticsCard'
import { DataTableReadonly } from '@/components/DataTable'
import { addCountryBaseLayer, updateCountryBaseLayerTheme, useMapResize } from '@/components/Map/ChoroplethMap'
import { client, extractApiError, unwrapClient, queryKeys } from '@/lib/api'
import { CHART_COLORS, formatBytes, formatCount, statusBadgeClass, cn, keyByOccurrence, formatCountMaybe, formatDuration, WINDOW_OPTIONS, BASELINE_OPTIONS, historyHoursFromExtents, normalizeTabKey, SECTION_LOADING_MESSAGES, groupInsightsBySection, summarizeSeverities, type InsightSectionGroup, type SectionableInsight, type SeveritySummary } from '@/lib/utils'
import { useDateFormat } from '@/hooks/useSSE'
import { useServiceStore, useSessionRoleStore } from '@/stores/filterStore'
import type { components } from '@/types/api.generated'
import dynamic from 'next/dynamic'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { ScrollArea } from '@/components/ui/scroll-area'
import { InsightCardData, type FiltersPayload } from '@/types/api'
import { parseAsString, useQueryState } from 'nuqs'
import { useEffectiveServiceId } from '@/hooks/useBootstrap'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { ReportLayout } from '@/components/ReportLayout'
import { useInsightsDefaults, useDebounce, useDebouncedFilterPayload } from '@/hooks/useDashboardCards'

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

maplibregl.setWorkerUrl('/maplibre-gl-worker.mjs')

// ---------------------------------------------------------------------------
// Shared Modal Shell & Section Primitives
// ---------------------------------------------------------------------------

interface InsightModalShellProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  maxWidthClass?: string
  scrollable?: boolean
  icon: React.ReactNode
  title: React.ReactNode
  description: React.ReactNode
  footer?: React.ReactNode
  children: React.ReactNode
}

function InsightModalShell({
  isOpen, onOpenChange, maxWidthClass = 'max-w-3xl', scrollable = true,
  icon, title, description, footer, children,
}: InsightModalShellProps) {
  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent className={cn(maxWidthClass, scrollable ? 'max-h-[90vh] flex flex-col p-6' : 'p-6')}>
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-lg font-bold">
            {icon}
            {title}
          </DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>
        {scrollable ? (
          <div className="flex-1 min-h-0 flex flex-col gap-6 py-4 overflow-y-auto">{children}</div>
        ) : (
          children
        )}
        {footer ?? <DialogFooter showCloseButton />}
      </DialogContent>
    </Dialog>
  )
}

interface FindingStatDef {
  icon: React.ReactNode
  label: string
  value: string
  valueClass?: string
  hint?: string
}

interface FindingEvidenceItem {
  lead: string
  body: React.ReactNode
}

function BotFindingSection({
  summary, stats, cols = 3, evidence,
}: {
  summary: React.ReactNode
  stats: FindingStatDef[]
  cols?: 3 | 4
  evidence: FindingEvidenceItem[]
}) {
  return (
    <>
      <div className="p-4 rounded-lg bg-muted/30 border border-border">
        <p className="text-sm leading-relaxed">{summary}</p>
      </div>
      <div className={cn('grid grid-cols-2 gap-4', cols === 3 ? 'md:grid-cols-3' : 'md:grid-cols-4')}>
        {stats.map((s) => (
          <InsightStatCard
            key={s.label}
            icon={s.icon}
            label={s.label}
            value={s.value}
            valueClass={s.valueClass}
            hint={s.hint}
          />
        ))}
      </div>
      <div className="rounded-lg bg-muted/10 border border-border p-4">
        <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
          <ScanSearch className="h-3.5 w-3.5" /> How we read the evidence
        </h4>
        <ul className="space-y-2.5 text-xs leading-relaxed">
          {evidence.map((ev) => (
            <li key={ev.lead}>
              <span className="font-semibold">{ev.lead}</span> {ev.body}
            </li>
          ))}
        </ul>
      </div>
    </>
  )
}

// ---------------------------------------------------------------------------
// 1. ImpossibleDistanceModal
// ---------------------------------------------------------------------------

function buildPhysicsFeatures(data: ImpossibleDistanceData): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: [
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [data.client_lon, data.client_lat] },
        properties: { type: 'client', title: 'Client' },
      },
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [data.pop_lon, data.pop_lat] },
        properties: { type: 'pop', title: `POP: ${data.pop}` },
      },
      {
        type: 'Feature',
        geometry: {
          type: 'LineString',
          coordinates: [[data.client_lon, data.client_lat], [data.pop_lon, data.pop_lat]],
        },
        properties: { type: 'path' },
      },
    ],
  }
}

function fitPhysicsBounds(map: maplibregl.Map, data: ImpossibleDistanceData): void {
  const bounds = new maplibregl.LngLatBounds()
  bounds.extend([data.client_lon, data.client_lat])
  bounds.extend([data.pop_lon, data.pop_lat])
  map.fitBounds(bounds, { padding: 50, maxZoom: 5, duration: 0 })
}

function PhysicsMap({ data, isDark }: { data: ImpossibleDistanceData; isDark: boolean }) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const [mapError, setMapError] = useState<string | null>(null)

  const hasCoords = Number.isFinite(data.client_lat) && Number.isFinite(data.client_lon)
    && Number.isFinite(data.pop_lat) && Number.isFinite(data.pop_lon)

  useEffect(() => {
    if (!mapContainer.current || !data || !hasCoords) return

    if (!map.current) {
      try {
        map.current = new maplibregl.Map({
          container: mapContainer.current,
          style: {
            version: 8,
            sources: {},
            layers: [{ id: 'background', type: 'background', paint: { 'background-color': isDark ? '#18181b' : '#f4f4f5' } }],
          },
          center: [(data.client_lon + data.pop_lon) / 2, (data.client_lat + data.pop_lat) / 2],
          zoom: 1,
          renderWorldCopies: false,
          preserveDrawingBuffer: true,
          interactive: false,
        } as maplibregl.MapOptions)

        map.current.on('load', () => {
          if (!map.current || !data) return
          addCountryBaseLayer(map.current, { isDark })
          map.current.addSource('points', { type: 'geojson', data: buildPhysicsFeatures(data) })
          map.current.addLayer({
            id: 'path-layer',
            type: 'line',
            source: 'points',
            filter: ['==', ['get', 'type'], 'path'],
            paint: { 'line-color': CHART_COLORS.danger, 'line-width': 2, 'line-dasharray': [2, 2] },
          })
          map.current.addLayer({
            id: 'points-layer',
            type: 'circle',
            source: 'points',
            filter: ['!=', ['get', 'type'], 'path'],
            paint: {
              'circle-radius': 6,
              'circle-color': ['match', ['get', 'type'], 'client', CHART_COLORS.info, 'pop', '#22c55e', '#000000'],
              'circle-stroke-width': 2,
              'circle-stroke-color': '#ffffff',
            },
          })
          fitPhysicsBounds(map.current, data)
        })
      } catch {
        map.current = null
        requestAnimationFrame(() => setMapError('Map failed to initialize.'))
      }
    } else if (map.current.isStyleLoaded()) {
      map.current.setPaintProperty('background', 'background-color', isDark ? '#18181b' : '#f4f4f5')
      updateCountryBaseLayerTheme(map.current, isDark)
      const source = map.current.getSource('points') as maplibregl.GeoJSONSource
      if (source) {
        source.setData(buildPhysicsFeatures(data))
        fitPhysicsBounds(map.current, data)
      }
    }
  }, [data, isDark, hasCoords])

  useMapResize(map, mapContainer)

  useEffect(() => {
    const t = setTimeout(() => {
      if (map.current) {
        map.current.resize()
        if (data && map.current.isStyleLoaded()) fitPhysicsBounds(map.current, data)
      }
    }, 150)
    return () => clearTimeout(t)
  }, [data])

  useEffect(() => () => {
    if (map.current) {
      map.current.remove()
      map.current = null
    }
  }, [])

  if (!hasCoords || mapError) {
    return (
      <div className="absolute inset-0 flex items-center justify-center text-xs text-muted-foreground">
        {mapError ?? 'Location data unavailable.'}
      </div>
    )
  }

  return <div ref={mapContainer} className="absolute inset-0 w-full h-full min-h-[300px]" />
}

export interface ImpossibleDistanceModalProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  data: ImpossibleDistanceData | null
}

export function ImpossibleDistanceModal({ isOpen, onOpenChange, data }: ImpossibleDistanceModalProps) {
  const { theme } = useTheme()
  const isDark = theme === 'dark'

  if (!data) return null

  const hasCoords = Number.isFinite(data.client_lat) && Number.isFinite(data.client_lon)
    && Number.isFinite(data.pop_lat) && Number.isFinite(data.pop_lon)

  const c = 299792.458
  const c_fibre = 200000
  const one_way_ms = data.tcp_rtt / 2 / 1000
  const required_speed = data.distance_km / (one_way_ms / 1000)
  const violation_ratio = required_speed / c_fibre
  const exceeds_vacuum = required_speed > c

  return (
    <InsightModalShell
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      maxWidthClass="max-w-3xl"
      scrollable={false}
      icon={<Zap className="h-5 w-5 text-yellow-600" />}
      title={`Physics Violation: ${data.label}`}
      description="Geographic distance between client and POP is physically impossible given the TCP RTT."
    >
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 py-4">
        <div className="md:col-span-2 relative border rounded-lg overflow-hidden bg-muted/20 h-[350px]">
          {isOpen && <PhysicsMap data={data} isDark={isDark} />}
          {hasCoords && (
            <div className="absolute top-2 left-2 flex flex-col gap-1 z-10 pointer-events-none">
              <div className="flex items-center gap-2 bg-background/90 backdrop-blur-sm px-2 py-1 rounded border text-[11px] sm:text-[10px] shadow-sm">
                <div className="h-2 w-2 rounded-full bg-[#3b82f6]" />
                <span>Client: {data.client_lat.toFixed(2)}, {data.client_lon.toFixed(2)}{data.city || data.country ? ` (${[data.city, data.country].filter(Boolean).join(', ')})` : ''}</span>
              </div>
              <div className="flex items-center gap-2 bg-background/90 backdrop-blur-sm px-2 py-1 rounded border text-[11px] sm:text-[10px] shadow-sm">
                <div className="h-2 w-2 rounded-full bg-[#22c55e]" />
                <span>POP ({data.pop}): {data.pop_lat.toFixed(2)}, {data.pop_lon.toFixed(2)}</span>
              </div>
            </div>
          )}
        </div>

        <div className="flex flex-col gap-4">
          <div className="space-y-3">
            <h4 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground flex items-center gap-1">
              <Globe className="h-3 w-3" /> Measured Stats
            </h4>
            <div className="grid grid-cols-2 gap-2">
              <div className="p-2 rounded-md bg-muted/50 border">
                <p className="text-[11px] sm:text-[10px] text-muted-foreground">TCP RTT</p>
                <p className="font-mono text-sm">{(data.tcp_rtt / 1000).toFixed(1)} ms</p>
              </div>
              <div className="p-2 rounded-md bg-muted/50 border">
                <p className="text-[11px] sm:text-[10px] text-muted-foreground">Geo Distance</p>
                <p className="font-mono text-sm">{formatCount(data.distance_km)} km</p>
              </div>
            </div>
          </div>

          <div className={cn(
            'p-3 rounded-lg border space-y-2',
            exceeds_vacuum ? 'bg-red-500/10 border-red-500/50' : 'bg-yellow-500/10 border-yellow-500/50',
          )}>
            <h4 className="text-xs font-bold uppercase flex items-center gap-1">
              <Zap className="h-3 w-3" /> Violation Report
            </h4>
            <div className="space-y-1">
              <p className="text-xs leading-relaxed">
                To cover {formatCount(data.distance_km)} km in {one_way_ms.toFixed(2)} ms (one-way),
                the signal would need to travel at:
              </p>
              <p className="font-mono text-lg font-bold text-center py-1">
                {formatCount(required_speed, { maximumFractionDigits: 0 })} km/s
              </p>
              <div className="space-y-1 text-xs sm:text-[11px]">
                <div className="flex justify-between items-center">
                  <span>vs. Fibre Speed (~200k km/s)</span>
                  <span className="font-bold text-red-500">{(violation_ratio * 100).toFixed(0)}% of limit</span>
                </div>
                {exceeds_vacuum && (
                  <div className="flex justify-between items-center font-bold text-red-600 animate-pulse">
                    <span>FASTER THAN LIGHT (VACUUM)</span>
                    <span>YES</span>
                  </div>
                )}
              </div>
            </div>
          </div>

          <div className="p-2 rounded bg-blue-500/5 border border-blue-500/20 text-[11px] sm:text-[10px] leading-relaxed">
            <Info className="h-3 w-3 inline mr-1 mb-0.5 text-blue-600" />
            <strong>Conclusion:</strong> The user is likely using a VPN, proxy, or GPS spoofing to appear in a location physically distant from the Fastly POP they are actually hitting.
          </div>
        </div>
      </div>
    </InsightModalShell>
  )
}

// ---------------------------------------------------------------------------
// 2. CacheCollapseModal
// ---------------------------------------------------------------------------

type CacheMiss = components['schemas']['CacheMissPoint']

export interface CacheCollapseModalProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  url: string | null
  windowHours: string
  baselineHours: string
}

export function CacheCollapseModal({
  isOpen, onOpenChange, url, windowHours, baselineHours,
}: CacheCollapseModalProps) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const { full, abbr } = useDateFormat()

  const { data, isLoading, error } = useQuery({
    queryKey: ['insights', 'cache-collapse-detail', activeServiceId, url, windowHours, baselineHours],
    queryFn: async ({ signal }) => unwrapClient(client.POST('/api/insights/cache-collapse-detail', {
      signal,
      body: { url: url!, window_size_hrs: parseFloat(windowHours), baseline_hours: parseFloat(baselineHours) },
    })),
    enabled: isOpen && !!url && !!activeServiceId,
    staleTime: 30000,
  })

  const errorMsg = error ? extractApiError(error) : null

  const stats = useMemo(() => {
    if (!data?.timeline) return null
    const totalRequests = data.timeline.reduce((acc, curr) => acc + (curr.total_requests || 0), 0)
    const realHits = data.timeline.reduce((acc, curr) => acc + (curr.real_hits || 0), 0)
    return { totalRequests, realHits, baselineHitRate: data.baseline_hit_rate, windowHitRate: data.window_hit_rate }
  }, [data])

  const chartData = useMemo(() => {
    if (!data?.timeline) return []
    return [
      {
        x: data.timeline.map((p) => p.bucket),
        y: data.timeline.map((p) => p.expected_hits),
        type: 'scatter',
        mode: 'lines',
        name: 'Expected HITs',
        line: { color: CHART_COLORS.info, width: 2, dash: 'dash' },
      },
      {
        x: data.timeline.map((p) => p.bucket),
        y: data.timeline.map((p) => p.real_hits),
        type: 'scatter',
        mode: 'lines',
        name: 'Real HITs',
        line: { color: CHART_COLORS.danger, width: 2 },
      },
    ]
  }, [data])

  const missColumns = useMemo<ColumnDef<CacheMiss>[]>(
    () => [
      {
        id: 'timestamp',
        header: `Timestamp (${abbr()})`,
        accessorFn: (e) => e.timestamp,
        size: 200,
        enableSorting: false,
        cell: ({ row }) => <span className="font-mono text-xs font-medium whitespace-nowrap">{full(row.original.timestamp)}</span>,
      },
      {
        id: 'cache',
        header: 'Cache Status',
        accessorFn: (e) => e.cache,
        size: 130,
        enableSorting: false,
        cell: ({ row }) => {
          const cache = row.original.cache
          return (
            <span className={cn(
              'px-1.5 py-0.5 rounded text-[10px] font-bold border',
              cache.includes('PASS') ? 'bg-amber-500/10 text-amber-500 border-amber-500/20' :
              cache.includes('MISS') ? 'bg-red-500/10 text-red-500 border-red-500/20' :
              'bg-muted text-muted-foreground border-border',
            )}>
              {cache}
            </span>
          )
        },
      },
      {
        id: 'pop',
        header: 'POP',
        accessorFn: (e) => e.pop ?? '',
        size: 90,
        enableSorting: false,
        cell: ({ row }) => <span className="font-mono text-xs">{row.original.pop || '—'}</span>,
      },
      {
        id: 'ip',
        header: 'Client IP',
        accessorFn: (e) => e.ip ?? '',
        size: 150,
        enableSorting: false,
        cell: ({ row }) => <span className="font-mono text-xs text-muted-foreground">{row.original.ip || '—'}</span>,
      },
      {
        id: 'status',
        header: 'HTTP Status',
        accessorFn: (e) => e.status ?? 0,
        size: 110,
        enableSorting: false,
        cell: ({ row }) => (
          <span className="block text-right">
            <span className={cn('px-1.5 py-0.5 rounded font-mono font-semibold border text-[10px]', statusBadgeClass(row.original.status, 'traffic-light'))}>
              {row.original.status || '—'}
            </span>
          </span>
        ),
      },
    ],
    [abbr, full],
  )

  const chartLayout = useMemo(() => ({ yaxis: { title: { text: 'Hits' } } }), [])

  return (
    <InsightModalShell
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      maxWidthClass="max-w-4xl"
      icon={<ShieldAlert className="h-5 w-5 text-red-500 shrink-0" />}
      title={<span className="truncate max-w-[90%]">Cache Analysis: {url}</span>}
      description="Cacheable hit ratio (HIT/(HIT+MISS), PASS excluded) and cacheability (PASS share) vs. baseline."
    >
      {isLoading && (
        <div className="flex-1 flex flex-col items-center justify-center py-20 text-muted-foreground text-sm gap-2">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
          <span>Analyzing historical logs and calculating cache timelines…</span>
        </div>
      )}

      {errorMsg && (
        <div className="flex-1 flex flex-col items-center justify-center py-20 text-center px-4">
          <AlertCircle className="h-10 w-10 text-destructive mb-3" />
          <h3 className="text-sm font-medium text-destructive">Failed to load collapse details</h3>
          <p className="text-xs text-muted-foreground mt-1 max-w-md">{errorMsg}</p>
        </div>
      )}

      {!isLoading && !errorMsg && data && stats && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <InsightStatCard icon={<Clock className="h-3 w-3" />} label="Baseline Hit Ratio" value={`${stats.baselineHitRate.toFixed(2)}%`} valueClass="text-blue-500" hint="cacheable (HIT/(HIT+MISS))" />
            <InsightStatCard icon={<ShieldAlert className="h-3 w-3 text-red-400" />} label="Recent Hit Ratio" value={`${stats.windowHitRate.toFixed(2)}%`} valueClass="text-red-500" hint="cacheable (HIT/(HIT+MISS))" />
            <InsightStatCard icon={<Activity className="h-3 w-3" />} label="Total Requests" value={formatCount(stats.totalRequests)} />
            <InsightStatCard icon={<CheckCircle2 className="h-3 w-3 text-emerald-500" />} label="Total Cache Hits" value={formatCount(stats.realHits)} />
          </div>

          <div className="rounded-lg bg-muted/10 border border-border p-4">
            <div className="flex items-center justify-between mb-3">
              <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground flex items-center gap-1.5">
                <Activity className="h-3.5 w-3.5" /> Recent Window Breakdown
              </h4>
              <span className="text-[11px] text-muted-foreground">
                PASS (uncacheable): {data.baseline_pass_rate.toFixed(1)}% → <span className="font-semibold text-amber-500">{data.window_pass_rate.toFixed(1)}%</span>
              </span>
            </div>
            <div className="grid grid-cols-3 gap-3 text-center">
              <div className="p-2 rounded-md bg-emerald-500/10 border border-emerald-500/20">
                <div className="font-mono text-lg font-bold text-emerald-500">{formatCount(data.breakdown?.hits ?? 0)}</div>
                <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Cacheable Hits</div>
              </div>
              <div className="p-2 rounded-md bg-red-500/10 border border-red-500/20">
                <div className="font-mono text-lg font-bold text-red-500">{formatCount(data.breakdown?.misses ?? 0)}</div>
                <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Misses</div>
              </div>
              <div className="p-2 rounded-md bg-amber-500/10 border border-amber-500/20">
                <div className="font-mono text-lg font-bold text-amber-500">{formatCount(data.breakdown?.passes ?? 0)}</div>
                <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Uncacheable (PASS)</div>
              </div>
            </div>
          </div>

          <div className="border border-border rounded-lg bg-muted/10 p-4 flex flex-col min-h-[300px]">
            <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
              <Activity className="h-3.5 w-3.5" /> Hits Timeline (Expected vs. Real Hits)
            </h4>
            <div className="flex-1 w-full min-h-[220px]">
              <TimeSeriesChart data={chartData} layout={chartLayout} timezone="UTC" height={220} />
            </div>
          </div>

          <div className="flex flex-col min-h-[250px] border border-border rounded-lg bg-muted/10 p-4">
            <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
              <FileText className="h-3.5 w-3.5" /> Recent Cache Misses (Max 50)
            </h4>
            {(!data.recent_misses || data.recent_misses.length === 0) ? (
              <div className="flex-1 flex items-center justify-center text-xs text-muted-foreground italic">
                No cache misses captured for this lookback.
              </div>
            ) : (
              <div className="flex-1">
                <DataTableReadonly
                  columns={missColumns}
                  data={data.recent_misses}
                  hideToolbar
                  showPagination={false}
                  tableCaption="Recent cache misses"
                />
              </div>
            )}
          </div>
        </>
      )}
    </InsightModalShell>
  )
}

// ---------------------------------------------------------------------------
// 3. BotFindingModal
// ---------------------------------------------------------------------------

export interface BotFindingModalProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  data: BotFindingModalData | null
}

export function BotFindingModal({ isOpen, onOpenChange, data }: BotFindingModalProps) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const investigateUrl = data?.item.investigate_url

  const investigateHref = useMemo(
    () => buildInvestigateHref(investigateUrl, activeServiceId),
    [investigateUrl, activeServiceId],
  )

  if (!data) return null

  const { insightId, item } = data

  const renderContent = () => {
    switch (insightId) {
      case 'crawler_disparity': {
        const m = item.meta
        const wBytes = m.w_bytes ?? 0
        const bytesPerRef = m.bytes_per_referral ?? 0
        const baseBytesPerRef = m.baseline_bytes_per_referral ?? 0
        const bwSharePct = m.bandwidth_share_pct ?? 0
        const isZeroReferral = m.referrals === 0
        return (
          <BotFindingSection
            cols={3}
            summary={
              <>
                <span className="font-semibold">{item.label}</span> executed{' '}
                <span className="font-semibold">{formatCount(m.crawls)}</span> automated requests
                {wBytes > 0 && (
                  <>
                    {' '}consuming <span className="font-semibold">{formatBytes(wBytes)}</span> of egress bandwidth
                    {bwSharePct > 0 && <> (<span className="font-semibold">{bwSharePct.toFixed(1)}%</span> of site bandwidth)</>}
                  </>
                )}{' '}
                while driving only{' '}
                <span className={cn('font-semibold', isZeroReferral ? 'text-red-500' : 'text-amber-500')}>
                  {formatCount(m.referrals)}
                </span>{' '}
                unique human referral {m.referrals === 1 ? 'IP' : 'IPs'} back to your service. That equates to{' '}
                <span className="font-semibold text-red-500">
                  {formatCount(m.crawls_per_referral, { maximumFractionDigits: 1 })} crawls/referral
                </span>
                {bytesPerRef > 0 && <> and <span className="font-semibold">{formatBytes(bytesPerRef)}/referral</span></>}.
              </>
            }
            stats={[
              { icon: <Hash className="h-3 w-3" />, label: 'Bot / Crawl Requests', value: formatCount(m.crawls), hint: 'current window' },
              { icon: <HardDrive className="h-3 w-3" />, label: 'Bandwidth Used', value: formatBytes(wBytes), hint: bwSharePct > 0 ? `${bwSharePct.toFixed(1)}% of site bandwidth` : 'total resp_bytes' },
              { icon: <Users className="h-3 w-3" />, label: 'Human Referrals', value: formatCount(m.referrals), valueClass: isZeroReferral ? 'text-red-500' : 'text-amber-500', hint: 'distinct referred IPs' },
              { icon: <Scale className="h-3 w-3" />, label: 'Crawls / Referral', value: formatCount(m.crawls_per_referral, { maximumFractionDigits: 1 }), valueClass: 'text-red-500', hint: `baseline: ${formatCount(m.baseline_crawls_per_referral, { maximumFractionDigits: 1 })}` },
              { icon: <HardDrive className="h-3 w-3" />, label: 'Bandwidth / Referral', value: formatBytes(bytesPerRef), hint: baseBytesPerRef > 0 ? `baseline: ${formatBytes(baseBytesPerRef)}` : 'cost per referred visitor' },
              { icon: <Bot className="h-3 w-3" />, label: 'Site Egress Share', value: `${bwSharePct.toFixed(1)}%`, hint: 'of total resp_bytes' },
            ]}
            evidence={[
              { lead: 'Unified bot & crawler attribution.', body: 'Requests are grouped dynamically from automated User-Agent tokens — covering search indexers, AI answer crawlers, training scrapers, headless browsers, and programmatic HTTP clients.' },
              { lead: 'Human referral correlation.', body: <>We count distinct client IPs arriving with a non-bot User-Agent and an HTTP <code>Referer</code> matching the platform&apos;s domain.</> },
              { lead: 'Value vs. cost disparity.', body: 'High crawl volume and egress bandwidth combined with near-zero referrals means this bot or crawler is harvesting content without returning organic search or discovery traffic.' },
            ]}
          />
        )
      }

      case 'stale_browser_version': {
        const m = item.meta
        return (
          <BotFindingSection
            cols={3}
            summary={
              <>
                Clients claiming <span className="font-semibold">{item.label}</span> generated{' '}
                <span className="font-semibold">{formatCount(m.requests)}</span> requests across{' '}
                <span className="font-semibold">{formatCount(m.ips)}</span> distinct IPs — surging{' '}
                <span className="font-semibold text-amber-500">{m.surge_ratio.toFixed(1)}×</span> over baseline.
              </>
            }
            stats={[
              { icon: <Hash className="h-3 w-3" />, label: 'Requests', value: formatCount(m.requests), hint: 'current window' },
              { icon: <Users className="h-3 w-3" />, label: 'Distinct IPs', value: formatCount(m.ips), hint: 'unique client IPs' },
              { icon: <Scale className="h-3 w-3" />, label: 'Surge Ratio', value: `${m.surge_ratio.toFixed(1)}×`, valueClass: 'text-amber-500', hint: 'vs. baseline rate' },
            ]}
            evidence={[
              { lead: 'Evergreen browser drift.', body: 'Real desktop and mobile Chrome installations auto-update silently. Pinned, outdated major versions almost always come from containerized headless browsers (Puppeteer, Playwright, Selenium) or hardcoded User-Agent strings.' },
              { lead: 'Multi-IP coordination.', body: `${formatCount(m.ips)} source IPs presenting the same outdated browser version indicates a distributed scraping pool sharing a single container image.` },
            ]}
          />
        )
      }

      case 'orphaned_deep_crawl': {
        const m = item.meta
        return (
          <BotFindingSection
            cols={4}
            summary={
              <>
                Client IP <span className="font-semibold">{item.label}</span> fetched{' '}
                <span className="font-semibold text-red-500">{formatCount(m.requests)}</span> deep resource requests without an HTTP{' '}
                <code>Referer</code> header across{' '}
                <span className="font-semibold">{formatCount(m.distinct_urls)}</span> distinct URLs (
                <span className="font-semibold">{m.requests_per_url.toFixed(1)}</span> requests/URL,{' '}
                <span className="font-semibold text-amber-500">{m.surge_ratio.toFixed(1)}×</span> baseline).
              </>
            }
            stats={[
              { icon: <ShieldAlert className="h-3 w-3" />, label: 'Orphan Requests', value: formatCount(m.requests), valueClass: 'text-red-500', hint: 'missing HTTP Referer' },
              { icon: <Globe className="h-3 w-3" />, label: 'Distinct URLs', value: formatCount(m.distinct_urls), hint: 'unique deep paths' },
              { icon: <Hash className="h-3 w-3" />, label: 'Requests / URL', value: m.requests_per_url.toFixed(1), hint: 'sweep density' },
              { icon: <Scale className="h-3 w-3" />, label: 'Surge Ratio', value: `${m.surge_ratio.toFixed(1)}×`, valueClass: 'text-amber-500', hint: 'vs. baseline rate' },
            ]}
            evidence={[
              { lead: 'Broken navigation graph.', body: <>Genuine users reach deep catalog or detail pages by clicking links from landing or search pages, which populates the <code>Referer</code> header. Hitting hundreds of deep paths directly with an empty referer is a hallmark of queue-driven scrapers.</> },
              { lead: `URL breadth (${formatCount(m.distinct_urls)} paths).`, body: 'High path diversity rules out a single bookmarked page or asset refresh and confirms systematic enumeration.' },
            ]}
          />
        )
      }

      case 'residential_fingerprint_dispersion': {
        const m = item.meta
        return (
          <BotFindingSection
            cols={4}
            summary={
              <>
                TLS fingerprint <span className="font-mono font-semibold">{m.ja4 || item.label}</span> appeared across{' '}
                <span className="font-semibold text-red-500">{formatCount(m.asns)}</span> distinct ASNs and{' '}
                <span className="font-semibold">{formatCount(m.ips)}</span> unique IPs over{' '}
                <span className="font-semibold">{formatCount(m.requests)}</span> requests (
                <span className="font-semibold text-amber-500">{m.asn_expansion_ratio.toFixed(1)}×</span> ASN expansion).
              </>
            }
            stats={[
              { icon: <Globe className="h-3 w-3" />, label: 'Distinct ASNs', value: formatCount(m.asns), valueClass: 'text-red-500', hint: 'autonomous systems' },
              { icon: <Users className="h-3 w-3" />, label: 'Distinct IPs', value: formatCount(m.ips), hint: 'rotated source IPs' },
              { icon: <Hash className="h-3 w-3" />, label: 'Total Requests', value: formatCount(m.requests), hint: 'current window' },
              { icon: <Fingerprint className="h-3 w-3" />, label: 'ASN Expansion', value: `${m.asn_expansion_ratio.toFixed(1)}×`, valueClass: 'text-amber-500', hint: `${formatCount(m.ips / Math.max(1, m.asns), { maximumFractionDigits: 1 })} IPs/ASN` },
            ]}
            evidence={[
              { lead: 'Residential proxy rotation.', body: 'Attackers route requests through residential proxy networks so each request arrives from a different home ISP IP and ASN, evading per-IP rate limits.' },
              { lead: 'Invariant TLS stack.', body: 'Because the underlying scraping binary or headless engine is identical across all proxy exits, the TLS ClientHello fingerprint (JA4/JA3) remains constant across dozens of autonomous systems.' },
            ]}
          />
        )
      }
    }
  }

  return (
    <InsightModalShell
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      maxWidthClass="max-w-3xl"
      icon={<ScanSearch className="h-5 w-5 text-primary shrink-0" />}
      title={<span className="truncate max-w-[90%]">Finding Details: {item.label}</span>}
      description="Detailed telemetry and behavioral evidence behind this bot detection finding."
      footer={
        <DialogFooter className="flex items-center justify-between sm:justify-between gap-2">
          {investigateHref ? (
            <Link href={investigateHref} target="_blank" rel="noopener noreferrer">
              <Button variant="outline" size="sm">
                Investigate in Logs <ArrowUpRight className="ml-1.5 h-3.5 w-3.5" />
              </Button>
            </Link>
          ) : (
            <div />
          )}
          <Button variant="secondary" size="sm" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        </DialogFooter>
      }
    >
      {renderContent()}
    </InsightModalShell>
  )
}

type CardOf<Id extends InsightCardData['id']> = Extract<InsightCardData, { id: Id }>

export type ImpossibleDistanceItem = CardOf<'impossible_distance'>['items'][number]
export type RepeatedPatternsItem = CardOf<'repeated_patterns'>['items'][number]
export type RepeatedPatternsFpItem = CardOf<'repeated_patterns_fp'>['items'][number]

export type BotFindingModalData =
  | {
      insightId: 'crawler_disparity'
      item: CardOf<'crawler_disparity'>['items'][number]
    }
  | {
      insightId: 'stale_browser_version'
      item: CardOf<'stale_browser_version'>['items'][number]
    }
  | {
      insightId: 'orphaned_deep_crawl'
      item: CardOf<'orphaned_deep_crawl'>['items'][number]
    }
  | {
      insightId: 'residential_fingerprint_dispersion'
      item: CardOf<'residential_fingerprint_dispersion'>['items'][number]
    }

export interface ImpossibleDistanceData {
  label: string
  client_lat: number
  client_lon: number
  pop_lat: number
  pop_lon: number
  pop: string
  tcp_rtt: number
  distance_km: number
  max_km: number
  country?: string
  city?: string
}

export interface ScriptedTrafficData {
  label: string
  score: number
  cv: number
  modal_frac: number
  mean_interval_s: number
  stddev_s: number
  mode_gap_s: number | null
  n_gaps: number
  n_events: number
  span_s: number
  rps: number
  distinct_ua: number
  distinct_ip?: number
}

export interface InsightHelpModalProps {
  insightId: string
  isOpen: boolean
  onOpenChange: (open: boolean) => void
}

export interface InsightContent {
  title: string
  icon: React.ReactNode
  description: React.ReactNode
  diagram?: React.ReactNode
  fields: string[]
}

interface DeltaIndicatorProps {
  current: number
  baseline: number | null | undefined
}

export function DeltaIndicator({ current, baseline }: DeltaIndicatorProps) {
  if (baseline == null || baseline === 0) return null
  const pct = ((current - baseline) / Math.abs(baseline)) * 100
  const abs = current - baseline
  const absStr = abs > 0 ? `+${formatCount(abs, { maximumFractionDigits: 1 })}` : formatCount(abs, { maximumFractionDigits: 1 })

  if (Math.abs(pct) < 1) return <Minus className="h-3 w-3 text-muted-foreground" />

  if (pct > 0) return (
    <span className="flex items-center gap-0.5 text-red-600 dark:text-red-400 text-[11px] sm:text-[10px] font-semibold" title={`${absStr} from baseline`}>
      <TrendingUp className="h-3 w-3" />
      {absStr} (+{Math.round(pct)}%)
    </span>
  )

  return (
    <span className="flex items-center gap-0.5 text-green-700 dark:text-green-400 text-[11px] sm:text-[10px] font-semibold" title={`${absStr} from baseline`}>
      <TrendingDown className="h-3 w-3" />
      {absStr} ({Math.round(pct)}%)
    </span>
  )
}

interface InsightStatCardProps {
  icon: React.ReactNode
  label: string
  value: string
  valueClass?: string
  hint?: string
}

export function InsightStatCard({
  icon,
  label,
  value,
  valueClass,
  hint,
}: InsightStatCardProps) {
  return (
    <div className="p-3 rounded-lg bg-muted/40 border border-border flex flex-col justify-between">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold flex items-center gap-1">
        {icon} {label}
      </div>
      <div className={cn('mt-1.5 font-mono text-xl font-bold', valueClass ?? 'text-foreground')}>
        {value}
      </div>
      {hint && <div className="text-[10px] text-muted-foreground mt-0.5">{hint}</div>}
    </div>
  )
}

const StatCard = InsightStatCard

export function buildInvestigateHref(
  investigateUrl: string | null | undefined,
  activeServiceId: string | null | undefined,
): string | null {
  if (!investigateUrl) return null
  const url = new URL(investigateUrl, 'http://localhost')
  if (!url.searchParams.has('service') && activeServiceId) {
    url.searchParams.set('service', activeServiceId)
  }
  return url.pathname + url.search
}

interface InsightItemRowProps {
  /** The owning card; its `id` fixes the type of `items[index].meta`. */
  insight: InsightCardData
  index: number
  onMapClick?: (data: ImpossibleDistanceData) => void
  onCacheCollapseClick?: (url: string) => void
  onScriptedTrafficClick?: (data: ScriptedTrafficData) => void
  onBotFindingClick?: (data: BotFindingModalData) => void
}

export function InsightItemRow({ insight, index, onMapClick, onCacheCollapseClick, onScriptedTrafficClick, onBotFindingClick }: InsightItemRowProps) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const item = insight.items[index]

  const investigateHref = React.useMemo(
    () => buildInvestigateHref(item.investigate_url, activeServiceId),
    [item.investigate_url, activeServiceId],
  )

  return (
    <div data-testid="insight-item-row" className="flex items-center justify-between text-xs p-2 rounded-md bg-muted/50 gap-2">
      <div className="flex flex-col min-w-0 flex-1">
        <div className="flex items-center gap-1.5 min-w-0">
          <span className="font-medium truncate">{item.label}</span>
          {insight.id === 'impossible_distance' && onMapClick && (
            <MapAction item={insight.items[index]} onMapClick={onMapClick} />
          )}
          {(insight.id === 'cache_collapse' || insight.id === 'cacheability_regression') && onCacheCollapseClick && (
            <Button
              variant="ghost"
              size="icon"
              aria-label={`Analyze ${item.label} cache behavior`}
              className="h-6 w-6 text-primary hover:text-primary/80 shrink-0"
              onClick={() => {
                onCacheCollapseClick(item.label)
              }}
              title="View Cache Analysis"
            >
              <LineChart className="h-3 w-3" aria-hidden="true" />
            </Button>
          )}
          {insight.id === 'repeated_patterns' && onScriptedTrafficClick && (
            <ScriptedAction
              item={insight.items[index]}
              distinct={{ distinct_ua: insight.items[index].meta.distinct_ua }}
              onScriptedTrafficClick={onScriptedTrafficClick}
            />
          )}
          {insight.id === 'repeated_patterns_fp' && onScriptedTrafficClick && (
            <ScriptedAction
              item={insight.items[index]}
              distinct={{ distinct_ua: 0, distinct_ip: insight.items[index].meta.distinct_ip }}
              onScriptedTrafficClick={onScriptedTrafficClick}
            />
          )}
          {(insight.id === 'crawler_disparity' ||
            insight.id === 'stale_browser_version' ||
            insight.id === 'orphaned_deep_crawl' ||
            insight.id === 'residential_fingerprint_dispersion') &&
            onBotFindingClick && (
              <Button
                variant="ghost"
                size="icon"
                aria-label={`View finding details for ${item.label}`}
                className="h-6 w-6 text-primary hover:text-primary/80 shrink-0"
                onClick={() => {
                  switch (insight.id) {
                    case 'crawler_disparity':
                      onBotFindingClick({ insightId: 'crawler_disparity', item: insight.items[index] })
                      break
                    case 'stale_browser_version':
                      onBotFindingClick({ insightId: 'stale_browser_version', item: insight.items[index] })
                      break
                    case 'orphaned_deep_crawl':
                      onBotFindingClick({ insightId: 'orphaned_deep_crawl', item: insight.items[index] })
                      break
                    case 'residential_fingerprint_dispersion':
                      onBotFindingClick({ insightId: 'residential_fingerprint_dispersion', item: insight.items[index] })
                      break
                  }
                }}
                title="View Finding Details"
              >
                <ScanSearch className="h-3 w-3" aria-hidden="true" />
              </Button>
            )}
        </div>
        {item.tooltip && (
          <span className="text-[11px] sm:text-[10px] text-muted-foreground truncate">{item.tooltip}</span>
        )}
      </div>
      <div className="flex flex-col items-end gap-0.5 shrink-0">
        <div className="flex items-center gap-1.5">
          <DeltaIndicator current={item.current_val ?? 0} baseline={item.baseline_val} />
          <span className="font-bold tabular-nums">
            {formatCountMaybe(item.current_val, { maximumFractionDigits: 1 })} {item.unit}
          </span>
        </div>
        {item.baseline_val != null && (
          <span className="text-[11px] sm:text-[10px] text-muted-foreground tabular-nums">
            {item.baseline_label || 'baseline'}: {formatCount(item.baseline_val, { maximumFractionDigits: 1 })} {item.unit}
          </span>
        )}
        {investigateHref && (
          <Link
            href={investigateHref}
            className="text-primary hover:underline flex items-center gap-0.5 text-[11px] sm:text-[10px] mt-0.5"
            target="_blank"
            rel="noopener noreferrer"
          >
            Investigate <ArrowUpRight className="h-2.5 w-2.5" aria-hidden="true" />
          </Link>
        )}
      </div>
    </div>
  )
}

function MapAction({ item, onMapClick }: { item: ImpossibleDistanceItem; onMapClick: (data: ImpossibleDistanceData) => void }) {
  const { client_lat, client_lon, pop_lat, pop_lon, pop, tcp_rtt, city, country } = item.meta
  if (!Number.isFinite(client_lat) || client_lat === null || client_lon === null || tcp_rtt === null) return null
  if (!Number.isFinite(pop_lat) || !Number.isFinite(pop_lon)) return null
  return (
    <Button
      variant="ghost"
      size="icon"
      aria-label={`Show ${item.label} on map`}
      className="h-6 w-6 text-primary hover:text-primary/80 shrink-0"
      onClick={() =>
        onMapClick({
          label: item.label,
          client_lat,
          client_lon,
          pop_lat,
          pop_lon,
          pop: pop ?? '',
          tcp_rtt,
          distance_km: item.current_val || 0,
          max_km: item.baseline_val || 0,
          city: city ?? undefined,
          country: country ?? undefined,
        })
      }
      title="View on Map"
    >
      <MapIcon className="h-3 w-3" aria-hidden="true" />
    </Button>
  )
}

function ScriptedAction({
  item,
  distinct,
  onScriptedTrafficClick,
}: {
  item: RepeatedPatternsItem | RepeatedPatternsFpItem
  distinct: Pick<ScriptedTrafficData, 'distinct_ua' | 'distinct_ip'>
  onScriptedTrafficClick: (data: ScriptedTrafficData) => void
}) {
  const m = item.meta
  return (
    <Button
      variant="ghost"
      size="icon"
      aria-label={`Why ${item.label} was flagged`}
      className="h-6 w-6 text-primary hover:text-primary/80 shrink-0"
      onClick={() =>
        onScriptedTrafficClick({
          label: item.label,
          score: m.score,
          cv: m.cv,
          modal_frac: m.modal_frac,
          mean_interval_s: m.mean_interval_s,
          stddev_s: m.stddev_s,
          mode_gap_s: m.mode_gap_s,
          n_gaps: m.n_gaps,
          n_events: m.n_events,
          span_s: m.span_s,
          rps: m.rps,
          ...distinct,
        })
      }
      title="Why we flagged this"
    >
      <ScanSearch className="h-3 w-3" aria-hidden="true" />
    </Button>
  )
}

interface ScriptedTrafficModalProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  data: ScriptedTrafficData | null
}

// Inter-arrival gap: keep sub-90s readable in seconds, humanize longer cadences.
function formatInterval(sec: number | null): string {
  if (sec == null) return '—'
  if (sec < 90) return `${formatCount(sec, { maximumFractionDigits: 1 })}s`
  return formatDuration(Math.max(0, sec))
}

export function ScriptedTrafficModal({ isOpen, onOpenChange, data }: ScriptedTrafficModalProps) {
  if (!data) return null

  const modalPct = Math.round(data.modal_frac * 100)
  const span = formatDuration(Math.max(0, data.span_s))
  // Score colour tracks the backend severity bands (≥90 critical, ≥70 warning).
  const scoreColor =
    data.score >= 90 ? 'text-red-500' : data.score >= 70 ? 'text-amber-500' : 'text-blue-500'

  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[90vh] flex flex-col p-6">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-lg font-bold">
            <ScanSearch className="h-5 w-5 text-primary shrink-0" />
            <span className="truncate max-w-[90%]">Why we flagged this: {data.label}</span>
          </DialogTitle>
          <DialogDescription>
            Evidence that this client&apos;s request timing is machine-regular rather than human.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 min-h-0 flex flex-col gap-6 py-4 overflow-y-auto">
          {/* Plain-language verdict assembled from the evidence below. */}
          <div className="p-4 rounded-lg bg-muted/30 border border-border">
            <p className="text-sm leading-relaxed">
              <span className="font-semibold">{data.label}</span> made{' '}
              <span className="font-semibold">{formatCount(data.n_events)}</span> requests over{' '}
              <span className="font-semibold">{span}</span> on a near-constant{' '}
              <span className="font-semibold">~{formatInterval(data.mean_interval_s)}</span> cadence
              {' '}— <span className="font-semibold">{modalPct}%</span> of the gaps between requests
              were identical, with only <span className="font-semibold">{formatInterval(data.stddev_s)}</span>{' '}
              of jitter (σ). A regularity score of{' '}
              <span className={cn('font-semibold', scoreColor)}>{data.score}/100</span> is far more
              consistent with an automated script — a scraper, poller, or cron-scheduled job — than
              with human browsing.
            </p>
          </div>

          {/* Measured signals. */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <StatCard
              icon={<Gauge className="h-3 w-3" />}
              label="Regularity Score"
              value={`${data.score}/100`}
              valueClass={scoreColor}
              hint="higher = more machine-like"
            />
            <StatCard
              icon={<Timer className="h-3 w-3" />}
              label="Mean Interval"
              value={formatInterval(data.mean_interval_s)}
              hint="avg. gap between requests"
            />
            <StatCard
              icon={<Activity className="h-3 w-3" />}
              label="Jitter (σ)"
              value={formatInterval(data.stddev_s)}
              hint="std. dev. of gaps"
            />
            <StatCard
              icon={<Repeat className="h-3 w-3" />}
              label="Modal Gap"
              value={formatInterval(data.mode_gap_s)}
              hint={`${modalPct}% of gaps match`}
            />
            <StatCard
              icon={<Hash className="h-3 w-3" />}
              label="Total Requests"
              value={formatCount(data.n_events)}
              hint={`${formatCount(data.n_gaps)} gaps measured`}
            />
            <StatCard
              icon={<Clock className="h-3 w-3" />}
              label="Observation Span"
              value={span}
            />
            <StatCard
              icon={<Gauge className="h-3 w-3" />}
              label="Requests / sec"
              value={formatCount(data.rps, { maximumFractionDigits: 3 })}
            />
            <StatCard
              icon={<Users className="h-3 w-3" />}
              label={data.distinct_ip != null ? 'Distinct IPs' : 'Distinct UAs'}
              value={formatCount(data.distinct_ip ?? data.distinct_ua)}
            />
          </div>

          {/* Why each signal matters — keep the score from being a black box. */}
          <div className="rounded-lg bg-muted/10 border border-border p-4">
            <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
              <ScanSearch className="h-3.5 w-3.5" /> How we read the evidence
            </h4>
            <ul className="space-y-2.5 text-xs leading-relaxed">
              <li>
                <span className="font-semibold">Cadence regularity (CV {data.cv.toFixed(2)}).</span>{' '}
                A low coefficient of variation means the time between requests barely changes — human
                traffic is bursty and irregular, scripts are not.
              </li>
              <li>
                <span className="font-semibold">Modal dominance ({modalPct}%).</span>{' '}
                The share of inter-arrival gaps that are exactly the modal interval
                {data.mode_gap_s != null ? <> (<span className="font-mono">{formatInterval(data.mode_gap_s)}</span>)</> : null}.
                A high share points to fixed-interval scheduling, like a cron job or polling loop.
              </li>
              <li>
                <span className="font-semibold">Volume &amp; span.</span>{' '}
                {formatCount(data.n_events)} requests over {span} ({formatCount(data.rps, { maximumFractionDigits: 3 })} req/s),
                sustained but below volumetric rate-limit thresholds — the pattern this insight is
                designed to catch.
              </li>
              <li>
                {data.distinct_ip != null ? (
                  <>
                    <span className="font-semibold">Distinct IPs ({formatCount(data.distinct_ip)}).</span>{' '}
                    This fingerprint appeared across multiple source addresses, which is why
                    IP-based detection alone would miss it. IP rotation with a stable TLS fingerprint
                    is a strong signal of coordinated scripted traffic.
                  </>
                ) : (
                  <>
                    <span className="font-semibold">Distinct user-agents ({formatCount(data.distinct_ua)}).</span>{' '}
                    Shown for context only — it is <span className="italic">not</span> a reason to dismiss
                    the flag. A high UA count combined with this regular timing usually signals a
                    UA-rotating scraper, which is <span className="font-semibold">more</span> suspicious,
                    not less.
                  </>
                )}
              </li>
            </ul>
          </div>
        </div>

        <DialogFooter showCloseButton />
      </DialogContent>
    </Dialog>
  )
}

interface InsightCardSkeletonProps {
  title: string
  description?: string
  loadingMessage?: string
}

export function InsightCardSkeleton({
  title,
  description,
  loadingMessage = 'Analyzing log patterns…',
}: InsightCardSkeletonProps) {
  return (
    <Card className="h-full flex flex-col min-h-[220px] [contain-intrinsic-size:220px] [content-visibility:auto] opacity-90">
      <CardHeader className="pb-2">
        <div className="flex items-start justify-between gap-2">
          <div className="flex items-center gap-2 min-w-0">
            <Loader2 className="h-5 w-5 shrink-0 text-muted-foreground animate-spin" />
            <CardTitle className="text-base leading-tight">{title}</CardTitle>
          </div>
        </div>
        {description && (
          <CardDescription className="text-xs mt-1">{description}</CardDescription>
        )}
      </CardHeader>

      <CardContent className="flex-1 flex flex-col items-center justify-center pt-0 pb-6 text-xs text-muted-foreground gap-1.5">
        <span>{loadingMessage}</span>
      </CardContent>
    </Card>
  )
}

interface InsightDataModalProps {
  insight: InsightCardData
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  onMapClick?: (data: ImpossibleDistanceData) => void
  onCacheCollapseClick?: (url: string) => void
  onScriptedTrafficClick?: (data: ScriptedTrafficData) => void
  onBotFindingClick?: (data: BotFindingModalData) => void
}

export function InsightDataModal({
  insight,
  isOpen,
  onOpenChange,
  onMapClick,
  onCacheCollapseClick,
  onScriptedTrafficClick,
  onBotFindingClick,
}: InsightDataModalProps) {
  if (!insight.items || insight.items.length === 0) return null

  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent data-testid="data-modal-open" className="max-w-2xl max-h-[85vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>All Data: {insight.title}</DialogTitle>
          <DialogDescription className="sr-only">
            Full list of records backing the “{insight.title}” insight.
          </DialogDescription>
        </DialogHeader>

        <ScrollArea className="flex-1 -mx-6 px-6">
          <div className="space-y-1.5 py-4">
            {keyByOccurrence(insight.items, (it) => it.label).map(({ key }, index) => (
              <InsightItemRow
                key={key}
                insight={insight}
                index={index}
                onMapClick={onMapClick}
                onCacheCollapseClick={onCacheCollapseClick}
                onScriptedTrafficClick={onScriptedTrafficClick}
                onBotFindingClick={onBotFindingClick}
              />
            ))}
          </div>
        </ScrollArea>
      </DialogContent>
    </Dialog>
  )
}

// Lazy-load the help modal: its index statically imports ~28KB of
// per-insight help copy (InsightHelpModal/sections/*) that otherwise ships
// in the /insights route chunk even though most users never open a help
// dialog. next/dynamic(ssr:false) defers that chunk to the first help-button
// click. The `helpEverOpened` latch below keeps the modal mounted after the
// first open (so Radix's close animation still plays) while still keeping
// the chunk off the cold-load critical path.
const InsightHelpModal = dynamic(
  () => import('./InsightHelpModal').then((m) => m.InsightHelpModal),
  { ssr: false },
)


interface InsightCardProps {
  insight: InsightCardData
  windowHours: string
  baselineHours: string
}

const SEVERITY_ICON = {
  clean: CheckCircle,
  info: Info,
  warning: AlertTriangle,
  critical: AlertCircle,
  error: AlertCircle,
}

// Icon colors use the -600 ramp on yellow/blue to clear WCAG 1.4.3
// 3:1 non-text contrast on the white card background (yellow-500 ~2.6:1,
// blue-500 ~3.5:1 marginal; -600 lands at ~4.5:1 and ~5:1 respectively).
const SEVERITY_ICON_COLOR = {
  clean: 'text-green-500',
  info: 'text-blue-600',
  warning: 'text-yellow-600',
  critical: 'text-red-500',
  error: 'text-red-600',
}

// Exported so the /insights section headers can reuse the exact same severity
// palette for their per-section rollup chips (keeps card + chip colours in one
// place). Keys match the backend severity ramp.
export const SEVERITY_BADGE_CLASS = {
  clean: 'bg-green-50 text-green-700 border-green-200 dark:bg-green-950/30 dark:text-green-400 dark:border-green-800',
  info: 'bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-950/30 dark:text-blue-400 dark:border-blue-800',
  warning: 'bg-yellow-50 text-yellow-700 border-yellow-200 dark:bg-yellow-950/30 dark:text-yellow-400 dark:border-yellow-800',
  critical: 'bg-red-50 text-red-700 border-red-200 dark:bg-red-950/30 dark:text-red-400 dark:border-red-800',
  error: 'bg-red-100 text-red-800 border-red-300 dark:bg-red-950/50 dark:text-red-400 dark:border-red-800',
}

export function InsightCard({ insight, windowHours, baselineHours }: InsightCardProps) {
  const [isHelpOpen, setIsHelpOpen] = useState(false)
  // Latches true on first help-button click so the lazy InsightHelpModal
  // mounts once and stays mounted (close animation), loading its chunk only
  // when a user actually asks for help.
  const [helpEverOpened, setHelpEverOpened] = useState(false)
  const [isDataModalOpen, setIsDataModalOpen] = useState(false)
  const [selectedMapItem, setSelectedMapItem] = useState<ImpossibleDistanceData | null>(null)
  const [selectedCollapseUrl, setSelectedCollapseUrl] = useState<string | null>(null)
  const [selectedScriptedItem, setSelectedScriptedItem] = useState<ScriptedTrafficData | null>(null)
  const [selectedBotFindingItem, setSelectedBotFindingItem] = useState<BotFindingModalData | null>(null)

  const Icon = SEVERITY_ICON[insight.severity as keyof typeof SEVERITY_ICON] || AlertCircle
  const iconColor = SEVERITY_ICON_COLOR[insight.severity as keyof typeof SEVERITY_ICON_COLOR] || 'text-muted-foreground'
  const badgeClass = SEVERITY_BADGE_CLASS[insight.severity as keyof typeof SEVERITY_BADGE_CLASS] || ''

  return (
    <>
      <Card data-testid="insight-card" className="h-full flex flex-col min-h-[220px] [contain-intrinsic-size:220px] [content-visibility:auto]">
        <CardHeader className="pb-2">
          <div className="flex items-start justify-between gap-2">
            <div className="flex items-center gap-2 min-w-0">
              <Icon className={cn('h-5 w-5 shrink-0', iconColor)} />
              <CardTitle className="text-base leading-tight">{insight.title}</CardTitle>
            </div>
            <div className="flex items-center gap-2 shrink-0">
              <Badge
                variant="outline"
                className={cn('text-[11px] sm:text-[10px] uppercase tracking-wider border', badgeClass)}
              >
                {insight.severity}
              </Badge>
              <Button
                variant="ghost"
                size="icon"
                aria-label="How this insight works"
                className="h-6 w-6 text-muted-foreground hover:text-foreground"
                onClick={() => { setHelpEverOpened(true); setIsHelpOpen(true) }}
                title="How this works"
              >
                <HelpCircle className="h-4 w-4" />
              </Button>
            </div>
          </div>
          <CardDescription className="text-xs mt-1">
            {insight.description}
          </CardDescription>
        </CardHeader>

        <CardContent className="flex-1 flex flex-col pt-0">
          <p className="text-sm font-medium my-2 leading-snug">{insight.summary}</p>

          {insight.items && insight.items.length > 0 && (
            <div className="space-y-1.5 mt-1">
              {keyByOccurrence(insight.items.slice(0, 5), (it) => it.label).map(({ key }, index) => (
                <InsightItemRow
                  key={key}
                  insight={insight}
                  index={index}
                  onMapClick={(data) => setSelectedMapItem(data)}
                  onCacheCollapseClick={(url) => setSelectedCollapseUrl(url)}
                  onScriptedTrafficClick={(data) => setSelectedScriptedItem(data)}
                  onBotFindingClick={(data) => setSelectedBotFindingItem(data)}
                />
              ))}
              {insight.items.length > 5 && (
                <div className="flex justify-center py-1">
                  <Button
                    variant="ghost"
                    size="sm"
                    className="text-[11px] sm:text-[10px] h-6 px-2 text-muted-foreground hover:text-foreground"
                    onClick={() => setIsDataModalOpen(true)}
                  >
                    + Show {insight.items.length - 5} more
                  </Button>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      {helpEverOpened && (
        <InsightHelpModal
          insightId={insight.id}
          isOpen={isHelpOpen}
          onOpenChange={setIsHelpOpen}
        />
      )}

      <InsightDataModal
        insight={insight}
        isOpen={isDataModalOpen}
        onOpenChange={setIsDataModalOpen}
        onMapClick={(data) => setSelectedMapItem(data)}
        onCacheCollapseClick={(url) => setSelectedCollapseUrl(url)}
        onScriptedTrafficClick={(data) => setSelectedScriptedItem(data)}
        onBotFindingClick={(data) => setSelectedBotFindingItem(data)}
      />

      <ImpossibleDistanceModal
        isOpen={!!selectedMapItem}
        onOpenChange={(open) => !open && setSelectedMapItem(null)}
        data={selectedMapItem}
      />

      <CacheCollapseModal
        isOpen={!!selectedCollapseUrl}
        onOpenChange={(open) => !open && setSelectedCollapseUrl(null)}
        url={selectedCollapseUrl}
        windowHours={windowHours}
        baselineHours={baselineHours}
      />

      <ScriptedTrafficModal
        isOpen={!!selectedScriptedItem}
        onOpenChange={(open) => !open && setSelectedScriptedItem(null)}
        data={selectedScriptedItem}
      />

      <BotFindingModal
        isOpen={!!selectedBotFindingItem}
        onOpenChange={(open) => !open && setSelectedBotFindingItem(null)}
        data={selectedBotFindingItem}
      />
    </>
  )
}

const STATUS_OPTIONS = [
  { label: 'All Statuses', value: 'all' },
  { label: 'Critical', value: 'critical' },
  { label: 'Warning', value: 'warning' },
  { label: 'Info', value: 'info' },
  { label: 'Clean', value: 'clean' },
  // `error` severity insights (the insight computation itself failed) were
  // silently dropped from every non-"all" filter without this option.
  { label: 'Error', value: 'error' },
]

// Full per-severity rollup chips (e.g. "2 CRITICAL" "3 WARNING" "4 CLEAN"),
// reusing the exact card palette (SEVERITY_BADGE_CLASS) so the panel header and
// the cards read the same. Shown in the ACTIVE panel header — the whole
// breakdown, not the compact critical/warning-only badges on the tab triggers.
// Renders nothing when there are no known severities (skeleton entries have
// none), so the loading panel header shows only the count.
function SeverityChips({ severities }: { severities: SeveritySummary[] }) {
  if (severities.length === 0) return null
  return (
    <span className="ml-1 flex flex-wrap items-center gap-1">
      {severities.map(({ severity, count }) => (
        <Badge
          key={severity}
          variant="outline"
          className={cn(
            'border px-1.5 py-0 text-[10px] font-medium uppercase tracking-wide tabular-nums',
            SEVERITY_BADGE_CLASS[severity as keyof typeof SEVERITY_BADGE_CLASS] ?? '',
          )}
        >
          {count} {severity}
        </Badge>
      ))}
    </span>
  )
}

// Compact per-tab severity badges: colored count-only pills for the high-signal
// severities (critical=red, warning=amber). clean/info/error are folded away
// here to keep the tab row scannable — the FULL breakdown lives in the active
// panel header (SeverityChips). Screen readers still get the severity name via
// the sr-only span so a bare "2" isn't ambiguous. Colors reuse the AA-cleared
// SEVERITY_BADGE_CLASS palette (axe color-contrast gate).
const TAB_BADGE_SEVERITIES = new Set(['critical', 'warning'])

function TabSeverityBadges({ severities }: { severities: SeveritySummary[] }) {
  const compact = severities.filter((s) => TAB_BADGE_SEVERITIES.has(s.severity))
  // Always reserve one pill's width: the skeleton tab bar (no severities) and the loaded one must
  // be the same width or every tab shifts when /api/insights lands (CLS).
  if (compact.length === 0) return <span aria-hidden="true" className="inline-block min-w-[1.125rem]" />
  return (
    <span className="flex min-w-[1.125rem] items-center gap-1">
      {compact.map(({ severity, count }) => (
        <span
          key={severity}
          className={cn(
            'inline-flex min-w-[1.125rem] items-center justify-center rounded-full border px-1 text-[10px] font-semibold leading-4 tabular-nums',
            SEVERITY_BADGE_CLASS[severity as keyof typeof SEVERITY_BADGE_CLASS] ?? '',
          )}
        >
          {count}
          <span className="sr-only"> {severity}</span>
        </span>
      ))}
    </span>
  )
}

// The tabs UI. One tab per NON-EMPTY section in fixed INSIGHT_SECTIONS order
// (+ Other last), the tab bar rendered from `groups` (skeleton groups during
// load, real groups after, so the row never pops in — CLS). base-ui Tabs give
// roving-tabindex + arrow-key nav for free, and Tabs.Panel UNMOUNTS inactive
// The tabs UI. One tab per section in fixed INSIGHT_SECTIONS order
// (All Categories first, then security, origin, edge, network, traffic, + Other last),
// the tab bar rendered from `groups` (skeleton groups during
// load, real groups after, so the row never pops in — CLS). base-ui Tabs give
// roving-tabindex + arrow-key nav for free, and Tabs.Panel UNMOUNTS inactive
// panels by default (keepMounted defaults false) — so a section's cards only
// render when its tab is active (the "load when I click it" behaviour the user
// asked for, even though /api/insights returns everything in one call).
//
// The active tab is controlled by the parent so the selection survives the
// loading→loaded swap and status-filter changes; `activeKey` falls back to the
// first group whenever the current selection isn't among the rendered groups.
function InsightTabs<T extends SectionableInsight>({
  groups,
  activeTab,
  onTabChange,
  isLoading,
  renderCards,
}: {
  groups: InsightSectionGroup<T>[]
  activeTab: string | null
  onTabChange: (key: string) => void
  isLoading?: boolean
  renderCards: (group: InsightSectionGroup<T>) => React.ReactNode
}) {
  const normalizedActive = normalizeTabKey(activeTab)
  const activeKey =
    groups.some((g) => g.section.key === normalizedActive)
      ? normalizedActive
      : groups[0]?.section.key ?? 'all'

  return (
    <Tabs value={activeKey} onValueChange={(value) => onTabChange(normalizeTabKey(String(value)))}>
      {/* h-auto + flex-wrap so the richer triggers (icon + label + count +
          severity pills) aren't clipped by the primitive's default h-8 and
          wrap instead of overflowing on narrow screens. */}
      <TabsList className="group-data-horizontal/tabs:h-auto w-full flex-wrap justify-start gap-1 p-1">
        {groups.map(({ section, cards }) => {
          const Icon = section.icon
          return (
            <TabsTrigger
              key={section.key}
              value={section.key}
              data-tab={section.key}
              data-category={section.key}
              data-alias={section.key === 'edge' ? 'performance' : section.key === 'network' ? 'routing' : undefined}
              className="h-auto flex-none gap-2 px-3 py-2 data-active:border-border data-active:font-semibold"
            >
              <Icon className="size-4 shrink-0" aria-hidden="true" />
              <span>{section.label}</span>
              <span className="min-w-[2ch] text-right text-xs tabular-nums">{cards.length}</span>
              <TabSeverityBadges severities={summarizeSeverities(cards)} />
            </TabsTrigger>
          )
        })}
      </TabsList>

      {groups.map((group) => {
        const { section, cards } = group
        const Icon = section.icon
        return (
          <TabsContent
            key={section.key}
            value={section.key}
            data-tab-content={section.key}
            data-alias={section.key === 'edge' ? 'performance' : section.key === 'network' ? 'routing' : undefined}
            className="mt-4 space-y-4"
          >
            <div>
              <h2 className="flex flex-wrap items-center gap-2 text-lg font-semibold tracking-tight">
                <Icon className="h-5 w-5 shrink-0 text-muted-foreground" aria-hidden="true" />
                <span>{section.label}</span>
                <span className="text-xs font-normal text-muted-foreground tabular-nums">
                  ({cards.length})
                </span>
                <SeverityChips severities={summarizeSeverities(cards)} />
              </h2>
              <p className="mt-1 text-sm text-muted-foreground">{section.description}</p>
            </div>
            {cards.length === 0 ? (
              isLoading ? (
                <div
                  className="text-center py-20 text-sm text-muted-foreground min-h-[300px] flex flex-col items-center justify-center [contain-intrinsic-size:300px] [content-visibility:auto]"
                  data-testid="insights-loading"
                >
                  <Loader2 className="inline-block animate-spin h-5 w-5 mb-2 text-primary" />
                  <span>{SECTION_LOADING_MESSAGES[section.key] ?? 'Loading insights…'}</span>
                </div>
              ) : (
                <div
                  className="py-16 text-center border rounded-xl border-dashed bg-muted/10 min-h-[300px] flex flex-col items-center justify-center [contain-intrinsic-size:300px] [content-visibility:auto]"
                  data-testid="insights-empty-pass"
                >
                  <CheckCircle className="h-10 w-10 text-emerald-500 mb-3" />
                  <h3 className="text-base font-semibold">
                    {section.key === 'all'
                      ? 'No anomalies detected'
                      : `No anomalies detected in ${section.label}`}
                  </h3>
                  <p className="text-xs text-muted-foreground mt-1 max-w-sm">
                    {section.key === 'all'
                      ? 'All monitored metrics across all categories are operating within normal baseline parameters.'
                      : 'All monitored metrics in this category are operating within normal baseline parameters.'}
                  </p>
                </div>
              )
            ) : (
              renderCards(group)
            )}
          </TabsContent>
        )
      })}
    </Tabs>
  )
}

// Lifted out of the ReportLayout render-prop so the hooks live at the
// top of a stable component instead of being recreated every time
// ReportLayout re-renders. Same shape as DashboardBody (item 30).
// Without this lift, React Query treats every ReportLayout re-render
// as a fresh mount and re-fires the /api/insights + /api/insight-
// availability requests — the local-dev duplicate-fetch pattern.
interface InsightsBodyProps {
  activeServiceId: string | null | undefined
  windowHours: string
  baselineHours: string
  statusFilter: string
  filterPayload?: FiltersPayload
  initialTab?: string | null
  relative: (iso: string) => string
  full: (iso: string) => string
  abbr: () => string
}

function InsightsBody({
  activeServiceId,
  windowHours,
  baselineHours,
  statusFilter,
  filterPayload,
  initialTab,
  relative,
  full,
  abbr,
}: InsightsBodyProps) {
  // Which section tab is active. Controlled here (not inside InsightTabs) so the
  // selection survives the loading→loaded swap between the two InsightTabs
  // instances and the status-filter regrouping.
  const [activeTab, setActiveTab] = useQueryState('tab', parseAsString.withDefault(normalizeTabKey(initialTab)))
  const hasActiveFilters = Boolean(filterPayload && Object.keys(filterPayload).length > 0)

  const { data, isLoading, error } = useQuery({
    queryKey: hasActiveFilters
      ? ['insights', activeServiceId, windowHours, baselineHours, filterPayload]
      : ['insights', activeServiceId, windowHours, baselineHours],
    queryFn: async ({ signal }) => {
      return unwrapClient(client.POST("/api/insights", { signal,
        body: {
          window_size_hrs: parseFloat(windowHours),
          baseline_hours: parseFloat(baselineHours),
          filters: filterPayload ?? {},
          force_refresh: false,
        }
      }))
    },
    enabled: !!activeServiceId,
    staleTime: 60000,
    placeholderData: keepPreviousData,
  })

  const { data: availability, error: availabilityError } = useQuery({
    queryKey: ['insights', 'availability', activeServiceId],
    queryFn: async ({ signal }) => {
      return unwrapClient(client.GET("/api/insight-availability", { signal }))
    },
    enabled: !!activeServiceId,
    // The active-insights list is derived from the service's column schema
    // and effectively never changes within a session. Long-cache it so a
    // warm navigation paints the per-insight skeleton cards instantly
    // instead of flashing an empty state for one round trip.
    staleTime: 5 * 60 * 1000,
  })

  const allInsights = data?.insights
  const filteredInsights = useMemo(() => {
    if (!allInsights) return []
    if (statusFilter === 'all') return allInsights
    return allInsights.filter((insight) => insight.severity === statusFilter)
  }, [allInsights, statusFilter])

  // Skeleton cards rendered while /api/insights is in flight come from the
  // /api/insight-availability response (titles + descriptions per available
  // insight). Single render path during loading — no SkeletonGrid → per-
  // insight swap; the only transition is content-fill when real data lands.
  const availableInsights = useMemo(() => {
    const list = availability?.insights
    if (!list) return []
    return list.filter((i) => i.available !== false)
  }, [availability])

  // Group both real cards and skeleton entries by section, in the same fixed
  // order, so the loading tab bar mirrors the loaded tab bar (CLS). Skeleton
  // entries have no severity → sortInsights orders them by title.
  const groupedInsights = useMemo(
    () =>
      groupInsightsBySection(filteredInsights, {
        includeAll: true,
        includeEmptySections: true,
      }),
    [filteredInsights],
  )
  const groupedSkeletons = useMemo(
    () =>
      groupInsightsBySection(availableInsights, {
        includeAll: true,
        includeEmptySections: true,
      }),
    [availableInsights],
  )

  return (
    <>
      {availability?.unavailable && availability.unavailable.length > 0 && (
        <Alert className="border-amber-500/30 bg-amber-500/5">
          <Info className="h-4 w-4 text-amber-500 shrink-0" />
          <AlertTitle className="text-amber-600 dark:text-amber-400 font-medium">
            {availability.unavailable.length} {availability.unavailable.length === 1 ? 'insight is' : 'insights are'} unavailable
          </AlertTitle>
          <AlertDescription className="text-xs text-muted-foreground mt-1 flex flex-wrap items-center justify-between gap-2">
            <span>
              {availability.unavailable.length} {availability.unavailable.length === 1 ? 'detector requires' : 'detectors require'} optional log fields to be enabled. Check service configuration.
            </span>
            <Link
              href={activeServiceId ? `/service-config?service=${activeServiceId}` : '/service-config'}
              className="inline-flex items-center text-xs font-semibold text-primary underline underline-offset-4 hover:text-primary/80"
            >
              Configure Service Log Fields &rarr;
            </Link>
          </AlertDescription>
        </Alert>
      )}

      {availabilityError && !availability && (
        <Alert>
          <Info className="h-4 w-4" />
          <AlertTitle>Availability check failed</AlertTitle>
          <AlertDescription className="text-xs">
            Couldn&apos;t determine which insights apply to this service — results below may be incomplete.
          </AlertDescription>
        </Alert>
      )}

      {isLoading ? (
        groupedSkeletons.length > 0 ? (
          <InsightTabs
            groups={groupedSkeletons}
            activeTab={activeTab}
            onTabChange={setActiveTab}
            isLoading={true}
            renderCards={({ cards, section }) => (
              <div
                className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
                data-testid="insights-grid-loading"
              >
                {cards.map((i) => (
                  <InsightCardSkeleton
                    key={i.id}
                    title={i.title ?? ''}
                    description={i.description}
                    loadingMessage={SECTION_LOADING_MESSAGES[section.key] ?? 'Analyzing log patterns…'}
                  />
                ))}
              </div>
            )}
          />
        ) : (
          <div className="text-center py-20 text-sm text-muted-foreground min-h-[300px] flex flex-col items-center justify-center [contain-intrinsic-size:300px] [content-visibility:auto]">
            <Loader2 className="inline-block animate-spin h-5 w-5 mb-2 text-primary" />
            <span>Evaluating anomaly detector baseline comparisons…</span>
          </div>
        )
      ) : error ? (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Error loading insights</AlertTitle>
          <AlertDescription>
            {error instanceof Error ? error.message : 'An unknown error occurred'}
          </AlertDescription>
        </Alert>
      ) : (
        <InsightTabs
          groups={groupedInsights}
          activeTab={activeTab}
          onTabChange={setActiveTab}
          isLoading={false}
          renderCards={({ cards }) => (
            <div
              className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
              data-testid="insights-grid"
            >
              {cards.map((insight) => (
                <InsightCard
                  key={insight.id}
                  insight={insight}
                  windowHours={windowHours}
                  baselineHours={baselineHours}
                />
              ))}
            </div>
          )}
        />
      )}

      {data && data.computed_at && (
        <div className="text-[10px] text-muted-foreground text-right italic">
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger render={<span className="" />}>
                Computed {relative(data.computed_at)}
              </TooltipTrigger>
              <TooltipContent className="text-xs">
                {full(data.computed_at)} {abbr()}
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
      )}
    </>
  )
}

export interface InsightsClientProps {
  initialTab?: string | null
  initialWindow?: string | null
  initialBaseline?: string | null
}

export function InsightsClient({
  initialTab,
  initialWindow,
  initialBaseline,
}: InsightsClientProps = {}) {
  // Window/baseline default adapt to how much history the active service has
  // (e.g. ~2h of data → "this hour vs the previous hour" instead of the 7-day
  // baseline that would just say "not enough data"). An explicit pick wins.
  const activeServiceId = useEffectiveServiceId()
  const isRemoteAnalyst = useSessionRoleStore((s) => s.isRemoteAnalyst)
  const defaults = useInsightsDefaults(activeServiceId)
  const filterPayload = useDebouncedFilterPayload(true)

  const [selectedWindow, setSelectedWindow] = useQueryState('window', initialWindow ? parseAsString.withDefault(initialWindow) : parseAsString)
  const [selectedBaseline, setSelectedBaseline] = useQueryState('baseline', initialBaseline ? parseAsString.withDefault(initialBaseline) : parseAsString)

  const windowHours = selectedWindow ?? defaults.windowHours
  const baselineHours = selectedBaseline ?? defaults.baselineHours

  const setWindowHours = (val: string) => {
    void setSelectedWindow(val)
    defaults.setWindowHours(val)
  }
  const setBaselineHours = (val: string) => {
    void setSelectedBaseline(val)
    defaults.setBaselineHours(val)
  }

  const [statusFilter, setStatusFilter] = useQueryState('status', parseAsString.withDefault('all'))
  const [isRescanning, setIsRescanning] = useState(false)
  const { relative, full, abbr } = useDateFormat()

  const queryClient = useQueryClient()
  const { data: extentsData } = useQuery({
    queryKey: queryKeys.logExtents(activeServiceId),
    queryFn: async () => {
      return unwrapClient(client.GET('/api/log-extents'))
    },
    enabled: !!activeServiceId,
    staleTime: 60_000,
  })

  // Pre-load / prefetch secondary month option (24h vs 720h) when enough data exists (>=30d history).
  React.useEffect(() => {
    if (!activeServiceId) return
    const history = historyHoursFromExtents(extentsData?.earliest_log_at)
    if (history !== null && history >= 720) {
      queryClient.prefetchQuery({
        queryKey: ['insights', activeServiceId, '24', '720'],
        queryFn: async ({ signal }) => {
          return unwrapClient(client.POST('/api/insights', {
            signal,
            body: {
              window_size_hrs: 24,
              baseline_hours: 720,
              filters: {},
              force_refresh: false,
            },
          }))
        },
        staleTime: 60_000,
      })
    }
  }, [activeServiceId, extentsData?.earliest_log_at, queryClient])

  // Debounce window/baseline so chained dropdown changes (e.g. switching
  // both window AND baseline in succession) only fire one /api/insights
  // request instead of two. 400ms gives the user enough time to flip
  // both selects without firing the intermediate query.
  const appliedWindowHours = useDebounce(windowHours, 400)
  const appliedBaselineHours = useDebounce(baselineHours, 400)

  const handleRescan = async () => {
    if (!activeServiceId || isRescanning) return
    setIsRescanning(true)
    const hasActiveFilters = Boolean(filterPayload && Object.keys(filterPayload).length > 0)
    try {
      const data = await unwrapClient(client.POST('/api/insights', {
        body: {
          window_size_hrs: parseFloat(appliedWindowHours),
          baseline_hours: parseFloat(appliedBaselineHours),
          filters: filterPayload ?? {},
          force_refresh: true,
        },
      }))
      if (data) {
        queryClient.setQueryData(
          hasActiveFilters
            ? ['insights', activeServiceId, appliedWindowHours, appliedBaselineHours, filterPayload]
            : ['insights', activeServiceId, appliedWindowHours, appliedBaselineHours],
          data,
        )
      }
      await queryClient.invalidateQueries({
        queryKey: ['insights', activeServiceId],
      })
    } catch (err) {
      console.error('Failed to re-scan insights:', err)
    } finally {
      setIsRescanning(false)
    }
  }

  // Compact header controls (Status / Window / vs / Baseline / Re-scan) — rendered
  // in the header row next to the title instead of a separate band below.
  const headerControls = (
    <div className="flex items-end gap-2">
      <div className="flex flex-col gap-1">
        <div className="flex items-center gap-1 ml-1 text-muted-foreground">
          <span className="text-[10px] uppercase font-bold">Status Filter</span>
          <span className="w-3 h-3 shrink-0" />
        </div>
        <Select value={statusFilter} onValueChange={(val) => val && setStatusFilter(val)}>
          <SelectTrigger className="w-[140px]">
            <Filter className="w-3 h-3 mr-2 opacity-50" />
            <SelectValue placeholder="Status" />
          </SelectTrigger>
          <SelectContent>
            {STATUS_OPTIONS.map(o => (
              <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="flex flex-col gap-1 ml-2">
        <div className="flex items-center gap-1 ml-1 text-muted-foreground">
          <span className="text-[10px] uppercase font-bold">Window</span>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger render={<span className=" hover:text-foreground transition-colors shrink-0" />}>
                <Info className="inline-block w-3 h-3 opacity-50" />
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-[200px] text-xs">
                The current time period you want to check for anomalies
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
        <Select value={windowHours} onValueChange={(val) => val && setWindowHours(val)}>
          <SelectTrigger className="w-[160px]">
            <SelectValue placeholder="Window" />
          </SelectTrigger>
          <SelectContent>
            {WINDOW_OPTIONS.map(o => (
              <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <span className="text-xs font-bold text-muted-foreground uppercase tracking-widest pb-2">vs</span>

      <div className="flex flex-col gap-1">
        <div className="flex items-center gap-1 ml-1 text-muted-foreground">
          <span className="text-[10px] uppercase font-bold">Baseline</span>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger render={<span className=" hover:text-foreground transition-colors shrink-0" />}>
                <Info className="inline-block w-3 h-3 opacity-50" />
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-[200px] text-xs">
                The historical period to compare against (acts as the &apos;normal&apos; baseline)
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
        <Select value={baselineHours} onValueChange={(val) => val && setBaselineHours(val)}>
          <SelectTrigger className="w-[160px]">
            <SelectValue placeholder="Baseline" />
          </SelectTrigger>
          <SelectContent>
            {BASELINE_OPTIONS.map(o => (
              <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {!isRemoteAnalyst && (
        <div className="flex flex-col gap-1 ml-2 justify-end">
          <Button
            variant="outline"
            size="sm"
            onClick={handleRescan}
            disabled={isRescanning || !activeServiceId}
            className="h-9 gap-1.5 px-3 text-xs font-medium"
            title="Force re-evaluation of all anomaly detectors against recent logs"
          >
            <RefreshCw className={cn("h-3.5 w-3.5", isRescanning && "animate-spin")} />
            <span>{isRescanning ? 'Scanning…' : 'Re-scan'}</span>
          </Button>
        </div>
      )}
    </div>
  )

  return (
    <ReportLayout
      title="Anomaly Detection"
      description="Automated insights comparing recent traffic to historical baselines."
      icon={Lightbulb}
      headerActions={headerControls}
    >
      {(ctx) => (
        <InsightsBody
          activeServiceId={ctx.activeServiceId}
          windowHours={appliedWindowHours}
          baselineHours={appliedBaselineHours}
          statusFilter={statusFilter}
          filterPayload={ctx.filterPayload}
          initialTab={initialTab}
          relative={relative}
          full={full}
          abbr={abbr}
        />
      )}
    </ReportLayout>
  )
}

export default InsightsClient
