'use client'

import React from 'react'
import Link from 'next/link'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog'
import {
  ScanSearch,
  Bot,
  HardDrive,
  Users,
  Hash,
  Scale,
  Fingerprint,
  Globe,
  ShieldAlert,
  ArrowUpRight,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { formatBytes, formatCount } from '@/lib/format'
import { useServiceStore } from '@/stores/filterStore'
import { InsightStatCard as StatCard, buildInvestigateHref } from '@/components/Insights/InsightCard'
import type { BotFindingModalData } from '@/components/Insights/InsightCard'

interface BotFindingModalProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  data: BotFindingModalData | null
}

export function BotFindingModal({ isOpen, onOpenChange, data }: BotFindingModalProps) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const investigateUrl = data?.item.investigate_url

  const investigateHref = React.useMemo(
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
          <>
            <div className="p-4 rounded-lg bg-muted/30 border border-border">
              <p className="text-sm leading-relaxed">
                <span className="font-semibold">{item.label}</span> executed{' '}
                <span className="font-semibold">{formatCount(m.crawls)}</span> automated requests
                {wBytes > 0 && (
                  <>
                    {' '}consuming <span className="font-semibold">{formatBytes(wBytes)}</span> of egress bandwidth
                    {bwSharePct > 0 && (
                      <>
                        {' '}(<span className="font-semibold">{bwSharePct.toFixed(1)}%</span> of site bandwidth)
                      </>
                    )}
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
                {bytesPerRef > 0 && (
                  <>
                    {' '}and <span className="font-semibold">{formatBytes(bytesPerRef)}/referral</span>
                  </>
                )}
                .
              </p>
            </div>

            <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
              <StatCard
                icon={<Hash className="h-3 w-3" />}
                label="Bot / Crawl Requests"
                value={formatCount(m.crawls)}
                hint="current window"
              />
              <StatCard
                icon={<HardDrive className="h-3 w-3" />}
                label="Bandwidth Used"
                value={formatBytes(wBytes)}
                hint={bwSharePct > 0 ? `${bwSharePct.toFixed(1)}% of site bandwidth` : 'total resp_bytes'}
              />
              <StatCard
                icon={<Users className="h-3 w-3" />}
                label="Human Referrals"
                value={formatCount(m.referrals)}
                valueClass={isZeroReferral ? 'text-red-500' : 'text-amber-500'}
                hint="distinct referred IPs"
              />
              <StatCard
                icon={<Scale className="h-3 w-3" />}
                label="Crawls / Referral"
                value={formatCount(m.crawls_per_referral, { maximumFractionDigits: 1 })}
                valueClass="text-red-500"
                hint={`baseline: ${formatCount(m.baseline_crawls_per_referral, { maximumFractionDigits: 1 })}`}
              />
              <StatCard
                icon={<HardDrive className="h-3 w-3" />}
                label="Bandwidth / Referral"
                value={formatBytes(bytesPerRef)}
                hint={baseBytesPerRef > 0 ? `baseline: ${formatBytes(baseBytesPerRef)}` : 'cost per referred visitor'}
              />
              <StatCard
                icon={<Bot className="h-3 w-3" />}
                label="Site Egress Share"
                value={`${bwSharePct.toFixed(1)}%`}
                hint="of total resp_bytes"
              />
            </div>

            <div className="rounded-lg bg-muted/10 border border-border p-4">
              <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
                <ScanSearch className="h-3.5 w-3.5" /> How we read the evidence
              </h4>
              <ul className="space-y-2.5 text-xs leading-relaxed">
                <li>
                  <span className="font-semibold">Unified bot &amp; crawler attribution.</span>{' '}
                  Requests are grouped dynamically from automated User-Agent tokens — covering search indexers, AI answer crawlers, training scrapers, headless browsers, and programmatic HTTP clients.
                </li>
                <li>
                  <span className="font-semibold">Human referral correlation.</span>{' '}
                  We count distinct client IPs arriving with a non-bot User-Agent and an HTTP <code>Referer</code> matching the platform&apos;s domain.
                </li>
                <li>
                  <span className="font-semibold">Value vs. cost disparity.</span>{' '}
                  High crawl volume and egress bandwidth combined with near-zero referrals means this bot or crawler is harvesting content without returning organic search or discovery traffic.
                </li>
              </ul>
            </div>
          </>
        )
      }

      case 'stale_browser_version': {
        const m = item.meta
        return (
          <>
            <div className="p-4 rounded-lg bg-muted/30 border border-border">
              <p className="text-sm leading-relaxed">
                Clients claiming <span className="font-semibold">{item.label}</span> generated{' '}
                <span className="font-semibold">{formatCount(m.requests)}</span> requests across{' '}
                <span className="font-semibold">{formatCount(m.ips)}</span> distinct IPs — surging{' '}
                <span className="font-semibold text-amber-500">{m.surge_ratio.toFixed(1)}×</span> over baseline.
              </p>
            </div>

            <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
              <StatCard
                icon={<Hash className="h-3 w-3" />}
                label="Requests"
                value={formatCount(m.requests)}
                hint="current window"
              />
              <StatCard
                icon={<Users className="h-3 w-3" />}
                label="Distinct IPs"
                value={formatCount(m.ips)}
                hint="unique client IPs"
              />
              <StatCard
                icon={<Scale className="h-3 w-3" />}
                label="Surge Ratio"
                value={`${m.surge_ratio.toFixed(1)}×`}
                valueClass="text-amber-500"
                hint="vs. baseline rate"
              />
            </div>

            <div className="rounded-lg bg-muted/10 border border-border p-4">
              <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
                <ScanSearch className="h-3.5 w-3.5" /> How we read the evidence
              </h4>
              <ul className="space-y-2.5 text-xs leading-relaxed">
                <li>
                  <span className="font-semibold">Evergreen browser drift.</span>{' '}
                  Real desktop and mobile Chrome installations auto-update silently. Pinned, outdated major versions almost always come from containerized headless browsers (Puppeteer, Playwright, Selenium) or hardcoded User-Agent strings.
                </li>
                <li>
                  <span className="font-semibold">Multi-IP coordination.</span>{' '}
                  {formatCount(m.ips)} source IPs presenting the same outdated browser version indicates a distributed scraping pool sharing a single container image.
                </li>
              </ul>
            </div>
          </>
        )
      }

      case 'orphaned_deep_crawl': {
        const m = item.meta
        return (
          <>
            <div className="p-4 rounded-lg bg-muted/30 border border-border">
              <p className="text-sm leading-relaxed">
                Client IP <span className="font-semibold">{item.label}</span> fetched{' '}
                <span className="font-semibold text-red-500">{formatCount(m.requests)}</span> deep resource requests without an HTTP{' '}
                <code>Referer</code> header across{' '}
                <span className="font-semibold">{formatCount(m.distinct_urls)}</span> distinct URLs (
                <span className="font-semibold">{m.requests_per_url.toFixed(1)}</span> requests/URL,{' '}
                <span className="font-semibold text-amber-500">{m.surge_ratio.toFixed(1)}×</span> baseline).
              </p>
            </div>

            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <StatCard
                icon={<ShieldAlert className="h-3 w-3" />}
                label="Orphan Requests"
                value={formatCount(m.requests)}
                valueClass="text-red-500"
                hint="missing HTTP Referer"
              />
              <StatCard
                icon={<Globe className="h-3 w-3" />}
                label="Distinct URLs"
                value={formatCount(m.distinct_urls)}
                hint="unique deep paths"
              />
              <StatCard
                icon={<Hash className="h-3 w-3" />}
                label="Requests / URL"
                value={m.requests_per_url.toFixed(1)}
                hint="sweep density"
              />
              <StatCard
                icon={<Scale className="h-3 w-3" />}
                label="Surge Ratio"
                value={`${m.surge_ratio.toFixed(1)}×`}
                valueClass="text-amber-500"
                hint="vs. baseline rate"
              />
            </div>

            <div className="rounded-lg bg-muted/10 border border-border p-4">
              <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
                <ScanSearch className="h-3.5 w-3.5" /> How we read the evidence
              </h4>
              <ul className="space-y-2.5 text-xs leading-relaxed">
                <li>
                  <span className="font-semibold">Broken navigation graph.</span>{' '}
                  Genuine users reach deep catalog or detail pages by clicking links from landing or search pages, which populates the <code>Referer</code> header. Hitting hundreds of deep paths directly with an empty referer is a hallmark of queue-driven scrapers.
                </li>
                <li>
                  <span className="font-semibold">URL breadth ({formatCount(m.distinct_urls)} paths).</span>{' '}
                  High path diversity rules out a single bookmarked page or asset refresh and confirms systematic enumeration.
                </li>
              </ul>
            </div>
          </>
        )
      }

      case 'residential_fingerprint_dispersion': {
        const m = item.meta
        return (
          <>
            <div className="p-4 rounded-lg bg-muted/30 border border-border">
              <p className="text-sm leading-relaxed">
                TLS fingerprint <span className="font-mono font-semibold">{m.ja4 || item.label}</span> appeared across{' '}
                <span className="font-semibold text-red-500">{formatCount(m.asns)}</span> distinct ASNs and{' '}
                <span className="font-semibold">{formatCount(m.ips)}</span> unique IPs over{' '}
                <span className="font-semibold">{formatCount(m.requests)}</span> requests (
                <span className="font-semibold text-amber-500">{m.asn_expansion_ratio.toFixed(1)}×</span> ASN expansion).
              </p>
            </div>

            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <StatCard
                icon={<Globe className="h-3 w-3" />}
                label="Distinct ASNs"
                value={formatCount(m.asns)}
                valueClass="text-red-500"
                hint="autonomous systems"
              />
              <StatCard
                icon={<Users className="h-3 w-3" />}
                label="Distinct IPs"
                value={formatCount(m.ips)}
                hint="rotated source IPs"
              />
              <StatCard
                icon={<Hash className="h-3 w-3" />}
                label="Total Requests"
                value={formatCount(m.requests)}
                hint="current window"
              />
              <StatCard
                icon={<Fingerprint className="h-3 w-3" />}
                label="ASN Expansion"
                value={`${m.asn_expansion_ratio.toFixed(1)}×`}
                valueClass="text-amber-500"
                hint={`${formatCount(m.ips / Math.max(1, m.asns), { maximumFractionDigits: 1 })} IPs/ASN`}
              />
            </div>

            <div className="rounded-lg bg-muted/10 border border-border p-4">
              <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
                <ScanSearch className="h-3.5 w-3.5" /> How we read the evidence
              </h4>
              <ul className="space-y-2.5 text-xs leading-relaxed">
                <li>
                  <span className="font-semibold">Residential proxy rotation.</span>{' '}
                  Attackers route requests through residential proxy networks so each request arrives from a different home ISP IP and ASN, evading per-IP rate limits.
                </li>
                <li>
                  <span className="font-semibold">Invariant TLS stack.</span>{' '}
                  Because the underlying scraping binary or headless engine is identical across all proxy exits, the TLS ClientHello fingerprint (JA4/JA3) remains constant across dozens of autonomous systems.
                </li>
              </ul>
            </div>
          </>
        )
      }
    }
  }

  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[90vh] flex flex-col p-6">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-lg font-bold">
            <ScanSearch className="h-5 w-5 text-primary shrink-0" />
            <span className="truncate max-w-[90%]">Finding Details: {item.label}</span>
          </DialogTitle>
          <DialogDescription>
            Detailed telemetry and behavioral evidence behind this bot detection finding.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 min-h-0 flex flex-col gap-6 py-4 overflow-y-auto">
          {renderContent()}
        </div>

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
      </DialogContent>
    </Dialog>
  )
}
