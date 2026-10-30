'use client'

import React from 'react'
import Link from 'next/link'
import { AlertTriangle, Archive, Database, FileText, HelpCircle, Layers, Loader2, Zap } from 'lucide-react'
import { HelpDialog } from '@/components/ui/confirm-dialog'
import { Skeleton } from '@/components/ui/skeleton'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { formatBytes, formatCount, formatPercent } from '@/lib/utils'
import type { FiltersPayload } from '@/types/api'
import type { AssetsAggregates } from './AssetsTables'

export type AssetTypeBreakdownRow = NonNullable<AssetsAggregates['asset_type_breakdown']>[number]
export type CachePerformanceRow = NonNullable<AssetsAggregates['cache_performance']>[number]
export type CompressionPerformanceRow = NonNullable<AssetsAggregates['compression_performance']>[number]

export interface AssetHelpContent {
  title: string
  calculated: string
  means: string
  action: string
}

export const CACHE_STATUS_COLORS: Record<string, string> = {
  HIT: 'bg-emerald-600',
  'HIT-SYNTHETIC': 'bg-emerald-400',
  'HIT-CLUSTER': 'bg-teal-500',
  'HIT-STALE': 'bg-yellow-500',
  'HIT-STALE-CLUSTER': 'bg-yellow-400',
  MISS: 'bg-amber-500',
  'MISS-CLUSTER': 'bg-orange-500',
  PASS: 'bg-rose-600',
  'PASS-WAIT': 'bg-rose-400',
  BYPASS: 'bg-slate-400',
  ERROR: 'bg-red-600',
  'ERROR-WAIT': 'bg-zinc-600',
}

export const COMPRESSION_COLORS: Record<string, string> = {
  br: 'bg-indigo-500',
  gzip: 'bg-blue-500',
  uncompressed: 'bg-slate-300 dark:bg-slate-700',
}

export function buildAssetFilterQueryString(assetType: string): string {
  const images = ['*.png', '*.jpg', '*.jpeg', '*.gif', '*.svg', '*.webp', '*.ico']
  const documents = ['*.pdf', '*.doc', '*.docx', '*.xls', '*.xlsx', '*.ppt', '*.pptx', '*.txt', '*.csv', '*.zip', '*.tar', '*.gz', '*.tgz', '*.rar', '*.7z']
  const jscss = ['*.js', '*.mjs', '*.css']
  const fonts = ['*.woff', '*.woff2', '*.ttf', '*.otf', '*.eot']
  const video = ['*.m3u8', '*.ts', '*.mp4', '*.m4s', '*.webm']

  let filterPayload: FiltersPayload = {}

  if (assetType === 'Images') {
    filterPayload = { url: { mode: 'include', values: images } }
  } else if (assetType === 'Documents') {
    filterPayload = { url: { mode: 'include', values: documents } }
  } else if (assetType === 'JavaScript/CSS') {
    filterPayload = { url: { mode: 'include', values: jscss } }
  } else if (assetType === 'Fonts') {
    filterPayload = { url: { mode: 'include', values: fonts } }
  } else if (assetType === 'Video') {
    filterPayload = { url: { mode: 'include', values: video } }
  } else if (assetType === 'API/Dynamic') {
    const allStatic = [...images, ...documents, ...jscss, ...fonts, ...video]
    filterPayload = { url: { mode: 'exclude', values: allStatic } }
  } else {
    return ''
  }

  return `&filters=${encodeURIComponent(JSON.stringify(filterPayload))}`
}

export function MissingAssetFieldsBanner({
  missingFields,
  activeServiceId,
}: {
  missingFields: string[]
  activeServiceId: string | null
}) {
  if (missingFields.length === 0) return null
  return (
    <div
      data-testid="missing-asset-fields-banner"
      className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-4 text-sm text-amber-800 dark:text-amber-200 flex items-start gap-3"
    >
      <AlertTriangle className="h-5 w-5 text-amber-500 shrink-0 mt-0.5" />
      <div className="space-y-1">
        <div className="font-semibold">Optional Log Fields Missing</div>
        <p className="text-xs text-amber-700 dark:text-amber-300">
          Advanced asset analytics works best when all static asset headers are captured. The following optional fields are currently not present in your service log format: <span className="font-mono font-bold">{missingFields.join(', ')}</span>.
        </p>
        <Link
          href={`/admin?service=${activeServiceId}`}
          className="inline-flex items-center text-xs font-semibold text-primary underline hover:text-primary/80 mt-1"
        >
          Configure Service Log Fields &rarr;
        </Link>
      </div>
    </div>
  )
}

export function AssetsOverviewGrid({
  breakdown,
  isLoading,
  activeServiceId,
  getTimeQueryString,
  getFilterQueryString,
  onSelectHelp,
}: {
  breakdown: AssetTypeBreakdownRow[]
  isLoading: boolean
  activeServiceId: string | null
  getTimeQueryString: () => string
  getFilterQueryString: (assetType: string) => string
  onSelectHelp: (help: AssetHelpContent) => void
}) {
  return (
    <div
      data-testid="assets-overview-grid"
      className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 min-h-[160px] lg:min-h-[304px] [contain-intrinsic-size:160px] [content-visibility:auto]"
    >
      {isLoading ? (
        ['Images', 'Documents', 'JavaScript/CSS', 'Fonts', 'Video', 'API/Dynamic'].map((type) => (
          <div
            key={type}
            className="bg-card text-card-foreground border border-border rounded-xl p-6 shadow-sm flex flex-col justify-between h-[140px]"
          >
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2">
                <Skeleton className="h-5 w-5 rounded-full" />
                <span className="font-semibold text-lg">{type}</span>
              </div>
              <Skeleton className="h-5 w-16 rounded-full" />
            </div>
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <Loader2 className="h-3.5 w-3.5 animate-spin text-primary" />
              <span>Analyzing static asset cache hit ratios...</span>
            </div>
            <div className="grid grid-cols-3 gap-2 pt-2 border-t border-border">
              <Skeleton className="h-8 w-full" />
              <Skeleton className="h-8 w-full" />
              <Skeleton className="h-8 w-full" />
            </div>
          </div>
        ))
      ) : breakdown.length === 0 ? (
        <div className="col-span-full flex flex-col items-center justify-center py-12 text-center bg-card border border-border rounded-xl p-6 min-h-[304px]">
          <Layers className="h-12 w-12 text-muted-foreground mb-3" />
          <h3 className="text-base font-bold">No Asset Metrics Yet</h3>
          <p className="text-xs text-muted-foreground max-w-md mt-1">
            No static assets or dynamic pages were requested in this time window.
          </p>
        </div>
      ) : (
        breakdown.map((row) => {
          let icon = HelpCircle
          if (row.asset_type === 'Images') icon = Layers
          else if (row.asset_type === 'Documents') icon = FileText
          else if (row.asset_type === 'JavaScript/CSS') icon = Archive
          else if (row.asset_type === 'Video') icon = Zap
          else if (row.asset_type === 'Fonts') icon = Database

          const IconComponent = icon

          return (
            <div
              key={row.asset_type}
              className="bg-card text-card-foreground border border-border rounded-xl p-6 shadow-sm hover:shadow-md transition-shadow flex flex-col justify-between h-[140px]"
            >
              <div className="flex items-center justify-between mb-4">
                <h4 className="font-semibold text-lg flex items-center gap-2">
                  <IconComponent className="h-5 w-5 text-primary" />
                  {row.asset_type}
                  <button
                    onClick={() => onSelectHelp({
                      title: `${row.asset_type} Overview Details`,
                      calculated: `Requests are categorized as "${row.asset_type}" based on extension pattern matching against the lowercase request URL. Images: (.png, .jpg, .jpeg, .gif, .svg, .webp, .ico). Documents: (.pdf, .doc, .docx, .xls, .xlsx, .ppt, .pptx, .txt, .csv, .zip, .tar, .gz, .tgz, .rar, .7z). JavaScript/CSS: (.js, .mjs, .css). Fonts: (.woff, .woff2, .ttf, .otf, .eot). Video: (.m3u8, .ts, .mp4, .m4s, .webm). Fallback: API/Dynamic. Values represent aggregate metrics for this group.`,
                      means: 'Shows high-level volume, cache absorption, and network payload optimization for this asset group. Highly valuable for checking general content-type health.',
                      action: "Aim for a Cache Hit Ratio > 90% for Images, JS/CSS, and Fonts. If the compression rate is low, double-check that Fastly's dynamic compression features (Gzip and Brotli) are enabled, and verify that origin server headers correctly classify the Content-Type.",
                    })}
                    className="text-muted-foreground hover:text-primary transition-colors focus:outline-none p-0.5 rounded hover:bg-muted"
                    title="Click for calculation details"
                  >
                    <HelpCircle className="h-4 w-4" />
                  </button>
                </h4>
                <Link
                  href={`/dashboard?service=${activeServiceId}${getTimeQueryString()}${getFilterQueryString(row.asset_type)}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-xs font-medium text-muted-foreground px-2.5 py-1 bg-muted hover:bg-primary hover:text-primary-foreground hover:shadow-sm cursor-pointer transition-all rounded-full"
                  title="Click to view these requests on the Dashboard"
                >
                  {formatCount(row.requests)} reqs
                </Link>
              </div>

              <div className="grid grid-cols-3 gap-2 pt-2 border-t border-border">
                <div>
                  <div className="text-[10px] uppercase tracking-wider text-muted-foreground mb-0.5">Egress</div>
                  <div className="text-sm font-bold truncate">{formatBytes(row.egress_bytes ?? 0)}</div>
                </div>
                <div>
                  <div className="text-[10px] uppercase tracking-wider text-muted-foreground mb-0.5">Cache Hit</div>
                  <div className="text-sm font-bold text-emerald-500">{formatPercent(row.cache_hit_ratio ?? 0)}</div>
                </div>
                <div>
                  <div className="text-[10px] uppercase tracking-wider text-muted-foreground mb-0.5">Compression</div>
                  <div className="text-sm font-bold text-indigo-500">{formatPercent(row.compression_rate ?? 0)}</div>
                </div>
              </div>
            </div>
          )
        })
      )}
    </div>
  )
}

export function AssetsEfficiencySection({
  breakdown,
  cachePerf,
  compPerf,
  isLoading,
  activeServiceId,
  getTimeQueryString,
  getFilterQueryString,
  onSelectHelp,
}: {
  breakdown: AssetTypeBreakdownRow[]
  cachePerf: CachePerformanceRow[]
  compPerf: CompressionPerformanceRow[]
  isLoading: boolean
  activeServiceId: string | null
  getTimeQueryString: () => string
  getFilterQueryString: (assetType: string) => string
  onSelectHelp: (help: AssetHelpContent) => void
}) {
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      {/* Cache Efficiency stacked horizontal bars */}
      <div
        data-testid="assets-cache-efficiency-card"
        className="bg-card text-card-foreground border border-border rounded-xl p-6 shadow-sm min-h-[380px] [contain-intrinsic-size:420px] [content-visibility:auto]"
      >
        <h3 className="font-bold text-lg mb-6 flex items-center gap-2">
          <Zap className="h-5 w-5 text-primary" />
          Cache Efficiency by Asset Type
          <button
            onClick={() => onSelectHelp({
              title: 'Cache Efficiency Breakdown',
              calculated: 'Calculated using the Fastly cache state field logged for each request (HIT, MISS, PASS, BYPASS, HIT-SYNTHETIC), grouped per asset category.',
              means: 'Shows exactly how much asset request volume is absorbed by the CDN cache vs. how much goes back to origin. HIT represents local delivery, HIT-SYNTHETIC is a local custom/redirect, MISS is a first-time fetch, and PASS/BYPASS mean cache-bypass occurred.',
              action: 'Maintain a HIT rate > 90% on static assets. If PASS or BYPASS ratios are high, review your Fastly VCL settings to ensure cache-bypass rules are restricted strictly to user-dynamic endpoints.',
            })}
            className="text-muted-foreground hover:text-primary transition-colors focus:outline-none p-0.5 rounded hover:bg-muted"
            title="Click for calculation details"
          >
            <HelpCircle className="h-4 w-4" />
          </button>
        </h3>
        {isLoading ? (
          <div className="flex flex-col items-center justify-center py-16 gap-3 text-muted-foreground">
            <Loader2 className="h-6 w-6 animate-spin text-primary" />
            <span className="text-sm font-medium">Evaluating cache absorption efficiency...</span>
          </div>
        ) : (!cachePerf || cachePerf.length === 0) ? (
          <div className="flex flex-col items-center justify-center py-16 text-center text-muted-foreground text-sm">
            No cache efficiency metrics recorded in this window.
          </div>
        ) : (
          <>
            <div className="space-y-6">
              <TooltipProvider delay={100}>
                {breakdown.map((row) => {
                  const type = row.asset_type
                  const typeStatuses = cachePerf.filter((c) => c.asset_type === type)
                  const totalRequests = typeStatuses.reduce((acc, curr) => acc + curr.requests, 0)

                  return (
                    <div key={type} className="space-y-2">
                      <div className="flex justify-between items-center text-sm">
                        <span className="font-semibold">{type}</span>
                        <Link
                          href={`/dashboard?service=${activeServiceId}${getTimeQueryString()}${getFilterQueryString(type)}`}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-muted-foreground text-xs hover:text-primary hover:underline transition-colors cursor-pointer font-medium"
                          title="Click to view these requests on the Dashboard"
                        >
                          {formatCount(totalRequests)} requests
                        </Link>
                      </div>
                      <div className="h-6 w-full bg-muted rounded-md overflow-hidden flex">
                        {typeStatuses.map((statusRow) => {
                          const pct = totalRequests > 0 ? (statusRow.requests / totalRequests) * 100 : 0
                          if (pct === 0) return null
                          return (
                            <Tooltip key={statusRow.cache_status}>
                              <TooltipTrigger
                                render={
                                  <div
                                    style={{ width: `${pct}%` }}
                                    className={`${CACHE_STATUS_COLORS[statusRow.cache_status] || 'bg-slate-500'} h-full transition-all relative group flex items-center justify-center text-[10px] font-bold text-white truncate px-1 cursor-help`}
                                  >
                                    {pct > 12 && statusRow.cache_status}
                                  </div>
                                }
                              />
                              <TooltipContent side="top" className="text-xs font-semibold">
                                {statusRow.cache_status}: {formatCount(statusRow.requests)} ({pct.toFixed(1)}%)
                              </TooltipContent>
                            </Tooltip>
                          )
                        })}
                      </div>
                    </div>
                  )
                })}
              </TooltipProvider>
            </div>
            {/* Legend */}
            <div className="flex flex-wrap gap-4 mt-6 pt-4 border-t border-border text-xs justify-center">
              {Object.keys(CACHE_STATUS_COLORS).map((status) => (
                <div key={status} className="flex items-center gap-1.5">
                  <div className={`h-3 w-3 rounded-full ${CACHE_STATUS_COLORS[status]}`} />
                  <span className="font-medium text-muted-foreground">{status}</span>
                </div>
              ))}
            </div>
          </>
        )}
      </div>

      {/* Compression Efficiency stacked horizontal bars */}
      <div
        data-testid="assets-compression-card"
        className="bg-card text-card-foreground border border-border rounded-xl p-6 shadow-sm min-h-[380px] [contain-intrinsic-size:420px] [content-visibility:auto]"
      >
        <h3 className="font-bold text-lg mb-6 flex items-center gap-2">
          <Archive className="h-5 w-5 text-primary" />
          Payload Compression Efficiency
          <button
            onClick={() => onSelectHelp({
              title: 'Payload Compression Breakdown',
              calculated: 'Calculated by reading the returned Content-Encoding headers from the delivery log (br = Brotli, gzip = Gzip, uncompressed = none) for all requests in each category.',
              means: 'Indicates how successfully payload sizes are minimized. Brotli is the gold standard for text assets, followed closely by Gzip. Uncompressed assets waste heavy network bandwidth.',
              action: 'Ensure your Fastly service features Brotli and Gzip compression rules enabled for text types. If dynamic content has uncompressed segments, verify that origin responses carry a valid Content-Type so the edge can compress it.',
            })}
            className="text-muted-foreground hover:text-primary transition-colors focus:outline-none p-0.5 rounded hover:bg-muted"
            title="Click for calculation details"
          >
            <HelpCircle className="h-4 w-4" />
          </button>
        </h3>
        {isLoading ? (
          <div className="flex flex-col items-center justify-center py-16 gap-3 text-muted-foreground">
            <Loader2 className="h-6 w-6 animate-spin text-primary" />
            <span className="text-sm font-medium">Evaluating payload compression efficiency...</span>
          </div>
        ) : (!compPerf || compPerf.length === 0) ? (
          <div className="flex flex-col items-center justify-center py-16 text-center text-muted-foreground text-sm">
            No compression performance metrics recorded in this window.
          </div>
        ) : (
          <>
            <div className="space-y-6">
              <TooltipProvider delay={100}>
                {breakdown.map((row) => {
                  const type = row.asset_type
                  const typeComp = compPerf.filter((c) => c.asset_type === type)
                  const totalRequests = typeComp.reduce((acc, curr) => acc + curr.requests, 0)

                  return (
                    <div key={type} className="space-y-2">
                      <div className="flex justify-between items-center text-sm">
                        <span className="font-semibold">{type}</span>
                        <Link
                          href={`/dashboard?service=${activeServiceId}${getTimeQueryString()}${getFilterQueryString(type)}`}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-muted-foreground text-xs hover:text-primary hover:underline transition-colors cursor-pointer font-medium"
                          title="Click to view these requests on the Dashboard"
                        >
                          {formatCount(totalRequests)} requests
                        </Link>
                      </div>
                      <div className="h-6 w-full bg-muted rounded-md overflow-hidden flex">
                        {typeComp.map((compRow) => {
                          const pct = totalRequests > 0 ? (compRow.requests / totalRequests) * 100 : 0
                          if (pct === 0) return null
                          return (
                            <Tooltip key={compRow.content_encoding}>
                              <TooltipTrigger
                                render={
                                  <div
                                    style={{ width: `${pct}%` }}
                                    className={`${COMPRESSION_COLORS[compRow.content_encoding] || 'bg-slate-500'} h-full transition-all relative group flex items-center justify-center text-[10px] font-bold text-white truncate px-1 cursor-help`}
                                  >
                                    {pct > 12 && compRow.content_encoding}
                                  </div>
                                }
                              />
                              <TooltipContent side="top" className="text-xs font-semibold">
                                {compRow.content_encoding}: {formatCount(compRow.requests)} ({pct.toFixed(1)}%)
                              </TooltipContent>
                            </Tooltip>
                          )
                        })}
                      </div>
                    </div>
                  )
                })}
              </TooltipProvider>
            </div>
            {/* Legend */}
            <div className="flex flex-wrap gap-4 mt-6 pt-4 border-t border-border text-xs justify-center">
              {Object.keys(COMPRESSION_COLORS).map((enc) => (
                <div key={enc} className="flex items-center gap-1.5">
                  <div className={`h-3 w-3 rounded-full ${COMPRESSION_COLORS[enc]}`} />
                  <span className="font-medium text-muted-foreground">{enc}</span>
                </div>
              ))}
            </div>
          </>
        )}
      </div>
    </div>
  )
}

export function AssetsHelpModal({
  activeHelp,
  onClose,
}: {
  activeHelp: AssetHelpContent | null
  onClose: () => void
}) {
  return (
    <HelpDialog
      open={!!activeHelp}
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title={activeHelp?.title || 'Metric Details'}
      icon={<HelpCircle className="h-5 w-5" />}
      size="xl"
    >
      {activeHelp && (
        <div className="space-y-6">
          {/* Information Rows */}
          <div className="space-y-4 text-left">
            {/* How it is calculated */}
            <div className="space-y-1.5">
              <h4 className="text-xs uppercase tracking-wider text-muted-foreground font-semibold flex items-center gap-1.5">
                <span className="h-1.5 w-1.5 rounded-full bg-blue-500" />
                How it is calculated
              </h4>
              <p className="text-sm leading-relaxed text-foreground/90 bg-muted/30 p-3 rounded-lg border border-border/50">
                {activeHelp.calculated}
              </p>
            </div>

            {/* What it means */}
            <div className="space-y-1.5">
              <h4 className="text-xs uppercase tracking-wider text-muted-foreground font-semibold flex items-center gap-1.5">
                <span className="h-1.5 w-1.5 rounded-full bg-amber-500" />
                What it means
              </h4>
              <p className="text-sm leading-relaxed text-foreground/90 bg-muted/30 p-3 rounded-lg border border-border/50">
                {activeHelp.means}
              </p>
            </div>

            {/* What to do with it */}
            <div className="space-y-1.5">
              <h4 className="text-xs uppercase tracking-wider text-muted-foreground font-semibold flex items-center gap-1.5">
                <span className="h-1.5 w-1.5 rounded-full bg-emerald-500" />
                What to do with it
              </h4>
              <p className="text-sm leading-relaxed text-foreground/90 bg-emerald-500/10 text-emerald-800 dark:text-emerald-300 p-3 rounded-lg border border-emerald-500/20">
                {activeHelp.action}
              </p>
            </div>
          </div>

          {/* Action Footer */}
          <div className="flex justify-end pt-2 border-t border-border">
            <button
              onClick={onClose}
              className="px-5 py-2.5 bg-primary text-primary-foreground font-semibold rounded-lg text-sm hover:bg-primary/90 hover:scale-[1.02] active:scale-[0.98] transition-all focus:outline-none"
            >
              Got it, close
            </button>
          </div>
        </div>
      )}
    </HelpDialog>
  )
}
