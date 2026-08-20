'use client'

import React from 'react'
import Link from 'next/link'
import { client } from '@/lib/api'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { useFilterStore } from '@/stores/filterStore'
import { quantizeAnchor } from '@/lib/time-window'
import { resolveRangeWire } from '@/lib/range-wire'
import { Layers, Database, HelpCircle, Archive, Zap, AlertTriangle, FileText } from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { HelpDialog } from '@/components/ui/help-dialog'
import { Tooltip, TooltipTrigger, TooltipContent, TooltipProvider } from '@/components/ui/tooltip'


// Helper to format byte counts into human-readable strings
function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

// Helper to format percentages
function formatPercent(ratio: number): string {
  return `${(ratio * 100).toFixed(1)}%`
}

export function AssetsReportContent({
  startTime,
  endTime,
  timezone,
  activeServiceId,
  filterPayload,
  config,
  relativeRange,
  isAutoRange,
  anchor,
}: any) {
  const bundle = useServiceQuery(
    ['assets', 'aggregates', activeServiceId, relativeRange || '24h', anchor, filterPayload],
    async ({ signal }) => {
      const { rangeKey, rangeBody } = resolveRangeWire({ relativeRange, isAutoRange, startTime, endTime, anchor })
      const { data } = await client.POST('/api/assets/aggregates', {
        signal,
        body: {
          filters: filterPayload,
          ...rangeBody,
        },
      })
      return data as any
    },
  )

  const isLoading = bundle.isLoading
  const isFetching = bundle.isFetching
  const error = bundle.error
  const data = bundle.data

  const getTimeQueryString = React.useCallback(() => {
    if (relativeRange) {
      return `&range=${encodeURIComponent(relativeRange)}`
    }
    if (startTime && endTime) {
      return `&start_time=${encodeURIComponent(startTime)}&end_time=${encodeURIComponent(endTime)}`
    }
    return ''
  }, [relativeRange, startTime, endTime])

  const getFilterQueryString = React.useCallback((assetType: string) => {
    const images = ["*.png", "*.jpg", "*.jpeg", "*.gif", "*.svg", "*.webp", "*.ico"]
    const documents = ["*.pdf", "*.doc", "*.docx", "*.xls", "*.xlsx", "*.ppt", "*.pptx", "*.txt", "*.csv", "*.zip", "*.tar", "*.gz", "*.tgz", "*.rar", "*.7z"]
    const jscss = ["*.js", "*.mjs", "*.css"]
    const fonts = ["*.woff", "*.woff2", "*.ttf", "*.otf", "*.eot"]
    const video = ["*.m3u8", "*.ts", "*.mp4", "*.m4s", "*.webm"]

    let filterPayload: any = {}

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
  }, [])

  const [activeHelp, setActiveHelp] = React.useState<{
    title: string
    calculated: string
    means: string
    action: string
  } | null>(null)

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center py-24 text-center">
        <AlertTriangle className="h-12 w-12 text-destructive mb-4" />
        <h3 className="text-lg font-bold">Error Loading Assets Data</h3>
        <p className="text-muted-foreground mt-2">There was a problem querying the database.</p>
      </div>
    )
  }

  if (isLoading) {
    return (
      <div className="grid grid-cols-1 gap-6 md:grid-cols-2 lg:grid-cols-3 py-6">
        {[1, 2, 3, 4, 5].map((i) => (
          <div key={i} className="animate-pulse bg-card border border-border rounded-xl p-6 h-48" />
        ))}
      </div>
    )
  }

  const breakdown = data?.asset_type_breakdown || []
  const cachePerf = data?.cache_performance || []
  const compPerf = data?.compression_performance || []
  const largeUncompressed = data?.large_uncompressed_assets || []
  const lowTtl = data?.low_ttl_assets || []

  const hasData = breakdown.length > 0

  if (!hasData) {
    return (
      <div className="flex flex-col items-center justify-center py-24 text-center">
        <Layers className="h-16 w-16 text-muted-foreground mb-4" />
        <h3 className="text-lg font-bold">No Asset Metrics Yet</h3>
        <p className="text-muted-foreground max-w-md mt-2">
          No static assets or dynamic pages were requested in this time window.
        </p>
      </div>
    )
  }

  // Helper to map cache statuses to pretty colors
  const cacheColors: Record<string, string> = {
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

  // Helper to map compression types to colors
  const compColors: Record<string, string> = {
    br: 'bg-indigo-500',
    gzip: 'bg-blue-500',
    uncompressed: 'bg-slate-300 dark:bg-slate-700',
  }

  return (
    <div className="space-y-8 py-6">
      {/* SECTION 1: Overview Cards Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {breakdown.map((row: any) => {
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
              className="bg-card text-card-foreground border border-border rounded-xl p-6 shadow-sm hover:shadow-md transition-shadow flex flex-col justify-between"
            >
              <div className="flex items-center justify-between mb-4">
                <h4 className="font-semibold text-lg flex items-center gap-2">
                  <IconComponent className="h-5 w-5 text-primary" />
                  {row.asset_type}
                  <button
                    onClick={() => setActiveHelp({
                      title: `${row.asset_type} Overview Details`,
                      calculated: `Requests are categorized as "${row.asset_type}" based on extension pattern matching against the lowercase request URL. Images: (.png, .jpg, .jpeg, .gif, .svg, .webp, .ico). Documents: (.pdf, .doc, .docx, .xls, .xlsx, .ppt, .pptx, .txt, .csv, .zip, .tar, .gz, .tgz, .rar, .7z). JavaScript/CSS: (.js, .mjs, .css). Fonts: (.woff, .woff2, .ttf, .otf, .eot). Video: (.m3u8, .ts, .mp4, .m4s, .webm). Fallback: API/Dynamic. Values represent aggregate metrics for this group.`,
                      means: "Shows high-level volume, cache absorption, and network payload optimization for this asset group. Highly valuable for checking general content-type health.",
                      action: "Aim for a Cache Hit Ratio > 90% for Images, JS/CSS, and Fonts. If the compression rate is low, double-check that Fastly's dynamic compression features (Gzip and Brotli) are enabled, and verify that origin server headers correctly classify the Content-Type."
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
                  {row.requests.toLocaleString()} reqs
                </Link>
              </div>

              <div className="grid grid-cols-3 gap-2 pt-2 border-t border-border">
                <div>
                  <div className="text-[10px] uppercase tracking-wider text-muted-foreground mb-0.5">Egress</div>
                  <div className="text-sm font-bold truncate">{formatBytes(row.egress_bytes)}</div>
                </div>
                <div>
                  <div className="text-[10px] uppercase tracking-wider text-muted-foreground mb-0.5">Cache Hit</div>
                  <div className="text-sm font-bold text-emerald-500">{formatPercent(row.cache_hit_ratio)}</div>
                </div>
                <div>
                  <div className="text-[10px] uppercase tracking-wider text-muted-foreground mb-0.5">Compression</div>
                  <div className="text-sm font-bold text-indigo-500">{formatPercent(row.compression_rate)}</div>
                </div>
              </div>
            </div>
          )
        })}
      </div>

      {/* SECTION 2: Stacked Visual Performance charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Cache Efficiency stacked horizontal bars */}
        <div className="bg-card text-card-foreground border border-border rounded-xl p-6 shadow-sm">
          <h3 className="font-bold text-lg mb-6 flex items-center gap-2">
            <Zap className="h-5 w-5 text-primary" />
            Cache Efficiency by Asset Type
            <button
              onClick={() => setActiveHelp({
                title: "Cache Efficiency Breakdown",
                calculated: "Calculated using the Fastly cache state field logged for each request (HIT, MISS, PASS, BYPASS, HIT-SYNTHETIC), grouped per asset category.",
                means: "Shows exactly how much asset request volume is absorbed by the CDN cache vs. how much goes back to origin. HIT represents local delivery, HIT-SYNTHETIC is a local custom/redirect, MISS is a first-time fetch, and PASS/BYPASS mean cache-bypass occurred.",
                action: "Maintain a HIT rate > 90% on static assets. If PASS or BYPASS ratios are high, review your Fastly VCL settings to ensure cache-bypass rules are restricted strictly to user-dynamic endpoints."
              })}
              className="text-muted-foreground hover:text-primary transition-colors focus:outline-none p-0.5 rounded hover:bg-muted"
              title="Click for calculation details"
            >
              <HelpCircle className="h-4 w-4" />
            </button>
          </h3>
          <div className="space-y-6">
            <TooltipProvider delay={100}>
              {breakdown.map((row: any) => {
                const type = row.asset_type
                const typeStatuses = cachePerf.filter((c: any) => c.asset_type === type)
                const totalRequests = typeStatuses.reduce((acc: number, curr: any) => acc + curr.requests, 0)

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
                        {totalRequests.toLocaleString()} requests
                      </Link>
                    </div>
                    <div className="h-6 w-full bg-muted rounded-md overflow-hidden flex">
                      {typeStatuses.map((statusRow: any) => {
                        const pct = totalRequests > 0 ? (statusRow.requests / totalRequests) * 100 : 0
                        if (pct === 0) return null
                        return (
                          <Tooltip key={statusRow.cache_status}>
                            <TooltipTrigger
                              render={
                                <div
                                  style={{ width: `${pct}%` }}
                                  className={`${cacheColors[statusRow.cache_status] || 'bg-slate-500'} h-full transition-all relative group flex items-center justify-center text-[10px] font-bold text-white truncate px-1 cursor-help`}
                                >
                                  {pct > 12 && statusRow.cache_status}
                                </div>
                              }
                            />
                            <TooltipContent side="top" className="text-xs font-semibold">
                              {statusRow.cache_status}: {statusRow.requests.toLocaleString()} ({pct.toFixed(1)}%)
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
            {Object.keys(cacheColors).map((status) => (
              <div key={status} className="flex items-center gap-1.5">
                <div className={`h-3 w-3 rounded-full ${cacheColors[status]}`} />
                <span className="font-medium text-muted-foreground">{status}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Compression Efficiency stacked horizontal bars */}
        <div className="bg-card text-card-foreground border border-border rounded-xl p-6 shadow-sm">
          <h3 className="font-bold text-lg mb-6 flex items-center gap-2">
            <Archive className="h-5 w-5 text-primary" />
            Payload Compression Efficiency
            <button
              onClick={() => setActiveHelp({
                title: "Payload Compression Breakdown",
                calculated: "Calculated by reading the returned Content-Encoding headers from the delivery log (br = Brotli, gzip = Gzip, uncompressed = none) for all requests in each category.",
                means: "Indicates how successfully payload sizes are minimized. Brotli is the gold standard for text assets, followed closely by Gzip. Uncompressed assets waste heavy network bandwidth.",
                action: "Ensure your Fastly service features Brotli and Gzip compression rules enabled for text types. If dynamic content has uncompressed segments, verify that origin responses carry a valid Content-Type so the edge can compress it."
              })}
              className="text-muted-foreground hover:text-primary transition-colors focus:outline-none p-0.5 rounded hover:bg-muted"
              title="Click for calculation details"
            >
              <HelpCircle className="h-4 w-4" />
            </button>
          </h3>
          <div className="space-y-6">
            <TooltipProvider delay={100}>
              {breakdown.map((row: any) => {
                const type = row.asset_type
                const typeComp = compPerf.filter((c: any) => c.asset_type === type)
                const totalRequests = typeComp.reduce((acc: number, curr: any) => acc + curr.requests, 0)

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
                        {totalRequests.toLocaleString()} requests
                      </Link>
                    </div>
                    <div className="h-6 w-full bg-muted rounded-md overflow-hidden flex">
                      {typeComp.map((compRow: any) => {
                        const pct = totalRequests > 0 ? (compRow.requests / totalRequests) * 100 : 0
                        if (pct === 0) return null
                        return (
                          <Tooltip key={compRow.content_encoding}>
                            <TooltipTrigger
                              render={
                                <div
                                  style={{ width: `${pct}%` }}
                                  className={`${compColors[compRow.content_encoding] || 'bg-slate-500'} h-full transition-all relative group flex items-center justify-center text-[10px] font-bold text-white truncate px-1 cursor-help`}
                                >
                                  {pct > 12 && compRow.content_encoding}
                                </div>
                              }
                            />
                            <TooltipContent side="top" className="text-xs font-semibold">
                              {compRow.content_encoding}: {compRow.requests.toLocaleString()} ({pct.toFixed(1)}%)
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
            {Object.keys(compColors).map((enc) => (
              <div key={enc} className="flex items-center gap-1.5">
                <div className={`h-3 w-3 rounded-full ${compColors[enc]}`} />
                <span className="font-medium text-muted-foreground">{enc}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* SECTION 3: Hotspots tables */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Large Uncompressed Assets table */}
        <div className="bg-card text-card-foreground border border-border rounded-xl p-6 shadow-sm overflow-hidden flex flex-col">
          <h3 className="font-bold text-lg mb-2 flex items-center gap-2 text-rose-500">
            <AlertTriangle className="h-5 w-5" />
            Large Uncompressed Assets
            <button
              onClick={() => setActiveHelp({
                title: "Large Uncompressed Assets",
                calculated: "Identified by scanning static asset requests (with text-based compressible headers like Javascript, CSS, HTML, SVG, Fonts, JSON) that were served without any gzip or Brotli (br) encoding where the file size exceeds 50KB.",
                means: "Lists specific raw files that are being delivered completely uncompressed, consuming massive excess CDN egress bandwidth and dragging down user load speed performance.",
                action: "Verify your Fastly Gzip/Brotli rules are active for this content-type. If the file is extremely large, check if your origin server is stripping or missing Accept-Encoding headers, or check if the asset has a Cache-Control header preventing compression."
              })}
              className="text-muted-foreground hover:text-primary transition-colors focus:outline-none p-0.5 rounded hover:bg-muted"
              title="Click for calculation details"
            >
              <HelpCircle className="h-4 w-4" />
            </button>
          </h3>
          <p className="text-xs text-muted-foreground mb-4">
            Compressible assets served without compression (br or gzip), consuming excessive origin & CDN bandwidth.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-sm text-left border-collapse">
              <thead>
                <tr className="border-b border-border text-xs text-muted-foreground uppercase font-medium">
                  <th className="py-3 px-2">Static Asset URL</th>
                  <th className="py-3 px-2 text-right">Requests</th>
                  <th className="py-3 px-2 text-right">Avg Size</th>
                  <th className="py-3 px-2 text-right">Bandwidth</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {largeUncompressed.length === 0 ? (
                  <tr>
                    <td colSpan={4} className="py-4 text-center text-muted-foreground text-xs">
                      No uncompressed static assets detected! Nice work.
                    </td>
                  </tr>
                ) : (
                  largeUncompressed.map((row: any, idx: number) => (
                    <tr key={idx} className="hover:bg-muted/50 transition-colors">
                      <td className="py-3 px-2 max-w-[200px] truncate font-mono text-xs" title={row.url}>
                        <Link
                          href={`/dashboard?service=${activeServiceId}${getTimeQueryString()}&filter_url=${encodeURIComponent(row.url)}`}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-primary hover:underline transition-colors font-medium cursor-pointer"
                        >
                          {row.url}
                        </Link>
                      </td>
                      <td className="py-3 px-2 text-right font-medium">{row.requests.toLocaleString()}</td>
                      <td className="py-3 px-2 text-right text-muted-foreground">{formatBytes(row.avg_bytes)}</td>
                      <td className="py-3 px-2 text-right font-bold text-rose-500">{formatBytes(row.total_bytes)}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* Low Cache-TTL table */}
        <div className="bg-card text-card-foreground border border-border rounded-xl p-6 shadow-sm overflow-hidden flex flex-col">
          <h3 className="font-bold text-lg mb-2 flex items-center gap-2 text-amber-500">
            <AlertTriangle className="h-5 w-5" />
            Low Cache-TTL Hotspots
            <button
              onClick={() => setActiveHelp({
                title: "Low Cache-TTL Hotspots",
                calculated: "Calculated by isolating static assets that suffer recurring Cache MISSes within the select window, indicating that the cache time-to-live is extremely low or set to 0.",
                means: "Indicates specific files that the CDN is forced to evict from local cache memory constantly, triggering repeated, costly roundtrips all the way to your origin database.",
                action: "Add or increase the public max-age and s-maxage values inside your origin's Cache-Control header (e.g. public, max-age=31536000, immutable). Alternatively, define a custom Fastly cache header override rule in your VCL configuration."
              })}
              className="text-muted-foreground hover:text-primary transition-colors focus:outline-none p-0.5 rounded hover:bg-muted"
              title="Click for calculation details"
            >
              <HelpCircle className="h-4 w-4" />
            </button>
          </h3>
          <p className="text-xs text-muted-foreground mb-4">
            Static resources receiving repeated Cache MISSes, potentially due to low/zero TTL configurations.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-sm text-left border-collapse">
              <thead>
                <tr className="border-b border-border text-xs text-muted-foreground uppercase font-medium">
                  <th className="py-3 px-2">Static Asset URL</th>
                  <th className="py-3 px-2">Type</th>
                  <th className="py-3 px-2 text-right">Cache Misses</th>
                  <th className="py-3 px-2 text-right">Avg TTL</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {lowTtl.length === 0 ? (
                  <tr>
                    <td colSpan={4} className="py-4 text-center text-muted-foreground text-xs">
                      No cache miss hot spots detected.
                    </td>
                  </tr>
                ) : (
                  lowTtl.map((row: any, idx: number) => (
                    <tr key={idx} className="hover:bg-muted/50 transition-colors">
                      <td className="py-3 px-2 max-w-[200px] truncate font-mono text-xs" title={row.url}>
                        <Link
                          href={`/dashboard?service=${activeServiceId}${getTimeQueryString()}&filter_url=${encodeURIComponent(row.url)}`}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-primary hover:underline transition-colors font-medium cursor-pointer"
                        >
                          {row.url}
                        </Link>
                      </td>
                      <td className="py-3 px-2">
                        <span className="text-xs font-medium px-2 py-0.5 bg-muted rounded-full">
                          {row.asset_type}
                        </span>
                      </td>
                      <td className="py-3 px-2 text-right font-bold text-amber-500">{row.requests.toLocaleString()}</td>
                      <td className="py-3 px-2 text-right text-muted-foreground">
                        {row.avg_ttl ? `${row.avg_ttl.toFixed(1)}s` : '0s'}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* Platform-standard Help Dialog Component */}
      <HelpDialog
        open={!!activeHelp}
        onOpenChange={(open) => {
          if (!open) setActiveHelp(null)
        }}
        title={activeHelp?.title || "Metric Details"}
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
                onClick={() => setActiveHelp(null)}
                className="px-5 py-2.5 bg-primary text-primary-foreground font-semibold rounded-lg text-sm hover:bg-primary/90 hover:scale-[1.02] active:scale-[0.98] transition-all focus:outline-none"
              >
                Got it, close
              </button>
            </div>
          </div>
        )}
      </HelpDialog>
    </div>
  )
}

export default function AssetsClient() {
  const relativeRange = useFilterStore((s) => s.relativeRange)
  const isAutoRange = useFilterStore((s) => s.isAutoRange)
  const storeEndTime = useFilterStore((s) => s.endTime)

  const anchor = React.useMemo(() => {
    return quantizeAnchor(storeEndTime)
  }, [storeEndTime])

  return (
    <ReportLayout
      title="Assets & Shield"
      description="Static asset delivery analysis, payload compression rates, and caching performance analyzer."
      icon={Layers}
      defaultInterval="1 hour"
    >
      {(props) => (
        <AssetsReportContent
          {...props}
          relativeRange={relativeRange}
          isAutoRange={isAutoRange}
          anchor={anchor}
        />
      )}
    </ReportLayout>
  )
}
