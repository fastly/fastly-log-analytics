'use client'

import React from 'react'
import Link from 'next/link'
import { AlertTriangle, HelpCircle, Loader2 } from 'lucide-react'
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly } from '@/components/DataTable'
import { formatBytes, formatCount } from '@/lib/utils'
import type { components } from '@/types/api.generated'
import type { AssetHelpContent } from './AssetsSummaryAndHelp'

export type AssetsAggregates = components['schemas']['AssetsAggregatesResponse']
export type LargeUncompressedRow = components['schemas']['LargeUncompressedAssetRow']
export type LowTtlRow = components['schemas']['LowTtlAssetRow']

export interface AssetsTableProps<T> {
  rows: T[]
  isLoading: boolean
  /** Dashboard deep link up to and including `filter_url=`; the asset URL is appended encoded. */
  urlHrefPrefix: string
}

export function UrlCell({ url, href }: { url: string; href: string }) {
  return (
    <span className="font-mono text-xs" title={url}>
      <Link
        href={href}
        target="_blank"
        rel="noopener noreferrer"
        className="text-primary hover:underline transition-colors font-medium cursor-pointer"
      >
        {url}
      </Link>
    </span>
  )
}

export function LoadingBlock({ message }: { message: string }) {
  return (
    <div className="py-8 text-center text-muted-foreground text-xs">
      <div className="flex items-center justify-center gap-2">
        <Loader2 className="h-4 w-4 animate-spin text-primary" />
        <span>{message}</span>
      </div>
    </div>
  )
}

export function LargeUncompressedTable({ rows, isLoading, urlHrefPrefix }: AssetsTableProps<LargeUncompressedRow>) {
  const columns = React.useMemo<ColumnDef<LargeUncompressedRow>[]>(
    () => [
      {
        id: 'url',
        header: 'Static Asset URL',
        accessorFn: (r) => r.url,
        size: 320,
        cell: ({ row }) => <UrlCell url={row.original.url} href={`${urlHrefPrefix}${encodeURIComponent(row.original.url)}`} />,
      },
      {
        id: 'requests',
        header: 'Requests',
        accessorFn: (r) => r.requests,
        size: 110,
        cell: ({ row }) => (
          <span className="block text-right text-sm font-medium">{formatCount(row.original.requests)}</span>
        ),
      },
      {
        id: 'avg_bytes',
        header: 'Avg Size',
        accessorFn: (r) => r.avg_bytes ?? 0,
        size: 110,
        cell: ({ row }) => (
          <span className="block text-right text-sm text-muted-foreground">
            {formatBytes(row.original.avg_bytes ?? 0)}
          </span>
        ),
      },
      {
        id: 'total_bytes',
        header: 'Bandwidth',
        accessorFn: (r) => r.total_bytes ?? 0,
        size: 120,
        cell: ({ row }) => (
          <span className="block text-right text-sm font-bold text-rose-500">
            {formatBytes(row.original.total_bytes ?? 0)}
          </span>
        ),
      },
    ],
    [urlHrefPrefix],
  )

  if (isLoading) return <LoadingBlock message="Scanning uncompressed static assets..." />
  return (
    <DataTableReadonly
      columns={columns}
      data={rows}
      hideToolbar
      showPagination={false}
      tableCaption="Large uncompressed static assets"
      emptyMessage="No uncompressed static assets detected! Nice work."
    />
  )
}

export function LowTtlTable({ rows, isLoading, urlHrefPrefix }: AssetsTableProps<LowTtlRow>) {
  const columns = React.useMemo<ColumnDef<LowTtlRow>[]>(
    () => [
      {
        id: 'url',
        header: 'Static Asset URL',
        accessorFn: (r) => r.url,
        size: 320,
        cell: ({ row }) => <UrlCell url={row.original.url} href={`${urlHrefPrefix}${encodeURIComponent(row.original.url)}`} />,
      },
      {
        id: 'asset_type',
        header: 'Type',
        accessorFn: (r) => r.asset_type,
        size: 110,
        cell: ({ row }) => (
          <span className="text-xs font-medium px-2 py-0.5 bg-muted rounded-full">{row.original.asset_type}</span>
        ),
      },
      {
        id: 'requests',
        header: 'Cache Misses',
        accessorFn: (r) => r.requests,
        size: 120,
        cell: ({ row }) => (
          <span className="block text-right text-sm font-bold text-amber-500">
            {formatCount(row.original.requests)}
          </span>
        ),
      },
      {
        id: 'avg_ttl',
        header: 'Avg TTL',
        accessorFn: (r) => r.avg_ttl ?? 0,
        size: 100,
        cell: ({ row }) => (
          <span className="block text-right text-sm text-muted-foreground">
            {row.original.avg_ttl ? `${row.original.avg_ttl.toFixed(1)}s` : '0s'}
          </span>
        ),
      },
    ],
    [urlHrefPrefix],
  )

  if (isLoading) return <LoadingBlock message="Detecting low cache-TTL hotspots..." />
  return (
    <DataTableReadonly
      columns={columns}
      data={rows}
      hideToolbar
      showPagination={false}
      tableCaption="Low cache-TTL static assets"
      emptyMessage="No cache miss hot spots detected."
    />
  )
}

export function AssetsHotspotsSection({
  largeUncompressed,
  lowTtl,
  isLoading,
  urlFilterHrefPrefix,
  onSelectHelp,
}: {
  largeUncompressed: LargeUncompressedRow[]
  lowTtl: LowTtlRow[]
  isLoading: boolean
  urlFilterHrefPrefix: string
  onSelectHelp: (help: AssetHelpContent) => void
}) {
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      {/* Large Uncompressed Assets table */}
      <div
        data-testid="assets-uncompressed-card"
        className="bg-card text-card-foreground border border-border rounded-xl p-6 shadow-sm overflow-hidden flex flex-col min-h-[320px] [contain-intrinsic-size:360px] [content-visibility:auto]"
      >
        <h3 className="font-bold text-lg mb-2 flex items-center gap-2 text-rose-500">
          <AlertTriangle className="h-5 w-5" />
          Large Uncompressed Assets
          <button
            onClick={() => onSelectHelp({
              title: 'Large Uncompressed Assets',
              calculated: 'Identified by scanning static asset requests (with text-based compressible headers like Javascript, CSS, HTML, SVG, Fonts, JSON) that were served without any gzip or Brotli (br) encoding where the file size exceeds 50KB.',
              means: 'Lists specific raw files that are being delivered completely uncompressed, consuming massive excess CDN egress bandwidth and dragging down user load speed performance.',
              action: 'Verify your Fastly Gzip/Brotli rules are active for this content-type. If the file is extremely large, check if your origin server is stripping or missing Accept-Encoding headers, or check if the asset has a Cache-Control header preventing compression.',
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
        <LargeUncompressedTable rows={largeUncompressed} isLoading={isLoading} urlHrefPrefix={urlFilterHrefPrefix} />
      </div>

      {/* Low Cache-TTL table */}
      <div
        data-testid="assets-low-ttl-card"
        className="bg-card text-card-foreground border border-border rounded-xl p-6 shadow-sm overflow-hidden flex flex-col min-h-[320px] [contain-intrinsic-size:360px] [content-visibility:auto]"
      >
        <h3 className="font-bold text-lg mb-2 flex items-center gap-2 text-amber-500">
          <AlertTriangle className="h-5 w-5" />
          Low Cache-TTL Hotspots
          <button
            onClick={() => onSelectHelp({
              title: 'Low Cache-TTL Hotspots',
              calculated: 'Calculated by isolating static assets that suffer recurring Cache MISSes within the select window, indicating that the cache time-to-live is extremely low or set to 0.',
              means: 'Indicates specific files that the CDN is forced to evict from local cache memory constantly, triggering repeated, costly roundtrips all the way to your origin database.',
              action: "Add or increase the public max-age and s-maxage values inside your origin's Cache-Control header (e.g. public, max-age=31536000, immutable). Alternatively, define a custom Fastly cache header override rule in your VCL configuration.",
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
        <LowTtlTable rows={lowTtl} isLoading={isLoading} urlHrefPrefix={urlFilterHrefPrefix} />
      </div>
    </div>
  )
}
