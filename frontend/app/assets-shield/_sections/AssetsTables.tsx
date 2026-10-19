'use client'

import React from 'react'
import Link from 'next/link'
import { Loader2 } from 'lucide-react'
import type { ColumnDef } from '@tanstack/react-table'

import { DataTableReadonly } from '@/components/DataTable/DataTableReadonly'
import { formatBytes, formatCount } from '@/lib/format'
import type { components } from '@/types/api.generated'

type LargeUncompressedRow = components['schemas']['LargeUncompressedAssetRow']
type LowTtlRow = components['schemas']['LowTtlAssetRow']

interface AssetsTableProps<T> {
  rows: T[]
  isLoading: boolean
  /** Dashboard deep link up to and including `filter_url=`; the asset URL is appended encoded. */
  urlHrefPrefix: string
}

function UrlCell({ url, href }: { url: string; href: string }) {
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

function LoadingBlock({ message }: { message: string }) {
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
