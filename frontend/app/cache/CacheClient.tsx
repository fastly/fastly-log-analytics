'use client'

import React, { useCallback, useEffect, useState } from 'react'
import {
  AlertTriangle,
  Database,
  HardDrive,
  Layers,
  Loader2,
  RefreshCw,
  Trash2,
} from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { formatBytes, showToast } from '@/lib/utils'
import { formatCount, formatLocalDateTime } from '@/lib/format'
import { IS_CLOUD_MODE } from '@/lib/cloud-engine/mode'
import {
  clearAllBrowserCaches,
  inspectBrowserCaches,
  type BrowserCacheSnapshot,
} from '@/lib/cloud-engine/dashboard/browser-cache-stats'
import { OPFS_DIRECTORY } from '@/lib/cloud-engine/dashboard/store-index'

function formatPercent(usage: number | null, quota: number | null): string {
  if (usage === null || quota === null || quota <= 0) return '—'
  const pct = (usage / quota) * 100
  return pct < 0.1 && pct > 0 ? '<0.1%' : `${pct.toFixed(1)}%`
}

function quotaPercentValue(usage: number | null, quota: number | null): number {
  if (usage === null || quota === null || quota <= 0) return 0
  return Math.min(100, Math.max(0, (usage / quota) * 100))
}

export function CacheClient() {
  const [snapshot, setSnapshot] = useState<BrowserCacheSnapshot | null>(null)
  const [loading, setLoading] = useState<boolean>(IS_CLOUD_MODE)
  const [clearing, setClearing] = useState<boolean>(false)
  const [error, setError] = useState<string | null>(null)

  const refresh = useCallback(async () => {
    if (!IS_CLOUD_MODE) return
    setLoading(true)
    setError(null)
    try {
      const next = await inspectBrowserCaches()
      setSnapshot(next)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to inspect browser cache')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    if (!IS_CLOUD_MODE) return
    let cancelled = false
    inspectBrowserCaches()
      .then((next) => {
        if (!cancelled) setSnapshot(next)
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to inspect browser cache')
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const handleClear = async () => {
    setClearing(true)
    setError(null)
    try {
      const next = await clearAllBrowserCaches()
      setSnapshot(next)
      showToast('Browser cache cleared', 'success')
    } catch (err) {
      const message =
        err instanceof Error ? err.message : 'Failed to clear browser cache'
      setError(message)
      showToast(message, 'error')
    } finally {
      setClearing(false)
    }
  }

  const cacheStorageEntries =
    snapshot?.caches.reduce((sum, b) => sum + b.entries, 0) ?? 0
  const cacheStorageBytes =
    snapshot?.caches.reduce((sum, b) => sum + b.bytes, 0) ?? 0
  const quotaPct = quotaPercentValue(
    snapshot?.usage ?? null,
    snapshot?.quota ?? null,
  )

  return (
    <div className="max-w-5xl mx-auto space-y-6" data-testid="browser-cache-page">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div className="space-y-1">
          <h1 className="text-2xl font-bold tracking-tight flex items-center gap-2">
            <HardDrive className="h-6 w-6 text-primary" aria-hidden="true" />
            Browser Cache
          </h1>
          <p className="text-sm text-muted-foreground">
            {IS_CLOUD_MODE
              ? 'Cloud caches Parquet log files, manifests, and query rollups locally in this browser to keep dashboards fast and avoid re-downloading data.'
              : 'Browser Parquet caching is only used in Cloud deployments. High-Scale queries ClickHouse directly on the server.'}
          </p>
        </div>
        {IS_CLOUD_MODE && (
          <div className="flex items-center gap-2 shrink-0">
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => {
                void refresh()
              }}
              disabled={loading || clearing}
              data-testid="refresh-browser-cache-btn"
            >
              <RefreshCw
                className={`h-4 w-4 mr-2 ${loading ? 'animate-spin' : ''}`}
                aria-hidden="true"
              />
              Refresh
            </Button>
            <Button
              type="button"
              variant="destructive"
              size="sm"
              onClick={() => {
                void handleClear()
              }}
              disabled={clearing}
              data-testid="clear-browser-cache-btn"
            >
              {clearing ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" aria-hidden="true" />
              ) : (
                <Trash2 className="h-4 w-4 mr-2" aria-hidden="true" />
              )}
              Clear Browser Cache
            </Button>
          </div>
        )}
      </div>

      {!IS_CLOUD_MODE ? (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Not applicable in High-Scale mode</CardTitle>
            <CardDescription>
              This deployment runs against ClickHouse and does not store Parquet files in browser storage.
            </CardDescription>
          </CardHeader>
        </Card>
      ) : (
        <>
          {error && (
            <div
              role="alert"
              className="flex items-start gap-3 rounded-lg border border-destructive/40 bg-destructive/10 p-4 text-sm text-destructive"
            >
              <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" aria-hidden="true" />
              <span>{error}</span>
            </div>
          )}

          {snapshot?.lastError && (
            <div
              role="status"
              data-testid="browser-cache-last-error"
              className="flex items-start gap-3 rounded-lg border border-amber-500/40 bg-amber-500/10 p-4 text-sm"
            >
              <AlertTriangle
                className="h-4 w-4 mt-0.5 shrink-0 text-amber-600 dark:text-amber-400"
                aria-hidden="true"
              />
              <div className="space-y-1">
                <div className="font-medium text-foreground">
                  Last cache write warning ({snapshot.lastError.cache})
                </div>
                <div className="text-muted-foreground font-mono text-xs break-all">
                  {snapshot.lastError.message}
                </div>
                <div className="text-xs text-muted-foreground">
                  File: <span className="font-mono">{snapshot.lastError.key}</span> ·{' '}
                  {formatBytes(snapshot.lastError.bytes)} ·{' '}
                  {formatLocalDateTime(snapshot.lastError.at)}
                </div>
              </div>
            </div>
          )}

          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <Card>
              <CardHeader className="pb-2">
                <CardDescription className="flex items-center justify-between">
                  <span>Cached Data</span>
                  <HardDrive className="h-4 w-4 text-muted-foreground" aria-hidden="true" />
                </CardDescription>
                <CardTitle
                  className="text-2xl tabular-nums"
                  data-testid="browser-cache-total-bytes"
                >
                  {snapshot ? formatBytes(snapshot.totalCachedBytes) : '—'}
                </CardTitle>
              </CardHeader>
              <CardContent className="text-xs text-muted-foreground tabular-nums">
                {snapshot
                  ? `${formatCount(snapshot.totalCachedEntries)} cached ${snapshot.totalCachedEntries === 1 ? 'item' : 'items'}`
                  : 'Measuring…'}
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-2">
                <CardDescription className="flex items-center justify-between">
                  <span>Origin Storage Quota</span>
                  {snapshot?.persisted !== null && snapshot?.persisted !== undefined && (
                    <Badge variant={snapshot.persisted ? 'secondary' : 'outline'}>
                      {snapshot.persisted ? 'Persistent' : 'Best-effort'}
                    </Badge>
                  )}
                </CardDescription>
                <CardTitle
                  className="text-2xl tabular-nums"
                  data-testid="browser-cache-quota-usage"
                >
                  {snapshot?.usage != null ? formatBytes(snapshot.usage) : '—'}
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-xs text-muted-foreground">
                <div className="flex items-center justify-between tabular-nums">
                  <span>
                    of {snapshot?.quota != null ? formatBytes(snapshot.quota) : 'unknown quota'}
                  </span>
                  <span className="font-medium text-foreground">
                    {formatPercent(snapshot?.usage ?? null, snapshot?.quota ?? null)}
                  </span>
                </div>
                <div className="h-1.5 w-full rounded-full bg-muted overflow-hidden">
                  <div
                    className="h-full bg-primary transition-all"
                    style={{ width: `${quotaPct}%` }}
                  />
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-2">
                <CardDescription className="flex items-center justify-between">
                  <span>OPFS Parquet Store</span>
                  <Database className="h-4 w-4 text-muted-foreground" aria-hidden="true" />
                </CardDescription>
                <CardTitle
                  className="text-2xl tabular-nums"
                  data-testid="browser-cache-opfs-bytes"
                >
                  {snapshot
                    ? snapshot.opfs.available
                      ? formatBytes(snapshot.opfs.bytes)
                      : 'Unavailable'
                    : '—'}
                </CardTitle>
              </CardHeader>
              <CardContent className="text-xs text-muted-foreground tabular-nums">
                {snapshot
                  ? snapshot.opfs.available
                    ? `${formatCount(snapshot.opfs.files)} ${snapshot.opfs.files === 1 ? 'file' : 'files'} in ${OPFS_DIRECTORY}`
                    : 'Origin Private File System not supported in this browser'
                  : 'Measuring…'}
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-2">
                <CardDescription className="flex items-center justify-between">
                  <span>Cache Storage</span>
                  <Layers className="h-4 w-4 text-muted-foreground" aria-hidden="true" />
                </CardDescription>
                <CardTitle
                  className="text-2xl tabular-nums"
                  data-testid="browser-cache-storage-bytes"
                >
                  {snapshot ? formatBytes(cacheStorageBytes) : '—'}
                </CardTitle>
              </CardHeader>
              <CardContent className="text-xs text-muted-foreground tabular-nums">
                {snapshot
                  ? `${formatCount(cacheStorageEntries)} ${cacheStorageEntries === 1 ? 'entry' : 'entries'} across ${snapshot.caches.length} ${snapshot.caches.length === 1 ? 'bucket' : 'buckets'}`
                  : 'Measuring…'}
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Storage Breakdown</CardTitle>
              <CardDescription>
                Per-store usage on this device. Clearing the cache removes local copies only — server-side data in Fastly Object Storage is never modified.
              </CardDescription>
            </CardHeader>
            <CardContent>
              {loading && !snapshot ? (
                <div className="flex items-center gap-2 text-sm text-muted-foreground py-6">
                  <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
                  Inspecting browser storage…
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table
                    className="w-full text-sm"
                    data-testid="browser-cache-breakdown-table"
                  >
                    <thead>
                      <tr className="border-b text-left text-xs text-muted-foreground">
                        <th className="py-2 pr-4 font-medium">Store</th>
                        <th className="py-2 pr-4 font-medium">Identifier</th>
                        <th className="py-2 pr-4 font-medium text-right">Items</th>
                        <th className="py-2 font-medium text-right">Size</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y">
                      {snapshot?.opfs.available && (
                        <tr>
                          <td className="py-2.5 pr-4 font-medium">
                            OPFS Parquet Store
                          </td>
                          <td className="py-2.5 pr-4 font-mono text-xs text-muted-foreground">
                            {OPFS_DIRECTORY}
                          </td>
                          <td className="py-2.5 pr-4 text-right tabular-nums">
                            {formatCount(snapshot.opfs.files)}
                          </td>
                          <td className="py-2.5 text-right font-mono text-xs tabular-nums">
                            {formatBytes(snapshot.opfs.bytes)}
                          </td>
                        </tr>
                      )}
                      {snapshot?.caches.map((bucket) => (
                        <tr key={bucket.name}>
                          <td className="py-2.5 pr-4 font-medium">{bucket.label}</td>
                          <td className="py-2.5 pr-4 font-mono text-xs text-muted-foreground">
                            {bucket.name}
                          </td>
                          <td className="py-2.5 pr-4 text-right tabular-nums">
                            {formatCount(bucket.entries)}
                          </td>
                          <td className="py-2.5 text-right font-mono text-xs tabular-nums">
                            {formatBytes(bucket.bytes)}
                          </td>
                        </tr>
                      ))}
                      {snapshot &&
                        !snapshot.opfs.available &&
                        snapshot.caches.length === 0 && (
                          <tr>
                            <td
                              colSpan={4}
                              className="py-6 text-center text-muted-foreground"
                            >
                              No browser caches currently stored.
                            </td>
                          </tr>
                        )}
                    </tbody>
                  </table>
                </div>
              )}
            </CardContent>
          </Card>
        </>
      )}
    </div>
  )
}
