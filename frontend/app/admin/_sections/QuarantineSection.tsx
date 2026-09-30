'use client'

import * as React from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, Download, FileWarning, Loader2, Trash2 } from 'lucide-react'

import { CardErrorState } from '@/components/CardErrorState'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { adminFetch } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import type { components } from '@/types/api.generated'

type QuarantineSummary = components['schemas']['QuarantineSummary']
type QuarantineListResponse = components['schemas']['QuarantineListResponse']
type QuarantineItem = components['schemas']['QuarantineEvidenceItem']

const PAGE_SIZE = 20

export function QuarantineSection() {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const queryClient = useQueryClient()
  const [showDetails, setShowDetails] = React.useState(false)
  const [errorCategory, setErrorCategory] = React.useState('')
  const [offset, setOffset] = React.useState(0)
  const [workingItemId, setWorkingItemId] = React.useState<number | null>(null)
  const [purgingAll, setPurgingAll] = React.useState(false)
  const [actionError, setActionError] = React.useState<string | null>(null)

  const summaryQuery = useQuery<QuarantineSummary>({
    queryKey: ['admin', 'quarantine', 'summary', activeServiceId],
    queryFn: async ({ signal }) => {
      const response = await adminFetch('/api/admin/quarantine/summary', { signal })
      if (!response.ok) throw new Error(`Unable to load quarantine summary (HTTP ${response.status})`)
      return response.json()
    },
    enabled: !!activeServiceId,
    staleTime: 30_000,
    refetchInterval: 60_000,
    refetchIntervalInBackground: false,
  })

  const listQuery = useQuery<QuarantineListResponse>({
    queryKey: ['admin', 'quarantine', 'list', activeServiceId, errorCategory, offset],
    queryFn: async ({ signal }) => {
      const params = new URLSearchParams({ limit: String(PAGE_SIZE), offset: String(offset) })
      if (errorCategory) params.set('error_category', errorCategory)
      const response = await adminFetch(`/api/admin/quarantine?${params}`, { signal })
      if (!response.ok) throw new Error(`Unable to load quarantine items (HTTP ${response.status})`)
      return response.json()
    },
    enabled: !!activeServiceId && showDetails,
    staleTime: 30_000,
    refetchInterval: 60_000,
    refetchIntervalInBackground: false,
  })

  if (!activeServiceId) return null

  if (summaryQuery.isError) {
    return (
      <Card>
        <CardContent className="pt-4">
          <CardErrorState
            title="Quarantine unavailable"
            message={summaryQuery.error.message}
            onRetry={() => void summaryQuery.refetch()}
          />
        </CardContent>
      </Card>
    )
  }

  const summary = summaryQuery.data
  const totalItems = summary?.total_items ?? 0
  if (!summaryQuery.isPending && totalItems === 0 && !showDetails) return null

  const invalidateQuarantine = async () => {
    await queryClient.invalidateQueries({ queryKey: ['admin', 'quarantine'] })
  }

  const handleDownload = async (item: QuarantineItem) => {
    setWorkingItemId(item.id)
    setActionError(null)
    try {
      const response = await adminFetch(`/api/admin/quarantine/download/${item.id}`)
      if (!response.ok) throw new Error(`Unable to download evidence (HTTP ${response.status})`)
      const url = URL.createObjectURL(await response.blob())
      const anchor = document.createElement('a')
      anchor.href = url
      anchor.download = `quarantine-item-${item.id}.dat`
      anchor.click()
      URL.revokeObjectURL(url)
    } catch (error) {
      setActionError(error instanceof Error ? error.message : 'Unable to download evidence.')
    } finally {
      setWorkingItemId(null)
    }
  }

  const handlePurge = async (itemId?: number) => {
    setPurgingAll(itemId === undefined)
    setWorkingItemId(itemId ?? null)
    setActionError(null)
    try {
      const query = itemId === undefined ? '' : `?item_id=${itemId}`
      const response = await adminFetch(`/api/admin/quarantine/purge${query}`, { method: 'POST' })
      if (!response.ok) throw new Error(`Unable to purge quarantine evidence (HTTP ${response.status})`)
      await invalidateQuarantine()
    } catch (error) {
      setActionError(error instanceof Error ? error.message : 'Unable to purge quarantine evidence.')
    } finally {
      setPurgingAll(false)
      setWorkingItemId(null)
    }
  }

  const categories = Object.keys(summary?.category_counts ?? {}).sort()
  const listedItems = listQuery.data?.items ?? []

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <CardTitle className="flex items-center gap-2 text-base">
            {totalItems > 0 ? (
              <AlertTriangle className="h-4 w-4 text-amber-500" />
            ) : (
              <FileWarning className="h-4 w-4 text-muted-foreground" />
            )}
            Ingest Quarantine
            <Badge variant="outline" className="text-xs text-amber-700 dark:text-amber-400 border-amber-500/40">
              {totalItems} item{totalItems !== 1 ? 's' : ''}
            </Badge>
          </CardTitle>
          <div className="flex items-center gap-2">
            <Button
              variant="ghost"
              size="sm"
              className="text-xs"
              onClick={() => setShowDetails(prev => !prev)}
            >
              {showDetails ? 'Hide details' : 'Details'}
            </Button>
            {totalItems > 0 && (
              <Button
                variant="outline"
                size="sm"
                className="text-xs"
                onClick={() => void handlePurge()}
                disabled={purgingAll}
              >
                {purgingAll && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
                {!purgingAll && <Trash2 className="h-3 w-3 mr-1" />}
                {purgingAll ? 'Purging…' : 'Purge all'}
              </Button>
            )}
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-3 pt-0">
        {summaryQuery.isPending ? (
          <p className="text-sm text-muted-foreground">Loading quarantine summary…</p>
        ) : (
          <>
            <div className="flex flex-wrap gap-x-6 gap-y-1 text-sm">
              <div>
                <span className="text-muted-foreground">Stored size: </span>
                <span className="font-medium tabular-nums">{formatBytes(summary?.total_bytes ?? 0)}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Request / RUM: </span>
                <span className="font-medium tabular-nums">
                  {summary?.request_items ?? 0} / {summary?.rum_items ?? 0}
                </span>
              </div>
              {summary?.oldest_at && (
                <div>
                  <span className="text-muted-foreground">Oldest: </span>
                  <span className="font-medium">{new Date(summary.oldest_at).toLocaleString()}</span>
                </div>
              )}
            </div>
            {showDetails && (
              <>
                <div className="flex flex-wrap items-center gap-3">
                  <label htmlFor="quarantine-category" className="text-sm font-medium">Error category</label>
                  <select
                    id="quarantine-category"
                    className="h-9 rounded-md border bg-background px-3 text-sm"
                    value={errorCategory}
                    onChange={event => {
                      setErrorCategory(event.target.value)
                      setOffset(0)
                    }}
                  >
                    <option value="">All categories</option>
                    {categories.map(category => <option key={category} value={category}>{category}</option>)}
                  </select>
                </div>
                {actionError && <p role="alert" className="text-sm text-destructive">{actionError}</p>}
                {listQuery.isError ? (
                  <CardErrorState
                    title="Quarantine items unavailable"
                    message={listQuery.error.message}
                    onRetry={() => void listQuery.refetch()}
                  />
                ) : listQuery.isPending ? (
                  <p className="text-sm text-muted-foreground">Loading quarantine items…</p>
                ) : listedItems.length === 0 ? (
                  <p className="text-sm text-muted-foreground">No matching quarantine items.</p>
                ) : (
                  <div className="space-y-2">
                    {listedItems.map(item => (
                      <QuarantineItemRow
                        key={item.id}
                        item={item}
                        working={workingItemId === item.id}
                        onDownload={() => void handleDownload(item)}
                        onPurge={() => void handlePurge(item.id)}
                      />
                    ))}
                    <div className="flex items-center justify-between">
                      <span className="text-xs text-muted-foreground">
                        {offset + 1}–{offset + listedItems.length} of {listQuery.data?.total ?? 0}
                      </span>
                      <div className="flex gap-2">
                        <Button
                          variant="outline"
                          size="sm"
                          disabled={offset === 0 || listQuery.isFetching}
                          onClick={() => setOffset(current => Math.max(0, current - PAGE_SIZE))}
                        >
                          Previous
                        </Button>
                        <Button
                          variant="outline"
                          size="sm"
                          disabled={offset + PAGE_SIZE >= (listQuery.data?.total ?? 0) || listQuery.isFetching}
                          onClick={() => setOffset(current => current + PAGE_SIZE)}
                        >
                          Next
                        </Button>
                      </div>
                    </div>
                  </div>
                )}
              </>
            )}
          </>
        )}
      </CardContent>
    </Card>
  )
}

function QuarantineItemRow({
  item,
  working,
  onDownload,
  onPurge,
}: {
  item: QuarantineItem
  working: boolean
  onDownload: () => void
  onPurge: () => void
}) {
  return (
    <div className="rounded-md border p-3">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0 space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="outline">{item.source_type}</Badge>
            <Badge variant="secondary">{item.error_category}</Badge>
            <span className="text-xs text-muted-foreground">#{item.id} · {formatBytes(item.byte_length)}</span>
          </div>
          <p className="break-all font-mono text-xs">{item.original_key}</p>
          <p className="text-xs text-muted-foreground">
            {item.line_ordinal === null ? 'Container' : `Line ${item.line_ordinal}`}
            {item.byte_offset !== null && ` · byte ${item.byte_offset}`}
            {' · '}{new Date(item.quarantined_at).toLocaleString()}
          </p>
          <p className="break-words text-sm">{item.error_text}</p>
          <p className="break-all font-mono text-[11px] text-muted-foreground">SHA-256: {item.sha256}</p>
        </div>
        <div className="flex shrink-0 gap-2">
          <Button variant="outline" size="sm" onClick={onDownload} disabled={working}>
            {working ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Download className="h-3 w-3 mr-1" />}
            {working ? 'Working…' : 'Download'}
          </Button>
          <Button variant="ghost" size="sm" onClick={onPurge} disabled={working}>
            <Trash2 className="h-3 w-3 mr-1" />
            Remove
          </Button>
        </div>
      </div>
    </div>
  )
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}
