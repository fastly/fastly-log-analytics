'use client'

import React, { useState } from 'react'
import {
  AlertTriangle,
  Download,
  FileWarning,
  Loader2,
  Trash2,
  Bot,
  RefreshCw,
  Wifi,
  ExternalLink,
  CloudDownload,
} from 'lucide-react'
import type { ColumnDef } from '@tanstack/react-table'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { CardErrorState } from '@/components/AnalyticsCard'
import type { components } from '@/types/api'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { NextRunCountdown } from '@/components/SyncStatusBadge/SyncStatusBadge'
import { DataTableReadonly } from '@/components/DataTable'
import { useDateFormat } from '@/hooks/useSSE'
import { adminFetch, adminJson, client, extractApiError, unwrapClient, queryKeys } from '@/lib/api'
import { useServiceStore } from '@/stores/filterStore'
import { useMounted } from '@/hooks/useBootstrap'
import {
  ELAPSED_RELATIVE_AGO_JUST_NOW,
  formatBytesTiered,
  formatCount,
  formatElapsed,
  formatLocalDateTime,
  downloadBlob,
} from '@/lib/utils'

type QuarantineSummary = components['schemas']['QuarantineSummary']
type QuarantineListResponse = components['schemas']['QuarantineListResponse']
type QuarantineItem = components['schemas']['QuarantineEvidenceItem']
const PAGE_SIZE = 20

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
    <div className="rounded-md border p-3 flex flex-wrap items-start justify-between gap-3">
      <div className="min-w-0 space-y-1">
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline">{item.source_type}</Badge>
          <Badge variant="secondary">{item.error_category}</Badge>
          <span className="text-xs text-muted-foreground">
            #{item.id} · {formatBytesTiered(item.byte_length)}
          </span>
        </div>
        <p className="break-all font-mono text-xs">{item.original_key}</p>
        <p className="text-xs text-muted-foreground">
          {item.line_ordinal === null ? 'Container' : `Line ${item.line_ordinal}`}
          {item.byte_offset !== null && ` · byte ${item.byte_offset}`} · {formatLocalDateTime(item.quarantined_at)}
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
  )
}

export function QuarantineSection() {
  const mounted = useMounted()
  const storeServiceId = useServiceStore((s) => s.activeServiceId)
  const activeServiceId = mounted ? storeServiceId : null
  const queryClient = useQueryClient()
  const [showDetails, setShowDetails] = useState(false)
  const [errorCategory, setErrorCategory] = useState('')
  const [offset, setOffset] = useState(0)
  const [workingItemId, setWorkingItemId] = useState<number | null>(null)
  const [purgingAll, setPurgingAll] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)

  const summaryQuery = useQuery<QuarantineSummary>({
    queryKey: queryKeys.admin.quarantineSummary(activeServiceId),
    queryFn: ({ signal }) =>
      adminJson(
        '/api/admin/quarantine/summary',
        '/api/admin/quarantine/summary',
        { signal },
        (status) => `Unable to load quarantine summary (HTTP ${status})`,
      ),
    enabled: !!activeServiceId,
    staleTime: 30_000,
    refetchInterval: 60_000,
    refetchIntervalInBackground: false,
  })

  const listQuery = useQuery<QuarantineListResponse>({
    queryKey: queryKeys.admin.quarantineList(activeServiceId, errorCategory, offset),
    queryFn: ({ signal }) => {
      const params = new URLSearchParams({
        limit: String(PAGE_SIZE),
        offset: String(offset),
        ...(errorCategory ? { error_category: errorCategory } : {}),
      })
      return adminJson(
        '/api/admin/quarantine',
        `/api/admin/quarantine?${params}`,
        { signal },
        (status) => `Unable to load quarantine items (HTTP ${status})`,
      )
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

  const handleDownload = async (item: QuarantineItem) => {
    setWorkingItemId(item.id)
    setActionError(null)
    try {
      const response = await adminFetch(`/api/admin/quarantine/download/${item.id}`)
      if (!response.ok) throw new Error(`Unable to download evidence (HTTP ${response.status})`)
      downloadBlob(await response.blob(), `quarantine-item-${item.id}.dat`)
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
      const response = await adminFetch(
        `/api/admin/quarantine/purge${itemId === undefined ? '' : `?item_id=${itemId}`}`,
        { method: 'POST' },
      )
      if (!response.ok) throw new Error(`Unable to purge quarantine evidence (HTTP ${response.status})`)
      await queryClient.invalidateQueries({ queryKey: queryKeys.admin.quarantineAll() })
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
            <Button variant="ghost" size="sm" className="text-xs" onClick={() => setShowDetails((prev) => !prev)}>
              {showDetails ? 'Hide details' : 'Details'}
            </Button>
            {totalItems > 0 && (
              <Button variant="outline" size="sm" className="text-xs" onClick={() => void handlePurge()} disabled={purgingAll}>
                {purgingAll ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Trash2 className="h-3 w-3 mr-1" />}
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
                <span className="font-medium tabular-nums">{formatBytesTiered(summary?.total_bytes ?? 0)}</span>
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
                  <span className="font-medium">{formatLocalDateTime(summary.oldest_at)}</span>
                </div>
              )}
            </div>
            {showDetails && (
              <>
                <div className="flex flex-wrap items-center gap-3">
                  <label htmlFor="quarantine-category" className="text-sm font-medium">
                    Error category
                  </label>
                  <select
                    id="quarantine-category"
                    className="h-9 rounded-md border bg-background px-3 text-sm"
                    value={errorCategory}
                    onChange={(event) => {
                      setErrorCategory(event.target.value)
                      setOffset(0)
                    }}
                  >
                    <option value="">All categories</option>
                    {categories.map((category) => (
                      <option key={category} value={category}>
                        {category}
                      </option>
                    ))}
                  </select>
                </div>
                {actionError && (
                  <p role="alert" className="text-sm text-destructive">
                    {actionError}
                  </p>
                )}
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
                    {listedItems.map((item) => (
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
                          onClick={() => setOffset((current) => Math.max(0, current - PAGE_SIZE))}
                        >
                          Previous
                        </Button>
                        <Button
                          variant="outline"
                          size="sm"
                          disabled={offset + PAGE_SIZE >= (listQuery.data?.total ?? 0) || listQuery.isFetching}
                          onClick={() => setOffset((current) => current + PAGE_SIZE)}
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

type BotSourceList = components['schemas']['BotSourcesResponse']
type BotSource = components['schemas']['BotSourceMeta']

function JobTip({
  className,
  tip,
  side,
  tipClass = 'text-xs',
  children,
}: {
  className: string
  tip: React.ReactNode
  side?: 'top'
  tipClass?: string
  children: React.ReactNode
}) {
  return (
    <TooltipProvider delay={200}>
      <Tooltip>
        <TooltipTrigger render={<span className={className} />}>{children}</TooltipTrigger>
        <TooltipContent side={side} className={tipClass}>
          {tip}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

function SystemJobBox({ job }: { job: components['schemas']['SystemJobStatus'] }) {
  const { timeAgo, full, abbr } = useDateFormat()
  const isError = job.status === 'error'
  return (
    <div
      className={`relative flex flex-col justify-center border rounded-md px-2.5 h-8 shrink-0 ${isError ? 'bg-destructive/10 border-destructive/50' : 'bg-muted/20 border-muted'} min-w-[250px] max-w-[320px] flex-1`}
    >
      <div className="flex items-center gap-2 w-full">
        <JobTip
          className={`text-[9px] font-bold uppercase tracking-wider shrink-0 truncate max-w-[120px] ${isError ? 'text-destructive' : 'text-muted-foreground'}`}
          side="top"
          tipClass="max-w-[250px] text-xs"
          tip={job.detail || job.name}
        >
          {job.name}
        </JobTip>
        <div className="w-px h-4 bg-border shrink-0" />
        <div
          className={`flex-1 min-w-0 flex items-center justify-between text-[9px] whitespace-nowrap ${isError ? 'text-destructive/80' : 'text-muted-foreground'}`}
        >
          <JobTip className="truncate pr-2 " tip={job.last_run_at ? `${full(job.last_run_at)} ${abbr()}` : 'Never'}>
            Last: {job.last_run_at ? timeAgo(job.last_run_at) : 'Never'}
          </JobTip>
          <JobTip className="truncate " tip={job.next_run_at ? `${full(job.next_run_at)} ${abbr()}` : 'Disabled'}>
            Next: <NextRunCountdown when={job.next_run_at} />
          </JobTip>
        </div>
      </div>
    </div>
  )
}

export function SystemJobsStrip() {
  const mounted = useMounted()
  const { data: systemJobsData } = useQuery({
    queryKey: queryKeys.systemJobs(),
    queryFn: ({ signal }) => unwrapClient(client.GET('/api/admin/system-jobs', { signal })),
    staleTime: 30_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })
  return (
    <div className="space-y-3 pt-2">
      <Label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Background Jobs</Label>
      <div className="flex flex-wrap gap-2">
        {mounted && systemJobsData ? (
          systemJobsData.jobs.map((job) => <SystemJobBox key={job.id} job={job} />)
        ) : (
          <div className="text-xs text-muted-foreground italic px-1 py-1">Loading background jobs...</div>
        )}
      </div>
    </div>
  )
}

function RebuildLocalViewButton() {
  const [busy, setBusy] = useState(false)
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function rebuild() {
    setBusy(true)
    setError(null)
    try {
      const { error: apiError } = await client.POST('/api/admin/rebuild-local-view', {})
      if (apiError) throw new Error(extractApiError(apiError))
      setConfirmOpen(false)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'rebuild failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <>
      <Button variant="outline" size="sm" onClick={() => setConfirmOpen(true)}>
        <CloudDownload className="h-3 w-3 mr-1.5" />
        Rebuild Local View
      </Button>
      <ConfirmDialog
        open={confirmOpen}
        onOpenChange={setConfirmOpen}
        title="Rebuild local view?"
        description={
          <>
            Clears the local view caches and rebuilds the DuckDB view over the DuckLake table. Un-committed buffer data
            is preserved. This can take a minute on large tables.
            {error && <span className="block mt-2 text-xs text-red-500">{error}</span>}
          </>
        }
        confirmLabel="Rebuild"
        isPending={busy}
        onConfirm={rebuild}
      />
    </>
  )
}

function fmtRelative(iso: string | null | undefined): string {
  return iso
    ? formatElapsed(Math.floor((Date.now() - new Date(iso).getTime()) / 1000), ELAPSED_RELATIVE_AGO_JUST_NOW)
    : '—'
}

export function BotSourcesPanel() {
  const mounted = useMounted()
  const [refreshingSource, setRefreshingSource] = useState<string | null>(null)
  const [enriching, setEnriching] = useState(false)
  const [enrichError, setEnrichError] = useState<string | null>(null)
  const { data: queryBotSourcesData, refetch: refetchBotSources } = useQuery({
    queryKey: ['bot-sources'],
    queryFn: async ({ signal }) => (await unwrapClient(client.GET('/api/admin/bot-sources', { signal }))) as BotSourceList,
    staleTime: 60_000,
  })
  const botSourcesData = mounted ? queryBotSourcesData : undefined

  async function handleRefreshBotSource(sourceId: string) {
    setRefreshingSource(sourceId)
    try {
      await unwrapClient(client.POST('/api/admin/bot-sources/{source_id}/refresh', { params: { path: { source_id: sourceId } } }))
      await refetchBotSources()
    } finally {
      setRefreshingSource(null)
    }
  }

  async function handleEnrichRdns() {
    setEnriching(true)
    setEnrichError(null)
    try {
      const { error: apiError } = await client.POST('/api/admin/rdns/enrich', {})
      if (apiError) throw new Error(extractApiError(apiError))
      await refetchBotSources()
    } catch (e) {
      setEnrichError(e instanceof Error ? e.message : 'rDNS enrichment failed')
    } finally {
      setEnriching(false)
    }
  }

  const sourceColumns: ColumnDef<BotSource>[] = [
    {
      id: 'source',
      header: 'Source',
      accessorFn: (src) => src.name,
      size: 260,
      enableSorting: false,
      cell: ({ row: { original: src } }) => (
        <div className="text-sm">
          <div className="flex items-center gap-1.5">
            <span className="font-medium">{src.name}</span>
            {src.url && (
              <a
                href={src.url}
                target="_blank"
                rel="noreferrer"
                className="text-muted-foreground hover:text-foreground opacity-50 hover:opacity-100 transition-opacity"
                title={`View source: ${src.url}`}
              >
                <ExternalLink className="h-3 w-3" />
              </a>
            )}
          </div>
          {!src.last_updated && <span className="text-xs text-amber-500 block mt-0.5">not cached</span>}
        </div>
      ),
    },
    {
      id: 'entries',
      header: 'Entries',
      accessorFn: (src) => src.entry_count ?? -1,
      size: 110,
      enableSorting: false,
      cell: ({ row }) => (
        <span className="block text-right text-sm tabular-nums text-muted-foreground">
          {row.original.entry_count != null ? formatCount(row.original.entry_count) : '—'}
        </span>
      ),
    },
    {
      id: 'last_updated',
      header: 'Last Updated',
      accessorFn: (src) => src.last_updated ?? '',
      size: 140,
      enableSorting: false,
      cell: ({ row }) => (
        <span className="block text-right text-sm text-muted-foreground">{fmtRelative(row.original.last_updated)}</span>
      ),
    },
    {
      id: 'actions',
      header: () => <span className="sr-only">Actions</span>,
      size: 130,
      enableSorting: false,
      cell: ({ row }) => (
        <div className="text-right">
          <Button
            variant="outline"
            size="sm"
            disabled={refreshingSource === row.original.id}
            onClick={() => handleRefreshBotSource(row.original.id)}
          >
            <RefreshCw className={`h-3 w-3 mr-1.5 ${refreshingSource === row.original.id ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
        </div>
      ),
    },
  ]

  return (
    <div className="p-4 border rounded-lg space-y-4">
      <div className="flex items-center gap-2">
        <Bot className="h-4 w-4 text-muted-foreground" />
        <Label className="text-sm font-medium">Bot Intelligence Sources</Label>
      </div>
      <p className="text-xs text-muted-foreground -mt-2">
        Known bot registries used to identify and verify bots in log traffic via UA matching and FCrDNS validation.
      </p>
      <DataTableReadonly
        columns={sourceColumns}
        data={botSourcesData?.sources ?? []}
        isLoading={!botSourcesData}
        hideToolbar
        showPagination={false}
        tableCaption="Bot intelligence sources"
      />
      {enrichError && (
        <div className="text-xs text-red-500" role="alert">
          {enrichError}
        </div>
      )}
      <div className="flex items-center justify-between text-sm">
        <div className="flex items-center gap-4 text-muted-foreground text-xs">
          <span className="flex items-center gap-1.5">
            <Wifi className="h-3.5 w-3.5" />
            rDNS cache:{' '}
            <strong className="text-foreground">
              {botSourcesData?.rdns.total != null ? formatCount(botSourcesData.rdns.total) : '—'}
            </strong>{' '}
            IPs
          </span>
          <span>
            Pending:{' '}
            <strong className="text-foreground">
              {botSourcesData?.rdns.pending != null ? formatCount(botSourcesData.rdns.pending) : '—'}
            </strong>
          </span>
          <span>Last enrichment: {fmtRelative(botSourcesData?.rdns.last_enrichment_at ?? null)}</span>
        </div>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() =>
              void adminFetch('/api/admin/rdns/export').then(async (res) => {
                if (res.ok) downloadBlob(await res.blob(), 'rdns_cache.csv')
              })
            }
            title="Download rDNS Cache as CSV"
          >
            <Download className="h-3 w-3 mr-1.5" />
            Export CSV
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={handleEnrichRdns}
            disabled={enriching}
            title="Resolve pending IPs now (runs one enrichment batch)"
          >
            <RefreshCw className={`h-3 w-3 mr-1.5 ${enriching ? 'animate-spin' : ''}`} />
            {enriching ? 'Enriching…' : 'Enrich Now'}
          </Button>
        </div>
      </div>
      <div className="space-y-3 pt-2">
        <Label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Maintenance</Label>
        <div className="flex flex-wrap gap-2">
          <RebuildLocalViewButton />
        </div>
        <p className="text-[11px] text-muted-foreground">
          Clears the local view caches and rebuilds the DuckDB view. The local buffer (un-committed data) is left alone.
        </p>
      </div>
      <SystemJobsStrip />
    </div>
  )
}
