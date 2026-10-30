'use client'

import React, { useState } from 'react'
import {
  Download,
  Bot,
  RefreshCw,
  Wifi,
  ExternalLink,
  CloudDownload,
} from 'lucide-react'
import type { ColumnDef } from '@tanstack/react-table'
import { useQuery } from '@tanstack/react-query'
import type { components } from '@/types/api'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { NextRunCountdown } from '@/components/SyncStatusBadge/SyncStatusBadge'
import { DataTableReadonly } from '@/components/DataTable'
import { useDateFormat } from '@/hooks/useSSE'
import { adminFetch, client, extractApiError, unwrapClient, queryKeys } from '@/lib/api'
import { useMounted } from '@/hooks/useBootstrap'
import {
  ELAPSED_RELATIVE_AGO_JUST_NOW,
  formatCount,
  formatElapsed,
  downloadBlob,
} from '@/lib/utils'

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
