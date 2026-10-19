'use client'
import React, { useState } from 'react'
import { ELAPSED_RELATIVE_AGO_JUST_NOW, formatCount, formatElapsed } from '@/lib/format'
import { useQuery } from '@tanstack/react-query'
import { client, extractApiError } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { components } from '@/types/api.generated'
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly } from '@/components/DataTable/DataTableReadonly'

type BotSourceList = components['schemas']['BotSourcesResponse']
type BotSource = components['schemas']['BotSourceMeta']
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog'
import {
  Bot,
  RefreshCw,
  Wifi,
  Download,
  Loader2,
  ExternalLink,
  CloudDownload,
} from 'lucide-react'

import { SystemJobsStrip } from './SystemStatus'

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
      <Dialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Rebuild local view from cloud?</DialogTitle>
            <DialogDescription>
              Clears local Iceberg caches and re-pulls metadata + parquet from FOS via CDN.
              Un-committed buffer data is preserved. This can take a minute on large tables.
            </DialogDescription>
          </DialogHeader>
          {error && <div className="text-xs text-red-500">{error}</div>}
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirmOpen(false)} disabled={busy}>
              Cancel
            </Button>
            <Button onClick={rebuild} disabled={busy}>
              {busy ? <Loader2 className="h-3 w-3 mr-1.5 animate-spin" /> : <CloudDownload className="h-3 w-3 mr-1.5" />}
              {busy ? 'Starting…' : 'Rebuild'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}

function fmtRelative(iso: string | null | undefined): string {
  if (!iso) return '—'
  return formatElapsed(Math.floor((Date.now() - new Date(iso).getTime()) / 1000), ELAPSED_RELATIVE_AGO_JUST_NOW)
}

export function BotSourcesPanel() {
  const [refreshingSource, setRefreshingSource] = useState<string | null>(null)
  const [enriching, setEnriching] = useState(false)
  const [enrichError, setEnrichError] = useState<string | null>(null)

  const { data: botSourcesData, refetch: refetchBotSources } = useQuery({
    queryKey: ['bot-sources'],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/admin/bot-sources", { signal })
      return data as BotSourceList
    },
    staleTime: 60_000,
  })

  async function handleRefreshBotSource(sourceId: string) {
    setRefreshingSource(sourceId)
    try {
      await client.POST("/api/admin/bot-sources/{source_id}/refresh", {
        params: { path: { source_id: sourceId } }
      })
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
      cell: ({ row }) => {
        const src = row.original
        return (
          <div className="text-sm">
            <div className="flex items-center gap-1.5">
              <span className="font-medium">{src.name}</span>
              {src.url && (
                <a href={src.url} target="_blank" rel="noreferrer" className="text-muted-foreground hover:text-foreground opacity-50 hover:opacity-100 transition-opacity" title={`View source: ${src.url}`}>
                  <ExternalLink className="h-3 w-3" />
                </a>
              )}
            </div>
            {!src.last_updated && (
              <span className="text-xs text-amber-500 block mt-0.5">not cached</span>
            )}
          </div>
        )
      },
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
        <span className="block text-right text-sm text-muted-foreground">
          {fmtRelative(row.original.last_updated)}
        </span>
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
            variant="outline" size="sm"
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

      {/* Sources table */}
      {/* M-6 (audit, mobile UX): overflow-x-auto so the bot-sources table
          scrolls on phones instead of clipping the actions column (the
          previous overflow-hidden combined with w-full pushed long
          source names + the trailing action button off the right edge). */}
      <DataTableReadonly
        columns={sourceColumns}
        data={botSourcesData?.sources ?? []}
        isLoading={!botSourcesData}
        hideToolbar
        showPagination={false}
        tableCaption="Bot intelligence sources"
      />

      {enrichError && <div className="text-xs text-red-500" role="alert">{enrichError}</div>}

      {/* rDNS cache stats */}
      <div className="flex items-center justify-between text-sm">
        <div className="flex items-center gap-4 text-muted-foreground text-xs">
          <span className="flex items-center gap-1.5">
            <Wifi className="h-3.5 w-3.5" />
            rDNS cache: <strong className="text-foreground">{botSourcesData?.rdns.total != null ? formatCount(botSourcesData.rdns.total) : '—'}</strong> IPs
          </span>
          <span>
            Pending: <strong className="text-foreground">{botSourcesData?.rdns.pending != null ? formatCount(botSourcesData.rdns.pending) : '—'}</strong>
          </span>
          <span>Last enrichment: {fmtRelative(botSourcesData?.rdns.last_enrichment_at ?? null)}</span>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" onClick={() => window.open('/api/admin/rdns/export', '_blank')} title="Download rDNS Cache as CSV">
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

      {/* Maintenance */}
      <div className="space-y-3 pt-2">
        <Label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Maintenance</Label>
        <div className="flex flex-wrap gap-2">
          <RebuildLocalViewButton />
        </div>
        <p className="text-[11px] text-muted-foreground">
          Drops local caches and re-pulls Iceberg metadata + parquet from FOS via CDN. The local buffer (un-committed data) is left alone.
        </p>
      </div>

      {/* System jobs */}
      <SystemJobsStrip />
    </div>
  )
}
