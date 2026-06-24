'use client'
import React, { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { client, extractApiError } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { SSEModal } from '@/components/SSEModal/SSEModal'
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
  Database,
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
    } catch (e: any) {
      setError(e?.message ?? 'rebuild failed')
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
  const diff = Date.now() - new Date(iso).getTime()
  const mins = Math.floor(diff / 60_000)
  if (mins < 2) return 'just now'
  if (mins < 60) return `${mins}m ago`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}h ago`
  return `${Math.floor(hrs / 24)}d ago`
}

export function BotSourcesPanel() {
  const queryClient = useQueryClient()
  const [refreshingSource, setRefreshingSource] = useState<string | null>(null)

  const { data: botSourcesData, refetch: refetchBotSources } = useQuery({
    queryKey: ['bot-sources'],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/admin/bot-sources", { signal })
      return data as any
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
      <div className="border rounded-md overflow-x-auto text-sm">
        <table className="w-full">
          <thead className="bg-muted/40">
            <tr>
              <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Source</th>
              <th className="text-right px-3 py-2 text-xs font-medium text-muted-foreground">Entries</th>
              <th className="text-right px-3 py-2 text-xs font-medium text-muted-foreground">Last Updated</th>
              <th className="px-3 py-2" />
            </tr>
          </thead>
          <tbody>
            {(botSourcesData?.sources ?? []).map((src: any) => (
              <tr key={src.id} className="border-t">
                <td className="px-3 py-2">
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
                </td>
                <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">
                  {src.entry_count?.toLocaleString() ?? '—'}
                </td>
                <td className="px-3 py-2 text-right text-muted-foreground">
                  {fmtRelative(src.last_updated)}
                </td>
                <td className="px-3 py-2 text-right">
                  <Button
                    variant="outline" size="sm"
                    disabled={refreshingSource === src.id}
                    onClick={() => handleRefreshBotSource(src.id)}
                  >
                    <RefreshCw className={`h-3 w-3 mr-1.5 ${refreshingSource === src.id ? 'animate-spin' : ''}`} />
                    Refresh
                  </Button>
                </td>
              </tr>
            ))}
            {!botSourcesData && (
              <tr><td colSpan={4} className="px-3 py-3 text-center text-xs text-muted-foreground">Loading…</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {/* rDNS cache stats */}
      <div className="flex items-center justify-between text-sm">
        <div className="flex items-center gap-4 text-muted-foreground text-xs">
          <span className="flex items-center gap-1.5">
            <Wifi className="h-3.5 w-3.5" />
            rDNS cache: <strong className="text-foreground">{botSourcesData?.rdns.total.toLocaleString() ?? '—'}</strong> IPs
          </span>
          <span>
            Pending: <strong className="text-foreground">{botSourcesData?.rdns.pending.toLocaleString() ?? '—'}</strong>
          </span>
          <span>Last enrichment: {fmtRelative(botSourcesData?.rdns.last_enrichment_at ?? null)}</span>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" onClick={() => window.open('/api/admin/rdns/export', '_blank')} title="Download rDNS Cache as CSV">
            <Download className="h-3 w-3 mr-1.5" />
            Export CSV
          </Button>
          <SSEModal
            title="Enrich rDNS Cache"
            description={
              <div className="space-y-2">
                <p>This will start a manual enrichment batch for the reverse DNS cache.</p>
                <p className="text-muted-foreground">It will resolve pending IPs and attempt to discover new IPs from your DuckDB log sources.</p>
              </div>
            }
            endpoint="/api/admin/bot-sources/rdns/enrich"
            body={{}}
            onClose={() => queryClient.invalidateQueries({ queryKey: ['bot-sources'] })}
            trigger={
              <Button variant="outline" size="sm">
                <RefreshCw className="h-3 w-3 mr-1.5" />
                Enrich Now
              </Button>
            }
          />
          <SSEModal
            title="Seed rDNS Backfill"
            description={
              <div className="space-y-2">
                <p>This will scan all log sources for the last 30 days to seed the rDNS cache.</p>
                <p className="text-muted-foreground text-xs italic">Note: This only enqueues IPs for later resolution. It does not perform lookups immediately.</p>
              </div>
            }
            endpoint="/api/admin/bot-sources/rdns/backfill"
            body={{}}
            onClose={() => queryClient.invalidateQueries({ queryKey: ['bot-sources'] })}
            trigger={
              <Button variant="outline" size="sm">
                <Database className="h-3 w-3 mr-1.5" />
                Seed Backfill
              </Button>
            }
          />
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
