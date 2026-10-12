'use client'

import React from 'react'
import { Ban, KeyRound, Loader2 } from 'lucide-react'
import { client } from '@/lib/api'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { resolveRangeWire } from '@/lib/range-wire'
import { formatDate } from '@/lib/date'
import { formatBytes, resolveCountryName } from '@/lib/format'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import type { components } from '@/types/api.generated'
import type { StreamingTabProps } from './QualityTab'
import { BlockMark, type Block, type BlockCategory, type BlockCondition, type BlockOption } from './blocklist'

type SubscriberIpsData = components['schemas']['SubscriberIpsResponse']

export type Recency = 'active' | 'recent' | 'stale'

const ACTIVE_MS = 5 * 60_000
const RECENT_MS = 20 * 60_000

/** Last seen ≤ 5 min ago → active, ≤ 20 min → recent, older (or unknown) → stale. Measured against wall-clock now. */
export function recencyOf(lastSeen: string | null | undefined, now: number): Recency {
  if (!lastSeen) return 'stale'
  const age = now - new Date(lastSeen).getTime()
  if (age <= ACTIVE_MS) return 'active'
  if (age <= RECENT_MS) return 'recent'
  return 'stale'
}

const RECENCY_STYLE: Record<Recency, { dot: string; label: string }> = {
  active: { dot: 'bg-green-500', label: 'Seen in the last 5 minutes' },
  recent: { dot: 'bg-yellow-500', label: 'Seen 5–20 minutes ago' },
  stale: { dot: 'bg-red-500', label: 'Not seen for over 20 minutes' },
}

function RecencyDot({ recency }: { recency: Recency }) {
  const { dot, label } = RECENCY_STYLE[recency]
  return (
    <span className="inline-flex" title={label} data-recency={recency}>
      <span className={cn('h-2 w-2 rounded-full', dot)} aria-hidden="true" />
      <span className="sr-only">{label}</span>
    </span>
  )
}

/** Re-render on an interval so the recency dots age while the dialog stays open. */
function useNow(intervalMs: number) {
  const [now, setNow] = React.useState(() => Date.now())
  React.useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs)
    return () => clearInterval(id)
  }, [intervalMs])
  return now
}

type SubscriberIpsDialogProps = StreamingTabProps & {
  subscriberId: string | null
  contentId: string | null
  onClose: () => void
  /** Admin-only: opens the Block dialog with this subscriber id and its IPs to choose from. */
  onBlock?: (options: BlockOption[], initial: BlockCondition) => void
  findBlock?: (category: BlockCategory, value: string) => Block | undefined
}

/** Every client IP one subscriber id was seen from in the window (admin-only drill-down). */
export function SubscriberIpsDialog({
  subscriberId,
  contentId,
  onClose,
  onBlock,
  findBlock,
  activeServiceId,
  filterPayload,
  startTime,
  endTime,
  relativeRange,
  isAutoRange,
  anchor,
  timezone,
}: SubscriberIpsDialogProps) {
  const { rangeKey, rangeBody } = resolveRangeWire({ relativeRange, isAutoRange, startTime, endTime, anchor })
  const now = useNow(15_000)

  const query = useServiceQuery<SubscriberIpsData | undefined>(
    ['cmcd', 'content-security', 'subscriber', activeServiceId, rangeKey, anchor, filterPayload, contentId, subscriberId],
    async ({ signal }) => {
      const { data, error } = await client.POST('/api/cmcd/content-security/subscriber', {
        signal,
        body: { filters: filterPayload, subscriber_id: subscriberId ?? '', content_id: contentId, ...rangeBody },
      })
      if (error) throw error
      return data
    },
    { enabled: subscriberId != null, refetchInterval: 30_000 },
  )

  // The query holds the previous subscriber's rows as placeholder data — never show them under a new id.
  const data = query.data?.subscriber_id === subscriberId ? query.data : undefined
  const rows = data?.ips ?? []
  const showBytes = rows.some((r) => r.bytes != null)

  const distinctIps = new Set(rows.map((r) => r.ip)).size
  const distinctCountries = new Set(rows.flatMap((r) => (r.country ? [r.country] : []))).size
  const plural = (n: number, one: string, many: string) => `${n.toLocaleString()} ${n === 1 ? one : many}`
  // Summarise the scale at a glance — usually what decides a block.
  const summary = rows.length
    ? [
        plural(distinctIps, 'IP address', 'IP addresses'),
        distinctCountries ? plural(distinctCountries, 'country', 'countries') : null,
      ]
        .filter(Boolean)
        .join(' · ')
    : null

  const openBlock = () => {
    if (!subscriberId || !onBlock) return
    const ips = [...new Set(rows.map((r) => r.ip))]
    onBlock(
      [
        { category: 'subscriber_id', value: subscriberId },
        ...ips.map((ip) => ({ category: 'ip' as const, value: ip })),
      ],
      { category: 'subscriber_id', value: subscriberId },
    )
  }

  let body: React.ReactNode
  if (query.error) {
    body = <p className="py-8 text-center text-sm text-destructive">Could not load IP addresses for this subscriber.</p>
  } else if (!data) {
    body = (
      <div className="flex items-center justify-center gap-2 py-8 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
        Loading IP addresses…
      </div>
    )
  } else if (!rows.length) {
    body = <p className="py-8 text-center text-sm text-muted-foreground">No IP addresses found in this time range.</p>
  } else {
    body = (
      <>
        <div className="max-h-[60vh] overflow-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-6">
                  <span className="sr-only">Status</span>
                </TableHead>
                <TableHead>IP Address</TableHead>
                <TableHead>Country</TableHead>
                <TableHead>Host</TableHead>
                <TableHead className="text-right">Requests</TableHead>
                {showBytes && <TableHead className="text-right">Bandwidth</TableHead>}
                <TableHead className="text-right">Last seen</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((row) => (
                <TableRow key={`${row.ip}|${row.country ?? ''}|${row.host ?? ''}`}>
                  <TableCell>
                    <RecencyDot recency={recencyOf(row.last_seen, now)} />
                  </TableCell>
                  <TableCell className="font-mono text-xs">
                    <span className="inline-flex items-center gap-1.5">
                      {row.ip}
                      {(() => {
                        const block = findBlock?.('ip', row.ip)
                        return block ? <BlockMark kbps={block.kbps} /> : null
                      })()}
                    </span>
                  </TableCell>
                  <TableCell>{row.country ? `${resolveCountryName(row.country)} (${row.country})` : '—'}</TableCell>
                  <TableCell className="max-w-[14rem] truncate" title={row.host ?? undefined}>
                    {row.host || '—'}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{row.requests.toLocaleString()}</TableCell>
                  {showBytes && (
                    <TableCell className="text-right tabular-nums">
                      {row.bytes == null ? '—' : formatBytes(row.bytes)}
                    </TableCell>
                  )}
                  <TableCell className="text-right tabular-nums whitespace-nowrap">
                    {row.last_seen ? formatDate(row.last_seen, timezone, 'yyyy-MM-dd HH:mm:ss') : '—'}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
        {data.truncated && (
          <p className="pt-2 text-[11px] text-muted-foreground">
            Showing the {rows.length.toLocaleString()} most recently seen combinations.
          </p>
        )}
      </>
    )
  }

  return (
    <Dialog open={subscriberId != null} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-w-4xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-base">
            <KeyRound className="h-4 w-4" aria-hidden="true" />
            <span className="truncate font-mono" title={subscriberId ?? undefined}>
              {subscriberId}
            </span>
          </DialogTitle>
          <DialogDescription>
            Each IP address, country and host combination this subscriber ID was seen from{contentId ? ` for ${contentId}` : ''}, most recent first.
          </DialogDescription>
        </DialogHeader>
        {(onBlock || rows.length > 0) && (
          <div className="flex min-h-8 items-center justify-between gap-3">
            <p className="text-xs text-muted-foreground tabular-nums">{summary}</p>
            {onBlock && (
              <Button variant="destructive" onClick={openBlock} disabled={!data}>
                <Ban className="h-3 w-3 mr-1" aria-hidden="true" />
                Block or throttle…
              </Button>
            )}
          </div>
        )}
        {body}
      </DialogContent>
    </Dialog>
  )
}
