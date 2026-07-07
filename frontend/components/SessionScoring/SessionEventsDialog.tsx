'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import { Activity, ChevronRight, X } from 'lucide-react'

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { client } from '@/lib/api'
import type { components } from '@/types/api.generated'

type SessionEvent = components['schemas']['ScoringSessionEvent']
type SessionEventsResponse = components['schemas']['ScoringSessionEventsResponse']

interface SessionEventsDialogProps {
  serviceId: string
  sid: string
  label?: string
  /**
   * Custom trigger element. Must be a single React element (button, icon,
   * link, etc.) — DialogTrigger forwards refs/aria props onto it. String
   * children won't work because base-ui's Dialog needs an element to
   * compose with.
   */
  trigger?: React.ReactElement
}

/**
 * Modal showing the URL/timestamp sequence for a single edge session.
 *
 * Built so the LabelsTab and TopFlaggedTable can both render the same
 * "view events" affordance — pass a custom trigger (icon button, link,
 * etc.) and the dialog handles the rest, fetching only when opened so a
 * 50-row table doesn't fire 50 events queries on mount.
 */
export function SessionEventsDialog({
  serviceId,
  sid,
  label,
  trigger,
}: SessionEventsDialogProps) {
  const [open, setOpen] = React.useState(false)

  const { data, isLoading, isError, error } = useQuery({
    queryKey: ['scoring-session-events', serviceId, sid],
    enabled: open,
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/sessions/{sid}/events' as any,
        {
          params: {
            path: { service_id: serviceId, sid },
            query: { since_days: 30 },
          },
        } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as SessionEventsResponse
    },
    staleTime: 30_000,
  })

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        render={
          trigger ?? (
            <button
              type="button"
              className="text-xs text-primary hover:underline inline-flex items-center gap-0.5"
              title="View session events"
            >
              <Activity className="h-3 w-3" />
              events
            </button>
          )
        }
      />
      <DialogContent className="max-w-3xl max-h-[80vh] overflow-hidden flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Activity className="h-4 w-4" />
            <span>Session</span>
            <code className="font-mono text-sm">{sid}</code>
            {label && (
              <Badge
                variant={label === 'good' ? 'success' : label === 'bad' ? 'destructive' : 'secondary'}
                className="uppercase text-[10px]"
              >
                {label}
              </Badge>
            )}
          </DialogTitle>
          <DialogDescription>
            URL sequence the session walked, oldest first. The L2 scorer evaluates each
            transition (prev URL → current URL) against the trained matrix — high-score
            transitions are the ones the matrix flagged as unusual.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto -mx-6 px-6">
          {isLoading && (
            <div className="space-y-2" role="status" aria-busy="true">
              <span className="sr-only">Loading session events</span>
              {Array.from({ length: 6 }).map((_, i) => (
                <Skeleton key={i} className="h-12 w-full" />
              ))}
            </div>
          )}

          {isError && (
            <div className="text-sm text-destructive p-3 border rounded-md">
              Failed to load events: {(error as Error)?.message ?? 'unknown'}
            </div>
          )}

          {data && data.event_count === 0 && (
            <div className="text-sm text-muted-foreground p-6 text-center border rounded-md">
              No events found for this sid in the last {data.since_days} days. The
              session may have rotated its cookie, or the rows haven&apos;t been
              ingested yet (cron ticks every ~30s; FOS-to-parquet lag is ~1–2 min).
            </div>
          )}

          {data && (data.event_count ?? 0) > 0 && (
            <div className="space-y-1">
              <div className="text-xs text-muted-foreground mb-2">
                {data.event_count} event{data.event_count === 1 ? '' : 's'} in last {data.since_days}d
              </div>
              {data.events.map((ev, i) => (
                <EventRow key={i} event={ev} index={i} prev={i > 0 ? data.events[i - 1] : null} />
              ))}
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}

function EventRow({
  event,
  index,
  prev,
}: {
  event: SessionEvent
  index: number
  prev: SessionEvent | null
}) {
  const ts = event.ts ? new Date(event.ts).toLocaleString() : '—'
  const score = event.edge_score
  const compliance = event.edge_cookie_compliance
  const reason = event.edge_score_reason
  const scoreTone =
    score == null ? '' : score >= 75 ? 'text-destructive' : score >= 50 ? 'text-amber-600' : 'text-muted-foreground'

  return (
    <div className="flex items-start gap-3 p-2 border rounded-md text-xs hover:bg-muted/40">
      <div className="flex-none w-6 text-muted-foreground font-mono pt-0.5">{index + 1}.</div>
      <div className="flex-1 min-w-0 space-y-1">
        <div className="flex items-center gap-2">
          {prev && (
            <span className="text-muted-foreground inline-flex items-center gap-0.5">
              <code className="text-[10px] truncate max-w-[200px]">{prev.url}</code>
              <ChevronRight className="h-3 w-3" />
            </span>
          )}
          <code className="font-mono text-xs truncate">{event.url}</code>
        </div>
        <div className="flex items-center gap-3 text-[10px] text-muted-foreground">
          <span>{ts}</span>
          {event.status != null && <span>HTTP {event.status}</span>}
          {compliance && <span>cookie:{compliance}</span>}
          {reason && <span className="text-amber-600">{reason}</span>}
        </div>
      </div>
      {score != null && (
        <div className={`flex-none w-12 text-right font-mono font-semibold ${scoreTone}`}>{score}</div>
      )}
    </div>
  )
}
