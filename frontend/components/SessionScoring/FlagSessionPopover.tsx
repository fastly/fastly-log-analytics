'use client'

import * as React from 'react'
import { Flag, Loader2 } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Textarea } from '@/components/ui/textarea'
import { client, extractApiError } from '@/lib/api'

export type LabelValue = 'good' | 'bad' | 'neutral'

interface FlagSessionPopoverProps {
  serviceId: string
  sid: string
  // Optional metadata captured at flag time — denormalized into the label
  // row so the Labels tab can show "what this session looked like" without
  // re-querying the logs.
  sampleIp?: string
  sampleUa?: string
  sampleUrl?: string
  // Currently-applied label (from the labels API). Undefined means we
  // don't know / haven't checked.
  currentLabel?: LabelValue | null
  // Id of the currently-applied label row, required to issue the DELETE
  // that un-flags the session. Optional — when absent the "Clear Label"
  // affordance is hidden.
  currentLabelId?: string | null
  trigger?: React.ReactNode
  onFlagged?: () => void
}

const LABEL_STYLES: Record<LabelValue, string> = {
  good: 'border-emerald-500 text-emerald-700 hover:bg-emerald-50',
  bad: 'border-rose-500 text-rose-700 hover:bg-rose-50',
  neutral: 'border-slate-400 text-slate-700 hover:bg-slate-50',
}

/**
 * Reusable popover for labeling a session as good / bad / neutral. Used
 * from both the SessionScoring admin page's TopFlaggedTable and from the
 * dashboard raw-logs table's Flag column.
 *
 * Always posts to /api/services/{serviceId}/scoring/labels, which upserts
 * on (service_id, sid), so re-flagging the same session overwrites
 * cleanly. The `currentLabel` prop is purely cosmetic (visual hint of
 * existing state) — the upsert semantics on the backend mean we never
 * need to choose between POST and PATCH from the client.
 */
export function FlagSessionPopover({
  serviceId,
  sid,
  sampleIp = '',
  sampleUa = '',
  sampleUrl = '',
  currentLabel,
  currentLabelId,
  trigger,
  onFlagged,
}: FlagSessionPopoverProps) {
  const [open, setOpen] = React.useState(false)
  const [notes, setNotes] = React.useState('')
  const [busy, setBusy] = React.useState<LabelValue | null>(null)
  const [clearing, setClearing] = React.useState(false)
  const [error, setError] = React.useState('')

  const disabled = !sid

  const submit = async (label: LabelValue) => {
    setBusy(label)
    setError('')
    try {
      await client.POST('/api/services/{service_id}/scoring/labels' as any, {
        params: { path: { service_id: serviceId } },
        body: {
          sid,
          label,
          notes,
          sample_ip: sampleIp,
          sample_ua: sampleUa,
          sample_url: sampleUrl,
        },
      } as any)
      setOpen(false)
      setNotes('')
      onFlagged?.()
    } catch (e: any) {
      setError(extractApiError(e) || 'Failed to save label')
    } finally {
      setBusy(null)
    }
  }

  const handleClearLabel = async () => {
    if (!currentLabelId) return
    setClearing(true)
    setError('')
    try {
      await client.DELETE('/api/services/{service_id}/scoring/labels/{label_id}' as any, {
        params: { path: { service_id: serviceId, label_id: currentLabelId } },
      } as any)
      setOpen(false)
      onFlagged?.()
    } catch (e: any) {
      setError(extractApiError(e) || 'Failed to clear label')
    } finally {
      setClearing(false)
    }
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        render={(props: React.ComponentPropsWithRef<'button'>) =>
          trigger ? (
            <button {...props}>{trigger}</button>
          ) : (
            <Button
              {...props}
              variant="ghost"
              size="icon"
              aria-label={disabled ? 'No session id (cookieless request)' : 'Flag this session'}
              className="h-7 w-7"
              disabled={disabled}
              title={disabled ? 'No session id (cookieless request)' : 'Flag this session'}
            >
              <Flag
                className={
                  currentLabel === 'bad'
                    ? 'h-3.5 w-3.5 text-rose-600 fill-rose-600'
                    : currentLabel === 'good'
                    ? 'h-3.5 w-3.5 text-emerald-600 fill-emerald-600'
                    : currentLabel === 'neutral'
                    ? 'h-3.5 w-3.5 text-slate-500 fill-slate-500'
                    : 'h-3.5 w-3.5 text-muted-foreground'
                }
              />
            </Button>
          )
        }
      />
      <PopoverContent className="w-80" align="end">
        <div className="space-y-3">
          <div>
            <p className="text-sm font-medium">Label session</p>
            <p className="text-xs text-muted-foreground font-mono break-all mt-0.5">sid: {sid || '(none)'}</p>
            {currentLabel && (
              <p className="text-xs text-muted-foreground mt-1">
                Currently: <span className="font-medium">{currentLabel}</span> · re-flagging overwrites
              </p>
            )}
          </div>
          <Textarea
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            placeholder="Notes (optional) — why this session?"
            className="text-xs min-h-[60px]"
            disabled={!!busy}
          />
          {error && <p className="text-xs text-rose-600">{error}</p>}
          <div className="grid grid-cols-3 gap-2">
            {(['good', 'neutral', 'bad'] as const).map((lbl) => (
              <Button
                key={lbl}
                variant="outline"
                size="sm"
                className={`text-xs ${LABEL_STYLES[lbl]}`}
                disabled={disabled || !!busy || clearing}
                onClick={() => submit(lbl)}
              >
                {busy === lbl ? <Loader2 className="h-3 w-3 animate-spin" /> : lbl}
              </Button>
            ))}
          </div>
          {currentLabel && currentLabelId && (
            <div className="pt-1 border-t">
              <Button
                variant="ghost"
                size="sm"
                className="w-full text-xs text-rose-600 hover:text-rose-700 hover:bg-rose-50"
                disabled={!!busy || clearing}
                onClick={handleClearLabel}
              >
                {clearing ? (
                  <Loader2 className="h-3 w-3 animate-spin" />
                ) : (
                  'Clear Label (Un-flag)'
                )}
              </Button>
            </div>
          )}
        </div>
      </PopoverContent>
    </Popover>
  )
}
