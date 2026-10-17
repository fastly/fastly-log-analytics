'use client'

import React from 'react'
import { Loader2 } from 'lucide-react'
import { extractApiError } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { ValuePicker } from '@/components/ui/value-picker'
import type { components } from '@/types/api.generated'
import { summarizePolicy, type Assignment, type AssignmentBody, type Policy } from './policyApi'

type ContentIdOption = components['schemas']['ContentSecurityContentId']

const pad = (n: number) => String(n).padStart(2, '0')

/** ISO instant → the `YYYY-MM-DDTHH:mm` value a datetime-local input takes (browser local time). */
export function isoToLocalInput(iso: string | null | undefined): string {
  if (!iso) return ''
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return ''
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`
}

/** A datetime-local value (browser local time) → ISO UTC, or null when empty or invalid. */
export function localInputToIso(value: string): string | null {
  if (!value) return null
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? null : d.toISOString()
}

const utcLabel = (iso: string | null) => (iso ? `${iso.slice(0, 16).replace('T', ' ')} UTC` : null)

/** A date-time field: local picker, with the UTC instant that will be sent shown beside it. */
function DateTimeField({
  id,
  label,
  value,
  onChange,
  emptyHint,
}: {
  id: string
  label: string
  value: string
  onChange: (value: string) => void
  emptyHint: string
}) {
  const utc = utcLabel(localInputToIso(value))
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id}>{label}</Label>
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
        <Input id={id} type="datetime-local" className="h-8 w-auto text-xs" value={value} onChange={(e) => onChange(e.target.value)} />
        <span className="text-xs text-muted-foreground tabular-nums" data-testid={`${id}-utc`}>
          {utc ?? emptyHint}
        </span>
      </div>
    </div>
  )
}

/** Schedule or edit a policy: which policy applies to which content, in which mode, and when. */
export function AssignmentDialog({
  open,
  onOpenChange,
  assignment,
  policies,
  contentIds,
  onSave,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  /** The assignment being edited; null creates a new one. */
  assignment: Assignment | null
  policies: Policy[]
  /** Content ids observed in the logs (suggestions only; any text is accepted). */
  contentIds: ContentIdOption[]
  onSave: (assignmentId: string | null, body: AssignmentBody) => Promise<unknown>
}) {
  const [scope, setScope] = React.useState<'default' | 'content_id'>('default')
  const [contentId, setContentId] = React.useState('')
  const [policyId, setPolicyId] = React.useState('')
  const [mode, setMode] = React.useState<'WARN' | 'ENFORCE'>('ENFORCE')
  const [starts, setStarts] = React.useState('')
  const [ends, setEnds] = React.useState('')
  const [busy, setBusy] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)

  React.useEffect(() => {
    if (!open) return
    /* eslint-disable react-hooks/set-state-in-effect */
    setScope(assignment?.scope_type ?? 'default')
    setContentId(assignment?.content_id ?? '')
    setPolicyId(assignment?.policy_id ?? policies[0]?.id ?? '')
    setMode(assignment?.mode ?? 'ENFORCE')
    setStarts(isoToLocalInput(assignment?.starts_at))
    setEnds(isoToLocalInput(assignment?.ends_at))
    setError(null)
    /* eslint-enable react-hooks/set-state-in-effect */
  }, [open, assignment, policies])

  const startIso = localInputToIso(starts)
  const endIso = localInputToIso(ends)
  const windowError = startIso && endIso && Date.parse(endIso) <= Date.parse(startIso) ? 'The end must be after the start.' : null
  const contentOk = scope === 'default' || contentId.trim() !== ''
  const canSave = !busy && policyId !== '' && contentOk && !windowError

  const save = async () => {
    if (!canSave) return
    setBusy(true)
    setError(null)
    try {
      await onSave(assignment?.id ?? null, {
        policy_id: policyId,
        scope_type: scope,
        content_id: scope === 'content_id' ? contentId.trim() : null,
        mode,
        starts_at: startIso,
        ends_at: endIso,
      })
      onOpenChange(false)
    } catch (e) {
      setError(extractApiError(e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={(next) => !busy && onOpenChange(next)}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle className="text-base">{assignment ? 'Edit scheduled policy' : 'Apply policy to content'}</DialogTitle>
          <DialogDescription>
            Choose which policy applies, to what content, and when. A content ID&apos;s own entry overrides the
            all-content one.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 pt-3">
          <fieldset className="space-y-2">
            <legend className="text-sm font-medium">Scope</legend>
            <div className="flex flex-col gap-1.5">
              <label className="flex items-center gap-2 text-sm">
                <input type="radio" name="cs-scope" checked={scope === 'default'} onChange={() => setScope('default')} />
                All content <span className="text-xs text-muted-foreground">(the default)</span>
              </label>
              <label className="flex items-center gap-2 text-sm">
                <input type="radio" name="cs-scope" checked={scope === 'content_id'} onChange={() => setScope('content_id')} />
                Content ID
              </label>
            </div>
            {scope === 'content_id' && (
              <div className="space-y-1">
                <ValuePicker
                  ariaLabel="Content ID"
                  value={contentId}
                  options={contentIds.map((c) => ({
                    value: c.content_id,
                    label: c.content_id,
                    detail: `${c.requests.toLocaleString()} requests`,
                  }))}
                  allowCustom
                  mono
                  onChange={setContentId}
                  placeholder="Pick an observed content ID or type one"
                  searchPlaceholder="Search or type a content ID…"
                  emptyText="Type a content ID to use it."
                />
                <p className="text-[11px] text-muted-foreground">
                  Matched exactly and case-sensitively. A content ID can only be scheduled for one policy at a time.
                </p>
              </div>
            )}
          </fieldset>

          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label id="cs-assignment-policy-label">Policy</Label>
              <ValuePicker
                labelledBy="cs-assignment-policy-label"
                value={policyId}
                options={policies.map((p) => ({ value: p.id, label: p.name, detail: summarizePolicy(p) }))}
                onChange={setPolicyId}
                placeholder="Choose a policy"
                searchPlaceholder="Search policies…"
                emptyText="No policy found."
              />
            </div>
            <div className="space-y-2">
              <Label id="cs-assignment-mode-label">Mode</Label>
              <Select value={mode} onValueChange={(v) => v && setMode(v as 'WARN' | 'ENFORCE')}>
                <SelectTrigger aria-labelledby="cs-assignment-mode-label" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="ENFORCE">Enforce (block and throttle)</SelectItem>
                  <SelectItem value="WARN">Test only (log, don&apos;t act)</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="space-y-3">
            <DateTimeField id="cs-assignment-starts" label="Starts" value={starts} onChange={setStarts} emptyHint="Immediately" />
            <DateTimeField id="cs-assignment-ends" label="Ends" value={ends} onChange={setEnds} emptyHint="No end" />
            <p className="text-[11px] text-muted-foreground">
              Pickers use your browser&apos;s local time; the UTC value that is saved is shown beside each. The start is
              inclusive and the end is exclusive.
            </p>
            {windowError && (
              <p role="alert" className="text-xs text-destructive">
                {windowError}
              </p>
            )}
          </div>

          {error && (
            <p role="alert" className="text-xs text-destructive">
              {error}
            </p>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={busy}>
            Cancel
          </Button>
          <Button onClick={() => void save()} disabled={!canSave}>
            {busy && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
            {busy ? 'Saving…' : 'Save'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
