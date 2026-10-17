'use client'

import React from 'react'
import Link from 'next/link'
import { Ban, Gauge, Loader2, ShieldCheck } from 'lucide-react'
import { client, extractApiError } from '@/lib/api'
import { buildServiceHref } from '@/lib/navigation'
import { showToastWithAction } from '@/lib/toast'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { ValuePicker } from '@/components/ui/value-picker'
import {
  BLOCK_CATEGORY_LABELS,
  THROTTLE_KBPS,
  describeRule,
  isRuleCategory,
  usePolicyActions,
  type BlockCategory,
  type EffectivePolicy,
  type LiveRuleBody,
  type LiveRuleResult,
  type RuleMark,
  modeLabel,
} from './policyApi'

export { BLOCK_CATEGORY_LABELS, THROTTLE_KBPS, describeRule, removeLabel } from './policyApi'
export type { BlockCategory } from './policyApi'

/** One `{category, value}` pair offered by, or passed to, the Block dialog. */
export interface BlockCondition {
  category: BlockCategory
  value: string
}

/** One pickable value in the Block dialog. `display` is the human label (e.g. a country name). */
export interface BlockOption extends BlockCondition {
  display?: string
}

const CUSTOM = '__custom__'

/** Inline mark beside a value with a rule: red ban (block), amber gauge (throttle) or green shield (allow). */
export function BlockMark({ rule }: { rule: RuleMark }) {
  const label = describeRule(rule)
  const className =
    rule.action === 'block'
      ? 'inline-flex shrink-0 text-destructive'
      : rule.action === 'pace'
        ? 'inline-flex shrink-0 text-amber-600 dark:text-amber-500'
        : 'inline-flex shrink-0 text-emerald-700 dark:text-emerald-500'
  return (
    <span className={className} title={label}>
      {rule.action === 'block' ? (
        <Ban className="h-3.5 w-3.5" aria-hidden="true" />
      ) : rule.action === 'pace' ? (
        <Gauge className="h-3.5 w-3.5" aria-hidden="true" />
      ) : (
        <ShieldCheck className="h-3.5 w-3.5" aria-hidden="true" />
      )}
      <span className="sr-only">{label}</span>
    </span>
  )
}

function verb(rule: RuleMark, what: string) {
  if (rule.action === 'block') return `Blocked ${what}`
  if (rule.action === 'pace') return `Throttled ${what} to ${rule.kbps} kbps`
  return `Allowed ${what}`
}

/**
 * Live-rule actions for the Monitor view: add a rule to the policy in effect (the server creates a
 * Default policy and assignment when none is), and undo or remove one.
 */
export function useLiveRuleActions() {
  const { removeRule, announce, invalidate } = usePolicyActions()

  const unblock = React.useCallback(
    async (policyId: string, category: BlockCategory, value: string, rule: RuleMark) => {
      const what = `${BLOCK_CATEGORY_LABELS[category]} ${value}`
      const message =
        rule.action === 'block'
          ? `Unblocked ${what}`
          : rule.action === 'pace'
            ? `Removed throttle on ${what}`
            : `Removed allow rule for ${what}`
      // Failures surface through the api client's mutation-error toast.
      await removeRule(policyId, category, value, message)
    },
    [removeRule],
  )

  /** Add a rule; an Undo toast for 10 s when it is live, a Retry toast when it is saved but not. */
  const addRule = React.useCallback(
    async (body: LiveRuleBody): Promise<LiveRuleResult> => {
      const { data, error } = await client.POST('/api/admin/content-security/live-rule', { body })
      if (error) throw error
      const rule: RuleMark = { action: body.action, kbps: body.kbps }
      const what = `${BLOCK_CATEGORY_LABELS[body.category]} ${body.value}`
      const message = verb(rule, what) + (data.created_default ? '. Created a Default policy for all content' : '')
      if (data.publish.status === 'published') {
        await invalidate()
        showToastWithAction(message, {
          actionLabel: 'Undo',
          kind: 'success',
          durationMs: 10_000,
          onAction: () => void unblock(data.policy.id, body.category, body.value, rule).catch(() => {}),
        })
      } else {
        await announce(data.publish, message)
      }
      return data
    },
    [announce, invalidate, unblock],
  )

  return { addRule, unblock }
}

/**
 * Confirm a rule: pick the category and value (defaulting to the one clicked) and the action, then
 * add it to the policy in effect for the content being viewed.
 */
export function BlockDialog({
  open,
  onOpenChange,
  options,
  initial,
  effective,
  activeServiceId,
  contentId,
  onConfirm,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  options: BlockOption[]
  initial: BlockCondition | null
  /** Policy in effect for `contentId`; undefined while loading. */
  effective: EffectivePolicy | undefined
  activeServiceId: string | null
  /** Content being viewed (null = all content). */
  contentId: string | null
  onConfirm: (body: LiveRuleBody) => Promise<unknown>
}) {
  const categories = React.useMemo(() => [...new Set(options.map((o) => o.category))], [options])
  const [category, setCategory] = React.useState<BlockCategory | null>(initial?.category ?? null)
  const [choice, setChoice] = React.useState<string>(initial?.value ?? '')
  const [custom, setCustom] = React.useState('')
  // 'block' | 'exempt' (shown as Allow) | '128' | '256' | '512'
  const [action, setAction] = React.useState<string>('block')
  const [busy, setBusy] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)

  // Re-seed from the clicked value each time the dialog opens.
  React.useEffect(() => {
    if (!open) return
    /* eslint-disable react-hooks/set-state-in-effect */
    setCategory(initial?.category ?? categories[0] ?? null)
    setChoice(initial?.value ?? '')
    setCustom('')
    setAction('block')
    setError(null)
    /* eslint-enable react-hooks/set-state-in-effect */
  }, [open, initial, categories])

  const individual = category != null && !isRuleCategory(category)
  const valueOptions = options.filter((o) => o.category === category)
  const value = (choice === CUSTOM ? custom : choice).trim()
  const configured = effective?.configured
  const paceKbps = Number(action)
  const isPace = action !== 'block' && action !== 'exempt'
  const rateOk = !isPace || (Number.isInteger(paceKbps) && paceKbps > 0)
  const canConfirm = configured === true && category != null && value !== '' && rateOk && !busy

  const changeCategory = (next: BlockCategory) => {
    setCategory(next)
    setChoice(options.find((o) => o.category === next)?.value ?? CUSTOM)
    setCustom('')
    // Individual rules are block-only.
    if (!isRuleCategory(next)) setAction('block')
  }

  const confirm = async () => {
    if (!canConfirm || category == null) return
    setBusy(true)
    setError(null)
    try {
      const body: LiveRuleBody = {
        category,
        value,
        action: isPace ? 'pace' : (action as 'block' | 'exempt'),
        kbps: isPace ? paceKbps : 0,
        content_id: contentId,
      }
      await onConfirm(body)
      onOpenChange(false)
    } catch (e) {
      setError(extractApiError(e))
    } finally {
      setBusy(false)
    }
  }

  const policy = effective?.policy
  const assignment = effective?.assignment
  const scope = contentId ?? 'all content'

  return (
    <Dialog open={open} onOpenChange={(next) => !busy && onOpenChange(next)}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-base">
            <Ban className="h-4 w-4" aria-hidden="true" />
            Block, throttle or allow
          </DialogTitle>
          <DialogDescription>
            Adds a rule to the policy in effect and publishes it to the blocker. You can undo this from the notification
            or the Blocked &amp; Throttled list.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 pt-3">
          {categories.length > 1 && (
            <div className="space-y-2">
              <Label id="block-category-label">Category</Label>
              <Select value={category ?? undefined} onValueChange={(v) => v && changeCategory(v as BlockCategory)}>
                <SelectTrigger aria-labelledby="block-category-label" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {categories.map((c) => (
                    <SelectItem key={c} value={c}>
                      {BLOCK_CATEGORY_LABELS[c]}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}

          <div className="space-y-2">
            <Label id="block-value-label">{category ? BLOCK_CATEGORY_LABELS[category] : 'Value'}</Label>
            <ValuePicker
              labelledBy="block-value-label"
              value={value}
              options={valueOptions.map((o) => ({
                value: o.value,
                label: o.display ?? o.value,
                detail: o.display && o.display !== o.value ? o.value : undefined,
              }))}
              allowCustom
              mono
              onChange={(next) => {
                if (valueOptions.some((o) => o.value === next)) {
                  setChoice(next)
                } else {
                  setChoice(CUSTOM)
                  setCustom(next)
                }
              }}
              placeholder="Choose or enter a value"
              searchPlaceholder="Search or type a value…"
              emptyText="Type a value to use it."
            />
          </div>

          <div className="space-y-2">
            <Label id="block-action-label">Action</Label>
            <Select value={action} onValueChange={(v) => v != null && setAction(v)}>
              <SelectTrigger aria-labelledby="block-action-label" className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="block">Block (no traffic allowed)</SelectItem>
                {!individual &&
                  THROTTLE_KBPS.map((rate) => (
                    <SelectItem key={rate} value={String(rate)}>
                      {`Throttle to: ${rate} kbps`}
                    </SelectItem>
                  ))}
                {!individual && <SelectItem value="exempt">Allow (skip this policy&apos;s other rules)</SelectItem>}
              </SelectContent>
            </Select>
            {individual && (
              <p className="text-[11px] text-muted-foreground">
                Subscriber ID and IP rules can only block. They also take precedence over Allow rules.
              </p>
            )}
          </div>

          {effective && configured !== false && (
            <div className="space-y-2 rounded-md border bg-muted/40 p-2.5 text-xs">
              {policy ? (
                <p>
                  Adds the rule to <strong>{policy.name}</strong>, the policy in effect for {scope}
                  {assignment ? ` (${modeLabel(assignment.mode)})` : ''}.
                </p>
              ) : (
                <p>
                  No policy is in effect for {scope}. A <strong>Default</strong> policy will be created and scheduled for all
                  content in Enforce mode.
                </p>
              )}
              {policy && effective.shared_with > 0 && (
                <p role="note" className="text-amber-800 dark:text-amber-400">
                  This policy is shared: it is also scheduled for {effective.shared_with} other{' '}
                  {effective.shared_with === 1 ? 'entry' : 'entries'} in this service, and the change applies to
                  all of them.
                </p>
              )}
              {policy && assignment?.scope_type === 'default' && contentId && (
                <p role="note" className="text-amber-800 dark:text-amber-400">
                  {contentId} has no schedule of its own, so this is the all-content policy and the change applies to all
                  content.
                </p>
              )}
            </div>
          )}

          {configured === false && (
            <p className="text-xs text-muted-foreground">
              No blocklist endpoint is configured. Set one in{' '}
              <Link className="underline" href={buildServiceHref('/admin', activeServiceId)}>
                Admin → Overall Settings
              </Link>
              .
            </p>
          )}
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
          <Button variant={action === 'exempt' ? 'default' : 'destructive'} onClick={() => void confirm()} disabled={!canConfirm}>
            {busy ? (
              <Loader2 className="h-3 w-3 mr-1 animate-spin" />
            ) : action === 'block' ? (
              <Ban className="h-3 w-3 mr-1" />
            ) : action === 'exempt' ? (
              <ShieldCheck className="h-3 w-3 mr-1" />
            ) : (
              <Gauge className="h-3 w-3 mr-1" />
            )}
            {action === 'block'
              ? busy
                ? 'Blocking…'
                : 'Block'
              : action === 'exempt'
                ? busy
                  ? 'Allowing…'
                  : 'Allow'
                : busy
                  ? 'Throttling…'
                  : 'Throttle'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
