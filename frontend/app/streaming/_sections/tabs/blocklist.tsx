'use client'

import React from 'react'
import Link from 'next/link'
import { useQueryClient } from '@tanstack/react-query'
import { Ban, Loader2 } from 'lucide-react'
import { client, extractApiError } from '@/lib/api'
import { buildServiceHref } from '@/lib/navigation'
import { showToast, showToastWithAction } from '@/lib/toast'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import type { components } from '@/types/api.generated'

type BlocksData = components['schemas']['ContentSecurityBlocksResponse']
export type Block = components['schemas']['ContentSecurityBlock']
/** One `{category, value}` pair; a block matches only when ALL of its conditions do. */
export type BlockCondition = components['schemas']['ContentSecurityBlockCondition']
export type BlockCategory = BlockCondition['category']

export const BLOCK_CATEGORY_LABELS: Record<BlockCategory, string> = {
  country: 'Country',
  referer: 'Referer',
  host: 'Host',
  proxy_type: 'Proxy type',
  proxy_description: 'Proxy description',
  subscriber_id: 'Subscriber ID',
  ip: 'IP address',
}

/** One pickable value in the Block dialog. `display` is the human label (e.g. a country name). */
export interface BlockOption {
  category: BlockCategory
  value: string
  display?: string
}

const CUSTOM = '__custom__'

/** Admin-only: the service's active blocks and whether a blocklist URL is configured. */
export function useBlocklist(activeServiceId: string | null, enabled: boolean) {
  return useServiceQuery<BlocksData | undefined>(
    ['content-security', 'blocks', activeServiceId],
    async ({ signal }) => {
      const { data, error } = await client.GET('/api/admin/content-security/blocks', { signal })
      if (error) throw error
      return data
    },
    { enabled },
  )
}

/** "Country RS" or, for a multi-condition block, "Country RS and Proxy type hosting". */
export function describeConditions(conditions: BlockCondition[]) {
  return conditions.map((c) => `${BLOCK_CATEGORY_LABELS[c.category]} ${c.value}`).join(' and ')
}

export function useBlockActions(activeServiceId: string | null) {
  const queryClient = useQueryClient()
  const invalidate = React.useCallback(
    () => queryClient.invalidateQueries({ queryKey: ['content-security', 'blocks', activeServiceId] }),
    [queryClient, activeServiceId],
  )

  const unblock = React.useCallback(
    async (block: Block) => {
      // Failures surface through the api client's mutation-error toast.
      const { error } = await client.DELETE('/api/admin/content-security/blocks/{block_id}', {
        params: { path: { block_id: block.id } },
      })
      await invalidate()
      if (error) throw error
      showToast(`Unblocked ${describeConditions(block.conditions)}`, 'info')
    },
    [invalidate],
  )

  const block = React.useCallback(
    async (conditions: BlockCondition[]) => {
      const { data, error } = await client.POST('/api/admin/content-security/blocks', { body: { conditions } })
      if (error) throw error
      await invalidate()
      showToastWithAction(`Blocked ${describeConditions(data.conditions)}`, {
        actionLabel: 'Undo',
        kind: 'success',
        durationMs: 10_000,
        onAction: () => void unblock(data).catch(() => {}),
      })
      return data
    },
    [invalidate, unblock],
  )

  return { block, unblock }
}

/** Confirm a block: pick the category and value (defaulting to the one clicked), then call the blocklist function. */
export function BlockDialog({
  open,
  onOpenChange,
  options,
  initial,
  configured,
  activeServiceId,
  onConfirm,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  options: BlockOption[]
  initial: BlockCondition | null
  /** undefined while the blocks query is loading. */
  configured: boolean | undefined
  activeServiceId: string | null
  onConfirm: (conditions: BlockCondition[]) => Promise<unknown>
}) {
  const categories = React.useMemo(() => [...new Set(options.map((o) => o.category))], [options])
  const [category, setCategory] = React.useState<BlockCategory | null>(initial?.category ?? null)
  const [choice, setChoice] = React.useState<string>(initial?.value ?? '')
  const [custom, setCustom] = React.useState('')
  const [busy, setBusy] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)

  // Re-seed from the clicked value each time the dialog opens.
  React.useEffect(() => {
    if (!open) return
    /* eslint-disable react-hooks/set-state-in-effect */
    setCategory(initial?.category ?? categories[0] ?? null)
    setChoice(initial?.value ?? '')
    setCustom('')
    setError(null)
    /* eslint-enable react-hooks/set-state-in-effect */
  }, [open, initial, categories])

  const valueOptions = options.filter((o) => o.category === category)
  const value = (choice === CUSTOM ? custom : choice).trim()
  const canConfirm = configured === true && category != null && value !== '' && !busy

  const changeCategory = (next: BlockCategory) => {
    setCategory(next)
    setChoice(options.find((o) => o.category === next)?.value ?? CUSTOM)
    setCustom('')
  }

  const confirm = async () => {
    if (!canConfirm || category == null) return
    setBusy(true)
    setError(null)
    try {
      // The dialog builds one condition today; the API takes the full AND list.
      await onConfirm([{ category, value }])
      onOpenChange(false)
    } catch (e) {
      setError(extractApiError(e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={(next) => !busy && onOpenChange(next)}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-base">
            <Ban className="h-4 w-4" aria-hidden="true" />
            Block access
          </DialogTitle>
          <DialogDescription>
            Sends the category and value to your blocklist endpoint. You can undo this from the notification or the
            Blocked list.
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
            <Select value={choice || undefined} onValueChange={(v) => v && setChoice(v)}>
              <SelectTrigger aria-labelledby="block-value-label" className="w-full font-mono text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {valueOptions.map((o) => (
                  <SelectItem key={o.value} value={o.value} className="font-mono text-xs">
                    {o.display ?? o.value}
                  </SelectItem>
                ))}
                <SelectItem value={CUSTOM}>Custom value…</SelectItem>
              </SelectContent>
            </Select>
            {choice === CUSTOM && (
              <Input
                aria-label={`Custom ${category ? BLOCK_CATEGORY_LABELS[category].toLowerCase() : 'value'}`}
                className="font-mono text-xs"
                value={custom}
                onChange={(e) => setCustom(e.target.value)}
                autoFocus
              />
            )}
          </div>

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
          <Button variant="destructive" onClick={() => void confirm()} disabled={!canConfirm}>
            {busy ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Ban className="h-3 w-3 mr-1" />}
            {busy ? 'Blocking…' : 'Block'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
