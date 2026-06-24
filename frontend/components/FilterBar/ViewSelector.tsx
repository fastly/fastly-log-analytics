'use client'

import * as React from 'react'
import { ChevronsUpDown, Trash2 } from 'lucide-react'
import { useRouter } from 'next/navigation'
import { useServiceStore } from '@/stores/serviceStore'
import { client } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useBootstrapPending } from '@/hooks/useIsDataReady'

import { Button } from '@/components/ui/button'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { showToast, showToastWithAction } from '@/lib/toast'
import type { components } from '@/types/api.generated'

type SavedView = components["schemas"]["SavedView"]

export function ViewSelector() {
  const [open, setOpen] = React.useState(false)
  const router = useRouter()
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const queryClient = useQueryClient()

  // Perf audit Phase D: useBootstrap seeds ['views', service_id] in
  // its queryFn. Gate on bootstrap pending so this query hits the
  // seeded cache on cold load instead of racing the seed.
  const bootstrapPending = useBootstrapPending()

  const { data: views } = useQuery({
    queryKey: ['views', activeServiceId],
    queryFn: async () => {
      if (!activeServiceId) return []
      const { data } = await client.GET("/api/views/{service_id}", {
        params: { path: { service_id: activeServiceId } }
      })
      return data as any
    },
    enabled: !!activeServiceId && !bootstrapPending,
  })

  // IDs hidden from the list while the post-confirm undo window is open.
  // The DELETE fires after the toast duration unless Undo cancels the timer.
  const [pendingDeleteIds, setPendingDeleteIds] = React.useState<Set<string>>(() => new Set())
  const pendingTimersRef = React.useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map())
  React.useEffect(() => {
    const timers = pendingTimersRef.current
    return () => {
      for (const [, t] of timers) clearTimeout(t)
      timers.clear()
    }
  }, [])

  // Controlled-mode ConfirmDialog state. We can't use native confirm() — it's
  // unstyled, blocks on iOS WebView, and ignores theme. Track the candidate
  // view in state; on confirm, the existing undo-toast delete flow runs.
  const [viewPendingConfirm, setViewPendingConfirm] = React.useState<SavedView | null>(null)

  const visibleViews: SavedView[] = React.useMemo(
    () => (views ?? []).filter((v: SavedView) => !pendingDeleteIds.has(v.id!)),
    [views, pendingDeleteIds],
  )

  const requestDelete = (e: React.MouseEvent, view: SavedView) => {
    e.stopPropagation()
    if (!view.id) return
    setViewPendingConfirm(view)
  }

  const runDelete = (view: SavedView) => {
    if (!view.id) return
    const id = view.id
    setPendingDeleteIds((prev) => {
      const next = new Set(prev)
      next.add(id)
      return next
    })
    const timer = setTimeout(async () => {
      pendingTimersRef.current.delete(id)
      try {
        await client.DELETE("/api/views/{view_id}", {
          params: { path: { view_id: id } }
        })
        queryClient.invalidateQueries({ queryKey: ['views', activeServiceId] })
        // Bootstrap response also carries seeded views; invalidate so the
        // next bootstrap refetch reflects the deletion.
        queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() })
      } catch {
        setPendingDeleteIds((prev) => {
          const next = new Set(prev)
          next.delete(id)
          return next
        })
        showToast(`Failed to delete view "${view.name}"`, 'error')
      }
    }, 5500)
    pendingTimersRef.current.set(id, timer)
    showToastWithAction(`Deleted view "${view.name}"`, {
      actionLabel: 'Undo',
      onAction: () => {
        const t = pendingTimersRef.current.get(id)
        if (t) {
          clearTimeout(t)
          pendingTimersRef.current.delete(id)
        }
        setPendingDeleteIds((prev) => {
          const next = new Set(prev)
          next.delete(id)
          return next
        })
      },
      durationMs: 5500,
    })
  }

  const handleSelect = (view: SavedView) => {
    const url = new URL(window.location.href)
    url.searchParams.set('view', view.id!)
    router.push(url.toString())
    setOpen(false)
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      {/* aria-label lives on PopoverTrigger directly (not on the Button
          render-prop) so it survives Base UI's SSR pass — render-prop
          merging drops aria-label from the inner element on the server,
          leaving the SSR'd button with no accessible name. Same pattern
          fixed for DashboardHeader's Cards button. */}
      <PopoverTrigger
        aria-label="Saved Views"
        render={
          <Button
            variant="outline"
            role="combobox"
            aria-expanded={open}
            className="h-9 sm:h-8 justify-between min-w-[140px] text-xs"
          >
            <span className="truncate">Saved Views</span>
            <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
          </Button>
        }
      />
      <PopoverContent className="w-[200px] p-0">
        <Command>
          <CommandInput placeholder="Search views..." />
          <CommandEmpty>No view found.</CommandEmpty>
          <CommandList>
            <CommandGroup>
              {visibleViews.map((view) => (
                <CommandItem
                  key={view.id}
                  value={view.name}
                  onSelect={() => handleSelect(view)}
                  className="flex items-center justify-between"
                >
                  <span className="truncate flex-1">{view.name}</span>
                  <Button
                    variant="ghost"
                    size="icon"
                    aria-label={`Delete view ${view.name}`}
                    className="h-6 w-6 text-muted-foreground hover:text-destructive"
                    onClick={(e) => requestDelete(e, view)}
                  >
                    <Trash2 className="h-3 w-3" />
                  </Button>
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
      <ConfirmDialog
        open={viewPendingConfirm !== null}
        onOpenChange={(o) => { if (!o) setViewPendingConfirm(null) }}
        title="Delete saved view"
        description={
          viewPendingConfirm
            ? `Delete view "${viewPendingConfirm.name}"? You'll have a few seconds to undo.`
            : ''
        }
        confirmLabel="Delete"
        isDangerous
        onConfirm={() => {
          if (viewPendingConfirm) {
            runDelete(viewPendingConfirm)
          }
          setViewPendingConfirm(null)
        }}
      />
    </Popover>
  )
}
