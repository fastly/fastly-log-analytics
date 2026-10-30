'use client'

import * as React from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { usePathname, useRouter } from 'next/navigation'
import { Bookmark, ChevronsUpDown, Trash2 } from 'lucide-react'
import { useShallow } from 'zustand/react/shallow'
import { client, queryKeys, unwrapClient } from '@/lib/api'
import { useBootstrapPending } from '@/hooks/useBootstrap'
import { useDeferredDeleteWithUndo } from '@/hooks/useSSE'
import { useFilterStore, useServiceStore } from '@/stores/filterStore'
import type { FilterPill } from '@/types/api'
import type { components } from '@/types/api.generated'
import { Button } from '@/components/ui/button'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'

export type SavedView = components['schemas']['SavedViewRecord']

export interface SavedViewStoreSnapshot {
  edgeOnly: boolean
  compareMode: boolean
  addFilter: (column: string, value: string, mode: 'include' | 'exclude') => void
  clearFilters: () => void
  setRange: (start: string, end: string) => void
  setRelativeRange: (label: string, start: string, end: string) => void
  toggleEdgeOnly: () => void
  toggleCompareMode: () => void
  setCompareRange: (start: string, end: string) => void
}

export function applySavedViewToStore(
  view: SavedView,
  fallbackStore?: SavedViewStoreSnapshot
): boolean {
  // filters_json carries TWO shapes for backward compatibility:
  //   - Legacy (older saved views): a bare FilterPill[] array.
  //   - Current: { filters: FilterPill[], _view_extras: {...} }
  //     where extras carries edgeOnly / compareMode / relativeRange.
  // Detect by Array.isArray and apply each branch's recovery.
  let parsed: ReturnType<typeof JSON.parse>
  try {
    parsed = JSON.parse(view.filters_json)
  } catch {
    return false
  }
  const isLegacy = Array.isArray(parsed)
  const pills = isLegacy ? parsed : parsed?.filters
  const extras = isLegacy ? null : parsed?._view_extras

  // Read latest toggle state after any async fetch (falling back to
  // hook-subscribed state when useFilterStore is mocked in unit tests).
  const liveState =
    typeof useFilterStore.getState === 'function' ? useFilterStore.getState() : null
  const store = liveState ?? fallbackStore
  if (!store) return false

  // Edge-only toggle — flip if it differs from current.
  if (extras?.edgeOnly != null && extras.edgeOnly !== store.edgeOnly) store.toggleEdgeOnly()

  // Compare-mode toggle + range MUST run BEFORE setRelativeRange / setRange
  // because toggleCompareMode() snaps startTime/endTime and clears relativeRange.
  if (extras?.compareMode != null && extras.compareMode !== store.compareMode) {
    store.toggleCompareMode()
  }
  if (extras?.compareStartTime && extras?.compareEndTime) {
    store.setCompareRange(extras.compareStartTime, extras.compareEndTime)
  }

  // Range — prefer relativeRange when extras carry one (rolling window
  // re-derived from now), else fall back to the saved absolute range.
  if (view.start_time && view.end_time) {
    if (extras?.relativeRange) {
      // setRelativeRange records the label so the URL-sync round-trips
      // as ?range=<label> and reload re-derives [now-duration, now].
      store.setRelativeRange(extras.relativeRange, view.start_time, view.end_time)
    } else {
      store.setRange(view.start_time, view.end_time)
    }
  }

  store.clearFilters()
  if (Array.isArray(pills)) {
    pills.forEach((f: FilterPill) => store.addFilter(f.column, f.value, f.mode))
  }
  return true
}

export function SaveViewDialog() {
  const [open, setOpen] = React.useState(false)
  const [name, setName] = React.useState('')

  // Capture the FULL store snapshot so restored views match the original
  // intent — previously edgeOnly / compareMode / relativeRange were silent
  // and a saved rolling-24h view came back as a frozen absolute window.
  const {
    startTime,
    endTime,
    filters,
    edgeOnly,
    compareMode,
    compareStartTime,
    compareEndTime,
    relativeRange,
  } = useFilterStore(
    useShallow((s) => ({
      startTime: s.startTime,
      endTime: s.endTime,
      filters: s.filters,
      edgeOnly: s.edgeOnly,
      compareMode: s.compareMode,
      compareStartTime: s.compareStartTime,
      compareEndTime: s.compareEndTime,
      relativeRange: s.relativeRange,
    }))
  )
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const pathname = usePathname()
  const queryClient = useQueryClient()

  const saveMutation = useMutation({
    mutationFn: async () => {
      if (!name || !activeServiceId) return
      // The backend stores filters_json as an opaque string; bundle the
      // extra controls into the same blob under a `_view_extras` key the
      // restore-handler reads back. Older saved views (no _view_extras)
      // still restore unchanged — the keys default to the store-init values.
      const filterPayload = {
        filters,
        _view_extras: { edgeOnly, compareMode, compareStartTime, compareEndTime, relativeRange },
      }
      await unwrapClient(
        client.POST('/api/views/', {
          body: {
            service_id: activeServiceId,
            name,
            filters_json: JSON.stringify(filterPayload),
            start_time: startTime,
            end_time: endTime,
            page: pathname || '/dashboard',
          },
        })
      )
    },
    onSuccess: () => {
      if (!activeServiceId) return
      queryClient.invalidateQueries({ queryKey: queryKeys.views(activeServiceId) })
      // Bootstrap response also carries seeded views; invalidate so the
      // next bootstrap refetch reflects the new view.
      queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() })
      setOpen(false)
      setName('')
    },
    onError: (error) => console.error('Failed to save view', error),
  })
  const isSaving = saveMutation.isPending

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        render={
          <Button variant="outline" size="sm" className="h-9 sm:h-8 gap-1.5 text-xs">
            <Bookmark className="h-3.5 w-3.5" />
            Save View
          </Button>
        }
      />
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle>Save Current View</DialogTitle>
          <DialogDescription>
            Save your current filters and time range to access them later.
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 py-4">
          <div className="grid grid-cols-4 items-center gap-4">
            <Label htmlFor="name" className="text-right">
              Name
            </Label>
            <Input
              id="name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. Weekly Error Spikes"
              className="col-span-3"
            />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button
            onClick={() => {
              if (name && activeServiceId) saveMutation.mutate()
            }}
            disabled={!name || isSaving}
          >
            {isSaving ? 'Saving...' : 'Save View'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

export function ViewSelector() {
  const [open, setOpen] = React.useState(false)
  const router = useRouter()
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const queryClient = useQueryClient()

  // Perf audit Phase D: useBootstrap seeds ['views', service_id] in
  // its queryFn. Gate on bootstrap pending so this query hits the
  // seeded cache on cold load instead of racing the seed.
  const bootstrapPending = useBootstrapPending()

  const { data: views } = useQuery({
    queryKey: queryKeys.views(activeServiceId),
    queryFn: async () => {
      if (!activeServiceId) return []
      return unwrapClient(
        client.GET('/api/views/{service_id}', { params: { path: { service_id: activeServiceId } } })
      )
    },
    enabled: !!activeServiceId && !bootstrapPending,
  })

  const { pendingDeleteIds, scheduleDelete } = useDeferredDeleteWithUndo()

  // Controlled-mode ConfirmDialog state. We can't use native confirm() — it's
  // unstyled, blocks on iOS WebView, and ignores theme. Track the candidate
  // view in state; on confirm, the existing undo-toast delete flow runs.
  const [viewPendingConfirm, setViewPendingConfirm] = React.useState<SavedView | null>(null)

  const visibleViews: SavedView[] = React.useMemo(
    () => (Array.isArray(views) ? views : []).filter((v) => !pendingDeleteIds.has(v.id)),
    [views, pendingDeleteIds]
  )

  const runDelete = (view: SavedView) => {
    if (!view.id) return
    const id = view.id
    scheduleDelete({
      id,
      toastMessage: `Deleted view "${view.name}"`,
      performDelete: async () => {
        await unwrapClient(
          client.DELETE('/api/views/{view_id}', { params: { path: { view_id: id } } })
        )
        queryClient.invalidateQueries({ queryKey: queryKeys.views(activeServiceId) })
        // Bootstrap response also carries seeded views; invalidate so the
        // next bootstrap refetch reflects the deletion.
        queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() })
      },
      errorMessage: `Failed to delete view "${view.name}"`,
    })
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
                  onSelect={() => {
                    const url = new URL(window.location.href)
                    url.searchParams.set('view', view.id!)
                    router.push(url.toString())
                    setOpen(false)
                  }}
                  className="flex items-center justify-between"
                >
                  <span className="truncate flex-1">{view.name}</span>
                  <Button
                    variant="ghost"
                    size="icon"
                    aria-label={`Delete view ${view.name}`}
                    className="h-6 w-6 text-muted-foreground hover:text-destructive"
                    onClick={(e) => {
                      e.stopPropagation()
                      if (view.id) setViewPendingConfirm(view)
                    }}
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
        onOpenChange={(o) => {
          if (!o) setViewPendingConfirm(null)
        }}
        title="Delete saved view"
        description={
          viewPendingConfirm
            ? `Delete view "${viewPendingConfirm.name}"? You'll have a few seconds to undo.`
            : ''
        }
        confirmLabel="Delete"
        isDangerous
        onConfirm={() => {
          if (viewPendingConfirm) runDelete(viewPendingConfirm)
          setViewPendingConfirm(null)
        }}
      />
    </Popover>
  )
}
