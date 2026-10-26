'use client'

import * as React from 'react'
import { useForm, useWatch } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { X, Bot, Bookmark, ChevronsUpDown, Trash2, Info, Plus, Filter, Search, Loader2, Check } from 'lucide-react'

import { cn, isIpFamilyField } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { useFilterStore, useTimezoneStore, useServiceStore } from '@/stores/filterStore'
import { formatForInput, parseFromInput, formatCount } from '@/lib/format'
import { useIsFetching, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { usePathname, useRouter } from 'next/navigation'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog'
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from '@/components/ui/command'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { client, unwrapClient } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'
import { useDebounce } from '@/hooks/useDashboardCards'
import { useBootstrapPending, useFieldValues, useLogFieldsCatalog, useMaskIps, useMounted, useServiceSwitchReset } from '@/hooks/useBootstrap'
import { useDeferredDeleteWithUndo } from '@/hooks/useSSE'
import type { components } from '@/types/api.generated'
import type { FilterPill } from '@/types/api'
import { useShallow } from 'zustand/react/shallow'

const COMMON_FIELD_IDS = [
  'ip', 'url', 'host', 'method', 'status', 'ua', 'country', 'city',
  'asn', 'p_type', 'p_desc', 'ja4', 'ja3', 'cache', 'edge', 'pop',
  'backend', 'proto', 'tls', 'referer', 'waf', 'waf_resp',
]

const addFilterSchema = z.object({
  field: z.string().min(1),
  value: z.string().min(1),
  mode: z.enum(['include', 'exclude']),
})

type AddFilterValues = z.infer<typeof addFilterSchema>

export function AddFilterDialogContent({ onClose }: { onClose: () => void }) {
  const maskIps = useMaskIps()
  const { register, handleSubmit, setValue, control, formState: { isValid } } = useForm<AddFilterValues>({
    resolver: zodResolver(addFilterSchema),
    mode: 'onChange',
    defaultValues: { field: maskIps ? 'url' : 'ip', value: '', mode: 'include' },
  })
  const { field, value, mode } = useWatch({ control }) as AddFilterValues
  const debouncedValue = useDebounce(value)

  const { data: catalog } = useLogFieldsCatalog()
  const commonFields = COMMON_FIELD_IDS
    .filter(id => !(maskIps && isIpFamilyField(id)))
    .map(id => ({ value: id, label: catalog?.fields?.find((f) => f.id === id)?.label ?? id }))

  const addFilter = useFilterStore(s => s.addFilter)
  const { data: suggestions, isLoading: isLoadingSuggestions } = useFieldValues({
    field, search: debouncedValue, limit: 10, enabled: debouncedValue.length > 0,
  })

  const handleAdd = handleSubmit((values) => {
    addFilter(values.field, values.value, values.mode)
    setValue('value', '')
    onClose()
  })

  return (
    <DialogContent className="sm:max-w-[420px] p-0 overflow-hidden gap-0">
      <form onSubmit={handleAdd}>
        <div className="p-5 pb-4">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2.5 text-lg font-semibold">
              <div className="flex h-8 w-8 items-center justify-center rounded-md bg-primary/10 border border-primary/20 shrink-0">
                <Filter className="h-4 w-4 text-primary" />
              </div>
              Add Custom Filter
            </DialogTitle>
          </DialogHeader>
        </div>

        <div className="px-5 py-6 bg-muted/30 border-y grid gap-5">
          <div className="grid gap-2">
            <Label className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest">Filter Type</Label>
            <Tabs value={mode} onValueChange={(v) => { if (v === 'include' || v === 'exclude') setValue('mode', v) }} className="w-full">
              <TabsList className="grid w-full grid-cols-2 h-9">
                <TabsTrigger value="include" className="text-xs gap-1.5"><Check className="h-3 w-3 text-green-500" /> Include</TabsTrigger>
                <TabsTrigger value="exclude" className="text-xs gap-1.5"><X className="h-3 w-3 text-red-500" /> Exclude</TabsTrigger>
              </TabsList>
            </Tabs>
          </div>

          <div className="grid gap-2">
            <Label className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest">Field</Label>
            <Select value={field} onValueChange={val => val && setValue('field', val)}>
              <SelectTrigger className="bg-background shadow-sm h-10 w-full justify-between pr-3">
                <SelectValue placeholder="Select field">
                  {(val) => commonFields.find(f => f.value === val)?.label ?? String(val)}
                </SelectValue>
              </SelectTrigger>
              <SelectContent align="start" className="max-h-[300px]">
                {commonFields.map(f => <SelectItem key={f.value} value={f.value}>{f.label}</SelectItem>)}
              </SelectContent>
            </Select>
          </div>

          <div className="grid gap-2">
            <Label className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest">Match Value</Label>
            <div className="relative group">
              <div className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none group-focus-within:text-primary transition-colors">
                <Search className="h-3.5 w-3.5" />
              </div>
              <Input
                placeholder={`Type a ${field} value...`} {...register('value')}
                className="bg-background shadow-sm h-10 pl-9" autoFocus autoComplete="off"
              />
              {isLoadingSuggestions && (
                <div className="absolute right-3 top-1/2 -translate-y-1/2">
                  <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
                </div>
              )}
            </div>

            {value.length > 0 && suggestions?.values && suggestions.values.length > 0 && (
              <div className="mt-1 border rounded-md bg-background shadow-lg overflow-hidden animate-in fade-in zoom-in-95 duration-100">
                <ScrollArea className={cn("max-h-[160px]", suggestions.values.length > 5 ? "h-[160px]" : "")}>
                  <div className="p-1">
                    {suggestions.values.map((v) => (
                      <button
                        key={String(v.value)} type="button"
                        onClick={() => setValue('value', String(v.value), { shouldValidate: true })}
                        className="w-full flex items-center justify-between px-3 py-2 text-xs hover:bg-muted rounded-sm transition-colors text-left group"
                      >
                        <span className="truncate max-w-[240px] font-medium">
                          {v.value === null ? <em className="text-muted-foreground">NULL</em> : String(v.label || v.value)}
                        </span>
                        <span className="text-[10px] text-muted-foreground group-hover:text-foreground tabular-nums">
                          {formatCount(v.count)} hits
                        </span>
                      </button>
                    ))}
                  </div>
                </ScrollArea>
              </div>
            )}

            <p className="text-[10px] text-muted-foreground mt-1 leading-tight flex items-start gap-1">
              <span className="text-primary font-bold">Pro Tip:</span>
              <span>Exact matches by default. Use <code className="bg-background border rounded-sm px-1 font-mono text-[9px]">*</code> for wildcards.</span>
            </p>
          </div>
        </div>

        <div className="p-4 bg-background border-t flex justify-end">
          <Button type="submit" disabled={!isValid} className="w-full sm:w-auto shadow-md gap-2 h-10 px-6">
            <Plus className="h-4 w-4" />Apply Filter
          </Button>
        </div>
      </form>
    </DialogContent>
  )
}

export function AddFilterDialog() {
  const [open, setOpen] = React.useState(false)
  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        render={
          <Button data-testid="add-filter-stub" variant="outline" size="sm" className="h-9 sm:h-7 gap-1 pr-2.5 sm:pr-2 pl-2 sm:pl-1.5 border-dashed text-xs sm:text-[11px]">
            <Plus className="h-3 w-3" /><span>Add Filter</span>
          </Button>
        }
      />
      {open && <AddFilterDialogContent onClose={() => setOpen(false)} />}
    </Dialog>
  )
}

type SavedView = components["schemas"]["SavedViewRecord"]

export interface SavedViewStoreSnapshot {
  edgeOnly: boolean; compareMode: boolean
  addFilter: (column: string, value: string, mode: 'include' | 'exclude') => void
  clearFilters: () => void
  setRange: (start: string, end: string) => void
  setRelativeRange: (label: string, start: string, end: string) => void
  toggleEdgeOnly: () => void
  toggleCompareMode: () => void
  setCompareRange: (start: string, end: string) => void
}

export function applySavedViewToStore(view: SavedView, fallbackStore?: SavedViewStoreSnapshot): boolean {
  // filters_json carries TWO shapes for backward compatibility:
  //   - Legacy (older saved views): a bare FilterPill[] array.
  //   - Current: { filters: FilterPill[], _view_extras: {...} }
  //     where extras carries edgeOnly / compareMode / relativeRange.
  // Detect by Array.isArray and apply each branch's recovery.
  let parsed: ReturnType<typeof JSON.parse>
  try { parsed = JSON.parse(view.filters_json) } catch { return false }
  const isLegacy = Array.isArray(parsed)
  const pills = isLegacy ? parsed : parsed?.filters
  const extras = isLegacy ? null : parsed?._view_extras

  // Read latest toggle state after any async fetch (falling back to
  // hook-subscribed state when useFilterStore is mocked in unit tests).
  const liveState = typeof useFilterStore.getState === 'function' ? useFilterStore.getState() : null
  const store = liveState ?? fallbackStore
  if (!store) return false

  // Edge-only toggle — flip if it differs from current.
  if (extras?.edgeOnly != null && extras.edgeOnly !== store.edgeOnly) store.toggleEdgeOnly()

  // Compare-mode toggle + range MUST run BEFORE setRelativeRange / setRange
  // because toggleCompareMode() snaps startTime/endTime and clears relativeRange.
  if (extras?.compareMode != null && extras.compareMode !== store.compareMode) store.toggleCompareMode()
  if (extras?.compareStartTime && extras?.compareEndTime) store.setCompareRange(extras.compareStartTime, extras.compareEndTime)

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
  if (Array.isArray(pills)) pills.forEach((f: FilterPill) => store.addFilter(f.column, f.value, f.mode))
  return true
}

export function SaveViewDialog() {
  const [open, setOpen] = React.useState(false)
  const [name, setName] = React.useState('')

  // Capture the FULL store snapshot so restored views match the original
  // intent — previously edgeOnly / compareMode / relativeRange were silent
  // and a saved rolling-24h view came back as a frozen absolute window.
  const { startTime, endTime, filters, edgeOnly, compareMode, compareStartTime, compareEndTime, relativeRange } = useFilterStore(
    useShallow(s => ({
      startTime: s.startTime, endTime: s.endTime, filters: s.filters, edgeOnly: s.edgeOnly,
      compareMode: s.compareMode, compareStartTime: s.compareStartTime, compareEndTime: s.compareEndTime, relativeRange: s.relativeRange,
    }))
  )
  const activeServiceId = useServiceStore(s => s.activeServiceId)
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
      await unwrapClient(client.POST("/api/views/", {
        body: {
          service_id: activeServiceId, name, filters_json: JSON.stringify(filterPayload),
          start_time: startTime, end_time: endTime, page: pathname || "/dashboard"
        }
      }))
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
            <Bookmark className="h-3.5 w-3.5" />Save View
          </Button>
        }
      />
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle>Save Current View</DialogTitle>
          <DialogDescription>Save your current filters and time range to access them later.</DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 py-4">
          <div className="grid grid-cols-4 items-center gap-4">
            <Label htmlFor="name" className="text-right">Name</Label>
            <Input id="name" value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. Weekly Error Spikes" className="col-span-3" />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)}>Cancel</Button>
          <Button onClick={() => { if (name && activeServiceId) saveMutation.mutate() }} disabled={!name || isSaving}>
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
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const queryClient = useQueryClient()

  // Perf audit Phase D: useBootstrap seeds ['views', service_id] in
  // its queryFn. Gate on bootstrap pending so this query hits the
  // seeded cache on cold load instead of racing the seed.
  const bootstrapPending = useBootstrapPending()

  const { data: views } = useQuery({
    queryKey: queryKeys.views(activeServiceId),
    queryFn: async () => {
      if (!activeServiceId) return []
      return unwrapClient(client.GET("/api/views/{service_id}", { params: { path: { service_id: activeServiceId } } }))
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
    [views, pendingDeleteIds],
  )

  const runDelete = (view: SavedView) => {
    if (!view.id) return
    const id = view.id
    scheduleDelete({
      id,
      toastMessage: `Deleted view "${view.name}"`,
      performDelete: async () => {
        await unwrapClient(client.DELETE("/api/views/{view_id}", { params: { path: { view_id: id } } }))
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
          <Button variant="outline" role="combobox" aria-expanded={open} className="h-9 sm:h-8 justify-between min-w-[140px] text-xs">
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
                  key={view.id} value={view.name}
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
                    variant="ghost" size="icon" aria-label={`Delete view ${view.name}`}
                    className="h-6 w-6 text-muted-foreground hover:text-destructive"
                    onClick={(e) => { e.stopPropagation(); if (view.id) setViewPendingConfirm(view) }}
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
        description={viewPendingConfirm ? `Delete view "${viewPendingConfirm.name}"? You'll have a few seconds to undo.` : ''}
        confirmLabel="Delete"
        isDangerous
        onConfirm={() => { if (viewPendingConfirm) runDelete(viewPendingConfirm); setViewPendingConfirm(null) }}
      />
    </Popover>
  )
}

/**
 * Slim read-only chip strip for pages where the global FilterBar is hidden
 * (/insights, /alerts, /admin, /share-login). Surfaces any filters or the
 * "Edge only" toggle that the user previously set on /dashboard etc. so
 * they aren't invisibly carried forward to a page that doesn't apply them.
 *
 * Renders nothing when no filters are set AND edgeOnly is off.
 *
 * The pills are READ-ONLY here — no toggle, no individual remove. The
 * single "Clear all" affordance clears the entire filter state at once.
 * Per-pill editing belongs on the FilterBar; surfacing it here would
 * suggest those filters apply to the current page, which they don't.
 */
export function ActiveFiltersBanner() {
  const { filters, edgeOnly, clearFilters, toggleEdgeOnly } = useFilterStore(
    useShallow((s) => ({ filters: s.filters, edgeOnly: s.edgeOnly, clearFilters: s.clearFilters, toggleEdgeOnly: s.toggleEdgeOnly })),
  )
  const hasFilters = filters.length > 0
  if (!hasFilters && !edgeOnly) return null

  return (
    <div role="region" aria-label="Active filters from other pages" className="border-b bg-muted/40 px-3 py-1.5 sm:px-4">
      <div className="flex items-center gap-2 flex-wrap text-xs">
        <span className="flex items-center gap-1 text-muted-foreground shrink-0">
          <Info className="h-3 w-3" aria-hidden="true" />
          Filters set elsewhere
          <span className="hidden sm:inline opacity-70">— don&apos;t affect this page</span>
        </span>
        {filters.map((filter) => (
          <Badge
            key={filter.id} variant={filter.mode === 'include' ? 'secondary' : 'outline'}
            className="gap-1 h-6 text-[11px] font-normal opacity-80"
            title={filter.mode === 'include' ? `Including ${filter.column} = ${filter.value}` : `Excluding ${filter.column} = ${filter.value}`}
          >
            <span className="font-bold" aria-hidden="true">{filter.mode === 'include' ? '+' : '−'}</span>
            <span className="opacity-70">{filter.column}:</span>
            <span className="max-w-[160px] truncate">{filter.value}</span>
          </Badge>
        ))}
        {edgeOnly && (
          <Badge variant="secondary" className="h-6 text-[11px] font-normal opacity-80" title="Edge-only toggle is on for dashboard / query pages">
            Edge only
          </Badge>
        )}
        <Button
          variant="ghost" size="sm" className="h-6 px-2 text-[11px] text-muted-foreground hover:text-foreground ml-auto"
          onClick={() => { if (hasFilters) clearFilters(); if (edgeOnly) toggleEdgeOnly() }}
        >
          Clear all
        </Button>
      </div>
    </div>
  )
}

// First-element of every React Query key whose results depend on the
// global start/end time range. Used by the active-pill loading dot's
// predicate so background polls (admin metrics, log-extents, etc.)
// don't blink the pill while their refetches are in flight.
const PRESET_WINDOWS: ReadonlyArray<readonly [string, number]> = [
  ['1h', 1], ['3h', 3], ['6h', 6], ['12h', 12], ['24h', 24], ['3d', 72], ['7d', 168], ['30d', 720],
]
const TIME_BOUND_QUERY_KEYS = new Set(['dashboard', 'sessions', 'usage', 'insights', 'usage-log'])

export const FilterBar = React.memo(function FilterBar() {
  const pathname = usePathname()
  const mounted = useMounted()
  // Selector form so unrelated serviceStore mutations (setServices,
  // setInitialized) don't trigger FilterBar re-renders.
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const {
    startTime, endTime, filters, edgeOnly, relativeRange, setRange, setRelativeRange,
    removeFilter, toggleFilterMode, toggleEdgeOnly, resetAll, resetRange,
    compareMode, compareStartTime, compareEndTime, toggleCompareMode, setCompareRange
  } = useFilterStore(useShallow(state => ({
    startTime: state.startTime, endTime: state.endTime, filters: state.filters, edgeOnly: state.edgeOnly,
    relativeRange: state.relativeRange, setRange: state.setRange, setRelativeRange: state.setRelativeRange,
    removeFilter: state.removeFilter, toggleFilterMode: state.toggleFilterMode, toggleEdgeOnly: state.toggleEdgeOnly,
    resetAll: state.resetAll, resetRange: state.resetRange, compareMode: state.compareMode,
    compareStartTime: state.compareStartTime, compareEndTime: state.compareEndTime,
    toggleCompareMode: state.toggleCompareMode, setCompareRange: state.setCompareRange
  })))

  // Local state for custom date picker
  const [localStart, setLocalStart] = React.useState('')
  const [localEnd, setLocalEnd] = React.useState('')
  const [localCompareStart, setLocalCompareStart] = React.useState('')
  const [localCompareEnd, setLocalCompareEnd] = React.useState('')

  const handleToggleEdgeOnly = React.useCallback((checked: boolean) => {
    React.startTransition(() => { if (checked !== edgeOnly) toggleEdgeOnly() })
  }, [edgeOnly, toggleEdgeOnly])

  const handleToggleCompareMode = React.useCallback((checked: boolean) => {
    React.startTransition(() => { if (checked !== compareMode) toggleCompareMode() })
  }, [compareMode, toggleCompareMode])

  function fmtBotId(id: string): string {
    return id.replace(/-/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
  }

  // Reset extents sync state ONLY when the user explicitly switches services,
  // not on initial page load or hydration.
  useServiceSwitchReset(activeServiceId, () => { resetRange() })

  const { timezone } = useTimezoneStore()
  const dateSource = JSON.stringify([startTime, endTime, compareStartTime, compareEndTime, timezone])
  const [syncedDateSource, setSyncedDateSource] = React.useState<string | null>(null)
  if (mounted && syncedDateSource !== dateSource) {
    setSyncedDateSource(dateSource)
    if (startTime && endTime) {
      setLocalStart(formatForInput(startTime, timezone))
      setLocalEnd(formatForInput(endTime, timezone))
    }
    if (compareStartTime && compareEndTime) {
      setLocalCompareStart(formatForInput(compareStartTime, timezone))
      setLocalCompareEnd(formatForInput(compareEndTime, timezone))
    }
  }

  const handleApplyCustomDate = React.useCallback(() => {
    React.startTransition(() => {
      if (localStart && localEnd) {
        const parsedStart = parseFromInput(localStart, timezone)
        const parsedEnd = parseFromInput(localEnd, timezone)
        if (parsedStart && parsedEnd) setRange(parsedStart, parsedEnd)
      }
      if (compareMode && localCompareStart && localCompareEnd) {
        const parsedStart = parseFromInput(localCompareStart, timezone)
        const parsedEnd = parseFromInput(localCompareEnd, timezone)
        if (parsedStart && parsedEnd) setCompareRange(parsedStart, parsedEnd)
      }
    })
  }, [localStart, localEnd, compareMode, localCompareStart, localCompareEnd, timezone, setRange, setCompareRange])

  // Same-duration compare presets. Offsets the CURRENT primary range
  // backwards by N days and commits straight to the store — local inputs
  // re-sync when the committed range changes. Same-duration matches what
  // toggleCompareMode auto-seeds, so "1 week ago" means "the same window
  // a week earlier" regardless of how long the primary window is. A
  // "previous period" button is intentionally absent because the compare
  // toggle already seeds that on activation.
  const applyComparePreset = React.useCallback((offsetDays: number) => {
    // Prefer the in-flight datetime input values so users get a preset
    // relative to what they're typing, not the last-applied range. Fall
    // back to the committed store range when locals haven't been touched
    // yet (compare toggle just flipped on).
    const primaryStartIso = parseFromInput(localStart, timezone) ?? startTime
    const primaryEndIso = parseFromInput(localEnd, timezone) ?? endTime
    const offsetMs = offsetDays * 24 * 60 * 60 * 1000
    const start = new Date(new Date(primaryStartIso).getTime() - offsetMs).toISOString()
    const end = new Date(new Date(primaryEndIso).getTime() - offsetMs).toISOString()
    React.startTransition(() => { setCompareRange(start, end) })
  }, [localStart, localEnd, startTime, endTime, timezone, setCompareRange])

  const handleReset = React.useCallback(() => { React.startTransition(() => { resetAll() }) }, [resetAll])

  const spanHours = React.useMemo(() => {
    if (!startTime || !endTime) return null
    return (new Date(endTime).getTime() - new Date(startTime).getTime()) / (1000 * 3600)
  }, [startTime, endTime])

  // Prefer the explicit `relativeRange` flag (set by pill click) over
  // duration-derivation. Derivation is the fallback for legacy bookmarks
  // and saved views whose absolute timestamps happen to match a pill.
  const activePreset = React.useMemo(() => {
    if (relativeRange) return relativeRange
    if (!spanHours || !endTime) return null
    if (Math.abs(new Date(endTime).getTime() - new Date().getTime()) > 60000) return null
    const h = Math.round(spanHours * 10) / 10
    return PRESET_WINDOWS.find(([, hrs]) => hrs === h)?.[0] ?? null
  }, [relativeRange, spanHours, endTime])

  // Pills call setRelativeRange so the URL persists as ?range=<label>
  // instead of ?start_time=&end_time=. Reload re-derives [now-duration, now]
  // from the label, so "last 24h" stays rolling.
  const pickRelative = React.useCallback((label: string, hours: number) => {
    const now = new Date()
    const start = new Date(now.getTime() - hours * 3600 * 1000).toISOString()
    React.startTransition(() => setRelativeRange(label, start, now.toISOString()))
  }, [setRelativeRange])

  // Pill flashes only for queries the date range actually affects.
  // Allowlist beats blocklist here: a new background poll added later
  // would silently re-introduce flicker if we just excluded known noisy
  // keys.
  const inFlightCount = useIsFetching({
    predicate: (q) => {
      const key = q.queryKey?.[0]
      return typeof key === 'string' && TIME_BOUND_QUERY_KEYS.has(key)
    }
  })

  const quickPresets = React.useMemo(
    () => [
      { label: '1h', value: () => pickRelative('1h', 1) },
      { label: '3h', value: () => pickRelative('3h', 3) },
      { label: '6h', value: () => pickRelative('6h', 6) },
      { label: '12h', value: () => pickRelative('12h', 12) },
      { label: '24h', value: () => pickRelative('24h', 24) },
      { label: '3d', value: () => pickRelative('3d', 72) },
      { label: '7d', value: () => pickRelative('7d', 168) },
      { label: '30d', value: () => pickRelative('30d', 720) },
    ],
    [pickRelative],
  )

  // Render the real bar structure from first paint instead of a fixed-height
  // placeholder. The old `if (!mounted) return <div h-20/>` placeholder was
  // 80px, but the real bar is 89px (one row) to 133px (wrapped, ≤1440px or
  // sidebar-collapsed) — so the placeholder→real swap pushed all page content
  // down ~53px on every filter-bar page (the dominant CLS source, ~0.20).
  // Rendering the real structure keeps the height correct (it wraps the same
  // way pre/post hydration); the persisted stores use skipHydration so they
  // return identical defaults on server + client-initial render. The only
  // hydration-sensitive bits — the `new Date()`-derived active preset and the
  // URL-hydrated filter pills / compare row — stay gated on `mounted` so they
  // appear only after hydration, within already-reserved fixed-height boxes.
  return (
    <div data-testid="filter-bar" className="flex flex-col gap-1.5 px-4 py-2 border-b bg-background sticky top-0 z-10 shrink-0">
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex items-center gap-0.5 border rounded-md p-0.5 bg-muted/20 h-10 sm:h-8">
          {quickPresets.map((preset) => {
            // `mounted` gate: activePreset derives from `new Date()` (non-
            // deterministic server vs client), so highlight only post-hydration.
            // Active state is className-only — no layout impact either way.
            const isActive = mounted && activePreset === preset.label
            const isActiveLoading = isActive && inFlightCount > 0
            return (
              <Button
                key={preset.label} variant={isActive ? 'default' : 'ghost'} size="sm" onClick={preset.value}
                aria-pressed={isActive} aria-busy={isActiveLoading || undefined}
                className={cn("relative h-9 sm:h-6.5 px-2.5 sm:px-2 text-xs sm:text-[11px]", isActive ? "shadow-sm" : "")}
              >
                {preset.label}
                {isActiveLoading && <span aria-hidden="true" className="ml-1 inline-flex h-1 w-1 rounded-full bg-primary-foreground animate-pulse" />}
              </Button>
            )
          })}
        </div>

        <div className="flex flex-col gap-0.5 w-full sm:w-auto">
          <div className="flex items-center gap-1.5 border rounded-md p-0.5 bg-muted/10 h-10 sm:h-8 w-full sm:w-auto">
            <Input
              type="datetime-local" aria-label="Range start" value={localStart} onChange={e => setLocalStart(e.target.value)}
              className="h-9 sm:h-7 text-xs sm:text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-full sm:w-[160px] flex-1 sm:flex-none px-1"
            />
            <span className="text-muted-foreground text-[10px]">-</span>
            <Input
              type="datetime-local" aria-label="Range end" value={localEnd} onChange={e => setLocalEnd(e.target.value)}
              className="h-9 sm:h-7 text-xs sm:text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-full sm:w-[160px] flex-1 sm:flex-none px-1"
            />
          </div>
          {pathname?.startsWith('/dashboard') && mounted && compareMode && (
            <div className="flex items-center gap-1.5 border rounded-md p-0.5 bg-orange-500/10 border-orange-500/20 h-10 sm:h-8 w-full sm:w-auto">
              <Input
                type="datetime-local" aria-label="Compare range start" value={localCompareStart} onChange={e => setLocalCompareStart(e.target.value)}
                className="h-9 sm:h-7 text-xs sm:text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-full sm:w-[160px] flex-1 sm:flex-none px-1 text-orange-600 dark:text-orange-400"
              />
              <span className="text-muted-foreground text-[10px]">-</span>
              <Input
                type="datetime-local" aria-label="Compare range end" value={localCompareEnd} onChange={e => setLocalCompareEnd(e.target.value)}
                className="h-9 sm:h-7 text-xs sm:text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-full sm:w-[160px] flex-1 sm:flex-none px-1 text-orange-600 dark:text-orange-400"
              />
              <div className="hidden sm:flex items-center gap-0.5 ml-1 pl-1 border-l border-orange-500/20" role="group" aria-label="Compare range presets">
                {([['1w', 7, 'Same window, 7 days earlier'], ['4w', 28, 'Same window, 28 days earlier'], ['1y', 365, 'Same window, 1 year earlier']] as const).map(([label, days, title]) => (
                  <Button
                    key={label} variant="ghost" size="sm" onClick={() => applyComparePreset(days)} title={title}
                    className="h-7 sm:h-6 px-1.5 text-[10px] font-medium text-orange-700 dark:text-orange-300 hover:bg-orange-500/20"
                  >
                    {label}
                  </Button>
                ))}
              </div>
            </div>
          )}
        </div>

        <div className="flex flex-col border-l pl-3 ml-1 gap-1 sm:gap-0 sm:-space-y-0.5">
          <Tooltip>
            <TooltipTrigger render={
              <div className="flex items-center space-x-2 h-7 sm:h-5">
                <Switch id="edge-only" size="sm" checked={mounted && edgeOnly} onCheckedChange={handleToggleEdgeOnly} />
                <Label htmlFor="edge-only" className="text-xs sm:text-[11px] font-medium leading-none cursor-pointer">Edge only</Label>
              </div>
            } />
            <TooltipContent side="bottom" className="max-w-[260px] text-xs">
              Restrict results to edge (CDN) requests only — excludes origin and shield hops so each request counts once.
            </TooltipContent>
          </Tooltip>
          {pathname?.startsWith('/dashboard') && (
            <Tooltip>
              <TooltipTrigger render={
                <div className="flex items-center space-x-2 h-7 sm:h-5">
                  <Switch id="compare-mode" size="sm" checked={mounted && compareMode} onCheckedChange={handleToggleCompareMode} />
                  <Label htmlFor="compare-mode" className="text-xs sm:text-[11px] font-medium leading-none cursor-pointer">Compare</Label>
                </div>
              } />
              <TooltipContent side="bottom" className="max-w-[260px] text-xs">
                Overlay a second time range to compare deltas. The compare range appears below the primary range.
              </TooltipContent>
            </Tooltip>
          )}
        </div>

        <div className="flex items-center gap-1.5">
          <Button size="sm" onClick={handleApplyCustomDate} className="h-9 sm:h-7 px-4 sm:px-3 text-xs sm:text-[11px]">Apply</Button>
          <Button variant="secondary" size="sm" onClick={handleReset} className="h-9 sm:h-7 px-4 sm:px-3 text-xs sm:text-[11px]">Reset</Button>
        </div>

        <div className="flex items-center gap-1.5 border-l pl-3 ml-1">
          <ViewSelector />
          <SaveViewDialog />
        </div>

        <div className="ml-auto" />
      </div>

      {!pathname?.startsWith('/usage') && (
        <div className="flex items-center gap-2 flex-wrap min-h-8 sm:min-h-7">
          {/* Pills derive from the URL-hydrated filter store; render post-mount
              so server (no pills) and client-initial render match. The row
              reserves min-h above so pills land in already-allocated space. */}
          {mounted && filters.map((filter) => (
            <Badge key={filter.id} variant={filter.mode === 'include' ? 'default' : 'destructive'} className="gap-1 pr-1 pl-1 h-8 sm:h-7 text-xs sm:text-[11px]">
              <Button
                variant="ghost" size="icon"
                aria-label={`${filter.mode === 'include' ? 'Including' : 'Excluding'} ${filter.column}=${filter.value}. Activate to toggle.`}
                aria-pressed={filter.mode === 'include'}
                className="h-6 w-6 sm:h-5 sm:w-5 p-0 hover:bg-transparent mr-1"
                onClick={() => React.startTransition(() => toggleFilterMode(filter.id))}
                title="Toggle Include/Exclude"
              >
                <span className="font-bold text-[10px]" aria-hidden="true">{filter.mode === 'include' ? '+' : '-'}</span>
              </Button>
              {filter.column === '_bot_name' ? (
                <><Bot className="h-3 w-3 opacity-70 mr-0.5" /><span className="opacity-70">Bot:</span><span className="max-w-[180px] truncate">{fmtBotId(filter.value)}</span></>
              ) : filter.column === '_ngwaf_bot_name' ? (
                <><Bot className="h-3 w-3 opacity-70 mr-0.5" /><span className="opacity-70">Fastly Verified Bot:</span><span className="max-w-[180px] truncate">{filter.value}</span></>
              ) : (
                <><span className="opacity-70">{filter.column}:</span><span className="max-w-[180px] truncate">{filter.value}</span></>
              )}
              <Button
                variant="ghost" size="icon" aria-label={`Remove filter ${filter.column}: ${filter.value}`} title="Remove filter"
                className="h-6 w-6 sm:h-5 sm:w-5 p-0 hover:bg-transparent ml-1"
                onClick={() => React.startTransition(() => removeFilter(filter.id))}
              >
                <X className="h-3 w-3" aria-hidden="true" />
              </Button>
            </Badge>
          ))}
          <AddFilterDialog />
        </div>
      )}
    </div>
  )
})
