'use client'

import * as React from 'react'
import { Check, ChevronsUpDown, Pin, PinOff } from 'lucide-react'
import { usePathname } from 'next/navigation'

import { cn } from '@/lib/utils'
import { Button, buttonVariants } from '@/components/ui/button'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from '@/components/ui/command'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import { useServiceStore } from '@/stores/serviceStore'
import { useLocalStorageString } from '@/hooks/useLocalStorageString'

// localStorage shape: pinned IDs (user-ordered) + MRU IDs (most-recent first).
// Keyed globally — admin's switch list and analyst's switch list both share
// the same browser profile, and analysts only see services their invite
// allows anyway, so cross-tenant leakage isn't a concern.
const PREFS_KEY = 'fla-service-switcher-prefs'
const MAX_MRU = 5

interface ServicePrefs {
  pinned: string[]
  mru: string[]
}

function parsePrefs(raw: string | null): ServicePrefs {
  if (!raw) return { pinned: [], mru: [] }
  try {
    const parsed = JSON.parse(raw)
    return {
      pinned: Array.isArray(parsed.pinned) ? parsed.pinned : [],
      mru: Array.isArray(parsed.mru) ? parsed.mru : [],
    }
  } catch {
    return { pinned: [], mru: [] }
  }
}

// Pre-fix this component also called useBootstrap() to "ensure services are
// loaded". AppLayout (which wraps this) already calls useBootstrap and
// populates useServiceStore as a side effect — having ServiceSwitcher call
// it too added a second hook subscription that triggered extra renders on
// every bootstrap settle, even though React Query deduped the network
// request itself. The store is the right read source.
export function ServiceSwitcher() {
  const [open, setOpen] = React.useState(false)
  // Selector form so unrelated serviceStore mutations don't re-render the
  // switcher. Each slice subscribes only to its own field.
  const services = useServiceStore((s) => s.services)
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const setActiveServiceId = useServiceStore((s) => s.setActiveServiceId)
  const pathname = usePathname()

  // The prefs live in localStorage, read as external state: the server snapshot is empty, so the
  // first client render matches the server and the stored prefs land right after hydration.
  const [prefsRaw, writePrefsRaw] = useLocalStorageString(PREFS_KEY)
  const prefs = React.useMemo(() => parsePrefs(prefsRaw), [prefsRaw])
  const savePrefs = React.useCallback((next: ServicePrefs) => writePrefsRaw(JSON.stringify(next)), [writePrefsRaw])

  // Touch MRU whenever the active service changes (even from outside this
  // switcher — e.g. /admin/_sections/ServicesTable also setActiveServiceId).
  React.useEffect(() => {
    if (!activeServiceId || prefs.mru[0] === activeServiceId) return
    savePrefs({
      ...prefs,
      mru: [activeServiceId, ...prefs.mru.filter((x) => x !== activeServiceId)].slice(0, MAX_MRU),
    })
  }, [activeServiceId, prefs, savePrefs])

  const activeService = services.find((s) => s.id === activeServiceId)
  const byId = React.useMemo(() => {
    const m = new Map<string, (typeof services)[number]>()
    for (const s of services) m.set(s.id, s)
    return m
  }, [services])

  // Bucket services into pinned / recent / all-others. Pinned and recent
  // dedupe against each other so a service that's both pinned AND recent
  // only renders in the pinned group (priority order: pinned > recent > all).
  const pinnedServices = React.useMemo(
    () => prefs.pinned.map((id) => byId.get(id)).filter(Boolean) as typeof services,
    [prefs.pinned, byId],
  )
  const pinnedSet = React.useMemo(() => new Set(prefs.pinned), [prefs.pinned])
  const recentServices = React.useMemo(
    () =>
      prefs.mru
        .filter((id) => !pinnedSet.has(id))
        .map((id) => byId.get(id))
        .filter(Boolean) as typeof services,
    [prefs.mru, pinnedSet, byId],
  )
  const recentSet = React.useMemo(() => new Set(recentServices.map((s) => s.id)), [recentServices])
  const otherServices = React.useMemo(
    () =>
      services
        .filter((s) => !pinnedSet.has(s.id) && !recentSet.has(s.id))
        .slice()
        .sort((a, b) => a.name.localeCompare(b.name)),
    [services, pinnedSet, recentSet],
  )

  const handleSelect = (id: string) => {
    setActiveServiceId(id)
    setOpen(false)
    // Hard reload so all panels (traffic chart, etc.) start fresh for the new
    // service rather than trying to reconcile stale React Query / Web Worker
    // state mid-flight. setActiveServiceId persists to localStorage so the
    // store rehydrates to the correct service on the reload.
    window.location.assign(`${pathname}?service=${id}`)
  }

  const handleTogglePin = (id: string) => {
    savePrefs({
      ...prefs,
      pinned: prefs.pinned.includes(id)
        ? prefs.pinned.filter((x) => x !== id)
        : [...prefs.pinned, id],
    })
  }

  const renderRow = (
    service: (typeof services)[number],
    opts: { pinned: boolean },
  ) => (
    <CommandItem
      key={service.id}
      value={service.name}
      onSelect={() => handleSelect(service.id)}
      className="group/row flex items-center gap-1 pr-1"
    >
      <Check
        className={cn(
          'mr-2 h-4 w-4 shrink-0',
          activeServiceId === service.id ? 'opacity-100' : 'opacity-0',
        )}
      />
      <span className="truncate flex-1">{service.name}</span>
      <Button
        variant="ghost"
        size="icon"
        aria-label={opts.pinned ? `Unpin ${service.name}` : `Pin ${service.name} to top`}
        title={opts.pinned ? 'Unpin' : 'Pin to top'}
        // Pin button lives inside a cmdk CommandItem — without
        // stopPropagation the parent's onSelect fires too and switches
        // services on every pin toggle.
        onClick={(e) => {
          e.stopPropagation()
          handleTogglePin(service.id)
        }}
        className={cn(
          'h-6 w-6 shrink-0 text-muted-foreground hover:text-foreground',
          // Always-visible icon when pinned (so the user can unpin);
          // hover-only when unpinned to keep the list visually quiet.
          opts.pinned
            ? 'opacity-90'
            : 'opacity-0 group-hover/row:opacity-60 focus-visible:opacity-100',
        )}
      >
        {opts.pinned ? <Pin className="h-3 w-3" /> : <PinOff className="h-3 w-3" />}
      </Button>
    </CommandItem>
  )

  return (
    <Popover open={open} onOpenChange={setOpen}>
      {/* M-5 (audit, mobile UX): w-auto + max-w on mobile so the trigger
          collapses around its label (or "Select service...") instead of
          eating 250px of header width on phones, where SyncStatusBadge,
          TimezoneSwitcher, and ThemeToggle then have nowhere to sit.
          On >=sm we restore the original fixed 250px so service names
          have room to breathe on desktop. */}
      <PopoverTrigger
        role="combobox"
        aria-expanded={open}
        aria-label="Active service"
        className={cn(
          buttonVariants({ variant: 'outline' }),
          'w-auto max-w-[60vw] sm:w-[250px] sm:max-w-none justify-between',
        )}
      >
        <span className="flex min-w-0 flex-1 items-center justify-between">
          <span className="truncate">
            {activeService ? activeService.name : 'Select service...'}
          </span>
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </span>
      </PopoverTrigger>
      <PopoverContent className="w-[260px] p-0">
        <Command>
          <CommandInput placeholder="Search service..." />
          <CommandList>
            <CommandEmpty>No service found.</CommandEmpty>
            {pinnedServices.length > 0 && (
              <CommandGroup heading="Pinned">
                {pinnedServices.map((s) => renderRow(s, { pinned: true }))}
              </CommandGroup>
            )}
            {recentServices.length > 0 && (
              <>
                {pinnedServices.length > 0 && <CommandSeparator />}
                <CommandGroup heading="Recent">
                  {recentServices.map((s) => renderRow(s, { pinned: false }))}
                </CommandGroup>
              </>
            )}
            {otherServices.length > 0 && (
              <>
                {(pinnedServices.length > 0 || recentServices.length > 0) && (
                  <CommandSeparator />
                )}
                <CommandGroup
                  heading={
                    pinnedServices.length > 0 || recentServices.length > 0
                      ? 'All services'
                      : undefined
                  }
                >
                  {otherServices.map((s) => renderRow(s, { pinned: false }))}
                </CommandGroup>
              </>
            )}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}
