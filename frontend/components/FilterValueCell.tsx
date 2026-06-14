'use client'

import * as React from 'react'
import { usePathname } from 'next/navigation'
import { ChevronDown, ExternalLink, Filter, Copy } from 'lucide-react'

import { cn } from '@/lib/utils'
import { useFilterStore } from '@/stores/filterStore'
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuShortcut,
} from '@/components/ui/dropdown-menu'

export interface FilterValueCellFilter {
  column: string
  value: string
}

interface FilterValueCellProps {
  filters: FilterValueCellFilter[]
  display?: React.ReactNode
  className?: string
  containerClassName?: string
}

// Pathname → human-readable page name used in the menu label. Lookup is
// intentionally exhaustive so a new page that adds the cell needs a
// matching entry — keeps the menu reading "Filter origin" instead of
// quietly degrading to "Filter this page" if someone forgets.
const PAGE_LABELS: Record<string, string> = {
  '/dashboard': 'dashboard',
  '/origin': 'origin',
  '/performance': 'performance',
  '/security': 'security',
  '/network': 'network',
  '/sessions': 'sessions',
  '/charts': 'charts',
  '/usage': 'usage',
  '/query': 'query',
}

function pageLabelFor(pathname: string | null): string {
  if (!pathname) return 'this page'
  for (const [prefix, label] of Object.entries(PAGE_LABELS)) {
    if (pathname === prefix || pathname.startsWith(prefix + '/')) return label
  }
  return 'this page'
}

export function buildDashboardFilterUrl(filters: FilterValueCellFilter[]): string {
  const qs = filters
    .map(f => `filter_${f.column}=${encodeURIComponent(f.value)}`)
    .join('&')
  return `/dashboard?${qs}`
}

export function FilterValueCell({
  filters,
  display,
  className,
  containerClassName,
}: FilterValueCellProps) {
  const pathname = usePathname()
  const addFilter = useFilterStore(state => state.addFilter)
  const onDashboard = pathname === '/dashboard'
  const pageLabel = pageLabelFor(pathname)
  const shownValue = display ?? filters[0]?.value ?? ''
  const [open, setOpen] = React.useState(false)

  const handleFilterHere = React.useCallback(() => {
    for (const f of filters) addFilter(f.column, f.value, 'include')
  }, [filters, addFilter])

  const handleOpenInDashboard = React.useCallback(() => {
    window.open(buildDashboardFilterUrl(filters), '_blank', 'noopener,noreferrer')
  }, [filters])

  const handleCopy = React.useCallback(() => {
    const v = filters[0]?.value
    if (v) navigator.clipboard?.writeText(v).catch(() => {})
  }, [filters])

  // Modifier-key shortcut: cmd/ctrl-click on the whole cell triggers
  // "Filter this page" directly, bypassing the menu. Tracked across both
  // mousedown and click because base-ui opens the menu on click and
  // preventDefault on mousedown alone doesn't stop the React click event.
  // Plain click still opens the menu — the modifier path is the override.
  const modifierRef = React.useRef(false)
  const handleMouseDown = React.useCallback(
    (e: React.MouseEvent) => {
      modifierRef.current = e.metaKey || e.ctrlKey
      if (modifierRef.current) {
        e.preventDefault()
        e.stopPropagation()
        handleFilterHere()
      }
    },
    [handleFilterHere],
  )
  const handleClick = React.useCallback((e: React.MouseEvent) => {
    if (modifierRef.current) {
      e.preventDefault()
      e.stopPropagation()
      modifierRef.current = false
      // base-ui's Trigger flips open AFTER React's synthetic click event
      // (via its own pointer-down handler). Calling setOpen(false) inside
      // this handler races against base-ui's setOpen(true). Queue the close
      // for the next tick so it always wins.
      setTimeout(() => setOpen(false), 0)
    }
  }, [])

  if (filters.length === 0 || shownValue === '' || shownValue == null) {
    return (
      <div className={cn('flex items-center gap-2', containerClassName)}>
        <span className={cn('truncate block', className)}>{shownValue}</span>
      </div>
    )
  }

  return (
    <DropdownMenu open={open} onOpenChange={setOpen}>
      <DropdownMenuTrigger
        render={
          <button
            type="button"
            aria-label={`Filter actions for ${filters[0].value}`}
            title="Click for actions · ⌘/Ctrl-click to filter this page"
            onMouseDown={handleMouseDown}
            onClick={handleClick}
            className={cn(
              'group flex items-center gap-2 text-left rounded-sm -mx-1 px-1 py-0.5 hover:bg-accent/60 data-[popup-open]:bg-accent/60 transition-colors w-full min-w-0',
              containerClassName,
            )}
          />
        }
      >
        <span className={cn('truncate block flex-1', className)}>{shownValue}</span>
        <ChevronDown
          className="h-3 w-3 opacity-0 group-hover:opacity-70 data-[popup-open]:opacity-100 shrink-0 text-muted-foreground transition-opacity"
          aria-hidden="true"
        />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" sideOffset={2} className="min-w-[200px]">
        <DropdownMenuItem onClick={handleFilterHere}>
          <Filter className="h-3.5 w-3.5" aria-hidden="true" />
          <span>Filter {pageLabel} page</span>
          <DropdownMenuShortcut>⌘+Click</DropdownMenuShortcut>
        </DropdownMenuItem>
        {!onDashboard && (
          <DropdownMenuItem onClick={handleOpenInDashboard}>
            <ExternalLink className="h-3.5 w-3.5" aria-hidden="true" />
            <span>Open in dashboard</span>
          </DropdownMenuItem>
        )}
        <DropdownMenuItem onClick={handleCopy}>
          <Copy className="h-3.5 w-3.5" aria-hidden="true" />
          <span>Copy value</span>
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
