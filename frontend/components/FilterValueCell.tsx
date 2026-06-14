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

  if (filters.length === 0 || shownValue === '' || shownValue == null) {
    // Empty cell — render the display (or empty) without a trigger.
    return (
      <div className={cn('flex items-center gap-2', containerClassName)}>
        <span className={cn('truncate block', className)}>{shownValue}</span>
      </div>
    )
  }

  return (
    <div className={cn('flex items-center gap-2 group', containerClassName)}>
      <span className={cn('truncate block', className)}>{shownValue}</span>
      <DropdownMenu>
        <DropdownMenuTrigger
          render={
            <button
              type="button"
              aria-label={`Filter actions for ${filters[0].value}`}
              className="opacity-0 group-hover:opacity-100 data-[popup-open]:opacity-100 transition-opacity shrink-0 inline-flex items-center justify-center h-5 w-5 rounded text-muted-foreground hover:text-foreground hover:bg-accent"
            />
          }
        >
          <ChevronDown className="h-3 w-3" aria-hidden="true" />
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start" sideOffset={2} className="min-w-[180px]">
          <DropdownMenuItem onClick={handleFilterHere}>
            <Filter className="h-3.5 w-3.5" aria-hidden="true" />
            <span>Filter {pageLabel} page</span>
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
    </div>
  )
}
